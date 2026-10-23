{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE MonoLocalBinds #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.IO.Parquet (
    -- * Reading
    readParquet,
    readParquetWithOpts,
    readParquetFiles,
    readParquetFilesWithOpts,

    -- * Writing
    writeParquet,
    writeParquetWithOptions,

    -- * Options
    ParquetReadOptions (..),
    defaultParquetReadOptions,
    ParquetWriteOptions (..),
    WriterStrategy (..),
    defaultParquetWriteOptions,

    -- * File metadata
    parseFileMetadata,
    readMetadataFromPath,
    readMetadataFromHandle,
    columnChunksForAll,

    -- * Schema description
    ColumnDescription (..),
    generateColumnDescriptions,
    getColumnNames,

    -- * Decoding
    parseParquetWithOpts,
    parseColumnChunks,
    getNonNullableColumn,
    getNullableColumn,
    getRepeatedColumn,
    applyDescLogicalType,
    applyLogicalType,
    nativeTypeHints,
    restoreNativeType,

    -- * Applying read options to a decoded frame
    applyReadOptions,
    applyPredicate,
    applySelectedColumns,
    applyRowRange,
    applySafeRead,

    -- * Data sources
    RandomAccess (..),
    ReaderIO (runReaderIO),
    FileBufferedOrSeekable,
    ForceNonSeekable,
    withFileBufferedOrSeekable,
    _readParquetWithOpts,
    readParquetWithMetadata,
    FileMetadata,
) where

import Control.Concurrent (getNumCapabilities)
import Control.Exception (throw)
import Control.Monad
import Control.Monad.IO.Class (MonadIO (..))
import Control.Monad.ST (stToIO)
import Data.Bits (Bits (shiftL), (.|.))
import qualified Data.ByteString as BS
import Data.Either (fromRight)
import Data.Functor ((<&>))
import Data.IORef (atomicModifyIORef', newIORef)
import Data.Int (Int32, Int64)
import Data.Maybe (fromJust, fromMaybe)
#if !MIN_VERSION_base(4,20,0)
import Data.List (foldl', transpose)
#else
import Data.List (transpose)
#endif
import qualified Data.List as L
import qualified Data.Map as Map
import qualified Data.Text as T
import Data.Time.Calendar (Day (ModifiedJulianDay))
import Data.Time.Clock (UTCTime (UTCTime), picosecondsToDiffTime)
import Data.Type.Equality (testEquality, (:~:) (Refl))
import qualified Data.Vector as Vector
import qualified Data.Vector.Mutable as MVector
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import DataFrame.Errors (DataFrameException (ColumnsNotFoundException))
import DataFrame.IO.Parquet.Dictionary (
    DictVals,
    dictDouble,
    dictFloat,
    dictInt32,
    dictInt64,
    word32At,
    word64At,
 )
import DataFrame.IO.Parquet.Page (
    PageDecoder,
    UnboxedPageDecoder,
    appendNullableStringPageIO,
    appendStringPageIO,
    boolDecoder,
    byteArrayDecoder,
    decodeRequiredNumericInto,
    doubleDecoder,
    fixedLenByteArrayDecoder,
    floatDecoder,
    foldColumnDataPagesM,
    foldColumnPagesM,
    int32Decoder,
    int64Decoder,
    int96Decoder,
 )
import DataFrame.IO.Parquet.Seeking (
    FileBufferedOrSeekable (..),
    ForceNonSeekable,
    withFileBufferedOrSeekable,
 )
import DataFrame.IO.Parquet.Thrift (
    ColumnChunk (..),
    ColumnMetaData (..),
    DecimalType (..),
    FileMetadata (..),
    KeyValue (..),
    LogicalType (..),
    RowGroup (..),
    Statistics (..),
    ThriftType (..),
    TimeUnit (..),
    TimestampType (..),
    unField,
 )
import DataFrame.IO.Parquet.Utils (
    ColumnDescription (..),
    foldNonNullable,
    foldNonNullableUnboxed,
    foldNullable,
    foldNullableUnboxed,
    foldRepeated,
    foldRepeatedUnboxed,
    generateColumnDescriptions,
    getColumnNames,
 )
import DataFrame.IO.Parquet.Writer (
    ParquetWriteOptions (..),
    WriterStrategy (..),
    defaultParquetWriteOptions,
    nativeTypeKeyPrefix,
    writeParquet,
    writeParquetWithOptions,
 )
import DataFrame.IO.Utils.RandomAccess (
    ChunkBuffer (..),
    RandomAccess (..),
    Range (Range),
    ReaderIO (runReaderIO),
 )
import DataFrame.Internal.Column (Column, Columnable)
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.Column.Builder (
    concatColumnsIO,
    freezeTextChunk,
    mergeTextChunks,
    newTextBuilder,
 )
import DataFrame.Internal.Control.Concurrent (forkJoin_)
import DataFrame.Internal.DataFrame (DataFrame (..))
import DataFrame.Internal.Expression (
    BinOpId (..),
    BinaryOp (binaryId),
    Expr (..),
    builtinOf,
    getColumns,
 )
import DataFrame.Operations.Merge ()
import qualified DataFrame.Operations.Subset as DS
import DataFrame.Schema (
    Schema (..),
    SchemaType,
    makeSchema,
    schemaType,
 )
import GHC.Float (castWord32ToFloat, castWord64ToDouble)
import qualified Pinch
import System.Directory (doesDirectoryExist)
import System.FilePath ((</>))
import System.FilePath.Glob (glob)
import System.IO (IOMode (ReadMode))
import Type.Reflection (typeRep)

-- | Options for reading Parquet data.
data ParquetReadOptions = ParquetReadOptions
    { selectedColumns :: Maybe [T.Text]
    -- ^ Columns to keep in the final dataframe.
    , predicate :: Maybe (Expr Bool)
    -- ^ Optional row filter expression applied before projection.
    , rowRange :: Maybe (Int, Int)
    -- ^ Optional row slice @(start, end)@, start-inclusive, end-exclusive.
    , safeColumns :: Bool
    -- ^ Promote every column to OptionalColumn after read.
    , widenIntegers :: Bool
    -- ^ Decode INT32 and INT64 columns as 'Int'.
    , parallel :: Bool
    -- ^ Decode row groups concurrently using every RTS capability.
    }
    deriving (Show)

-- | Default Parquet read options.
defaultParquetReadOptions :: ParquetReadOptions
defaultParquetReadOptions =
    ParquetReadOptions
        { selectedColumns = Nothing
        , predicate = Nothing
        , rowRange = Nothing
        , safeColumns = False
        , widenIntegers = False
        , parallel = True
        }

-- | Read a parquet file from path and load it into a dataframe.
readParquet :: FilePath -> IO DataFrame
readParquet = readParquetWithOpts defaultParquetReadOptions

-- | Read a Parquet file using explicit read options.
readParquetWithOpts :: ParquetReadOptions -> FilePath -> IO DataFrame
readParquetWithOpts = _readParquetWithOpts Nothing

-- | Internal entry point used by tests to force non-seekable mode.
_readParquetWithOpts ::
    ForceNonSeekable -> ParquetReadOptions -> FilePath -> IO DataFrame
_readParquetWithOpts extraConfig opts path = do
    metadata <-
        withFileBufferedOrSeekable
            extraConfig
            path
            ReadMode
            (runReaderIO parseFileMetadata)
    readParquetWithMetadata extraConfig metadata opts path

-- | Read a Parquet file whose footer has already been parsed.
readParquetWithMetadata ::
    ForceNonSeekable ->
    FileMetadata ->
    ParquetReadOptions ->
    FilePath ->
    IO DataFrame
readParquetWithMetadata extraConfig metadata opts path = do
    caps <- getNumCapabilities
    let nRowGroups = length (unField metadata.row_groups)
    if parallel opts && extraConfig /= Just True && caps > 1 && nRowGroups > 1
        then decodeRowGroupsParallel caps opts path metadata
        else withFileBufferedOrSeekable extraConfig path ReadMode $ \file ->
            runReaderIO (parseParquetWithOpts opts) file

-- | Decode row groups in parallel, one task per (row group, column).
decodeRowGroupsParallel ::
    Int -> ParquetReadOptions -> FilePath -> FileMetadata -> IO DataFrame
decodeRowGroupsParallel caps opts0 path metadata = do
    let plan = planColumns opts0 metadata
        descs = Vector.fromList (planDescs plan)
        allRowGroupRows =
            map (fromIntegral . unField . rg_num_rows) (unField metadata.row_groups)
        allOffsets = scanl (+) 0 allRowGroupRows
        -- Row groups whose statistics rule out the predicate are skipped.
        statsGroups =
            [ g
            | (g, rg) <- zip [0 ..] (unField metadata.row_groups)
            , maybe True (rowGroupMayMatch metadata rg) (predicate opts0)
            ]
        -- A row range only touches overlapping groups, then is rebased.
        (keptGroups, opts) = case rowRange opts0 of
            Nothing -> (statsGroups, opts0)
            Just (s, e) ->
                let groups =
                        [ g
                        | (g, off, n) <- zip3 [0 ..] allOffsets allRowGroupRows
                        , off < e
                        , off + n > s
                        ]
                    base = case groups of
                        [] -> 0
                        (g : _) -> allOffsets !! g
                 in (groups, opts0{rowRange = Just (s - base, e - base)})
        chunks =
            Vector.fromList
                [Vector.fromList [cs !! g | g <- keptGroups] | cs <- planChunks plan]
        rowGroupRows = Vector.fromList [allRowGroupRows !! g | g <- keptGroups]
        rowOffsets = Vector.prescanl' (+) 0 rowGroupRows
        totalRows = Vector.sum rowGroupRows
        nCols = Vector.length descs
        nRg = Vector.length rowGroupRows
        nTasks = nCols * nRg
        taskChunk t = let (r, c) = t `divMod` nCols in chunks Vector.! c Vector.! r
    sinks <- Vector.mapM (newSink (widenIntegers opts) totalRows) descs
    results <- MVector.new nTasks
    counter <- newIORef (0 :: Int)
    -- Each worker owns a handle, so chunk reads run in parallel too.
    let worker = withFileBufferedOrSeekable Nothing path ReadMode $ \file ->
            let loop = do
                    t <- atomicModifyIORef' counter (\n -> (n + 1, n))
                    when (t < nTasks) $ do
                        let (r, c) = t `divMod` nCols
                            desc = descs Vector.! c
                            chunk = taskChunk t
                            (base, len) = chunkRange chunk
                        bytes <- runReaderIO (readBytes (Range base len)) file
                        let buffer = ChunkBuffer base bytes
                            into ::
                                forall a.
                                (VU.Unbox a) =>
                                (DictVals -> Maybe (VU.Vector a)) ->
                                (BS.ByteString -> Int -> a) ->
                                Int ->
                                VUM.IOVector a ->
                                IO ()
                            into pick plainAt width out =
                                void $
                                    runReaderIO
                                        ( decodeRequiredNumericInto
                                            pick
                                            plainAt
                                            width
                                            desc
                                            chunk
                                            out
                                            (rowOffsets Vector.! r)
                                        )
                                        buffer
                        case sinks Vector.! c of
                            SinkInt out width ->
                                if width == 4
                                    then into (fmap (VU.map fromIntegral) . dictInt32) plainIntFrom32 4 out
                                    else into (fmap (VU.map fromIntegral) . dictInt64) plainIntFrom64 8 out
                            SinkInt32 out -> into dictInt32 plainInt32 4 out
                            SinkInt64 out -> into dictInt64 plainInt64 8 out
                            SinkFloat out -> into dictFloat plainFloat 4 out
                            SinkDouble out -> into dictDouble plainDouble 8 out
                            SinkRows -> do
                                col <-
                                    runReaderIO
                                        (parseColumnChunks (rowGroupRows Vector.! r) [chunk] desc)
                                        buffer
                                MVector.write results (c * nRg + r) col
                        loop
             in loop
    forkJoin_ (replicate (max 1 (min caps nTasks)) worker)
    cols <- forM [0 .. nCols - 1] $ \c -> case sinks Vector.! c of
        SinkInt out _ -> DI.UnboxedColumn Nothing <$> VU.unsafeFreeze out
        SinkInt32 out -> DI.UnboxedColumn Nothing <$> VU.unsafeFreeze out
        SinkInt64 out -> DI.UnboxedColumn Nothing <$> VU.unsafeFreeze out
        SinkFloat out -> DI.UnboxedColumn Nothing <$> VU.unsafeFreeze out
        SinkDouble out -> DI.UnboxedColumn Nothing <$> VU.unsafeFreeze out
        SinkRows ->
            concatColumnsIO
                =<< forM [0 .. nRg - 1] (\r -> MVector.read results (c * nRg + r))
    pure (assembleDataFrame opts totalRows metadata plan cols)

-- | Where a column's decoded values go.
data ColumnSink
    = -- | Widened integers; the width is the source's bytes per value.
      SinkInt !(VUM.IOVector Int) !Int
    | SinkInt32 !(VUM.IOVector Int32)
    | SinkInt64 !(VUM.IOVector Int64)
    | SinkFloat !(VUM.IOVector Float)
    | SinkDouble !(VUM.IOVector Double)
    | SinkRows

newSink :: Bool -> Int -> ColumnDescription -> IO ColumnSink
newSink widen totalRows description
    | description.maxRepetitionLevel /= 0 || description.maxDefinitionLevel /= 0 =
        pure SinkRows
    | otherwise = case description.colElementType of
        Just (INT32 _)
            | widen -> (`SinkInt` 4) <$> VUM.unsafeNew totalRows
            | otherwise -> SinkInt32 <$> VUM.unsafeNew totalRows
        Just (INT64 _)
            | widen -> (`SinkInt` 8) <$> VUM.unsafeNew totalRows
            | otherwise -> SinkInt64 <$> VUM.unsafeNew totalRows
        Just (FLOAT _) -> SinkFloat <$> VUM.unsafeNew totalRows
        Just (DOUBLE _) -> SinkDouble <$> VUM.unsafeNew totalRows
        _ -> pure SinkRows

plainIntFrom32 :: BS.ByteString -> Int -> Int
plainIntFrom32 bs o = fromIntegral (fromIntegral (word32At bs o) :: Int32)
{-# INLINE plainIntFrom32 #-}

plainIntFrom64 :: BS.ByteString -> Int -> Int
plainIntFrom64 bs o = fromIntegral (word64At bs o)
{-# INLINE plainIntFrom64 #-}

plainInt32 :: BS.ByteString -> Int -> Int32
plainInt32 bs o = fromIntegral (word32At bs o)
{-# INLINE plainInt32 #-}

plainInt64 :: BS.ByteString -> Int -> Int64
plainInt64 bs o = fromIntegral (word64At bs o)
{-# INLINE plainInt64 #-}

plainFloat :: BS.ByteString -> Int -> Float
plainFloat bs o = castWord32ToFloat (word32At bs o)
{-# INLINE plainFloat #-}

plainDouble :: BS.ByteString -> Int -> Double
plainDouble bs o = castWord64ToDouble (word64At bs o)
{-# INLINE plainDouble #-}

-- | Whether a row group can hold a row satisfying the predicate.
rowGroupMayMatch :: FileMetadata -> RowGroup -> Expr Bool -> Bool
rowGroupMayMatch metadata rg = go
  where
    allNames = getColumnNames (drop 1 (unField metadata.schema))
    chunks = unField (rg_columns rg)
    go :: Expr Bool -> Bool
    go (Binary op l r)
        | Just tag <- builtinOf (binaryId op)
        , tag `elem` [BAnd, BNullAnd] = case (l, r) of
            (l' :: Expr lt, r' :: Expr rt)
                | Just Refl <- testEquality (typeRep @lt) (typeRep @Bool)
                , Just Refl <- testEquality (typeRep @rt) (typeRep @Bool) ->
                    go l' && go r'
            _ -> True
        | otherwise = case (l, r) of
            (Col name :: Expr lt, Lit (v :: rt))
                | Just Refl <- testEquality (typeRep @lt) (typeRep @Int)
                , Just Refl <- testEquality (typeRep @rt) (typeRep @Int) ->
                    maybe True (\t -> decide name t v) (builtinOf (binaryId op))
            (Lit (v :: lt), Col name :: Expr rt)
                | Just Refl <- testEquality (typeRep @lt) (typeRep @Int)
                , Just Refl <- testEquality (typeRep @rt) (typeRep @Int) ->
                    maybe True (\t -> decide name (flipped t) v) (builtinOf (binaryId op))
            _ -> True
    go _ = True
    flipped :: BinOpId -> BinOpId
    flipped n = case n of
        BLt -> BGt
        BGt -> BLt
        BLeq -> BGeq
        BGeq -> BLeq
        other -> other
    -- @column <cmp> v@ can hold for some row in [lo, hi].
    decide :: T.Text -> BinOpId -> Int -> Bool
    decide name cmp v = case columnRange name of
        Nothing -> True
        Just (lo, hi) -> case cmp of
            BEq -> lo <= v && v <= hi
            BNeq -> not (lo == v && hi == v)
            BLt -> lo < v
            BLeq -> lo <= v
            BGt -> hi > v
            BGeq -> hi >= v
            _ -> True
    columnRange :: T.Text -> Maybe (Int, Int)
    columnRange name = do
        i <- L.findIndex (columnMatches allNames name) allNames
        cc <- chunks `atMay` i
        meta <- unField (cc_meta_data cc)
        stats <- unField (cmd_statistics meta)
        let decodeInt bs = case unField (cmd_type meta) of
                INT32 _
                    | BS.length bs == 4 ->
                        Just (fromIntegral (fromIntegral (word32At bs 0) :: Int32))
                INT64 _ | BS.length bs == 8 -> Just (fromIntegral (word64At bs 0))
                _ -> Nothing
        lo <-
            decodeInt
                =<< (unField (stats_min_value stats) `orElse` unField (stats_min stats))
        hi <-
            decodeInt
                =<< (unField (stats_max_value stats) `orElse` unField (stats_max stats))
        pure (lo, hi)
    orElse (Just x) _ = Just x
    orElse Nothing y = y
    atMay xs i = if i >= 0 && i < length xs then Just (xs !! i) else Nothing

-- | File offset and byte length of a column chunk (dictionary page included).
chunkRange :: ColumnChunk -> (Integer, Int)
chunkRange columnChunk =
    let meta = fromJust . unField $ columnChunk.cc_meta_data
        dataOffset = fromIntegral . unField $ meta.cmd_data_page_offset
        dictOffset = fromIntegral <$> unField meta.cmd_dictionary_page_offset
     in ( fromMaybe dataOffset dictOffset
        , fromIntegral . unField $ meta.cmd_total_compressed_size
        )

-- | The columns a read decodes, after projection, in schema order.
data ColumnPlan = ColumnPlan
    { planNames :: [T.Text]
    , planDescs :: [ColumnDescription]
    , planChunks :: [[ColumnChunk]]
    -- ^ Per column, one chunk per row group.
    }

-- | Read Parquet files from a directory or glob path.
readParquetFiles :: FilePath -> IO DataFrame
readParquetFiles = readParquetFilesWithOpts defaultParquetReadOptions

-- | Read multiple Parquet files (directory or glob) using options.
readParquetFilesWithOpts :: ParquetReadOptions -> FilePath -> IO DataFrame
readParquetFilesWithOpts opts path = do
    isDir <- doesDirectoryExist path

    let pat = if isDir then path </> "*.parquet" else path

    matches <- glob pat

    files <- L.sort <$> filterM (fmap not . doesDirectoryExist) matches

    case files of
        [] ->
            error $
                "readParquetFiles: no parquet files found for " ++ path
        _ -> do
            let optsWithoutRowRange = opts{rowRange = Nothing}
            dfs <- mapM (readParquetWithOpts optsWithoutRowRange) files
            pure (applyRowRange opts (mconcat dfs))

-- | Parse a Parquet file via the 'RandomAccess' handle.
parseParquetWithOpts ::
    (RandomAccess m, MonadIO m) =>
    ParquetReadOptions ->
    m DataFrame
{-# SPECIALIZE parseParquetWithOpts ::
    ParquetReadOptions -> ReaderIO FileBufferedOrSeekable DataFrame
    #-}
{-# INLINEABLE parseParquetWithOpts #-}
parseParquetWithOpts opts = do
    metadata <- parseFileMetadata
    let plan = planColumns opts metadata
        vectorLength = fileRowCount metadata
    rawCols <-
        zipWithM (parseColumnChunks vectorLength) (planChunks plan) (planDescs plan)
    return (assembleDataFrame opts vectorLength metadata plan rawCols)

-- | Row count from the footer, falling back to the sum of row-group counts.
fileRowCount :: FileMetadata -> Int
fileRowCount metadata =
    let topLevelRows = fromIntegral . unField $ metadata.num_rows :: Int
        rgRows =
            sum $ map (fromIntegral . unField . rg_num_rows) (unField metadata.row_groups) ::
                Int
     in if topLevelRows > 0 then topLevelRows else rgRows

columnMatches :: [T.Text] -> T.Text -> T.Text -> Bool
columnMatches allNames requested name =
    name == requested
        || (leafOf name == requested && requested `notElem` allNames)

-- | The last dot-separated segment of a column path.
leafOf :: T.Text -> T.Text
leafOf = last . T.splitOn "."

-- | Validate the requested columns against the schema and pick what to decode.
planColumns :: ParquetReadOptions -> FileMetadata -> ColumnPlan
planColumns opts metadata =
    let schemaElems = unField metadata.schema
        allNames = getColumnNames (drop 1 schemaElems)
        knownNames = L.nub (allNames ++ map leafOf allNames)
        predicateColumns = maybe [] (L.nub . getColumns) (predicate opts)
        selectedColumnsForRead = case selectedColumns opts of
            Nothing -> Nothing
            Just selected -> Just (L.nub (selected ++ predicateColumns))

        -- TODO: With rowRange set, skip row groups the range does not overlap.

        -- TODO: Skip row groups whose statistics rule out the predicate.

        -- Validate selected columns
        validated = case selectedColumnsForRead of
            Nothing -> ()
            Just requested ->
                let missing = [r | r <- requested, not (any (columnMatches allNames r) allNames)]
                 in if L.null missing
                        then ()
                        else
                            throw
                                ( ColumnsNotFoundException
                                    missing
                                    "readParquetWithOpts"
                                    knownNames
                                )

        descriptions = generateColumnDescriptions schemaElems
        chunks = columnChunksForAll metadata
        nCols = length chunks
        nDescs = length descriptions

        counted =
            if nCols == nDescs
                then ()
                else
                    error $
                        "Column count mismatch: got "
                            <> show nCols
                            <> " columns but schema implied "
                            <> show nDescs
                            <> " columns"

        -- Decode only the columns needed for the output plus any predicate.
        keep name = case selectedColumnsForRead of
            Nothing -> True
            Just req -> any (\r -> columnMatches allNames r name) req
        kept = filter (\(n, _, _) -> keep n) (zip3 allNames chunks descriptions)
     in validated `seq`
            counted `seq`
                ColumnPlan
                    { planNames = [n | (n, _, _) <- kept]
                    , planDescs = [d | (_, _, d) <- kept]
                    , planChunks = [c | (_, c, _) <- kept]
                    }

-- | Apply logical types and read options to the decoded columns.
assembleDataFrame ::
    ParquetReadOptions -> Int -> FileMetadata -> ColumnPlan -> [Column] -> DataFrame
assembleDataFrame opts totalRows metadata plan rawCols =
    let hints = nativeTypeHints metadata
        keptNames = planNames plan
        widen col
            | not (widenIntegers opts) = col
            | otherwise = case col of
                DI.UnboxedColumn Nothing (v :: VU.Vector t)
                    | Just Refl <- testEquality (typeRep @t) (typeRep @Int32) ->
                        DI.UnboxedColumn Nothing (VU.map (fromIntegral @Int32 @Int) v)
                    | Just Refl <- testEquality (typeRep @t) (typeRep @Int64) ->
                        DI.UnboxedColumn Nothing (VU.map (fromIntegral @Int64 @Int) v)
                _ -> col
        finalCols =
            zipWith
                (restoreNativeType hints)
                keptNames
                (map widen (zipWith applyDescLogicalType (planDescs plan) rawCols))
        indices = Map.fromList $ zip keptNames [0 ..]
        dimensions = (totalRows, length finalCols)
        df =
            DataFrame
                (Vector.fromListN (length finalCols) finalCols)
                indices
                dimensions
                Map.empty
     in applyReadOptions opts df

-- | Parse the file-level Thrift metadata from the Parquet footer.
parseFileMetadata :: (RandomAccess m) => m FileMetadata
parseFileMetadata = do
    footerBytes <- readSuffix 8
    let magic = BS.drop 4 footerBytes
    when (magic /= "PAR1") $
        error
            ( "Not a valid Parquet file: expected magic bytes \"PAR1\", got "
                ++ show magic
            )
    let size = getMetadataSize footerBytes
    rawMetadata <- readSuffix (size + 8) <&> BS.take size
    case Pinch.decode Pinch.compactProtocol rawMetadata of
        Left e -> error $ "Failed to parse Parquet metadata: " ++ show e
        Right metadata -> return metadata
  where
    getMetadataSize footer =
        let sizes :: [Int]
            sizes = map (fromIntegral . BS.index footer) [0 .. 3]
         in foldl' (.|.) 0 $ zipWith shiftL sizes [0, 8 .. 24]

-- | Read the file metadata from a Parquet file at the given path.
readMetadataFromPath :: FilePath -> IO FileMetadata
readMetadataFromPath path =
    withFileBufferedOrSeekable Nothing path ReadMode $
        runReaderIO parseFileMetadata

-- | Read only the file metadata from an open 'FileBufferedOrSeekable' handle.
readMetadataFromHandle :: FileBufferedOrSeekable -> IO FileMetadata
readMetadataFromHandle = runReaderIO parseFileMetadata

-- | Collect column chunks per column (transposed across all row groups).
columnChunksForAll :: FileMetadata -> [[ColumnChunk]]
columnChunksForAll =
    transpose . map (unField . rg_columns) . unField . row_groups

-- | Dispatch a column's chunks to the correct decoder path.
parseColumnChunks ::
    (RandomAccess m, MonadIO m) =>
    Int ->
    [ColumnChunk] ->
    ColumnDescription ->
    m Column
{-# SPECIALIZE parseColumnChunks ::
    Int ->
    [ColumnChunk] ->
    ColumnDescription ->
    ReaderIO FileBufferedOrSeekable Column
    #-}
{-# SPECIALIZE parseColumnChunks ::
    Int ->
    [ColumnChunk] ->
    ColumnDescription ->
    ReaderIO ChunkBuffer Column
    #-}
{-# INLINEABLE parseColumnChunks #-}
parseColumnChunks totalRows chunks description
    | description.maxRepetitionLevel == 0 && description.maxDefinitionLevel == 0 =
        getNonNullableColumn totalRows description chunks
    | description.maxRepetitionLevel == 0 =
        getNullableColumn totalRows description chunks
    | otherwise =
        getRepeatedColumn description chunks

-- | Decode a required (non-nullable, non-repeated) column.
{-# INLINEABLE getNonNullableColumn #-}
getNonNullableColumn ::
    forall m.
    (RandomAccess m, MonadIO m) =>
    Int ->
    ColumnDescription ->
    [ColumnChunk] ->
    m Column
getNonNullableColumn totalRows description chunks =
    case description.colElementType of
        Just (BOOLEAN _) -> unboxedGo boolDecoder
        Just (INT32 _) -> unboxedGo int32Decoder
        Just (INT64 _) -> unboxedGo int64Decoder
        Just (INT96 _) -> go int96Decoder
        Just (FLOAT _) -> unboxedGo floatDecoder
        Just (DOUBLE _) -> unboxedGo doubleDecoder
        Just (BYTE_ARRAY _) -> goPackedText
        Just (FIXED_LEN_BYTE_ARRAY _) -> case description.typeLength of
            Nothing -> error "FIXED_LEN_BYTE_ARRAY requires type_length to be set"
            Just tl -> go (fixedLenByteArrayDecoder (fromIntegral tl))
        Nothing -> error "Column has no Parquet type"
  where
    go ::
        forall a.
        (Columnable a) =>
        PageDecoder a ->
        m Column
    go decoder =
        foldNonNullable totalRows (foldColumnPagesM description decoder chunks)

    -- Decode non-nullable UTF-8 BYTE_ARRAY straight into one 'PackedText'.
    goPackedText :: m Column
    goPackedText = do
        builder <-
            liftIO $ stToIO (newTextBuilder totalRows (textByteHint totalRows chunks))
        _ <-
            foldColumnDataPagesM
                description
                chunks
                ( \() (dict, enc, nPresent, valBytes, _, _) ->
                    liftIO (appendStringPageIO builder dict enc nPresent valBytes)
                )
                ()
        chunk <- liftIO $ stToIO (freezeTextChunk builder)
        pure (mergeTextChunks [chunk])

    unboxedGo ::
        forall a.
        (Columnable a, VU.Unbox a) =>
        UnboxedPageDecoder a ->
        m Column
    unboxedGo decoder =
        foldNonNullableUnboxed totalRows (foldColumnPagesM description decoder chunks)

-- | Initial byte capacity for a text column.
textByteHint :: Int -> [ColumnChunk] -> Int
textByteHint totalRows chunks =
    max (totalRows * 8) $
        sum
            [ fromIntegral (unField (cmd_total_uncompressed_size meta))
            | cc <- chunks
            , Just meta <- [unField (cc_meta_data cc)]
            ]

-- | Decode an optional (nullable) column.
{-# INLINEABLE getNullableColumn #-}
getNullableColumn ::
    forall m.
    (RandomAccess m, MonadIO m) =>
    Int ->
    ColumnDescription ->
    [ColumnChunk] ->
    m Column
getNullableColumn totalRows description chunks =
    case description.colElementType of
        Just (BOOLEAN _) -> unboxedGo boolDecoder
        Just (INT32 _) -> unboxedGo int32Decoder
        Just (INT64 _) -> unboxedGo int64Decoder
        Just (INT96 _) -> go int96Decoder
        Just (FLOAT _) -> unboxedGo floatDecoder
        Just (DOUBLE _) -> unboxedGo doubleDecoder
        Just (BYTE_ARRAY _) -> goPackedTextNullable
        Just (FIXED_LEN_BYTE_ARRAY _) -> case description.typeLength of
            Nothing -> error "FIXED_LEN_BYTE_ARRAY requires type_length to be set"
            Just tl -> go (fixedLenByteArrayDecoder (fromIntegral tl))
        Nothing -> error "Column has no Parquet type"
  where
    maxDef :: Int
    maxDef = fromIntegral description.maxDefinitionLevel

    go ::
        forall a.
        (Columnable a) =>
        PageDecoder a ->
        m Column
    go decoder =
        foldNullable maxDef totalRows (foldColumnPagesM description decoder chunks)

    -- Nullable UTF-8 BYTE_ARRAY: decode straight into a 'PackedText'.
    goPackedTextNullable :: m Column
    goPackedTextNullable = do
        builder <-
            liftIO $ stToIO (newTextBuilder totalRows (textByteHint totalRows chunks))
        _ <-
            foldColumnDataPagesM
                description
                chunks
                ( \() (dict, enc, nPresent, valBytes, defs, _) ->
                    liftIO
                        (appendNullableStringPageIO builder maxDef dict enc nPresent valBytes defs)
                )
                ()
        chunk <- liftIO $ stToIO (freezeTextChunk builder)
        pure (mergeTextChunks [chunk])
    unboxedGo ::
        forall a.
        (Columnable a, VU.Unbox a) =>
        UnboxedPageDecoder a ->
        m Column
    unboxedGo decoder =
        foldNullableUnboxed
            maxDef
            totalRows
            (foldColumnPagesM description decoder chunks)

-- | Decode a repeated (list/nested) column.
{-# INLINEABLE getRepeatedColumn #-}
getRepeatedColumn ::
    forall m.
    (RandomAccess m, MonadIO m) =>
    ColumnDescription ->
    [ColumnChunk] ->
    m Column
getRepeatedColumn description chunks =
    case description.colElementType of
        Just (BOOLEAN _) -> unboxedGo boolDecoder
        Just (INT32 _) -> unboxedGo int32Decoder
        Just (INT64 _) -> unboxedGo int64Decoder
        Just (INT96 _) -> go int96Decoder
        Just (FLOAT _) -> unboxedGo floatDecoder
        Just (DOUBLE _) -> unboxedGo doubleDecoder
        Just (BYTE_ARRAY _) -> go byteArrayDecoder
        Just (FIXED_LEN_BYTE_ARRAY _) -> case description.typeLength of
            Nothing -> error "FIXED_LEN_BYTE_ARRAY requires type_length to be set"
            Just tl -> go (fixedLenByteArrayDecoder (fromIntegral tl))
        Nothing -> error "Column has no Parquet type"
  where
    maxRep :: Int
    maxRep = fromIntegral description.maxRepetitionLevel
    maxDef :: Int
    maxDef = fromIntegral description.maxDefinitionLevel

    go ::
        forall a.
        ( Columnable a
        , Columnable (Maybe [Maybe a])
        , Columnable (Maybe [Maybe [Maybe a]])
        , Columnable (Maybe [Maybe [Maybe [Maybe a]]])
        ) =>
        PageDecoder a ->
        m Column
    go decoder =
        foldRepeated maxRep maxDef (foldColumnPagesM description decoder chunks)

    unboxedGo ::
        forall a.
        ( VU.Unbox a
        , Columnable a
        , Columnable (Maybe [Maybe a])
        , Columnable (Maybe [Maybe [Maybe a]])
        , Columnable (Maybe [Maybe [Maybe [Maybe a]]])
        ) =>
        UnboxedPageDecoder a ->
        m Column
    unboxedGo decoder =
        foldRepeatedUnboxed maxRep maxDef (foldColumnPagesM description decoder chunks)

applyRowRange :: ParquetReadOptions -> DataFrame -> DataFrame
applyRowRange opts df =
    maybe df (`DS.range` df) (rowRange opts)

applySelectedColumns :: ParquetReadOptions -> DataFrame -> DataFrame
applySelectedColumns opts df =
    maybe df (`DS.select` df) (selectedColumns opts)

applyPredicate :: ParquetReadOptions -> DataFrame -> DataFrame
applyPredicate opts df =
    maybe df (`DS.filter` df) (predicate opts)

applySafeRead :: ParquetReadOptions -> DataFrame -> DataFrame
applySafeRead opts df
    | safeColumns opts = df{columns = Vector.map DI.ensureOptional (columns df)}
    | otherwise = df

applyReadOptions :: ParquetReadOptions -> DataFrame -> DataFrame
applyReadOptions opts =
    applySafeRead opts
        . applyRowRange opts
        . applySelectedColumns opts
        . applyPredicate opts

-- | Apply a column's logical type annotation to its decoded values.
applyDescLogicalType :: ColumnDescription -> DI.Column -> DI.Column
applyDescLogicalType desc = applyLogicalType (colLogicalType desc)

applyLogicalType :: Maybe LogicalType -> DI.Column -> DI.Column
applyLogicalType (Just (LT_TIMESTAMP f)) col =
    let ts = unField f
        unit = unField ts.timestamp_unit
        -- Convert at native precision (ticks per second, ps per tick).
        conv = case unit of
            MILLIS _ -> epochToUTCTime 1_000 1_000_000_000
            MICROS _ -> epochToUTCTime 1_000_000 1_000_000
            NANOS _ -> epochToUTCTime 1_000_000_000 1_000
     in fromRight col $ DI.mapColumn conv col
applyLogicalType (Just (LT_DECIMAL f)) col =
    let dt = unField f
        scale = unField dt.decimal_scale
        precision = unField dt.decimal_precision
     in if precision <= 9
            then
                fromRight col $
                    DI.mapColumn (\raw -> fromIntegral @Int32 @Double raw / 10 ^ scale) col
            else
                if precision <= 18
                    then
                        fromRight col $
                            DI.mapColumn (\raw -> fromIntegral @Int64 @Double raw / 10 ^ scale) col
                    else col
applyLogicalType _ col = col

nativeTypeHints :: FileMetadata -> Schema
nativeTypeHints metadata =
    makeSchema
        [ (name, ty)
        | kv <- concat (unField metadata.key_value_metadata)
        , Just name <- [T.stripPrefix nativeTypeKeyPrefix (unField kv.kv_key)]
        , Just value <- [unField kv.kv_value]
        , Just ty <- [Map.lookup value stampedSchemaTypes]
        ]

stampedSchemaTypes :: Map.Map T.Text SchemaType
stampedSchemaTypes =
    Map.fromList $
        concat
            [ entry @Int "Int"
            , entry @Int32 "Int32"
            , entry @Int64 "Int64"
            , entry @Integer "Integer"
            , entry @Float "Float"
            , entry @Double "Double"
            , entry @Bool "Bool"
            , entry @T.Text "Text"
            , entry @UTCTime "UTCTime"
            ]
  where
    entry ::
        forall a.
        (Columnable a, Read a, Columnable (Maybe a)) =>
        T.Text ->
        [(T.Text, SchemaType)]
    entry name =
        [ (name, schemaType @a)
        , ("Maybe " <> name, schemaType @(Maybe a))
        ]

restoreNativeType :: Schema -> T.Text -> DI.Column -> DI.Column
restoreNativeType hints name col = case lookupHint of
    Just ty
        | stampedAs @Int ty -> narrow (fromIntegral @Int64 @Int)
        | stampedAs @Integer ty -> narrow (fromIntegral @Int64 @Integer)
    _ -> col
  where
    lookupHint =
        case Map.lookup name (elements hints) of
            Just ty -> Just ty
            Nothing -> Map.lookup (leafOf name) (elements hints)
    stampedAs ::
        forall a. (Columnable a, Read a, Columnable (Maybe a)) => SchemaType -> Bool
    stampedAs ty = ty == schemaType @a || ty == schemaType @(Maybe a)
    narrow :: (Columnable a) => (Int64 -> a) -> DI.Column
    narrow f = fromRight col (DI.mapColumn f col)

-- | Convert an epoch timestamp with the given tick rate to 'UTCTime'.
epochToUTCTime :: Int64 -> Integer -> Int64 -> UTCTime
epochToUTCTime ticksPerSecond psPerTick v =
    let (s, subTicks) = v `divMod` ticksPerSecond
        (days, sInDay) = s `divMod` 86_400
        ps = fromIntegral sInDay * 1_000_000_000_000 + fromIntegral subTicks * psPerTick
     in UTCTime
            (ModifiedJulianDay (40_587 + fromIntegral days))
            (picosecondsToDiffTime ps)
