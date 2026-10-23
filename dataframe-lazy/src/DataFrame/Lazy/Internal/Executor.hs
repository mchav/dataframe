{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}

-- | Pull-based execution engine where each operator returns a 'Stream'.
module DataFrame.Lazy.Internal.Executor (
    CsvReader,
    execute,
    foldBatches,
) where

import Control.Concurrent (forkFinally, forkIO, getNumCapabilities)
import Control.Concurrent.Async (mapConcurrently)
import Control.Concurrent.STM (atomically)
import Control.Concurrent.STM.TBQueue (newTBQueueIO, readTBQueue, writeTBQueue)
import Control.Exception (evaluate, finally, throwIO)
import Control.Monad (filterM, forM, forM_, unless, when)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as C8
import qualified Data.ByteString.Unsafe as BSU
import Data.IORef
import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Maybe
import qualified Data.Set as S
import qualified Data.Text as T
import Data.Type.Equality (TestEquality (testEquality), type (:~:) (Refl))
import qualified Data.Vector.Unboxed as VU
import Data.Word (Word8)
import DataFrame.IO.CSV (
    CsvBytesReader,
    CsvReader,
    ReadOptions (..),
    schemaReadOptions,
 )
import qualified DataFrame.IO.Parquet as Parquet
import DataFrame.IO.Parquet.Thrift (unField)
import qualified DataFrame.IO.Parquet.Thrift as Thrift
import qualified DataFrame.Internal.Column as C
import qualified DataFrame.Internal.DataFrame as D
import qualified DataFrame.Internal.Expression as E
import qualified DataFrame.Lazy.IO.Binary as Bin
import DataFrame.Lazy.Internal.LogicalPlan (
    BatchFn (..),
    BatchFn2 (..),
    DataSource (..),
 )
import DataFrame.Lazy.Internal.PhysicalPlan
import qualified DataFrame.Operations.Aggregation as Agg
import qualified DataFrame.Operations.Core as Core
import qualified DataFrame.Operations.Join as Join
import DataFrame.Operations.Merge ()
import qualified DataFrame.Operations.Permutation as Perm
import qualified DataFrame.Operations.Subset as Sub
import qualified DataFrame.Operations.Transformations as Trans
import DataFrame.Schema (elements)
import System.Directory (doesDirectoryExist, removeFile)
import System.FilePath ((</>))
import System.FilePath.Glob (glob)
import System.IO (IOMode (ReadMode), withFile)
import System.IO.Temp (emptySystemTempFile)
import Type.Reflection (typeRep)

-- | A pull-based stream whose action yields the next batch or 'Nothing'.
newtype Stream = Stream {pullBatch :: IO (Maybe D.DataFrame)}

-- | Wrap a computed 'DataFrame' as a single-batch stream.
materialized :: D.DataFrame -> IO Stream
materialized df = do
    ref <- newIORef (Just df)
    return . Stream $ do
        mb <- readIORef ref
        writeIORef ref Nothing
        return mb

-- | Apply a function to every batch of a stream.
mapStream :: (D.DataFrame -> D.DataFrame) -> Stream -> Stream
mapStream f s = Stream (fmap f <$> pullBatch s)

-- | Drain a stream and concatenate its batches into one DataFrame.
collectStream :: Stream -> IO D.DataFrame
collectStream stream = go []
  where
    go acc = do
        mb <- pullBatch stream
        case mb of
            Nothing -> return (concatBatches (reverse acc))
            Just df -> go (df : acc)

-- | Concatenate a list of same-schema batches column-by-column in one pass.
concatBatches :: [D.DataFrame] -> D.DataFrame
concatBatches [] = D.empty
concatBatches [df] = df
concatBatches batches@(first : _) =
    D.fromColumns
        [ (name, C.concatManyColumns [D.unsafeGetColumn name b | b <- batches])
        | name <- D.columnNames first
        ]

-- | True when every scan leaf is a finite local source.
isBounded :: PhysicalPlan -> Bool
isBounded (PhysicalScan (CsvSource{}) _) = True
isBounded (PhysicalScan (CsvSourceStreaming{}) _) = False
isBounded (PhysicalScan (CsvSourceStreamingBytes{}) _) = False
isBounded (PhysicalScan (ParquetSource _) _) = True
isBounded (PhysicalProject _ c) = isBounded c
isBounded (PhysicalFilter _ c) = isBounded c
isBounded (PhysicalDerive _ _ c) = isBounded c
isBounded (PhysicalLimit _ c) = isBounded c
isBounded (PhysicalSort _ c) = isBounded c
isBounded (PhysicalHashAggregate _ _ c) = isBounded c
isBounded (PhysicalSpill c _) = isBounded c
isBounded (PhysicalSourceDF _ _) = True
isBounded (PhysicalRename _ c) = isBounded c
isBounded (PhysicalOffset _ c) = isBounded c
isBounded (PhysicalMapBatch _ _ c) = isBounded c
isBounded (PhysicalBarrier _ _ c) = isBounded c
isBounded (PhysicalBarrier2 _ _ l r) = isBounded l && isBounded r
isBounded (PhysicalHashJoin _ _ l r) = isBounded l && isBounded r
isBounded (PhysicalSortMergeJoin _ _ l r) = isBounded l && isBounded r

-- | Execute a physical plan into a single complete 'DataFrame'.
execute :: PhysicalPlan -> IO D.DataFrame
execute plan = buildStream plan >>= collectStream

-- | Strictly fold a function over every batch a physical plan produces.
foldBatches ::
    (b -> D.DataFrame -> IO b) -> b -> PhysicalPlan -> IO b
foldBatches f seed plan = do
    stream <- buildStream plan
    let loop !acc = do
            mb <- pullBatch stream
            case mb of
                Nothing -> return acc
                Just batch -> do
                    !acc' <- f acc batch
                    loop acc'
    loop seed

buildStream :: PhysicalPlan -> IO Stream
buildStream (PhysicalScan (CsvSource path sep reader) cfg) =
    executeCsvScan path sep reader cfg
buildStream (PhysicalScan (CsvSourceStreaming path sep reader) cfg) =
    executeCsvScanStreaming path sep reader cfg
buildStream (PhysicalScan (CsvSourceStreamingBytes path sep reader) cfg) =
    executeCsvScanStreamingBytes path sep reader cfg
buildStream (PhysicalScan (ParquetSource path) cfg) =
    executeParquetScan path cfg
buildStream (PhysicalSpill child path) = do
    df <- execute child
    Bin.spillToDisk path df
    df' <- Bin.readSpilled path
    materialized df'
buildStream (PhysicalFilter p child) = do
    childStream <- buildStream child
    return . Stream $
        ( do
            mb <- pullBatch childStream
            return $ fmap (Sub.filter p) mb
        )
buildStream (PhysicalProject cols child) = do
    childStream <- buildStream child
    return . Stream $
        ( do
            mb <- pullBatch childStream
            return $ fmap (Sub.select cols) mb
        )
buildStream (PhysicalDerive name uexpr child) = do
    childStream <- buildStream child
    return . Stream $
        ( do
            mb <- pullBatch childStream
            return $ fmap (Trans.deriveMany [(name, uexpr)]) mb
        )
buildStream (PhysicalRename pairs child) =
    mapStream (Core.renameMany pairs) <$> buildStream child
buildStream (PhysicalMapBatch _ (BatchFn f) child) =
    mapStream f <$> buildStream child
buildStream (PhysicalBarrier _ (BatchFn f) child) =
    execute child >>= materialized . f
buildStream (PhysicalBarrier2 _ (BatchFn2 f) l r) = do
    leftDf <- execute l
    rightDf <- execute r
    materialized (f leftDf rightDf)
buildStream (PhysicalOffset n child) = do
    childStream <- buildStream child
    skippedRef <- newIORef (0 :: Int)
    let next = do
            mb <- pullBatch childStream
            case mb of
                Nothing -> return Nothing
                Just df -> do
                    skipped <- readIORef skippedRef
                    let rows = Core.nRows df
                        toSkip = min rows (n - skipped)
                    writeIORef skippedRef (skipped + toSkip)
                    if toSkip == rows && rows > 0
                        then next
                        else return (Just (Sub.drop toSkip df))
    return (Stream next)
buildStream (PhysicalLimit n child) = do
    childStream <- buildStream child
    countRef <- newIORef (0 :: Int)
    return . Stream $
        ( do
            remaining <- readIORef countRef
            if remaining >= n
                then return Nothing
                else do
                    mb <- pullBatch childStream
                    case mb of
                        Nothing -> return Nothing
                        Just df -> do
                            let toTake = min (Core.nRows df) (n - remaining)
                            modifyIORef' countRef (+ toTake)
                            return $ Just (Sub.take toTake df)
        )
buildStream (PhysicalSort ords child) = do
    df <- execute child
    materialized (Perm.sortBy ords df)
buildStream (PhysicalHashAggregate keys aggs child)
    | isBounded child = do
        df <- execute child
        let result = Agg.aggregate aggs (Agg.groupBy keys df)
        materialized result
buildStream (PhysicalHashAggregate keys aggs child) = do
    childStream <- buildStream child
    if all (isStreamableAgg . snd) aggs
        then do
            let (partialAggs, mergeAggs, finalizer) = buildAggPlan aggs
            nCaps <- getNumCapabilities
            let workers = max 1 nCaps
            partials <-
                mapConcurrently
                    (\_ -> workerLoop childStream keys partialAggs mergeAggs)
                    [1 .. workers]
            mFinal <-
                let nonEmpty = Data.Maybe.catMaybes partials
                 in case nonEmpty of
                        [] -> return Nothing
                        [single] -> return (Just (finalizer single))
                        (a : rest) -> do
                            !merged <- mergePartials keys mergeAggs a rest
                            return (Just (finalizer merged))
            ref <- newIORef mFinal
            return . Stream $ do
                mb <- readIORef ref
                writeIORef ref Nothing
                return mb
        else do
            df <- collectStream childStream
            materialized (Agg.aggregate aggs (Agg.groupBy keys df))
buildStream (PhysicalSourceDF bs df) = do
    let total = Core.nRows df
    posRef <- newIORef (0 :: Int)
    return . Stream $ do
        i <- readIORef posRef
        if i >= total
            then return Nothing
            else do
                let n = min bs (total - i)
                    batch = Sub.range (i, i + n) df
                writeIORef posRef (i + n)
                return (Just batch)
buildStream (PhysicalHashJoin jt keyPairs leftPlan rightPlan)
    | isBounded leftPlan = do
        leftDf <- execute leftPlan
        rightDf <- execute rightPlan
        materialized (performJoin jt keyPairs leftDf rightDf)
buildStream (PhysicalHashJoin jt keyPairs leftPlan rightPlan) =
    case jt of
        Join.INNER -> streamingHashJoin assembleInnerBatch
        Join.LEFT -> streamingHashJoin assembleLeftBatch
        _ -> do
            leftDf <- execute leftPlan
            rightDf <- execute rightPlan
            materialized (performJoin jt keyPairs leftDf rightDf)
  where
    streamingHashJoin assembleFn = do
        rightDf <- execute rightPlan
        let rightDf' = renameRightKeys keyPairs rightDf
            joinKeys = map fst keyPairs
            csSet = S.fromList joinKeys
            rightHashes = Join.buildHashColumn joinKeys rightDf'
            ci = Join.buildCompactIndex rightHashes
        leftStream <- buildStream leftPlan
        return . Stream $ do
            mBatch <- pullBatch leftStream
            case mBatch of
                Nothing -> return Nothing
                Just probeBatch -> do
                    let probeHashes = Join.buildHashColumn joinKeys probeBatch
                        (probeIxs, buildIxs) = Join.hashProbeKernel ci probeHashes
                    return . Just $ assembleFn csSet probeBatch rightDf' probeIxs buildIxs

    assembleLeftBatch csSet probeBatch rightDf' probeIxs buildIxs =
        let batchN = Core.nRows probeBatch
            matched =
                VU.accumulate
                    (\_ b -> b)
                    (VU.replicate batchN False)
                    (VU.map (,True) probeIxs)
            unmatchedIxs = VU.findIndices not matched
            allProbeIxs = probeIxs VU.++ unmatchedIxs
            allBuildIxs = buildIxs VU.++ VU.replicate (VU.length unmatchedIxs) (-1)
         in Join.assembleLeft csSet probeBatch rightDf' allProbeIxs allBuildIxs

    assembleInnerBatch = Join.assembleInner
buildStream (PhysicalSortMergeJoin jt keyPairs leftPlan rightPlan) = do
    leftDf <- execute leftPlan
    rightDf <- execute rightPlan
    materialized (performJoin jt keyPairs leftDf rightDf)

-- | One worker's loop: pull shared batches into a per-worker accumulator.
workerLoop ::
    Stream ->
    [T.Text] ->
    [E.NamedExpr] ->
    [E.NamedExpr] ->
    IO (Maybe D.DataFrame)
workerLoop childStream keys partialAggs mergeAggs = loop Nothing
  where
    loop !acc = do
        mb <- pullBatch childStream
        case mb of
            Nothing -> return acc
            Just batch -> do
                !partial <-
                    evaluate . D.forceDataFrame $
                        Agg.aggregate partialAggs (Agg.groupBy keys batch)
                !next <- case acc of
                    Nothing -> return (Just partial)
                    Just a -> do
                        !merged <-
                            evaluate . D.forceDataFrame $
                                Agg.aggregate mergeAggs (Agg.groupBy keys (a <> partial))
                        return (Just merged)
                loop next

-- | Merge a head accumulator with the rest of the workers' partials.
mergePartials ::
    [T.Text] ->
    [E.NamedExpr] ->
    D.DataFrame ->
    [D.DataFrame] ->
    IO D.DataFrame
mergePartials keys mergeAggs = go
  where
    go !acc [] = return acc
    go !acc (p : ps) = do
        !merged <-
            evaluate . D.forceDataFrame $
                Agg.aggregate mergeAggs (Agg.groupBy keys (acc <> p))
        go merged ps

isStreamableAgg :: E.SomeExpr -> Bool
isStreamableAgg (E.SomeExpr (E.Agg (E.CollectAgg _ _) _)) = False
isStreamableAgg (E.SomeExpr (E.Agg (E.FoldAgg _ Nothing (_ :: a -> b -> a)) _)) =
    case testEquality (typeRep @a) (typeRep @b) of
        Just Refl -> True
        Nothing -> False
isStreamableAgg (E.SomeExpr (E.Agg (E.FoldAgg _ (Just _) (_ :: a -> b -> a)) _)) =
    case testEquality (typeRep @a) (typeRep @Int) of
        Just Refl -> True
        Nothing ->
            case testEquality (typeRep @a) (typeRep @b) of
                Just Refl -> True
                Nothing -> False
isStreamableAgg (E.SomeExpr (E.Agg (E.MergeAgg{}) _)) = True
isStreamableAgg _ = False

-- | Build the (partial, merge, finalizer) plan for streamable aggregates.
buildAggPlan ::
    [(T.Text, E.SomeExpr)] ->
    ( [(T.Text, E.SomeExpr)]
    , [(T.Text, E.SomeExpr)]
    , D.DataFrame -> D.DataFrame
    )
buildAggPlan aggs = foldl combine ([], [], id) (map processAgg aggs)
  where
    combine (p1, m1, f1) (p2, m2, f2) = (p1 ++ p2, m1 ++ m2, f1 . f2)

    -- Partial and merge steps are new operators, never the builtin itself.
    phase :: T.Text -> E.OpRef E.AggId -> E.OpRef E.AggId
    phase p ident = case ident of
        E.Builtin a -> E.Named (p <> E.aggName a)
        E.Named n -> E.Named (p <> n)
        E.Anonymous l -> E.Anonymous (p <> l)

    processAgg ::
        (T.Text, E.SomeExpr) ->
        ([(T.Text, E.SomeExpr)], [(T.Text, E.SomeExpr)], D.DataFrame -> D.DataFrame)
    processAgg (name, ue) = case ue of
        E.SomeExpr (E.Agg (E.FoldAgg n Nothing (f :: a -> b -> a)) (_ :: E.Expr b)) ->
            case testEquality (typeRep @a) (typeRep @b) of
                Just Refl ->
                    ( [(name, ue)]
                    , [(name, E.SomeExpr (E.Agg (E.FoldAgg n Nothing f) (E.Col @a name)))]
                    , id
                    )
                Nothing ->
                    case testEquality (typeRep @a) (typeRep @Int) of
                        Just Refl ->
                            ( [(name, ue)]
                            ,
                                [
                                    ( name
                                    , E.SomeExpr
                                        ( E.Agg
                                            (E.FoldAgg (E.Builtin E.ASum) Nothing ((+) :: Int -> Int -> Int))
                                            (E.Col @Int name)
                                        )
                                    )
                                ]
                            , id
                            )
                        Nothing -> ([(name, ue)], [(name, ue)], id)
        E.SomeExpr (E.Agg (E.FoldAgg n (Just _) (f :: a -> b -> a)) (_ :: E.Expr b)) ->
            case testEquality (typeRep @a) (typeRep @Int) of
                Just Refl ->
                    ( [(name, ue)]
                    ,
                        [
                            ( name
                            , E.SomeExpr
                                ( E.Agg
                                    (E.FoldAgg (E.Builtin E.ASum) Nothing ((+) :: Int -> Int -> Int))
                                    (E.Col @Int name)
                                )
                            )
                        ]
                    , id
                    )
                Nothing ->
                    case testEquality (typeRep @a) (typeRep @b) of
                        Just Refl ->
                            ( [(name, ue)]
                            , [(name, E.SomeExpr (E.Agg (E.FoldAgg n Nothing f) (E.Col @a name)))]
                            , id
                            )
                        Nothing -> ([(name, ue)], [(name, ue)], id)
        E.SomeExpr
            ( E.Agg
                    ( E.MergeAgg
                            n
                            seed
                            (step :: acc -> b -> acc)
                            (merge :: acc -> acc -> acc)
                            (fin :: acc -> a)
                        )
                    (inner :: E.Expr b)
                ) ->
                let partialExpr =
                        E.SomeExpr
                            ( E.Agg
                                (E.FoldAgg (phase "partial_" n) (Just seed) step)
                                inner
                            )
                    mergeExpr =
                        E.SomeExpr
                            ( E.Agg
                                (E.FoldAgg (phase "merge_" n) Nothing merge)
                                (E.Col @acc name)
                            )
                    finalize df =
                        let accCol = D.unsafeGetColumn name df
                            finalCol =
                                either
                                    (error "buildAggPlan: MergeAgg finalize failed")
                                    id
                                    (C.mapColumn @acc @a fin accCol)
                         in D.insertColumn name finalCol df
                 in ( [(name, partialExpr)]
                    , [(name, mergeExpr)]
                    , finalize
                    )
        _ -> ([(name, ue)], [(name, ue)], id)

-- | Scan a Parquet file, directory or glob; each file becomes one batch.
executeParquetScan :: FilePath -> ScanConfig -> IO Stream
executeParquetScan path cfg = do
    isDir <- doesDirectoryExist path
    let pat = if isDir then path </> "*" else path
    matches <- glob pat
    files <- filterM (fmap not . doesDirectoryExist) matches
    when (null files) $
        error ("executeParquetScan: no parquet files found for " ++ path)
    let opts =
            Parquet.defaultParquetReadOptions
                { Parquet.selectedColumns = Just (M.keys (elements (scanSchema cfg)))
                , Parquet.predicate = scanPushdownPredicate cfg
                }
    ref <- newIORef files
    return . Stream $ do
        fs <- readIORef ref
        case fs of
            [] -> return Nothing
            (f : rest) -> do
                writeIORef ref rest
                -- One footer parse orders the selection and feeds the read.
                metadata <- Parquet.readMetadataFromPath f
                let names = Parquet.getColumnNames (drop 1 (unField (Thrift.schema metadata)))
                    opts' =
                        opts{Parquet.selectedColumns = fileOrder names <$> Parquet.selectedColumns opts}
                Just <$> Parquet.readParquetWithMetadata Nothing metadata opts' f

-- | Read options for a CSV scan: schema gives types, source the separator.
scanReadOptions :: Char -> ScanConfig -> ReadOptions
scanReadOptions sep cfg =
    (schemaReadOptions (scanSchema cfg)){columnSeparator = sep}

-- | Parallel CSV scan splitting the file at newlines into slices.
executeCsvScan :: FilePath -> Char -> CsvReader -> ScanConfig -> IO Stream
executeCsvScan path sep reader cfg = do
    nCaps <- getNumCapabilities
    header <- withFile path ReadMode C8.hGetLine
    chunkPaths <- splitCsvAtNewlines (max 1 nCaps) path

    let opts = inFileOrder (headerNames sep header) (scanReadOptions sep cfg)
        batchSz = scanBatchSize cfg
    chunkDfs <- mapConcurrently (reader opts) chunkPaths
    mapM_ removeFile chunkPaths

    queue <- newTBQueueIO (fromIntegral (max 4 (2 * nCaps)))
    _ <- forkIO $ do
        forM_ chunkDfs $ \df ->
            forM_ (sliceIntoBatches batchSz df) $ \b ->
                atomically (writeTBQueue queue (Just b))
        atomically (writeTBQueue queue Nothing)
    return . Stream $
        ( do
            mb <- atomically (readTBQueue queue)
            case mb of
                Nothing -> atomically (writeTBQueue queue Nothing) >> return Nothing
                Just df ->
                    let df' = case scanPushdownPredicate cfg of
                            Nothing -> df
                            Just p -> Sub.filter p df
                     in return (Just df')
        )

-- | Column names in a CSV header line.
headerNames :: Char -> BS.ByteString -> [T.Text]
headerNames sep =
    map (T.dropAround (\c -> c == '"' || c == '\r') . T.pack . C8.unpack)
        . C8.split sep
        . stripBom
  where
    stripBom b = Data.Maybe.fromMaybe b (BS.stripPrefix "\xEF\xBB\xBF" b)

-- | Read the selected columns in file order, like eager reads do.
inFileOrder :: [T.Text] -> ReadOptions -> ReadOptions
inFileOrder names opts = opts{readColumns = fileOrder names <$> readColumns opts}

-- | Order requested names as they appear in a file (matching by leaf name).
fileOrder :: [T.Text] -> [T.Text] -> [T.Text]
fileOrder names requested =
    L.sortOn position requested
  where
    leaf = T.takeWhileEnd (/= '.')
    -- Unknown names trail, so the reader still reports them as missing.
    position r =
        Data.Maybe.fromMaybe
            (length names)
            (L.findIndex (\n -> n == r || leaf n == r) names)

executeCsvScanStreaming ::
    FilePath -> Char -> CsvReader -> ScanConfig -> IO Stream
executeCsvScanStreaming path sep reader =
    executeCsvScanStreamingWith path sep (Left reader)

executeCsvScanStreamingBytes ::
    FilePath -> Char -> CsvBytesReader -> ScanConfig -> IO Stream
executeCsvScanStreamingBytes path sep reader =
    executeCsvScanStreamingWith path sep (Right reader)

executeCsvScanStreamingWith ::
    FilePath ->
    Char ->
    Either CsvReader CsvBytesReader ->
    ScanConfig ->
    IO Stream
executeCsvScanStreamingWith path sep reader cfg = do
    let schemaOpts = scanReadOptions sep cfg
        batchSz = scanBatchSize cfg
        windowBytes = 64 * 1024 * 1024 :: Int
    queue <- newTBQueueIO 8
    _ <-
        forkFinally
            ( withFile path ReadMode $ \h -> do
                rawHeader <- C8.hGetLine h
                let header = stripUtf8Bom rawHeader
                    opts = inFileOrder (headerNames sep header) schemaOpts
                let feed bytes =
                        unless (BS.null bytes) $ do
                            let window = header <> BS.singleton nl <> bytes
                            parsed <- case reader of
                                Right bytesReader -> bytesReader opts window
                                Left pathReader -> do
                                    p <- emptySystemTempFile "lazy_csv_win_.csv"
                                    (BS.writeFile p window >> pathReader opts p)
                                        `finally` removeFile p
                            forM_ (sliceIntoBatches batchSz parsed) $ \b ->
                                atomically (writeTBQueue queue (Right (Just b)))
                    loop leftover = do
                        chunk <- BS.hGet h windowBytes
                        if BS.null chunk
                            then feed leftover
                            else do
                                let buf = leftover <> chunk
                                case lastCompleteRecordNewline buf of
                                    Nothing -> loop buf
                                    Just i -> feed (BS.take i buf) >> loop (BS.drop (i + 1) buf)
                loop BS.empty
            )
            ( \result ->
                atomically $
                    writeTBQueue queue $
                        case result of
                            Left err -> Left err
                            Right () -> Right Nothing
            )
    return . Stream $ do
        item <- atomically (readTBQueue queue)
        case item of
            Left err -> throwIO err
            Right Nothing ->
                atomically (writeTBQueue queue (Right Nothing)) >> return Nothing
            Right (Just df) ->
                let df' = case scanPushdownPredicate cfg of
                        Nothing -> df
                        Just p -> Sub.filter p df
                 in return (Just df')
  where
    nl :: Word8
    nl = 0x0A

    stripUtf8Bom bytes =
        Data.Maybe.fromMaybe bytes (BS.stripPrefix "\xEF\xBB\xBF" bytes)

-- | Find the last record-ending LF in a CSV buffer.
lastCompleteRecordNewline :: BS.ByteString -> Maybe Int
lastCompleteRecordNewline bytes
    | not (BS.elem quote bytes) = BS.elemIndexEnd nl bytes
    | otherwise = go 0 False Nothing
  where
    !len = BS.length bytes

    -- Each read stays inside its bounds-checked branch (no shared guard).
    go !i !inside !lastNewline
        | i >= len = lastNewline
        | otherwise = case BSU.unsafeIndex bytes i of
            current
                | current == quote ->
                    if inside && quoteAt (i + 1)
                        then go (i + 2) True lastNewline
                        else go (i + 1) (not inside) lastNewline
                | current == nl && not inside -> go (i + 1) inside (Just i)
                | otherwise -> go (i + 1) inside lastNewline

    quoteAt !i
        | i >= len = False
        | otherwise = BSU.unsafeIndex bytes i == quote

    quote :: Word8
    quote = 0x22

    nl :: Word8
    nl = 0x0A

-- | Slice a 'DataFrame' into row-bounded batches of at most @n@ rows.
sliceIntoBatches :: Int -> D.DataFrame -> [D.DataFrame]
sliceIntoBatches n df =
    let total = Core.nRows df
        starts = [0, n .. total - 1]
     in [Sub.range (s, min (s + n) total) df | s <- starts]

-- | Split a CSV at newline boundaries into temp files, each with the header.
splitCsvAtNewlines :: Int -> FilePath -> IO [FilePath]
splitCsvAtNewlines n path = do
    bs <- BS.readFile path
    let (header, rest) = BS.break (== nl) bs
        body = BS.drop 1 rest
        bodyLen = BS.length body
        rawOffsets = [(bodyLen * i) `div` n | i <- [0 .. n]]
        snapped = 0 : map (snap body) (init (drop 1 rawOffsets)) ++ [bodyLen]
        ranges = zip snapped (drop 1 snapped)
        slices =
            [ BS.take (hi - lo) (BS.drop lo body)
            | (lo, hi) <- ranges
            , hi > lo
            ]
    forM slices $ \chunk -> do
        p <- emptySystemTempFile "lazy_csv_chunk_.csv"
        BS.writeFile p (header <> BS.singleton nl <> chunk)
        return p
  where
    nl :: Word8
    nl = 0x0A
    snap body off =
        case BS.elemIndex nl (BS.drop off body) of
            Just i -> off + i + 1
            Nothing -> BS.length body

-- | Join as the eager API does, after renaming right keys to left names.
performJoin ::
    Join.JoinType ->
    [(T.Text, T.Text)] ->
    D.DataFrame ->
    D.DataFrame ->
    D.DataFrame
performJoin jt keyPairs leftDf rightDf =
    Join.join jt (map fst keyPairs) leftDf (renameRightKeys keyPairs rightDf)

-- | Give each right key its left counterpart's name.
renameRightKeys :: [(T.Text, T.Text)] -> D.DataFrame -> D.DataFrame
renameRightKeys keyPairs =
    Core.renameMany [(r, l) | (l, r) <- keyPairs, l /= r]
