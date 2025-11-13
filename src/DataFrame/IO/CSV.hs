{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.IO.CSV where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as Builder
import qualified Data.ByteString.Char8 as C8
import qualified Data.List as L
import qualified Data.Map.Strict as M
import qualified Data.Proxy as P
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Text.IO as TIO
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as VM
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Applicative (many, (<|>))
import Control.Monad (forM_, unless, zipWithM, zipWithM_)
import Data.Attoparsec.ByteString.Char8 hiding (endOfLine)
import Data.Bits (shiftL)
import Data.Char
import Data.Either
import Data.Function (on)
import Data.Functor
import Data.IORef
import Data.Maybe
import Data.Type.Equality (TestEquality (testEquality))
import DataFrame.Internal.Column (Column (..), columnLength)
import DataFrame.Internal.DataFrame (DataFrame (..))
import DataFrame.Internal.Parsing
import DataFrame.Internal.Schema
import DataFrame.Operations.Typing
import System.IO
import Type.Reflection
import Prelude hiding (concat, takeWhile)

data GrowingVector a = GrowingVector
    { gvData :: !(IORef (VM.IOVector a))
    , gvSize :: !(IORef Int)
    , gvCapacity :: !(IORef Int)
    }

data GrowingUnboxedVector a = GrowingUnboxedVector
    { guvData :: !(IORef (VUM.IOVector a))
    , guvSize :: !(IORef Int)
    , guvCapacity :: !(IORef Int)
    }

data GrowingColumn
    = GrowingInt !(GrowingUnboxedVector Int) !(IORef [Int])
    | GrowingDouble !(GrowingUnboxedVector Double) !(IORef [Int])
    | GrowingText !(GrowingVector T.Text) !(IORef [Int])

data HeaderSpec
    = -- | File has no header row
      NoHeader
    | -- | Use first row as column names
      UseFirstRow
    | -- | Supply names for a no-header file
      ProvideNames [T.Text]
    deriving (Eq, Show)

data TypeSpec
    = InferFromSample Int
    | SpecifyTypes [SchemaType]
    | NoInference

shouldInferFromSample :: TypeSpec -> Bool
shouldInferFromSample (InferFromSample _) = True
shouldInferFromSample _ = False

typeInferenceSampleSize :: TypeSpec -> Int
typeInferenceSampleSize (InferFromSample n) = n
typeInferenceSampleSize _ = 0

-- | CSV read parameters.
data ReadOptions = ReadOptions
    { headerSpec :: HeaderSpec
    -- ^ Where to get the headers from. (default: UseFirstRow)
    , typeSpec :: TypeSpec
    -- ^ Whether/how to infer types. (default: InferFromSample 100)
    , safeRead :: Bool
    -- ^ Whether to partially parse values into `Maybe`/`Either`. (default: True)
    , chunkSize :: Int
    -- ^ Default chunk size (in bytes) for csv reader. (default: 512'000)
    , dateFormat :: String
    {- ^ Format of date fields as recognized by the Data.Time.Format module.

    __Examples:__

    @
    > parseTimeM True defaultTimeLocale "%Y/%-m/%-d" "2010/3/04" :: Maybe Day
    Just 2010-03-04
    > parseTimeM True defaultTimeLocale "%d/%-m/%-Y" "04/3/2010" :: Maybe Day
    Just 2010-03-04
    @
    -}
    }

defaultReadOptions :: ReadOptions
defaultReadOptions =
    ReadOptions
        { headerSpec = UseFirstRow
        , typeSpec = InferFromSample 100
        , safeRead = True
        , chunkSize = 512_000
        , dateFormat = "%Y-%m-%d"
        }

newGrowingVector :: Int -> IO (GrowingVector a)
newGrowingVector !initCap = do
    vec <- VM.unsafeNew initCap
    GrowingVector <$> newIORef vec <*> newIORef 0 <*> newIORef initCap

newGrowingUnboxedVector :: (VUM.Unbox a) => Int -> IO (GrowingUnboxedVector a)
newGrowingUnboxedVector !initCap = do
    vec <- VUM.unsafeNew initCap
    GrowingUnboxedVector <$> newIORef vec <*> newIORef 0 <*> newIORef initCap

appendGrowingVector :: GrowingVector a -> a -> IO ()
appendGrowingVector (GrowingVector vecRef sizeRef capRef) !val = do
    size <- readIORef sizeRef
    cap <- readIORef capRef
    vec <- readIORef vecRef

    vec' <-
        if size >= cap
            then do
                let !newCap = cap `shiftL` 1
                newVec <- VM.unsafeGrow vec newCap
                writeIORef vecRef newVec
                writeIORef capRef newCap
                return newVec
            else return vec

    VM.unsafeWrite vec' size val
    writeIORef sizeRef $! size + 1

appendGrowingUnboxedVector ::
    (VUM.Unbox a) => GrowingUnboxedVector a -> a -> IO ()
appendGrowingUnboxedVector (GrowingUnboxedVector vecRef sizeRef capRef) !val = do
    size <- readIORef sizeRef
    cap <- readIORef capRef
    vec <- readIORef vecRef

    vec' <-
        if size >= cap
            then do
                let !newCap = cap `shiftL` 1
                newVec <- VUM.unsafeGrow vec newCap
                writeIORef vecRef newVec
                writeIORef capRef newCap
                return newVec
            else return vec

    VUM.unsafeWrite vec' size val
    writeIORef sizeRef $! size + 1

freezeGrowingVector :: GrowingVector a -> IO (V.Vector a)
freezeGrowingVector (GrowingVector vecRef sizeRef _) = do
    vec <- readIORef vecRef
    size <- readIORef sizeRef
    V.freeze (VM.slice 0 size vec)

freezeGrowingUnboxedVector ::
    (VUM.Unbox a) => GrowingUnboxedVector a -> IO (VU.Vector a)
freezeGrowingUnboxedVector (GrowingUnboxedVector vecRef sizeRef _) = do
    vec <- readIORef vecRef
    size <- readIORef sizeRef
    VU.freeze (VUM.slice 0 size vec)

{- | Read CSV file from path and load it into a dataframe.

==== __Example__
@
ghci> D.readCsv ".\/data\/taxi.csv"

@
-}
readCsv :: FilePath -> IO DataFrame
readCsv = readSeparated ',' defaultReadOptions

{- | Read CSV file from path and load it into a dataframe.

==== __Example__
@
ghci> D.readCsvWithOpts ".\/data\/taxi.csv" (D.defaultReadOptions { dateFormat = "%d/%-m/%-Y" })

@
-}
readCsvWithOpts :: ReadOptions -> FilePath -> IO DataFrame
readCsvWithOpts = readSeparated ','

{- | Read TSV (tab separated) file from path and load it into a dataframe.

==== __Example__
@
ghci> D.readTsv ".\/data\/taxi.tsv"

@
-}
readTsv :: FilePath -> IO DataFrame
readTsv = readSeparated '\t' defaultReadOptions

{- | Read text file with specified delimiter into a dataframe.

==== __Example__
@
ghci> D.readSeparated ';' D.defaultReadOptions ".\/data\/taxi.txt"

@
-}
readSeparated :: Char -> ReadOptions -> FilePath -> IO DataFrame
readSeparated !sep !opts !path = withFile path ReadMode $ \handle -> do
    hSetBuffering handle (BlockBuffering (Just (chunkSize opts)))

    firstLine <- C8.hGetLine handle
    let firstRow = parseLine sep firstLine
        columnNames = case headerSpec opts of
            NoHeader -> map (T.pack . show) [0 .. length firstRow - 1]
            UseFirstRow -> map (stripQuotes . TE.decodeUtf8Lenient) firstRow
            ProvideNames ns -> ns

    unless (headerSpec opts == UseFirstRow) $ hSeek handle AbsoluteSeek 0

    dataLine <- C8.hGetLine handle
    let dataRow = parseLine sep dataLine
    growingCols <- initializeColumns dataRow opts

    processRow 0 dataRow growingCols

    processFile handle sep growingCols (chunkSize opts) 1

    frozenCols <- V.fromList <$> mapM freezeGrowingColumn growingCols

    let numRows = maybe 0 columnLength (frozenCols V.!? 0)
        df =
            DataFrame
                { columns = frozenCols
                , columnIndices = M.fromList (zip columnNames [0 ..])
                , dataframeDimensions = (numRows, V.length frozenCols)
                }
    return $
        if shouldInferFromSample (typeSpec opts)
            then
                parseDefaults
                    (typeInferenceSampleSize (typeSpec opts))
                    (safeRead opts)
                    (dateFormat opts)
                    df
            else df

initializeColumns :: [BS.ByteString] -> ReadOptions -> IO [GrowingColumn]
initializeColumns row opts = case typeSpec opts of
    NoInference -> zipWithM initColumn row (expandTypes [])
    InferFromSample _ -> zipWithM initColumn row (expandTypes [])
    SpecifyTypes ts -> zipWithM initColumn row (expandTypes ts)
  where
    expandTypes xs = xs ++ replicate (length row - length xs) (schemaType @T.Text)
    initColumn :: BS.ByteString -> SchemaType -> IO GrowingColumn
    initColumn bs t = do
        nullsRef <- newIORef []
        case t of
            SType (_ :: P.Proxy a) -> case testEquality (typeRep @a) (typeRep @Int) of
                Just Refl -> GrowingInt <$> newGrowingUnboxedVector 1_024 <*> pure nullsRef
                Nothing -> case testEquality (typeRep @a) (typeRep @Double) of
                    Just Refl -> GrowingDouble <$> newGrowingUnboxedVector 1_024 <*> pure nullsRef
                    Nothing -> GrowingText <$> newGrowingVector 1_024 <*> pure nullsRef

data InferredType = IntType | DoubleType | TextType

inferType :: T.Text -> InferredType
inferType !t
    | T.null t = TextType
    | isJust (readInt t) = IntType
    | isJust (readDouble t) = DoubleType
    | otherwise = TextType

processRow :: Int -> [BS.ByteString] -> [GrowingColumn] -> IO ()
processRow !rowIdx !vals !cols = zipWithM_ (processValue rowIdx) vals cols
  where
    processValue :: Int -> BS.ByteString -> GrowingColumn -> IO ()
    processValue !idx !bs !col = do
        let !val = (stripQuotes . TE.decodeUtf8Lenient) bs
        case col of
            GrowingInt gv nulls ->
                case readByteStringInt bs of
                    Just !i -> appendGrowingUnboxedVector gv i
                    Nothing -> do
                        appendGrowingUnboxedVector gv 0
                        modifyIORef' nulls (idx :)
            GrowingDouble gv nulls ->
                case readByteStringDouble bs of
                    Just !d -> appendGrowingUnboxedVector gv d
                    Nothing -> do
                        appendGrowingUnboxedVector gv 0.0
                        modifyIORef' nulls (idx :)
            GrowingText gv nulls ->
                if isNull val
                    then do
                        appendGrowingVector gv T.empty
                        modifyIORef' nulls (idx :)
                    else appendGrowingVector gv val

isNull :: T.Text -> Bool
isNull t = T.null t || t == "NA" || t == "NULL" || t == "null"

processFile :: Handle -> Char -> [GrowingColumn] -> Int -> Int -> IO ()
processFile !handle !sep !cols !chunk r = do
    let go remain !rowIdx =
            parseWith (C8.hGetNonBlocking handle chunk) (parseRow sep) remain >>= \case
                Fail unconsumed ctx er -> do
                    erpos <- hTell handle
                    fail $
                        "Failed to parse CSV file around "
                            <> show erpos
                            <> " byte; due: "
                            <> show er
                            <> "; context: "
                            <> show ctx
                Partial c -> do
                    fail "Partial handler is called"
                Done (unconsumed :: C8.ByteString) (row :: [C8.ByteString]) -> do
                    processRow rowIdx row cols
                    unless (null row || unconsumed == mempty) $ go unconsumed $! rowIdx + 1
    go "" r

parseLine :: Char -> BS.ByteString -> [BS.ByteString]
parseLine !sep = fromRight [] . parseOnly (record sep)

parseRow :: Char -> Parser [C8.ByteString]
parseRow sep = record sep <* (endOfLine <|> endOfInput) <?> "CSV row"

record :: Char -> Parser [BS.ByteString]
record sep = field sep `sepBy` char sep <?> "CSV record"

field :: Char -> Parser BS.ByteString
field sep = quotedField <|> unquotedField sep <?> "CSV field"

unquotedField :: Char -> Parser BS.ByteString
unquotedField sep = takeWhile (\c -> c /= sep && c /= '\n' && c /= '\r') <?> "unquoted field"

quotedField :: Parser BS.ByteString
quotedField =
    do
        char '"'
        contents <- Builder.toLazyByteString <$> parseQuotedContents
        char '"'
        return $ BS.toStrict contents
        <?> "quoted field"
  where
    parseQuotedContents = mconcat <$> many quotedChar
    quotedChar =
        Builder.byteString <$> takeWhile1 (/= '"')
            <|> ((char '"' *> char '"') Data.Functor.$> Builder.char8 '"')
            <?> "quoted field content"

endOfLine :: Parser ()
endOfLine =
    (void (string "\r\n") <|> void (char '\n') <|> void (char '\r'))
        <?> "line ending"

freezeGrowingColumn :: GrowingColumn -> IO Column
freezeGrowingColumn (GrowingInt gv nullsRef) = do
    vec <- freezeGrowingUnboxedVector gv
    nulls <- readIORef nullsRef
    if null nulls
        then return $ UnboxedColumn vec
        else do
            let size = VU.length vec
            mvec <- VM.new size
            forM_ [0 .. size - 1] $ \i -> do
                if i `elem` nulls
                    then VM.write mvec i Nothing
                    else VM.write mvec i (Just (vec VU.! i))
            OptionalColumn <$> V.freeze mvec
freezeGrowingColumn (GrowingDouble gv nullsRef) = do
    vec <- freezeGrowingUnboxedVector gv
    nulls <- readIORef nullsRef
    if null nulls
        then return $ UnboxedColumn vec
        else do
            let size = VU.length vec
            mvec <- VM.new size
            forM_ [0 .. size - 1] $ \i -> do
                if i `elem` nulls
                    then VM.write mvec i Nothing
                    else VM.write mvec i (Just (vec VU.! i))
            OptionalColumn <$> V.freeze mvec
freezeGrowingColumn (GrowingText gv nullsRef) = do
    vec <- freezeGrowingVector gv
    nulls <- readIORef nullsRef
    if null nulls
        then return $ BoxedColumn vec
        else do
            let size = V.length vec
            mvec <- VM.new size
            forM_ [0 .. size - 1] $ \i -> do
                if i `elem` nulls
                    then VM.write mvec i Nothing
                    else VM.write mvec i (Just (vec V.! i))
            OptionalColumn <$> V.freeze mvec

writeCsv :: FilePath -> DataFrame -> IO ()
writeCsv = writeSeparated ','

writeSeparated ::
    -- | Separator
    Char ->
    -- | Path to write to
    FilePath ->
    DataFrame ->
    IO ()
writeSeparated c filepath df = withFile filepath WriteMode $ \handle -> do
    let (rows, _) = dataframeDimensions df
    let headers = map fst (L.sortBy (compare `on` snd) (M.toList (columnIndices df)))
    TIO.hPutStrLn handle (T.intercalate ", " headers)
    forM_ [0 .. (rows - 1)] $ \i -> do
        let row = getRowAsText df i
        TIO.hPutStrLn handle (T.intercalate ", " row)

getRowAsText :: DataFrame -> Int -> [T.Text]
getRowAsText df i = V.ifoldr go [] (columns df)
  where
    indexMap = M.fromList (map (\(a, b) -> (b, a)) $ M.toList (columnIndices df))
    go k (BoxedColumn (c :: V.Vector a)) acc = case c V.!? i of
        Just e -> textRep : acc
          where
            textRep = case testEquality (typeRep @a) (typeRep @T.Text) of
                Just Refl -> e
                Nothing -> case typeRep @a of
                    App t1 t2 -> case eqTypeRep t1 (typeRep @Maybe) of
                        Just HRefl -> case testEquality t2 (typeRep @T.Text) of
                            Just Refl -> fromMaybe "null" e
                            Nothing -> (fromOptional . T.pack . show) e
                              where
                                fromOptional s
                                    | T.isPrefixOf "Just " s = T.drop (T.length "Just ") s
                                    | otherwise = "null"
                        Nothing -> (T.pack . show) e
                    _ -> (T.pack . show) e
        Nothing ->
            error $
                "Column "
                    ++ T.unpack (indexMap M.! k)
                    ++ " has less items than "
                    ++ "the other columns at index "
                    ++ show i
    go k (UnboxedColumn c) acc = case c VU.!? i of
        Just e -> T.pack (show e) : acc
        Nothing ->
            error $
                "Column "
                    ++ T.unpack (indexMap M.! k)
                    ++ " has less items than "
                    ++ "the other columns at index "
                    ++ show i
    go k (OptionalColumn (c :: V.Vector (Maybe a))) acc = case c V.!? i of
        Just e -> textRep : acc
          where
            textRep = case testEquality (typeRep @a) (typeRep @T.Text) of
                Just Refl -> fromMaybe "Nothing" e
                Nothing -> (T.pack . show) e
        Nothing ->
            error $
                "Column "
                    ++ T.unpack (indexMap M.! k)
                    ++ " has less items than "
                    ++ "the other columns at index "
                    ++ show i

stripQuotes :: T.Text -> T.Text
stripQuotes txt =
    case T.uncons txt of
        Just ('"', rest) ->
            case T.unsnoc rest of
                Just (middle, '"') -> middle
                _ -> txt
        _ -> txt
