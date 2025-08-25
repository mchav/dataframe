{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.IO.CSV where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as C8
import qualified Data.List as L
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import qualified Data.Text.Encoding as TE
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as VM
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Applicative (many, (*>), (<$>), (<*), (<*>), (<|>))
import Control.DeepSeq (NFData(..), deepseq)
import Control.Monad (forM_, unless, when, zipWithM_)
import Control.Monad.ST (runST)
import Data.Attoparsec.ByteString.Char8
import Data.Bits (shiftL)
import Data.Char
import Data.Function (on)
import Data.IORef
import Data.Maybe
import Data.Type.Equality (
    TestEquality (testEquality),
    type (:~:) (Refl),
 )
import DataFrame.Internal.Column (Column (..), MutableColumn (..), columnLength, freezeColumn', writeColumn)
import DataFrame.Internal.DataFrame (DataFrame (..))
import DataFrame.Internal.Parsing
import DataFrame.Operations.Typing
import GHC.IO.Handle (Handle)
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

data ReadOptions = ReadOptions
    { hasHeader :: Bool
    , inferTypes :: Bool
    , safeRead :: Bool
    , chunkSize :: Int
    }

defaultOptions :: ReadOptions
defaultOptions = ReadOptions
    { hasHeader = True
    , inferTypes = True
    , safeRead = True
    , chunkSize = 65536
    }

newGrowingVector :: Int -> IO (GrowingVector a)
newGrowingVector !initCap = do
    vec <- VM.unsafeNew initCap
    GrowingVector <$> newIORef vec <*> newIORef 0 <*> newIORef initCap

newGrowingUnboxedVector :: VUM.Unbox a => Int -> IO (GrowingUnboxedVector a)
newGrowingUnboxedVector !initCap = do
    vec <- VUM.unsafeNew initCap
    GrowingUnboxedVector <$> newIORef vec <*> newIORef 0 <*> newIORef initCap

appendGrowingVector :: GrowingVector a -> a -> IO ()
appendGrowingVector (GrowingVector vecRef sizeRef capRef) !val = do
    size <- readIORef sizeRef
    cap <- readIORef capRef
    vec <- readIORef vecRef
    
    vec' <- if size >= cap
        then do
            let !newCap = cap `shiftL` 1
            newVec <- VM.unsafeGrow vec newCap
            writeIORef vecRef newVec
            writeIORef capRef newCap
            return newVec
        else return vec
    
    VM.unsafeWrite vec' size val
    writeIORef sizeRef $! size + 1

appendGrowingUnboxedVector :: VUM.Unbox a => GrowingUnboxedVector a -> a -> IO ()
appendGrowingUnboxedVector (GrowingUnboxedVector vecRef sizeRef capRef) !val = do
    size <- readIORef sizeRef
    cap <- readIORef capRef
    vec <- readIORef vecRef
    
    vec' <- if size >= cap
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

freezeGrowingUnboxedVector :: VUM.Unbox a => GrowingUnboxedVector a -> IO (VU.Vector a)
freezeGrowingUnboxedVector (GrowingUnboxedVector vecRef sizeRef _) = do
    vec <- readIORef vecRef
    size <- readIORef sizeRef
    VU.freeze (VUM.slice 0 size vec)

readCsv :: String -> IO DataFrame
readCsv = readSeparated ',' defaultOptions

readTsv :: String -> IO DataFrame
readTsv = readSeparated '\t' defaultOptions

readSeparated :: Char -> ReadOptions -> String -> IO DataFrame
readSeparated !sep !opts !path = do
    withFile path ReadMode $ \handle -> do
        hSetBuffering handle (BlockBuffering (Just (chunkSize opts)))

        firstLine <- C8.hGetLine handle
        let firstRow = parseLine sep firstLine
            columnNames = if hasHeader opts
                then map (T.filter (/= '\"') . TE.decodeUtf8Lenient) firstRow
                else map (T.singleton . intToDigit) [0 .. length firstRow - 1]
        
        unless (hasHeader opts) $ hSeek handle AbsoluteSeek 0
        
        dataLine <- C8.hGetLine handle
        let dataRow = parseLine sep dataLine
        growingCols <- initializeColumns dataRow opts
        
        processRow 0 dataRow growingCols
        
        processFile handle sep growingCols 1
        
        frozenCols <- V.fromList <$> mapM freezeGrowingColumn growingCols
        
        let numRows = case frozenCols V.!? 0 of
                Just col -> columnLength col
                Nothing -> 0
        
        return $ DataFrame
            { columns = frozenCols
            , columnIndices = M.fromList (zip columnNames [0..])
            , dataframeDimensions = (numRows, V.length frozenCols)
            }

initializeColumns :: [BS.ByteString] -> ReadOptions -> IO [GrowingColumn]
initializeColumns row opts = mapM initColumn row
  where
    initColumn :: BS.ByteString -> IO GrowingColumn
    initColumn bs = do
        nullsRef <- newIORef []
        let val = TE.decodeUtf8Lenient bs
        if inferTypes opts
            then case inferType val of
                IntType -> GrowingInt <$> newGrowingUnboxedVector 1024 <*> pure nullsRef
                DoubleType -> GrowingDouble <$> newGrowingUnboxedVector 1024 <*> pure nullsRef
                TextType -> GrowingText <$> newGrowingVector 1024 <*> pure nullsRef
            else GrowingText <$> newGrowingVector 1024 <*> pure nullsRef

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
        let !val = TE.decodeUtf8Lenient bs
        case col of
            GrowingInt gv nulls -> 
                case readInt val of
                    Just !i -> appendGrowingUnboxedVector gv i
                    Nothing -> do
                        appendGrowingUnboxedVector gv 0
                        modifyIORef' nulls (idx:)
            
            GrowingDouble gv nulls ->
                case readDouble val of
                    Just !d -> appendGrowingUnboxedVector gv d
                    Nothing -> do
                        appendGrowingUnboxedVector gv 0.0
                        modifyIORef' nulls (idx:)
            
            GrowingText gv nulls ->
                if isNull val
                    then do
                        appendGrowingVector gv T.empty
                        modifyIORef' nulls (idx:)
                    else appendGrowingVector gv val

isNull :: T.Text -> Bool
isNull t = T.null t || t == "NA" || t == "NULL" || t == "null"

processFile :: Handle -> Char -> [GrowingColumn] -> Int -> IO ()
processFile !handle !sep !cols = go
  where
    go !rowIdx = do
        eof <- hIsEOF handle
        unless eof $ do
            line <- C8.hGetLine handle
            unless (BS.null line) $ do
                let !vals = parseLine sep line
                processRow rowIdx vals cols
                go $! rowIdx + 1

parseLine :: Char -> BS.ByteString -> [BS.ByteString]
parseLine !sep = either (const []) id . parseOnly (record sep)

record :: Char -> Parser [BS.ByteString]
record !sep = field sep `sepBy` char sep

field :: Char -> Parser BS.ByteString
field !sep = quotedField <|> unquotedField sep

unquotedField :: Char -> Parser BS.ByteString
unquotedField !sep = takeWhile (\c -> c /= sep && c /= '\n' && c /= '\r' && c /= '"')

quotedField :: Parser BS.ByteString
quotedField = do
    char '"'
    contents <- BS.concat <$> many (unquote <|> escaped)
    char '"'
    return contents
  where
    unquote = takeWhile1 (\c -> c /= '"' && c /= '\\')
    escaped = char '\\' *> (C8.singleton <$> anyChar)

freezeGrowingColumn :: GrowingColumn -> IO Column
freezeGrowingColumn (GrowingInt gv nullsRef) = do
    vec <- freezeGrowingUnboxedVector gv
    nulls <- readIORef nullsRef
    if null nulls
        then return $ UnboxedColumn vec
        else do
            let size = VU.length vec
            mvec <- VM.new size
            forM_ [0..size-1] $ \i -> do
                if i `elem` nulls
                    then VM.write mvec i Nothing
                    else VM.write mvec i (Just (vec VU.! i))
            BoxedColumn <$> V.freeze mvec

freezeGrowingColumn (GrowingDouble gv nullsRef) = do
    vec <- freezeGrowingUnboxedVector gv
    nulls <- readIORef nullsRef
    if null nulls
        then return $ UnboxedColumn vec
        else do
            let size = VU.length vec
            mvec <- VM.new size
            forM_ [0..size-1] $ \i -> do
                if i `elem` nulls
                    then VM.write mvec i Nothing
                    else VM.write mvec i (Just (vec VU.! i))
            BoxedColumn <$> V.freeze mvec

freezeGrowingColumn (GrowingText gv nullsRef) = do
    vec <- freezeGrowingVector gv
    nulls <- readIORef nullsRef
    if null nulls
        then return $ BoxedColumn vec
        else do
            let size = V.length vec
            mvec <- VM.new size
            forM_ [0..size-1] $ \i -> do
                if i `elem` nulls
                    then VM.write mvec i Nothing
                    else VM.write mvec i (Just (vec V.! i))
            BoxedColumn <$> V.freeze mvec

writeCsv :: String -> DataFrame -> IO ()
writeCsv = writeSeparated ','

writeSeparated ::
    -- | Separator
    Char ->
    -- | Path to write to
    String ->
    DataFrame ->
    IO ()
writeSeparated c filepath df = withFile filepath WriteMode $ \handle -> do
    let (rows, columns) = dataframeDimensions df
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
                            Nothing -> (fromOptional . (T.pack . show)) e
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
