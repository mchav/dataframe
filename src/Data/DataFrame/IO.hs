{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE Strict #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}

module Data.DataFrame.IO (
    readSeparated,
    writeSeparated,
    readCsv,
    writeCsv,
    readTsv,
    splitIgnoring,
    readValue,
    readInt,
    readInteger,
    readDouble,
    safeReadValue,
    readWithDefault,
    defaultOptions,
    ReadOptions(..)) where

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Mutable as VM

import Control.Monad (foldM_, forM_, replicateM_, foldM, when, zipWithM_, unless, join)
import Control.Monad.IO.Class (MonadIO(liftIO))
import Control.Monad.ST ( ST, runST )
import Data.Char (intToDigit)
import Data.DataFrame.Internal
import Data.DataFrame.Operations (addColumn, addColumn', columnNames, parseDefaults, parseDefault)
import Data.DataFrame.Util
import Data.List (transpose, foldl')
import Data.Maybe ( fromMaybe, isJust )
import Data.Type.Equality
  ( TestEquality (testEquality),
    type (:~:) (Refl)
  )
import GHC.IO (unsafePerformIO)
import GHC.IO.Handle
    ( hClose, hSeek, SeekMode(AbsoluteSeek), hIsEOF )
import GHC.IO.Handle.Types (Handle)
import GHC.Stack (HasCallStack)
import System.Directory ( removeFile, getTemporaryDirectory )
import System.IO ( withFile, IOMode(ReadMode, WriteMode), openTempFile )
import Type.Reflection
import Text.Read (readMaybe)
import Data.Function (on)
import Unsafe.Coerce (unsafeCoerce)
import Control.Concurrent (newMVar)
import Control.Concurrent.MVar
import Control.Concurrent.Async
import Data.Typeable (cast)

-- | Record for CSV read options.
data ReadOptions = ReadOptions {
    hasHeader :: Bool,
    inferTypes :: Bool,
    safeRead :: Bool
}

-- | By default we assume the file has a header, we infer the types on read
-- and we convert any rows with nullish objects into Maybe (safeRead).
defaultOptions :: ReadOptions
defaultOptions = ReadOptions { hasHeader = True, inferTypes = True, safeRead = True }

-- | Reads a CSV file from the given path.
-- Note this file stores intermediate temporary files
-- while converting the CSV from a row to a columnar format.
readCsv :: String -> IO DataFrame
readCsv = readSeparated ',' defaultOptions

-- | Reads a tab separated file from the given path.
-- Note this file stores intermediate temporary files
-- while converting the CSV from a row to a columnar format.
readTsv :: String -> IO DataFrame
readTsv = readSeparated '\t' defaultOptions

-- | Reads a character separated file into a dataframe using mutable vectors.
readSeparated :: Char -> ReadOptions -> String -> IO DataFrame
readSeparated c opts path = withFile path ReadMode $ \handle -> do
    firstRow <- map T.strip . T.split (c ==) <$> TIO.hGetLine handle
    let columnNames = if hasHeader opts
                      then map (T.filter (/= '\"')) firstRow
                      else map (T.singleton . intToDigit) [0..(length firstRow - 1)]
    let numColumns = length columnNames
    -- If there was no header rewind the file cursor.
    unless (hasHeader opts) $ hSeek handle AbsoluteSeek 0

    -- Read the file from which we'll infer types.
    dataRow <- map T.strip . T.split (c ==) <$> TIO.hGetLine handle
    mCols' <- VM.new numColumns
    getInitialDataVectors 0 mCols' dataRow

    -- Initialize mutable vectors for each column
    mutableCols <- newMVar mCols'
    rowCounts <- newMVar =<< VM.replicate numColumns 0

    -- Read rows into the mutable vectors
    fillColumns c mutableCols rowCounts handle

    -- Freeze the mutable vectors into immutable ones
    cols <- mapConcurrently (freezeColumn rowCounts mutableCols) (V.generate numColumns id)
    return $ DataFrame {
            columns = V.map (unJustColumn opts) cols,
            freeIndices = [],
            columnIndices = M.fromList (zip columnNames [0..]),
            dataframeDimensions = (maybe 0 columnLength (cols V.!? 0), V.length cols)
        }

getInitialDataVectors :: Int -> VM.IOVector Column -> [T.Text] -> IO ()
getInitialDataVectors _ _ [] = return ()
getInitialDataVectors i mCol (x:xs) = do
    let x' = removeQuotes x
    col <- case inferValueType x of
            "Int" -> MutableColumn <$> ((VM.new 1024 :: IO (VM.IOVector (Maybe Int))) >>= \c -> VM.write c 0 (readInt x') >> return c)
            "Double" -> MutableColumn <$> ((VM.new 1024 :: IO (VM.IOVector (Maybe Double))) >>= \c -> VM.write c 0 (readDouble x') >> return c)
            _ -> MutableColumn <$> ((VM.new 1024 :: IO (VM.IOVector (Maybe T.Text))) >>= \c -> VM.write c 0 (if isNullish x' then Nothing else Just x') >> return c)
    VM.write mCol i col
    getInitialDataVectors (i + 1) mCol xs

writeCsv :: String -> DataFrame -> IO ()
writeCsv = writeSeparated ','

writeSeparated :: Char      -- ^ Separator
               -> String    -- ^ Path to write to
               -> DataFrame
               -> IO ()
writeSeparated c filepath df = withFile filepath WriteMode $ \handle ->do
    let (rows, columns) = dataframeDimensions df
    let headers = map fst (L.sortBy (compare `on` snd) (M.toList (columnIndices df)))
    TIO.hPutStrLn handle (T.intercalate ", " headers)
    forM_ [0..(rows - 1)] $ \i -> do
        let row = getRowAsText df i
        TIO.hPutStrLn handle (T.intercalate ", " row)

getRowAsText :: DataFrame -> Int -> [T.Text]
getRowAsText df i = V.ifoldr go [] (columns df)
  where
    indexMap = M.fromList (map (\(a, b) -> (b, a)) $ M.toList (columnIndices df))
    go k Nothing acc = acc
    go k (Just (BoxedColumn (c :: V.Vector a))) acc = case c V.!? i of
        Just e -> textRep : acc
            where textRep = case testEquality (typeRep @a) (typeRep @T.Text) of
                    Just Refl -> e
                    Nothing   -> case typeRep @a of
                        App t1 t2 -> case eqTypeRep t1 (typeRep @Maybe) of
                            Just HRefl -> case testEquality t2 (typeRep @T.Text) of
                                Just Refl -> fromMaybe "null" e
                                Nothing -> (fromOptional . (T.pack . show)) e
                                            where fromOptional s
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
    go k (Just (UnboxedColumn c)) acc = case c VU.!? i of
        Just e -> T.pack (show e) : acc
        Nothing ->
            error $
                "Column "
                ++ T.unpack (indexMap M.! k)
                ++ " has less items than "
                ++ "the other columns at index "
                ++ show i

-- | Reads rows from the handle and stores values in mutable vectors.
fillColumns :: Char -> MVar (VM.IOVector Column) -> MVar (VM.IOVector Int) -> Handle -> IO ()
fillColumns c mutableCols rowCounts handle = do
    isEOF <- hIsEOF handle
    unless isEOF $ do
        row <- map T.strip . split c <$> TIO.hGetLine handle
        withAsync (zipWithM_ (writeValue mutableCols rowCounts) [0..] row) wait
        fillColumns c mutableCols rowCounts handle

-- | Writes a value into the appropriate column, resizing the vector if necessary.
writeValue :: MVar (VM.IOVector Column) -> MVar (VM.IOVector Int) -> Int -> T.Text -> IO ()
writeValue mutableColsVar rowCountsVar colIndex value = do
    mutableCols <- takeMVar mutableColsVar
    rowCounts <- takeMVar rowCountsVar
    col <- VM.read mutableCols colIndex
    count <- VM.read rowCounts colIndex
    let capacity = columnLength col
    when (count >= capacity) $ do
        -- Double the size of the vector if it's full
        let newCapacity = capacity * 2
        newCol <- growColumn newCapacity col
        VM.write mutableCols colIndex newCol

    -- In case we resized we need to get the column again.
    col' <- VM.read mutableCols colIndex
    writeColumn count value col'
    VM.write rowCounts colIndex (count + 1)
    putMVar mutableColsVar mutableCols
    putMVar rowCountsVar rowCounts

-- | Freezes a mutable vector into an immutable one, trimming it to the actual row count.
freezeColumn :: MVar (VM.IOVector Int) -> MVar (VM.IOVector Column) -> Int -> IO Column
freezeColumn rowCountsVar mutableColsVar colIndex = do
    rowCounts <- readMVar rowCountsVar
    mutableCols <- readMVar mutableColsVar
    count <- VM.read rowCounts colIndex
    col <- VM.read mutableCols colIndex
    freezeColumn' col count

-- | Constructs a dataframe column, optionally inferring types.
mkColumn :: ReadOptions -> V.Vector T.Text -> Maybe Column
mkColumn opts colData =
    let col = Just $ BoxedColumn colData
    in if inferTypes opts then parseDefault (safeRead opts) col else col

unJustColumn :: ReadOptions -> Column -> Maybe Column
unJustColumn opts c@(BoxedColumn (col :: V.Vector a)) = case typeRep @a of
    App t1 t2 -> case eqTypeRep t1 (typeRep @Maybe) of
        Just HRefl -> if not (V.all isJust col)
                      then Just c
                      else case testEquality (typeRep @a) (typeRep @(Maybe Int)) of
                            Just Refl -> Just $ UnboxedColumn $ VU.convert (V.map (fromMaybe 0) col)
                            Nothing -> case testEquality (typeRep @a) (typeRep @(Maybe Double)) of
                                Just Refl -> Just $ UnboxedColumn $ VU.convert (V.map (fromMaybe 0) col)
                                Nothing -> case testEquality (typeRep @a) (typeRep @(Maybe T.Text)) of
                                    Just Refl -> Just $ BoxedColumn $ V.map (fromMaybe "") col
        Nothing -> Just c
    _ -> Just c

-- | A naive splitting algorithm. If there are no quotes in the string
-- we split by the character. Otherwise we interate through the
-- string to identify parts where the character is not used to separate
-- columns.
split :: Char -> T.Text -> [T.Text]
split c s
    | not (hasCommaInQuotes s) = map removeQuotes $ T.split (c ==) s
    | otherwise           = splitIgnoring c '\"' s

-- TODO: This currently doesn't handle anything except quotes. It should
-- generalize to handle ther structures e.g braces and parens.
-- This should probably use a stack.
splitIgnoring :: Char -> Char -> T.Text -> [T.Text]
splitIgnoring c o s = ret
    where (_, acc, res) = T.foldr (splitIgnoring' c o) (False, "", []) s
          ret = if acc == "" then res else acc : res

splitIgnoring' :: Char -> Char -> Char -> (Bool, T.Text, [T.Text]) -> (Bool, T.Text, [T.Text])
splitIgnoring' c o curr (!inIgnore, !word, !res)
    | curr == o                  = (not inIgnore, word, res)
    | isTerminal && not inIgnore = (inIgnore, "", word:res)
    | otherwise                  = (inIgnore, T.singleton curr `T.append` word, res)
        where isTerminal = curr == c || (curr == '\r' && word /= "")

hasCommaInQuotes :: T.Text -> Bool
hasCommaInQuotes = snd . T.foldl' go (False, False)
    where go (!inQuotes, !hasComma) c
            | c == ',' && inQuotes = (inQuotes, True)
            | c == '\"' = (not inQuotes, hasComma)
            | otherwise = (inQuotes, hasComma)
