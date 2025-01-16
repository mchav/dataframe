{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}
module Data.DataFrame.IO.CSV where

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.IO as TLIO
import qualified Data.Text.IO as TIO
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Mutable as VM
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Monad (forM_, zipWithM_, unless)
import Data.Char
import Data.DataFrame.Internal.Column (Column(..), freezeColumn', writeColumn, columnLength)
import Data.DataFrame.Internal.DataFrame (DataFrame(..))
import Data.DataFrame.Internal.Parsing
import Data.DataFrame.Operations.Typing
import Data.Function (on)
import Data.Maybe
import Data.Type.Equality
  ( TestEquality (testEquality),
    type (:~:) (Refl)
  )
import GHC.IO.Handle (Handle)
import System.IO
import Type.Reflection

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
    firstRow <- split c <$> TIO.hGetLine handle
    let columnNames = if hasHeader opts
                      then map (T.filter (/= '\"')) firstRow
                      else map (T.singleton . intToDigit) [0..(length firstRow - 1)]
    -- If there was no header rewind the file cursor.
    unless (hasHeader opts) $ hSeek handle AbsoluteSeek 0

    -- Initialize mutable vectors for each column
    let numColumns = length columnNames
    dataRow <- split c <$> TIO.hGetLine handle
    totalRows <- countRows path
    let actualRows = if hasHeader opts then totalRows - 1 else totalRows
    nullIndices <- VM.new numColumns
    VM.set nullIndices []
    mutableCols <- VM.new numColumns
    getInitialDataVectors actualRows mutableCols dataRow

    -- Read rows into the mutable vectors
    fillColumns 1 c mutableCols nullIndices handle

    -- Freeze the mutable vectors into immutable ones
    nulls' <- V.unsafeFreeze nullIndices
    cols <- V.mapM (freezeColumn mutableCols nulls' opts) (V.generate numColumns id)
    return $ DataFrame {
            columns = cols,
            freeIndices = [],
            columnIndices = M.fromList (zip columnNames [0..]),
            dataframeDimensions = (maybe 0 columnLength (cols V.! 0), V.length cols)
        }

getInitialDataVectors :: Int -> VM.IOVector Column -> [T.Text] -> IO ()
getInitialDataVectors n mCol xs = do
    forM_ (zip [0..] xs) $ \(i, x) -> do
        let x' = removeQuotes x
        col <- case inferValueType x of
                "Int" -> MutableUnboxedColumn <$> ((VUM.new n :: IO (VUM.IOVector Int)) >>= \c -> VUM.write c 0 (fromMaybe 0 $ readInt x') >> return c)
                "Double" -> MutableUnboxedColumn <$> ((VUM.new n :: IO (VUM.IOVector Double)) >>= \c -> VUM.write c 0 (fromMaybe 0 $ readDouble x') >> return c)
                _ -> MutableBoxedColumn <$> ((VM.new n :: IO (VM.IOVector T.Text)) >>= \c -> VM.write c 0 x' >> return c)
        VM.write mCol i col

inferValueType :: T.Text -> T.Text
inferValueType s = let    
        example = s
    in case readInt example of
        Just _ -> "Int"
        Nothing -> case readDouble example of
            Just _ -> "Double"
            Nothing -> "Other"

-- | Reads rows from the handle and stores values in mutable vectors.
fillColumns :: Int -> Char -> VM.IOVector Column -> VM.IOVector [(Int, T.Text)] -> Handle -> IO ()
fillColumns i c mutableCols nullIndices handle = do
    isEOF <- hIsEOF handle
    unless isEOF $ do
        row <- split c <$> TIO.hGetLine handle
        zipWithM_ (writeValue mutableCols nullIndices i) [0..] row
        fillColumns (i + 1) c mutableCols nullIndices handle

-- | Writes a value into the appropriate column, resizing the vector if necessary.
writeValue :: VM.IOVector Column -> VM.IOVector [(Int, T.Text)] -> Int -> Int -> T.Text -> IO ()
writeValue mutableCols nullIndices count colIndex value = do
    col <- VM.read mutableCols colIndex
    res <- writeColumn count value col
    case res of
        Left value -> VM.modify nullIndices ((count, value) :) colIndex
        Right _ -> return ()

-- | Freezes a mutable vector into an immutable one, trimming it to the actual row count.
freezeColumn :: VM.IOVector Column -> V.Vector [(Int, T.Text)] -> ReadOptions -> Int -> IO (Maybe Column)
freezeColumn mutableCols nulls opts colIndex = do
    col <- VM.read mutableCols colIndex
    Just <$> freezeColumn' (nulls V.! colIndex) col

-- | Constructs a dataframe column, optionally inferring types.
mkColumn :: ReadOptions -> V.Vector T.Text -> Maybe Column
mkColumn opts colData =
    let col = Just $ BoxedColumn colData
    in if inferTypes opts then parseDefault (safeRead opts) col else col


-- | A naive splitting algorithm. If there are no quotes in the string
-- we split by the character. Otherwise we interate through the
-- string to identify parts where the character is not used to separate
-- columns.
split :: Char -> T.Text -> [T.Text]
split c s
    | not (hasCommaInQuotes s) = map (removeQuotes . T.strip) $ T.split (c ==) s
    | otherwise           = splitIgnoring c '\"' s

-- TODO: This currently doesn't handle anything except quotes. It should
-- generalize to handle ther structures e.g braces and parens.
-- This should probably use a stack.
splitIgnoring :: Char -> Char -> T.Text -> [T.Text]
splitIgnoring c o s = reverse ret
    where (_, acc, res) = T.foldl (splitIgnoring' c o) (False, "", []) s
          ret = if acc == "" && T.last s /= ',' then res else acc : res

splitIgnoring' :: Char -> Char -> (Bool, T.Text, [T.Text]) -> Char -> (Bool, T.Text, [T.Text])
splitIgnoring' c o (!inIgnore, !word, !res) curr 
    | curr == o                  = (not inIgnore, word, res)
    | isTerminal && not inIgnore = (inIgnore, "", word:res)
    | otherwise                  = (inIgnore, word `T.snoc` curr, res)
        where isTerminal = curr == c || (curr == '\r' && word /= "")

hasCommaInQuotes :: T.Text -> Bool
hasCommaInQuotes = snd . T.foldl' go (False, False)
    where go (!inQuotes, !hasComma) c
            | c == ',' && inQuotes = (inQuotes, True)
            | c == '\"' = (not inQuotes, hasComma)
            | otherwise = (inQuotes, hasComma)

removeQuotes :: T.Text -> T.Text
removeQuotes s
    | T.null s' = s'
    | otherwise = if T.head s' == '\"' && T.last s' == '\"' then T.init (T.tail s') else s'
        where s' = T.strip s


-- | First pass to count rows for exact allocation
countRows :: FilePath -> IO Int
countRows path = withFile path ReadMode $ \handle -> do
    let loop !n = do
            eof <- hIsEOF handle
            if eof
            then return n
            else TLIO.hGetLine handle >> loop (n + 1) 
    loop 0

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
