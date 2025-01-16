{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}
module Data.DataFrame.IO.CSV where

-- import qualified Data.Array as Array
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
import qualified Streamly.Data.Array as Array
import qualified Streamly.Data.Fold as Fold
import qualified Streamly.Data.Stream as Stream
import qualified Streamly.Data.Stream.Prelude as Stream
import qualified Streamly.Internal.Data.Array as Array (compactOnByte, toStream)
import qualified Streamly.FileSystem.File as File
import qualified Streamly.FileSystem.Handle as Handle
import qualified Streamly.Unicode.Stream as Stream

import Control.Applicative ((<$>), (<|>), (<*>), (<*), (*>), many)
import Control.Monad (forM_, zipWithM_, unless, void, zipWithM)
import Data.Attoparsec.Text
import Data.Char
import Data.DataFrame.Internal.Column (Column(..), freezeColumn', writeColumn, columnLength)
import Data.DataFrame.Internal.DataFrame (DataFrame(..))
import Data.DataFrame.Internal.Parsing
import Data.DataFrame.Operations.Typing
import Data.Either
import Data.Function (on, (&))
import Data.Maybe
import Data.Type.Equality
  ( TestEquality (testEquality),
    type (:~:) (Refl)
  )
import Data.Word
import GHC.Conc (numCapabilities)
import GHC.IO.Handle (Handle)
import GHC.IO (unsafePerformIO)
import Prelude hiding (concat, takeWhile)
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
    firstRow <- parseSep c <$> TIO.hGetLine handle
    let columnNames = if hasHeader opts
                      then map (T.filter (/= '\"')) firstRow
                      else map (T.singleton . intToDigit) [0..(length firstRow - 1)]
    -- If there was no header rewind the file cursor.
    unless (hasHeader opts) $ hSeek handle AbsoluteSeek 0

    -- Initialize mutable vectors for each column
    let numColumns = length columnNames
    dataRow <- parseSep c <$> TIO.hGetLine handle
    totalRows <- wc path
    let actualRows = if hasHeader opts then totalRows - 1 else totalRows
    nullIndices <- VM.new numColumns
    VM.set nullIndices []
    mutableCols <- VM.new numColumns
    getInitialDataVectors actualRows mutableCols dataRow

    -- Read rows into the mutable vectors
    fillColumns c mutableCols nullIndices handle

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
        col <- case inferValueType x of
                "Int" -> MutableUnboxedColumn <$> ((VUM.new n :: IO (VUM.IOVector Int)) >>= \c -> VUM.write c 0 (fromMaybe 0 $ readInt x) >> return c)
                "Double" -> MutableUnboxedColumn <$> ((VUM.new n :: IO (VUM.IOVector Double)) >>= \c -> VUM.write c 0 (fromMaybe 0 $ readDouble x) >> return c)
                _ -> MutableBoxedColumn <$> ((VM.new n :: IO (VM.IOVector T.Text)) >>= \c -> VM.write c 0 x >> return c)
        VM.write mCol i col

inferValueType :: T.Text -> T.Text
inferValueType s = let
        example = s
    in case readInt example of
        Just _ -> "Int"
        Nothing -> case readDouble example of
            Just _ -> "Double"
            Nothing -> "Other"

wc :: String -> IO Int
wc file = File.read file
    & Stream.decodeUtf8
    & Stream.fold (Fold.foldl' (\l ch -> if ch == '\n' then l + 1 else l) 0)

fillColumns :: Char -> VM.IOVector Column -> VM.IOVector [(Int, T.Text)] -> Handle -> IO ()
fillColumns c mutableCols nullIndices handle =
      Handle.read handle
        & Stream.decodeUtf8
        & Stream.splitOn (== '\n') Fold.toList
        & Stream.filter (not . null)
        & Stream.zipWith (,) (Stream.fromList [1..])
        & Stream.fold (Fold.drainMapM (parseLine c mutableCols nullIndices))

parseLine :: Char -> VM.IOVector Column -> VM.IOVector [(Int, T.Text)] -> (Int, [Char]) -> IO ()
parseLine c mutableCols nullIndices (i, arr) = do
    zipWithM_ (writeValue mutableCols nullIndices i) [0..] (parseSep c (T.pack arr))

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

parseSep :: Char -> T.Text -> [T.Text]
parseSep c s = either error id (parseOnly (record c) s)

record :: Char -> Parser [T.Text]
record c =
   field c `sepBy1` char c
   <?> "record"

field :: Char -> Parser T.Text
field c =
   quotedField <|> unquotedField c
   <?> "field"

unquotedField :: Char -> Parser T.Text
unquotedField sep =
   takeWhile (\c -> c /= sep && c /= '\n' && c /= '\r' && c /= '"')
   <?> "unquoted field"

insideQuotes :: Parser T.Text
insideQuotes =
   T.append <$> takeWhile (/= '"')
            <*> (T.concat <$> many (T.cons <$> dquotes <*> insideQuotes))
   <?> "inside of double quotes"
   where
      dquotes =
         string "\"\"" >> return '"'
         <?> "paired double quotes"

quotedField :: Parser T.Text
quotedField =
   char '"' *> insideQuotes <* char '"'
   <?> "quoted field"

lineEnd :: Parser ()
lineEnd =
   void (char '\n') <|> void (string "\r\n") <|> void (char '\r')
   <?> "end of line"

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
