{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE Strict #-}
module Data.DataFrame.IO.CSV where

import qualified Data.ByteString.Char8 as C
import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.IO as TLIO
import qualified Data.Text.IO as TIO
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Mutable as VM
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Applicative ((<$>), (<|>), (<*>), (<*), (*>), many)
import Control.Monad (forM_, zipWithM_, unless, void)
import Data.Attoparsec.Text
import Data.Char
import Data.DataFrame.Internal.Column (Column(..), freezeColumn', writeColumn, columnLength)
import Data.DataFrame.Internal.DataFrame (DataFrame(..))
import Data.DataFrame.Internal.Parsing
import Data.DataFrame.Operations.Typing
import Data.Function (on)
import Data.IORef
import Data.Maybe
import Data.Text.Encoding (decodeUtf8Lenient)
import Data.Type.Equality
  ( TestEquality (testEquality),
    type (:~:) (Refl)
  )
import GHC.IO.Handle (Handle)
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
readSeparated c opts path = do
    totalRows <- countRows c path
    withFile path ReadMode $ \handle -> do
        firstRow <- map T.strip . parseSep c <$> TIO.hGetLine handle
        let columnNames = if hasHeader opts
                        then map (T.filter (/= '\"')) firstRow
                        else map (T.singleton . intToDigit) [0..(length firstRow - 1)]
        -- If there was no header rewind the file cursor.
        unless (hasHeader opts) $ hSeek handle AbsoluteSeek 0

        -- Initialize mutable vectors for each column
        let numColumns = length columnNames
        let numRows = if hasHeader opts then totalRows - 1 else totalRows
        -- Use this row to infer the types of the rest of the column.
        -- TODO: this isn't robust but in so far as this is a guess anyway
        -- it's probably fine. But we should probably sample n rows and pick
        -- the most likely type from the sample.
        dataRow <- map T.strip . parseSep c <$> TIO.hGetLine handle

        -- This array will track the indices of all null values for each column.
        -- If any exist then the column will be an optional type.
        nullIndices <- VM.unsafeNew numColumns
        VM.set nullIndices []
        mutableCols <- VM.unsafeNew numColumns
        getInitialDataVectors numRows mutableCols dataRow

        -- Read rows into the mutable vectors
        fillColumns numRows c mutableCols nullIndices handle

        -- Freeze the mutable vectors into immutable ones
        nulls' <- V.unsafeFreeze nullIndices
        cols <- V.mapM (freezeColumn mutableCols nulls' opts) (V.generate numColumns id)
        return $ DataFrame {
                columns = cols,
                freeIndices = [],
                columnIndices = M.fromList (zip columnNames [0..]),
                dataframeDimensions = (maybe 0 columnLength (cols V.! 0), V.length cols)
            }
{-# INLINE readSeparated #-}

getInitialDataVectors :: Int -> VM.IOVector Column -> [T.Text] -> IO ()
getInitialDataVectors n mCol xs = do
    forM_ (zip [0..] xs) $ \(i, x) -> do
        col <- case inferValueType x of
                "Int" -> MutableUnboxedColumn <$>  ((VUM.unsafeNew n :: IO (VUM.IOVector Int)) >>= \c -> VUM.unsafeWrite c 0 (fromMaybe 0 $ readInt x) >> return c)
                "Double" -> MutableUnboxedColumn <$> ((VUM.unsafeNew n :: IO (VUM.IOVector Double)) >>= \c -> VUM.unsafeWrite c 0 (fromMaybe 0 $ readDouble x) >> return c)
                _ -> MutableBoxedColumn <$> ((VM.unsafeNew n :: IO (VM.IOVector T.Text)) >>= \c -> VM.unsafeWrite c 0 x >> return c)
        VM.unsafeWrite mCol i col
{-# INLINE getInitialDataVectors #-}

inferValueType :: T.Text -> T.Text
inferValueType s = let
        example = s
    in case readInt example of
        Just _ -> "Int"
        Nothing -> case readDouble example of
            Just _ -> "Double"
            Nothing -> "Other"
{-# INLINE inferValueType #-}

-- | Reads rows from the handle and stores values in mutable vectors.
fillColumns :: Int -> Char -> VM.IOVector Column -> VM.IOVector [(Int, T.Text)] -> Handle -> IO ()
fillColumns n c mutableCols nullIndices handle = do
    input <- newIORef (mempty :: T.Text)
    forM_ [1..n] $ \i -> do
        isEOF <- hIsEOF handle
        input' <- readIORef input
        unless (isEOF && input' == mempty) $ do
              parseWith (TIO.hGetChunk handle) (parseRow c) input' >>= \case
                Fail unconsumed ctx er -> do
                  erpos <- hTell handle
                  fail $ "Failed to parse CSV file around " <> show erpos <> " byte; due: "
                    <> show er <> "; context: " <> show ctx
                Partial c -> do
                  fail "Partial handler is called"
                Done (unconsumed :: T.Text) (row :: [T.Text]) -> do
                  writeIORef input unconsumed
                  zipWithM_ (writeValue mutableCols nullIndices i) [0..] row
{-# INLINE fillColumns #-}

-- | Writes a value into the appropriate column, resizing the vector if necessary.
writeValue :: VM.IOVector Column -> VM.IOVector [(Int, T.Text)] -> Int -> Int -> T.Text -> IO ()
writeValue mutableCols nullIndices count colIndex value = do
    col <- VM.unsafeRead mutableCols colIndex
    res <- writeColumn count value col
    let modify value = VM.unsafeModify nullIndices ((count, value) :) colIndex
    either modify (const (return ())) res
{-# INLINE writeValue #-}

-- | Freezes a mutable vector into an immutable one, trimming it to the actual row count.
freezeColumn :: VM.IOVector Column -> V.Vector [(Int, T.Text)] -> ReadOptions -> Int -> IO (Maybe Column)
freezeColumn mutableCols nulls opts colIndex = do
    col <- VM.unsafeRead mutableCols colIndex
    Just <$> freezeColumn' (nulls V.! colIndex) col
{-# INLINE freezeColumn #-}

parseSep :: Char -> T.Text -> [T.Text]
parseSep c s = either error id (parseOnly (record c) s)
{-# INLINE parseSep #-}

record :: Char -> Parser [T.Text]
record c =
   field c `sepBy1` char c
   <?> "record"
{-# INLINE record #-}

parseRow :: Char -> Parser [T.Text]
parseRow c = (record c <* lineEnd)  <?> "record-new-line"

field :: Char -> Parser T.Text
field c =
   quotedField <|> unquotedField c
   <?> "field"
{-# INLINE field #-}

unquotedField :: Char -> Parser T.Text
unquotedField sep =
   takeWhile nonTerminal <?> "unquoted field"
   where nonTerminal = (`S.notMember` S.fromList [sep, '\n', '\r', '"'])
{-# INLINE unquotedField #-}

insideQuotes :: Parser T.Text
insideQuotes =
   T.append <$> takeWhile (/= '"')
            <*> (T.concat <$> many (T.cons <$> dquotes <*> insideQuotes))
   <?> "inside of double quotes"
   where
      dquotes =
         string "\"\"" >> return '"'
         <?> "paired double quotes"
{-# INLINE insideQuotes #-}

quotedField :: Parser T.Text
quotedField =
   char '"' *> insideQuotes <* char '"'
   <?> "quoted field"
{-# INLINE quotedField #-}

lineEnd :: Parser ()
lineEnd =
   (endOfLine <|> endOfInput)
   <?> "end of line"
{-# INLINE lineEnd #-}

-- | First pass to count rows for exact allocation
countRows :: Char -> FilePath -> IO Int
countRows c path = withFile path ReadMode $! go 0 ""
   where
      go !n !input h = do
         isEOF <- hIsEOF h
         if isEOF && input == mempty
            then pure n
            else
               parseWith (TIO.hGetChunk h) (parseRow c) input >>= \case
                  Fail unconsumed ctx er -> do
                    erpos <- hTell h
                    fail $ "Failed to parse CSV file around " <> show erpos <> " byte; due: "
                      <> show er <> "; context: " <> show ctx
                  Partial c -> do
                    fail $ "Partial handler is called; n = " <> show n
                  Done (unconsumed :: T.Text) _ ->
                    go (n + 1) unconsumed h
{-# INLINE countRows #-}

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
    go k (Just (OptionalColumn (c :: V.Vector (Maybe a)))) acc = case c V.!? i of
        Just e -> textRep : acc
            where textRep = case testEquality (typeRep @a) (typeRep @T.Text) of
                    Just Refl -> fromMaybe "Nothing" e
                    Nothing   -> (T.pack . show) e
        Nothing ->
            error $
                "Column "
                ++ T.unpack (indexMap M.! k)
                ++ " has less items than "
                ++ "the other columns at index "
                ++ show i
