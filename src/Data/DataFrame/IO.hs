{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE Strict #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}

module Data.DataFrame.IO (
    readSeparated,
    readCsv,
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

import qualified Data.Map as M
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as VM

import Control.Monad (foldM_, forM_, replicateM_, foldM, when, zipWithM_, unless)
import Control.Monad.IO.Class (MonadIO(liftIO))
import Control.Monad.ST ( ST, runST )
import Data.Char (intToDigit)
import Data.DataFrame.Internal ( empty, DataFrame, Column(..) )
import Data.DataFrame.Operations (addColumn, addColumn', columnNames, parseDefaults, parseDefault)
import Data.DataFrame.Util
import Data.List (transpose, foldl')
import Data.Maybe ( fromMaybe )
import GHC.IO (unsafePerformIO)
import GHC.IO.Handle
    ( hClose, hSeek, SeekMode(AbsoluteSeek), hIsEOF )
import GHC.IO.Handle.Types (Handle)
import GHC.Stack (HasCallStack)
import Text.Read (readMaybe)
import System.Directory ( removeFile, getTemporaryDirectory )
import System.IO ( withFile, IOMode(ReadMode), openTempFile )

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
    -- If there was no header rewind the file cursor.
    unless (hasHeader opts) $ hSeek handle AbsoluteSeek 0

    -- Initialize mutable vectors for each column
    let numColumns = length columnNames
    mutableCols <- VM.replicateM numColumns (VM.new 1024)  -- Start with a capacity of 1024 rows per column
    rowCounts <- VM.replicate numColumns 0  -- Keeps track of the row count for each column

    -- Read rows into the mutable vectors
    fillColumns c mutableCols rowCounts handle

    -- Freeze the mutable vectors into immutable ones
    cols <- mapM (freezeColumn rowCounts mutableCols) [0..(numColumns - 1)]
    let dfColumns = zipWith (mkColumn opts) columnNames cols
    return $ foldl (\df (name, col) -> addColumn' name col df) empty dfColumns

-- | Reads rows from the handle and stores values in mutable vectors.
fillColumns :: Char -> VM.IOVector (VM.IOVector T.Text) -> VM.IOVector Int -> Handle -> IO ()
fillColumns c mutableCols rowCounts handle = do
    isEOF <- hIsEOF handle
    unless isEOF $ do
        row <- split c <$> TIO.hGetLine handle
        zipWithM_ (writeValue mutableCols rowCounts) [0..] row
        fillColumns c mutableCols rowCounts handle

-- | Writes a value into the appropriate column, resizing the vector if necessary.
writeValue :: VM.IOVector (VM.IOVector T.Text) -> VM.IOVector Int -> Int -> T.Text -> IO ()
writeValue mutableCols rowCounts colIndex value = do
    col <- VM.read mutableCols colIndex
    count <- VM.read rowCounts colIndex
    let capacity = VM.length col
    when (count >= capacity) $ do
        -- Double the size of the vector if it's full
        let newCapacity = capacity * 2
        newCol <- VM.grow col newCapacity
        VM.write mutableCols colIndex newCol
    
    -- In case we resized we need to get the column again.
    col' <- VM.read mutableCols colIndex
    VM.write col' count value
    VM.write rowCounts colIndex (count + 1)

-- | Freezes a mutable vector into an immutable one, trimming it to the actual row count.
freezeColumn :: VM.IOVector Int -> VM.IOVector (VM.IOVector T.Text) -> Int -> IO (V.Vector T.Text)
freezeColumn rowCounts mutableCols colIndex = do
    count <- VM.read rowCounts colIndex
    col <- VM.read mutableCols colIndex
    V.freeze (VM.slice 0 count col)

-- | Constructs a dataframe column, optionally inferring types.
mkColumn :: ReadOptions -> T.Text -> V.Vector T.Text -> (T.Text, Maybe Column)
mkColumn opts name colData =
    let col = Just $ BoxedColumn colData
    in (name, if inferTypes opts then parseDefault (safeRead opts) col else col)


-- | A naive splitting algorithm. If there are no quotes in the string
-- we split by the character. Otherwise we interate through the
-- string to identify parts where the character is not used to separate
-- columns.
split :: Char -> T.Text -> [T.Text]
split c s
    | not (T.elem '\"' s) = T.split (c ==) s
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
