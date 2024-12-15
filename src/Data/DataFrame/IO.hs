{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE Strict #-}
{-# LANGUAGE ScopedTypeVariables #-}

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

-- | Reads a character separated file into a dataframe.
-- Note this file stores intermediate temporary files
-- while converting the CSV from a row to a columnar format.
readSeparated :: Char -> ReadOptions-> String -> IO DataFrame
readSeparated c opts path = withFile path ReadMode $ \handle -> do
    firstRow <- map T.strip . T.split (c ==) <$> TIO.hGetLine handle
    let columnNames = if hasHeader opts
                      then map (T.filter (/= '\"')) firstRow
                      else map (T.singleton . intToDigit) [0..(length firstRow - 1)]
    -- If there was no header rewind the file cursor.
    unless (hasHeader opts) $ hSeek handle AbsoluteSeek 0
    tmpFiles <- getTempFiles columnNames
    mkColumns c tmpFiles handle
    df <- foldM (\df (i, name) -> do
        let h = snd $ tmpFiles !! i
        hSeek h AbsoluteSeek 0
        col'' <- V.fromList . T.lines <$> TIO.hGetContents h
        let col' = BoxedColumn col''
        let col = if inferTypes opts then parseDefault (safeRead opts) col' else col'
        return $ addColumn' name col df) empty (zip [0..] columnNames)
    mapM_ (\(f, h) -> hClose handle >> removeFile f) tmpFiles
    return df

-- | Gets a list of tmp files named after the column names.
-- If no columns are specified then the names default to column indices.
getTempFiles :: [T.Text] -> IO [(String, Handle)]
getTempFiles cnames = do
    tmpFolder <- getTemporaryDirectory
    mapM (openTempFile tmpFolder . T.unpack) cnames

-- | Extracts each row from the character separated file
-- and writes each value to a file storing all the values
-- in that value's column.
mkColumns :: Char -> [(String, Handle)] -> Handle -> IO ()
mkColumns c tmpFiles inputHandle = do
    row <- TIO.hGetLine inputHandle
    let splitRow = (map (T.filter (/= '\"')) . split c) row
    zipWithM_ (\s (f, h) -> TIO.hPutStrLn h s) splitRow tmpFiles
    isEOF <- hIsEOF inputHandle
    if isEOF then return () else mkColumns c tmpFiles inputHandle

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
splitIgnoring c o s = splitIgnoring' c o s False ""

splitIgnoring' :: Char -> Char -> T.Text -> Bool -> T.Text -> [T.Text]
splitIgnoring' c o s inIgnore acc
    | T.empty == s                                        = [acc | acc /= ""]
    | T.head s == o                                       = splitIgnoring' c o (T.tail s) (not inIgnore) (acc `T.append` T.singleton (T.head s))
    | (T.head s == c || T.head s == '\r') && not inIgnore = acc : splitIgnoring' c o (T.tail s) inIgnore ""
    | otherwise                                           = splitIgnoring' c o (T.tail s) inIgnore (acc `T.append` T.singleton (T.head s))
