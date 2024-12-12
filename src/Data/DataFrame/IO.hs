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

import Control.Monad (foldM_, forM_, replicateM_, foldM, zipWithM_)
import Control.Monad.IO.Class (MonadIO(liftIO))
import Control.Monad.ST ( ST, runST )
import Data.DataFrame.Internal ( empty, DataFrame, Column(..) )
import Data.DataFrame.Operations (addColumn, addColumn', columnNames, parseDefaults, parseDefault)
import Data.DataFrame.Util
import Data.List (transpose, foldl')
import Data.Maybe ( fromMaybe )
import GHC.IO (unsafePerformIO)
import GHC.IO.Handle
    ( hClose, hFlush, hSeek, SeekMode(AbsoluteSeek), hIsEOF )
import GHC.IO.Handle.Types (Handle)
import GHC.Stack (HasCallStack)
import Text.Read (readMaybe)
import System.Directory ( removeFile )
import System.IO ( withFile, IOMode(ReadMode), openTempFile )

data ReadOptions = ReadOptions {
    hasHeader :: Bool,
    inferTypes :: Bool,
    safeRead :: Bool
}

defaultOptions :: ReadOptions
defaultOptions = ReadOptions { hasHeader = True, inferTypes = True, safeRead = True }

readCsv :: String -> IO DataFrame
readCsv = readSeparated ',' defaultOptions

readTsv :: String -> IO DataFrame
readTsv = readSeparated '\t' defaultOptions

readSeparated :: Char -> ReadOptions-> String -> IO DataFrame
readSeparated c opts path = withFile path ReadMode $ \handle -> do
    columnNames <-if hasHeader opts
                  then map T.strip . T.split (c ==) <$> TIO.hGetLine handle
                  else return [] -- TODO: this current doesn't work for a CSV file
                                 -- with no header. We can read a line and seek back.
    tmpFiles <- getTempFiles columnNames
    mkColumns c tmpFiles handle
    df <- foldM (\df (i, name) -> do
        let h = snd $ tmpFiles !! i
        hSeek h AbsoluteSeek 0
        col'' <- T.lines <$> TIO.hGetContents h
        let col' = MkColumn (V.fromList col'')
        let col = if inferTypes opts then parseDefault (safeRead opts) col' else col'
        return $ addColumn' name col df) empty (zip [0..] columnNames)
    mapM_ (\(f, h) -> hClose handle >> removeFile f) tmpFiles
    return df


getTempFiles :: [T.Text] -> IO [(String, Handle)]
getTempFiles cnames = do
    mapM (openTempFile "/tmp" . T.unpack) cnames


mkColumns :: Char -> [(String, Handle)] -> Handle -> IO ()
mkColumns c tmpFiles inputHandle = do
    row <- TIO.hGetLine inputHandle
    let splitRow = split c row
    zipWithM_ (\s (f, h) -> TIO.hPutStrLn h s >> hFlush h) splitRow tmpFiles
    isEOF <- hIsEOF inputHandle
    if isEOF then return () else mkColumns c tmpFiles inputHandle

split :: Char -> T.Text -> [T.Text]
split c s
    | not (T.elem '\"' s) = T.split (c ==) s
    | otherwise           = splitIgnoring c '\"' s

splitIgnoring :: Char -> Char -> T.Text -> [T.Text]
splitIgnoring c o s = splitIgnoring' c o s False ""

splitIgnoring' :: Char -> Char -> T.Text -> Bool -> T.Text -> [T.Text]
splitIgnoring' c o s inIgnore acc
    | T.empty == s                                        = [acc | acc /= ""]
    | T.head s == o                                       = splitIgnoring' c o (T.tail s) (not inIgnore) (acc `T.append` T.singleton (T.head s))
    | (T.head s == c || T.head s == '\r') && not inIgnore = acc : splitIgnoring' c o (T.tail s) inIgnore ""
    | otherwise                                           = splitIgnoring' c o (T.tail s) inIgnore (acc `T.append` T.singleton (T.head s))
