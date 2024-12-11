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

import qualified Data.ByteString.Char8 as C
import qualified Data.Map as M
import qualified Data.Set as S
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as VM

import Control.Monad (foldM_, forM_, replicateM_, foldM, zipWithM_)
import Control.Monad.IO.Class (MonadIO(liftIO))
import Control.Monad.ST ( ST, runST )
import Data.DataFrame.Internal ( empty, DataFrame, Column(..) )
import Data.DataFrame.Operations (addColumn, addColumn', columnNames, parseDefaults, parseDefault)
import Data.List (transpose, foldl')
import Data.Maybe ( fromMaybe )
import GHC.IO (unsafePerformIO)
import GHC.IO.Handle
    ( hClose, hFlush, hSeek, SeekMode(AbsoluteSeek), hIsEOF )
import GHC.IO.Handle.Types (Handle)
import GHC.Stack (HasCallStack)
import Text.Read (readMaybe)
import Data.ByteString.Lex.Fractional ( readDecimal )
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
                  then map C.strip . C.split c <$> C.hGetLine handle
                  else return [] -- TODO: this current doesn't work for a CSV file
                                 -- with no header. We can read a line and seek back.
    tmpFiles <- getTempFiles columnNames
    mkColumns c tmpFiles handle
    df <- foldM (\df (i, name) -> do
        let h = snd $ tmpFiles !! i
        hSeek h AbsoluteSeek 0
        col'' <- C.lines <$> C.hGetContents h
        let col' = MkColumn (V.fromList col'')
        let col = if inferTypes opts then parseDefault (safeRead opts) col' else col'
        return $ addColumn' name col df) empty (zip [0..] columnNames)
    mapM_ (\(f, h) -> hClose handle >> removeFile f) tmpFiles
    return df


getTempFiles :: [C.ByteString] -> IO [(String, Handle)]
getTempFiles cnames = do
    mapM (openTempFile "/tmp" . C.unpack) cnames


mkColumns :: Char -> [(String, Handle)] -> Handle -> IO ()
mkColumns c tmpFiles inputHandle = do
    row <- C.hGetLine inputHandle
    let splitRow = split c row
    zipWithM_ (\s (f, h) -> C.hPutStrLn h s >> hFlush h) splitRow tmpFiles
    isEOF <- hIsEOF inputHandle
    if isEOF then return () else mkColumns c tmpFiles inputHandle

split :: Char -> C.ByteString -> [C.ByteString]
split c s
    | C.notElem '\"' s  = C.split c s
    | otherwise         = splitIgnoring c '\"' s

splitIgnoring :: Char -> Char -> C.ByteString -> [C.ByteString]
splitIgnoring c o s = splitIgnoring' c o s False ""

splitIgnoring' :: Char -> Char -> C.ByteString -> Bool -> C.ByteString -> [C.ByteString]
splitIgnoring' c o s inIgnore acc
    | C.empty == s                                        = [acc | acc /= ""]
    | C.head s == o                                       = splitIgnoring' c o (C.tail s) (not inIgnore) (acc `C.append` C.singleton (C.head s))
    | (C.head s == c || C.head s == '\r') && not inIgnore = acc : splitIgnoring' c o (C.tail s) inIgnore ""
    | otherwise                                           = splitIgnoring' c o (C.tail s) inIgnore (acc `C.append` C.singleton (C.head s))

readValue :: (HasCallStack, Read a) => C.ByteString -> a
readValue s = case readMaybe (C.unpack s) of
    Nothing    -> error $ "Could not read value: " ++ C.unpack s
    Just value -> value

readInteger :: HasCallStack => C.ByteString -> Integer
readInteger s = case C.readInteger (C.strip s) of
    Nothing    -> error $ "Could not read Integer value: " ++ C.unpack s
    Just (value, _) -> value

readInt :: HasCallStack => C.ByteString -> Int
readInt s = case C.readInt (C.strip s) of
    Nothing    -> error $ "Could not read int value: " ++ C.unpack s
    Just (value, _) -> value

readDouble :: HasCallStack => C.ByteString -> Double
readDouble s = let isNegative = C.head s == '-'
    in case readDecimal (if isNegative then C.tail s else s) of
            Nothing -> error $ "Could not read Double value: " ++ C.unpack s
            Just(value, _) -> (if isNegative then -1.0 else 1.0) * value

safeReadValue :: (Read a) => C.ByteString -> Maybe a
safeReadValue s = readMaybe (C.unpack s)

readWithDefault :: (HasCallStack, Read a) => a -> C.ByteString -> a
readWithDefault v s = fromMaybe v (readMaybe (C.unpack s))
