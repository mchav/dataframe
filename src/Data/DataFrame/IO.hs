{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE Strict #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE TypeApplications #-}

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
    readWithDefault) where

import qualified Data.ByteString.Char8 as C
import qualified Data.Map as M
import qualified Data.Set as S
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as VM

import Control.Monad.ST ( ST, runST )
import Data.DataFrame.Internal ( empty, DataFrame )
import Data.DataFrame.Operations (addColumn, columnNames, parseDefaults)
import Data.List (transpose, foldl')
import Data.Maybe ( fromMaybe )
import GHC.Stack (HasCallStack)
import Text.Read (readMaybe)
import Data.ByteString.Lex.Fractional ( readDecimal )
import System.IO ( withFile, IOMode(ReadMode) )
import Control.Monad (foldM_, forM_, replicateM_)
import Control.Monad.IO.Class (MonadIO(liftIO))

readCsv :: String -> IO DataFrame
readCsv = readSeparated ','

readTsv :: String -> IO DataFrame
readTsv = readSeparated '\t'

readSeparated :: Char -> String -> IO DataFrame
readSeparated c path = withFile path ReadMode $ \handle -> do
    columnNames <- map C.strip . C.split c <$> C.hGetLine handle
    rs <- C.lines <$> C.hGetContents handle
    let vals = mkColumns c (length columnNames) rs
    let df = foldl' (\df (i, name) -> addColumn name (vals V.! i) df) empty (zip [0..] columnNames)
    return $ parseDefaults df

-- Read CSV into columnar format using mutable 2D Vectors
-- Ugly but saves us ~2GB in memory allocation vs using 
-- a list of lists + transpose 
mkColumns :: Char -> Int -> [C.ByteString] -> V.Vector (V.Vector C.ByteString)
mkColumns c columnLength xs = let
        rowLength = length xs
    in runST $ do
        modifier <- VM.new columnLength :: ST s (VM.MVector s (VM.MVector s C.ByteString))
        forM_ [0..(columnLength - 1)] $ \i ->  do
            v <- VM.new rowLength :: ST s (VM.MVector s C.ByteString)
            VM.write modifier i v
        foldM_ (\rowIndex s -> do
            let rowValues = split c s
            foldM_ (\columnIndex s' -> do
                column <- VM.read modifier columnIndex
                VM.write column rowIndex (C.strip s')
                return (columnIndex + 1)) 0 rowValues
            return (rowIndex + 1)) 0 xs
        res <- VM.new columnLength :: ST s (VM.MVector s (V.Vector C.ByteString))
        VM.imapM_ (\i v -> do
            r <- V.freeze v
            VM.write res i r) modifier
        V.freeze res

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
