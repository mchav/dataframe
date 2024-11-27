{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE Strict #-}

module Data.DataFrame.IO (
    readSeparated,
    readCsv,
    readTsv,
    splitIgnoring,
    readValue,
    safeReadValue,
    readWithDefault) where

import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import qualified Data.Map as M
import qualified Data.Vector as V

import Data.DataFrame.Internal ( empty, DataFrame )
import Data.DataFrame.Operations (addColumn)
import Data.List (transpose, foldl')
import Data.Maybe
import GHC.Stack (HasCallStack)
import Text.Read (readMaybe)

readCsv :: String -> IO DataFrame
readCsv = readSeparated ','

readTsv :: String -> IO DataFrame
readTsv = readSeparated '\t'

readSeparated :: Char -> String -> IO DataFrame
readSeparated c path = do
    contents <- TIO.readFile path
    let all = map (split c) (T.lines contents)
    let columnNames = head all
    let rows = transpose (tail all)
    let columnar = zip columnNames rows
    return $ foldl' (\df (name, vals) -> addColumn name (V.fromList vals) df) empty columnar

split :: Char -> T.Text -> [T.Text]
split c = splitIgnoring c '\"'

splitIgnoring :: Char -> Char -> T.Text -> [T.Text]
splitIgnoring c o s = splitIgnoring' c o s False ""

splitIgnoring' :: Char -> Char -> T.Text -> Bool -> T.Text -> [T.Text]
splitIgnoring' c o s inIgnore acc
    | T.empty == s                                        = [acc]
    | T.head s == o                                       = splitIgnoring' c o (T.tail s) (not inIgnore) (acc `T.append` T.singleton (T.head s))
    | (T.head s == c || T.head s == '\r') && not inIgnore = acc : splitIgnoring' c o (T.tail s) inIgnore ""
    | otherwise                                           = splitIgnoring' c o (T.tail s) inIgnore (acc `T.append` T.singleton (T.head s))

readValue :: (HasCallStack, Read a) => T.Text -> a
readValue s = case readMaybe (T.unpack s) of
    Nothing    -> error $ "Could not read value: " ++ T.unpack s
    Just value -> value

safeReadValue :: (Read a) => T.Text -> Maybe a
safeReadValue s = readMaybe (T.unpack s)

readWithDefault :: (HasCallStack, Read a) => a -> T.Text -> a
readWithDefault v s = fromMaybe v (readMaybe (T.unpack s))
