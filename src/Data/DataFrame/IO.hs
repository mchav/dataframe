{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE Strict #-}

module Data.DataFrame.IO (readCsv, readTsv) where

import Data.DataFrame.Internal ( empty, DataFrame )
import Data.DataFrame.Operations (addColumn)
import Data.List (transpose)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import qualified Data.Map as M

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
    return $ foldr (\(name, vals) df -> addColumn name vals df) empty columnar

split :: Char -> T.Text -> [T.Text]
split c s = split' c s False ""

split' :: Char -> T.Text -> Bool -> T.Text -> [T.Text]
split' c s inQuote acc
    | T.empty == s                                       = [acc]
    | T.head s == '\"'                                   = split' c (T.tail s) (not inQuote) (acc `T.append` T.singleton (T.head s))
    | (T.head s == c || T.head s == '\r') && not inQuote = acc : split' c (T.tail s) inQuote ""
    | otherwise                                          = split' c (T.tail s) inQuote (acc `T.append` T.singleton (T.head s))
