module Data.DataFrame.IO (readCsv) where

import Data.DataFrame.Internal
import Data.DataFrame.Operations (addColumn)
import Data.List (transpose)

readCsv :: String -> IO DataFrame
readCsv path = do
    contents <- readFile path
    let all = map (splitOn (=='\t')) (lines contents)
    let columnNames = head all
    let rows = transpose (tail all)
    return $ foldr (\(name, vals) d -> addColumn name vals d) empty (zip columnNames rows)

splitOn :: (Char -> Bool) -> String -> [String]
splitOn p s = case dropWhile p s of
                "" -> []
                s' -> w : splitOn p s''
                    where (w, s'') = break p s'
