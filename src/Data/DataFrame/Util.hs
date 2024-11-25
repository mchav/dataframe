module Data.DataFrame.Util where

import Data.Array ( Ix(range), Array, (!), array )
import Data.List (transpose, intercalate, groupBy, sortBy)

-- Apply a function to the second value of a tuple.
applySnd :: (b -> c) -> (a, b) -> (a, c)
applySnd f (x, y) = (x, f y)

-- Utility functions to show a DataFrame as a Markdown table.

-- a type for fill functions
type Filler = Int -> String -> String

-- a type for describing table columns
data ColDesc t = ColDesc { colTitleFill :: Filler
                         , colTitle     :: String
                         , colValueFill :: Filler
                         }

-- functions that fill a string (s) to a given width (n) by adding pad
-- character (c) to align left, right, or center
fillLeft :: a -> Int -> [a] -> [a]
fillLeft c n s = s ++ replicate (n - length s) c

fillRight :: a -> Int -> [a] -> [a]
fillRight c n s = replicate (n - length s) c ++ s

fillCenter :: a -> Int -> [a] -> [a]
fillCenter c n s = replicate l c ++ s ++ replicate r c
    where x = n - length s
          l = x `div` 2
          r = x - l

-- functions that fill with spaces
left :: Int -> [Char] -> [Char]
left = fillLeft ' '

right :: Int -> [Char] -> [Char]
right = fillRight ' '

center :: Int -> [Char] -> [Char]
center = fillCenter ' '

showTable :: [String] -> [[String]] -> String
showTable header rows =
    let 
        cs = map (\h -> ColDesc center h left) header
        widths = [maximum $ map length col | col <- transpose $ header : rows]
        separator = intercalate "-+-" [replicate width '-' | width <- widths]
        fillCols fill cols = intercalate " | " [fill c width col | (c, width, col) <- zip3 cs widths cols]
    in
        unlines $ fillCols colTitleFill header : separator : map (fillCols colValueFill) rows

headOr :: a -> [a] -> a
headOr v []     = v
headOr _ (x:xs) = x

editDistance :: Eq a => [a] -> [a] -> Int
editDistance xs ys = table ! (m,n)
    where
    (m,n) = (length xs, length ys)
    x     = array (1,m) (zip [1..] xs)
    y     = array (1,n) (zip [1..] ys)
    
    table :: Array (Int,Int) Int
    table = array bnds [(ij, dist ij) | ij <- range bnds]
    bnds  = ((0,0),(m,n))
    
    dist (0,j) = j
    dist (i,0) = i
    dist (i,j) = minimum [table ! (i-1,j) + 1, table ! (i,j-1) + 1,
        if x ! i == y ! j then table ! (i-1,j-1) else 1 + table ! (i-1,j-1)]