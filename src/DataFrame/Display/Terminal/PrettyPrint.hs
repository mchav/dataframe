{-# LANGUAGE OverloadedStrings #-}
module DataFrame.Display.Terminal.PrettyPrint where

import qualified Data.Text as T

import Data.List (transpose)

-- Utility functions to show a DataFrame as a Markdown-ish table.

-- Adapted from: https://stackoverflow.com/questions/5929377/format-list-output-in-haskell
-- a type for fill functions
type Filler = Int -> T.Text -> T.Text

-- a type for describing table columns
data ColDesc t = ColDesc
  { colTitleFill :: Filler,
    colTitle :: T.Text,
    colValueFill :: Filler
  }

-- functions that fill a string (s) to a given width (n) by adding pad
-- character (c) to align left, right, or center
fillLeft :: Char -> Int -> T.Text -> T.Text
fillLeft c n s = s `T.append` T.replicate (n - T.length s) (T.singleton c)

fillRight :: Char -> Int -> T.Text -> T.Text
fillRight c n s = T.replicate (n - T.length s) (T.singleton c) `T.append` s

fillCenter :: Char -> Int -> T.Text -> T.Text
fillCenter c n s = T.replicate l (T.singleton c) `T.append` s `T.append` T.replicate r (T.singleton c)
  where
    x = n - T.length s
    l = x `div` 2
    r = x - l

-- functions that fill with spaces
left :: Int -> T.Text -> T.Text
left = fillLeft ' '

right :: Int -> T.Text -> T.Text
right = fillRight ' '

center :: Int -> T.Text -> T.Text
center = fillCenter ' '

showTable :: [T.Text] -> [T.Text] -> [[T.Text]] -> T.Text
showTable header types rows =
  let cs = map (\h -> ColDesc center h left) header
      widths = [maximum $ map T.length col | col <- transpose $ header : types : rows]
      border = T.intercalate "---" [T.replicate width (T.singleton '-') | width <- widths]
      separator = T.intercalate "-|-" [T.replicate width (T.singleton '-') | width <- widths]
      fillCols fill cols = T.intercalate " | " [fill c width col | (c, width, col) <- zip3 cs widths cols]
   in T.unlines $ border : fillCols colTitleFill header : separator : fillCols colTitleFill types : separator : map (fillCols colValueFill) rows

showTableProperMarkdown :: [T.Text] -> [T.Text] -> [[T.Text]] -> T.Text
showTableProperMarkdown header types rows =
  let headerWithTypes = zipWith (\h t -> h <> "<br>" <> t) header types
      cs = map (\h -> ColDesc center h left) headerWithTypes
      widths = [maximum $ map T.length col | col <- transpose $ headerWithTypes : rows]
      border = T.intercalate "---" [T.replicate width (T.singleton '-') | width <- widths]
      separator = T.intercalate "-|-" [T.replicate width (T.singleton '-') | width <- widths]
      fillCols fill cols = T.intercalate " | " [fill c width col | (c, width, col) <- zip3 cs widths cols]
   in T.unlines $ border : fillCols colTitleFill headerWithTypes : separator : map (fillCols colValueFill) rows
