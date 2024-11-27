{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
module Data.DataFrame.Util where

import qualified Data.Text as T
import qualified Data.Vector as V

import Data.Array ( Ix(range), Array, (!), array )
import Data.List (transpose, intercalate, groupBy, sortBy)
import Data.Typeable (cast)
import Text.Read ( readMaybe ) 
import Type.Reflection ( TypeRep )

-- Apply a function to the second value of a tuple.
applySnd :: (b -> c) -> (a, b) -> (a, c)
applySnd f (x, y) = (x, f y)

-- Utility functions to show a DataFrame as a Markdown table.

-- Adapted from: https://stackoverflow.com/questions/5929377/format-list-output-in-haskell
-- a type for fill functions
type Filler = Int -> T.Text -> T.Text

-- a type for describing table columns
data ColDesc t = ColDesc { colTitleFill :: Filler
                         , colTitle     :: T.Text
                         , colValueFill :: Filler
                         }

-- functions that fill a string (s) to a given width (n) by adding pad
-- character (c) to align left, right, or center
fillLeft :: Char -> Int -> T.Text -> T.Text
fillLeft c n s = s `T.append` T.replicate (n - T.length s) (T.singleton c)

fillRight :: Char -> Int -> T.Text -> T.Text
fillRight c n s = T.replicate (n - T.length s) (T.singleton c) `T.append` s

fillCenter :: Char -> Int -> T.Text -> T.Text
fillCenter c n s = T.replicate l c' `T.append` s `T.append` T.replicate r c'
    where x = n - T.length s
          l = x `div` 2
          r = x - l
          c' = T.singleton c

-- functions that fill with spaces
left :: Int -> T.Text -> T.Text
left = fillLeft ' '

right :: Int -> T.Text -> T.Text
right = fillRight ' '

center :: Int -> T.Text -> T.Text
center = fillCenter ' '

showTable :: [T.Text] -> [[T.Text]] -> T.Text
showTable header rows =
    let
        cs = map (\h -> ColDesc center h left) header
        widths = [maximum $ map T.length col | col <- transpose $ header : rows]
        separator = T.intercalate "-|-" [T.replicate width "-" | width <- widths]
        fillCols fill cols = T.intercalate " | " [fill c width col | (c, width, col) <- zip3 cs widths cols]
    in
        T.unlines $ fillCols colTitleFill header : separator : map (fillCols colValueFill) rows

headOr :: a -> [a] -> a
headOr v []     = v
headOr _ (x:xs) = x

editDistance :: T.Text -> T.Text -> Int
editDistance xs ys = table ! (m,n)
    where
    (m,n) = (T.length xs, T.length ys)
    x     = array (1,m) (zip [1..] (T.unpack xs))
    y     = array (1,n) (zip [1..] (T.unpack ys))

    table :: Array (Int,Int) Int
    table = array bnds [(ij, dist ij) | ij <- range bnds]
    bnds  = ((0,0),(m,n))

    dist (0,j) = j
    dist (i,0) = i
    dist (i,j) = minimum [table ! (i-1,j) + 1, table ! (i,j-1) + 1,
        if x ! i == y ! j then table ! (i-1,j-1) else 1 + table ! (i-1,j-1)]


-- terminal color functions
red :: String -> String
red s = "\ESC[31m[" ++ s ++ "\ESC[0m"

columnNotFound :: T.Text -> T.Text -> [T.Text] -> String
columnNotFound name callPoint columns = red "\n\n[ERROR] " ++
        "Column not found: " ++ T.unpack name ++ " for operation " ++
        T.unpack callPoint ++ "\n\tDid you mean " ++
        T.unpack (guessColumnName name columns) ++ "?\n\n"


typeMismatchError :: T.Text
                  -> T.Text
                  -> Type.Reflection.TypeRep a
                  -> Type.Reflection.TypeRep b
                  -> String
typeMismatchError name callPoint givenType expectedType = red
        $ "\n\n[Error] Wrong type specified for column: " ++
            T.unpack name ++ "\n\tTried to get a column of type: " ++
            show givenType ++ " but column was of type: " ++ show expectedType ++
            "\n\tWhen calling function: " ++ T.unpack callPoint ++ "\n\n"

guessColumnName :: T.Text -> [T.Text] -> T.Text
guessColumnName userInput columns = snd
                             $ minimum
                             $ map (\k -> (editDistance userInput k, k))
                             columns

inferType :: V.Vector T.Text -> T.Text
inferType xs
    | xs == V.empty = "Unknown"
    | otherwise = case readMaybe @Int $ T.unpack $ V.head xs of
                        Just _ -> "Int"
                        Nothing -> case readMaybe @Integer $ T.unpack $ V.head xs of
                                        Just _ -> "Integer"
                                        Nothing -> case readMaybe @Double $ T.unpack $ V.head xs of
                                                        Just _ -> "Double"
                                                        Nothing -> case readMaybe @Bool $ T.unpack $ V.head xs of
                                                                        Just _  -> "Bool"
                                                                        Nothing -> "Text"
