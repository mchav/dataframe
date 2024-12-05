{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
module Data.DataFrame.Util where

import qualified Data.ByteString.Char8 as C
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as VM

import Data.Array ( Ix(range), Array, (!), array )
import Data.List (transpose, intercalate, groupBy, sortBy)
import Data.Typeable (cast)
import Text.Read ( readMaybe )
import Type.Reflection ( TypeRep )
import Control.Monad.ST (runST)
import Control.Monad (foldM_)

-- Apply a function to the second value of a tuple.
applySnd :: (b -> c) -> (a, b) -> (a, c)
applySnd f (x, y) = (x, f y)

-- Utility functions to show a DataFrame as a Markdown table.

-- Adapted from: https://stackoverflow.com/questions/5929377/format-list-output-in-haskell
-- a type for fill functions
type Filler = Int -> C.ByteString -> C.ByteString

-- a type for describing table columns
data ColDesc t = ColDesc { colTitleFill :: Filler
                         , colTitle     :: C.ByteString
                         , colValueFill :: Filler
                         }

-- functions that fill a string (s) to a given width (n) by adding pad
-- character (c) to align left, right, or center
fillLeft :: Char -> Int -> C.ByteString -> C.ByteString
fillLeft c n s = s `C.append` C.replicate (n - C.length s) c

fillRight :: Char -> Int -> C.ByteString -> C.ByteString
fillRight c n s = C.replicate (n - C.length s) c `C.append` s

fillCenter :: Char -> Int -> C.ByteString -> C.ByteString
fillCenter c n s = C.replicate l c `C.append` s `C.append` C.replicate r c
    where x = n - C.length s
          l = x `div` 2
          r = x - l

-- functions that fill with spaces
left :: Int -> C.ByteString -> C.ByteString
left = fillLeft ' '

right :: Int -> C.ByteString -> C.ByteString
right = fillRight ' '

center :: Int -> C.ByteString -> C.ByteString
center = fillCenter ' '

showTable :: [C.ByteString] -> [[C.ByteString]] -> C.ByteString
showTable header rows =
    let
        cs = map (\h -> ColDesc center h left) header
        widths = [maximum $ map C.length col | col <- transpose $ header : rows]
        separator = C.intercalate "-|-" [C.replicate width '-' | width <- widths]
        fillCols fill cols = C.intercalate " | " [fill c width col | (c, width, col) <- zip3 cs widths cols]
    in
        C.unlines $ fillCols colTitleFill header : separator : map (fillCols colValueFill) rows

headOr :: a -> [a] -> a
headOr v []     = v
headOr _ (x:xs) = x

editDistance :: C.ByteString -> C.ByteString -> Int
editDistance xs ys = table ! (m,n)
    where
    (m,n) = (C.length xs, C.length ys)
    x     = array (1,m) (zip [1..] (C.unpack xs))
    y     = array (1,n) (zip [1..] (C.unpack ys))

    table :: Array (Int,Int) Int
    table = array bnds [(ij, dist ij) | ij <- range bnds]
    bnds  = ((0,0),(m,n))

    dist (0,j) = j
    dist (i,0) = i
    dist (i,j) = minimum [table ! (i-1,j) + 1, table ! (i,j-1) + 1,
        if x ! i == y ! j then table ! (i-1,j-1) else 1 + table ! (i-1,j-1)]


-- terminal color functions
red :: String -> String
red s = "\ESC[31m" ++ s ++ "\ESC[0m"
green :: String -> String
green s= "\ESC[32m" ++ s ++ "\ESC[0m"
brightGreen :: String -> String
brightGreen s = "\ESC[92m" ++ s ++ "\ESC[0m"

columnNotFound :: C.ByteString -> C.ByteString -> [C.ByteString] -> String
columnNotFound name callPoint columns = red "\n\n[ERROR] " ++
        "Column not found: " ++ C.unpack name ++ " for operation " ++
        C.unpack callPoint ++ "\n\tDid you mean " ++
        C.unpack (guessColumnName name columns) ++ "?\n\n"


typeMismatchError :: Type.Reflection.TypeRep a
                  -> Type.Reflection.TypeRep b
                  -> String
typeMismatchError givenType expectedType = red
        $ red "\n\n[Error]: Type Mismatch" ++ "\n\tWhile running your code I tried to "
        ++ "get a column of type: " ++ green (show givenType) ++
        " but column was of type: " ++ red (show expectedType)

addCallPointInfo :: C.ByteString -> Maybe C.ByteString -> String -> String
addCallPointInfo name (Just cp) err = err ++ ("\n\tThis happened when calling function " ++
                                              brightGreen (C.unpack cp) ++ " on the column " ++
                                              brightGreen (C.unpack name) ++ "\n\n" ++
                                              typeAnnotationSuggestion (C.unpack cp))
addCallPointInfo name Nothing err = err ++ ("\n\tOn the column " ++ C.unpack name ++ "\n\n" ++
                                            typeAnnotationSuggestion "<function>")

typeAnnotationSuggestion :: String -> String
typeAnnotationSuggestion cp = "\n\n\tTry adding a type at the end of the function e.g " ++
                              "change\n\t\t" ++ red (cp ++ " arg1 arg2") ++ " to \n\t\t" ++
                              green ("(" ++ cp ++ " arg1 arg2 :: <Type>)") ++ "\n\tor add " ++
                              "{-# LANGUAGE TypeApplications #-} to the top of your " ++
                              "file then change the call to \n\t\t" ++
                              brightGreen (cp ++ " @<Type> arg1 arg2") 

guessColumnName :: C.ByteString -> [C.ByteString] -> C.ByteString
guessColumnName userInput columns = case map (\k -> (editDistance userInput k, k)) columns of
    []   -> ""
    res  -> (snd . minimum) res

inferType :: V.Vector C.ByteString -> C.ByteString
inferType xs
    | xs == V.empty = "Unknown"
    | otherwise = case readMaybe @Int $ C.unpack $ V.head xs of
                        Just _ -> "Int"
                        Nothing -> case readMaybe @Integer $ C.unpack $ V.head xs of
                                        Just _ -> "Integer"
                                        Nothing -> case readMaybe @Double $ C.unpack $ V.head xs of
                                                        Just _ -> "Double"
                                                        Nothing -> case readMaybe @Bool $ C.unpack $ V.head xs of
                                                                        Just _  -> "Bool"
                                                                        Nothing -> "Text"

getIndices :: [Int] -> V.Vector a -> V.Vector a
getIndices indices xs = runST $ do
    xs' <- VM.new (length indices)
    foldM_ (\acc index -> case xs V.!? index of
                            Just v -> VM.write xs' acc v >> return (acc + 1)
                            Nothing -> error "A column has less entries than other rows")
                            0 indices
    V.freeze xs'

appendWithFrontMin :: (Ord a) => a -> [a] -> [a]
appendWithFrontMin x []     = [x]
appendWithFrontMin x (y:ys) = if x < y then x:y:ys else y:x:ys
