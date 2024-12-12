{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
module Data.DataFrame.Util where

import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as VM
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Data.Array ( Ix(range), Array, (!), array )
import Data.List (transpose, intercalate, groupBy, sortBy)
import Data.Maybe
import Data.Text.Read
import Data.Typeable (cast)
import GHC.Stack (HasCallStack)
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
fillCenter c n s = T.replicate l (T.singleton c) `T.append` s `T.append` T.replicate r (T.singleton c)
    where x = n - T.length s
          l = x `div` 2
          r = x - l

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
        separator = T.intercalate "-|-" [T.replicate width (T.singleton '-') | width <- widths]
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
red s = "\ESC[31m" ++ s ++ "\ESC[0m"
green :: String -> String
green s= "\ESC[32m" ++ s ++ "\ESC[0m"
brightGreen :: String -> String
brightGreen s = "\ESC[92m" ++ s ++ "\ESC[0m"
brightBlue :: String -> String
brightBlue s = "\ESC[94m" ++ s ++ "\ESC[0m"

columnNotFound :: T.Text -> T.Text -> [T.Text] -> String
columnNotFound name callPoint columns = red "\n\n[ERROR] " ++
        "Column not found: " ++ T.unpack name ++ " for operation " ++
        T.unpack callPoint ++ "\n\tDid you mean " ++
        T.unpack (guessColumnName name columns) ++ "?\n\n"


typeMismatchError :: Type.Reflection.TypeRep a
                  -> Type.Reflection.TypeRep b
                  -> String
typeMismatchError givenType expectedType = red
        $ red "\n\n[Error]: Type Mismatch" ++
        "\n\tWhile running your code I tried to " ++
        "get a column of type: " ++ green (show givenType) ++
        " but column was of type: " ++ red (show expectedType)

addCallPointInfo :: T.Text -> Maybe T.Text -> String -> String
addCallPointInfo name (Just cp) err = err ++ ("\n\tThis happened when calling function " ++
                                              brightGreen (T.unpack cp) ++ " on the column " ++
                                              brightGreen (T.unpack name) ++ "\n\n" ++
                                              typeAnnotationSuggestion (T.unpack cp))
addCallPointInfo name Nothing err = err ++ ("\n\tOn the column " ++ T.unpack name ++ "\n\n" ++
                                            typeAnnotationSuggestion "<function>")

typeAnnotationSuggestion :: String -> String
typeAnnotationSuggestion cp = "\n\n\tTry adding a type at the end of the function e.g " ++
                              "change\n\t\t" ++ red (cp ++ " arg1 arg2") ++ " to \n\t\t" ++
                              green ("(" ++ cp ++ " arg1 arg2 :: <Type>)") ++ "\n\tor add " ++
                              "{-# LANGUAGE TypeApplications #-} to the top of your " ++
                              "file then change the call to \n\t\t" ++
                              brightGreen (cp ++ " @<Type> arg1 arg2") 

guessColumnName :: T.Text -> [T.Text] -> T.Text
guessColumnName userInput columns = case map (\k -> (editDistance userInput k, k)) columns of
    []   -> ""
    res  -> (snd . minimum) res

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

getIndices :: [Int] -> V.Vector a -> V.Vector a
getIndices indices xs = runST $ do
    xs' <- VM.new (length indices)
    foldM_ (\acc index -> case xs V.!? index of
                            Just v -> VM.write xs' acc v >> return (acc + 1)
                            Nothing -> error "A column has less entries than other rows")
                            0 indices
    V.freeze xs'

getIndicesUnboxed :: VU.Unbox a => [Int] -> VU.Vector a -> VU.Vector a
getIndicesUnboxed indices xs = runST $ do
    xs' <- VUM.new (length indices)
    foldM_ (\acc index -> case xs VU.!? index of
                            Just v -> VUM.write xs' acc v >> return (acc + 1)
                            Nothing -> error "A column has less entries than other rows")
                            0 indices
    VU.freeze xs'

appendWithFrontMin :: (Ord a) => a -> [a] -> [a]
appendWithFrontMin x []     = [x]
appendWithFrontMin x (y:ys) = if x < y then x:y:ys else y:x:ys

readValue :: (HasCallStack, Read a) => T.Text -> a
readValue s = case readMaybe (T.unpack s) of
    Nothing    -> error $ "Could not read value: " ++ T.unpack s
    Just value -> value

readInteger :: HasCallStack => T.Text -> Maybe Integer
readInteger s = case signed decimal (T.strip s) of
    Left _    -> Nothing
    Right (value, _) -> Just value

readInt :: HasCallStack => T.Text -> Maybe Int
readInt s = case signed decimal (T.strip s) of
    Left _ -> Nothing
    Right (value, "") -> Just value
    Right (value, _) -> Nothing

readDouble :: HasCallStack => T.Text -> Maybe Double
readDouble s =
    case signed double s of
        Left _ -> Nothing
        Right (value, _) -> Just value

safeReadValue :: (Read a) => T.Text -> Maybe a
safeReadValue s = readMaybe (T.unpack s)

readWithDefault :: (HasCallStack, Read a) => a -> T.Text -> a
readWithDefault v s = fromMaybe v (readMaybe (T.unpack s))
