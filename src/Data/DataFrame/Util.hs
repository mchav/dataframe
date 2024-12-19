{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Data.DataFrame.Util where

import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as VM
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Monad (foldM_)
import Control.Monad.ST (runST)
import Data.Array (Array, Ix (range), array, (!))
import Data.List (groupBy, intercalate, sortBy, transpose)
import Data.Maybe
import Data.Text.Read
import Data.Typeable (cast)
import GHC.Stack (HasCallStack)
import Text.Read (readMaybe)
import Type.Reflection (TypeRep)

-- Apply a function to the second value of a tuple.
applySnd :: (b -> c) -> (a, b) -> (a, c)
applySnd f (x, y) = (x, f y)

-- Utility functions to show a DataFrame as a Markdown table.

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

headOr :: a -> [a] -> a
headOr v [] = v
headOr _ (x : xs) = x

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
            Just _ -> "Bool"
            Nothing -> "Text"

getIndices :: [Int] -> V.Vector a -> V.Vector a
getIndices indices xs = runST $ do
  xs' <- VM.new (length indices)
  foldM_
    ( \acc index -> case xs V.!? index of
        Just v -> VM.write xs' acc v >> return (acc + 1)
        Nothing -> error "A column has less entries than other rows"
    )
    0
    indices
  V.freeze xs'

getIndicesUnboxed :: (VU.Unbox a) => [Int] -> VU.Vector a -> VU.Vector a
getIndicesUnboxed indices xs = runST $ do
  xs' <- VUM.new (length indices)
  foldM_
    ( \acc index -> case xs VU.!? index of
        Just v -> VUM.write xs' acc v >> return (acc + 1)
        Nothing -> error "A column has less entries than other rows"
    )
    0
    indices
  VU.freeze xs'

appendWithFrontMin :: (Ord a) => a -> [a] -> [a]
appendWithFrontMin x [] = [x]
appendWithFrontMin x (y : ys) = if x < y then x : y : ys else y : x : ys

readValue :: (HasCallStack, Read a) => T.Text -> a
readValue s = case readMaybe (T.unpack s) of
  Nothing -> error $ "Could not read value: " ++ T.unpack s
  Just value -> value

readInteger :: (HasCallStack) => T.Text -> Maybe Integer
readInteger s = case signed decimal (T.strip s) of
  Left _ -> Nothing
  Right (value, "") -> Just value
  Right (value, _) -> Nothing

readInt :: (HasCallStack) => T.Text -> Maybe Int
readInt s = case signed decimal (T.strip s) of
  Left _ -> Nothing
  Right (value, "") -> Just value
  Right (value, _) -> Nothing

readDouble :: (HasCallStack) => T.Text -> Maybe Double
readDouble s =
  case signed double s of
    Left _ -> Nothing
    Right (value, "") -> Just value
    Right (value, _) -> Nothing

safeReadValue :: (Read a) => T.Text -> Maybe a
safeReadValue s = readMaybe (T.unpack s)

readWithDefault :: (HasCallStack, Read a) => a -> T.Text -> a
readWithDefault v s = fromMaybe v (readMaybe (T.unpack s))
