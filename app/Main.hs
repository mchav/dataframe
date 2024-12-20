{-# LANGUAGE ExtendedDefaultRules #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Main where

import qualified Data.DataFrame as D
import Data.DataFrame.Operations (dimensions)
import Data.Function ((&))
import Data.List (delete)
import Data.Maybe (fromMaybe, isJust, isNothing)
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU

-- Numbers default to int and strings to text
default (Int, T.Text)

-- Example usage of DataFrame library

main :: IO ()
main = do
  putStrLn "Housing"
  housing
  putStrLn $ replicate 100 '-'

  putStrLn "Chipotle Data"
  chipotle
  putStrLn $ replicate 100 '-'

  putStrLn "One Billion Row Challenge"
  oneBillingRowChallenge
  putStrLn $ replicate 100 '-'

  putStrLn "Covid Data"
  covid
  putStrLn $ replicate 100 '-'


mean :: (Fractional a, VG.Vector v a) => v a -> a
mean xs = VG.sum xs / fromIntegral (VG.length xs)

oneBillingRowChallenge :: IO ()
oneBillingRowChallenge = do
  parsed <- D.readSeparated ';' D.defaultOptions "./data/measurements.txt"
  print $
    parsed
      & D.groupBy ["City"]
      & D.reduceBy "Measurement" (\v -> (VG.minimum v, mean @Double v, VG.maximum v))

housing :: IO ()
housing = do
  parsed <- D.readCsv "./data/housing.csv"

  -- Sample.
  mapM_ (print . (\name -> (name, D.columnSize name parsed))) (D.columnNames parsed)

  print $ D.take 5 parsed

  D.plotHistograms D.VerticalHistogram parsed

covid :: IO ()
covid = do
  rawFrame <- D.readCsv "./data/effects-of-covid-19-on-trade-at-15-december-2021-provisional.csv"
  print $ dimensions rawFrame
  print $ D.take 10 rawFrame

  D.plotHistograms D.VerticalHistogram rawFrame

  -- value of all exports from 2015
  print $
    rawFrame
      & D.filter "Direction" (== "Exports")
      & D.select ["Direction", "Year", "Country", "Value"]
      & D.groupBy ["Direction", "Year", "Country"]
      & D.reduceBy "Value" VG.sum

chipotle :: IO ()
chipotle = do
  rawFrame <- D.readTsv "./data/chipotle.tsv"
  print $ D.dimensions rawFrame

  -- -- Sampling the dataframe
  print $ D.take 5 rawFrame

  -- Transform the data from a raw string into
  -- respective types (throws error on failure)
  let f =
        rawFrame
          -- Change a specfic order ID
          & D.applyWhere "order_id" (== 1) "quantity" (+ 2)
          -- Index based change.
          & D.applyAtIndex 0 "quantity" (flip (-) 2)
          -- drop dollar sign and parse price as double
          & D.apply "item_price" (D.readValue @Double . T.drop 1)
          -- Custom parsing
          & D.apply "choice_description" toIngredientList

  -- sample the dataframe.
  print $ D.take 10 f

  -- Create a total_price column that is quantity * item_price
  let multiply (a :: Int) (b :: Double) = fromIntegral a * b
  let withTotalPrice = D.combine "total_price" multiply "quantity" "item_price" f

  -- sample a filtered subset of the dataframe
  putStrLn "Sample dataframe"
  print $
    withTotalPrice
      & D.select ["quantity", "item_name", "item_price", "total_price"]
      & D.filter "total_price" ((<) @Double 100)
      & D.take 10

  -- Check how many chicken burritos were ordered.
  -- There are two ways to checking how many chicken burritos
  -- were ordered.
  let searchTerm = "Chicken Burrito" :: T.Text

  print $
    f
      & D.select ["item_name", "quantity"]
      -- It's more efficient to filter before grouping.
      & D.filter "item_name" (searchTerm ==)
      & D.groupBy ["item_name"]
      & D.reduceBy "quantity" VG.sum
      & D.sortBy "quantity" D.Descending

  -- Similarly, we can aggregate quantities by all rows.
  print $
    f
      & D.select ["item_name", "quantity"]
      & D.groupBy ["item_name"]
      & D.reduceBy "quantity" VG.sum
      & D.sortBy "quantity" D.Descending
      & D.take 10

  let firstOrder =
        withTotalPrice
          & D.filter "choice_description" (any (T.isInfixOf "Guacamole") . fromMaybe [])
          & D.filter "item_name" (("Chicken Bowl" :: T.Text) ==)

  print $ D.take 10 firstOrder

-- An example of a parsing function.
toIngredientList :: Maybe T.Text -> Maybe [T.Text]
toIngredientList Nothing = Nothing
toIngredientList (Just v)
  | v == "" = Just []
  | v == "NULL" = Nothing
  | T.isPrefixOf "[" v = toIngredientList $ Just $ T.init (T.tail v)
  | not (T.isInfixOf "," v) = Just [v]
  | otherwise = foldl (\a b -> (++) <$> a <*> b) (Just []) (map (toIngredientList . Just . T.strip) (D.splitIgnoring ',' '[' v))
