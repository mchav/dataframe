{-# LANGUAGE ExtendedDefaultRules #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TupleSections #-}

module Main where

import qualified DataFrame as D
import qualified DataFrame.Functions as F
import DataFrame (dimensions, (|>))
import Data.List (delete)
import Data.Maybe (fromMaybe, isJust, isNothing)
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU

-- Numbers default to int and double, and strings to text
default (Int, T.Text, Double)

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

oneBillingRowChallenge :: IO ()
oneBillingRowChallenge = do
  parsed <- D.readSeparated ';' D.defaultOptions "./data/measurements.txt"
  print $
    parsed
      |> D.groupBy ["City"]
      |> D.aggregate [ (F.minimum "Measurement") `F.as` "minimum"
                     , (F.mean "Measurement")    `F.as` "mean"
                     , (F.maximum "Measurement") `F.as` "maximum"]
      |> D.sortBy D.Ascending ["City"]

housing :: IO ()
housing = do
  parsed <- D.readCsv "./data/housing.csv"

  print $ D.columnInfo parsed

  -- Sample.
  print $ D.take 5 parsed

  D.plotHistograms D.PlotAll D.VerticalHistogram parsed

covid :: IO ()
covid = do
  rawFrame <- D.readCsv "./data/effects-of-covid-19-on-trade-at-15-december-2021-provisional.csv"
  print $ dimensions rawFrame
  print $ D.take 10 rawFrame

  D.plotHistograms D.PlotAll D.VerticalHistogram rawFrame

  -- value of all exports from 2015
  print $
    rawFrame
      |> D.filter "Direction" (== "Exports")
      |> D.select ["Direction", "Year", "Country", "Value"]
      |> D.groupBy ["Direction", "Year", "Country"]
      |> D.aggregate [(F.sum "Value") `F.as` "TotalValue"]

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
          |> D.applyWhere (== 1) "order_id" (+ 2) "quantity"
          -- Index based change.
          |> D.applyAtIndex 0 (\n -> n - 2) "quantity"
          -- Custom parsing: drop dollar sign and parse price as double
          |> D.apply (D.readValue @Double . T.drop 1) "item_price"

  -- sample the dataframe.
  print $ D.take 10 f

  -- Create a total_price column that is quantity * item_price
  let withTotalPrice = D.derive "total_price" (F.lift fromIntegral (F.col @Int "quantity") * F.col @Double"item_price") f

  -- sample a filtered subset of the dataframe
  putStrLn "Sample dataframe"
  print $
    withTotalPrice
      |> D.select ["quantity", "item_name", "item_price", "total_price"]
      |> D.filter "total_price" (100.0 <)
      |> D.take 10

  -- Check how many chicken burritos were ordered.
  -- There are two ways to checking how many chicken burritos
  -- were ordered.
  let searchTerm = "Chicken Burrito" :: T.Text

  print $
    f
      |> D.select ["item_name", "quantity"]
      -- It's more efficient to filter before grouping.
      |> D.filter "item_name" (searchTerm ==)
      |> D.groupBy ["item_name"]
      |> D.aggregate [ (F.sum "quantity")     `F.as` "sum"
                     , (F.maximum "quantity") `F.as` "max"
                     , (F.mean "quantity")    `F.as` "mean"]
      |> D.sortBy D.Descending ["sum"]

  -- Similarly, we can aggregate quantities by all rows.
  print $
    f
      |> D.select ["item_name", "quantity"]
      |> D.groupBy ["item_name"]
      |> D.aggregate [ (F.sum "quantity")     `F.as` "sum"
                     , (F.maximum "quantity") `F.as` "maximum"
                     , (F.mean "quantity")    `F.as` "mean"]
      |> D.take 10

  let firstOrder =
        withTotalPrice
          |> D.filterBy (maybe False (T.isInfixOf "Guacamole")) "choice_description"
          |> D.filterBy (("Chicken Bowl" :: T.Text) ==) "item_name"

  print $ D.take 10 firstOrder

