{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Main where

import Data.List (delete)
import qualified Data.DataFrame as D
import qualified Data.ByteString.Char8 as C
import qualified Data.Vector as V

import Data.DataFrame.Operations (dimensions)
import Data.Maybe (fromMaybe, isNothing, isJust)
import Data.Function

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

mean :: V.Vector Double -> Double
mean xs = V.sum xs / fromIntegral (V.length xs)

oneBillingRowChallenge :: IO ()
oneBillingRowChallenge = do
    rawFrame <- D.readSeparated ';' "./data/measurements.txt"
    let parsed = D.apply "Measurement" D.readDouble rawFrame
    print $ parsed
          & D.groupBy ["City"]
          & D.reduceBy "Measurement" (\v -> (V.minimum v, mean v, V.maximum v))

housing :: IO ()
housing = do
    rawFrame <- D.readCsv "./data/housing.csv"
    mapM_ print $ D.info rawFrame

    -- Convert every column except "ocean_proximity" to doubles
    let doubleColumns = "ocean_proximity" `delete` D.columnNames rawFrame
    let parsed = foldr (`D.apply` D.safeReadValue @Double) rawFrame doubleColumns

    -- Sample.
    mapM_ (print . (\name -> (name, D.columnSize name parsed))) (D.columnNames parsed)

    print (D.valueCounts @C.ByteString "ocean_proximity" parsed)

covid :: IO ()
covid = do
    rawFrame <- D.readCsv "./data/effects-of-covid-19-on-trade-at-15-december-2021-provisional.csv"
    mapM_ print $ D.info rawFrame
    print $ dimensions rawFrame
    print $ D.take 10 rawFrame
    -- value of all exports from 2015
    let parsed = rawFrame
               & D.apply "Year" D.readInt
               & D.apply "Value" D.readInteger
    print $ parsed
          & D.filter "Direction" ((==) @C.ByteString "Exports")
          & D.select ["Direction", "Year", "Country", "Value"]
          & D.groupBy ["Direction", "Year", "Country"]
          & D.reduceBy @Integer "Value" V.sum

chipotle :: IO ()
chipotle = do
    rawFrame <- D.readTsv "./data/chipotle.tsv"
    -- Information on non-null values and inferred data types
    mapM_ print $ D.info rawFrame

    print $ D.dimensions rawFrame

    -- -- Sampling the dataframe
    print $ D.take 5 rawFrame

    -- Transform the data from a raw string into
    -- respective types (throws error on failure)
    let f = rawFrame
          & D.apply "quantity" D.readInteger
          & D.apply "order_id" D.readInteger
          -- Change a specfic order ID
          & D.applyWhere "order_id" ((==) @Integer 1) "quantity" ((+) @Integer 2)
          -- Index based change.
          & D.applyAtIndex 0 "quantity" (flip ((-) @Integer) 2)
          -- drop dollar sign and parse price as double
          & D.apply "item_price" (D.readValue @Double . C.drop 1)
          -- Custom parsing 
          & D.apply "choice_description" toIngredientList

    -- sample the dataframe.
    print $ D.take 10 f

    -- Create a total_price column that is quantity * item_price
    let multiply (a :: Integer) (b :: Double) = fromIntegral a * b
    let withTotalPrice = D.addColumn "total_price"
                                     (D.combine "quantity" "item_price" multiply f) f

    -- sample a filtered subset of the dataframe
    putStrLn "Sample dataframe"
    print $ withTotalPrice 
          & D.select ["quantity", "item_name", "item_price", "total_price"]
          & D.filter "total_price" ((<) @Double 100)
          & D.take 10 

    -- Check how many chicken burritos were ordered.
    -- There are two ways to checking how many chicken burritos
    -- were ordered.
    let searchTerm = "Chicken Burrito" :: C.ByteString

    -- 1) Using sumWhere
    print (D.sumWhere "item_name"
                      (searchTerm ==)
                      "quantity" f :: Integer)

    -- 2) Using select + reduce
    print $ f
          & D.select ["item_name", "quantity"]
          -- It's more efficient to filter before grouping.
          & D.filter "item_name" (searchTerm ==)
          & D.groupBy ["item_name"]
          & D.reduceBy @Integer "quantity" V.sum

    -- Similarly, we can aggregate quantities by all rows.
    print $ f
          & D.select ["item_name", "quantity"]
          & D.groupBy ["item_name"]
          & D.reduceBy @Integer "quantity" V.sum

    let firstOrder = withTotalPrice
                   & D.filter "choice_description" (any (C.isInfixOf "Guacamole"). fromMaybe [])
                   & D.filter "item_name" (("Chicken Bowl" :: C.ByteString) ==)

    print $ D.sum @Integer "quantity" firstOrder
    print $ D.sum @Double "item_price" firstOrder
    print $ D.take 10 firstOrder

-- An example of a parsing function.
toIngredientList :: C.ByteString -> Maybe [C.ByteString]
toIngredientList v
    | v == ""                 = Just []
    | v == "NULL"             = Nothing
    | C.isPrefixOf "[" v      = toIngredientList $ C.init (C.tail v)
    | not (C.isInfixOf "," v) = Just [v]
    | otherwise = foldl (\a b -> (++) <$> a <*> b) (Just []) (map (toIngredientList . C.strip) (D.splitIgnoring ',' '[' v))
