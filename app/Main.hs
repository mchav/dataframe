{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}
module Main where

import Data.List (delete)
import qualified Data.DataFrame as D
import qualified Data.Text as T
import qualified Data.Vector as V
import Data.DataFrame.Operations (dimensions)
import Data.Maybe (fromMaybe, isNothing, isJust)

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
    let parsed = D.apply "Measurement" (D.readValue @Double) rawFrame
    print $ D.reduceBy "Measurement" (\v -> (V.minimum v, mean v, V.maximum v))
          . D.groupBy "City"
          $ parsed

housing :: IO ()
housing = do
    rawFrame <- D.readCsv "./data/housing.csv"
    mapM_ print $ D.info rawFrame

    -- Convert every column except "ocean_proximity" to doubles
    let doubleColumns = "ocean_proximity" `delete` D.columnNames rawFrame
    let parsed = foldr (`D.apply` D.safeReadValue @Double) rawFrame doubleColumns

    -- Sample.

    mapM_ (print . (\name -> (name, D.columnSize name parsed))) (D.columnNames parsed)

    print (D.valueCounts @T.Text "ocean_proximity" parsed)

covid :: IO ()
covid = do
    rawFrame <- D.readCsv "./data/effects-of-covid-19-on-trade-at-15-december-2021-provisional.csv"
    mapM_ print $ D.info rawFrame
    print $ dimensions rawFrame
    print $ D.take 10 rawFrame
    -- value of all exports from 2015
    let parsed = D.apply "Value" (D.readValue @Int)
               . D.apply "Year" (D.readValue @Int)
               $ rawFrame
    let exports2015 = D.filter "Direction" ((==) @T.Text "Exports")
                    . D.filter "Year" ((==) @Int 2015)
                    $ parsed
    print (D.sum @Int "Value" exports2015)

chipotle :: IO ()
chipotle = do
    rawFrame <- D.readTsv "./data/chipotle.tsv"
    -- Information on non-null values and inferred data types
    mapM_ print $ D.info rawFrame
    
    print $ D.dimensions rawFrame

    -- Sampling the dataframe
    print $ D.take 5 rawFrame

    -- Transform the data from a raw string into
    -- respective types (throws error on failure)
    let f = D.apply "choice_description" toIngredientList
          -- drop dollar sign and parse price as double
          . D.apply "item_price" (D.readValue @Double . T.drop 1)
          -- Index based change.
          . D.applyAtIndex 0 "quantity" (flip ((-) @Integer) 2)
          -- Change a specfic order ID
          . D.applyWhere "order_id" ((==) @Integer 1) "quantity" ((+) @Integer 2)
          . D.apply "order_id" (D.readValue @Integer)
          . D.apply "quantity" (D.readValue @Integer)
          $ rawFrame

    -- sample the dataframe.
    print $ D.take 10 f

    -- Create a total_price column that is quantity * item_price
    let totalPrice = V.zipWith (*)
                (V.map fromIntegral $ D.getUnindexedColumn @Integer "quantity" f)
                (D.getUnindexedColumn @Double "item_price" f)
    let withTotalPrice = D.addColumn "total_price" totalPrice f

    -- sample a filtered subset of the dataframe
    putStrLn "Sample dataframe"
    print $ D.take 10
          . D.filter "total_price" ((>) @Double 100)
          . D.select ["quantity", "item_name", "item_price", "total_price"]
          $ withTotalPrice

    -- Check how many chicken burritos were ordered.
    -- There are two ways to checking how many chicken burritos
    -- were ordered.
    let searchTerm = "Chicken Burrito" :: T.Text
    
    -- 1) Using sumWhere
    print (D.sumWhere "item_name"
                      (searchTerm ==)
                      "quantity" f :: Integer)

    -- 2) Using select + reduce
    print $ D.reduceBy @Integer "quantity" V.sum
          . D.groupBy "item_name"
          -- It's more efficient to filter before grouping.
          . D.filter "item_name" (searchTerm ==)
          . D.select ["item_name", "quantity"]
          $ f

    -- Similarly, we can aggregate quantities by all rows.
    print $ D.reduceBy @Integer "quantity" V.sum
          . D.groupBy "item_name"
          . D.select ["item_name", "quantity"]
          $ f

    let firstOrder = D.filter "choice_description" (any (T.isInfixOf "Guacamole"). fromMaybe [])
                   . D.filter "item_name" (("Chicken Bowl" :: T.Text) ==)
                   $ withTotalPrice

    print $ D.sum @Integer "quantity" firstOrder
    print $ D.sum @Double "item_price" firstOrder
    print $ D.take 10 firstOrder

-- An example of a parsing function.
toIngredientList :: T.Text -> Maybe [T.Text]
toIngredientList v
    | v == ""                 = Just []
    | v == "NULL"             = Nothing
    | T.isPrefixOf "[" v      = toIngredientList $ T.init (T.tail v)
    | not (T.isInfixOf "," v) = Just [v]
    | otherwise = foldl (\a b -> (++) <$> a <*> b) (Just []) (map (toIngredientList . T.strip) (D.splitIgnoring ',' '[' v))
