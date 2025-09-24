{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Main where

import qualified Data.Text as T
import qualified DataFrame as D
import qualified DataFrame.Functions as F

import DataFrame ((|>))

main :: IO ()
main = do
    raw <- D.readTsv "../data/chipotle.tsv"
    print $ D.dimensions raw

    -- -- Sampling the dataframe
    print $ D.take 5 raw

    -- Transform the data from a raw string into
    -- respective types (throws error on failure)
    let df =
            raw
                -- Change a specfic order ID
                |> D.applyWhere (== (1 :: Int)) "order_id" (+ (2 :: Int)) "quantity"
                -- Index based change.
                |> D.applyAtIndex 0 ((\n -> n - 2) :: Int -> Int) "quantity"
                -- Custom parsing: drop dollar sign and parse price as double
                |> D.apply (D.readValue @Double . T.drop 1) "item_price"

    -- sample the dataframe.
    print $ D.take 10 df

    -- Create a total_price column that is quantity * item_price
    let withTotalPrice =
            D.derive
                "total_price"
                (F.lift fromIntegral (F.col @Int "quantity") * F.col @Double "item_price")
                df

    -- sample a filtered subset of the dataframe
    putStrLn "Sample dataframe"
    print $
        withTotalPrice
            |> D.select ["quantity", "item_name", "item_price", "total_price"]
            |> D.filter "total_price" ((100.0 :: Double) <)
            |> D.take 10

    -- Check how many chicken burritos were ordered.
    -- There are two ways to checking how many chicken burritos
    -- were ordered.
    let searchTerm = "Chicken Burrito" :: T.Text

    print $
        df
            |> D.select ["item_name", "quantity"]
            -- It's more efficient to filter before grouping.
            |> D.filter "item_name" (searchTerm ==)
            |> D.groupBy ["item_name"]
            |> D.aggregate
                [ F.sum (F.col @Int "quantity") `F.as` "sum"
                , F.maximum (F.col @Int "quantity") `F.as` "max"
                , F.mean (F.col @Int "quantity") `F.as` "mean"
                ]
            |> D.sortBy D.Descending ["sum"]

    -- Similarly, we can aggregate quantities by all rows.
    print $
        df
            |> D.select ["item_name", "quantity"]
            |> D.groupBy ["item_name"]
            |> D.aggregate
                [ F.sum (F.col @Int "quantity") `F.as` "sum"
                , F.maximum (F.col @Int "quantity") `F.as` "maximum"
                , F.mean (F.col @Int "quantity") `F.as` "mean"
                ]
            |> D.take 10

    let firstOrder =
            withTotalPrice
                |> D.filterBy (maybe False (T.isInfixOf "Guacamole")) "choice_description"
                |> D.filterBy (("Chicken Bowl" :: T.Text) ==) "item_name"

    print $ D.take 10 firstOrder
