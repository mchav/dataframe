{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Main where

import Data.Maybe
import qualified Data.Text as T
import qualified DataFrame as D
import qualified DataFrame.Functions as F
import Text.Read (readMaybe)

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
                |> D.derive
                    "quantity"
                    ( F.ifThenElse
                        (F.col @Int "order_id" F.== F.lit 1)
                        (F.col @Int "quantity" + F.lit 2)
                        (F.col @Int "quantity")
                    )
                -- Custom parsing: drop dollar sign and parse price as double
                |> D.derive
                    "item_price"
                    (F.lift (readMaybe @Double . T.unpack . T.drop 1) (F.col "item_price"))

    -- sample the dataframe.
    print $ D.take 10 df

    -- Create a total_price column that is quantity * item_price
    let withTotalPrice =
            D.derive
                "total_price"
                ( F.lift2
                    (\l r -> fmap (* l) r)
                    (F.lift fromIntegral (F.col @Int "quantity"))
                    (F.col @(Maybe Double) "item_price")
                )
                df

    -- sample a filtered subset of the dataframe
    putStrLn "Sample dataframe"
    print $
        withTotalPrice
            |> D.select ["quantity", "item_name", "item_price", "total_price"]
            |> D.filterWhere
                (F.lift (fromMaybe False . fmap (> 100)) (F.col @(Maybe Double) "total_price"))
            |> D.take 10

    -- Check how many chicken burritos were ordered.
    -- There are two ways to checking how many chicken burritos
    -- were ordered.
    let searchTerm = "Chicken Burrito" :: T.Text

    print $
        df
            |> D.select ["item_name", "quantity"]
            -- It's more efficient to filter before grouping.
            |> D.filterWhere (F.col "item_name" F.== F.lit searchTerm)
            |> D.groupBy ["item_name"]
            |> D.aggregate
                [ F.sum (F.col @Int "quantity") `F.as` "sum"
                , F.maximum (F.col @Int "quantity") `F.as` "max"
                , F.mean (F.col @Int "quantity") `F.as` "mean"
                ]
            |> D.sortBy D.Descending ["sum"]

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
                |> D.filterWhere
                    ( (F.lift (maybe False (T.isInfixOf "Guacamole")) (F.col "choice_description"))
                        `F.and` (F.col @T.Text "item_name" F.== F.lit "Chicken Bowl")
                    )

    print $ D.take 10 firstOrder
