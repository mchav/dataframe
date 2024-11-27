# dataframe

An intuitive, dynamically-typed DataFrame library.

Goals:
* Exploratory data analysis
* Shallow learning curve

Non-goals:
* Type safety

Example usage

```haskell
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}
module Main where

import qualified Data.Text as T
import qualified Data.DataFrame as D

main :: IO ()
main = do
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

```

Future work:
* Apache arrow and Parquet compatability
* Integration with plotting libraries
* Integration with common data formats
* Change default to String implementation to Text
