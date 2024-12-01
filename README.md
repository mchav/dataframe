# DataFrame

An intuitive, dynamically-typed DataFrame library.

Goals:
* Exploratory data analysis
* Shallow learning curve

Non-goals:
* Static types/strong type-safety

Example usage

```haskell
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}
-- We use this to give defaults to numeric and string
-- types that way we don't have to always
-- specify types.
{-# LANGUAGE ExtendedDefaultRules #-}
module Main where

import qualified Data.ByteString.Char8 as C
import qualified Data.DataFrame as D

import Data.Maybe (fromMaybe, isNothing, isJust)
import Data.Function (&)

-- Numbers default to int and strings to bytestring
default (Int, C.ByteString)

main :: IO ()
main = do
    rawFrame <- D.readTsv "./data/chipotle.tsv"
    print $ D.dimensions rawFrame

    -- -- Sampling the dataframe
    print $ D.take 5 rawFrame

    -- Transform the data from a raw string into
    -- respective types (throws error on failure)
    let f = rawFrame
          -- Change a specfic order ID
          & D.applyWhere "order_id" (==1) "quantity" (+2)
          -- Index based change.
          & D.applyAtIndex 0 "quantity" (flip (-) 2)
          -- drop dollar sign and parse price as double
          & D.apply "item_price" (D.readValue @Double . C.drop 1)
          -- Custom parsing 
          & D.apply "choice_description" toIngredientList

    -- sample the dataframe.
    print $ D.take 10 f

    -- Create a total_price column that is quantity * item_price
    let multiply (a :: Int) (b :: Double) = fromIntegral a * b
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
                      "quantity" f)

    -- 2) Using select + reduce
    print $ f
          & D.select ["item_name", "quantity"]
          -- It's more efficient to filter before grouping.
          & D.filter "item_name" (searchTerm ==)
          & D.groupBy ["item_name"]
          & D.reduceBy "quantity" V.sum

    -- Similarly, we can aggregate quantities by all rows.
    print $ f
          & D.select ["item_name", "quantity"]
          & D.groupBy ["item_name"]
          & D.reduceBy "quantity" V.sum

    let firstOrder = withTotalPrice
                   & D.filter "choice_description" (any (C.isInfixOf "Guacamole"). fromMaybe [])
                   & D.filter "item_name" (("Chicken Bowl" :: C.ByteString) ==)

    print $ D.sum "quantity" firstOrder
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
```

Future work:
* Apache arrow and Parquet compatability
* Integration with plotting libraries (considering ASCII terminal plots for now)
* Integration with common data formats
