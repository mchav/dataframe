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
module Main where

import qualified Data.ByteString.Char8 as C
import qualified Data.DataFrame as D

import Data.Function (&)

main :: IO ()
main = do
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

```

Future work:
* Apache arrow and Parquet compatability
* Integration with plotting libraries (considering ASCII terminal plots for now)
* Integration with common data formats
