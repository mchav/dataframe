# dataframe

An intuitive, dynamically-typed DataFrame library.

Goals:
* Exploratory data analysis
* Shallow learning curve

Non-goals:
* Type safety

Example usage

```haskell
module Main where

import qualified Data.Text as T
import qualified Data.DataFrame as D

main :: IO ()
main = do
    rawFrame <- D.readTsv "./data/chipotle.tsv"
    print $ dimensions rawFrame

    -- Transform the data from a raw string into
    -- respective types (throws error on failure)
    let f = D.applyWhere "order_id" ((1 :: Int) ==) "quantity" ((2 :: Int) +)
          . D.apply "order_id" toInt
          . D.apply "quantity" toInt
          $ rawFrame
    
    -- sample the dataframe
    print $ D.take 10 f

    -- Check how many chicken burritos were ordered.
    print $ D.sumWhere "item_name" (("Chicken Burrito" :: T.Text) ==) "quantity" f

    -- Filter all rows in order 1 contaning chips.
    let firstOrder = D.filterWhere "order_id" ((1 :: Int) ==)
                   . D.filterWhere "item_name" (T.isInfixOf "Chips")
                   $ f
    
    -- sum quantity of chips
    print $ D.sum "quantity" firstOrder
    print firstOrder

-- An example of a parsing function.
toInt :: HasCallStack => T.Text -> Int
toInt s = case readMaybe (T.unpack s) of
    Nothing    -> error $ "Could not read value: " ++ T.unpack s
    Just value -> value

toLong :: HasCallStack => T.Text -> Integer
toLong s = case readMaybe (T.unpack s) of
    Nothing    -> error $ "Could not read value: " ++ T.unpack s
    Just value -> value

```

Future work:
* Apache arrow and Parquet compatability
* Integration with plotting libraries
* Integration with common data formats
* Change default to String implementation to Text
