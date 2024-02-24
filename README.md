# dataframe

An intuitive, dynamically-typed DataFrame library.

Example usage for [Chipotle data](https://raw.githubusercontent.com/justmarkham/DAT8/master/data/chipotle.tsv).

```haskell
module Main where

import qualified Data.DataFrame as D

main :: IO ()
main = do
    rawFrame <- D.readCsv "./chipotle.tsv"
    putStrLn "Sample the first 3 rows of the dataframe"
    print (D.take 3 rawFrame)
    let f = D.apply "quantity" (read :: String -> Int)
          $ D.apply "order_id" (read :: String -> Int)
          $ D.apply "item_price" priceToDouble
          $ rawFrame
    putStrLn "Get the dimensions of the dataframe"
    print (D.dimensions f)
    putStrLn "List the column names in alphabetical order"
    print (D.columnNames f)

-- An example of a parsing function.
priceToDouble :: String -> Double
priceToDouble  = (read :: String -> Double) . (drop 1 )

```
