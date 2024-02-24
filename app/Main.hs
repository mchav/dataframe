module Main where

import qualified Data.DataFrame as D

-- Example usage of DataFrame library

main :: IO ()
main = do
    rawFrame <- D.readCsv "/home/yavinda/code/data/chipotle.tsv"
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
