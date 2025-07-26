
-- Useful Haskell extensions.
{-# LANGUAGE OverloadedStrings #-} -- Allow string literal to be interpreted as any other string type.
{-# LANGUAGE TypeApplications #-} -- Convenience syntax for specifiying the type `sum a b :: Int` vs `sum @Int a b'. 

import qualified DataFrame as D -- import for general functionality.
import qualified DataFrame.Functions as F -- import for column expressions.

import DataFrame ((|>)) -- import chaining operator with unqualified.

main :: IO ()
main = do
    df <- D.readTsv "./data/chipotle.tsv"
    let quantity = F.col "quantity" :: D.Expr Int -- A typed reference to a column.
    print (df
      |> D.select ["item_name", "quantity"]
      |> D.groupBy ["item_name"]
      |> D.aggregate [ (F.sum quantity)     `F.as` "sum_quantity"
                     , (F.mean quantity)    `F.as` "mean_quantity"
                     , (F.maximum quantity) `F.as` "maximum_quantity"
                     ]
      |> D.sortBy D.Descending ["sum_quantity"]
      |> D.take 10)
