{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE StrictData #-}
module Main where

import qualified Data.DataFrame as D
import qualified Data.Text as T
import qualified Data.Vector as V
import GHC.Stack (HasCallStack)
import Text.Read (readMaybe)
import Data.DataFrame.Operations (dimensions)

-- Example usage of DataFrame library

main :: IO ()
main = covid >> chipotle

covid :: IO ()
covid = do
    rawFrame <- D.readCsv "./data/effects-of-covid-19-on-trade-at-15-december-2021-provisional.csv"
    print $ dimensions rawFrame
    print $ D.take 10 rawFrame
    -- value of all exports from 2015
    let parsed = D.apply "Value" toInt
               . D.apply "Year" toInt
               $ rawFrame
    let exports2015 = D.filterWhere "Direction" (("Exports" :: T.Text) ==)
                    . D.filterWhere "Year" ((2015 :: Int) ==)
                    $ parsed
    print $ D.sum "Value" exports2015

chipotle :: IO ()
chipotle = do
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
