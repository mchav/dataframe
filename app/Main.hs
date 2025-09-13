{-# LANGUAGE NumericUnderscores #-}
-- Useful Haskell extensions.
-- Allow string literal to be interpreted as any other string type.
{-# LANGUAGE OverloadedStrings #-}
-- Convenience syntax for specifiying the type `sum a b :: Int` vs `sum @Int a b'.
{-# LANGUAGE TypeApplications #-}

import qualified DataFrame as D -- import for general functionality.
import qualified DataFrame.Functions as F -- import for column expressions.

import DataFrame ((|>)) -- import chaining operator with unqualified.

import Control.Monad
import qualified Data.List as L
import Data.Maybe
import Data.Function

main :: IO ()
main = do
    df' <- D.readCsv "./data/housing.csv"
    let df = df' |> D.exclude ["total_bedrooms", "ocean_proximity"]

    let expr = ((F.col @Double "housing_median_age" + (F.col @Double "median_income" * F.col @Double "latitude")) + ((F.col @Double "median_income" + F.col @Double "median_income") / (F.col @Double "median_income" + F.col @Double "latitude"))) / F.col @Double "latitude"

    let augmented = D.derive "generated_feature" expr df'

    let correlationWithHouseValue columnName = (columnName, fromMaybe 0 (D.correlation columnName "median_house_value" augmented))

    let correlations = map correlationWithHouseValue (D.columnNames augmented)

    mapM_ print (L.sortBy (flip compare `on` snd) correlations)
    -- print $ F.search ("median_house_value") 4 df
