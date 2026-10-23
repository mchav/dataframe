{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE Strict #-}
{-# LANGUAGE TemplateHaskell #-}

module OneBillionRowChallenge (run) where

import qualified DataFrame as D
import qualified DataFrame.Functions as F
import qualified DataFrame.Lazy as L

import Data.Time
import DataFrame.Expression.Operators
import DataFrame.IO.CSV (readSeparated)

-- The full 1brc file is not checked in; use a small sample for the schema.
$( D.deriveSchemaValuesFromCsvFile
    "measurement"
    "../data/measurements_sample.csv"
 )

run :: IO ()
run = do
    startCalculation <- getCurrentTime
    df <-
        L.scanCsvStreamingWith
            readSeparated
            measurementSchema
            "../../1brc/measurements.txt"
            |> L.groupBy [F.name measurementNames]
            |> L.aggregate
                [ F.minimum measurementTemperature `as` "minimum"
                , F.mean measurementTemperature `as` "mean"
                , F.maximum measurementTemperature `as` "maximum"
                ]
            |> L.run
    print $ D.sortBy [D.Asc measurementNames] df
    endCalculation <- getCurrentTime
    let calculationTime = diffUTCTime endCalculation startCalculation
    putStrLn $ "Calculation Time: " ++ show calculationTime
