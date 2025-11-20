{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE Strict #-}
{-# LANGUAGE TypeApplications #-}

module Main where

import qualified DataFrame as D
import qualified DataFrame.Functions as F

import Data.Time
import DataFrame ((|>))
import System.Mem

main :: IO ()
main = do
    startRead <- getCurrentTime
    parsed <-
        D.readSeparated ';' D.defaultReadOptions "../../1brc/data/measurements.txt"
    endRead <- getCurrentTime
    let readTime = diffUTCTime endRead startRead
    putStrLn $ "Read Time: " ++ show readTime
    performGC
    let measurement = F.col @Double "measurement"
    startCalculation <- getCurrentTime
    print $
        parsed
            |> D.groupBy ["city"]
            |> D.aggregate
                [ F.minimum measurement `F.as` "minimum"
                , F.mean measurement `F.as` "mean"
                , F.maximum measurement `F.as` "maximum"
                ]
            |> D.sortBy [D.Asc "city"]
    endCalculation <- getCurrentTime
    let calculationTime = diffUTCTime endCalculation startCalculation
    putStrLn $ "Calculation Time: " ++ show calculationTime
