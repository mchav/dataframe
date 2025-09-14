{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

import qualified DataFrame as D
import qualified DataFrame.Functions as F

import Criterion.Main
import DataFrame ((|>))
import System.Process

haskell :: IO ()
haskell = do
    output <- readProcess "cabal" ["run", "dataframe", "-O2"] ""
    putStrLn output

polars :: IO ()
polars = do
    output <- readProcess "./benchmark/dataframe_benchmark/bin/python3" ["./benchmark/polars/polars_benchmark.py"] ""
    putStrLn output

pandas :: IO ()
pandas = do
    output <- readProcess "./benchmark/dataframe_benchmark/bin/python3" ["./benchmark/pandas/pandas_benchmark.py"] ""
    putStrLn output

groupByHaskell :: IO ()
groupByHaskell = do
    df <- D.readCsv "./data/housing.csv"
    print $
        df
            |> D.groupBy ["ocean_proximity"]
            |> D.aggregate
                [ (F.minimum (F.col @Double "median_house_value")) `F.as` "minimum_median_house_value"
                , (F.maximum (F.col @Double "median_house_value")) `F.as` "maximum_median_house_value"
                ]

groupByPolars :: IO ()
groupByPolars = do
    output <- readProcess "./benchmark/dataframe_benchmark/bin/python3" ["./benchmark/polars/group_by.py"] ""
    putStrLn output

groupByPandas :: IO ()
groupByPandas = do
    output <- readProcess "./benchmark/dataframe_benchmark/bin/python3" ["./benchmark/pandas/group_by.py"] ""
    putStrLn output

main = do
    output <- readProcess "cabal" ["build", "-O2"] ""
    putStrLn output
    defaultMain
        [ bgroup
            "stats"
            [ bench "simpleStatsHaskell" $ nfIO haskell
            , bench "simpleStatsPandas" $ nfIO pandas
            , bench "simpleStatsPolars" $ nfIO polars
            , bench "groupByHaskell" $ nfIO groupByHaskell
            , bench "groupByPolars" $ nfIO groupByPolars
            , bench "groupByPandas" $ nfIO groupByPandas
            ]
        ]
