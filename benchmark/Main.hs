{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

import qualified DataFrame as D
import qualified DataFrame.Functions as F

import Control.Monad (void)
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

explorer :: IO ()
explorer = do
    _ <- readProcess "./benchmark/dataframe_benchmark/bin/mix" ["deps.get"] ""
    output <- readProcess "./benchmark/dataframe_benchmark/bin/mix" ["run", "./benchmark/explorer/explorer_benchmark.exs"] ""
    putStrLn output

groupByHaskell :: IO ()
groupByHaskell = do
    df <- D.readCsv "./data/housing.csv"
    print $
        df
            |> D.groupBy ["ocean_proximity"]
            |> D.aggregate
                [ F.minimum (F.col @Double "median_house_value") `F.as` "minimum_median_house_value"
                , F.maximum (F.col @Double "median_house_value") `F.as` "maximum_median_house_value"
                ]

groupByPolars :: IO ()
groupByPolars = do
    output <- readProcess "./benchmark/dataframe_benchmark/bin/python3" ["./benchmark/polars/group_by.py"] ""
    putStrLn output

groupByPandas :: IO ()
groupByPandas = do
    output <- readProcess "./benchmark/dataframe_benchmark/bin/python3" ["./benchmark/pandas/group_by.py"] ""
    putStrLn output

groupByExplorer :: IO ()
groupByExplorer = do
    output <- readProcess "./benchmark/dataframe_benchmark/bin/mix" ["run", "./benchmark/explorer/group_by.exs"] ""
    putStrLn output

parseFile :: String -> IO ()
parseFile path = void $ D.readCsv path

parseCovidEffectsCSV :: IO ()
parseCovidEffectsCSV = parseFile "./data/effects-of-covid-19-on-trade-at-15-december-2021-provisional.csv"

parseHousingCSV :: IO ()
parseHousingCSV = parseFile "./data/housing.csv"

parseStarWarsCSV :: IO ()
parseStarWarsCSV = parseFile "./data/starwars.csv"

parseChipotleTSV :: IO ()
parseChipotleTSV = void $ D.readTsv "./data/chipotle.tsv"

parseMeasurementsTXT :: IO ()
parseMeasurementsTXT = parseFile "./data/measurements.txt"

main :: IO ()
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
        , bench "groupByExplorer" $ nfIO groupByExplorer
        ],
      bgroup
        "Parsing"
        [ bench "effects-of-covid-19-on-trade-at-15-december-2021-provisional.csv (9.0 MB)" $ nfIO parseCovidEffectsCSV
        , bench "housing.csv (1.4 MB)" $ nfIO parseHousingCSV
        , bench "starwars.csv (10 KB)" $ nfIO parseStarWarsCSV
        , bench "chipotle.tsv (356 KB)" $ nfIO parseChipotleTSV
        , bench "measurements.txt (135 KB)" $ nfIO parseMeasurementsTXT
        ]
    ]
