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
    output <- readProcess "cabal" ["run", "dataframe-benchmark-example", "-O2"] ""
    putStrLn output

polars :: IO ()
polars = do
    output <-
        readProcess
            "./benchmark/dataframe_benchmark/bin/python3"
            ["./benchmark/polars/polars_benchmark.py"]
            ""
    putStrLn output

pandas :: IO ()
pandas = do
    output <-
        readProcess
            "./benchmark/dataframe_benchmark/bin/python3"
            ["./benchmark/pandas/pandas_benchmark.py"]
            ""
    putStrLn output

explorer :: IO ()
explorer = do
    output <-
        readProcess "mix" ["run", "./benchmark/explorer/explorer_benchmark.exs"] ""
    putStrLn output

groupByHaskell :: IO ()
groupByHaskell = do
    df <- D.readCsv "./data/housing.csv"
    print $
        df
            |> D.groupBy ["ocean_proximity"]
            |> D.aggregate
                [ F.minimum (F.col @Double "median_house_value")
                    `F.as` "minimum_median_house_value"
                , F.maximum (F.col @Double "median_house_value")
                    `F.as` "maximum_median_house_value"
                ]

groupByPolars :: IO ()
groupByPolars = do
    output <-
        readProcess
            "./benchmark/dataframe_benchmark/bin/python3"
            ["./benchmark/polars/group_by.py"]
            ""
    putStrLn output

groupByPandas :: IO ()
groupByPandas = do
    output <-
        readProcess
            "./benchmark/dataframe_benchmark/bin/python3"
            ["./benchmark/pandas/group_by.py"]
            ""
    putStrLn output

groupByExplorer :: IO ()
groupByExplorer = do
    output <-
        readProcess
            "./benchmark/dataframe_benchmark/bin/mix"
            ["run", "./benchmark/explorer/group_by.exs"]
            ""
    putStrLn output

parseFile :: String -> IO ()
parseFile = void . D.readCsv

parseFileUnstable :: String -> IO ()
parseFileUnstable = void . D.readCsvUnstable

parseFileUnstableSIMD :: String -> IO ()
parseFileUnstableSIMD = void . D.fastReadCsvUnstable

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
            ]
        , bgroup
            "housing.csv (1.4 MB)"
            [ bench "Attoparsec" $ nfIO $ parseFile "./data/housing.csv"
            , bench "Native Haskell" $ nfIO $ parseFileUnstable "./data/housing.csv"
            , bench "SIMD" $ nfIO $ parseFileUnstableSIMD "./data/housing.csv"
            ]
        , bgroup
            "effects-of-covid-19-on-trade-at-15-december-2021-provisional.csv (9.1 MB)"
            [ bench "Attoparsec" $
                nfIO $
                    parseFile
                        "./data/effects-of-covid-19-on-trade-at-15-december-2021-provisional.csv"
            , bench "Native Haskell" $
                nfIO $
                    parseFileUnstable
                        "./data/effects-of-covid-19-on-trade-at-15-december-2021-provisional.csv"
            , bench "SIMD" $
                nfIO $
                    parseFileUnstableSIMD
                        "./data/effects-of-covid-19-on-trade-at-15-december-2021-provisional.csv"
            ]
            -- this file was removed as it's too large you may substitute
            -- with your own if you wish to run it locally on a large file
            -- , bgroup
            --     "customers.csv (334 MB)"
            --     [ bench "Attoparsec" $ nfIO $ parseFile "./data/customers.csv"
            --     , bench "Native Haskell" $ nfIO $ parseFileUnstable "./data/customers.csv"
            --     , bench "SIMD" $ nfIO $ parseFileUnstableSIMD "./data/customers.csv"
            --     ]
        ]
