{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

import qualified DataFrame as D
import qualified DataFrame.Functions as F
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Monad (replicateM)
import Criterion.Main
import DataFrame ((|>))
import Data.Time
import System.Process
import System.Random.Stateful

haskell :: IO ()
haskell = do
  let n = 100_000_000
  g <- newIOGenM =<< newStdGen
  let range = (-20.0 :: Double, 20.0 :: Double)
  startGeneration <- getCurrentTime
  ns <- VU.replicateM n (uniformRM range g)
  xs <- VU.replicateM n (uniformRM range g)
  ys <- VU.replicateM n (uniformRM range g)
  let df = D.fromUnamedColumns (map D.fromUnboxedVector [ns, xs, ys])
  endGeneration <- getCurrentTime

  let generationTime = diffUTCTime endGeneration startGeneration
  putStrLn $ "Data generation Time: " ++ (show generationTime)

  startCalculation <- getCurrentTime
  print $ D.mean "0" df
  print $ D.variance "1" df
  print $ D.correlation "1" "2" df
  endCalculation <- getCurrentTime

  let calculationTime = diffUTCTime endCalculation startCalculation
  putStrLn $ "Calculation Time: " ++ (show calculationTime)

  startFilter <- getCurrentTime
  print $ D.filter "0" (>= (19.9 :: Double)) df D.|> D.take 10
  endFilter <- getCurrentTime

  let filterTime = diffUTCTime endFilter startFilter
  putStrLn $ "Filter Time: " ++ (show filterTime)

  let totalTime = diffUTCTime endFilter startGeneration
  putStrLn $ "Total Time: " ++ (show totalTime)

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
  print $ df |> D.groupBy ["ocean_proximity"]
             |> D.aggregate [ F.alias "minimum_median_house_value" (F.minimum @Double "median_house_value")
                            , F.alias "maximum_median_house_value" (F.maximum @Double "median_house_value")]

groupByPolars :: IO ()
groupByPolars = do
  output <- readProcess "./benchmark/dataframe_benchmark/bin/python3" ["./benchmark/polars/group_by.py"] ""
  putStrLn output

groupByPandas :: IO ()
groupByPandas = do
  output <- readProcess "./benchmark/dataframe_benchmark/bin/python3" ["./benchmark/pandas/group_by.py"] ""
  putStrLn output

main = do
  defaultMain [
    bgroup "stats" [ bench  "simpleStatsHaskell" $ nfIO haskell
                   , bench  "simpleStatsPandas" $ nfIO pandas
                   , bench  "simpleStatsPolars" $ nfIO polars
                   , bench  "groupByHaskell" $ nfIO groupByHaskell
                   , bench  "groupByPolars"  $ nfIO groupByPolars
                   , bench  "groupByPandas"  $ nfIO groupByPandas
                   ]
    ]
