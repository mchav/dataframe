{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}

import qualified DataFrame as D
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Monad (replicateM)
import Criterion.Main
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

main = do
  defaultMain [
    bgroup "stats" [ bench  "haskell" $ nfIO haskell
                   , bench  "polars"  $ nfIO polars
                   , bench  "pandas"  $ nfIO pandas
                   ]
    ]
