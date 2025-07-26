{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications  #-}
module Main where

import qualified DataFrame as D
import qualified DataFrame.Functions as F

import DataFrame ((|>))

main :: IO ()
main = do
  parsed <- D.readSeparated ';' D.defaultOptions "./data/measurements.txt"
  let measurement = (F.col @Double "Measurement")
  print $
    parsed
      |> D.groupBy ["City"]
      |> D.aggregate [ (F.minimum measurement) `F.as` "minimum"
                     , (F.mean measurement)    `F.as` "mean"
                     , (F.maximum measurement) `F.as` "maximum"]
      |> D.sortBy D.Ascending ["City"]
