{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications  #-}
module Main where

import qualified DataFrame as D
import qualified DataFrame.Functions as F

import DataFrame ((|>))

main :: IO ()
main = do
  parsed <- D.readSeparated ';' D.defaultOptions "./data/measurements.txt"
  print $
    parsed
      |> D.groupBy ["City"]
      |> D.aggregate [ (F.minimum @Double "Measurement") `F.as` "minimum"
                     , (F.mean "Measurement")            `F.as` "mean"
                     , (F.maximum @Double "Measurement") `F.as` "maximum"]
      |> D.sortBy D.Ascending ["City"]
