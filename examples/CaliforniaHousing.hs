{-# LANGUAGE OverloadedStrings #-}
module Main where

import qualified DataFrame as D

main :: IO ()
main = do
  parsed <- D.readCsv "./data/housing.csv"

  print $ D.describeColumns parsed

  print $ D.take 5 parsed

  D.plotHistograms D.PlotAll D.VerticalHistogram parsed
