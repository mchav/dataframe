{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}

import qualified Data.DataFrame as D
import qualified Data.Vector.Unboxed as VU

import Control.Monad (replicateM)
import Criterion.Main
import System.Random (randomRIO)

stats :: Int -> IO ()
stats n = do
  ns <- VU.replicateM n (randomRIO (-20.0 :: Double, 20.0))
  xs <- VU.replicateM n (randomRIO (-20.0 :: Double, 20.0))
  ys <- VU.replicateM n (randomRIO (-20.0 :: Double, 20.0))
  let df = D.fromList [("first", D.UnboxedColumn ns),
                       ("second", D.UnboxedColumn xs),
                       ("third", D.UnboxedColumn ys)]
  
  print $ D.mean "first" df
  print $ D.variance "second" df
  print $ D.correlation "second" "third" df
  print $ D.select ["first"] df D.|> D.take 1

main = defaultMain [
  bgroup "stats" [ bench    "300_000" $ nfIO (stats 100_000)
                 , bench  "3_000_000" $ nfIO (stats 1_000_000)
                 , bench "30_000_000" $ nfIO (stats 30_000_000)
                 ]
  ]
