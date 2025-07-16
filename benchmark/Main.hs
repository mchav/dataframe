{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}

import qualified DataFrame as D
import qualified Data.Vector.Unboxed as VU

import Control.Monad (replicateM)
import Criterion.Main
import System.Random (randomRIO)

stats :: Int -> IO ()
stats n = do
  ns <- do
    ns' <- VU.replicateM n (randomRIO (-20.0 :: Double, 20.0))
    pure $ replicate 3 ns'
  let df = D.fromUnamedColumns (map D.fromUnboxedVector ns)
  
  print $ D.mean "0" df
  print $ D.variance "1" df
  print $ D.correlation "1" "2" df
  print $ D.filter "0" (>= (19.9 :: Double)) df D.|> D.take 10

main = defaultMain [
  bgroup "stats" [ bench     "300_000" $ nfIO (stats 100_000)
                 , bench   "3_000_000" $ nfIO (stats 1_000_000)
                 , bench "300_000_000" $ nfIO (stats 300_000_000)
                 ]
  ]
