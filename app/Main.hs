{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

import qualified DataFrame as D
import qualified DataFrame.Functions as F
import qualified Data.Vector.Unboxed as VU

import Control.Monad (replicateM)
import DataFrame ((|>))
import System.Random.Stateful

main :: IO ()
main =  do
  let n = 100_000_000
  g <- newIOGenM =<< newStdGen
  
  let range = (-20.0 :: Double, 20.0 :: Double)
  ns <- VU.replicateM n (uniformRM range g)
  xs <- VU.replicateM n (uniformRM range g)
  ys <- VU.replicateM n (uniformRM range g)
  
  let df = D.fromUnnamedColumns (map D.fromUnboxedVector [ns, xs, ys])
  print $ D.mean "0" df
  print $ D.variance "1" df
  print $ D.correlation "1" "2" df

  print $ D.filter "0" (>= (19.9 :: Double)) df D.|> D.take 10
