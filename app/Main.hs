
-- Useful Haskell extensions.
{-# LANGUAGE OverloadedStrings #-} -- Allow string literal to be interpreted as any other string type.
{-# LANGUAGE TypeApplications #-} -- Convenience syntax for specifiying the type `sum a b :: Int` vs `sum @Int a b'. 
{-# LANGUAGE NumericUnderscores #-}

import qualified DataFrame as D -- import for general functionality.
import qualified DataFrame.Functions as F -- import for column expressions.

import DataFrame ((|>)) -- import chaining operator with unqualified.

import qualified Data.Vector.Unboxed as VU
import Control.Monad (replicateM)
import Data.Time
import System.Random.Stateful

main :: IO ()
main = do
    let n = 100_000_000
    g <- newIOGenM =<< newStdGen
    let range = (-20.0 :: Double, 20.0 :: Double)
    startGeneration <- getCurrentTime
    ns <- VU.replicateM n (uniformRM range g)
    xs <- VU.replicateM n (uniformRM range g)
    ys <- VU.replicateM n (uniformRM range g)
    let df = D.fromUnnamedColumns (map D.fromUnboxedVector [ns, xs, ys])
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
