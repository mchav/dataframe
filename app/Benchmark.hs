{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}

import Data.Time
import qualified Data.Vector.Unboxed as VU
import qualified DataFrame as D
import System.Random.Stateful

main :: IO ()
main = do
    let n = 100_000_000
    g <- newIOGenM =<< newStdGen
    let range = (0 :: Double, 1 :: Double)
    startGeneration <- getCurrentTime
    ns <- VU.replicateM n (uniformRM range g)
    xs <- VU.replicateM n (uniformRM range g)
    ys <- VU.replicateM n (uniformRM range g)
    let df = D.fromUnnamedColumns (map D.fromUnboxedVector [ns, xs, ys])
    print df
    endGeneration <- getCurrentTime
    let generationTime = diffUTCTime endGeneration startGeneration
    putStrLn $ "Data generation Time: " ++ show generationTime
    startCalculation <- getCurrentTime
    print $ D.mean "0" df
    print $ D.variance "1" df
    print $ D.correlation "1" "2" df
    endCalculation <- getCurrentTime
    let calculationTime = diffUTCTime endCalculation startCalculation
    putStrLn $ "Calculation Time: " ++ show calculationTime
    startFilter <- getCurrentTime
    print $ D.filter "0" (> (0.971 :: Double)) df D.|> D.take 10
    endFilter <- getCurrentTime
    let filterTime = diffUTCTime endFilter startFilter
    putStrLn $ "Filter Time: " ++ show filterTime
    let totalTime = diffUTCTime endFilter startGeneration
    putStrLn $ "Total Time: " ++ show totalTime
