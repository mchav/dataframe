{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}

module DataFrame.Internal.Statistics where

import qualified Data.Vector.Algorithms.Intro as VA
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Exception (throw)
import Control.Monad.ST (runST)
import DataFrame.Errors (DataFrameException (..))

mean' :: VU.Vector Double -> Double
mean' samp = VU.sum samp / fromIntegral (VU.length samp)
{-# INLINE mean' #-}

median' :: VU.Vector Double -> Double
median' samp
    | VU.null samp = throw $ EmptyDataSetException "median"
    | otherwise = runST $ do
        mutableSamp <- VU.thaw samp
        VA.sort mutableSamp
        let len = VU.length samp
            middleIndex = len `div` 2
        middleElement <- VUM.read mutableSamp middleIndex
        if odd len
            then pure middleElement
            else do
                prev <- VUM.read mutableSamp (middleIndex - 1)
                pure ((middleElement + prev) / 2)
{-# INLINE median' #-}

-- accumulator: count, mean, m2
data VarAcc = VarAcc !Int !Double !Double deriving (Show)

varianceStep :: VarAcc -> Double -> VarAcc
varianceStep (VarAcc !n !mean !m2) !x =
    let !n' = n + 1
        !delta = x - mean
        !mean' = mean + delta / fromIntegral n'
        !m2' = m2 + delta * (x - mean')
     in VarAcc n' mean' m2'
{-# INLINE varianceStep #-}

computeVariance :: VarAcc -> Double
computeVariance (VarAcc !n _ !m2)
    | n < 2 = 0 -- or error "variance of <2 samples"
    | otherwise = m2 / fromIntegral (n - 1)
{-# INLINE computeVariance #-}

variance' :: VU.Vector Double -> Double
variance' = computeVariance . VU.foldl' varianceStep (VarAcc 0 0 0)
{-# INLINE variance' #-}

-- accumulator: count, mean, m2, m3
data SkewAcc = SkewAcc !Int !Double !Double !Double deriving (Show)

skewnessStep :: SkewAcc -> Double -> SkewAcc
skewnessStep (SkewAcc !n !mean !m2 !m3) !x =
    let !n' = n + 1
        !k = fromIntegral n'
        !delta = x - mean
        !mean' = mean + delta / k
        !m2' = m2 + (delta ^ 2 * (k - 1)) / k
        !m3' = m3 + (delta ^ 3 * (k - 1) * (k - 2)) / k ^ 2 - (3 * delta * m2) / k
     in SkewAcc n' mean' m2' m3'
{-# INLINE skewnessStep #-}

computeSkewness :: SkewAcc -> Double
computeSkewness (SkewAcc n _ m2 m3)
    | n < 3 = 0 -- or error "skewness of <3 samples"
    | otherwise = (sqrt (fromIntegral n - 1) * m3) / sqrt (m2 ^ 3)
{-# INLINE computeSkewness #-}

skewness' :: VU.Vector Double -> Double
skewness' = computeSkewness . VU.foldl' skewnessStep (SkewAcc 0 0 0 0)
{-# INLINE skewness' #-}

correlation' :: VU.Vector Double -> VU.Vector Double -> Maybe Double
correlation' xs ys
    | VU.length xs /= VU.length ys = Nothing
    | nI < 2 = Nothing
    | otherwise =
        let !nf = fromIntegral nI
            (!sumX, !sumY, !sumSquaredX, !sumSquaredY, !sumXY) = go 0 0 0 0 0 0
            !num = nf * sumXY - sumX * sumY
            !den = sqrt ((nf * sumSquaredX - sumX * sumX) * (nf * sumSquaredY - sumY * sumY))
         in pure (num / den)
  where
    !nI = VU.length xs
    go !i !sumX !sumY !sumSquaredX !sumSquaredY !sumXY
        | i < nI =
            let !x = VU.unsafeIndex xs i
                !y = VU.unsafeIndex ys i
                !sumX' = sumX + x
                !sumY' = sumY + y
                !sumSquaredX' = sumSquaredX + x * x
                !sumSquaredY' = sumSquaredY + y * y
                !sumXY' = sumXY + x * y
             in go (i + 1) sumX' sumY' sumSquaredX' sumSquaredY' sumXY'
        | otherwise = (sumX, sumY, sumSquaredX, sumSquaredY, sumXY)
{-# INLINE correlation' #-}

quantiles' :: VU.Vector Int -> Int -> VU.Vector Double -> VU.Vector Double
quantiles' qs q samp
    | VU.null samp = throw $ EmptyDataSetException "quantiles"
    | q < 2 = throw $ WrongQuantileNumberException q
    | VU.any (\i -> i < 0 || i > q) qs = throw $ WrongQuantileIndexException qs q
    | otherwise = runST $ do
        let !n = VU.length samp
        mutableSamp <- VU.thaw samp
        VA.sort mutableSamp
        VU.mapM (\i -> do
            let !p = fromIntegral i / fromIntegral q
                !position = p * fromIntegral (n - 1)
                !index = floor position
                !f = position - fromIntegral index
            x <- VUM.read mutableSamp index
            if f == 0
                then return x
                else do
                    y <- VUM.read mutableSamp (index + 1)
                    return $ (1 - f) * x + f * y
            ) qs
{-# INLINE quantiles' #-}

interQuartileRange' :: VU.Vector Double -> Double
interQuartileRange' samp =
    let quartiles = quantiles' (VU.fromList [1, 3]) 4 samp
     in quartiles VU.! 1 - quartiles VU.! 0
{-# INLINE interQuartileRange' #-}
