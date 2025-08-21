{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE BangPatterns #-}
module DataFrame.Operations.Statistics where

import Data.Bifunctor (second)
import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Vector.Generic as VG
import qualified Data.Vector as V
import qualified Data.Vector.Algorithms.Intro as VA
import qualified Data.Vector.Unboxed as VU
import qualified Statistics.Quantile as SS
import qualified Statistics.Sample as SS

import Prelude as P

import Control.Exception (throw)
import Control.Monad.ST (runST)
import DataFrame.Errors (DataFrameException(..))
import DataFrame.Internal.Column
import DataFrame.Internal.DataFrame (DataFrame(..), getColumn, empty, unsafeGetColumn)
import DataFrame.Operations.Core
import DataFrame.Operations.Subset (filterJust)
import Data.Foldable (asum)
import Data.Maybe (isJust, fromMaybe)
import Data.Function ((&))
import Data.Type.Equality (type (:~:)(Refl), TestEquality (testEquality))
import Type.Reflection (typeRep)
import qualified Data.Bifunctor as Data
import DataFrame.Internal.Row (toAny, showValue)
import GHC.Float (int2Double)
import Text.Printf (printf)

frequencies :: T.Text -> DataFrame -> DataFrame
frequencies name df = let
    counts :: forall a . Columnable a => [(a, Int)]
    counts =  valueCounts name df
    calculatePercentage cs k = toAny $ toPct2dp (fromIntegral k / fromIntegral (P.sum $ map snd cs))
    initDf = empty & insertVector "Statistic" (V.fromList ["Count" :: T.Text,  "Percentage (%)"])
    freqs :: forall v a . (VG.Vector v a, Columnable a) => v a -> DataFrame
    freqs col = L.foldl' (\d (col, k) -> insertVector (showValue @a col) (V.fromList [toAny k, calculatePercentage (counts @a) k]) d) initDf counts
  in case getColumn name df of
      Nothing -> throw $ ColumnNotFoundException name "frequencies" (map fst $ M.toList $ columnIndices df)
      Just ((BoxedColumn (column :: V.Vector a))) -> freqs column
      Just ((OptionalColumn (column :: V.Vector a))) -> freqs column
      Just ((UnboxedColumn (column :: VU.Vector a))) -> freqs column

mean :: T.Text -> DataFrame -> Maybe Double
mean = applyStatistic mean'

median :: T.Text -> DataFrame -> Maybe Double
median = applyStatistic median'

standardDeviation :: T.Text -> DataFrame -> Maybe Double
standardDeviation = applyStatistic SS.fastStdDev

skewness :: T.Text -> DataFrame -> Maybe Double
skewness = applyStatistic SS.skewness

variance :: T.Text -> DataFrame -> Maybe Double
variance = applyStatistic variance'

interQuartileRange :: T.Text -> DataFrame -> Maybe Double
interQuartileRange = applyStatistic (SS.midspread SS.medianUnbiased 4)

correlation :: T.Text -> T.Text -> DataFrame -> Maybe Double
correlation first second df = do
  f <- _getColumnAsDouble first df
  s <- _getColumnAsDouble second df
  return $ SS.correlation (VG.zip f s)

_getColumnAsDouble :: T.Text -> DataFrame -> Maybe (VU.Vector Double)
_getColumnAsDouble name df = case getColumn name df of
  Just (UnboxedColumn (f :: VU.Vector a)) -> case testEquality (typeRep @a) (typeRep @Double) of
    Just Refl -> Just f
    Nothing -> case testEquality (typeRep @a) (typeRep @Int) of
      Just Refl -> Just $ VU.map fromIntegral f
      Nothing -> Nothing
  _ -> Nothing

sum :: forall a. (Columnable a, Num a, VU.Unbox a) => T.Text -> DataFrame -> Maybe a
sum name df = case getColumn name df of
  Nothing -> throw $ ColumnNotFoundException name "sum" (map fst $ M.toList $ columnIndices df)
  Just ((UnboxedColumn (column :: VU.Vector a'))) -> case testEquality (typeRep @a') (typeRep @a) of
    Just Refl -> Just $ VG.sum column
    Nothing -> Nothing

applyStatistic :: (VU.Vector Double -> Double) -> T.Text -> DataFrame -> Maybe Double
applyStatistic f name df = case getColumn name (filterJust name df) of
      Nothing -> throw $ ColumnNotFoundException name "applyStatistic" (map fst $ M.toList $ columnIndices df)
      Just column@(UnboxedColumn (col :: VU.Vector a)) -> case testEquality (typeRep @a) (typeRep @Double) of
        Just Refl -> reduceColumn f column
        Nothing -> do
          matching <- asum [mapColumn (fromIntegral :: Int -> Double) column,
                mapColumn (fromIntegral :: Integer -> Double) column,
                mapColumn (realToFrac :: Float -> Double) column,
                Just column ]
          reduceColumn f matching
      _ -> Nothing

applyStatistics :: (VU.Vector Double -> VU.Vector Double) -> T.Text -> DataFrame -> Maybe (VU.Vector Double)
applyStatistics f name df = case getColumn name (filterJust name df) of
  Just ((UnboxedColumn (column :: VU.Vector a'))) -> case testEquality (typeRep @a') (typeRep @Int) of
    Just Refl -> Just $! f (VU.map fromIntegral column)
    Nothing -> case testEquality (typeRep @a') (typeRep @Double) of
      Just Refl -> Just $! f column
      Nothing -> case testEquality (typeRep @a') (typeRep @Float) of
        Just Refl -> Just $! f (VG.map realToFrac column)
        Nothing -> Nothing
  _ -> Nothing

summarize :: DataFrame -> DataFrame
summarize df = fold columnStats (columnNames df) (fromNamedColumns [("Statistic", fromList ["Count" :: T.Text, "Mean", "Minimum", "25%" ,"Median", "75%", "Max", "StdDev", "IQR", "Skewness"])])
  where columnStats name d = if all isJust (stats name) then insertUnboxedVector name (VU.fromList (map (roundTo 2 . fromMaybe 0) $ stats name)) d else d
        stats name = let
            count = fromIntegral . numElements <$> getColumn name df
            quantiles = applyStatistics (SS.quantilesVec SS.medianUnbiased (VU.fromList [0,1,2,3,4]) 4) name df
            min' = flip (VG.!) 0 <$> quantiles
            quartile1 = flip (VG.!) 1 <$> quantiles
            median' = flip (VG.!) 2 <$> quantiles
            quartile3 = flip (VG.!) 3 <$> quantiles
            max' = flip (VG.!) 4 <$> quantiles
            iqr = (-) <$> quartile3 <*> quartile1
          in [count,
              mean name df,
              min',
              quartile1,
              median',
              quartile3,
              max',
              standardDeviation name df,
              iqr,
              skewness name df]

-- | Round a @Double@ to Specified Precision
roundTo :: Int -> Double -> Double
roundTo n x = fromInteger (round $ x * (10^n)) / (10.0^^n)

toPct2dp :: Double -> String
toPct2dp x
  | x < 0.00005 = "<0.01%"
  | otherwise   = printf "%.2f%%" (x * 100)

mean' :: VU.Vector Double -> Double
mean' samp = let
    (!total, !n) = VG.foldl' (\(!total, !n) v -> (total + v, n + 1))  (0 :: Double, 0 :: Int) samp
  in total / fromIntegral n

median' :: VU.Vector Double -> Double
median' samp = runST $ do
  mutableSamp <- VU.thaw samp
  VA.sort mutableSamp
  sortedSamp <- VU.freeze mutableSamp
  let
    length = VU.length samp
    middleIndex = length `div` 2
  return $
    if odd length
      then sortedSamp VU.! middleIndex
      else (sortedSamp VU.! (middleIndex - 1) + sortedSamp VU.! middleIndex) / 2

-- accumulator: count, mean, m2
data VarAcc = VarAcc !Int !Double !Double  deriving Show

step :: VarAcc -> Double -> VarAcc
step (VarAcc !n !mean !m2) !x =
  let !n'    = n + 1
      !delta = x - mean
      !mean' = mean + delta / fromIntegral n'
      !m2'   = m2 + delta * (x - mean')
  in  VarAcc n' mean' m2'

computeVariance :: VarAcc -> Double
computeVariance (VarAcc n _ m2)
  | n < 2     = 0                -- or error "variance of <2 samples"
  | otherwise = m2 / fromIntegral (n - 1)

variance' :: VU.Vector Double -> Double
variance' = computeVariance . VG.foldl' step (VarAcc 0 0 0)
