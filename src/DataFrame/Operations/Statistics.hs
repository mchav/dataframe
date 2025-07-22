{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE BangPatterns #-}
module DataFrame.Operations.Statistics where

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Vector.Generic as VG
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Statistics.Quantile as SS
import qualified Statistics.Sample as SS

import Prelude as P

import Control.Exception (throw)
import DataFrame.Errors (DataFrameException(..))
import DataFrame.Internal.Column
import DataFrame.Internal.DataFrame (DataFrame(..), getColumn, empty)
import DataFrame.Operations.Core
import Data.Foldable (asum)
import Data.Maybe (isJust, fromMaybe)
import Data.Function ((&))
import Data.Type.Equality (type (:~:)(Refl), TestEquality (testEquality))
import Type.Reflection (typeRep)


frequencies :: T.Text -> DataFrame -> DataFrame
frequencies name df = case getColumn name df of
  Nothing -> throw $ ColumnNotFoundException name "frequencies" (map fst $ M.toList $ columnIndices df)
  Just ((BoxedColumn (column :: V.Vector a))) -> let
      counts = valueCounts @a name df
      total = P.sum $ map snd counts
      vText :: forall a . (Columnable a) => a -> T.Text
      vText c' = case testEquality (typeRep @a) (typeRep @T.Text) of
        Just Refl -> c'
        Nothing -> case testEquality (typeRep @a) (typeRep @String) of
          Just Refl -> T.pack c'
          Nothing -> (T.pack . show) c'
      initDf = empty & insertVector "Statistic" (V.fromList ["Count" :: T.Text,  "Percentage (%)"])
    in L.foldl' (\df (col, k) -> insertVector (vText col) (V.fromList [k, k * 100 `div` total]) df) initDf counts
  Just ((OptionalColumn (column :: V.Vector a))) -> let
      counts = valueCounts @a name df
      total = P.sum $ map snd counts
      vText :: forall a . (Columnable a) => a -> T.Text
      vText c' = case testEquality (typeRep @a) (typeRep @T.Text) of
        Just Refl -> c'
        Nothing -> case testEquality (typeRep @a) (typeRep @String) of
          Just Refl -> T.pack c'
          Nothing -> (T.pack . show) c'
      initDf = empty & insertVector "Statistic" (V.fromList ["Count" :: T.Text,  "Percentage (%)"])
    in L.foldl' (\df (col, k) -> insertVector (vText col) (V.fromList [k, k * 100 `div` total]) df) initDf counts
  Just ((UnboxedColumn (column :: VU.Vector a))) -> let
      counts = valueCounts @a name df
      total = P.sum $ map snd counts
      vText :: forall a . (Columnable a) => a -> T.Text
      vText c' = case testEquality (typeRep @a) (typeRep @T.Text) of
        Just Refl -> c'
        Nothing -> case testEquality (typeRep @a) (typeRep @String) of
          Just Refl -> T.pack c'
          Nothing -> (T.pack . show) c'
      initDf = empty & insertVector "Statistic" (V.fromList ["Count" :: T.Text,  "Percentage (%)"])
    in L.foldl' (\df (col, k) -> insertVector (vText col) (V.fromList [k, k * 100 `div` total]) df) initDf counts
  _ -> error $ "There are ungrouped columns"

mean :: T.Text -> DataFrame -> Maybe Double
mean = applyStatistic mean'

median :: T.Text -> DataFrame -> Maybe Double
median = applyStatistic (SS.median SS.medianUnbiased)

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
  return $ SS.correlation2 f s

_getColumnAsDouble :: T.Text -> DataFrame -> Maybe (VU.Vector Double)
_getColumnAsDouble name df = case getColumn name df of
  Just (UnboxedColumn (f :: VU.Vector a)) -> case testEquality (typeRep @a) (typeRep @Double) of
    Just Refl -> Just f
    Nothing -> case testEquality (typeRep @a) (typeRep @Int) of
      Just Refl -> Just $ VU.map fromIntegral f
      Nothing -> Nothing
  _ -> Nothing

sum :: T.Text -> DataFrame -> Maybe Double
sum name df = case getColumn name df of
  Nothing -> throw $ ColumnNotFoundException name "sum" (map fst $ M.toList $ columnIndices df)
  Just ((UnboxedColumn (column :: VU.Vector a'))) -> case testEquality (typeRep @a') (typeRep @Int) of
    Just Refl -> Just $ VG.sum (VU.map fromIntegral column)
    Nothing -> case testEquality (typeRep @a') (typeRep @Double) of
      Just Refl -> Just $ VG.sum column
      Nothing -> Nothing

applyStatistic :: (VU.Vector Double -> Double) -> T.Text -> DataFrame -> Maybe Double
applyStatistic f name df = case getColumn name df of
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
applyStatistics f name df = case getColumn name df of
  Just ((UnboxedColumn (column :: VU.Vector a'))) -> case testEquality (typeRep @a') (typeRep @Int) of
    Just Refl -> Just $! f (VU.map fromIntegral column)
    Nothing -> case testEquality (typeRep @a') (typeRep @Double) of
      Just Refl -> Just $! f column
      Nothing -> case testEquality (typeRep @a') (typeRep @Float) of
        Just Refl -> Just $! f (VG.map realToFrac column)
        Nothing -> Nothing
  _ -> Nothing

summarize :: DataFrame -> DataFrame
summarize df = fold columnStats (columnNames df) (fromNamedColumns [("Statistic", fromList ["Mean" :: T.Text, "Minimum", "25%" ,"Median", "75%", "Max", "StdDev", "IQR", "Skewness"])])
  where columnStats name d = if all isJust (stats name) then insertUnboxedVector name (VU.fromList (map (roundTo 2 . fromMaybe 0) $ stats name)) d else d
        stats name = let
            quantiles = applyStatistics (SS.quantilesVec SS.medianUnbiased (VU.fromList [0,1,2,3,4]) 4) name df
            min' = flip (VG.!) 0 <$> quantiles
            quartile1 = flip (VG.!) 1 <$> quantiles
            median' = flip (VG.!) 2 <$> quantiles
            quartile3 = flip (VG.!) 3 <$> quantiles
            max' = flip (VG.!) 4 <$> quantiles
            iqr = (-) <$> quartile3 <*> quartile1
          in [mean name df,
              min',
              quartile1,
              median',
              quartile3,
              max',
              standardDeviation name df,
              iqr,
              skewness name df]
        roundTo :: Int -> Double -> Double
        roundTo n x = fromInteger (round $ x * (10^n)) / (10.0^^n)

mean' :: VU.Vector Double -> Double
mean' samp = let
    (!total, !n) = VG.foldl' (\(!total, !n) v -> (total + v, n + 1))  (0 :: Double, 0 :: Int) samp
  in total / fromIntegral n

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
