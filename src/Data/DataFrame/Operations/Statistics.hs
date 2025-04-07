{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE StrictData #-}
module Data.DataFrame.Operations.Statistics where

import qualified Data.List as L
import qualified Data.Text as T
import qualified Data.Vector.Generic as VG
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Statistics.Quantile as SS
import qualified Statistics.Sample as SS

import Prelude as P

import Control.Exception (throw)
import Data.DataFrame.Errors (DataFrameException(..))
import Data.DataFrame.Internal.Column
import Data.DataFrame.Internal.DataFrame (DataFrame(..), getColumn, empty)
import Data.DataFrame.Internal.Types (Columnable, transform)
import Data.DataFrame.Operations.Core
import Data.Foldable (asum)
import Data.Maybe (isJust, fromMaybe)
import Data.Function ((&))
import Data.Type.Equality (type (:~:)(Refl), TestEquality (testEquality))
import Type.Reflection (typeRep)


frequencies :: T.Text -> DataFrame -> DataFrame
frequencies name df = case getColumn name df of
  Just ((BoxedColumn (column :: V.Vector a))) -> let
      counts = valueCounts @a name df
      total = P.sum $ map snd counts
      vText :: forall a . (Columnable a) => a -> T.Text
      vText c' = case testEquality (typeRep @a) (typeRep @T.Text) of
        Just Refl -> c'
        Nothing -> case testEquality (typeRep @a) (typeRep @String) of
          Just Refl -> T.pack c'
          Nothing -> (T.pack . show) c'
      initDf = empty & insertColumn "Statistic" (V.fromList ["Count" :: T.Text,  "Percentage (%)"])
    in L.foldl' (\df (col, k) -> insertColumn (vText col) (V.fromList [k, k * 100 `div` total]) df) initDf counts
  Just ((OptionalColumn (column :: V.Vector a))) -> let
      counts = valueCounts @a name df
      total = P.sum $ map snd counts
      vText :: forall a . (Columnable a) => a -> T.Text
      vText c' = case testEquality (typeRep @a) (typeRep @T.Text) of
        Just Refl -> c'
        Nothing -> case testEquality (typeRep @a) (typeRep @String) of
          Just Refl -> T.pack c'
          Nothing -> (T.pack . show) c'
      initDf = empty & insertColumn "Statistic" (V.fromList ["Count" :: T.Text,  "Percentage (%)"])
    in L.foldl' (\df (col, k) -> insertColumn (vText col) (V.fromList [k, k * 100 `div` total]) df) initDf counts
  Just ((UnboxedColumn (column :: VU.Vector a))) -> let
      counts = valueCounts @a name df
      total = P.sum $ map snd counts
      vText :: forall a . (Columnable a) => a -> T.Text
      vText c' = case testEquality (typeRep @a) (typeRep @T.Text) of
        Just Refl -> c'
        Nothing -> case testEquality (typeRep @a) (typeRep @String) of
          Just Refl -> T.pack c'
          Nothing -> (T.pack . show) c'
      initDf = empty & insertColumn "Statistic" (V.fromList ["Count" :: T.Text,  "Percentage (%)"])
    in L.foldl' (\df (col, k) -> insertColumn (vText col) (V.fromList [k, k * 100 `div` total]) df) initDf counts

mean :: T.Text -> DataFrame -> Maybe Double
mean = applyStatistic SS.mean

median :: T.Text -> DataFrame -> Maybe Double
median = applyStatistic (SS.median SS.medianUnbiased)

standardDeviation :: T.Text -> DataFrame -> Maybe Double
standardDeviation = applyStatistic SS.fastStdDev

skewness :: T.Text -> DataFrame -> Maybe Double
skewness = applyStatistic SS.skewness

variance :: T.Text -> DataFrame -> Maybe Double
variance = applyStatistic SS.variance

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
  Just ((UnboxedColumn (column :: VU.Vector a'))) -> case testEquality (typeRep @a') (typeRep @Int) of
    Just Refl -> Just $ VG.sum (VU.map fromIntegral column)
    Nothing -> case testEquality (typeRep @a') (typeRep @Double) of
      Just Refl -> Just $ VG.sum column
      Nothing -> Nothing
  Nothing -> Nothing

applyStatistic :: (VU.Vector Double -> Double) -> T.Text -> DataFrame -> Maybe Double
applyStatistic f name df = do
      column <- getColumn name df
      if columnTypeString column == "Double"
      then safeReduceColumn f column
      else do
        matching <- asum [ transform (fromIntegral :: Int -> Double) column,
                          transform (realToFrac :: Float -> Double) column,
                          Just column ]
        safeReduceColumn f matching

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
summarize df = fold columnStats (columnNames df) (fromList [("Statistic", toColumn ["Mean" :: T.Text, "Minimum", "25%" ,"Median", "75%", "Max", "StdDev", "IQR", "Skewness"])])
  where columnStats name d = if all isJust (stats name) then insertUnboxedColumn name (VU.fromList (map (roundTo 2 . fromMaybe 0) $ stats name)) d else d
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
