{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Operations.Statistics where

import Data.Bifunctor (second)
import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Algorithms.Intro as VA
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Prelude as P

import Control.Exception (throw)
import Control.Monad
import Control.Monad.ST (runST)
import qualified Data.Bifunctor as Data
import Data.Foldable (asum)
import Data.Function ((&))
import Data.Maybe (fromMaybe, isJust)
import Data.Type.Equality (TestEquality (testEquality), type (:~:) (Refl))
import DataFrame.Errors (DataFrameException (..))
import DataFrame.Internal.Column
import DataFrame.Internal.Statistics
import DataFrame.Internal.DataFrame (DataFrame (..), empty, getColumn, unsafeGetColumn)
import DataFrame.Internal.Row (showValue, toAny)
import DataFrame.Internal.Types
import DataFrame.Operations.Core
import DataFrame.Operations.Subset (filterJust)
import GHC.Float (int2Double)
import Text.Printf (printf)
import Type.Reflection (typeRep)

{- | Show a frequency table for a categorical feaure.

__Examples:__

@
ghci> df <- D.readCsv "./data/housing.csv"

ghci> D.frequencies "ocean_proximity" df

----------------------------------------------------------------------------
index |   Statistic    | <1H OCEAN | INLAND | ISLAND | NEAR BAY | NEAR OCEAN
------|----------------|-----------|--------|--------|----------|-----------
 Int  |      Text      |    Any    |  Any   |  Any   |   Any    |    Any
------|----------------|-----------|--------|--------|----------|-----------
0     | Count          | 9136      | 6551   | 5      | 2290     | 2658
1     | Percentage (%) | 44.26%    | 31.74% | 0.02%  | 11.09%   | 12.88%
@
-}
frequencies :: T.Text -> DataFrame -> DataFrame
frequencies name df =
    let
        counts :: forall a. (Columnable a) => [(a, Int)]
        counts = valueCounts name df
        calculatePercentage cs k = toAny $ toPct2dp (fromIntegral k / fromIntegral (P.sum $ map snd cs))
        initDf = empty & insertVector "Statistic" (V.fromList ["Count" :: T.Text, "Percentage (%)"])
        freqs :: forall v a. (VG.Vector v a, Columnable a) => v a -> DataFrame
        freqs col = L.foldl' (\d (col, k) -> insertVector (showValue @a col) (V.fromList [toAny k, calculatePercentage (counts @a) k]) d) initDf counts
     in
        case getColumn name df of
            Nothing -> throw $ ColumnNotFoundException name "frequencies" (map fst $ M.toList $ columnIndices df)
            Just ((BoxedColumn (column :: V.Vector a))) -> freqs column
            Just ((OptionalColumn (column :: V.Vector a))) -> freqs column
            Just ((UnboxedColumn (column :: VU.Vector a))) -> freqs column

-- | Calculates the mean of a given column as a standalone value.
mean :: T.Text -> DataFrame -> Maybe Double
mean = applyStatistic mean'

-- | Calculates the median of a given column as a standalone value.
median :: T.Text -> DataFrame -> Maybe Double
median = applyStatistic median'

-- | Calculates the standard deviation of a given column as a standalone value.
standardDeviation :: T.Text -> DataFrame -> Maybe Double
standardDeviation = applyStatistic (sqrt . variance')

-- | Calculates the skewness of a given column as a standalone value.
skewness :: T.Text -> DataFrame -> Maybe Double
skewness = applyStatistic skewness'

-- | Calculates the variance of a given column as a standalone value.
variance :: T.Text -> DataFrame -> Maybe Double
variance = applyStatistic variance'

-- | Calculates the inter-quartile range of a given column as a standalone value.
interQuartileRange :: T.Text -> DataFrame -> Maybe Double
interQuartileRange = applyStatistic interQuartileRange'

-- | Calculates the Pearson's correlation coefficient between two given columns as a standalone value.
correlation :: T.Text -> T.Text -> DataFrame -> Maybe Double
correlation first second df = do
    f <- _getColumnAsDouble first df
    s <- _getColumnAsDouble second df
    correlation' f s

_getColumnAsDouble :: T.Text -> DataFrame -> Maybe (VU.Vector Double)
_getColumnAsDouble name df = case getColumn name df of
    Just (UnboxedColumn (f :: VU.Vector a)) -> case testEquality (typeRep @a) (typeRep @Double) of
        Just Refl -> Just f
        Nothing -> case sIntegral @a of
            STrue -> Just (VU.map fromIntegral f)
            SFalse -> case sFloating @a of
                STrue -> Just (VU.map realToFrac f)
                SFalse -> Nothing
    Nothing -> throw $ ColumnNotFoundException name "applyStatistic" (map fst $ M.toList $ columnIndices df)
    _ -> Nothing
{-# INLINE _getColumnAsDouble #-}

-- | Calculates the sum of a given column as a standalone value.
sum :: forall a. (Columnable a, Num a, VU.Unbox a) => T.Text -> DataFrame -> Maybe a
sum name df = case getColumn name df of
    Nothing -> throw $ ColumnNotFoundException name "sum" (map fst $ M.toList $ columnIndices df)
    Just ((UnboxedColumn (column :: VU.Vector a'))) -> case testEquality (typeRep @a') (typeRep @a) of
        Just Refl -> Just $ VG.sum column
        Nothing -> Nothing

applyStatistic :: (VU.Vector Double -> Double) -> T.Text -> DataFrame -> Maybe Double
applyStatistic f name df = join $ fmap apply (_getColumnAsDouble name (filterJust name df))
  where
    apply col =
        let
            res = (f col)
         in
            if isNaN res then Nothing else pure res
{-# INLINE applyStatistic #-}

applyStatistics :: (VU.Vector Double -> VU.Vector Double) -> T.Text -> DataFrame -> Maybe (VU.Vector Double)
applyStatistics f name df = fmap f (_getColumnAsDouble name (filterJust name df))

-- | Descriprive statistics of the numeric columns.
summarize :: DataFrame -> DataFrame
summarize df = fold columnStats (columnNames df) (fromNamedColumns [("Statistic", fromList ["Count" :: T.Text, "Mean", "Minimum", "25%", "Median", "75%", "Max", "StdDev", "IQR", "Skewness"])])
  where
    columnStats name d = if all isJust (stats name) then insertUnboxedVector name (VU.fromList (map (roundTo 2 . fromMaybe 0) $ stats name)) d else d
    stats name =
        let
            count = fromIntegral . numElements <$> getColumn name df
            quantiles = applyStatistics (quantiles' (VU.fromList [0, 1, 2, 3, 4]) 4) name df
            min' = flip (VG.!) 0 <$> quantiles
            quartile1 = flip (VG.!) 1 <$> quantiles
            median' = flip (VG.!) 2 <$> quantiles
            quartile3 = flip (VG.!) 3 <$> quantiles
            max' = flip (VG.!) 4 <$> quantiles
            iqr = (-) <$> quartile3 <*> quartile1
         in
            [ count
            , mean name df
            , min'
            , quartile1
            , median'
            , quartile3
            , max'
            , standardDeviation name df
            , iqr
            , skewness name df
            ]

-- | Round a @Double@ to Specified Precision
roundTo :: Int -> Double -> Double
roundTo n x = fromInteger (round $ x * (10 ^ n)) / (10.0 ^^ n)

toPct2dp :: Double -> String
toPct2dp x
    | x < 0.00005 = "<0.01%"
    | otherwise = printf "%.2f%%" (x * 100)
