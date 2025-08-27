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
import qualified Statistics.Quantile as SS
import qualified Statistics.Sample as SS

import Prelude as P

import Control.Exception (throw)
import Control.Monad.ST (runST)
import qualified Data.Bifunctor as Data
import Data.Foldable (asum)
import Data.Function ((&))
import Data.Maybe (fromMaybe, isJust)
import Data.Type.Equality (TestEquality (testEquality), type (:~:) (Refl))
import DataFrame.Errors (DataFrameException (..))
import DataFrame.Internal.Column
import DataFrame.Internal.DataFrame (DataFrame (..), empty, getColumn, unsafeGetColumn)
import DataFrame.Internal.Row (showValue, toAny)
import DataFrame.Operations.Core
import DataFrame.Operations.Subset (filterJust)
import GHC.Float (int2Double)
import Text.Printf (printf)
import Type.Reflection (typeRep)

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

mean :: T.Text -> DataFrame -> Maybe Double
mean = applyStatistic mean'

median :: T.Text -> DataFrame -> Maybe Double
median = applyStatistic median'

standardDeviation :: T.Text -> DataFrame -> Maybe Double
standardDeviation = applyStatistic (sqrt . variance')

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
            Nothing -> case testEquality (typeRep @a) (typeRep @Float) of
                Just Refl -> Just $ VU.map realToFrac f
                Nothing -> Nothing
    _ -> Nothing
{-# INLINE _getColumnAsDouble #-}

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
        Just Refl -> Just (f col)
        Nothing -> do
            col' <- _getColumnAsDouble name df
            pure (f col')
    _ -> Nothing
{-# INLINE applyStatistic #-}

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
summarize df = fold columnStats (columnNames df) (fromNamedColumns [("Statistic", fromList ["Count" :: T.Text, "Mean", "Minimum", "25%", "Median", "75%", "Max", "StdDev", "IQR", "Skewness"])])
  where
    columnStats name d = if all isJust (stats name) then insertUnboxedVector name (VU.fromList (map (roundTo 2 . fromMaybe 0) $ stats name)) d else d
    stats name =
        let
            count = fromIntegral . numElements <$> getColumn name df
            quantiles = applyStatistics (SS.quantilesVec SS.medianUnbiased (VU.fromList [0, 1, 2, 3, 4]) 4) name df
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

mean' :: VU.Vector Double -> Double
mean' samp = VU.sum samp / fromIntegral (VU.length samp)
{-# INLINE mean #-}

median' :: VU.Vector Double -> Double
median' samp
    | VU.null samp = throw $ EmptyDataSetException "median"
    | otherwise    = runST $ do
        mutableSamp <- VU.thaw samp
        VA.sort mutableSamp
        let len = VU.length samp
            middleIndex = len `div` 2
        middleElement <- VUM.read mutableSamp middleIndex
        if odd len then pure middleElement
        else do
            prev <-VUM.read mutableSamp (middleIndex - 1)
            pure ((middleElement + prev) / 2)
{-# INLINE median' #-}

-- accumulator: count, mean, m2
data VarAcc = VarAcc !Int !Double !Double deriving (Show)

step :: VarAcc -> Double -> VarAcc
step (VarAcc !n !mean !m2) !x =
    let !n' = n + 1
        !delta = x - mean
        !mean' = mean + delta / fromIntegral n'
        !m2' = m2 + delta * (x - mean')
     in VarAcc n' mean' m2'
{-# INLINE step #-}

computeVariance :: VarAcc -> Double
computeVariance (VarAcc !n _ !m2)
    | n < 2 = 0 -- or error "variance of <2 samples"
    | otherwise = m2 / fromIntegral (n - 1)
{-# INLINE computeVariance #-}

variance' :: VU.Vector Double -> Double
variance' = computeVariance . VU.foldl' step (VarAcc 0 0 0)
{-# INLINE variance' #-}
