{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Operations.Statistics where

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU

import Prelude as P

import Control.Exception (throw)
import Data.Function ((&))
import Data.Maybe (fromMaybe, isJust)
import Data.Type.Equality (TestEquality (testEquality), type (:~:) (Refl))
import DataFrame.Errors (DataFrameException (..))
import DataFrame.Internal.Column
import DataFrame.Internal.DataFrame (
    DataFrame (..),
    columnAsUnboxedVector,
    empty,
    getColumn,
 )
import DataFrame.Internal.Expression
import DataFrame.Internal.Row (showValue, toAny)
import DataFrame.Internal.Statistics
import DataFrame.Internal.Types
import DataFrame.Operations.Core
import DataFrame.Operations.Subset (filterJust)
import Text.Printf (printf)
import Type.Reflection (typeRep)

{- | Show a frequency table for a categorical feaure.

__Examples:__

@
ghci> df <- D.readCsv ".\/data\/housing.csv"

ghci> D.frequencies "ocean_proximity" df

---------------------------------------------------------------------
   Statistic    | <1H OCEAN | INLAND | ISLAND | NEAR BAY | NEAR OCEAN
----------------|-----------|--------|--------|----------|-----------
      Text      |    Any    |  Any   |  Any   |   Any    |    Any
----------------|-----------|--------|--------|----------|-----------
 Count          | 9136      | 6551   | 5      | 2290     | 2658
 Percentage (%) | 44.26%    | 31.74% | 0.02%  | 11.09%   | 12.88%
@
-}
frequencies :: T.Text -> DataFrame -> DataFrame
frequencies name df =
    let
        counts :: forall a. (Columnable a) => [(a, Int)]
        counts = valueCounts (Col @a name) df
        calculatePercentage cs k = toAny $ toPct2dp (fromIntegral k / fromIntegral (P.sum $ map snd cs))
        initDf =
            empty
                & insertVector "Statistic" (V.fromList ["Count" :: T.Text, "Percentage (%)"])
        freqs :: forall v a. (VG.Vector v a, Columnable a) => v a -> DataFrame
        freqs col =
            L.foldl'
                ( \d (col, k) ->
                    insertVector
                        (showValue @a col)
                        (V.fromList [toAny k, calculatePercentage (counts @a) k])
                        d
                )
                initDf
                counts
     in
        case getColumn name df of
            Nothing ->
                throw $ ColumnNotFoundException name "frequencies" (M.keys $ columnIndices df)
            Just ((BoxedColumn (column :: V.Vector a))) -> freqs column
            Just ((OptionalColumn (column :: V.Vector a))) -> freqs column
            Just ((UnboxedColumn (column :: VU.Vector a))) -> freqs column

-- | Calculates the mean of a given column as a standalone value.
mean ::
    forall a. (Columnable a, Real a, VU.Unbox a) => Expr a -> DataFrame -> Double
mean (Col name) df = case columnAsUnboxedVector @a name df of
    Right xs -> mean' xs
    Left e -> throw e
mean expr df = case interpret df expr of
    Left e -> throw e
    Right (TColumn col) -> case toUnboxedVector @a col of
        Left e -> throw e
        Right xs -> mean' xs

-- | Calculates the median of a given column as a standalone value.
median ::
    forall a. (Columnable a, Real a, VU.Unbox a) => Expr a -> DataFrame -> Double
median (Col name) df = case columnAsUnboxedVector @a name df of
    Right xs -> median' xs
    Left e -> throw e
median expr df = case interpret df expr of
    Left e -> throw e
    Right (TColumn col) -> case toUnboxedVector @a col of
        Left e -> throw e
        Right xs -> median' xs

-- | Calculates the standard deviation of a given column as a standalone value.
standardDeviation ::
    forall a. (Columnable a, Real a, VU.Unbox a) => Expr a -> DataFrame -> Double
standardDeviation (Col name) df = case columnAsUnboxedVector @a name df of
    Right xs -> (sqrt . variance') xs
    Left e -> throw e
standardDeviation expr df = case interpret df expr of
    Left e -> throw e
    Right (TColumn col) -> case toUnboxedVector @a col of
        Left e -> throw e
        Right xs -> (sqrt . variance') xs

-- | Calculates the skewness of a given column as a standalone value.
skewness ::
    forall a. (Columnable a, Real a, VU.Unbox a) => Expr a -> DataFrame -> Double
skewness (Col name) df = case columnAsUnboxedVector @a name df of
    Right xs -> skewness' xs
    Left e -> throw e
skewness expr df = case interpret df expr of
    Left e -> throw e
    Right (TColumn col) -> case toUnboxedVector @a col of
        Left e -> throw e
        Right xs -> skewness' xs

-- | Calculates the variance of a given column as a standalone value.
variance ::
    forall a. (Columnable a, Real a, VU.Unbox a) => Expr a -> DataFrame -> Double
variance (Col name) df = case columnAsUnboxedVector @a name df of
    Right xs -> variance' xs
    Left e -> throw e
variance expr df = case interpret df expr of
    Left e -> throw e
    Right (TColumn col) -> case toUnboxedVector @a col of
        Left e -> throw e
        Right xs -> variance' xs

-- | Calculates the inter-quartile range of a given column as a standalone value.
interQuartileRange ::
    forall a. (Columnable a, Real a, VU.Unbox a) => Expr a -> DataFrame -> Double
interQuartileRange (Col name) df = case columnAsUnboxedVector @a name df of
    Right xs -> interQuartileRange' xs
    Left e -> throw e
interQuartileRange expr df = case interpret df expr of
    Left e -> throw e
    Right (TColumn col) -> case toUnboxedVector @a col of
        Left e -> throw e
        Right xs -> interQuartileRange' xs

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
    Nothing ->
        throw $
            ColumnNotFoundException name "applyStatistic" (M.keys $ columnIndices df)
    _ -> Nothing
{-# INLINE _getColumnAsDouble #-}

-- | Calculates the sum of a given column as a standalone value.
sum ::
    forall a. (Columnable a, Num a) => Expr a -> DataFrame -> a
sum (Col name) df = case getColumn name df of
    Nothing -> throw $ ColumnNotFoundException name "sum" (M.keys $ columnIndices df)
    Just ((UnboxedColumn (column :: VU.Vector a'))) -> case testEquality (typeRep @a') (typeRep @a) of
        Just Refl -> VG.sum column
        Nothing -> 0
    Just ((BoxedColumn (column :: V.Vector a'))) -> case testEquality (typeRep @a') (typeRep @a) of
        Just Refl -> VG.sum column
        Nothing -> 0
    Just ((OptionalColumn (column :: V.Vector (Maybe a')))) -> case testEquality (typeRep @a') (typeRep @a) of
        Just Refl -> VG.sum (VG.map (fromMaybe 0) column)
        Nothing -> 0
sum expr df = case interpret df expr of
    Left e -> throw e
    Right (TColumn xs) -> case toVector @a @V.Vector xs of
        Left e -> throw e
        Right xs -> VG.sum xs

applyStatistic ::
    (VU.Vector Double -> Double) -> T.Text -> DataFrame -> Maybe Double
applyStatistic f name df = apply =<< _getColumnAsDouble name (filterJust name df)
  where
    apply col =
        let
            res = f col
         in
            if isNaN res then Nothing else pure res
{-# INLINE applyStatistic #-}

applyStatistics ::
    (VU.Vector Double -> VU.Vector Double) ->
    T.Text ->
    DataFrame ->
    Maybe (VU.Vector Double)
applyStatistics f name df = fmap f (_getColumnAsDouble name (filterJust name df))

-- | Descriptive statistics of the numeric columns.
summarize :: DataFrame -> DataFrame
summarize df =
    fold
        columnStats
        (columnNames df)
        ( fromNamedColumns
            [
                ( "Statistic"
                , fromList
                    [ "Count" :: T.Text
                    , "Mean"
                    , "Minimum"
                    , "25%"
                    , "Median"
                    , "75%"
                    , "Max"
                    , "StdDev"
                    , "IQR"
                    , "Skewness"
                    ]
                )
            ]
        )
  where
    columnStats name d =
        if all isJust (stats name)
            then
                insertUnboxedVector
                    name
                    (VU.fromList (map (roundTo 2 . fromMaybe 0) $ stats name))
                    d
            else d
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
            , mean' <$> _getColumnAsDouble name df
            , min'
            , quartile1
            , median'
            , quartile3
            , max'
            , sqrt . variance' <$> _getColumnAsDouble name df
            , iqr
            , skewness' <$> _getColumnAsDouble name df
            ]

-- | Round a @Double@ to Specified Precision
roundTo :: Int -> Double -> Double
roundTo n x = fromInteger (round $ x * 10 ^ n) / 10.0 ^^ n

toPct2dp :: Double -> String
toPct2dp x
    | x < 0.00005 = "<0.01%"
    | otherwise = printf "%.2f%%" (x * 100)
