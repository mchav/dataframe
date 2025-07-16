{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE FlexibleContexts #-}
module DataFrame.Operations.Aggregation where

import qualified Data.Set as S

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Map.Strict as MS
import qualified Data.Text as T
import qualified Data.Vector.Generic as VG
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as VM
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Algorithms.Merge as VA
import qualified Statistics.Quantile as SS
import qualified Statistics.Sample as SS

import Control.Exception (throw)
import Control.Monad (foldM_)
import Control.Monad.ST (runST)
import DataFrame.Internal.Column (Column(..), fromVector, getIndicesUnboxed, getIndices, Columnable)
import DataFrame.Internal.DataFrame (DataFrame(..), empty, getColumn)
import DataFrame.Internal.Parsing
import DataFrame.Internal.Types
import DataFrame.Errors
import DataFrame.Operations.Core
import DataFrame.Operations.Subset
import Data.Function ((&))
import Data.Hashable
import qualified Data.HashTable.ST.Basic as H
import Data.Maybe
import Data.Type.Equality (type (:~:)(Refl), TestEquality(..))
import Type.Reflection (typeRep, typeOf)

-- | O(k * n) groups the dataframe by the given rows aggregating the remaining rows
-- into vector that should be reduced later.
groupBy ::
  [T.Text] ->
  DataFrame ->
  DataFrame
groupBy names df
  | any (`notElem` columnNames df) names = throw $ ColumnNotFoundException (T.pack $ show $ names L.\\ columnNames df) "groupBy" (columnNames df)
  | otherwise = L.foldl' insertColumns initDf groupingColumns
  where
    indicesToGroup = M.elems $ M.filterWithKey (\k _ -> k `elem` names) (columnIndices df)
    rowRepresentations = VU.generate (fst (dimensions df)) (mkRowRep indicesToGroup df)

    valueIndices = V.fromList $ map (VG.map fst) $ VG.groupBy (\a b -> snd a == snd b) (runST $ do
      withIndexes <- VG.thaw $ VG.indexed rowRepresentations
      VA.sortBy (\(a, b) (a', b') -> compare b b') withIndexes
      VG.unsafeFreeze withIndexes)

    -- These are the indexes of the grouping/key rows i.e the minimum elements
    -- of the list.
    keyIndices = VU.generate (VG.length valueIndices) (\i -> VG.minimum $ valueIndices VG.! i)
    -- this will be our main worker function in the fold that takes all
    -- indices and replaces each value in a column with a list of
    -- the elements with the indices where the grouped row
    -- values are the same.
    insertColumns = groupColumns valueIndices df
    -- Out initial DF will just be all the grouped rows added to an
    -- empty dataframe. The entries are dedued and are in their
    -- initial order.
    initDf = L.foldl' (mkGroupedColumns keyIndices df) empty names
    -- All the rest of the columns that we are grouping by.
    groupingColumns = columnNames df L.\\ names

mkRowRep :: [Int] -> DataFrame -> Int -> Int
mkRowRep groupColumnIndices df i = hash (map mkHash groupColumnIndices)
  where
    getHashedElem (BoxedColumn (c :: V.Vector a)) j = hash' @a (c V.! j)
    getHashedElem (UnboxedColumn (c :: VU.Vector a)) j = hash' @a (c VU.! j)
    getHashedElem (OptionalColumn (c :: V.Vector a)) j = hash' @a (c V.! j)
    getHashedElem _ _ = 0
    mkHash j = getHashedElem ((V.!) (columns df) j) i 

-- | This hash function returns the hash when given a non numeric type but
-- the value when given a numeric.
hash' :: Columnable a => a -> Double
hash' value = case testEquality (typeOf value) (typeRep @Double) of
  Just Refl -> value
  Nothing -> case testEquality (typeOf value) (typeRep @Int) of
    Just Refl -> fromIntegral value
    Nothing -> case testEquality (typeOf value) (typeRep @T.Text) of
      Just Refl -> fromIntegral $ hash value
      Nothing -> fromIntegral $ hash (show value)

mkGroupedColumns :: VU.Vector Int -> DataFrame -> DataFrame -> T.Text -> DataFrame
mkGroupedColumns indices df acc name =
  case (V.!) (columns df) (columnIndices df M.! name) of
    BoxedColumn column ->
      let vs = indices `getIndices` column
       in insertVector name vs acc
    OptionalColumn column ->
      let vs = indices `getIndices` column
       in insertVector name vs acc
    UnboxedColumn column ->
      let vs = indices `getIndicesUnboxed` column
       in insertUnboxedVector name vs acc

groupColumns :: V.Vector (VU.Vector Int) -> DataFrame -> DataFrame -> T.Text -> DataFrame
groupColumns indices df acc name =
  case (V.!) (columns df) (columnIndices df M.! name) of
    BoxedColumn column ->
      let vs = V.map (`getIndices` column) indices
       in insertColumn name (GroupedBoxedColumn vs) acc
    OptionalColumn column ->
      let vs = V.map (`getIndices` column) indices
       in insertColumn name (GroupedBoxedColumn vs) acc
    UnboxedColumn column ->
      let vs = V.map (`getIndicesUnboxed` column) indices
       in insertColumn name (GroupedUnboxedColumn vs) acc

data Aggregation = Count
                 | Mean
                 | Minimum
                 | Median
                 | Maximum
                 | Sum deriving (Show, Eq)

groupByAgg :: Aggregation -> [T.Text] -> DataFrame -> DataFrame
groupByAgg agg columnNames df = let
  in case agg of
    Count -> insertVectorWithDefault @Int 1 (T.pack (show agg)) V.empty df
           & groupBy columnNames
           & reduceBy @Int VG.length "Count"
    _ -> error "UNIMPLEMENTED"

-- O (k * n) Reduces a vector valued volumn with a given function.
reduceBy ::
  forall a b . (Columnable a, Columnable b) =>
  (forall v . (VG.Vector v a) => v a -> b) ->
  T.Text ->
  DataFrame ->
  DataFrame
reduceBy f name df = case getColumn name df of
    Just ((GroupedBoxedColumn (column :: V.Vector (V.Vector a')))) -> case testEquality (typeRep @a) (typeRep @a') of
      Just Refl -> insertColumn name (fromVector (VG.map f column)) df
      Nothing -> error "Type error"
    Just ((GroupedUnboxedColumn (column :: V.Vector (VU.Vector a')))) -> case testEquality (typeRep @a) (typeRep @a') of
      Just Refl -> insertColumn name (fromVector (VG.map f column)) df
      Nothing -> error "Type error"
    _ -> error "Column is ungrouped"

reduceByAgg :: Aggregation
            -> T.Text
            -> DataFrame
            -> DataFrame
reduceByAgg agg name df = case agg of
  Count   -> case getColumn name df of
    Just ((GroupedBoxedColumn (column :: V.Vector (V.Vector a')))) ->  insertColumn name (fromVector (VG.map VG.length column)) df
    Just ((GroupedUnboxedColumn (column :: V.Vector (VU.Vector a')))) ->  insertColumn name (fromVector (VG.map VG.length column)) df
    _ -> error $ "Cannot count ungrouped Column: " ++ T.unpack name 
  Mean    -> case getColumn name df of
    Just ((GroupedBoxedColumn (column :: V.Vector (V.Vector a')))) -> case testEquality (typeRep @a') (typeRep @Int) of
      Just Refl -> insertColumn name (fromVector (VG.map (SS.mean . VG.map fromIntegral) column)) df
      Nothing -> case testEquality (typeRep @a') (typeRep @Double) of
        Just Refl -> insertColumn name (fromVector (VG.map SS.mean column)) df
        Nothing -> case testEquality (typeRep @a') (typeRep @Float) of
          Just Refl -> insertColumn name (fromVector (VG.map (SS.mean . VG.map realToFrac) column)) df
          Nothing -> error $ "Cannot get mean of non-numeric column: " ++ T.unpack name -- Not sure what to do with no numeric - return nothing???
    Just ((GroupedUnboxedColumn (column :: V.Vector (VU.Vector a')))) -> case testEquality (typeRep @a') (typeRep @Int) of
      Just Refl -> insertColumn name (fromVector (VG.map (SS.mean . VG.map fromIntegral) column)) df
      Nothing -> case testEquality (typeRep @a') (typeRep @Double) of
        Just Refl -> insertColumn name (fromVector (VG.map SS.mean column)) df
        Nothing -> case testEquality (typeRep @a') (typeRep @Float) of
          Just Refl -> insertColumn name (fromVector (VG.map (SS.mean . VG.map realToFrac) column)) df
          Nothing -> error $ "Cannot get mean of non-numeric column: " ++ T.unpack name -- Not sure what to do with no numeric - return nothing???
  Minimum -> case getColumn name df of
    Just ((GroupedBoxedColumn (column :: V.Vector (V.Vector a')))) ->  insertColumn name (fromVector (VG.map VG.minimum column)) df
    Just ((GroupedUnboxedColumn (column :: V.Vector (VU.Vector a')))) ->  insertColumn name (fromVector (VG.map VG.minimum column)) df
  Maximum -> case getColumn name df of
    Just ((GroupedBoxedColumn (column :: V.Vector (V.Vector a')))) ->  insertColumn name (fromVector (VG.map VG.maximum column)) df
    Just ((GroupedUnboxedColumn (column :: V.Vector (VU.Vector a')))) ->  insertColumn name (fromVector (VG.map VG.maximum column)) df
  Sum -> case getColumn name df of
    Just ((GroupedBoxedColumn (column :: V.Vector (V.Vector a')))) -> case testEquality (typeRep @a') (typeRep @Int) of
      Just Refl -> insertColumn name (fromVector (VG.map VG.sum column)) df
      Nothing -> case testEquality (typeRep @a') (typeRep @Double) of
        Just Refl -> insertColumn name (fromVector (VG.map VG.sum column)) df
        Nothing -> error $ "Cannot get sum of non-numeric column: " ++ T.unpack name -- Not sure what to do with no numeric - return nothing???
    Just ((GroupedUnboxedColumn (column :: V.Vector (VU.Vector a')))) -> case testEquality (typeRep @a') (typeRep @Int) of
      Just Refl -> insertColumn name (fromVector (VG.map VG.sum column)) df
      Nothing -> case testEquality (typeRep @a') (typeRep @Double) of
        Just Refl -> insertColumn name (fromVector (VG.map VG.sum column)) df
        Nothing -> error $ "Cannot get sum of non-numeric column: " ++ T.unpack name -- Not sure what to do with no numeric - return nothing???
  _ -> error "UNIMPLEMENTED"

aggregate :: [(T.Text, Aggregation)] -> DataFrame -> DataFrame
aggregate aggs df = let
    f (name, agg) d = cloneColumn name alias d & reduceByAgg agg alias
      where alias = (T.pack . show) agg <> "_" <> name 
  in fold f aggs df & exclude (map fst aggs)


appendWithFrontMin :: (Ord a) => a -> [a] -> [a]
appendWithFrontMin x [] = [x]
appendWithFrontMin x xs@(f:rest)
  | x < f = x:xs
  | otherwise = f:x:rest
{-# INLINE appendWithFrontMin #-}

distinct :: DataFrame -> DataFrame
distinct df = groupBy (columnNames df) df
