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
import qualified Statistics.Quantile as SS
import qualified Statistics.Sample as SS

import Control.Exception (throw)
import Control.Monad (foldM_)
import Control.Monad.ST (runST)
import DataFrame.Internal.Column (Column(..), toColumn', getIndicesUnboxed, getIndices, Columnable)
import DataFrame.Internal.DataFrame (DataFrame(..), empty, getColumn)
import DataFrame.Internal.Parsing
import DataFrame.Internal.Types
import DataFrame.Errors
import DataFrame.Operations.Core
import DataFrame.Operations.Subset
import Data.Function ((&))
import Data.Hashable
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
    insertOrAdjust k v m = if MS.notMember k m then MS.insert k [v] m else MS.adjust (appendWithFrontMin v) k m
    -- Create a string representation of each row.
    values = V.generate (fst (dimensions df)) (mkRowRep df (S.fromList names))
    -- Create a mapping from the row representation to the list of indices that
    -- have that row representation. This will allow us sortedIndexesto combine the indexes
    -- where the rows are the same.
    valueIndices = V.ifoldl' (\m index rowRep -> insertOrAdjust rowRep index m) M.empty values
    -- Since the min is at the head this allows us to get the min in constant time and sort by it
    -- That way we can recover the original order of the rows.
    -- valueIndicesInitOrder = L.sortBy (compare `on` snd) $! MS.toList $ MS.map VU.head valueIndices
    valueIndicesInitOrder = runST $ do
      v <- VM.new (MS.size valueIndices)
      foldM_ (\i idxs -> VM.write v i (VU.fromList idxs) >> return (i + 1)) 0 valueIndices
      V.unsafeFreeze v

    -- These are the indexes of the grouping/key rows i.e the minimum elements
    -- of the list.
    keyIndices = VU.generate (VG.length valueIndicesInitOrder) (\i -> VG.head $ valueIndicesInitOrder VG.! i)
    -- this will be our main worker function in the fold that takes all
    -- indices and replaces each value in a column with a list of
    -- the elements with the indices where the grouped row
    -- values are the same.
    insertColumns = groupColumns valueIndicesInitOrder df
    -- Out initial DF will just be all the grouped rows added to an
    -- empty dataframe. The entries are dedued and are in their
    -- initial order.
    initDf = L.foldl' (mkGroupedColumns keyIndices df) empty names
    -- All the rest of the columns that we are grouping by.
    groupingColumns = columnNames df L.\\ names

mkRowRep :: DataFrame -> S.Set T.Text -> Int -> Int
mkRowRep df names i = hash $ V.ifoldl' go [] (columns df)
  where
    indexMap = M.fromList (map (\(a, b) -> (b, a)) $ M.toList (columnIndices df))
    go acc k Nothing = acc
    go acc k (Just (BoxedColumn (c :: V.Vector a))) =
      if S.notMember (indexMap M.! k) names
        then acc
        else case c V.!? i of
          Just e -> hash' @a e : acc
          Nothing ->
            error $
              "Column "
                ++ T.unpack (indexMap M.! k)
                ++ " has less items than "
                ++ "the other columns at index "
                ++ show i
    go acc k (Just (OptionalColumn (c :: V.Vector (Maybe a)))) =
      if S.notMember (indexMap M.! k) names
        then acc
        else case c V.!? i of
          Just e -> hash' @(Maybe a) e : acc
          Nothing ->
            error $
              "Column "
                ++ T.unpack (indexMap M.! k)
                ++ " has less items than "
                ++ "the other columns at index "
                ++ show i
    go acc k (Just (UnboxedColumn (c :: VU.Vector a))) =
      if S.notMember (indexMap M.! k) names
        then acc
        else case c VU.!? i of
          Just e -> hash' @a e : acc
          Nothing ->
            error $
              "Column "
                ++ T.unpack (indexMap M.! k)
                ++ " has less items than "
                ++ "the other columns at index "
                ++ show i

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
    Nothing -> error "Unexpected"
    (Just (BoxedColumn column)) ->
      let vs = indices `getIndices` column
       in insertColumn name vs acc
    (Just (OptionalColumn column)) ->
      let vs = indices `getIndices` column
       in insertColumn name vs acc
    (Just (UnboxedColumn column)) ->
      let vs = indices `getIndicesUnboxed` column
       in insertUnboxedColumn name vs acc

groupColumns :: V.Vector (VU.Vector Int) -> DataFrame -> DataFrame -> T.Text -> DataFrame
groupColumns indices df acc name =
  case (V.!) (columns df) (columnIndices df M.! name) of
    Nothing -> df
    (Just (BoxedColumn column)) ->
      let vs = V.map (`getIndices` column) indices
       in insertColumn' name (Just $ GroupedBoxedColumn vs) acc
    (Just (OptionalColumn column)) ->
      let vs = V.map (`getIndices` column) indices
       in insertColumn' name (Just $ GroupedBoxedColumn vs) acc
    (Just (UnboxedColumn column)) ->
      let vs = V.map (`getIndicesUnboxed` column) indices
       in insertColumn' name (Just $ GroupedUnboxedColumn vs) acc

data Aggregation = Count
                 | Mean
                 | Minimum
                 | Median
                 | Maximum
                 | Sum deriving (Show, Eq)

groupByAgg :: Aggregation -> [T.Text] -> DataFrame -> DataFrame
groupByAgg agg columnNames df = let
  in case agg of
    Count -> insertColumnWithDefault @Int 1 (T.pack (show agg)) V.empty df
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
      Just Refl -> insertColumn' name (Just $ toColumn' (VG.map f column)) df
      Nothing -> error "Type error"
    Just ((GroupedUnboxedColumn (column :: V.Vector (VU.Vector a')))) -> case testEquality (typeRep @a) (typeRep @a') of
      Just Refl -> insertColumn' name (Just $ toColumn' (VG.map f column)) df
      Nothing -> error "Type error"
    _ -> error "Column is ungrouped"

reduceByAgg :: Aggregation
            -> T.Text
            -> DataFrame
            -> DataFrame
reduceByAgg agg name df = case agg of
  Count   -> case getColumn name df of
    Just ((GroupedBoxedColumn (column :: V.Vector (V.Vector a')))) ->  insertColumn' name (Just $ toColumn' (VG.map VG.length column)) df
    Just ((GroupedUnboxedColumn (column :: V.Vector (VU.Vector a')))) ->  insertColumn' name (Just $ toColumn' (VG.map VG.length column)) df
    _ -> error $ "Cannot count ungrouped Column: " ++ T.unpack name 
  Mean    -> case getColumn name df of
    Just ((GroupedBoxedColumn (column :: V.Vector (V.Vector a')))) -> case testEquality (typeRep @a') (typeRep @Int) of
      Just Refl -> insertColumn' name (Just $ toColumn' (VG.map (SS.mean . VG.map fromIntegral) column)) df
      Nothing -> case testEquality (typeRep @a') (typeRep @Double) of
        Just Refl -> insertColumn' name (Just $ toColumn' (VG.map SS.mean column)) df
        Nothing -> case testEquality (typeRep @a') (typeRep @Float) of
          Just Refl -> insertColumn' name (Just $ toColumn' (VG.map (SS.mean . VG.map realToFrac) column)) df
          Nothing -> error $ "Cannot get mean of non-numeric column: " ++ T.unpack name -- Not sure what to do with no numeric - return nothing???
    Just ((GroupedUnboxedColumn (column :: V.Vector (VU.Vector a')))) -> case testEquality (typeRep @a') (typeRep @Int) of
      Just Refl -> insertColumn' name (Just $ toColumn' (VG.map (SS.mean . VG.map fromIntegral) column)) df
      Nothing -> case testEquality (typeRep @a') (typeRep @Double) of
        Just Refl -> insertColumn' name (Just $ toColumn' (VG.map SS.mean column)) df
        Nothing -> case testEquality (typeRep @a') (typeRep @Float) of
          Just Refl -> insertColumn' name (Just $ toColumn' (VG.map (SS.mean . VG.map realToFrac) column)) df
          Nothing -> error $ "Cannot get mean of non-numeric column: " ++ T.unpack name -- Not sure what to do with no numeric - return nothing???
  Minimum -> case getColumn name df of
    Just ((GroupedBoxedColumn (column :: V.Vector (V.Vector a')))) ->  insertColumn' name (Just $ toColumn' (VG.map VG.minimum column)) df
    Just ((GroupedUnboxedColumn (column :: V.Vector (VU.Vector a')))) ->  insertColumn' name (Just $ toColumn' (VG.map VG.minimum column)) df
  Maximum -> case getColumn name df of
    Just ((GroupedBoxedColumn (column :: V.Vector (V.Vector a')))) ->  insertColumn' name (Just $ toColumn' (VG.map VG.maximum column)) df
    Just ((GroupedUnboxedColumn (column :: V.Vector (VU.Vector a')))) ->  insertColumn' name (Just $ toColumn' (VG.map VG.maximum column)) df
  Sum -> case getColumn name df of
    Just ((GroupedBoxedColumn (column :: V.Vector (V.Vector a')))) -> case testEquality (typeRep @a') (typeRep @Int) of
      Just Refl -> insertColumn' name (Just $ toColumn' (VG.map VG.sum column)) df
      Nothing -> case testEquality (typeRep @a') (typeRep @Double) of
        Just Refl -> insertColumn' name (Just $ toColumn' (VG.map VG.sum column)) df
        Nothing -> error $ "Cannot get sum of non-numeric column: " ++ T.unpack name -- Not sure what to do with no numeric - return nothing???
    Just ((GroupedUnboxedColumn (column :: V.Vector (VU.Vector a')))) -> case testEquality (typeRep @a') (typeRep @Int) of
      Just Refl -> insertColumn' name (Just $ toColumn' (VG.map VG.sum column)) df
      Nothing -> case testEquality (typeRep @a') (typeRep @Double) of
        Just Refl -> insertColumn' name (Just $ toColumn' (VG.map VG.sum column)) df
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
