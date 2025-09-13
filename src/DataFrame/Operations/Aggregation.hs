{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Operations.Aggregation where

import qualified Data.Set as S
import qualified DataFrame.Functions as F

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Map.Strict as MS
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Algorithms.Merge as VA
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Mutable as VM
import qualified Data.Vector.Unboxed as VU

import Control.Exception (throw)
import Control.Monad (foldM_)
import Control.Monad.ST (runST)
import Data.Function ((&))
import Data.Hashable
import Data.List ((\\))
import Data.Maybe
import Data.Type.Equality (TestEquality (..), type (:~:) (Refl))
import DataFrame.Errors
import DataFrame.Internal.Column (
    Column (..),
    Columnable,
    atIndicesStable,
    columnVersionString,
    fromVector,
    getIndices,
    getIndicesUnboxed,
    unwrapTypedColumn,
 )
import DataFrame.Internal.DataFrame (DataFrame (..), GroupedDataFrame (..), empty, getColumn, unsafeGetColumn)
import DataFrame.Internal.Expression
import DataFrame.Internal.Parsing
import DataFrame.Internal.Types
import DataFrame.Operations.Core
import DataFrame.Operations.Merge
import DataFrame.Operations.Subset
import Type.Reflection (typeOf, typeRep)

{- | O(k * n) groups the dataframe by the given rows aggregating the remaining rows
into vector that should be reduced later.
-}
groupBy ::
    [T.Text] ->
    DataFrame ->
    GroupedDataFrame
groupBy names df
    | any (`notElem` columnNames df) names = throw $ ColumnNotFoundException (T.pack $ show $ names L.\\ columnNames df) "groupBy" (columnNames df)
    | otherwise = Grouped df names (VG.map fst valueIndices) (VU.fromList (reverse (changingPoints valueIndices)))
  where
    indicesToGroup = M.elems $ M.filterWithKey (\k _ -> k `elem` names) (columnIndices df)
    rowRepresentations = VU.generate (fst (dimensions df)) (mkRowRep indicesToGroup df)

    valueIndices = runST $ do
        withIndexes <- VG.thaw $ VG.indexed rowRepresentations
        VA.sortBy (\(a, b) (a', b') -> compare b b') withIndexes
        VG.unsafeFreeze withIndexes

changingPoints :: (Eq a, VU.Unbox a) => VU.Vector (Int, a) -> [Int]
changingPoints vs = VG.length vs : (fst (VU.ifoldl findChangePoints initialState vs))
  where
    initialState = ([0], snd (VG.head vs))
    findChangePoints (offsets, currentVal) index (_, newVal)
        | currentVal == newVal = (offsets, currentVal)
        | otherwise = (index : offsets, newVal)

mkRowRep :: [Int] -> DataFrame -> Int -> Int
mkRowRep groupColumnIndices df i = case h of
    (x : []) -> x
    xs -> hash h
  where
    h = (map mkHash groupColumnIndices)
    getHashedElem :: Column -> Int -> Int
    getHashedElem (BoxedColumn (c :: V.Vector a)) j = hash' @a (c V.! j)
    getHashedElem (UnboxedColumn (c :: VU.Vector a)) j = hash' @a (c VU.! j)
    getHashedElem (OptionalColumn (c :: V.Vector a)) j = hash' @a (c V.! j)
    mkHash j = getHashedElem ((V.!) (columns df) j) i

{- | This hash function returns the hash when given a non numeric type but
the value when given a numeric.
-}
hash' :: (Columnable a) => a -> Int
hash' value = case testEquality (typeOf value) (typeRep @Double) of
    Just Refl -> round $ value * 1000
    Nothing -> case testEquality (typeOf value) (typeRep @Int) of
        Just Refl -> value
        Nothing -> case testEquality (typeOf value) (typeRep @T.Text) of
            Just Refl -> hash value
            Nothing -> hash (show value)

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

{- | Aggregate a grouped dataframe using the expressions given.
All ungrouped columns will be dropped.
-}
aggregate :: [(T.Text, UExpr)] -> GroupedDataFrame -> DataFrame
aggregate aggs gdf@(Grouped df groupingColumns valueIndices offsets) =
    let
        df' = selectIndices (VG.map (valueIndices VG.!) (VG.init offsets)) (select groupingColumns df)
        groupedColumns = columnNames df L.\\ groupingColumns
        f (name, Wrap (expr :: Expr a)) d =
            let
                value = interpretAggregation @a gdf expr
             in
                insertColumn name (unwrapTypedColumn value) d
     in
        fold f aggs df'

selectIndices :: VU.Vector Int -> DataFrame -> DataFrame
selectIndices xs df =
    df
        { columns = VG.map (atIndicesStable xs) (columns df)
        , dataframeDimensions = (VG.length xs, VG.length (columns df))
        }

-- | Filter out all non-unique values in a dataframe.
distinct :: DataFrame -> DataFrame
distinct df = selectIndices (VG.map (indices VG.!) (VG.init os)) df
  where
    (Grouped _ _ indices os) = groupBy (columnNames df) df
