{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE FlexibleContexts #-}
module DataFrame.Operations.Aggregation where

import qualified Data.Set as S
import qualified DataFrame.Functions as F

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
import DataFrame.Internal.Column (Column(..), fromVector,
                                  getIndicesUnboxed, getIndices, 
                                  Columnable, unwrapTypedColumn,
                                  columnVersionString)
import DataFrame.Internal.DataFrame (DataFrame(..), empty, getColumn, unsafeGetColumn)
import DataFrame.Internal.Expression
import DataFrame.Internal.Parsing
import DataFrame.Internal.Types
import DataFrame.Errors
import DataFrame.Operations.Core
import DataFrame.Operations.Subset
import Data.Function ((&))
import Data.Hashable
import Data.List ((\\))
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
mkRowRep groupColumnIndices df i = if length h == 1 then head h else hash h
  where
    h = (map mkHash groupColumnIndices)
    getHashedElem :: Column -> Int -> Int
    getHashedElem (BoxedColumn (c :: V.Vector a)) j = hash' @a (c V.! j)
    getHashedElem (UnboxedColumn (c :: VU.Vector a)) j = hash' @a (c VU.! j)
    getHashedElem (OptionalColumn (c :: V.Vector a)) j = hash' @a (c V.! j)
    getHashedElem _ _ = 0
    mkHash j = getHashedElem ((V.!) (columns df) j) i 

-- | This hash function returns the hash when given a non numeric type but
-- the value when given a numeric.
hash' :: Columnable a => a -> Int
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

aggregate :: [(T.Text, UExpr)] -> DataFrame -> DataFrame
aggregate aggs df = let
    groupingColumns = Prelude.filter (\c -> not $ T.isPrefixOf "Grouped" (T.pack $ columnVersionString (fromMaybe (error "Unexpected") (getColumn c df)))) (columnNames df)
    df' = select groupingColumns df
    f (name, Wrap (expr :: Expr a)) d = let
        value = interpret @a df expr
      in insertColumn name (unwrapTypedColumn value) d
  in fold f aggs df'

distinct :: DataFrame -> DataFrame
distinct df = groupBy (columnNames df) df

distinctBy :: [T.Text] -> DataFrame -> DataFrame
distinctBy names df = let
    excluded = (columnNames df) \\ names
    distinctColumns = groupBy names df
    aggF name = case unsafeGetColumn name distinctColumns of
      GroupedBoxedColumn (column :: V.Vector (V.Vector a)) -> (F.anyValue (F.col @a name)) `F.as` name
      GroupedUnboxedColumn (column :: V.Vector (VU.Vector a)) -> (F.anyValue (F.col @a name)) `F.as` name
      _ -> error $ "Column isn't grouped: " ++ (T.unpack name)
  in aggregate (map aggF excluded) distinctColumns
