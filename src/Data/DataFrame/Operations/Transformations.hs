{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
module Data.DataFrame.Operations.Transformations where

import qualified Data.List as L
import qualified Data.Text as T
import qualified Data.Map as M
import qualified Data.Vector.Generic as VG
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import Control.Exception (throw)
import Data.DataFrame.Errors (DataFrameException(..))
import Data.DataFrame.Internal.Column (Column(..), columnTypeString, itransform, ifoldrColumn)
import Data.DataFrame.Internal.DataFrame (DataFrame(..), getColumn)
import Data.DataFrame.Internal.Function (Function(..), funcApply)
import Data.DataFrame.Internal.Row (mkRowFromArgs)
import Data.DataFrame.Internal.Types (Columnable, RowValue, toRowValue, transform)
import Data.DataFrame.Operations.Core
import Data.Maybe
import Type.Reflection (typeRep, typeOf)

-- | O(k) Apply a function to a given column in a dataframe.
apply ::
  forall b c.
  (Columnable b, Columnable c) =>
  -- | function to apply
  (b -> c) ->
  -- | Column name
  T.Text ->
  -- | DataFrame to apply operation to
  DataFrame ->
  DataFrame
apply f columnName d = case getColumn columnName d of
  Nothing -> throw $ ColumnNotFoundException columnName "apply" (map fst $ M.toList $ columnIndices d)
  Just column -> case transform f column of
    Nothing -> throw $ TypeMismatchException' (typeRep @b) (columnTypeString column) columnName "apply"
    column' -> insertColumn' columnName column' d

-- | O(k) Apply a function to a combination of columns in a dataframe and
-- add the result into `alias` column.
deriveFrom :: ([T.Text], Function) -> T.Text -> DataFrame -> DataFrame
deriveFrom (args, f) name df = case f of
  (F4 (f' :: a -> b -> c -> d -> e)) -> let
      xs = VG.map (\row -> funcApply @e row f) $ V.generate (fst (dimensions df)) (mkRowFromArgs args df)
    in insertColumn name xs df
  (F3 (f' :: a -> b -> c -> d)) -> let
      xs = VG.map (\row -> funcApply @d row f) $ V.generate (fst (dimensions df)) (mkRowFromArgs args df)
    in insertColumn name xs df
  (F2 (f' :: a -> b -> c)) -> let
      xs = VG.map (\row -> funcApply @c row f) $ V.generate (fst (dimensions df)) (mkRowFromArgs args df)
    in insertColumn name xs df
  (F1 (f' :: a -> b)) -> let
      xs = VG.map (\row -> funcApply @b row f) $ V.generate (fst (dimensions df)) (mkRowFromArgs args df)
    in insertColumn name xs df

-- | O(k) Apply a function to a given column in a dataframe and
-- add the result into alias column.

derive ::
  forall b c.
  (Columnable b, Columnable c) =>
  -- | New name
  T.Text ->
  -- | function to apply
  (b -> c) ->
  -- | Derivative column name
  T.Text ->
  -- | DataFrame to apply operation to
  DataFrame ->
  DataFrame
derive alias f columnName d = case getColumn columnName d of
  Nothing -> throw $ ColumnNotFoundException columnName "derive" (map fst $ M.toList $ columnIndices d)
  Just column -> case transform f column of
    Nothing  -> throw $ TypeMismatchException (typeOf column) (typeRep @b) columnName "derive"
    Just res -> insertColumn' alias (Just res) d

-- | O(k * n) Apply a function to given column names in a dataframe.
applyMany ::
  (Columnable b, Columnable c) =>
  (b -> c) ->
  [T.Text] ->
  DataFrame ->
  DataFrame
applyMany f names df = L.foldl' (flip (apply f)) df names

-- | O(k) Convenience function that applies to an int column.
applyInt ::
  (Columnable b) =>
  -- | Column name
  -- | function to apply
  (Int -> b) ->
  T.Text ->
  -- | DataFrame to apply operation to
  DataFrame ->
  DataFrame
applyInt = apply

-- | O(k) Convenience function that applies to an double column.
applyDouble ::
  (Columnable b) =>
  -- | Column name
  -- | function to apply
  (Double -> b) ->
  T.Text ->
  -- | DataFrame to apply operation to
  DataFrame ->
  DataFrame
applyDouble = apply

-- | O(k * n) Apply a function to a column only if there is another column
-- value that matches the given criterion.
--
-- > applyWhere "Age" (<20) "Generation" (const "Gen-Z")
applyWhere ::
  forall a b .
  (Columnable a, Columnable b) =>
  (a -> Bool) -> -- Filter condition
  T.Text -> -- Criterion Column
  (b -> b) -> -- function to apply
  T.Text -> -- Column name
  DataFrame -> -- DataFrame to apply operation to
  DataFrame
applyWhere condition filterColumnName f columnName df = case getColumn filterColumnName df of
  Nothing -> throw $ ColumnNotFoundException filterColumnName "applyWhere" (map fst $ M.toList $ columnIndices df)
  Just column -> case ifoldrColumn (\i val acc -> if condition val then V.cons i acc else acc) V.empty column of
      Nothing -> throw $ TypeMismatchException' (typeRep @a) (columnTypeString column) filterColumnName "applyWhere"
      Just indexes -> if V.null indexes
                      then df
                      else L.foldl' (\d i -> applyAtIndex i f columnName d) df indexes

-- | O(k) Apply a function to the column at a given index.
applyAtIndex ::
  forall a.
  (Columnable a) =>
  -- | Index
  Int ->
  -- | function to apply
  (a -> a) ->
  -- | Column name
  T.Text ->
  -- | DataFrame to apply operation to
  DataFrame ->
  DataFrame
applyAtIndex i f columnName df = case getColumn columnName df of
  Nothing -> throw $ ColumnNotFoundException columnName "applyAtIndex" (map fst $ M.toList $ columnIndices df)
  Just column -> case itransform (\index value -> if index == i then f value else value) column of
    Nothing -> throw $ TypeMismatchException' (typeRep @a) (columnTypeString column) columnName "applyAtIndex"
    column' -> insertColumn' columnName column' df

impute ::
  forall b .
  (Columnable b) =>
  T.Text    ->
  b         ->
  DataFrame ->
  DataFrame
impute columnName value df = case getColumn columnName df of
  Nothing -> throw $ ColumnNotFoundException columnName "impute" (map fst $ M.toList $ columnIndices df)
  Just (OptionalColumn _) -> apply (fromMaybe value) columnName df
  _ -> error "Cannot impute to a non-Empty column"
