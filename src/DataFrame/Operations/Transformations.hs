{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE FlexibleContexts #-}
module DataFrame.Operations.Transformations where

import qualified Data.List as L
import qualified Data.Text as T
import qualified Data.Map as M
import qualified Data.Vector.Generic as VG
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import Control.Exception (throw)
import DataFrame.Errors (DataFrameException(..), TypeErrorContext(..))
import DataFrame.Internal.Column (Column(..), columnTypeString, imapColumn, ifoldrColumn, TypedColumn (TColumn), Columnable, mapColumn, unwrapTypedColumn)
import DataFrame.Internal.DataFrame (DataFrame(..), getColumn)
import DataFrame.Internal.Expression
import DataFrame.Internal.Row (mkRowFromArgs, RowValue, toRowValue)
import DataFrame.Operations.Core
import Data.Maybe
import Type.Reflection (typeRep, typeOf, TypeRep)

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
apply f columnName d = case safeApply f columnName d of
  Left exception -> throw exception
  Right df       -> df

-- | O(k) Safe version of the apply function. Returns (instead of throwing) the error.
safeApply ::
  forall b c.
  (Columnable b, Columnable c) =>
  -- | function to apply
  (b -> c) ->
  -- | Column name
  T.Text ->
  -- | DataFrame to apply operation to
  DataFrame ->
  Either DataFrameException DataFrame
safeApply f columnName d = case getColumn columnName d of
  Nothing -> Left $ ColumnNotFoundException columnName "apply" (map fst $ M.toList $ columnIndices d)
  Just column -> case mapColumn f column of
    Nothing -> Left $ TypeMismatchException (MkTypeErrorContext
                                                    { userType = Right $ typeRep @b
                                                    , expectedType = Left (columnTypeString column) :: Either String (TypeRep ()) 
                                                    , errorColumnName = Just (T.unpack columnName)
                                                    , callingFunctionName = Just "apply"
                                                    })
    Just column' -> Right $ insertColumn columnName column' d

-- | O(k) Apply a function to a combination of columns in a dataframe and
-- add the result into `alias` column.
derive :: forall a . Columnable a => T.Text -> Expr a -> DataFrame -> DataFrame
derive name expr df = let
    value = interpret @a df expr
  in insertColumn name (unwrapTypedColumn value) df


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
      Nothing -> throw $ TypeMismatchException (MkTypeErrorContext
                                                        { userType = Right $ typeRep @a
                                                        , expectedType = Left (columnTypeString column) :: Either String (TypeRep ()) 
                                                        , errorColumnName = Just (T.unpack columnName)
                                                        , callingFunctionName = Just "applyWhere"
                                                        })
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
  Just column -> case imapColumn (\index value -> if index == i then f value else value) column of
    Nothing -> throw $ TypeMismatchException (MkTypeErrorContext
                                                        { userType = Right $ typeRep @a
                                                        , expectedType = Left (columnTypeString column) :: Either String (TypeRep ()) 
                                                        , errorColumnName = Just (T.unpack columnName)
                                                        , callingFunctionName = Just "applyAtIndex"
                                                        })
    Just column' -> insertColumn columnName column' df

impute ::
  forall b .
  (Columnable b) =>
  T.Text    ->
  b         ->
  DataFrame ->
  DataFrame
impute columnName value df = case getColumn columnName df of
  Nothing -> throw $ ColumnNotFoundException columnName "impute" (map fst $ M.toList $ columnIndices df)
  Just (OptionalColumn _) -> case safeApply (fromMaybe value) columnName df of
    Left (TypeMismatchException context) -> throw $ TypeMismatchException (context { callingFunctionName = Just "impute" })
    Left exception -> throw exception
    Right res      -> res
  _ -> error "Cannot impute to a non-Empty column"
