{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- | Converts a parquet schema into an empty 'DataFrame'.
module DataFrame.IO.Parquet.Schema (
    schemaToEmptyDataFrame,
    schemaElemToColumn,
    emptyColumnForType,
    emptyNullableColumnForType,
) where

import Data.Int (Int32, Int64)
import qualified Data.Maybe as Maybe
import qualified Data.Set as S
import qualified Data.Text as T

import DataFrame.IO.Parquet.Thrift (
    SchemaElement,
    ThriftType (..),
    name,
    num_children,
    schematype,
    unField,
 )
import DataFrame.Internal.Column (Column, fromList)
import DataFrame.Internal.DataFrame (DataFrame, fromColumns)

-- | Build an empty 'DataFrame' from a flat list of 'SchemaElement's.
schemaToEmptyDataFrame :: S.Set T.Text -> [SchemaElement] -> DataFrame
schemaToEmptyDataFrame nullableCols elems =
    let leafElems =
            filter (\e -> Maybe.fromMaybe 0 (unField e.num_children) == 0) elems
     in fromColumns (map (schemaElemToColumn nullableCols) leafElems)

-- | Convert a single 'SchemaElement' into a named empty 'Column'.
schemaElemToColumn :: S.Set T.Text -> SchemaElement -> (T.Text, Column)
schemaElemToColumn nullableCols element =
    let colName = unField element.name
        isNull = colName `S.member` nullableCols
        column =
            if isNull
                then emptyNullableColumnForType (unField element.schematype)
                else emptyColumnForType (unField element.schematype)
     in (colName, column)

-- | An empty 'Column' of the given parquet physical type.
emptyColumnForType :: Maybe ThriftType -> Column
emptyColumnForType = \case
    Just (BOOLEAN _) -> fromList @Bool []
    Just (INT32 _) -> fromList @Int32 []
    Just (INT64 _) -> fromList @Int64 []
    Just (INT96 _) -> fromList @Int64 []
    Just (FLOAT _) -> fromList @Float []
    Just (DOUBLE _) -> fromList @Double []
    Just (BYTE_ARRAY _) -> fromList @T.Text []
    Just (FIXED_LEN_BYTE_ARRAY _) -> fromList @T.Text []
    other -> error $ "Unsupported parquet type for column: " <> show other

-- | Like 'emptyColumnForType' but produces a nullable @Maybe a@ column.
emptyNullableColumnForType :: Maybe ThriftType -> Column
emptyNullableColumnForType = \case
    Just (BOOLEAN _) -> fromList @(Maybe Bool) []
    Just (INT32 _) -> fromList @(Maybe Int32) []
    Just (INT64 _) -> fromList @(Maybe Int64) []
    Just (INT96 _) -> fromList @(Maybe Int64) []
    Just (FLOAT _) -> fromList @(Maybe Float) []
    Just (DOUBLE _) -> fromList @(Maybe Double) []
    Just (BYTE_ARRAY _) -> fromList @(Maybe T.Text) []
    Just (FIXED_LEN_BYTE_ARRAY _) -> fromList @(Maybe T.Text) []
    other -> error $ "Unsupported parquet type for column: " <> show other
