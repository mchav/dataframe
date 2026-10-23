{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- | Typed Parquet reading, validated against a type-level schema.
module DataFrame.Typed.IO.Parquet (
    readParquet,
    readParquetWithError,
    readParquetWithOpts,
    readParquetFiles,
    readParquetFilesWithOpts,
    writeParquet,
    writeParquetWithOptions,
) where

import Control.Applicative ((<|>))
import Control.Exception (SomeException, try)
import qualified Data.Text as T

import DataFrame.IO.Parquet (
    ParquetReadOptions (..),
    ParquetWriteOptions,
    defaultParquetReadOptions,
 )
import qualified DataFrame.IO.Parquet as Parquet
import DataFrame.Typed.Freeze (freezeOrThrow, freezeWithError, thaw)
import DataFrame.Typed.Schema (KnownSchema, schemaColumnNames)
import DataFrame.Typed.Types (TypedDataFrame)

-- | Read only the columns the schema names, unless the caller chose.
schemaOptions ::
    forall cols. (KnownSchema cols) => ParquetReadOptions -> ParquetReadOptions
schemaOptions opts =
    opts
        { selectedColumns = selectedColumns opts <|> Just (schemaColumnNames @cols)
        }

-- | Read a Parquet file into a typed DataFrame, throwing on mismatch.
readParquet ::
    forall cols. (KnownSchema cols) => FilePath -> IO (TypedDataFrame cols)
readParquet = readParquetWithOpts @cols defaultParquetReadOptions

-- | Read a Parquet file, returning an error on mismatch.
readParquetWithError ::
    forall cols.
    (KnownSchema cols) =>
    FilePath -> IO (Either T.Text (TypedDataFrame cols))
readParquetWithError path = do
    r <-
        try
            (Parquet.readParquetWithOpts (schemaOptions @cols defaultParquetReadOptions) path)
    pure $ case r of
        Left (e :: SomeException) -> Left (T.pack (show e))
        Right df -> freezeWithError df

-- | Read a Parquet file with custom options, throwing on mismatch.
readParquetWithOpts ::
    forall cols.
    (KnownSchema cols) =>
    ParquetReadOptions -> FilePath -> IO (TypedDataFrame cols)
readParquetWithOpts opts path =
    Parquet.readParquetWithOpts (schemaOptions @cols opts) path
        >>= freezeOrThrow @cols

-- | Read a directory\/glob of Parquet files into a typed DataFrame.
readParquetFiles ::
    forall cols. (KnownSchema cols) => FilePath -> IO (TypedDataFrame cols)
readParquetFiles = readParquetFilesWithOpts @cols defaultParquetReadOptions

-- | Read a directory\/glob of Parquet files with custom options.
readParquetFilesWithOpts ::
    forall cols.
    (KnownSchema cols) =>
    ParquetReadOptions -> FilePath -> IO (TypedDataFrame cols)
readParquetFilesWithOpts opts path =
    Parquet.readParquetFilesWithOpts (schemaOptions @cols opts) path
        >>= freezeOrThrow @cols

-- | Write a typed DataFrame to a Parquet file.
writeParquet :: FilePath -> TypedDataFrame cols -> IO ()
writeParquet path = Parquet.writeParquet path . thaw

-- | Write a typed DataFrame to a Parquet file with custom options.
writeParquetWithOptions ::
    ParquetWriteOptions -> FilePath -> TypedDataFrame cols -> IO ()
writeParquetWithOptions opts path =
    Parquet.writeParquetWithOptions opts path . thaw
