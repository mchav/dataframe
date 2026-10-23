{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- | Typed CSV reading driven entirely by the type-level schema.
module DataFrame.Typed.IO.CSV (
    readCsv,
    readCsvWithError,
    readTsv,
    readCsvWithOpts,
    readSeparated,
    writeCsv,
    writeTsv,
    writeSeparated,
) where

import Control.Applicative ((<|>))
import Control.Exception (SomeException, try)
import qualified Data.Text as T

import DataFrame.IO.CSV (ReadOptions (..), TypeSpec (..), defaultReadOptions)
import qualified DataFrame.IO.CSV as CSV
import DataFrame.Schema (RuntimeSchema (..), elements)
import DataFrame.Typed.Freeze (freezeOrThrow, freezeWithError, thaw)
import DataFrame.Typed.Schema (KnownSchema)
import DataFrame.Typed.Types (TypedDataFrame)

import qualified Data.Map.Strict as M

-- | Fold a type-level schema into read options: its columns and types.
schemaOptions :: forall cols. (RuntimeSchema cols) => ReadOptions -> ReadOptions
schemaOptions opts =
    opts
        { typeSpec = SpecifyTypes (M.toList declared) (typeSpec opts)
        , readColumns = readColumns opts <|> Just (M.keys declared)
        }
  where
    declared = elements (runtimeSchema @cols)

-- | Read a CSV into a typed DataFrame, throwing on schema mismatch.
readCsv ::
    forall cols.
    (KnownSchema cols, RuntimeSchema cols) =>
    FilePath -> IO (TypedDataFrame cols)
readCsv = readCsvWithOpts @cols defaultReadOptions

-- | Read a CSV, returning an error on schema mismatch instead of throwing.
readCsvWithError ::
    forall cols.
    (KnownSchema cols, RuntimeSchema cols) =>
    FilePath -> IO (Either T.Text (TypedDataFrame cols))
readCsvWithError path = do
    -- A missing column can fail the projection before the freeze does.
    r <- try (CSV.readCsvWithOpts (schemaOptions @cols defaultReadOptions) path)
    pure $ case r of
        Left (e :: SomeException) -> Left (T.pack (show e))
        Right df -> freezeWithError df

-- | Read a tab-separated file into a typed DataFrame, throwing on mismatch.
readTsv ::
    forall cols.
    (KnownSchema cols, RuntimeSchema cols) =>
    FilePath -> IO (TypedDataFrame cols)
readTsv = readCsvWithOpts @cols defaultReadOptions{columnSeparator = '\t'}

-- | Read a CSV with custom options, throwing on schema mismatch.
readCsvWithOpts ::
    forall cols.
    (KnownSchema cols, RuntimeSchema cols) =>
    ReadOptions -> FilePath -> IO (TypedDataFrame cols)
readCsvWithOpts opts path =
    CSV.readCsvWithOpts (schemaOptions @cols opts) path >>= freezeOrThrow @cols

-- | Write a typed DataFrame to a CSV file.
writeCsv :: FilePath -> TypedDataFrame cols -> IO ()
writeCsv path = CSV.writeCsv path . thaw

-- | Write a typed DataFrame to a tab-separated file.
writeTsv :: FilePath -> TypedDataFrame cols -> IO ()
writeTsv path = CSV.writeTsv path . thaw

-- | Read a character-separated file with custom options; throws on mismatch.
readSeparated ::
    forall cols.
    (KnownSchema cols, RuntimeSchema cols) =>
    ReadOptions -> FilePath -> IO (TypedDataFrame cols)
readSeparated = readCsvWithOpts @cols

-- | Write a typed DataFrame with the given separator.
writeSeparated :: Char -> FilePath -> TypedDataFrame cols -> IO ()
writeSeparated sep path = CSV.writeSeparated sep path . thaw
