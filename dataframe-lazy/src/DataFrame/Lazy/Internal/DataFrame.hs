{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module DataFrame.Lazy.Internal.DataFrame where

import qualified Data.Text as T
import DataFrame.IO.CSV (
    CsvBytesReader,
    CsvReader,
    decodeSeparatedStrict,
    readSeparated,
 )
import qualified DataFrame.Internal.Column as C
import qualified DataFrame.Internal.DataFrame as D
import qualified DataFrame.Internal.Expression as E
import DataFrame.Lazy.Internal.Executor (execute)
import DataFrame.Lazy.Internal.LogicalPlan (
    DataSource (..),
    LogicalPlan (..),
    SortOrder (..),
 )
import qualified DataFrame.Lazy.Internal.Optimizer as Opt
import DataFrame.Operations.Join (JoinType)
import DataFrame.Schema (Schema)

-- | A lazy query: a 'LogicalPlan' whose execution waits for 'runDataFrame'.
data LazyDataFrame = LazyDataFrame
    { plan :: LogicalPlan
    , batchSize :: Int
    }

instance Show LazyDataFrame where
    show ldf =
        "LazyDataFrame { batchSize = "
            <> (show (batchSize ldf) <> (", plan = " <> (show (plan ldf) <> " }")))

-- | Optimise and stream-execute the lazy query into a 'D.DataFrame'.
runDataFrame :: LazyDataFrame -> IO D.DataFrame
runDataFrame ldf = execute (Opt.optimize (batchSize ldf) (plan ldf))

-- | Lift an already-loaded eager 'D.DataFrame' into the lazy plan.
fromDataFrame :: D.DataFrame -> LazyDataFrame
fromDataFrame df = LazyDataFrame{plan = SourceDF df, batchSize = 1_000_000}

-- | Scan a CSV file with the default comma separator and strict reader.
scanCsv :: Schema -> T.Text -> LazyDataFrame
scanCsv = scanCsvWith readSeparated

-- | Like 'scanCsv' but with an explicit 'CsvReader'.
scanCsvWith :: CsvReader -> Schema -> T.Text -> LazyDataFrame
scanCsvWith reader schema path =
    LazyDataFrame
        { plan = Scan (CsvSource (T.unpack path) ',' reader) schema
        , batchSize = 1_000_000
        }

-- | Scan a CSV in bounded-memory windows with the default strict reader.
scanCsvStreaming :: Schema -> T.Text -> LazyDataFrame
scanCsvStreaming = scanCsvStreamingBytesWith decodeSeparatedStrict

-- | Like 'scanCsvWith', but reads the file in bounded-memory windows.
scanCsvStreamingWith :: CsvReader -> Schema -> T.Text -> LazyDataFrame
scanCsvStreamingWith reader schema path =
    LazyDataFrame
        { plan = Scan (CsvSourceStreaming (T.unpack path) ',' reader) schema
        , batchSize = 1_000_000
        }

-- | Stream a CSV in bounded-memory windows decoded by a 'CsvBytesReader'.
scanCsvStreamingBytesWith :: CsvBytesReader -> Schema -> T.Text -> LazyDataFrame
scanCsvStreamingBytesWith reader schema path =
    LazyDataFrame
        { plan = Scan (CsvSourceStreamingBytes (T.unpack path) ',' reader) schema
        , batchSize = 1_000_000
        }

-- | Scan a character-separated file with the default strict reader.
scanSeparated :: Char -> Schema -> T.Text -> LazyDataFrame
scanSeparated = scanSeparatedWith readSeparated

-- | Like 'scanSeparated' but with an explicit CSV reader.
scanSeparatedWith ::
    CsvReader -> Char -> Schema -> T.Text -> LazyDataFrame
scanSeparatedWith reader sep schema path =
    LazyDataFrame
        { plan = Scan (CsvSource (T.unpack path) sep reader) schema
        , batchSize = 1_000_000
        }

-- | Scan a Parquet file, directory of files, or glob pattern.
scanParquet :: Schema -> T.Text -> LazyDataFrame
scanParquet schema path =
    LazyDataFrame
        { plan = Scan (ParquetSource (T.unpack path)) schema
        , batchSize = 1_000_000
        }

-- | Add a computed column (or overwrite an existing one).
derive ::
    (C.Columnable a) => T.Text -> E.Expr a -> LazyDataFrame -> LazyDataFrame
derive name expr ldf =
    ldf{plan = Derive name (E.SomeExpr expr) (plan ldf)}

-- | Retain only the listed columns.
select :: [T.Text] -> LazyDataFrame -> LazyDataFrame
select cols ldf = ldf{plan = Project cols (plan ldf)}

-- | Keep rows that satisfy the predicate.
filter :: E.Expr Bool -> LazyDataFrame -> LazyDataFrame
filter cond ldf = ldf{plan = Filter cond (plan ldf)}

-- | Join two lazy queries on the given key columns.
join ::
    JoinType ->
    -- | Left join key column name
    T.Text ->
    -- | Right join key column name
    T.Text ->
    -- | Left sub-query
    LazyDataFrame ->
    -- | Right sub-query
    LazyDataFrame ->
    LazyDataFrame
join jt leftKey rightKey left right =
    LazyDataFrame
        { plan = Join jt leftKey rightKey (plan left) (plan right)
        , batchSize = batchSize left
        }

-- | Group by a set of columns and compute aggregate expressions.
groupBy ::
    -- | Group-by key columns
    [T.Text] ->
    -- | @[(outputName, aggregateExpr)]@
    [(T.Text, E.SomeExpr)] ->
    LazyDataFrame ->
    LazyDataFrame
groupBy keys aggs ldf = ldf{plan = Aggregate keys aggs (plan ldf)}

-- | Sort the result by the given @(column, direction)@ pairs.
sortBy :: [(T.Text, SortOrder)] -> LazyDataFrame -> LazyDataFrame
sortBy cols ldf = ldf{plan = Sort cols (plan ldf)}

-- | Retain at most @n@ rows.
take :: Int -> LazyDataFrame -> LazyDataFrame
take n ldf = ldf{plan = Limit n (plan ldf)}
