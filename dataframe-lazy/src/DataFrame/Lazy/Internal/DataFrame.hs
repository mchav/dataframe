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
 )
import qualified DataFrame.Lazy.Internal.Optimizer as Opt
import DataFrame.Operations.Join (JoinType (..))
import DataFrame.Operations.Permutation (SortOrder)
import DataFrame.Schema (Schema)

-- | A lazy query: a 'LogicalPlan' whose execution waits for 'run'.
data LazyDataFrame = LazyDataFrame
    { plan :: LogicalPlan
    , batchSize :: Int
    }

instance Show LazyDataFrame where
    show ldf =
        "LazyDataFrame { batchSize = "
            <> (show (batchSize ldf) <> (", plan = " <> (show (plan ldf) <> " }")))

-- | Optimise and stream-execute the lazy query into a 'D.DataFrame'.
run :: LazyDataFrame -> IO D.DataFrame
run ldf = execute (Opt.optimize (batchSize ldf) (plan ldf))

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

-- | Join two lazy queries on key columns shared by name, like eager @join@.
join :: JoinType -> [T.Text] -> LazyDataFrame -> LazyDataFrame -> LazyDataFrame
join jt keys = joinOn jt [(k, k) | k <- keys]

-- | Join on @(leftKey, rightKey)@ pairs; the result keeps the left names.
joinOn ::
    JoinType ->
    [(T.Text, T.Text)] ->
    LazyDataFrame ->
    LazyDataFrame ->
    LazyDataFrame
joinOn jt keyPairs left right =
    LazyDataFrame
        { plan = Join jt keyPairs (plan left) (plan right)
        , batchSize = batchSize left
        }

-- | Inner join on the given key columns.
innerJoin :: [T.Text] -> LazyDataFrame -> LazyDataFrame -> LazyDataFrame
innerJoin = join INNER

-- | Left join, keeping every row of the left query.
leftJoin :: [T.Text] -> LazyDataFrame -> LazyDataFrame -> LazyDataFrame
leftJoin = join LEFT

-- | Right join, keeping every row of the right query.
rightJoin :: [T.Text] -> LazyDataFrame -> LazyDataFrame -> LazyDataFrame
rightJoin = join RIGHT

-- | Full outer join, keeping every row of both queries.
fullOuterJoin :: [T.Text] -> LazyDataFrame -> LazyDataFrame -> LazyDataFrame
fullOuterJoin = join FULL_OUTER

-- | A lazy query grouped by key columns, awaiting 'aggregate'.
data LazyGroupedDataFrame = LazyGroupedDataFrame [T.Text] LazyDataFrame
    deriving (Show)

-- | Group by key columns; finish with 'aggregate', as in the eager API.
groupBy :: [T.Text] -> LazyDataFrame -> LazyGroupedDataFrame
groupBy = LazyGroupedDataFrame

-- | Compute @[(outputName, aggregateExpr)]@ for each group.
aggregate :: [E.NamedExpr] -> LazyGroupedDataFrame -> LazyDataFrame
aggregate aggs (LazyGroupedDataFrame keys ldf) =
    ldf{plan = Aggregate keys aggs (plan ldf)}

-- | Sort by the given orders, as the eager @sortBy@ does.
sortBy :: [SortOrder] -> LazyDataFrame -> LazyDataFrame
sortBy ords ldf = ldf{plan = Sort ords (plan ldf)}

-- | Retain at most @n@ rows.
take :: Int -> LazyDataFrame -> LazyDataFrame
take n ldf = ldf{plan = Limit n (plan ldf)}
