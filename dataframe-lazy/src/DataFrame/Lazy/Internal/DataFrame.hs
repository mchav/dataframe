{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module DataFrame.Lazy.Internal.DataFrame where

import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import DataFrame.IO.CSV (
    CsvBytesReader,
    CsvReader,
    decodeSeparatedStrict,
    readSeparated,
 )
import qualified DataFrame.Internal.Column as C
import qualified DataFrame.Internal.DataFrame as D
import qualified DataFrame.Internal.Expression as E
import DataFrame.Internal.Expression.Operators.Nullable (BaseType)
import DataFrame.Lazy.Internal.Executor (execute)
import DataFrame.Lazy.Internal.LogicalPlan (
    BatchFn (..),
    BatchFn2 (..),
    ColumnEffect (..),
    DataSource (..),
    LogicalPlan (..),
    opaque,
 )
import qualified DataFrame.Lazy.Internal.Optimizer as Opt
import qualified DataFrame.Operations.Aggregation as Agg
import qualified DataFrame.Operations.Core as Core
import DataFrame.Operations.Join (JoinType (..))
import DataFrame.Operations.Merge ()
import qualified DataFrame.Operations.Merge as Merge
import DataFrame.Operations.Permutation (SortOrder)
import qualified DataFrame.Operations.Permutation as Perm
import qualified DataFrame.Operations.SetOps as SetOps
import qualified DataFrame.Operations.Statistics as Stats
import DataFrame.Operations.Subset (SelectionCriteria, SplittableGen)
import qualified DataFrame.Operations.Subset as Sub
import DataFrame.Operations.Transformations (ImputeOp)
import qualified DataFrame.Operations.Transformations as Trans
import DataFrame.Schema (Schema)
import System.Random (RandomGen)
import Prelude hiding (drop, filter, take)

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

-- * Generic nodes

-- | Run a row-local eager op on each batch as it streams.
mapBatch ::
    T.Text ->
    ColumnEffect ->
    (D.DataFrame -> D.DataFrame) ->
    LazyDataFrame ->
    LazyDataFrame
mapBatch lbl eff f ldf = ldf{plan = MapBatch lbl eff (BatchFn f) (plan ldf)}

-- | Collect the whole input, then run an eager op on it.
barrier ::
    T.Text ->
    ColumnEffect ->
    (D.DataFrame -> D.DataFrame) ->
    LazyDataFrame ->
    LazyDataFrame
barrier lbl eff f ldf = ldf{plan = Barrier lbl eff (BatchFn f) (plan ldf)}

-- | Collect both inputs, then run an eager two-frame op on them.
barrier2 ::
    T.Text ->
    (D.DataFrame -> D.DataFrame -> D.DataFrame) ->
    LazyDataFrame ->
    LazyDataFrame ->
    LazyDataFrame
barrier2 lbl f l r = l{plan = Barrier2 lbl (BatchFn2 f) (plan l) (plan r)}

-- | Effect of an op that reads the given columns and defines others.
effect :: [T.Text] -> [T.Text] -> ColumnEffect
effect rs = ColumnEffect (Just rs)

-- | Effect of an op that only looks at row positions, never at values.
positional :: ColumnEffect
positional = effect [] []

-- | Concatenate rows, as the eager @<>@ does.
instance Semigroup LazyDataFrame where
    (<>) = barrier2 "append" (<>)

instance Monoid LazyDataFrame where
    mempty = fromDataFrame D.empty

-- * Row operations

-- | Keep rows whose value of the expression satisfies the predicate.
filterBy ::
    (C.Columnable a) => (a -> Bool) -> E.Expr a -> LazyDataFrame -> LazyDataFrame
filterBy p e = mapBatch "filterBy" (effect (E.getColumns e) []) (Sub.filterBy p e)

-- | Drop rows with Nothing in the column and unwrap its Justs.
filterJust :: T.Text -> LazyDataFrame -> LazyDataFrame
filterJust c = mapBatch "filterJust" (effect [c] [c]) (Sub.filterJust c)

-- | Keep only rows with Nothing in the column.
filterNothing :: T.Text -> LazyDataFrame -> LazyDataFrame
filterNothing c = mapBatch "filterNothing" (effect [c] []) (Sub.filterNothing c)

-- | Keep rows with no Nothing in any column.
filterAllJust :: LazyDataFrame -> LazyDataFrame
filterAllJust = mapBatch "filterAllJust" opaque Sub.filterAllJust

-- | Keep rows that are Nothing in every column.
filterAllNothing :: LazyDataFrame -> LazyDataFrame
filterAllNothing = mapBatch "filterAllNothing" opaque Sub.filterAllNothing

-- | Skip the first @n@ rows.
drop :: Int -> LazyDataFrame -> LazyDataFrame
drop n ldf = ldf{plan = Offset (max 0 n) (plan ldf)}

-- | Keep the last @n@ rows.
takeLast :: Int -> LazyDataFrame -> LazyDataFrame
takeLast n = barrier "takeLast" positional (Sub.takeLast n)

-- | Drop the last @n@ rows.
dropLast :: Int -> LazyDataFrame -> LazyDataFrame
dropLast n = barrier "dropLast" positional (Sub.dropLast n)

-- | Rows @[start, end)@, clamped like the eager @range@.
range :: (Int, Int) -> LazyDataFrame -> LazyDataFrame
range (start, end) = take (max 0 (end - start')) . drop start'
  where
    start' = max 0 start

-- | The first @rows@ rows of the first @cols@ columns.
cube :: (Int, Int) -> LazyDataFrame -> LazyDataFrame
cube (rows, cols) =
    mapBatch "cube" opaque (Sub.selectBy [Sub.byIndexRange (0, cols - 1)])
        . take rows

-- | Rows at the given positions.
selectRows :: [Int] -> LazyDataFrame -> LazyDataFrame
selectRows ixs = barrier "selectRows" positional (Sub.selectRows ixs)

-- | Keep each row with probability @p@.
sample :: (RandomGen g) => g -> Double -> LazyDataFrame -> LazyDataFrame
sample g p = barrier "sample" positional (Sub.sample g p)

-- | Shuffle the rows.
shuffle :: (RandomGen g) => g -> LazyDataFrame -> LazyDataFrame
shuffle g = barrier "shuffle" positional (Perm.shuffle g)

-- | Drop duplicate rows.
distinct :: LazyDataFrame -> LazyDataFrame
distinct = barrier "distinct" opaque Agg.distinct

-- | Rows @[offset, offset + count)@ after sorting by the given orders.
topBy :: Int -> Int -> [SortOrder] -> LazyDataFrame -> LazyDataFrame
topBy offset count ords =
    barrier
        "topBy"
        (effect (concatMap Perm.sortOrderColumns ords) [])
        (Perm.topBy offset count ords)

-- | Split into a sample of fraction @p@ and the remainder.
randomSplit ::
    (RandomGen g) => g -> Double -> LazyDataFrame -> (LazyDataFrame, LazyDataFrame)
randomSplit g p ldf =
    ( barrier "randomSplit" positional (fst . Sub.randomSplit g p) ldf
    , barrier "randomSplit" positional (snd . Sub.randomSplit g p) ldf
    )

-- | Split into @k@ folds.
kFolds :: (RandomGen g) => g -> Int -> LazyDataFrame -> [LazyDataFrame]
kFolds g k ldf =
    [ barrier "kFolds" positional ((!! i) . Sub.kFolds g k) ldf
    | i <- [0 .. k - 1]
    ]

-- | Sample a fraction of each stratum of the expression.
stratifiedSample ::
    (SplittableGen g, C.Columnable a) =>
    g -> Double -> E.Expr a -> LazyDataFrame -> LazyDataFrame
stratifiedSample g p e =
    barrier
        "stratifiedSample"
        (effect (E.getColumns e) [])
        (Sub.stratifiedSample g p e)

-- | Split each stratum of the expression into a sample and the remainder.
stratifiedSplit ::
    (SplittableGen g, C.Columnable a) =>
    g -> Double -> E.Expr a -> LazyDataFrame -> (LazyDataFrame, LazyDataFrame)
stratifiedSplit g p e ldf =
    ( barrier "stratifiedSplit" eff (fst . Sub.stratifiedSplit g p e) ldf
    , barrier "stratifiedSplit" eff (snd . Sub.stratifiedSplit g p e) ldf
    )
  where
    eff = effect (E.getColumns e) []

-- * Column operations

-- | Add several columns, each computed from the input frame.
deriveMany :: [E.NamedExpr] -> LazyDataFrame -> LazyDataFrame
deriveMany nes =
    mapBatch
        "deriveMany"
        (effect (concatMap (\(_, E.SomeExpr e) -> E.getColumns e) nes) (map fst nes))
        (Trans.deriveMany nes)

-- | 'derive', also returning a reference to the new column.
deriveWithExpr ::
    (C.Columnable a) =>
    T.Text -> E.Expr a -> LazyDataFrame -> (E.Expr a, LazyDataFrame)
deriveWithExpr name e ldf = (E.Col name, derive name e ldf)

-- | Select columns by name, property or range.
selectBy :: [SelectionCriteria] -> LazyDataFrame -> LazyDataFrame
selectBy crit = mapBatch "selectBy" opaque (Sub.selectBy crit)

-- | Drop the listed columns.
exclude :: [T.Text] -> LazyDataFrame -> LazyDataFrame
exclude cols = mapBatch "exclude" (effect cols []) (Sub.exclude cols)

-- | Rename a column.
rename :: T.Text -> T.Text -> LazyDataFrame -> LazyDataFrame
rename old new = renameMany [(old, new)]

-- | Rename columns by @(old, new)@ pairs.
renameMany :: [(T.Text, T.Text)] -> LazyDataFrame -> LazyDataFrame
renameMany pairs ldf = ldf{plan = Rename pairs (plan ldf)}

-- | Copy a column under a new name.
cloneColumn :: T.Text -> T.Text -> LazyDataFrame -> LazyDataFrame
cloneColumn old new =
    mapBatch "cloneColumn" (effect [old] [new]) (Core.cloneColumn old new)

-- | Insert a column from a container aligned with the whole result.
insert ::
    (C.Columnable a, Foldable t) => T.Text -> t a -> LazyDataFrame -> LazyDataFrame
insert name xs = barrier "insert" (effect [] [name]) (Core.insert name xs)

-- | Insert a boxed vector aligned with the whole result.
insertVector ::
    (C.Columnable a) => T.Text -> V.Vector a -> LazyDataFrame -> LazyDataFrame
insertVector name xs =
    barrier "insertVector" (effect [] [name]) (Core.insertVector name xs)

-- | Insert an unboxed vector aligned with the whole result.
insertUnboxedVector ::
    (C.Columnable a, VU.Unbox a) =>
    T.Text -> VU.Vector a -> LazyDataFrame -> LazyDataFrame
insertUnboxedVector name xs =
    barrier
        "insertUnboxedVector"
        (effect [] [name])
        (Core.insertUnboxedVector name xs)

-- | Insert a container, padding with the default to the result's length.
insertWithDefault ::
    (C.Columnable a, Foldable t) =>
    a -> T.Text -> t a -> LazyDataFrame -> LazyDataFrame
insertWithDefault d name xs =
    barrier
        "insertWithDefault"
        (effect [] [name])
        (Core.insertWithDefault d name xs)

-- | Insert a vector, padding with the default to the result's length.
insertVectorWithDefault ::
    (C.Columnable a) => a -> T.Text -> V.Vector a -> LazyDataFrame -> LazyDataFrame
insertVectorWithDefault d name xs =
    barrier
        "insertVectorWithDefault"
        (effect [] [name])
        (Core.insertVectorWithDefault d name xs)

-- | Insert a prepared column aligned with the whole result.
insertColumn :: T.Text -> C.Column -> LazyDataFrame -> LazyDataFrame
insertColumn name c = barrier "insertColumn" (effect [] [name]) (D.insertColumn name c)

-- | Replace Nothing in a column with a value.
impute ::
    (ImputeOp a) => E.Expr a -> BaseType a -> LazyDataFrame -> LazyDataFrame
impute e v = mapBatch "impute" (effect cols cols) (Trans.impute e v)
  where
    cols = E.getColumns e

-- | Replace Nothing with a statistic of the whole column.
imputeWith ::
    (ImputeOp a, C.Columnable (BaseType a)) =>
    (E.Expr (BaseType a) -> E.Expr (BaseType a)) ->
    E.Expr a ->
    LazyDataFrame ->
    LazyDataFrame
imputeWith f e = barrier "imputeWith" (effect cols cols) (Stats.imputeWith f e)
  where
    cols = E.getColumns e

-- | Map a function over a column.
apply ::
    (C.Columnable b, C.Columnable c) =>
    (b -> c) -> T.Text -> LazyDataFrame -> LazyDataFrame
apply f name = mapBatch "apply" (effect [name] [name]) (Trans.apply f name)

-- | 'apply' over an Int column.
applyInt ::
    (C.Columnable b) => (Int -> b) -> T.Text -> LazyDataFrame -> LazyDataFrame
applyInt = apply

-- | 'apply' over a Double column.
applyDouble ::
    (C.Columnable b) => (Double -> b) -> T.Text -> LazyDataFrame -> LazyDataFrame
applyDouble = apply

-- | Map a function over several columns.
applyMany ::
    (C.Columnable b, C.Columnable c) =>
    (b -> c) -> [T.Text] -> LazyDataFrame -> LazyDataFrame
applyMany f names = mapBatch "applyMany" (effect names names) (Trans.applyMany f names)

-- | Map over the target column where the criterion column passes.
applyWhere ::
    (C.Columnable a, C.Columnable b) =>
    (a -> Bool) -> T.Text -> (b -> b) -> T.Text -> LazyDataFrame -> LazyDataFrame
applyWhere p fc f tc =
    mapBatch "applyWhere" (effect [fc, tc] [tc]) (Trans.applyWhere p fc f tc)

-- | Map a function over one row of a column.
applyAtIndex ::
    (C.Columnable a) => Int -> (a -> a) -> T.Text -> LazyDataFrame -> LazyDataFrame
applyAtIndex i f name =
    barrier "applyAtIndex" (effect [name] [name]) (Trans.applyAtIndex i f name)

-- | Thread the query through a step for each element.
fold ::
    (a -> LazyDataFrame -> LazyDataFrame) -> [a] -> LazyDataFrame -> LazyDataFrame
fold f xs ldf = foldl (flip f) ldf xs

-- * Merges and set operations

-- | Place two results side by side.
(|||) :: LazyDataFrame -> LazyDataFrame -> LazyDataFrame
(|||) = barrier2 "|||" (Merge.|||)

-- | Rows in either result, without duplicates.
union :: LazyDataFrame -> LazyDataFrame -> LazyDataFrame
union = barrier2 "union" SetOps.union

-- | Rows in both results.
intersect :: LazyDataFrame -> LazyDataFrame -> LazyDataFrame
intersect = barrier2 "intersect" SetOps.intersect

-- | Rows in the first result but not the second.
difference :: LazyDataFrame -> LazyDataFrame -> LazyDataFrame
difference = barrier2 "difference" SetOps.difference

-- | Rows in exactly one of the results.
symmetricDifference :: LazyDataFrame -> LazyDataFrame -> LazyDataFrame
symmetricDifference = barrier2 "symmetricDifference" SetOps.symmetricDifference

-- * Frame-valued summaries

-- | Summary statistics per numeric column.
summarize :: LazyDataFrame -> LazyDataFrame
summarize = barrier "summarize" opaque Stats.summarize

-- | Name, type and null counts per column.
describeColumns :: LazyDataFrame -> LazyDataFrame
describeColumns = barrier "describeColumns" opaque Core.describeColumns

-- | Value counts and proportions of an expression.
frequencies ::
    (C.Columnable a, Ord a) => E.Expr a -> LazyDataFrame -> LazyDataFrame
frequencies e = barrier "frequencies" opaque (Stats.frequencies e)
