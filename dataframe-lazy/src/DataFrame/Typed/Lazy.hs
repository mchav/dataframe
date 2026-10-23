{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

-- | Typed lazy queries with the same operations as "DataFrame.Typed".
module DataFrame.Typed.Lazy (
    -- * Core types
    TypedLazyDataFrame,
    TypedLazyGrouped,
    TypedDataFrame,

    -- * Data sources
    scanCsv,
    scanCsvWith,
    scanSeparated,
    scanSeparatedWith,
    scanParquet,
    fromDataFrame,

    -- * Schema-preserving operations
    filter,
    filterBy,
    filterJust,
    filterNothing,
    filterAllJust,
    filterAllNothing,
    take,
    takeLast,
    drop,
    dropLast,
    range,
    cube,
    sortBy,
    topBy,
    distinct,
    sample,
    shuffle,

    -- * Schema-modifying operations
    derive,
    deriveMany,
    deriveWithExpr,
    select,
    exclude,
    rename,
    renameMany,
    impute,
    imputeWith,
    insert,
    insertColumn,
    insertVector,
    insertUnboxedVector,
    insertWithDefault,
    insertVectorWithDefault,
    cloneColumn,
    dropColumn,
    replaceColumn,
    apply,
    applyInt,
    applyDouble,
    applyMany,
    applyWhere,
    applyAtIndex,
    fold,
    empty,

    -- * Merges and set operations
    append,
    (|||),
    union,
    intersect,
    difference,
    symmetricDifference,

    -- * Sampling and splitting
    randomSplit,
    kFolds,
    selectRows,
    stratifiedSample,
    stratifiedSplit,

    -- * Frame-valued summaries (untyped results)
    summarize,
    describeColumns,
    frequencies,

    -- * Aggregation
    groupBy,
    as,
    aggregate,

    -- * Joins
    JoinType (..),
    join,
    joinOn,
    innerJoin,
    leftJoin,
    rightJoin,
    fullOuterJoin,

    -- * Execution
    run,

    -- * Values (run the query)
    safeApply,
    null,
    dimensions,
    nRows,
    nColumns,
    columnNames,
    valueCounts,
    valueProportions,
    mean,
    meanMaybe,
    median,
    medianMaybe,
    percentile,
    genericPercentile,
    standardDeviation,
    skewness,
    variance,
    interQuartileRange,
    sum,
    correlation,
    columnAsVector,
    columnAsList,
    columnAsIntVector,
    columnAsDoubleVector,
    columnAsFloatVector,
    columnAsUnboxedVector,
    toDoubleMatrix,
    toFloatMatrix,
    toIntMatrix,
    toRecords,

    -- * Rendering and output
    DisplayOptions (..),
    defaultDisplayOptions,
    display,
    prettyPrint,
    toCsv,
    toCsv',
    toMarkdown,
    toMarkdown',
    toSeparated,
    writeCsv,
    writeTsv,
    writeSeparated,
    writeParquet,
    writeParquetWithOptions,

    -- * Typed expressions (combinators live in "DataFrame.Typed.Functions")
    TExpr (..),
    TSortOrder (..),
    col,
    lit,
    ifThenElse,
    caseWhen,
    orElse,
    (.==.),
    (./=.),
    (.<.),
    (.<=.),
    (.>=.),
    (.>.),
    (.+.),
    (.-.),
    (.*.),
    (./.),
    (.^^.),
    (.^.),
    (.+),
    (.-),
    (.*),
    (./),
    (.^^),
    (.^),
    (.==),
    (./=),
    (.<),
    (.<=),
    (.>=),
    (.>),
    (.&&.),
    (.||.),
    (.&&),
    (.||),
    (|>),
) where

import Data.Kind (Type)
import Data.Proxy (Proxy (..))
import qualified Data.Text as T
import GHC.TypeLits (KnownSymbol, Symbol, symbolVal)
import Prelude hiding (drop, filter, null, sum, take)

import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import DataFrame.Display (DisplayOptions (..), defaultDisplayOptions)
import DataFrame.Errors (DataFrameException)
import DataFrame.Expression.Operators ((|>))
import qualified DataFrame.Functions as DF
import DataFrame.IO.CSV (CsvReader)
import DataFrame.IO.Parquet (ParquetWriteOptions)
import qualified DataFrame.Internal.Column as C
import qualified DataFrame.Internal.Expression as E
import DataFrame.Internal.Expression.Operators.Nullable (BaseType)
import qualified DataFrame.Lazy.Internal.Collect as LC
import DataFrame.Lazy.Internal.DataFrame (LazyDataFrame)
import qualified DataFrame.Lazy.Internal.DataFrame as L
import DataFrame.Operations.Join (JoinType (..))
import DataFrame.Operations.Subset (SplittableGen)
import DataFrame.Operations.Transformations (ImputeOp)
import DataFrame.Schema (RuntimeSchema (..))
import qualified DataFrame.Typed.Access as DTA
import DataFrame.Typed.Aggregate (as)
import qualified DataFrame.Typed.Apply as DTApply
import DataFrame.Typed.Expr (
    caseWhen,
    col,
    ifThenElse,
    lit,
    orElse,
    (.&&),
    (.&&.),
    (.*),
    (.*.),
    (.+),
    (.+.),
    (.-),
    (.-.),
    (./),
    (./.),
    (./=),
    (./=.),
    (.<),
    (.<.),
    (.<=),
    (.<=.),
    (.==),
    (.==.),
    (.>),
    (.>.),
    (.>=),
    (.>=.),
    (.^),
    (.^.),
    (.^^),
    (.^^.),
    (.||),
    (.||.),
 )
import DataFrame.Typed.Freeze (thaw, unsafeFreeze)
import DataFrame.Typed.Join (
    Fsts,
    JoinKeys,
    JoinOnSchema,
    JoinSchema,
    KnownJoinType (..),
    SwapPairs,
 )
import DataFrame.Typed.Operations (AllKnownPairs (..), toUntypedSort)
import qualified DataFrame.Typed.Operations as DT
import DataFrame.Typed.Record (HasSchema (..))
import DataFrame.Typed.Schema
import DataFrame.Typed.Types
import System.Random (RandomGen)

-- | A lazy query with compile-time schema tracking.
newtype TypedLazyDataFrame (cols :: [(Symbol, Type)]) = TLD {_unTLD :: LazyDataFrame}

instance Show (TypedLazyDataFrame cols) where
    show (TLD ldf) = "TypedLazyDataFrame { " ++ show ldf ++ " }"

-- | Scan a CSV file; the column types come from @cols@.
scanCsv ::
    forall cols. (RuntimeSchema cols) => T.Text -> TypedLazyDataFrame cols
scanCsv path = TLD (L.scanCsv (runtimeSchema @cols) path)

-- | Like 'scanCsv' but with an explicit CSV reader.
scanCsvWith ::
    forall cols.
    (RuntimeSchema cols) => CsvReader -> T.Text -> TypedLazyDataFrame cols
scanCsvWith reader path = TLD (L.scanCsvWith reader (runtimeSchema @cols) path)

-- | Scan a character-separated file; the column types come from @cols@.
scanSeparated ::
    forall cols. (RuntimeSchema cols) => Char -> T.Text -> TypedLazyDataFrame cols
scanSeparated sep path = TLD (L.scanSeparated sep (runtimeSchema @cols) path)

-- | Like 'scanSeparated' but with an explicit CSV reader.
scanSeparatedWith ::
    forall cols.
    (RuntimeSchema cols) =>
    CsvReader -> Char -> T.Text -> TypedLazyDataFrame cols
scanSeparatedWith reader sep path =
    TLD (L.scanSeparatedWith reader sep (runtimeSchema @cols) path)

-- | Scan a Parquet file, directory, or glob; the columns come from @cols@.
scanParquet ::
    forall cols. (RuntimeSchema cols) => T.Text -> TypedLazyDataFrame cols
scanParquet path = TLD (L.scanParquet (runtimeSchema @cols) path)

-- | Lift an already-loaded eager 'TypedDataFrame' into a lazy plan.
fromDataFrame :: TypedDataFrame cols -> TypedLazyDataFrame cols
fromDataFrame (TDF df) = TLD (L.fromDataFrame df)

-- | Keep rows that satisfy the predicate.
filter :: TExpr cols Bool -> TypedLazyDataFrame cols -> TypedLazyDataFrame cols
filter (TExpr expr) (TLD ldf) = TLD (L.filter expr ldf)

-- | Retain at most @n@ rows.
take :: Int -> TypedLazyDataFrame cols -> TypedLazyDataFrame cols
take n (TLD ldf) = TLD (L.take n ldf)

-- | Sort by the given typed orders, as the eager typed @sortBy@ does.
sortBy ::
    [TSortOrder cols] -> TypedLazyDataFrame cols -> TypedLazyDataFrame cols
sortBy ords (TLD ldf) = TLD (L.sortBy (map toUntypedSort ords) ldf)

-- | Add a computed column.
derive ::
    forall name a cols.
    (KnownSymbol name, C.Columnable a, AssertAbsent name cols) =>
    TExpr cols a ->
    TypedLazyDataFrame cols ->
    TypedLazyDataFrame (Snoc cols '(name, a))
derive (TExpr expr) (TLD ldf) =
    TLD (L.derive (T.pack (symbolVal (Proxy @name))) expr ldf)

-- | Retain only the listed columns.
select ::
    forall (names :: [Symbol]) cols.
    (AllKnownSymbol names, AssertAllPresent names cols) =>
    TypedLazyDataFrame cols ->
    TypedLazyDataFrame (SubsetSchema names cols)
select (TLD ldf) = TLD (L.select (symbolVals @names) ldf)

-- | A typed lazy query grouped by @keys@, awaiting 'aggregate'.
newtype TypedLazyGrouped (keys :: [Symbol]) (cols :: [(Symbol, Type)])
    = TLG L.LazyGroupedDataFrame

-- | Group by key columns; finish with 'aggregate'.
groupBy ::
    forall (keys :: [Symbol]) cols.
    (AllKnownSymbol keys, AssertAllPresent keys cols) =>
    TypedLazyDataFrame cols ->
    TypedLazyGrouped keys cols
groupBy (TLD ldf) = TLG (L.groupBy (symbolVals @keys) ldf)

-- | Aggregate each group; build the outputs with 'as', as in the eager API.
aggregate ::
    forall keys cols aggs.
    (TAgg keys cols '[] -> TAgg keys cols aggs) ->
    TypedLazyGrouped keys cols ->
    TypedLazyDataFrame (Append (GroupKeyColumns keys cols) (Reverse aggs))
aggregate build (TLG grouped) =
    TLD (L.aggregate (taggToNamedExprs (build TAggNil)) grouped)

-- | Typed lazy join: @join \@'INNER \@'["key"] l r@.
join ::
    forall (jt :: JoinType) (keys :: [Symbol]) left right.
    (KnownJoinType jt, JoinKeys keys left right) =>
    TypedLazyDataFrame left ->
    TypedLazyDataFrame right ->
    TypedLazyDataFrame (JoinSchema jt keys left right)
join (TLD l) (TLD r) = TLD (L.join (joinTypeVal @jt) (symbolVals @keys) l r)

-- | Join on @(leftKey, rightKey)@ pairs; the result keeps the left names.
joinOn ::
    forall (jt :: JoinType) (pairs :: [(Symbol, Symbol)]) left right.
    ( KnownJoinType jt
    , AllKnownPairs pairs
    , JoinKeys (Fsts pairs) left (RenameManyInSchema (SwapPairs pairs) right)
    ) =>
    TypedLazyDataFrame left ->
    TypedLazyDataFrame right ->
    TypedLazyDataFrame (JoinOnSchema jt pairs left right)
joinOn (TLD l) (TLD r) = TLD (L.joinOn (joinTypeVal @jt) (pairVals @pairs) l r)

-- | Typed inner join on one or more key columns.
innerJoin ::
    forall (keys :: [Symbol]) left right.
    (JoinKeys keys left right) =>
    TypedLazyDataFrame left ->
    TypedLazyDataFrame right ->
    TypedLazyDataFrame (InnerJoinSchema keys left right)
innerJoin = join @'INNER @keys

-- | Typed left join; the right side's non-key columns become @Maybe@.
leftJoin ::
    forall (keys :: [Symbol]) left right.
    (JoinKeys keys left right) =>
    TypedLazyDataFrame left ->
    TypedLazyDataFrame right ->
    TypedLazyDataFrame (LeftJoinSchema keys left right)
leftJoin = join @'LEFT @keys

-- | Typed right join; the left side's non-key columns become @Maybe@.
rightJoin ::
    forall (keys :: [Symbol]) left right.
    (JoinKeys keys left right) =>
    TypedLazyDataFrame left ->
    TypedLazyDataFrame right ->
    TypedLazyDataFrame (RightJoinSchema keys left right)
rightJoin = join @'RIGHT @keys

-- | Typed full outer join; non-key columns on both sides become @Maybe@.
fullOuterJoin ::
    forall (keys :: [Symbol]) left right.
    (JoinKeys keys left right) =>
    TypedLazyDataFrame left ->
    TypedLazyDataFrame right ->
    TypedLazyDataFrame (FullOuterJoinSchema keys left right)
fullOuterJoin = join @'FULL_OUTER @keys

-- | Execute the lazy query and return a typed DataFrame.
run :: TypedLazyDataFrame cols -> IO (TypedDataFrame cols)
run (TLD ldf) = unsafeFreeze <$> L.run ldf

-- * Helpers

nameOf :: forall (n :: Symbol). (KnownSymbol n) => T.Text
nameOf = T.pack (symbolVal (Proxy @n))

-- | Run the query and apply a typed eager function to the result.
collectWith :: (TypedDataFrame cols -> r) -> TypedLazyDataFrame cols -> IO r
collectWith f tld = f <$> run tld

-- * Row operations

filterBy ::
    (C.Columnable a) =>
    (a -> Bool) ->
    TExpr cols a ->
    TypedLazyDataFrame cols ->
    TypedLazyDataFrame cols
filterBy p (TExpr e) (TLD ldf) = TLD (L.filterBy p e ldf)

filterJust ::
    forall name cols.
    (KnownSymbol name, AssertPresent name cols) =>
    TypedLazyDataFrame cols -> TypedLazyDataFrame (StripMaybeAt name cols)
filterJust (TLD ldf) = TLD (L.filterJust (nameOf @name) ldf)

filterNothing ::
    forall name cols.
    (KnownSymbol name, AssertPresent name cols) =>
    TypedLazyDataFrame cols -> TypedLazyDataFrame cols
filterNothing (TLD ldf) = TLD (L.filterNothing (nameOf @name) ldf)

filterAllJust ::
    TypedLazyDataFrame cols -> TypedLazyDataFrame (StripAllMaybe cols)
filterAllJust (TLD ldf) = TLD (L.filterAllJust ldf)

filterAllNothing :: TypedLazyDataFrame cols -> TypedLazyDataFrame cols
filterAllNothing (TLD ldf) = TLD (L.filterAllNothing ldf)

takeLast :: Int -> TypedLazyDataFrame cols -> TypedLazyDataFrame cols
takeLast n (TLD ldf) = TLD (L.takeLast n ldf)

drop :: Int -> TypedLazyDataFrame cols -> TypedLazyDataFrame cols
drop n (TLD ldf) = TLD (L.drop n ldf)

dropLast :: Int -> TypedLazyDataFrame cols -> TypedLazyDataFrame cols
dropLast n (TLD ldf) = TLD (L.dropLast n ldf)

range :: (Int, Int) -> TypedLazyDataFrame cols -> TypedLazyDataFrame cols
range r (TLD ldf) = TLD (L.range r ldf)

cube :: (Int, Int) -> TypedLazyDataFrame cols -> TypedLazyDataFrame cols
cube c (TLD ldf) = TLD (L.cube c ldf)

topBy ::
    Int ->
    Int ->
    [TSortOrder cols] ->
    TypedLazyDataFrame cols ->
    TypedLazyDataFrame cols
topBy offset count ords (TLD ldf) =
    TLD (L.topBy offset count (map toUntypedSort ords) ldf)

distinct :: TypedLazyDataFrame cols -> TypedLazyDataFrame cols
distinct (TLD ldf) = TLD (L.distinct ldf)

sample ::
    (RandomGen g) =>
    g -> Double -> TypedLazyDataFrame cols -> TypedLazyDataFrame cols
sample g p (TLD ldf) = TLD (L.sample g p ldf)

shuffle ::
    (RandomGen g) => g -> TypedLazyDataFrame cols -> TypedLazyDataFrame cols
shuffle g (TLD ldf) = TLD (L.shuffle g ldf)

-- * Column operations

deriveMany ::
    forall new cols.
    (AssertAllAbsent (Reverse new) cols) =>
    (TAgg '[] cols '[] -> TAgg '[] cols new) ->
    TypedLazyDataFrame cols ->
    TypedLazyDataFrame (Append cols (Reverse new))
deriveMany build (TLD ldf) =
    TLD (L.deriveMany (taggToNamedExprs (build TAggNil)) ldf)

deriveWithExpr ::
    forall name a cols.
    (KnownSymbol name, C.Columnable a, AssertAbsent name cols) =>
    TExpr cols a ->
    TypedLazyDataFrame cols ->
    ( TExpr (Snoc cols '(name, a)) a
    , TypedLazyDataFrame (Snoc cols '(name, a))
    )
deriveWithExpr e tld = (TExpr (E.Col (nameOf @name)), derive @name e tld)

exclude ::
    forall (names :: [Symbol]) cols.
    (AllKnownSymbol names) =>
    TypedLazyDataFrame cols -> TypedLazyDataFrame (ExcludeSchema names cols)
exclude (TLD ldf) = TLD (L.exclude (symbolVals @names) ldf)

rename ::
    forall old new cols.
    (KnownSymbol old, KnownSymbol new) =>
    TypedLazyDataFrame cols -> TypedLazyDataFrame (RenameInSchema old new cols)
rename (TLD ldf) = TLD (L.rename (nameOf @old) (nameOf @new) ldf)

renameMany ::
    forall (pairs :: [(Symbol, Symbol)]) cols.
    (AllKnownPairs pairs) =>
    TypedLazyDataFrame cols -> TypedLazyDataFrame (RenameManyInSchema pairs cols)
renameMany (TLD ldf) = TLD (L.renameMany (pairVals @pairs) ldf)

impute ::
    forall name a cols.
    (KnownSymbol name, C.Columnable a, Maybe a ~ Lookup name cols) =>
    a -> TypedLazyDataFrame cols -> TypedLazyDataFrame (Impute name cols)
impute v (TLD ldf) =
    TLD
        (L.derive (nameOf @name) (DF.fromMaybe v (DF.col @(Maybe a) (nameOf @name))) ldf)

imputeWith ::
    (ImputeOp a, C.Columnable (BaseType a)) =>
    (TExpr cols (BaseType a) -> TExpr cols (BaseType a)) ->
    TExpr cols a ->
    TypedLazyDataFrame cols ->
    TypedLazyDataFrame cols
imputeWith f (TExpr e) (TLD ldf) = TLD (L.imputeWith (unTExpr . f . TExpr) e ldf)

insert ::
    forall name a cols t.
    (KnownSymbol name, C.Columnable a, Foldable t, AssertAbsent name cols) =>
    t a -> TypedLazyDataFrame cols -> TypedLazyDataFrame ('(name, a) ': cols)
insert xs (TLD ldf) = TLD (L.insert (nameOf @name) xs ldf)

insertColumn ::
    forall name a cols.
    (KnownSymbol name, C.Columnable a, AssertAbsent name cols) =>
    C.Column -> TypedLazyDataFrame cols -> TypedLazyDataFrame ('(name, a) ': cols)
insertColumn c (TLD ldf) = TLD (L.insertColumn (nameOf @name) c ldf)

insertVector ::
    forall name a cols.
    (KnownSymbol name, C.Columnable a, AssertAbsent name cols) =>
    V.Vector a -> TypedLazyDataFrame cols -> TypedLazyDataFrame ('(name, a) ': cols)
insertVector xs (TLD ldf) = TLD (L.insertVector (nameOf @name) xs ldf)

insertUnboxedVector ::
    forall name a cols.
    (KnownSymbol name, C.Columnable a, VU.Unbox a, AssertAbsent name cols) =>
    VU.Vector a ->
    TypedLazyDataFrame cols ->
    TypedLazyDataFrame ('(name, a) ': cols)
insertUnboxedVector xs (TLD ldf) = TLD (L.insertUnboxedVector (nameOf @name) xs ldf)

insertWithDefault ::
    forall name a cols t.
    (KnownSymbol name, C.Columnable a, Foldable t, AssertAbsent name cols) =>
    a -> t a -> TypedLazyDataFrame cols -> TypedLazyDataFrame ('(name, a) ': cols)
insertWithDefault d xs (TLD ldf) = TLD (L.insertWithDefault d (nameOf @name) xs ldf)

insertVectorWithDefault ::
    forall name a cols.
    (KnownSymbol name, C.Columnable a, AssertAbsent name cols) =>
    a ->
    V.Vector a ->
    TypedLazyDataFrame cols ->
    TypedLazyDataFrame ('(name, a) ': cols)
insertVectorWithDefault d xs (TLD ldf) =
    TLD (L.insertVectorWithDefault d (nameOf @name) xs ldf)

cloneColumn ::
    forall old new cols.
    ( KnownSymbol old
    , KnownSymbol new
    , AssertPresent old cols
    , AssertAbsent new cols
    ) =>
    TypedLazyDataFrame cols -> TypedLazyDataFrame ('(new, Lookup old cols) ': cols)
cloneColumn (TLD ldf) = TLD (L.cloneColumn (nameOf @old) (nameOf @new) ldf)

dropColumn ::
    forall name cols.
    (KnownSymbol name, AssertPresent name cols) =>
    TypedLazyDataFrame cols -> TypedLazyDataFrame (RemoveColumn name cols)
dropColumn (TLD ldf) = TLD (L.exclude [nameOf @name] ldf)

replaceColumn ::
    forall name a cols.
    ( KnownSymbol name
    , C.Columnable a
    , a ~ SafeLookup name cols
    , AssertPresent name cols
    ) =>
    TExpr cols a -> TypedLazyDataFrame cols -> TypedLazyDataFrame cols
replaceColumn (TExpr e) (TLD ldf) = TLD (L.derive (nameOf @name) e ldf)

apply ::
    forall name a b cols.
    ( KnownSymbol name
    , a ~ SafeLookup name cols
    , C.Columnable a
    , C.Columnable b
    , AssertPresent name cols
    ) =>
    (a -> b) ->
    TypedLazyDataFrame cols ->
    TypedLazyDataFrame (SetColumnType name b cols)
apply f (TLD ldf) = TLD (L.apply f (nameOf @name) ldf)

applyInt ::
    forall name b cols.
    ( KnownSymbol name
    , Int ~ SafeLookup name cols
    , C.Columnable b
    , AssertPresent name cols
    ) =>
    (Int -> b) ->
    TypedLazyDataFrame cols ->
    TypedLazyDataFrame (SetColumnType name b cols)
applyInt = apply @name

applyDouble ::
    forall name b cols.
    ( KnownSymbol name
    , Double ~ SafeLookup name cols
    , C.Columnable b
    , AssertPresent name cols
    ) =>
    (Double -> b) ->
    TypedLazyDataFrame cols ->
    TypedLazyDataFrame (SetColumnType name b cols)
applyDouble = apply @name

applyMany ::
    forall (names :: [Symbol]) a cols.
    (AllKnownSymbol names, C.Columnable a, AssertAllColumnsHaveType names a cols) =>
    (a -> a) -> TypedLazyDataFrame cols -> TypedLazyDataFrame cols
applyMany f (TLD ldf) = TLD (L.applyMany f (symbolVals @names) ldf)

applyWhere ::
    forall filterName targetName a b cols.
    ( KnownSymbol filterName
    , KnownSymbol targetName
    , a ~ SafeLookup filterName cols
    , b ~ SafeLookup targetName cols
    , C.Columnable a
    , C.Columnable b
    , AssertPresent filterName cols
    , AssertPresent targetName cols
    ) =>
    (a -> Bool) -> (b -> b) -> TypedLazyDataFrame cols -> TypedLazyDataFrame cols
applyWhere p f (TLD ldf) =
    TLD (L.applyWhere p (nameOf @filterName) f (nameOf @targetName) ldf)

applyAtIndex ::
    forall name a cols.
    ( KnownSymbol name
    , a ~ SafeLookup name cols
    , C.Columnable a
    , AssertPresent name cols
    ) =>
    Int -> (a -> a) -> TypedLazyDataFrame cols -> TypedLazyDataFrame cols
applyAtIndex i f (TLD ldf) = TLD (L.applyAtIndex i f (nameOf @name) ldf)

fold ::
    (a -> TypedLazyDataFrame cols -> TypedLazyDataFrame cols) ->
    [a] ->
    TypedLazyDataFrame cols ->
    TypedLazyDataFrame cols
fold f xs tld = foldl (flip f) tld xs

-- | The query with no rows and no columns.
empty :: TypedLazyDataFrame '[]
empty = TLD (L.fromDataFrame (thaw DT.empty))

-- * Merges and set operations

append ::
    TypedLazyDataFrame cols -> TypedLazyDataFrame cols -> TypedLazyDataFrame cols
append (TLD a) (TLD b) = TLD (a <> b)

(|||) ::
    (AssertDisjoint left right) =>
    TypedLazyDataFrame left ->
    TypedLazyDataFrame right ->
    TypedLazyDataFrame (Append left right)
TLD a ||| TLD b = TLD (a L.||| b)

union ::
    TypedLazyDataFrame cols -> TypedLazyDataFrame cols -> TypedLazyDataFrame cols
union (TLD a) (TLD b) = TLD (L.union a b)

intersect ::
    TypedLazyDataFrame cols -> TypedLazyDataFrame cols -> TypedLazyDataFrame cols
intersect (TLD a) (TLD b) = TLD (L.intersect a b)

difference ::
    TypedLazyDataFrame cols -> TypedLazyDataFrame cols -> TypedLazyDataFrame cols
difference (TLD a) (TLD b) = TLD (L.difference a b)

symmetricDifference ::
    TypedLazyDataFrame cols -> TypedLazyDataFrame cols -> TypedLazyDataFrame cols
symmetricDifference (TLD a) (TLD b) = TLD (L.symmetricDifference a b)

-- * Sampling and splitting

randomSplit ::
    (RandomGen g) =>
    g ->
    Double ->
    TypedLazyDataFrame cols ->
    (TypedLazyDataFrame cols, TypedLazyDataFrame cols)
randomSplit g p (TLD ldf) = let (a, b) = L.randomSplit g p ldf in (TLD a, TLD b)

kFolds ::
    (RandomGen g) =>
    g -> Int -> TypedLazyDataFrame cols -> [TypedLazyDataFrame cols]
kFolds g k (TLD ldf) = map TLD (L.kFolds g k ldf)

selectRows :: [Int] -> TypedLazyDataFrame cols -> TypedLazyDataFrame cols
selectRows ixs (TLD ldf) = TLD (L.selectRows ixs ldf)

stratifiedSample ::
    (SplittableGen g, C.Columnable a) =>
    g ->
    Double ->
    TExpr cols a ->
    TypedLazyDataFrame cols ->
    TypedLazyDataFrame cols
stratifiedSample g p (TExpr e) (TLD ldf) = TLD (L.stratifiedSample g p e ldf)

stratifiedSplit ::
    (SplittableGen g, C.Columnable a) =>
    g ->
    Double ->
    TExpr cols a ->
    TypedLazyDataFrame cols ->
    (TypedLazyDataFrame cols, TypedLazyDataFrame cols)
stratifiedSplit g p (TExpr e) (TLD ldf) =
    let (a, b) = L.stratifiedSplit g p e ldf in (TLD a, TLD b)

-- * Frame-valued summaries

summarize :: TypedLazyDataFrame cols -> LazyDataFrame
summarize (TLD ldf) = L.summarize ldf

describeColumns :: TypedLazyDataFrame cols -> LazyDataFrame
describeColumns (TLD ldf) = L.describeColumns ldf

frequencies ::
    (C.Columnable a, Ord a) =>
    TExpr cols a -> TypedLazyDataFrame cols -> LazyDataFrame
frequencies (TExpr e) (TLD ldf) = L.frequencies e ldf

-- * Values

safeApply ::
    forall name a b cols.
    ( KnownSymbol name
    , a ~ SafeLookup name cols
    , C.Columnable a
    , C.Columnable b
    , AssertPresent name cols
    ) =>
    (a -> b) ->
    TypedLazyDataFrame cols ->
    IO (Either DataFrameException (TypedDataFrame (SetColumnType name b cols)))
safeApply f = collectWith (DTApply.safeApply @name f)

null :: TypedLazyDataFrame cols -> IO Bool
null (TLD ldf) = LC.null ldf

dimensions :: TypedLazyDataFrame cols -> IO (Int, Int)
dimensions (TLD ldf) = LC.dimensions ldf

nRows :: TypedLazyDataFrame cols -> IO Int
nRows (TLD ldf) = LC.nRows ldf

nColumns :: TypedLazyDataFrame cols -> IO Int
nColumns (TLD ldf) = LC.nColumns ldf

columnNames :: TypedLazyDataFrame cols -> IO [T.Text]
columnNames (TLD ldf) = LC.columnNames ldf

valueCounts ::
    (Ord a, C.Columnable a) =>
    TExpr cols a -> TypedLazyDataFrame cols -> IO [(a, Int)]
valueCounts (TExpr e) (TLD ldf) = LC.valueCounts e ldf

valueProportions ::
    (Ord a, C.Columnable a) =>
    TExpr cols a -> TypedLazyDataFrame cols -> IO [(a, Double)]
valueProportions (TExpr e) (TLD ldf) = LC.valueProportions e ldf

mean ::
    (C.Columnable a, Real a, VU.Unbox a) =>
    TExpr cols a -> TypedLazyDataFrame cols -> IO Double
mean (TExpr e) (TLD ldf) = LC.mean e ldf

meanMaybe ::
    (C.Columnable a, Real a) =>
    TExpr cols (Maybe a) -> TypedLazyDataFrame cols -> IO Double
meanMaybe (TExpr e) (TLD ldf) = LC.meanMaybe e ldf

median ::
    (C.Columnable a, Real a, VU.Unbox a) =>
    TExpr cols a -> TypedLazyDataFrame cols -> IO Double
median (TExpr e) (TLD ldf) = LC.median e ldf

medianMaybe ::
    (C.Columnable a, Real a) =>
    TExpr cols (Maybe a) -> TypedLazyDataFrame cols -> IO Double
medianMaybe (TExpr e) (TLD ldf) = LC.medianMaybe e ldf

percentile ::
    (C.Columnable a, Real a, VU.Unbox a) =>
    Int -> TExpr cols a -> TypedLazyDataFrame cols -> IO Double
percentile p (TExpr e) (TLD ldf) = LC.percentile p e ldf

genericPercentile ::
    (C.Columnable a, Ord a) =>
    Int -> TExpr cols a -> TypedLazyDataFrame cols -> IO a
genericPercentile p (TExpr e) (TLD ldf) = LC.genericPercentile p e ldf

standardDeviation ::
    (C.Columnable a, Real a, VU.Unbox a) =>
    TExpr cols a -> TypedLazyDataFrame cols -> IO Double
standardDeviation (TExpr e) (TLD ldf) = LC.standardDeviation e ldf

skewness ::
    (C.Columnable a, Real a, VU.Unbox a) =>
    TExpr cols a -> TypedLazyDataFrame cols -> IO Double
skewness (TExpr e) (TLD ldf) = LC.skewness e ldf

variance ::
    (C.Columnable a, Real a, VU.Unbox a) =>
    TExpr cols a -> TypedLazyDataFrame cols -> IO Double
variance (TExpr e) (TLD ldf) = LC.variance e ldf

interQuartileRange ::
    (C.Columnable a, Real a, VU.Unbox a) =>
    TExpr cols a -> TypedLazyDataFrame cols -> IO Double
interQuartileRange (TExpr e) (TLD ldf) = LC.interQuartileRange e ldf

sum ::
    (C.Columnable a, Num a) => TExpr cols a -> TypedLazyDataFrame cols -> IO a
sum (TExpr e) (TLD ldf) = LC.sum e ldf

correlation ::
    forall c1 c2 a b cols.
    ( KnownSymbol c1
    , KnownSymbol c2
    , a ~ SafeLookup c1 cols
    , b ~ SafeLookup c2 cols
    , C.Columnable a
    , C.Columnable b
    , Real a
    , Real b
    , VU.Unbox a
    , VU.Unbox b
    , AssertPresent c1 cols
    , AssertPresent c2 cols
    ) =>
    TypedLazyDataFrame cols -> IO (Maybe Double)
correlation (TLD ldf) = LC.correlation (nameOf @c1) (nameOf @c2) ldf

columnAsVector ::
    forall name cols a.
    ( KnownSymbol name
    , a ~ SafeLookup name cols
    , C.Columnable a
    , AssertPresent name cols
    ) =>
    TypedLazyDataFrame cols -> IO (V.Vector a)
columnAsVector = collectWith (DTA.columnAsVector @name)

columnAsList ::
    forall name cols a.
    ( KnownSymbol name
    , a ~ SafeLookup name cols
    , C.Columnable a
    , AssertPresent name cols
    ) =>
    TypedLazyDataFrame cols -> IO [a]
columnAsList = collectWith (DTA.columnAsList @name)

columnAsIntVector ::
    forall name cols a.
    ( KnownSymbol name
    , a ~ SafeLookup name cols
    , C.Columnable a
    , AssertRealColumn "columnAsIntVector" name a
    , Real a
    , VU.Unbox a
    , AssertPresent name cols
    ) =>
    TypedLazyDataFrame cols -> IO (VU.Vector Int)
columnAsIntVector = collectWith (DTA.columnAsIntVector @name)

columnAsDoubleVector ::
    forall name cols a.
    ( KnownSymbol name
    , a ~ SafeLookup name cols
    , C.Columnable a
    , AssertRealColumn "columnAsDoubleVector" name a
    , Real a
    , VU.Unbox a
    , AssertPresent name cols
    ) =>
    TypedLazyDataFrame cols -> IO (VU.Vector Double)
columnAsDoubleVector = collectWith (DTA.columnAsDoubleVector @name)

columnAsFloatVector ::
    forall name cols a.
    ( KnownSymbol name
    , a ~ SafeLookup name cols
    , C.Columnable a
    , AssertRealColumn "columnAsFloatVector" name a
    , Real a
    , VU.Unbox a
    , AssertPresent name cols
    ) =>
    TypedLazyDataFrame cols -> IO (VU.Vector Float)
columnAsFloatVector = collectWith (DTA.columnAsFloatVector @name)

columnAsUnboxedVector ::
    forall name cols a.
    ( KnownSymbol name
    , a ~ SafeLookup name cols
    , C.Columnable a
    , VU.Unbox a
    , AssertPresent name cols
    ) =>
    TypedLazyDataFrame cols -> IO (VU.Vector a)
columnAsUnboxedVector = collectWith (DTA.columnAsUnboxedVector @name)

toDoubleMatrix ::
    (AllColumnsReal "toDoubleMatrix" cols) =>
    TypedLazyDataFrame cols -> IO (V.Vector (VU.Vector Double))
toDoubleMatrix = collectWith DTA.toDoubleMatrix

toFloatMatrix ::
    (AllColumnsReal "toFloatMatrix" cols) =>
    TypedLazyDataFrame cols -> IO (V.Vector (VU.Vector Float))
toFloatMatrix = collectWith DTA.toFloatMatrix

toIntMatrix ::
    (AllColumnsReal "toIntMatrix" cols) =>
    TypedLazyDataFrame cols -> IO (V.Vector (VU.Vector Int))
toIntMatrix = collectWith DTA.toIntMatrix

-- | Parse records out of the result.
toRecords ::
    forall a.
    (HasSchema a) => TypedLazyDataFrame (Schema a) -> IO (Either T.Text [a])
toRecords (TLD ldf) = LC.toRecords ldf

-- * Rendering and output

display :: DisplayOptions -> TypedLazyDataFrame cols -> IO ()
display opts (TLD ldf) = LC.display opts ldf

-- | Render a typed expression as source-like text.
prettyPrint :: TExpr cols a -> String
prettyPrint (TExpr e) = E.prettyPrint e

toCsv :: TypedLazyDataFrame cols -> IO T.Text
toCsv (TLD ldf) = LC.toCsv ldf

toCsv' :: TypedLazyDataFrame cols -> IO String
toCsv' (TLD ldf) = LC.toCsv' ldf

toMarkdown :: TypedLazyDataFrame cols -> IO T.Text
toMarkdown (TLD ldf) = LC.toMarkdown ldf

toMarkdown' :: TypedLazyDataFrame cols -> IO String
toMarkdown' (TLD ldf) = LC.toMarkdown' ldf

toSeparated :: Char -> TypedLazyDataFrame cols -> IO T.Text
toSeparated sep (TLD ldf) = LC.toSeparated sep ldf

writeCsv :: FilePath -> TypedLazyDataFrame cols -> IO ()
writeCsv path (TLD ldf) = LC.writeCsv path ldf

writeTsv :: FilePath -> TypedLazyDataFrame cols -> IO ()
writeTsv path (TLD ldf) = LC.writeTsv path ldf

writeSeparated :: Char -> FilePath -> TypedLazyDataFrame cols -> IO ()
writeSeparated sep path (TLD ldf) = LC.writeSeparated sep path ldf

writeParquet :: FilePath -> TypedLazyDataFrame cols -> IO ()
writeParquet path (TLD ldf) = LC.writeParquet path ldf

writeParquetWithOptions ::
    ParquetWriteOptions -> FilePath -> TypedLazyDataFrame cols -> IO ()
writeParquetWithOptions opts path (TLD ldf) = LC.writeParquetWithOptions opts path ldf
