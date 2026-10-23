{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

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
    take,
    sortBy,

    -- * Schema-modifying operations
    derive,
    select,

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
import Prelude hiding (filter, take)

import DataFrame.Expression.Operators ((|>))
import DataFrame.IO.CSV (CsvReader)
import qualified DataFrame.Internal.Column as C
import DataFrame.Lazy.Internal.DataFrame (LazyDataFrame)
import qualified DataFrame.Lazy.Internal.DataFrame as L
import DataFrame.Operations.Join (JoinType (..))
import DataFrame.Schema (RuntimeSchema (..))
import DataFrame.Typed.Aggregate (as)
import DataFrame.Typed.Expr
import DataFrame.Typed.Freeze (unsafeFreeze)
import DataFrame.Typed.Join (
    Fsts,
    JoinKeys,
    JoinOnSchema,
    JoinSchema,
    KnownJoinType (..),
    SwapPairs,
 )
import DataFrame.Typed.Operations (AllKnownPairs (..), toUntypedSort)
import DataFrame.Typed.Schema
import DataFrame.Typed.Types

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
