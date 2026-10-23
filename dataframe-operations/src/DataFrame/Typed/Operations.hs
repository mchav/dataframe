{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

module DataFrame.Typed.Operations (
    -- * Schema-preserving operations
    filter,
    filterBy,
    filterAllJust,
    filterJust,
    filterNothing,
    filterAllNothing,
    sortBy,
    topBy,
    take,
    takeLast,
    drop,
    dropLast,
    range,
    cube,
    distinct,
    sample,
    shuffle,

    -- * Schema-modifying operations
    derive,
    deriveMany,
    impute,
    select,
    exclude,
    rename,
    renameMany,
    insert,
    insertColumn,
    insertVector,
    cloneColumn,
    dropColumn,
    replaceColumn,

    -- * Frequencies
    valueCounts,
    valueProportions,

    -- * Construction and folds
    empty,
    null,
    fold,

    -- * Metadata
    dimensions,
    nRows,
    nColumns,
    columnNames,

    -- * Vertical merge
    append,

    -- * Set algebra (topos operations)
    union,
    intersect,
    difference,
    symmetricDifference,

    -- * Helpers
    AllKnownPairs (..),
    toUntypedSort,
) where

import qualified Data.List as L
import Data.Proxy (Proxy (..))
import qualified Data.Text as T
import qualified Data.Vector as V
import GHC.TypeLits (KnownSymbol, Symbol, symbolVal)
import System.Random (RandomGen)
import Prelude hiding (drop, filter, null, take)

import qualified DataFrame.Functions as DF
import DataFrame.Internal.Column (Columnable)
import qualified DataFrame.Internal.Column as C
import qualified DataFrame.Internal.DataFrame as D
import qualified DataFrame.Operations.Aggregation as DA
import qualified DataFrame.Operations.Core as D
import DataFrame.Operations.Merge ()
import qualified DataFrame.Operations.Permutation as D
import qualified DataFrame.Operations.SetOps as DS
import qualified DataFrame.Operations.Subset as D
import qualified DataFrame.Operations.Transformations as D

import DataFrame.Typed.Freeze (unsafeFreeze)
import DataFrame.Typed.Schema
import DataFrame.Typed.Types (
    TAgg (..),
    TExpr (..),
    TSortOrder (..),
    TypedDataFrame (..),
    taggToNamedExprs,
 )

-- | Filter rows where a boolean expression evaluates to True.
filter :: TExpr cols Bool -> TypedDataFrame cols -> TypedDataFrame cols
filter (TExpr expr) (TDF df) = TDF (D.filter expr df)

-- | Filter rows by applying a predicate to a typed expression.
filterBy ::
    (Columnable a) =>
    (a -> Bool) -> TExpr cols a -> TypedDataFrame cols -> TypedDataFrame cols
filterBy pred' (TExpr expr) (TDF df) = TDF (D.filterBy pred' expr df)

-- | Keep only rows where all Optional columns have Just values.
filterAllJust :: TypedDataFrame cols -> TypedDataFrame (StripAllMaybe cols)
filterAllJust (TDF df) = unsafeFreeze (D.filterAllJust df)

-- | Keep only rows where the named column has Just values.
filterJust ::
    forall name cols.
    ( KnownSymbol name
    , AssertPresent name cols
    ) =>
    TypedDataFrame cols -> TypedDataFrame (StripMaybeAt name cols)
filterJust (TDF df) = unsafeFreeze (D.filterJust colName df)
  where
    colName = T.pack (symbolVal (Proxy @name))

-- | Keep only rows where the named column has Nothing.
filterNothing ::
    forall name cols.
    ( KnownSymbol name
    , AssertPresent name cols
    ) =>
    TypedDataFrame cols -> TypedDataFrame cols
filterNothing (TDF df) = TDF (D.filterNothing colName df)
  where
    colName = T.pack (symbolVal (Proxy @name))

-- | Keep only rows where every nullable column has Nothing.
filterAllNothing :: TypedDataFrame cols -> TypedDataFrame cols
filterAllNothing (TDF df) = TDF (D.filterAllNothing df)

-- | Sort by the given typed sort orders, validated against the schema.
sortBy :: [TSortOrder cols] -> TypedDataFrame cols -> TypedDataFrame cols
sortBy ords (TDF df) = TDF (D.sortBy (map toUntypedSort ords) df)

-- | Rows @[offset, offset + count)@ of the frame sorted by the given orders.
topBy ::
    Int -> Int -> [TSortOrder cols] -> TypedDataFrame cols -> TypedDataFrame cols
topBy offset count ords (TDF df) =
    TDF (D.topBy offset count (map toUntypedSort ords) df)

toUntypedSort :: TSortOrder cols -> D.SortOrder
toUntypedSort (Asc (TExpr e)) = D.Asc e
toUntypedSort (Desc (TExpr e)) = D.Desc e

-- | Take the first @n@ rows.
take :: Int -> TypedDataFrame cols -> TypedDataFrame cols
take n (TDF df) = TDF (D.take n df)

-- | Take the last @n@ rows.
takeLast :: Int -> TypedDataFrame cols -> TypedDataFrame cols
takeLast n (TDF df) = TDF (D.takeLast n df)

-- | Drop the first @n@ rows.
drop :: Int -> TypedDataFrame cols -> TypedDataFrame cols
drop n (TDF df) = TDF (D.drop n df)

-- | Drop the last @n@ rows.
dropLast :: Int -> TypedDataFrame cols -> TypedDataFrame cols
dropLast n (TDF df) = TDF (D.dropLast n df)

-- | Take rows in the given range (start, end).
range :: (Int, Int) -> TypedDataFrame cols -> TypedDataFrame cols
range r (TDF df) = TDF (D.range r df)

-- | Take a sub-cube of the DataFrame.
cube :: (Int, Int) -> TypedDataFrame cols -> TypedDataFrame cols
cube c (TDF df) = TDF (D.cube c df)

-- | Remove duplicate rows.
distinct :: TypedDataFrame cols -> TypedDataFrame cols
distinct (TDF df) = TDF (DA.distinct df)

-- | Randomly sample a fraction of rows.
sample ::
    (RandomGen g) => g -> Double -> TypedDataFrame cols -> TypedDataFrame cols
sample g frac (TDF df) = TDF (D.sample g frac df)

-- | Shuffle all rows randomly.
shuffle :: (RandomGen g) => g -> TypedDataFrame cols -> TypedDataFrame cols
shuffle g (TDF df) = TDF (D.shuffle g df)

-- | Derive a new column from a typed expression; its name must be absent.
derive ::
    forall name a cols.
    ( KnownSymbol name
    , Columnable a
    , AssertAbsent name cols
    ) =>
    TExpr cols a ->
    TypedDataFrame cols ->
    TypedDataFrame (Snoc cols '(name, a))
derive (TExpr expr) (TDF df) = unsafeFreeze (D.derive colName expr df)
  where
    colName = T.pack (symbolVal (Proxy @name))

-- | Add several columns at once; build them with 'DataFrame.Typed.as'.
deriveMany ::
    forall new cols.
    (AssertAllAbsent (Reverse new) cols) =>
    (TAgg '[] cols '[] -> TAgg '[] cols new) ->
    TypedDataFrame cols ->
    TypedDataFrame (Append cols (Reverse new))
deriveMany build (TDF df) =
    unsafeFreeze (D.deriveMany (taggToNamedExprs (build TAggNil)) df)

impute ::
    forall name a cols.
    ( KnownSymbol name
    , Columnable a
    , Maybe a ~ Lookup name cols
    ) =>
    a ->
    TypedDataFrame cols ->
    TypedDataFrame (Impute name cols)
impute value (TDF df) =
    unsafeFreeze
        (D.derive colName (DF.fromMaybe value (DF.col @(Maybe a) colName)) df)
  where
    colName = T.pack (symbolVal (Proxy @name))

-- | Select a subset of columns by name.
select ::
    forall (names :: [Symbol]) cols.
    (AllKnownSymbol names, AssertAllPresent names cols) =>
    TypedDataFrame cols -> TypedDataFrame (SubsetSchema names cols)
select (TDF df) = unsafeFreeze (D.select (symbolVals @names) df)

-- | Exclude columns by name.
exclude ::
    forall (names :: [Symbol]) cols.
    (AllKnownSymbol names) =>
    TypedDataFrame cols -> TypedDataFrame (ExcludeSchema names cols)
exclude (TDF df) = unsafeFreeze (D.exclude (symbolVals @names) df)

-- | Rename a column.
rename ::
    forall old new cols.
    (KnownSymbol old, KnownSymbol new) =>
    TypedDataFrame cols -> TypedDataFrame (RenameInSchema old new cols)
rename (TDF df) = unsafeFreeze (D.rename oldName newName df)
  where
    oldName = T.pack (symbolVal (Proxy @old))
    newName = T.pack (symbolVal (Proxy @new))

-- | Rename multiple columns from a type-level list of pairs.
renameMany ::
    forall (pairs :: [(Symbol, Symbol)]) cols.
    (AllKnownPairs pairs) =>
    TypedDataFrame cols -> TypedDataFrame (RenameManyInSchema pairs cols)
renameMany (TDF df) = unsafeFreeze (foldRenames (pairVals @pairs) df)
  where
    foldRenames [] df' = df'
    foldRenames ((old, new) : rest) df' = foldRenames rest (D.rename old new df')

-- | Insert a new column from a Foldable container.
insert ::
    forall name a cols t.
    ( KnownSymbol name
    , Columnable a
    , Foldable t
    , AssertAbsent name cols
    ) =>
    t a -> TypedDataFrame cols -> TypedDataFrame ('(name, a) ': cols)
insert xs (TDF df) = unsafeFreeze (D.insert colName xs df)
  where
    colName = T.pack (symbolVal (Proxy @name))

-- | Insert a raw 'Column' value.
insertColumn ::
    forall name a cols.
    ( KnownSymbol name
    , Columnable a
    , AssertAbsent name cols
    ) =>
    C.Column -> TypedDataFrame cols -> TypedDataFrame ('(name, a) ': cols)
insertColumn col (TDF df) = unsafeFreeze (D.insertColumn colName col df)
  where
    colName = T.pack (symbolVal (Proxy @name))

-- | Insert a boxed 'Vector'.
insertVector ::
    forall name a cols.
    ( KnownSymbol name
    , Columnable a
    , AssertAbsent name cols
    ) =>
    V.Vector a -> TypedDataFrame cols -> TypedDataFrame ('(name, a) ': cols)
insertVector vec (TDF df) = unsafeFreeze (D.insertVector colName vec df)
  where
    colName = T.pack (symbolVal (Proxy @name))

-- | Clone an existing column under a new name.
cloneColumn ::
    forall old new cols.
    ( KnownSymbol old
    , KnownSymbol new
    , AssertPresent old cols
    , AssertAbsent new cols
    ) =>
    TypedDataFrame cols -> TypedDataFrame ('(new, Lookup old cols) ': cols)
cloneColumn (TDF df) = unsafeFreeze (D.cloneColumn oldName newName df)
  where
    oldName = T.pack (symbolVal (Proxy @old))
    newName = T.pack (symbolVal (Proxy @new))

-- | Drop a column by name.
dropColumn ::
    forall name cols.
    ( KnownSymbol name
    , AssertPresent name cols
    ) =>
    TypedDataFrame cols -> TypedDataFrame (RemoveColumn name cols)
dropColumn (TDF df) = unsafeFreeze (D.exclude [colName] df)
  where
    colName = T.pack (symbolVal (Proxy @name))

-- | Replace an existing column with values from a typed expression.
replaceColumn ::
    forall name a cols.
    ( KnownSymbol name
    , Columnable a
    , a ~ SafeLookup name cols
    , AssertPresent name cols
    ) =>
    TExpr cols a -> TypedDataFrame cols -> TypedDataFrame cols
replaceColumn (TExpr expr) (TDF df) = unsafeFreeze (D.derive colName expr df)
  where
    colName = T.pack (symbolVal (Proxy @name))

-- | Vertically merge two DataFrames with the same schema.
append :: TypedDataFrame cols -> TypedDataFrame cols -> TypedDataFrame cols
append (TDF a) (TDF b) = TDF (a <> b)

-- | Rows appearing in either DataFrame, deduplicated (set union).
union :: TypedDataFrame cols -> TypedDataFrame cols -> TypedDataFrame cols
union (TDF a) (TDF b) = TDF (DS.union a b)

-- | Rows appearing in both DataFrames, deduplicated (set intersection).
intersect :: TypedDataFrame cols -> TypedDataFrame cols -> TypedDataFrame cols
intersect (TDF a) (TDF b) = TDF (DS.intersect a b)

-- | Rows in the left DataFrame but not the right, deduplicated.
difference :: TypedDataFrame cols -> TypedDataFrame cols -> TypedDataFrame cols
difference (TDF a) (TDF b) = TDF (DS.difference a b)

-- | Rows in exactly one of the two DataFrames, deduplicated.
symmetricDifference ::
    TypedDataFrame cols -> TypedDataFrame cols -> TypedDataFrame cols
symmetricDifference (TDF a) (TDF b) = TDF (DS.symmetricDifference a b)

-- | The frame with no rows and no columns.
empty :: TypedDataFrame '[]
empty = TDF D.empty

-- | True when the frame has no rows or no columns.
null :: TypedDataFrame cols -> Bool
null (TDF df) = D.null df

-- | Thread the frame through a schema-preserving step for each element.
fold ::
    (a -> TypedDataFrame cols -> TypedDataFrame cols) ->
    [a] ->
    TypedDataFrame cols ->
    TypedDataFrame cols
fold f xs df = L.foldl' (flip f) df xs

dimensions :: TypedDataFrame cols -> (Int, Int)
dimensions (TDF df) = D.dimensions df

nRows :: TypedDataFrame cols -> Int
nRows (TDF df) = D.nRows df

nColumns :: TypedDataFrame cols -> Int
nColumns (TDF df) = D.nColumns df

columnNames :: TypedDataFrame cols -> [T.Text]
columnNames (TDF df) = D.columnNames df

-- | Count occurrences of each distinct value in a column.
valueCounts ::
    (Ord a, Columnable a) => TExpr cols a -> TypedDataFrame cols -> [(a, Int)]
valueCounts (TExpr e) (TDF df) = D.valueCounts e df

-- | Proportion of each distinct value in a column.
valueProportions ::
    (Ord a, Columnable a) => TExpr cols a -> TypedDataFrame cols -> [(a, Double)]
valueProportions (TExpr e) (TDF df) = D.valueProportions e df

-- | Helper class for extracting [(Text, Text)] from type-level pairs.
class AllKnownPairs (pairs :: [(Symbol, Symbol)]) where
    pairVals :: [(T.Text, T.Text)]

instance AllKnownPairs '[] where
    pairVals = []

instance
    (KnownSymbol a, KnownSymbol b, AllKnownPairs rest) =>
    AllKnownPairs ('(a, b) ': rest)
    where
    pairVals =
        ( T.pack (symbolVal (Proxy @a))
        , T.pack (symbolVal (Proxy @b))
        )
            : pairVals @rest
