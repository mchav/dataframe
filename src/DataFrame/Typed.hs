{-# LANGUAGE CPP #-}
{-# LANGUAGE DataKinds #-}

-- | A type-safe layer that tracks column names and types over the core API.
module DataFrame.Typed (
    -- * Core types
    TypedDataFrame,
    TypedGrouped,
    These (..),

    -- * Typed expressions
    TExpr (..),
    col,
    lit,
    ifThenElse,
    caseWhen,
    orElse,
    lift,
    lift2,
    nullLift,
    nullLift2,

    -- * Same-type comparison operators
    (.==.),
    (./=.),
    (.<.),
    (.<=.),
    (.>=.),
    (.>.),

    -- * Nullable-aware arithmetic operators
    (.+),
    (.-),
    (.*),
    (./),

    -- * Nullable-aware comparison operators (three-valued logic)
    (.==),
    (./=),
    (.<),
    (.<=),
    (.>=),
    (.>),

    -- * Logical operators
    (.&&.),
    (.||.),
    DataFrame.Typed.Expr.not,

    -- * Aggregation expression combinators
    DataFrame.Typed.Expr.sum,
    mean,
    median,
    count,
    countAll,
    DataFrame.Typed.Expr.minimum,
    DataFrame.Typed.Expr.maximum,
    collect,
    over,

    -- * Expression combinators (full DataFrame.Functions parity)
    DataFrame.Typed.Expr.div,
    DataFrame.Typed.Expr.mod,
    mode,
    sumMaybe,
    DataFrame.Typed.Expr.meanMaybe,
    DataFrame.Typed.Expr.variance,
    DataFrame.Typed.Expr.medianMaybe,
    DataFrame.Typed.Expr.percentile,
    stddev,
    stddevMaybe,
    zScore,
    pow,
    relu,
    DataFrame.Typed.Expr.min,
    DataFrame.Typed.Expr.max,
    reduce,
    toMaybe,
    fromMaybe,
    isJust,
    isNothing,
    fromJust,
    whenPresent,
    whenBothPresent,
    recode,
    recodeWithCondition,
    recodeWithDefault,
    firstOrNothing,
    lastOrNothing,
    splitOn,
    match,
    matchAll,
    parseDate,
    daysBetween,
    bind,

    -- * Cast / coercion expressions
    castExpr,
    castExprWithDefault,
    castExprEither,
    unsafeCastExpr,
    toDouble,

    -- * Typed sort orders
    TSortOrder (..),
    asc,
    desc,

    -- * Freeze / thaw boundary
    freeze,
    freezeWithError,
    thaw,
    unsafeFreeze,

    -- * Typed column access
    columnAsVector,
    columnAsList,
    columnAsIntVector,
    columnAsDoubleVector,
    columnAsFloatVector,
    columnAsUnboxedVector,
    toDoubleMatrix,
    toFloatMatrix,
    toIntMatrix,

    -- * Schema-preserving operations
    filterWhere,
    filter,
    filterBy,
    filterAllJust,
    filterJust,
    filterNothing,
    filterAllNothing,
    sortBy,
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

    -- * Joins
    innerJoin,
    leftJoin,
    rightJoin,
    fullOuterJoin,

    -- * GroupBy and Aggregation
    groupBy,
    as,
    aggregate,
    aggregateUntyped,

    -- * Column transformations
    applyColumn,
    applyMany,
    applyWhere,
    applyAtIndex,
    safeApply,
    deriveWithExpr,
    insertWithDefault,
    insertVectorWithDefault,
    insertUnboxedVector,
    (|||),

    -- * Sampling and splitting
    randomSplit,
    kFolds,
    selectRows,
    stratifiedSample,
    stratifiedSplit,

    -- * Frequencies
    valueCounts,
    valueProportions,

#ifdef WITH_TH
    -- * Template Haskell
    deriveSchema,
#ifdef WITH_CSV_TH
    deriveSchemaFromCsvFile,
    deriveSchemaFromCsvFileWith,
#endif
#ifdef WITH_PARQUET_TH
    deriveSchemaFromParquetFile,
#endif
    deriveSchemaFromType,
    deriveSchemaFromTypeWith,
    SchemaOptions (..),
    defaultSchemaOptions,
#endif

    -- * Record bridge (ADT <-> TypedDataFrame)
    HasSchema (..),
    fromRecordsTyped,
    toRecordsTyped,

    -- * Generics opt-in for schema derivation
    SchemaOf,
    SchemaOfRaw,
    NameCase (..),
    genericToColumns,
    genericFromColumns,

    -- * Schema type families (for advanced use)
    Lookup,
    SafeLookup,
    HasName,
    SubsetSchema,
    ExcludeSchema,
    RenameInSchema,
    RenameManyInSchema,
    RemoveColumn,
    Impute,
    SetColumnType,
    Append,
    Reverse,
    StripAllMaybe,
    StripMaybeAt,
    GroupKeyColumns,
    InnerJoinSchema,
    LeftJoinSchema,
    RightJoinSchema,
    FullOuterJoinSchema,
    AssertAbsent,
    AssertAllPresent,
    AssertPresent,
    AssertDisjoint,
    AssertRealColumn,
    AllColumnsReal,
    IsRealType,

    -- * Constraints
    KnownSchema (..),
    schemaColumnNames,
    AllKnownSymbol (..),
) where

import Prelude hiding (drop, filter, take)

import DataFrame.Typed.Access (
    columnAsDoubleVector,
    columnAsFloatVector,
    columnAsIntVector,
    columnAsList,
    columnAsUnboxedVector,
    columnAsVector,
    toDoubleMatrix,
    toFloatMatrix,
    toIntMatrix,
 )
import DataFrame.Typed.Aggregate (
    aggregate,
    aggregateUntyped,
    as,
    groupBy,
 )
import DataFrame.Typed.Apply (
    applyAtIndex,
    applyColumn,
    applyMany,
    applyWhere,
    deriveWithExpr,
    insertUnboxedVector,
    insertVectorWithDefault,
    insertWithDefault,
    safeApply,
    (|||),
 )
import DataFrame.Typed.Sampling (
    kFolds,
    randomSplit,
    selectRows,
    stratifiedSample,
    stratifiedSplit,
 )
import DataFrame.Typed.Expr
import DataFrame.Typed.Freeze (freeze, freezeWithError, thaw, unsafeFreeze)
import DataFrame.Typed.Generic (
    NameCase (..),
    SchemaOf,
    SchemaOfRaw,
    genericFromColumns,
    genericToColumns,
 )
import DataFrame.Typed.Join (fullOuterJoin, innerJoin, leftJoin, rightJoin)
import DataFrame.Typed.Operations
import DataFrame.Typed.Record (
    HasSchema (..),
    fromRecordsTyped,
    toRecordsTyped,
 )
import DataFrame.Typed.Schema
#ifdef WITH_TH
import DataFrame.Typed.TH (
    SchemaOptions (..),
    defaultSchemaOptions,
    deriveSchema,
#ifdef WITH_CSV_TH
    deriveSchemaFromCsvFile,
    deriveSchemaFromCsvFileWith,
#endif
#ifdef WITH_PARQUET_TH
    deriveSchemaFromParquetFile,
#endif
    deriveSchemaFromType,
    deriveSchemaFromTypeWith,
 )
#endif
import DataFrame.Typed.Types (
    TSortOrder (..),
    These (..),
    TypedDataFrame,
    TypedGrouped,
 )
