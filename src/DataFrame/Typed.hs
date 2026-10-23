{-# LANGUAGE CPP #-}
{-# LANGUAGE DataKinds #-}

-- | A type-safe layer that tracks column names and types over the core API.
module DataFrame.Typed (
    -- * Core types
    TypedDataFrame,
    TypedGrouped,
    These (..),

    -- * Typed expressions (combinators live in "DataFrame.Typed.Functions")
    TExpr (..),
    col,
    lit,
    ifThenElse,
    caseWhen,
    orElse,

    -- * Same-type comparison operators
    (.==.),
    (./=.),
    (.<.),
    (.<=.),
    (.>=.),
    (.>.),

    -- * Same-type arithmetic operators
    (.+.),
    (.-.),
    (.*.),
    (./.),
    (.^^.),
    (.^.),

    -- * Nullable-aware arithmetic operators
    (.+),
    (.-),
    (.*),
    (./),
    (.^^),
    (.^),

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
    (.&&),
    (.||),

    -- * Typed sort orders
    TSortOrder (..),

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

    -- * Construction and folds
    empty,
    null,
    fold,
    (|>),

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
    JoinType (..),
    join,
    joinOn,
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
    apply,
    applyInt,
    applyDouble,
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

    -- * Statistics
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
    frequencies,
    imputeWith,
    summarize,
    describeColumns,

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
    fromRecords,
    toRecords,

    -- * Display and rendering
    DisplayOptions (..),
    defaultDisplayOptions,
    display,
    prettyPrint,
    toMarkdown,
    toMarkdown',
    toCsv,
    toCsv',
    toSeparated,

    -- * IO
    readJSON,
    readJSONEither,
#ifdef WITH_CSV
    ReadOptions (..),
    defaultReadOptions,
    readCsv,
    readCsvWithError,
    readCsvWithOpts,
    readTsv,
    readSeparated,
    writeCsv,
    writeTsv,
    writeSeparated,
#endif
#ifdef WITH_PARQUET
    ParquetReadOptions (..),
    ParquetWriteOptions (..),
    defaultParquetReadOptions,
    defaultParquetWriteOptions,
    readParquet,
    readParquetWithError,
    readParquetWithOpts,
    readParquetFiles,
    readParquetFilesWithOpts,
    writeParquet,
    writeParquetWithOptions,
#endif

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

import Prelude hiding (drop, filter, null, sum, take)

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
    apply,
    applyDouble,
    applyInt,
    applyAtIndex,
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
import DataFrame.Typed.Expr (
    TExpr (..),
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
import DataFrame.Typed.Statistics (
    correlation,
    describeColumns,
    frequencies,
    genericPercentile,
    imputeWith,
    interQuartileRange,
    mean,
    meanMaybe,
    median,
    medianMaybe,
    percentile,
    skewness,
    standardDeviation,
    summarize,
    sum,
    variance,
 )
import DataFrame.Typed.Freeze (freeze, freezeWithError, thaw, unsafeFreeze)
import DataFrame.Typed.Generic (
    NameCase (..),
    SchemaOf,
    SchemaOfRaw,
    genericFromColumns,
    genericToColumns,
 )
import DataFrame.Display (DisplayOptions (..), defaultDisplayOptions)
import DataFrame.Expression.Operators ((|>))
import DataFrame.Operations.Join (JoinType (..))
#ifdef WITH_CSV
import DataFrame.IO.CSV (ReadOptions (..), defaultReadOptions)
import DataFrame.Typed.IO.CSV (
    readCsv,
    readCsvWithError,
    readCsvWithOpts,
    readSeparated,
    readTsv,
    writeCsv,
    writeSeparated,
    writeTsv,
 )
#endif
#ifdef WITH_PARQUET
import DataFrame.IO.Parquet (
    ParquetReadOptions (..),
    ParquetWriteOptions (..),
    defaultParquetReadOptions,
    defaultParquetWriteOptions,
 )
import DataFrame.Typed.IO.Parquet (
    readParquet,
    readParquetFiles,
    readParquetFilesWithOpts,
    readParquetWithError,
    readParquetWithOpts,
    writeParquet,
    writeParquetWithOptions,
 )
#endif
import DataFrame.Typed.IO.JSON (readJSON, readJSONEither)
import DataFrame.Typed.Interop (
    display,
    fromRecords,
    prettyPrint,
    toCsv,
    toCsv',
    toMarkdown,
    toMarkdown',
    toRecords,
    toSeparated,
 )
import DataFrame.Typed.Join (
    fullOuterJoin,
    innerJoin,
    join,
    joinOn,
    leftJoin,
    rightJoin,
 )
import DataFrame.Typed.Operations
import DataFrame.Typed.Record (HasSchema (..))
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
