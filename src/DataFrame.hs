{-# LANGUAGE OverloadedStrings #-}

{- |
Module      : DataFrame
Copyright   : (c) 2025
License     : GPL-3.0
Maintainer  : mschavinda@gmail.com
Stability   : experimental
Portability : POSIX

Batteries-included entry point for the DataFrame library.

This module re-exports the most commonly used pieces of the @dataframe@ library so you
can get productive fast in GHCi, IHaskell, or scripts.

__Naming convention__
* Use the @D.@ (\"DataFrame\") prefix for core table operations.
* Use the @F.@ (\"Functions\") prefix for the expression DSL (columns, math, aggregations).

Example session:

@
-- GHCi quality-of-life:
ghci> :set -XOverloadedStrings -XTypeApplications
ghci> :module + DataFrame as D, DataFrame.Functions as F, Data.Text (Text)
@

= Quick start
Load a CSV, select a few columns, filter, derive a column, then group + aggregate:

@
-- 1) Load data
ghci> df0 <- D.readCsv "data/housing.csv"
ghci> D.describeColumns df0
--------------------------------------------------------------------------------------------------------------------
index |    Column Name     | # Non-null Values | # Null Values | # Partially parsed | # Unique Values |     Type
------|--------------------|-------------------|---------------|--------------------|-----------------|-------------
 Int  |        Text        |        Int        |      Int      |        Int         |       Int       |     Text
------|--------------------|-------------------|---------------|--------------------|-----------------|-------------
0     | ocean_proximity    | 20640             | 0             | 0                  | 5               | Text
1     | median_house_value | 20640             | 0             | 0                  | 3842            | Double
2     | median_income      | 20640             | 0             | 0                  | 12928           | Double
3     | households         | 20640             | 0             | 0                  | 1815            | Double
4     | population         | 20640             | 0             | 0                  | 3888            | Double
5     | total_bedrooms     | 20640             | 0             | 0                  | 1924            | Maybe Double
6     | total_rooms        | 20640             | 0             | 0                  | 5926            | Double
7     | housing_median_age | 20640             | 0             | 0                  | 52              | Double
8     | latitude           | 20640             | 0             | 0                  | 862             | Double
9     | longitude          | 20640             | 0             | 0                  | 844             | Double

-- 2) Project & filter
ghci> let df1 = df1 = D.filter @Text "ocean_proximity" (== "ISLAND") df0 D.|> D.select ["median_house_value", "median_income", "ocean_proximity"]

-- 3) Add a derived column using the expression DSL
--    (col types are explicit via TypeApplications)
ghci> df2 = D.derive "rooms_per_household" (F.col @Double "total_rooms" / F.col @Double "households") df0

-- 4) Group + aggregate
ghci> let grouped   = D.groupBy ["ocean_proximity"] df0
ghci> let summary   =
         D.aggregate
             [ F.maximum (F.col @Double "median_house_value") `F.as` "max_house_value"]
             grouped
ghci> D.take 5 summary
-----------------------------------------
index | ocean_proximity | max_house_value
------|-----------------|----------------
 Int  |      Text       |     Double
------|-----------------|----------------
0     | <1H OCEAN       | 500001.0
1     | INLAND          | 500001.0
2     | ISLAND          | 450000.0
3     | NEAR BAY        | 500001.0
4     | NEAR OCEAN      | 500001.0
@

== Simple operations (cheat sheet)
Most users only need a handful of verbs:

__I/O__

  * @D.readCsv :: FilePath -> IO DataFrame@
  * @D.writeCsv :: FilePath -> DataFrame -> IO ()@
  * @D.readParquet :: FilePath -> IO DataFrame@

__Exploration__

  * @D.take :: Int -> DataFrame -> DataFrame@
  * @D.takeLast :: Int -> DataFrame -> DataFrame@
  * @D.describeColumns :: DataFrame -> DataFrame@
  * @D.summarize :: DataFrame -> DataFrame@

__Row ops__

  * @D.filter  :: Columnable a => Text -> (a -> Bool) -> DataFrame -> DataFrame@
  * @D.sortBy  :: SortOrder -> [Text] -> DataFrame -> DataFrame@

__Column ops__

  * @D.select     :: [Text] -> DataFrame -> DataFrame@
  * @D.exclude       :: [Text] -> DataFrame -> DataFrame@
  * @D.rename     :: [(Text,Text)] -> DataFrame -> DataFrame@
  * @D.derive :: Text -> D.Expr a -> DataFrame -> DataFrame@

__Group & aggregate__

  * @D.groupBy   :: [Text] -> DataFrame -> GroupedDataFrame@
  * @D.aggregate :: [(Text, F.UExpr)] -> GroupedDataFrame -> DataFrame@

__Joins__

  * @D.innerJoin / D.leftJoin / D.rightJoin / D.fullJoin@

== Expression DSL (F.*) at a glance
Columns (typed):

@
F.col   @Text   "ocean_proximity"
F.col   @Double "total_rooms"
F.lit   @Double 1.0
@

Math & comparisons (overloaded by type):

@
(+), (-), (*), (/), abs, log, exp, round
(F.eq), (F.gt), (F.geq), (F.lt), (F.leq)
@

Aggregations (for 'D.aggregate'):

@
F.count @a (F.col @a "c")
F.sum   @Double (F.col @Double "x")
F.mean  @Double (F.col @Double "x")
F.min   @t (F.col @t "x")
F.max   @t (F.col @t "x")
@

== REPL power-tool: ':exposeColumns'

Use @:exposeColumns <df>@ in GHCi/IHaskell to turn each column of a bound 'DataFrame'
into a local binding with the same (mangled if needed) name and the column's concrete
vector type. This is great for quick ad-hoc analysis, plotting, or hand-rolled checks.

@
-- Suppose df has columns: "passengers" :: Int, "fare" :: Double, "payment" :: Text
ghci> :set -XTemplateHaskell
ghci> :exposeColumns df

-- Now you have in scope:
ghci> :type passengers
passengers :: Expr Int

ghci> :type fare
fare :: Expr Double

ghci> :type payment
payment :: Expr Text

-- You can use them directly:
ghci> D.derive "fare_with_tip" (fare * F.lit 1.2)
@

Notes:

* Name mangling: spaces and non-identifier characters are replaced (e.g. @"trip id"@ -> @trip_id@).
* Optional/nullable columns are exposed as @Expr (Maybe a)@.
-}
module DataFrame (
    -- * Core data structures
    module Dataframe,
    module Column,
    module Expression,

    -- * Core dataframe operations
    module Core,

    -- * I/O
    module CSV,
    module Parquet,

    -- * Operations
    module Subset,
    module Transformations,
    module Aggregation,
    module Sorting,
    module Merge,
    module Join,
    module Statistics,

    -- * Errors
    module Errors,

    -- * Plotting
    module Plot,

    -- * Convenience functions
    (|>),
)
where

import DataFrame.Display.Terminal.Plot as Plot
import DataFrame.Errors as Errors
import DataFrame.IO.CSV as CSV (ReadOptions (..), defaultOptions, readCsv, readSeparated, readTsv)
import DataFrame.IO.Parquet as Parquet (readParquet)
import DataFrame.Internal.Column as Column (
    Column,
    fromList,
    fromUnboxedVector,
    fromVector,
    toList,
    toVector,
 )
import DataFrame.Internal.DataFrame as Dataframe (
    DataFrame,
    GroupedDataFrame,
    columnAsVector,
    empty,
    toMatrix,
 )
import DataFrame.Internal.Expression as Expression (Expr)
import DataFrame.Operations.Aggregation as Aggregation (aggregate, distinct, groupBy)
import DataFrame.Operations.Core as Core hiding (ColumnInfo (..), nulls, partiallyParsed, renameSafe)
import DataFrame.Operations.Join as Join
import DataFrame.Operations.Merge as Merge
import DataFrame.Operations.Sorting as Sorting
import DataFrame.Operations.Statistics as Statistics (
    correlation,
    frequencies,
    interQuartileRange,
    mean,
    median,
    skewness,
    standardDeviation,
    sum,
    summarize,
    variance,
 )
import DataFrame.Operations.Subset as Subset (
    cube,
    drop,
    dropLast,
    exclude,
    filter,
    filterAllJust,
    filterBy,
    filterJust,
    filterWhere,
    range,
    select,
    selectBy,
    selectIntRange,
    selectRange,
    take,
    takeLast,
 )
import DataFrame.Operations.Transformations as Transformations

import Data.Function
import Data.List
import qualified Data.Text as T
import qualified Data.Text.IO as T

(|>) = (&)
