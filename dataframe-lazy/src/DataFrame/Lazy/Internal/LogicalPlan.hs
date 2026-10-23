{-# LANGUAGE GADTs #-}

module DataFrame.Lazy.Internal.LogicalPlan where

import qualified Data.Text as T
import DataFrame.IO.CSV (CsvBytesReader, CsvReader)
import qualified DataFrame.Internal.DataFrame as D
import qualified DataFrame.Internal.Expression as E
import DataFrame.Operations.Join (JoinType)
import DataFrame.Operations.Permutation (SortOrder)
import DataFrame.Schema (Schema)

-- | Data source for a scan node.
data DataSource
    = -- | path, separator, CSV reader (e.g. attoparsec or SIMD)
      CsvSource FilePath Char CsvReader
    | CsvSourceStreaming FilePath Char CsvReader
    | CsvSourceStreamingBytes FilePath Char CsvBytesReader
    | ParquetSource FilePath

instance Show DataSource where
    show (CsvSource path sep _) =
        "CsvSource " ++ show path ++ " " ++ show sep ++ " <reader>"
    show (CsvSourceStreaming path sep _) =
        "CsvSourceStreaming " ++ show path ++ " " ++ show sep ++ " <reader>"
    show (CsvSourceStreamingBytes path sep _) =
        "CsvSourceStreamingBytes " ++ show path ++ " " ++ show sep ++ " <reader>"
    show (ParquetSource path) = "ParquetSource " ++ show path

-- | Relational-algebra tree describing what the query computes.
data LogicalPlan
    = -- | Read columns described by the schema from a source.
      Scan DataSource Schema
    | -- | Retain only the listed columns.
      Project [T.Text] LogicalPlan
    | -- | Keep rows matching the predicate.
      Filter (E.Expr Bool) LogicalPlan
    | -- | Add or overwrite a column via an expression.
      Derive T.Text E.SomeExpr LogicalPlan
    | -- | Join two sub-plans on @(leftKey, rightKey)@ pairs.
      Join JoinType [(T.Text, T.Text)] LogicalPlan LogicalPlan
    | -- | Group then aggregate.
      Aggregate [T.Text] [(T.Text, E.SomeExpr)] LogicalPlan
    | -- | Sort by the given orders, as the eager @sortBy@ does.
      Sort [SortOrder] LogicalPlan
    | -- | Retain at most N rows.
      Limit Int LogicalPlan
    | -- | Lift an already-loaded DataFrame into the lazy plan.
      SourceDF D.DataFrame
    deriving (Show)
