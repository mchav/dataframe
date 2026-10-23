module DataFrame.Lazy.Internal.PhysicalPlan where

import qualified Data.Text as T
import qualified DataFrame.Internal.DataFrame as D
import qualified DataFrame.Internal.Expression as E
import DataFrame.Lazy.Internal.LogicalPlan (BatchFn, BatchFn2, DataSource)
import DataFrame.Operations.Join (JoinType)
import DataFrame.Operations.Permutation (SortOrder)
import DataFrame.Schema (Schema)

-- | Scan-level configuration: batch size, separator, optional pushdowns.
data ScanConfig = ScanConfig
    { scanBatchSize :: !Int
    , scanSeparator :: !Char
    , scanSchema :: !Schema
    , scanPushdownPredicate :: !(Maybe (E.Expr Bool))
    }
    deriving (Show)

-- | Physical plan: nodes carry what the executor needs to pick algorithms.
data PhysicalPlan
    = PhysicalScan DataSource ScanConfig
    | PhysicalProject [T.Text] PhysicalPlan
    | PhysicalFilter (E.Expr Bool) PhysicalPlan
    | PhysicalDerive T.Text E.SomeExpr PhysicalPlan
    | PhysicalHashJoin JoinType [(T.Text, T.Text)] PhysicalPlan PhysicalPlan
    | PhysicalSortMergeJoin JoinType [(T.Text, T.Text)] PhysicalPlan PhysicalPlan
    | PhysicalHashAggregate [T.Text] [(T.Text, E.SomeExpr)] PhysicalPlan
    | PhysicalSort [SortOrder] PhysicalPlan
    | PhysicalLimit Int PhysicalPlan
    | PhysicalRename [(T.Text, T.Text)] PhysicalPlan
    | PhysicalOffset Int PhysicalPlan
    | PhysicalMapBatch T.Text BatchFn PhysicalPlan
    | PhysicalBarrier T.Text BatchFn PhysicalPlan
    | PhysicalBarrier2 T.Text BatchFn2 PhysicalPlan PhysicalPlan
    | -- | Materialize child to a binary file on disk (used for build sides).
      PhysicalSpill PhysicalPlan FilePath
    | -- | Emit an already-loaded DataFrame as a stream of batches of size @n@.
      PhysicalSourceDF Int D.DataFrame
    deriving (Show)
