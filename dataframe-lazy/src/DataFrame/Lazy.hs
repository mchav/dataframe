-- | Lazy, streaming queries with the same operations as "DataFrame".
module DataFrame.Lazy (
    module DataFrame.Lazy.Internal.DataFrame,
    module DataFrame.Expression.Operators,
    SortOrder (..),
    JoinType (..),
) where

import DataFrame.Expression.Operators
import DataFrame.Lazy.Internal.DataFrame
import DataFrame.Operations.Join (JoinType (..))
import DataFrame.Operations.Permutation (SortOrder (..))
