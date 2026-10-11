{-# LANGUAGE ScopedTypeVariables #-}

-- | Vocabulary shared by the aggregation planner and kernels.
module DataFrame.Internal.Aggregation.Reduction (
    Reduction (..),
    cleanDoubleVector,
) where

import qualified Data.Vector.Unboxed as VU

import DataFrame.Internal.Column (
    Column (..),
    materializePacked,
 )
import DataFrame.Internal.Column.Conversion (toDoubleVector)

-- | A recognised fast-path reduction over a single value column.
data Reduction
    = RSum
    | RCount
    | RMin
    | RMax
    | RMean
    | RStd
    | RVar
    deriving (Eq, Show)

cleanDoubleVector :: Column -> Maybe (VU.Vector Double)
cleanDoubleVector col = case col of
    UnboxedColumn Nothing _ -> either (const Nothing) Just (toDoubleVector col)
    BoxedColumn Nothing _ -> either (const Nothing) Just (toDoubleVector col)
    p@(PackedText _ _) -> cleanDoubleVector (materializePacked p)
    _ -> Nothing
