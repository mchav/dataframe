{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}

module DataFrame.Operations.SetOps (
    union,
    intersect,
    difference,
    symmetricDifference,
) where

import Prelude hiding (filter)

import qualified Data.Vector.Unboxed as VU

import DataFrame.Internal.DataFrame (
    DataFrame (..),
    GroupedDataFrame (..),
    columnNames,
 )
import DataFrame.Operations.Aggregation (distinct, groupBy, selectIndices)
import DataFrame.Operations.Merge ()

-- | All rows that appear in either dataframe, deduplicated.
union :: DataFrame -> DataFrame -> DataFrame
union a b = distinct (a <> b)

-- | Rows that appear in both dataframes, deduplicated.
intersect :: DataFrame -> DataFrame -> DataFrame
intersect = setOp (&&)

-- | Rows in the left dataframe but not the right, deduplicated.
difference :: DataFrame -> DataFrame -> DataFrame
difference = setOp (\inLeft inRight -> inLeft && not inRight)

-- | Rows present in exactly one of the two dataframes, deduplicated.
symmetricDifference :: DataFrame -> DataFrame -> DataFrame
symmetricDifference a b = difference a b `union` difference b a

-- | Core engine for 'intersect' and 'difference'.
setOp :: (Bool -> Bool -> Bool) -> DataFrame -> DataFrame -> DataFrame
setOp keep a b =
    selectIndices (VU.fromList chosen) combined
  where
    leftRows = fst (dataframeDimensions a)
    combined = a <> b
    Grouped _ _ vis offs _ = groupBy (columnNames combined) combined
    nGroups = VU.length offs - 1
    chosen =
        [ VU.head members
        | k <- [0 .. nGroups - 1]
        , let !s = VU.unsafeIndex offs k
              !e = VU.unsafeIndex offs (k + 1)
              members = VU.slice s (e - s) vis
              inLeft = VU.any (< leftRows) members
              inRight = VU.any (>= leftRows) members
        , keep inLeft inRight
        ]
