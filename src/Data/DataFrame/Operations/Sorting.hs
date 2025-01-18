{-# LANGUAGE OverloadedStrings #-}
module Data.DataFrame.Operations.Sorting where

import qualified Data.List as L
import qualified Data.Text as T
import qualified Data.Vector as V

import Control.Exception (throw)
import Data.DataFrame.Errors (DataFrameException(..))
import Data.DataFrame.Internal.Column
import Data.DataFrame.Internal.DataFrame (DataFrame(..), getColumn)
import Data.DataFrame.Internal.Row
import Data.DataFrame.Operations.Core

-- | Sort order taken as a parameter by the sortby function.
data SortOrder = Ascending | Descending deriving (Eq)

-- | O(k log n) Sorts the dataframe by a given row.
--
-- > sortBy "Age" df
sortBy ::
  SortOrder ->
  [T.Text] ->
  DataFrame ->
  DataFrame
sortBy order names df
  | not $ any (`elem` columnNames df) names = throw $ ColumnNotFoundException (T.pack $ show $ names L.\\ columnNames df) "sortBy" (columnNames df)
  | otherwise = let
      -- TODO: Remove the SortOrder defintion from operations so we can share it between here and internal and
      -- we don't have to do this Bool mapping.
      indexes = sortedIndexes' (order == Ascending) (toRowVector names df)
      pick idxs col = atIndicesStable idxs <$> col
    in df {columns = V.map (pick indexes) (columns df)}
