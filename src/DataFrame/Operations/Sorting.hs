{-# LANGUAGE OverloadedStrings #-}

module DataFrame.Operations.Sorting where

import qualified Data.List as L
import qualified Data.Text as T
import qualified Data.Vector as V

import Control.Exception (throw)
import DataFrame.Errors (DataFrameException (..))
import DataFrame.Internal.Column
import DataFrame.Internal.DataFrame (DataFrame (..))
import DataFrame.Internal.Row
import DataFrame.Operations.Core

-- | Sort order taken as a parameter by the 'sortBy' function.
data SortOrder = Ascending | Descending deriving (Eq)

{- | O(k log n) Sorts the dataframe by a given row.

> sortBy Ascending ["Age"] df
-}
sortBy ::
    SortOrder ->
    [T.Text] ->
    DataFrame ->
    DataFrame
sortBy order names df
    | any (`notElem` columnNames df) names =
        throw $
            ColumnNotFoundException
                (T.pack $ show $ names L.\\ columnNames df)
                "sortBy"
                (columnNames df)
    | otherwise =
        let
            indexes = sortedIndexes' (order == Ascending) (toRowVector names df)
         in
            df{columns = V.map (atIndicesStable indexes) (columns df)}
