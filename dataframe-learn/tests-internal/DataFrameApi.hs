-- | The umbrella subset these internal tests use, re-exported under one alias.
module DataFrameApi (
    DataFrame,
    fromColumns,
    fromUnnamedColumns,
    dimensions,
    nRows,
    rename,
    exclude,
    randomSplit,
    derive,
    readCsv,
) where

import DataFrame.Core (DataFrame, fromColumns)
import DataFrame.IO.CSV (readCsv)
import DataFrame.Operations.Core (dimensions, fromUnnamedColumns, nRows, rename)
import DataFrame.Operations.Subset (exclude, randomSplit)
import DataFrame.Operations.Transformations (derive)
