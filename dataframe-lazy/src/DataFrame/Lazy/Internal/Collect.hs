{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Lazy ops that return values: each runs the query, then the eager op.
module DataFrame.Lazy.Internal.Collect (
    -- * Shape
    dimensions,
    nRows,
    nColumns,
    columnNames,
    null,

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
    valueCounts,
    valueProportions,

    -- * Column and row access
    columnAsVector,
    columnAsList,
    columnAsIntVector,
    columnAsDoubleVector,
    columnAsFloatVector,
    columnAsUnboxedVector,
    toDoubleMatrix,
    toFloatMatrix,
    toIntMatrix,
    toRowList,
    toRowVector,
    toRecords,
    safeApply,

    -- * Rendering and output
    display,
    toCsv,
    toCsv',
    toMarkdown,
    toMarkdown',
    toSeparated,
    writeCsv,
    writeTsv,
    writeSeparated,
    writeParquet,
    writeParquetWithOptions,
) where

import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import Prelude hiding (null, sum)

import DataFrame.Display (DisplayOptions)
import qualified DataFrame.Display as Display
import DataFrame.Errors (DataFrameException)
import qualified DataFrame.IO.CSV as CSV
import DataFrame.IO.Parquet (ParquetWriteOptions)
import qualified DataFrame.IO.Parquet as Parquet
import qualified DataFrame.Internal.Column as C
import qualified DataFrame.Internal.DataFrame as D
import qualified DataFrame.Internal.Expression as E
import DataFrame.Internal.Row (Any, Row)
import qualified DataFrame.Internal.Row as Row
import DataFrame.Lazy.Internal.DataFrame (LazyDataFrame, run, select)
import qualified DataFrame.Operations.Core as Core
import qualified DataFrame.Operations.Statistics as Stats
import qualified DataFrame.Operations.Transformations as Trans
import DataFrame.Typed.Record (HasSchema)
import qualified DataFrame.Typed.Record as Record

-- | Run only the columns an expression reads.
runFor :: E.Expr a -> LazyDataFrame -> IO D.DataFrame
runFor e ldf = case E.getColumns e of
    [] -> run ldf
    cols -> run (select cols ldf)

-- | Rows and columns of the result.
dimensions :: LazyDataFrame -> IO (Int, Int)
dimensions ldf = Core.dimensions <$> run ldf

-- | Number of rows in the result.
nRows :: LazyDataFrame -> IO Int
nRows ldf = Core.nRows <$> run ldf

-- | Number of columns in the result.
nColumns :: LazyDataFrame -> IO Int
nColumns ldf = Core.nColumns <$> run ldf

-- | Column names of the result.
columnNames :: LazyDataFrame -> IO [T.Text]
columnNames ldf = D.columnNames <$> run ldf

-- | True when the result has no rows or no columns.
null :: LazyDataFrame -> IO Bool
null ldf = D.null <$> run ldf

mean ::
    (C.Columnable a, Real a, VU.Unbox a) => E.Expr a -> LazyDataFrame -> IO Double
mean e ldf = Stats.mean e <$> runFor e ldf

meanMaybe ::
    (C.Columnable a, Real a) => E.Expr (Maybe a) -> LazyDataFrame -> IO Double
meanMaybe e ldf = Stats.meanMaybe e <$> runFor e ldf

median ::
    (C.Columnable a, Real a, VU.Unbox a) => E.Expr a -> LazyDataFrame -> IO Double
median e ldf = Stats.median e <$> runFor e ldf

medianMaybe ::
    (C.Columnable a, Real a) => E.Expr (Maybe a) -> LazyDataFrame -> IO Double
medianMaybe e ldf = Stats.medianMaybe e <$> runFor e ldf

percentile ::
    (C.Columnable a, Real a, VU.Unbox a) =>
    Int -> E.Expr a -> LazyDataFrame -> IO Double
percentile p e ldf = Stats.percentile p e <$> runFor e ldf

genericPercentile ::
    (C.Columnable a, Ord a) => Int -> E.Expr a -> LazyDataFrame -> IO a
genericPercentile p e ldf = Stats.genericPercentile p e <$> runFor e ldf

standardDeviation ::
    (C.Columnable a, Real a, VU.Unbox a) => E.Expr a -> LazyDataFrame -> IO Double
standardDeviation e ldf = Stats.standardDeviation e <$> runFor e ldf

skewness ::
    (C.Columnable a, Real a, VU.Unbox a) => E.Expr a -> LazyDataFrame -> IO Double
skewness e ldf = Stats.skewness e <$> runFor e ldf

variance ::
    (C.Columnable a, Real a, VU.Unbox a) => E.Expr a -> LazyDataFrame -> IO Double
variance e ldf = Stats.variance e <$> runFor e ldf

interQuartileRange ::
    (C.Columnable a, Real a, VU.Unbox a) => E.Expr a -> LazyDataFrame -> IO Double
interQuartileRange e ldf = Stats.interQuartileRange e <$> runFor e ldf

sum :: (C.Columnable a, Num a) => E.Expr a -> LazyDataFrame -> IO a
sum e ldf = Stats.sum e <$> runFor e ldf

-- | Pearson correlation of two named columns.
correlation :: T.Text -> T.Text -> LazyDataFrame -> IO (Maybe Double)
correlation a b ldf = Stats.correlation a b <$> run (select [a, b] ldf)

valueCounts ::
    (Ord a, C.Columnable a) => E.Expr a -> LazyDataFrame -> IO [(a, Int)]
valueCounts e ldf = Core.valueCounts e <$> runFor e ldf

valueProportions ::
    (Ord a, C.Columnable a) => E.Expr a -> LazyDataFrame -> IO [(a, Double)]
valueProportions e ldf = Core.valueProportions e <$> runFor e ldf

columnAsVector ::
    (C.Columnable a) =>
    E.Expr a -> LazyDataFrame -> IO (Either DataFrameException (V.Vector a))
columnAsVector e ldf = Core.columnAsVector e <$> runFor e ldf

columnAsList :: (C.Columnable a) => E.Expr a -> LazyDataFrame -> IO [a]
columnAsList e ldf = Core.columnAsList e <$> runFor e ldf

columnAsIntVector ::
    (C.Columnable a, Num a) =>
    E.Expr a -> LazyDataFrame -> IO (Either DataFrameException (VU.Vector Int))
columnAsIntVector e ldf = Core.columnAsIntVector e <$> runFor e ldf

columnAsDoubleVector ::
    (C.Columnable a, Num a) =>
    E.Expr a -> LazyDataFrame -> IO (Either DataFrameException (VU.Vector Double))
columnAsDoubleVector e ldf = Core.columnAsDoubleVector e <$> runFor e ldf

columnAsFloatVector ::
    (C.Columnable a, Num a) =>
    E.Expr a -> LazyDataFrame -> IO (Either DataFrameException (VU.Vector Float))
columnAsFloatVector e ldf = Core.columnAsFloatVector e <$> runFor e ldf

columnAsUnboxedVector ::
    (C.Columnable a, VU.Unbox a) =>
    E.Expr a -> LazyDataFrame -> IO (Either DataFrameException (VU.Vector a))
columnAsUnboxedVector e ldf = Core.columnAsUnboxedVector e <$> runFor e ldf

toDoubleMatrix ::
    LazyDataFrame -> IO (Either DataFrameException (V.Vector (VU.Vector Double)))
toDoubleMatrix ldf = Core.toDoubleMatrix <$> run ldf

toFloatMatrix ::
    LazyDataFrame -> IO (Either DataFrameException (V.Vector (VU.Vector Float)))
toFloatMatrix ldf = Core.toFloatMatrix <$> run ldf

toIntMatrix ::
    LazyDataFrame -> IO (Either DataFrameException (V.Vector (VU.Vector Int)))
toIntMatrix ldf = Core.toIntMatrix <$> run ldf

toRowList :: LazyDataFrame -> IO [[(T.Text, Any)]]
toRowList ldf = Row.toRowList <$> run ldf

toRowVector :: [T.Text] -> LazyDataFrame -> IO (V.Vector Row)
toRowVector names ldf = Row.toRowVector names <$> run ldf

-- | Parse records out of the result.
toRecords :: (HasSchema a) => LazyDataFrame -> IO (Either T.Text [a])
toRecords ldf = Record.toRecords <$> run ldf

-- | Run the query, then 'apply' without throwing.
safeApply ::
    (C.Columnable b, C.Columnable c) =>
    (b -> c) ->
    T.Text ->
    LazyDataFrame ->
    IO (Either DataFrameException D.DataFrame)
safeApply f name ldf = Trans.safeApply f name <$> run ldf

display :: DisplayOptions -> LazyDataFrame -> IO ()
display opts ldf = run ldf >>= Display.display opts

toCsv :: LazyDataFrame -> IO T.Text
toCsv ldf = D.toCsv <$> run ldf

toCsv' :: LazyDataFrame -> IO String
toCsv' ldf = D.toCsv' <$> run ldf

toMarkdown :: LazyDataFrame -> IO T.Text
toMarkdown ldf = D.toMarkdown <$> run ldf

toMarkdown' :: LazyDataFrame -> IO String
toMarkdown' ldf = D.toMarkdown' <$> run ldf

toSeparated :: Char -> LazyDataFrame -> IO T.Text
toSeparated sep ldf = D.toSeparated sep <$> run ldf

writeCsv :: FilePath -> LazyDataFrame -> IO ()
writeCsv path ldf = run ldf >>= CSV.writeCsv path

writeTsv :: FilePath -> LazyDataFrame -> IO ()
writeTsv path ldf = run ldf >>= CSV.writeTsv path

writeSeparated :: Char -> FilePath -> LazyDataFrame -> IO ()
writeSeparated sep path ldf = run ldf >>= CSV.writeSeparated sep path

writeParquet :: FilePath -> LazyDataFrame -> IO ()
writeParquet path ldf = run ldf >>= Parquet.writeParquet path

writeParquetWithOptions ::
    ParquetWriteOptions -> FilePath -> LazyDataFrame -> IO ()
writeParquetWithOptions opts path ldf =
    run ldf >>= Parquet.writeParquetWithOptions opts path
