module DataFrame.IO.Parquet.ColumnStatistics where

import Data.Int
import Data.Word

data ColumnStatistics = ColumnStatistics
    { columnMin :: [Word8]
    , columnMax :: [Word8]
    , columnNullCount :: Int64
    , columnDistictCount :: Int64
    , columnMinValue :: [Word8]
    , columnMaxValue :: [Word8]
    , isColumnMaxValueExact :: Bool
    , isColumnMinValueExact :: Bool
    }
    deriving (Show, Eq)

emptyColumnStatistics :: ColumnStatistics
emptyColumnStatistics = ColumnStatistics [] [] 0 0 [] [] False False
