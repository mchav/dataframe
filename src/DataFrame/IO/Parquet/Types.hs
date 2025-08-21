module DataFrame.IO.Parquet.Types where

import Data.Int

data ParquetType
    = PBOOLEAN
    | PINT32
    | PINT64
    | PINT96
    | PFLOAT
    | PDOUBLE
    | PBYTE_ARRAY
    | PFIXED_LEN_BYTE_ARRAY
    | PARQUET_TYPE_UNKNOWN
    deriving (Show, Eq)

parquetTypeFromInt :: Int32 -> ParquetType
parquetTypeFromInt 0 = PBOOLEAN
parquetTypeFromInt 1 = PINT32
parquetTypeFromInt 2 = PINT64
parquetTypeFromInt 3 = PINT96
parquetTypeFromInt 4 = PFLOAT
parquetTypeFromInt 5 = PDOUBLE
parquetTypeFromInt 6 = PBYTE_ARRAY
parquetTypeFromInt 7 = PFIXED_LEN_BYTE_ARRAY
parquetTypeFromInt _ = PARQUET_TYPE_UNKNOWN
