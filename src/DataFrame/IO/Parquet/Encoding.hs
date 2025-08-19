module DataFrame.IO.Parquet.Encoding where

import Data.Int

data ParquetEncoding
  = EPLAIN
  | EPLAIN_DICTIONARY
  | ERLE
  | EBIT_PACKED
  | EDELTA_BINARY_PACKED
  | EDELTA_LENGTH_BYTE_ARRAY
  | EDELTA_BYTE_ARRAY
  | ERLE_DICTIONARY
  | EBYTE_STREAM_SPLIT
  | PARQUET_ENCODING_UNKNOWN
  deriving (Show, Eq)

parquetEncodingFromInt :: Int32 -> ParquetEncoding
parquetEncodingFromInt 0 = EPLAIN
parquetEncodingFromInt 2 = EPLAIN_DICTIONARY
parquetEncodingFromInt 3 = ERLE
parquetEncodingFromInt 4 = EBIT_PACKED
parquetEncodingFromInt 5 = EDELTA_BINARY_PACKED
parquetEncodingFromInt 6 = EDELTA_LENGTH_BYTE_ARRAY
parquetEncodingFromInt 7 = EDELTA_BYTE_ARRAY
parquetEncodingFromInt 8 = ERLE_DICTIONARY
parquetEncodingFromInt 9 = EBYTE_STREAM_SPLIT
parquetEncodingFromInt _ = PARQUET_ENCODING_UNKNOWN
