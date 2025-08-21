module DataFrame.IO.Parquet.Compression where

import Data.Int

data CompressionCodec
    = UNCOMPRESSED
    | SNAPPY
    | GZIP
    | LZO
    | BROTLI
    | LZ4
    | ZSTD
    | LZ4_RAW
    | COMPRESSION_CODEC_UNKNOWN
    deriving (Show, Eq)

compressionCodecFromInt :: Int32 -> CompressionCodec
compressionCodecFromInt 0 = UNCOMPRESSED
compressionCodecFromInt 1 = SNAPPY
compressionCodecFromInt 2 = GZIP
compressionCodecFromInt 3 = LZO
compressionCodecFromInt 4 = BROTLI
compressionCodecFromInt 5 = LZ4
compressionCodecFromInt 6 = ZSTD
compressionCodecFromInt 7 = LZ4_RAW
compressionCodecFromInt _ = COMPRESSION_CODEC_UNKNOWN
