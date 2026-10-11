-- | SIMD-accelerated, chunk-parallel CSV reader.
module DataFrame.IO.CSV.Fast (
    fastReadCsv,
    readCsvFast,
    fastReadTsv,
    readTsvFast,
    fastReadCsvWithOpts,
    fastReadTsvWithOpts,
    fastReadCsvWithSchema,
    fastReadCsvProj,
    fastReadCsvFromBytes,
    fastReadCsvFromBytesWithSchema,
    readSeparated,
    readSeparatedFromBytes,
    readSeparatedFromBytesChunks,
    getDelimiterIndices,
    CsvParseError (..),
) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI
import qualified Data.Map as M
import qualified Data.Vector.Storable as VS

import Data.Char (ord)
import Data.Text (Text)
import Data.Word (Word8)
import System.IO.MMap (Mode (WriteCopy), mmapFileForeignPtr)

import DataFrame.IO.CSV (
    CsvReader,
    ReadOptions (..),
    TypeSpec (..),
    defaultReadOptions,
    validateReadOptions,
 )
import DataFrame.IO.CSV.Fast.Core (checkSeparatorAgrees, readSeparatedCore)
import DataFrame.IO.CSV.Fast.Index (
    CsvParseError (..),
    comma,
    getDelimiterIndices,
 )
import DataFrame.Internal.DataFrame (DataFrame)
import DataFrame.Schema (Schema (..))

-- | The separator a 'ReadOptions' asks for, as the scanner's byte.
separatorByte :: ReadOptions -> Word8
separatorByte = fromIntegral . ord . columnSeparator

-- | Read a CSV file with the default options.
fastReadCsv :: FilePath -> IO DataFrame
fastReadCsv = fastReadCsvWithOpts defaultReadOptions

-- | Alias for 'fastReadCsv'.
readCsvFast :: FilePath -> IO DataFrame
readCsvFast = fastReadCsv

-- | Read a TSV file with the default options.
fastReadTsv :: FilePath -> IO DataFrame
fastReadTsv = fastReadTsvWithOpts defaultReadOptions

-- | Alias for 'fastReadTsv'.
readTsvFast :: FilePath -> IO DataFrame
readTsvFast = fastReadTsv

-- | Like 'fastReadCsv' but takes a 'ReadOptions' record.
fastReadCsvWithOpts :: CsvReader
fastReadCsvWithOpts opts = readSeparated (separatorByte opts) opts

-- | TSV counterpart to 'fastReadCsvWithOpts'.
fastReadTsvWithOpts :: ReadOptions -> FilePath -> IO DataFrame
fastReadTsvWithOpts opts = fastReadCsvWithOpts opts{columnSeparator = '\t'}

-- | Read a CSV and coerce each column to the declared 'Schema' type.
fastReadCsvWithSchema :: Schema -> FilePath -> IO DataFrame
fastReadCsvWithSchema schema =
    readSeparated
        comma
        defaultReadOptions
            { typeSpec =
                SpecifyTypes (M.toList (elements schema)) (typeSpec defaultReadOptions)
            }

-- | In-memory version of 'fastReadCsv'.
fastReadCsvFromBytes :: BS.ByteString -> IO DataFrame
fastReadCsvFromBytes = readSeparatedFromBytes comma defaultReadOptions

-- | In-memory version of 'fastReadCsvWithSchema'.
fastReadCsvFromBytesWithSchema :: Schema -> BS.ByteString -> IO DataFrame
fastReadCsvFromBytesWithSchema schema =
    readSeparatedFromBytes
        comma
        defaultReadOptions
            { typeSpec =
                SpecifyTypes (M.toList (elements schema)) (typeSpec defaultReadOptions)
            }

-- | Read a CSV and return only the named columns, in the order given.
fastReadCsvProj :: [Text] -> FilePath -> IO DataFrame
fastReadCsvProj projection =
    readSeparated comma defaultReadOptions{readColumns = Just projection}

-- | Read a delimited file via a private mmap, without copying.
readSeparated ::
    Word8 ->
    ReadOptions ->
    FilePath ->
    IO DataFrame
readSeparated separator opts filePath = do
    validateReadOptions opts
    checkSeparatorAgrees separator opts
    (bufferPtr, offset, len) <-
        mmapFileForeignPtr
            filePath
            WriteCopy
            Nothing
    readSeparatedCore
        Nothing
        separator
        opts
        (VS.unsafeFromForeignPtr bufferPtr offset len)

-- | Like 'readSeparated' but reads from an already-loaded byte buffer.
readSeparatedFromBytes ::
    Word8 ->
    ReadOptions ->
    BS.ByteString ->
    IO DataFrame
readSeparatedFromBytes separator opts bs = do
    let (fp, off, len) = BSI.toForeignPtr bs
    readSeparatedCore Nothing separator opts (VS.unsafeFromForeignPtr fp off len)

-- | 'readSeparatedFromBytes' with a forced chunk count.
readSeparatedFromBytesChunks ::
    Int ->
    Word8 ->
    ReadOptions ->
    BS.ByteString ->
    IO DataFrame
readSeparatedFromBytesChunks nChunks separator opts bs = do
    let (fp, off, len) = BSI.toForeignPtr bs
    readSeparatedCore
        (Just nChunks)
        separator
        opts
        (VS.unsafeFromForeignPtr fp off len)
