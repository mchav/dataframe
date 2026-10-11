{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Strict single-pass RFC 4180 CSV reading and writing for dataframes.
module DataFrame.IO.CSV (
    -- * Reading
    readCsv,
    readTsv,
    readCsvWithOpts,
    readSeparated,
    readCsvWithSchema,
    CsvReader,
    CsvBytesReader,
    schemaReadOptions,
    decodeSeparated,
    decodeSeparatedStrict,
    fromCsv,
    fromCsvBytes,

    -- * Options
    module DataFrame.IO.CSV.Internal.Options,

    -- * Writing
    writeCsv,
    writeTsv,
    writeSeparated,

    -- * Helpers
    stripQuotes,
) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Text.IO.Utf8 as TIO

import Control.Exception (SomeException, catch)
import Data.Maybe (fromMaybe)
import DataFrame.IO.CSV.Internal.Options
import DataFrame.IO.CSV.Internal.Read (decodeCsvStrict)
import DataFrame.Internal.DataFrame (DataFrame (..), toSeparated)
import DataFrame.Schema (Schema, elements)

-- | Read a CSV file from a path into a dataframe.
readCsv :: FilePath -> IO DataFrame
readCsv = readSeparated defaultReadOptions

-- | A reader the lazy scan can drive, such as 'readSeparated'.
type CsvReader = ReadOptions -> FilePath -> IO DataFrame

type CsvBytesReader = ReadOptions -> BS.ByteString -> IO DataFrame

-- | Read options derived from a 'Schema', which sets types and columns.
schemaReadOptions :: Schema -> ReadOptions
schemaReadOptions schema =
    defaultReadOptions
        { typeSpec =
            SpecifyTypes (M.toList (elements schema)) (typeSpec defaultReadOptions)
        , readColumns = Just (M.keys (elements schema))
        }

-- | Read a CSV, coercing columns to the types declared in a 'Schema'.
readCsvWithSchema :: Schema -> FilePath -> IO DataFrame
readCsvWithSchema schema =
    readSeparated
        defaultReadOptions
            { typeSpec =
                SpecifyTypes
                    (M.toList (elements schema))
                    (typeSpec defaultReadOptions)
            }

-- | Read a CSV file from a path using the given 'ReadOptions'.
readCsvWithOpts :: ReadOptions -> FilePath -> IO DataFrame
readCsvWithOpts = readSeparated

-- | Read a tab-separated file into a dataframe.
readTsv :: FilePath -> IO DataFrame
readTsv = readSeparated (defaultReadOptions{columnSeparator = '\t'})

-- | Read a text file with a chosen delimiter into a dataframe.
readSeparated :: ReadOptions -> FilePath -> IO DataFrame
readSeparated opts path = do
    validateReadOptions opts
    let stripUtf8Bom b = fromMaybe b (BS.stripPrefix "\xEF\xBB\xBF" b)
    csvData <- stripUtf8Bom <$> BS.readFile path
    decodeSeparatedStrict opts csvData

-- | Decode in-memory CSV bytes into a fully forced dataframe.
decodeSeparated :: ReadOptions -> BL.ByteString -> IO DataFrame
decodeSeparated opts csvData = decodeSeparatedStrict opts (BL.toStrict csvData)

-- | Decode a strict in-memory CSV buffer into a fully forced dataframe.
decodeSeparatedStrict :: CsvBytesReader
decodeSeparatedStrict = decodeCsvStrict

-- | Write a dataframe to a comma-separated file.
writeCsv :: FilePath -> DataFrame -> IO ()
writeCsv = writeSeparated ','

-- | Write a dataframe to a tab-separated file.
writeTsv :: FilePath -> DataFrame -> IO ()
writeTsv = writeSeparated '\t'

-- | Write a dataframe to a file using the given field separator.
writeSeparated ::
    -- | Separator
    Char ->
    -- | Path to write to
    FilePath ->
    DataFrame ->
    IO ()
writeSeparated c filepath df = TIO.writeFile filepath (toSeparated c df)

-- | Parse a CSV string into a DataFrame using default options.
fromCsv :: String -> IO (Either String DataFrame)
fromCsv s = do
    let bs = BL.fromStrict (TE.encodeUtf8 (T.pack s))
    (Right <$> decodeSeparated defaultReadOptions bs)
        `catch` (\(e :: SomeException) -> pure (Left (show e)))

-- | Parse a lazy 'ByteString' of CSV data using default options.
fromCsvBytes :: BL.ByteString -> IO DataFrame
fromCsvBytes = decodeSeparated defaultReadOptions

-- | Strip one layer of double-quote delimiters from a field, if present.
stripQuotes :: T.Text -> T.Text
stripQuotes txt =
    case T.uncons txt of
        Just ('"', rest) ->
            case T.unsnoc rest of
                Just (middle, '"') -> middle
                _ -> txt
        _ -> txt
