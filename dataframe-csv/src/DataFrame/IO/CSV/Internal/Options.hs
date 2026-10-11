{-# LANGUAGE OverloadedStrings #-}

-- | Configuration types for the CSV readers.
module DataFrame.IO.CSV.Internal.Options (
    HeaderSpec (..),
    TypeSpec (..),
    RaggedRowPolicy (..),
    UnclosedQuotePolicy (..),
    ReadOptions (..),
    defaultReadOptions,
    shouldInferFromSample,
    schemaTypeMap,
    typeInferenceSampleSize,
    resolveSelection,
    InvalidReadOptions (..),
    readOptionsErrors,
    validateReadOptions,
) where

import qualified Data.List as L
import qualified Data.Map.Strict as M
import qualified Data.Text as T

import Control.Exception (Exception, throw, throwIO)
import Control.Monad (unless)
import DataFrame.Errors (DataFrameException (ColumnsNotFoundException))
import DataFrame.Operations.Typing (SafeReadMode (..))
import DataFrame.Schema (SchemaType)

-- | Where a reader's column names come from.
data HeaderSpec
    = NoHeader
    | -- | Take names from the first row (the default).
      UseFirstRow
    | -- | Name the leading columns; any beyond the given list are numbered.
      ProvideNames [T.Text]
    deriving (Eq, Show)

-- | How a reader decides each column's type.
data TypeSpec
    = -- | Infer each column's type from the first @n@ rows.
      InferFromSample Int
    | -- | Pin the listed columns to their given types, with a fallback.
      SpecifyTypes [(T.Text, SchemaType)] TypeSpec
    | -- | Every column is read as 'Data.Text.Text', no inference at all.
      NoInference

-- | How the fast reader treats a row with the wrong field count.
data RaggedRowPolicy
    = -- | Fill missing cells with nulls and silently drop extras.
      PadWithNull
    | -- | Alias of 'PadWithNull' for callers who only care about extras.
      Truncate
    | -- | Raise a 'CsvParseError' on any row whose field count differs.
      RaiseOnRagged
    deriving (Eq, Show)

-- | How the fast reader should treat an unclosed quoted field at EOF.
data UnclosedQuotePolicy
    = -- | Raise a 'DataFrame.IO.CSV.Fast.CsvParseError'.  Default.
      RaiseOnUnclosedQuote
    | -- | Return the rows parsed before the stray quote; drop the rest.
      BestEffort
    deriving (Eq, Show)

-- | CSV read parameters.
data ReadOptions = ReadOptions
    { headerSpec :: HeaderSpec
    -- ^ Where to get the headers from. (default: UseFirstRow)
    , typeSpec :: TypeSpec
    -- ^ Whether/how to infer types. (default: InferFromSample 100)
    , safeRead :: SafeReadMode
    -- ^ Default 'SafeReadMode' for columns without an override.
    , safeReadOverrides :: [(T.Text, SafeReadMode)]
    -- ^ Per-column 'SafeReadMode' overrides; takes precedence over 'safeRead'.
    , dateFormat :: String
    -- ^ Format of date fields, as recognized by Data.Time.Format.
    , columnSeparator :: Char
    -- ^ Character that separates column values.
    , numRowsToRead :: Maybe Int
    -- ^ Maximum number of data rows to read ('Nothing' = all).
    , readColumns :: Maybe [T.Text]
    -- ^ Columns to read ('Nothing' = all); unselected ones are never decoded.
    , missingIndicators :: [T.Text]
    -- ^ Values that should be read as `Nothing`.
    , fastCsvOnRaggedRow :: RaggedRowPolicy
    -- ^ @dataframe-fastcsv@: how to treat rows with a non-header field count.
    , fastCsvOnUnclosedQuote :: UnclosedQuotePolicy
    -- ^ @dataframe-fastcsv@: how to treat an unclosed quoted field at EOF.
    , fastCsvTrimUnquoted :: Bool
    -- ^ @dataframe-fastcsv@: strip whitespace from unquoted fields if True.
    }

-- | Whether a 'TypeSpec' still infers types from a sample.
shouldInferFromSample :: TypeSpec -> Bool
shouldInferFromSample (InferFromSample _) = True
shouldInferFromSample (SpecifyTypes _ fallback) = shouldInferFromSample fallback
shouldInferFromSample _ = False

-- | The explicit column/type pairs a 'TypeSpec' declares, if any.
schemaTypeMap :: TypeSpec -> M.Map T.Text SchemaType
schemaTypeMap (SpecifyTypes xs _) = M.fromList xs
schemaTypeMap _ = M.empty

-- | A 'ReadOptions' value that contradicts itself, listing every problem.
newtype InvalidReadOptions = InvalidReadOptions [T.Text]

instance Show InvalidReadOptions where
    show (InvalidReadOptions problems) =
        T.unpack
            (T.intercalate "\n" ("Invalid ReadOptions:" : map ("  - " <>) problems))

instance Exception InvalidReadOptions

-- | Everything self-contradictory about @opts@, judged without a file.
readOptionsErrors :: ReadOptions -> [T.Text]
readOptionsErrors opts =
    concat
        [ [ "readColumns is an empty selection; omit it to read every column"
          | Just [] <- [readColumns opts]
          ]
        , [ "readColumns names "
                <> T.intercalate ", " dups
                <> " more than once"
          | not (null dups)
          ]
        , [ "numRowsToRead is negative: " <> T.pack (show n)
          | Just n <- [numRowsToRead opts]
          , n < 0
          ]
        , [ "headerSpec is ProvideNames with no names"
          | ProvideNames [] <- [headerSpec opts]
          ]
        , [ "columnSeparator is " <> T.pack (show sep) <> ", which cannot delimit fields"
          | let sep = columnSeparator opts
          , sep `elem` ['"', '\n', '\r']
          ]
        ]
  where
    dups = maybe [] (\cs -> L.nub (cs L.\\ L.nub cs)) (readColumns opts)

-- | Throw 'InvalidReadOptions' if @opts@ contradicts itself.
validateReadOptions :: ReadOptions -> IO ()
validateReadOptions opts =
    unless (null problems) (throwIO (InvalidReadOptions problems))
  where
    problems = readOptionsErrors opts

-- | Resolve 'readColumns' against header names to (name, field index) pairs.
resolveSelection :: ReadOptions -> [T.Text] -> [(T.Text, Int)]
resolveSelection opts names = case readColumns opts of
    Nothing -> indexed
    Just requested -> case filter (`M.notMember` fieldIndex) requested of
        [] -> [(n, fieldIndex M.! n) | n <- requested]
        missing -> throw (ColumnsNotFoundException missing "readCsvWithOpts" names)
  where
    indexed = zip names [0 ..]
    fieldIndex = M.fromList indexed

-- | The sample size a 'TypeSpec' infers from; 0 if it never infers.
typeInferenceSampleSize :: TypeSpec -> Int
typeInferenceSampleSize (InferFromSample n) = n
typeInferenceSampleSize (SpecifyTypes _ fallback) = typeInferenceSampleSize fallback
typeInferenceSampleSize _ = 0

-- | The default 'ReadOptions': infer from 100 rows, header row, all data.
defaultReadOptions :: ReadOptions
defaultReadOptions =
    ReadOptions
        { headerSpec = UseFirstRow
        , typeSpec = InferFromSample 100
        , safeRead = NoSafeRead
        , safeReadOverrides = []
        , dateFormat = "%Y-%m-%d"
        , columnSeparator = ','
        , numRowsToRead = Nothing
        , readColumns = Nothing
        , missingIndicators =
            ["Nothing", "NULL", "", " ", "nan", "null", "N/A", "NaN", "NAN", "NA"]
        , fastCsvOnRaggedRow = PadWithNull
        , fastCsvOnUnclosedQuote = RaiseOnUnclosedQuote
        , fastCsvTrimUnquoted = False
        }
