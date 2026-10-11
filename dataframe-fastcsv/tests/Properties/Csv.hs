{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- | QuickCheck properties for the SIMD CSV parser.
module Properties.Csv (tests) where

import Control.Exception (try)
import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Text.IO.Utf8 as TIO
import qualified Data.Vector as V

import DataFrame.IO.CSV (defaultReadOptions)

import Data.Type.Equality (testEquality, (:~:) (Refl))
import DataFrame.IO.CSV.Fast (CsvParseError (..))
import qualified DataFrame.IO.CSV.Fast as D
import DataFrame.Internal.Column (Column (..), materializePacked)
import DataFrame.Internal.DataFrame (
    DataFrame (..),
    columnIndices,
    columns,
    dataframeDimensions,
 )

import System.Directory (removeFile)
import Test.QuickCheck
import Test.QuickCheck.Monadic (PropertyM, assert, monadicIO, run)
import Type.Reflection (typeRep)

-- | A generator for a single CSV cell.
newtype Cell = Cell {unCell :: T.Text}
    deriving (Eq, Show)

instance Arbitrary Cell where
    arbitrary = do
        leading <- elements "abcdefghij"
        rest <- listOf (elements "abcdef,\n\"\r ")
        pure (Cell (T.pack (leading : rest)))
    shrink (Cell t)
        | T.null t || T.length t == 1 = []
        | otherwise = [Cell (T.take (T.length t - 1) t)]

-- | Like 'Cell' but without line breaks.
newtype SingleLineCell = SingleLineCell {unSingleLineCell :: T.Text}
    deriving (Eq, Show)

instance Arbitrary SingleLineCell where
    arbitrary = do
        leading <- elements "abcdefghij"
        rest <- listOf (elements "abcdef, \"")
        pure (SingleLineCell (T.pack (leading : rest)))
    shrink (SingleLineCell t)
        | T.null t || T.length t == 1 = []
        | otherwise = [SingleLineCell (T.take (T.length t - 1) t)]

-- | A generator for a non-empty list of ASCII column names, unique.
newtype ColumnNames = ColumnNames {unColumnNames :: [T.Text]}
    deriving (Eq, Show)

instance Arbitrary ColumnNames where
    arbitrary = do
        n <- chooseInt (1, 4)
        pure (ColumnNames [T.pack ('c' : show i) | i <- [0 .. n - 1]])
    shrink (ColumnNames ns)
        | length ns <= 1 = []
        | otherwise = [ColumnNames (init ns)]

-- | RFC 4180 cell encoding.
encodeCell :: Char -> T.Text -> T.Text
encodeCell sep t
    | T.any needsQuote t = T.concat ["\"", T.replace "\"" "\"\"" t, "\""]
    | otherwise = t
  where
    needsQuote c = c == sep || c == '\n' || c == '\r' || c == '"'

-- | Encode a grid of cells as an RFC 4180 CSV.
encodeCsv :: Char -> [T.Text] -> [[T.Text]] -> T.Text
encodeCsv sep header rows =
    T.unlines (encodeRow header : map encodeRow rows)
  where
    encodeRow = T.intercalate (T.singleton sep) . map (encodeCell sep)

-- | Pull the raw 'Text' values of the named column out of a DataFrame.
columnAsText :: T.Text -> DataFrame -> Maybe [T.Text]
columnAsText name df = do
    idx <- M.lookup name (columnIndices df)
    col <- columns df V.!? idx
    case materializePacked col of
        BoxedColumn _ (vec :: V.Vector a) ->
            case testEquality (typeRep @a) (typeRep @T.Text) of
                Just Refl -> Just (V.toList vec)
                Nothing -> Nothing
        UnboxedColumn{} -> Nothing
        PackedText{} -> Nothing
        MergedColumn{} -> Nothing

-- | Run a property in @IO@ against a generated CSV text.
withCsvFile :: String -> T.Text -> (FilePath -> IO a) -> IO a
withCsvFile label body action = do
    let path = "./tests/data/unstable_csv/fastcsv_prop_" <> label <> ".csv"
    TIO.writeFile path body
    r <- action path
    removeFile path
    pure r

-- Properties

-- | A UTF-8 BOM does not change how a CSV parses.
prop_bom_invariant :: ColumnNames -> [[Cell]] -> Property
prop_bom_invariant (ColumnNames header) rows = monadicIO $ do
    let cellRows = map (map unCell) rows
        csv = encodeCsv ',' header cellRows
        csvWithBom = T.pack "\xFEFF" <> csv
    plain <- run $ withCsvFile "bom_plain" csv D.fastReadCsv
    withBom <- run $ withCsvFile "bom_with" csvWithBom D.fastReadCsv
    assert (plain == withBom)
    assert (dataframeDimensions plain == dataframeDimensions withBom)

-- | A CSV parses the same with @\n@ or @\r\n@ record separators.
prop_crlf_invariant :: ColumnNames -> [[SingleLineCell]] -> Property
prop_crlf_invariant (ColumnNames header) rows = monadicIO $ do
    let cellRows = map (map unSingleLineCell) rows
        lfCsv = encodeCsv ',' header cellRows
        crlfCsv = T.replace "\n" "\r\n" lfCsv
    dfLf <- run $ withCsvFile "crlf_lf" lfCsv D.fastReadCsv
    dfCrlf <- run $ withCsvFile "crlf_crlf" crlfCsv D.fastReadCsv
    assert (dfLf == dfCrlf)

-- | A DataFrame encoded as RFC 4180 round-trips through the fast reader.
prop_roundtrip_ascii :: Property
prop_roundtrip_ascii = forAll (chooseInt (0, 12)) $ \nRows ->
    forAll (chooseInt (1, 4)) $ \nCols ->
        let header = [T.pack ('c' : show i) | i <- [0 .. nCols - 1]]
         in forAll (vectorOf nRows (vectorOf nCols arbitrary)) $
                \(rawRows :: [[Cell]]) -> monadicIO $ do
                    let rows = map (map unCell) rawRows
                        csv = encodeCsv ',' header rows
                    df <- run $ withCsvFile "roundtrip" csv D.fastReadCsv
                    assertColumns header rows df

assertColumns :: [T.Text] -> [[T.Text]] -> DataFrame -> PropertyM IO ()
assertColumns header rows df = do
    let (gotRows, gotCols) = dataframeDimensions df
    assert (gotRows == length rows)
    assert (gotCols == length header)
    let expected = L.transpose rows
    mapM_
        ( \(name, expectedCol) ->
            case columnAsText name df of
                Just actual -> assert (actual == expectedCol)
                Nothing -> assert False
        )
        (zip header expected)

-- | A file ending in an unmatched quote raises 'CsvUnclosedQuote'.
prop_unclosed_quote_throws :: Property
prop_unclosed_quote_throws = forAll (listOf1 arbitrary) $ \(cells :: [Cell]) ->
    monadicIO $ do
        let plainRow =
                T.intercalate "," (map (encodeCell ',' . unCell) cells)
            csv = "v\n" <> plainRow <> ",\"dangling\n"
        result <- run $ do
            let path = "./tests/data/unstable_csv/fastcsv_prop_unclosed.csv"
            TIO.writeFile path csv
            r <- try @CsvParseError (D.fastReadCsv path)
            removeFile path
            pure r
        case result of
            Left CsvUnclosedQuote -> assert True
            _ -> assert False

-- | Files whose length sits at a SIMD chunk boundary parse correctly.
prop_boundary_sizes :: Property
prop_boundary_sizes =
    forAll (elements [64, 127, 128, 129, 192]) $ \targetBytes ->
        let header = "v\n"
            bodyLen = targetBytes - T.length header
            -- Build a body of exactly bodyLen bytes ending in a newline.
            rows = take bodyLen $ cycle (map T.singleton "abcdefghij")
            body = T.intercalate "\n" rows <> "\n"
            csv = header <> T.take bodyLen body
         in counterexample (show csv) $ monadicIO $ do
                df <- run $ withCsvFile "boundary" csv D.fastReadCsv
                let (rs, cs) = dataframeDimensions df
                -- Property: one column, consistent metadata, any row count.
                assert (cs == 1)
                assert (rs >= 0)

-- | A chunk-parallel read equals the sequential read.
prop_parallel_equals_sequential :: ColumnNames -> [[Cell]] -> Property
prop_parallel_equals_sequential (ColumnNames header) rows =
    forAll (chooseInt (2, 7)) $ \nChunks ->
        forAll (vectorOf (length rows) numCell) $ \numCol -> monadicIO $ do
            let cellRows =
                    zipWith (\r n -> map unCell r <> [n]) rows numCol
                csv = encodeCsv ',' (header <> ["num"]) cellRows
                bytes = TE.encodeUtf8 csv
            seqDf <- run (D.readSeparatedFromBytes 0x2C defaultReadOptions bytes)
            parDf <-
                run (D.readSeparatedFromBytesChunks nChunks 0x2C defaultReadOptions bytes)
            assert (seqDf == parDf)
  where
    numCell =
        frequency
            [ (4, T.pack . show <$> chooseInt (-1000, 1000))
            , (2, T.pack . show <$> (choose (-10, 10) :: Gen Double))
            , (1, pure "")
            ]

tests :: [Property]
tests =
    [ property prop_bom_invariant
    , property prop_crlf_invariant
    , property prop_roundtrip_ascii
    , property prop_unclosed_quote_throws
    , property prop_boundary_sizes
    , property prop_parallel_equals_sequential
    ]
