{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- | Pins for the single-pass inference lattice and Int to Double promotion.
module Operations.Inference where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BL
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified DataFrame as D
import qualified DataFrame.IO.CSV as CSV
import qualified DataFrame.Internal.Column as DI
import qualified DataFrame.Operations.Typing as D

import Data.Time (Day)
import DataFrame.Internal.DataFrame (getColumn)
import Test.HUnit (Test (TestCase, TestLabel), assertEqual, assertFailure)

assumeBytes :: [Maybe BS.ByteString] -> D.ParsingAssumption
assumeBytes = D.makeParsingAssumptionBytes "%Y-%m-%d" . V.fromList

assumeText :: [Maybe T.Text] -> D.ParsingAssumption
assumeText = D.makeParsingAssumption "%Y-%m-%d" . V.fromList

-- Candidate-mask priorities reproduce the documented fallback order.
latticePriorities :: Test
latticePriorities = TestCase $ do
    let cases =
            [ ([Just "True", Just "FALSE"], D.BoolAssumption, "bools")
            , ([Just "1", Just "-42"], D.IntAssumption, "ints")
            , ([Just "1", Just "2.5"], D.DoubleAssumption, "int+double")
            , ([Just "1e3", Just "0.5"], D.DoubleAssumption, "doubles")
            , ([Just "2024-01-02", Just "1999-12-31"], D.DateAssumption, "dates")
            , ([Just "abc", Just "1"], D.TextAssumption, "text")
            , ([Nothing, Nothing], D.NoAssumption, "all null")
            , ([], D.NoAssumption, "empty sample")
            , ([Nothing, Just "7"], D.IntAssumption, "null then int")
            ]
    mapM_
        ( \(cells, expected, what) -> do
            assertEqual ("bytes: " <> what) expected (assumeBytes cells)
            assertEqual
                ("text: " <> what)
                expected
                (assumeText (map (fmap TE.decodeUtf8) cells))
        )
        cases

-- An overflowing integer clears the Int candidate, so the column is Double.
latticeOverflowIntIsDouble :: Test
latticeOverflowIntIsDouble = TestCase $ do
    assertEqual
        "bytes overflow -> Double"
        D.DoubleAssumption
        (assumeBytes [Just "9223372036854775808"])
    assertEqual
        "text overflow -> Double"
        D.DoubleAssumption
        (assumeText [Just "9223372036854775808"])

-- Byte-level strip-tolerant classification accepts ASCII-padded numerics.
latticePaddedIntIsInt :: Test
latticePaddedIntIsInt =
    TestCase
        (assertEqual "padded int" D.IntAssumption (assumeText [Just " 42 "]))

-- An overflowing cell inside the sample yields a Double column.
parseOverflowIntsAsDoubles :: Test
parseOverflowIntsAsDoubles =
    let beforeParse = ["9223372036854775808", "1", "2"] :: [T.Text]
        afterParse = [9.223372036854776e18, 1, 2] :: [Double]
        expected = DI.fromVector (V.fromList (map Just afterParse))
        actual =
            D.parseDefault (D.defaultParseOptions{D.sampleSize = 3}) $
                DI.fromVector (V.fromList beforeParse)
     in TestCase (assertEqual "overflow -> Double column" expected actual)

-- A Double cell far past the sample promotes the built Int prefix in place.
promoteIntPrefixToDouble :: Test
promoteIntPrefixToDouble =
    let ints = map (T.pack . show) ([1 .. 150] :: [Int])
        beforeParse = ints ++ ["0.5"]
        afterParse = map fromIntegral ([1 .. 150] :: [Int]) ++ [0.5] :: [Double]
        expected = DI.UnboxedColumn Nothing (VU.fromList afterParse)
        actual =
            D.parseDefault
                (D.defaultParseOptions{D.sampleSize = 10, D.parseSafe = D.NoSafeRead})
                $ DI.fromVector (V.fromList beforeParse)
     in TestCase (assertEqual "Int prefix promoted to Double" expected actual)

-- An overflowing integer past the sample promotes with its true value.
promoteOverflowPastSample :: Test
promoteOverflowPastSample =
    let ints = map (T.pack . show) ([1 .. 30] :: [Int])
        beforeParse = ints ++ ["18446744073709551616", "1.5"]
        afterParse =
            map fromIntegral ([1 .. 30] :: [Int])
                ++ [1.8446744073709552e19, 1.5] ::
                [Double]
        expected = DI.UnboxedColumn Nothing (VU.fromList afterParse)
        actual =
            D.parseDefault
                (D.defaultParseOptions{D.sampleSize = 10, D.parseSafe = D.NoSafeRead})
                $ DI.fromVector (V.fromList beforeParse)
     in TestCase (assertEqual "overflow past sample -> true Double" expected actual)

-- Promotion preserves null positions accumulated during the Int phase.
promotionPreservesNulls :: Test
promotionPreservesNulls =
    let beforeParse =
            map (T.pack . show) ([1 .. 20] :: [Int])
                ++ [""]
                ++ map (T.pack . show) ([21 .. 40] :: [Int])
                ++ ["2.5"]
        afterParse =
            map (Just . fromIntegral) ([1 .. 20] :: [Int])
                ++ [Nothing]
                ++ map (Just . fromIntegral) ([21 .. 40] :: [Int])
                ++ [Just 2.5] ::
                [Maybe Double]
        expected = DI.fromVector (V.fromList afterParse)
        actual =
            D.parseDefault (D.defaultParseOptions{D.sampleSize = 10}) $
                DI.fromVector (V.fromList beforeParse)
     in TestCase (assertEqual "nulls survive promotion" expected actual)

-- A cell parsing as neither Int nor Double demotes the column to Text.
promotionDemotesToText :: Test
promotionDemotesToText =
    let beforeParse = map (T.pack . show) ([1 .. 30] :: [Int]) ++ ["abc"]
        expected = DI.BoxedColumn Nothing (V.fromList beforeParse)
        actual =
            D.parseDefault
                (D.defaultParseOptions{D.sampleSize = 10, D.parseSafe = D.NoSafeRead})
                $ DI.fromVector (V.fromList beforeParse)
     in TestCase (assertEqual "demotes to Text, raw values kept" expected actual)

-- Default (cassava) reader, inferred path: promotion runs over raw bytes.
readerPromotesIntColumn :: Test
readerPromotesIntColumn = TestCase $ do
    let csv =
            "x\n"
                <> T.intercalate "\n" (map (T.pack . show) ([1 .. 150] :: [Int]))
                <> "\n0.5\n"
        expected =
            DI.UnboxedColumn
                Nothing
                (VU.fromList (map fromIntegral ([1 .. 150] :: [Int]) ++ [0.5] :: [Double]))
    df <- CSV.fromCsvBytes (BL.fromStrict (TE.encodeUtf8 csv))
    case getColumn "x" df of
        Just col -> assertEqual "reader Int->Double promotion" expected col
        Nothing -> assertFailure "column x missing"

-- Default reader: canonical missing tokens null the cell via the fast path.
readerCanonicalMissingTokens :: Test
readerCanonicalMissingTokens = TestCase $ do
    let csv = "x,y\nNA,1\nnan,2\nNothing,3\n ,4\nNULL,5\n"
        expectedX = DI.fromVector (V.fromList (replicate 5 (Nothing :: Maybe T.Text)))
        expectedY = DI.UnboxedColumn Nothing (VU.fromList ([1 .. 5] :: [Int]))
    df <- CSV.fromCsvBytes (BL.fromStrict (TE.encodeUtf8 csv))
    case getColumn "x" df of
        Just col -> assertEqual "all-missing column" expectedX col
        Nothing -> assertFailure "column x missing"
    case getColumn "y" df of
        Just col -> assertEqual "int column" expectedY col
        Nothing -> assertFailure "column y missing"

-- A custom missing list replaces the canonical one, so "NA" stays text.
readerCustomMissingTokens :: Test
readerCustomMissingTokens = TestCase $ do
    let csv = "x\nfoo\nNA\n7\n"
        opts = CSV.defaultReadOptions{CSV.missingIndicators = ["foo"]}
        expected =
            DI.fromVector
                (V.fromList [Nothing, Just ("NA" :: T.Text), Just "7"])
    df <- CSV.decodeSeparated opts (BL.fromStrict (TE.encodeUtf8 csv))
    case getColumn "x" df of
        Just col -> assertEqual "custom list only" expected col
        Nothing -> assertFailure "column x missing"

-- Default reader date columns still parse via the fast date path.
readerDatesStillParse :: Test
readerDatesStillParse = TestCase $ do
    let csv = "d\n2024-01-01\n2024-01-02\n2024-01-03\n"
        expected =
            DI.fromVector
                (V.fromList (map (read @Day) ["2024-01-01", "2024-01-02", "2024-01-03"]))
    df <- CSV.fromCsvBytes (BL.fromStrict (TE.encodeUtf8 csv))
    case getColumn "d" df of
        Just col -> assertEqual "date column" expected col
        Nothing -> assertFailure "column d missing"

tests :: [Test]
tests =
    [ TestLabel "lattice_priorities" latticePriorities
    , TestLabel "lattice_overflow_int_is_double" latticeOverflowIntIsDouble
    , TestLabel "lattice_padded_int_is_int" latticePaddedIntIsInt
    , TestLabel "parse_overflow_ints_as_doubles" parseOverflowIntsAsDoubles
    , TestLabel "promote_int_prefix_to_double" promoteIntPrefixToDouble
    , TestLabel "promote_overflow_past_sample" promoteOverflowPastSample
    , TestLabel "promotion_preserves_nulls" promotionPreservesNulls
    , TestLabel "promotion_demotes_to_text" promotionDemotesToText
    , TestLabel "reader_promotes_int_column" readerPromotesIntColumn
    , TestLabel "reader_canonical_missing_tokens" readerCanonicalMissingTokens
    , TestLabel "reader_custom_missing_tokens" readerCustomMissingTokens
    , TestLabel "reader_dates_still_parse" readerDatesStillParse
    ]
