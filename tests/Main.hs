{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified DataFrame as D
import qualified DataFrame.Internal.Column as DI
import qualified DataFrame.Operations.Typing as D
import qualified System.Exit as Exit

import Data.Time
import Test.HUnit

import qualified Functions
import qualified Operations.Aggregations
import qualified Operations.Apply
import qualified Operations.Core
import qualified Operations.Derive
import qualified Operations.Filter
import qualified Operations.GroupBy
import qualified Operations.InsertColumn
import qualified Operations.Merge
import qualified Operations.ReadCsv
import qualified Operations.Sort
import qualified Operations.Statistics
import qualified Operations.Take
import qualified Parquet

testData :: D.DataFrame
testData =
    D.fromNamedColumns
        [ ("test1", DI.fromList ([1 .. 26] :: [Int]))
        , ("test2", DI.fromList ['a' .. 'z'])
        ]

-- Dimensions
correctDimensions :: Test
correctDimensions = TestCase (assertEqual "should be (26, 2)" (26, 2) (D.dimensions testData))

emptyDataframeDimensions :: Test
emptyDataframeDimensions = TestCase (assertEqual "should be (0, 0)" (0, 0) (D.dimensions D.empty))

dimensionsTest :: [Test]
dimensionsTest =
    [ TestLabel "dimensions_correctDimensions" correctDimensions
    , TestLabel "dimensions_emptyDataframeDimensions" emptyDataframeDimensions
    ]

-- parsing.
parseDate :: Test
parseDate =
    let
        expected =
            DI.BoxedColumn
                ( V.fromList
                    [fromGregorian 2020 02 14, fromGregorian 2021 02 14, fromGregorian 2022 02 14]
                )
        actual =
            D.parseDefault
                10
                True
                "%Y-%m-%d"
                (DI.fromVector (V.fromList ["2020-02-14" :: T.Text, "2021-02-14", "2022-02-14"]))
     in
        TestCase (assertEqual "Correctly parses gregorian date" expected actual)

parseInt :: Test
parseInt =
    let
        expected =
            DI.UnboxedColumn
                ( VU.fromList
                    [1 :: Int .. 100]
                )
        actual =
            D.parseDefault
                10
                True
                "%Y-%m-%d"
                (DI.fromVector (T.pack . show <$> V.fromList [1 .. 100]))
     in
        TestCase (assertEqual "Correctly parses integers" expected actual)

parseMostlyCorrectlyFormattedInts :: Test
parseMostlyCorrectlyFormattedInts =
    let
        expected =
            DI.OptionalColumn
                ( V.fromList
                    ((Just <$> [1 :: Int .. 100]) ++ [Nothing])
                )
        actual =
            D.parseDefault
                10
                True
                "%Y-%m-%d"
                (DI.fromVector (V.fromList (T.pack <$> (show <$> [1 .. 100]) ++ ["*"])))
     in
        TestCase
            ( assertEqual
                "Correctly parses a list of 100 integers followed by a * as Maybe Int"
                expected
                actual
            )

parseMaybeInt :: Test
parseMaybeInt =
    let expected =
            DI.OptionalColumn
                ( V.fromList
                    [Just (1 :: Int), Nothing, Just 3, Just 4, Just 5]
                )
        actual =
            D.parseDefault
                10
                True
                "%Y-%m-%d"
                (DI.fromVector (V.fromList ["1" :: T.Text, "N/A", "3", "4", "5"]))
     in TestCase (assertEqual "Correctly parses optional integers" expected actual)

parseOnlyDoubles :: Test
parseOnlyDoubles =
    let expected =
            DI.UnboxedColumn
                ( VU.fromList
                    [1.0 :: Double, 2.0, 3.5, 4.9328, 5.399]
                )
        actual =
            D.parseDefault
                10
                True
                "%Y-%m-%d"
                (DI.fromVector (V.fromList ["1.0" :: T.Text, "2.0", "3.5", "4.9328", "5.399"]))
     in TestCase
            (assertEqual "Correctly parses correctly formatted doubles" expected actual)

parseMixtureOfDoublesAndInts :: Test
parseMixtureOfDoublesAndInts =
    let expected =
            DI.UnboxedColumn
                ( VU.fromList
                    [1.0 :: Double, 2, 3, 4.9328, 5.399]
                )
        actual =
            D.parseDefault
                10
                True
                "%Y-%m-%d"
                (DI.fromVector (V.fromList ["1.0" :: T.Text, "2", "3", "4.9328", "5.399"]))
     in TestCase
            (assertEqual "Correctly parses correctly formatted doubles" expected actual)

parseMixtureOfDoublesAndIntsWithOneIncorrectFormatting :: Test
parseMixtureOfDoublesAndIntsWithOneIncorrectFormatting =
    let expected =
            DI.OptionalColumn
                ( V.fromList
                    ((Just <$> [1.0 :: Double .. 50.0]) ++ (Just <$> [10.0 .. 100.0]) ++ [Nothing])
                )
        actual =
            D.parseDefault
                10
                True
                "%Y-%m-%d"
                ( DI.fromVector
                    ( V.fromList $
                        T.pack <$> ((show <$> [1 .. 50]) ++ (show <$> [10.0 .. 100.0]) ++ ["0-2"])
                    )
                )
     in TestCase
            ( assertEqual
                "Correctly parses mixture of mostly correctly formatted doubles and ints"
                expected
                actual
            )

parseMaybeDouble :: Test
parseMaybeDouble =
    let
        expected =
            DI.OptionalColumn
                ( V.fromList
                    [Just 1 :: Maybe Double, Nothing, Just 3, Just 4, Just 5]
                )
        actual =
            D.parseDefault
                10
                True
                "%Y-%m-%d"
                (DI.fromVector (V.fromList ["1" :: T.Text, "N/A", "3.0", "4", "5"]))
     in
        TestCase (assertEqual "Correctly parses optional doubles" expected actual)

onlyOneIncorrectDateParsesAsMaybeDate :: Test
onlyOneIncorrectDateParsesAsMaybeDate =
    let
        expected =
            DI.OptionalColumn
                ( V.fromList
                    [ Just $ fromGregorian 2020 02 14
                    , Nothing
                    , Just $ fromGregorian 2022 02 16
                    , Just $ fromGregorian 2022 02 17
                    , Just $ fromGregorian 2022 02 18
                    , Just $ fromGregorian 2022 02 19
                    , Just $ fromGregorian 2022 02 20
                    , Just $ fromGregorian 2022 02 21
                    , Just $ fromGregorian 2022 02 22
                    , Just $ fromGregorian 2022 02 23
                    ]
                )
        actual =
            D.parseDefault
                10
                True
                "%Y-%m-%d"
                ( DI.fromVector
                    ( V.fromList
                        [ "2020-02-14" :: T.Text
                        , "2020-02-1("
                        , "2022-02-16"
                        , "2022-02-17"
                        , "2022-02-18"
                        , "2022-02-19"
                        , "2022-02-20"
                        , "2022-02-21"
                        , "2022-02-22"
                        , "2022-02-23"
                        ]
                    )
                )
     in
        TestCase
            ( assertEqual
                "Parses Maybe for gregorian date one incorrect date, when other 9 are correct"
                expected
                actual
            )

incompleteDataParseMaybe :: Test
incompleteDataParseMaybe =
    let
        expected =
            DI.OptionalColumn
                ( V.fromList
                    [ Just $ fromGregorian 2020 02 14
                    , Nothing
                    , Just $ fromGregorian 2022 02 16
                    , Just $ fromGregorian 2022 02 17
                    , Just $ fromGregorian 2022 02 18
                    , Just $ fromGregorian 2022 02 19
                    , Just $ fromGregorian 2022 02 20
                    , Just $ fromGregorian 2022 02 21
                    , Just $ fromGregorian 2022 02 22
                    , Just $ fromGregorian 2022 02 23
                    ]
                )
        actual =
            D.parseDefault
                10
                True
                "%Y-%m-%d"
                ( DI.fromVector
                    ( V.fromList
                        [ "2020-02-14" :: T.Text
                        , ""
                        , "2022-02-16"
                        , "2022-02-17"
                        , "2022-02-18"
                        , "2022-02-19"
                        , "2022-02-20"
                        , "2022-02-21"
                        , "2022-02-22"
                        , "2022-02-23"
                        ]
                    )
                )
     in
        TestCase
            ( assertEqual
                "Parses Maybe for gregorian date one missing date, when other 9 are present"
                expected
                actual
            )

parseTests :: [Test]
parseTests =
    [ TestLabel "parseDate" parseDate
    , TestLabel "incompleteDataParseMaybe" incompleteDataParseMaybe
    , TestLabel
        "onlyOneIncorrectDateParsesAsMaybeDate"
        onlyOneIncorrectDateParsesAsMaybeDate
    , TestLabel "parseInt" parseInt
    , TestLabel "parseMaybeInt" parseMaybeInt
    , TestLabel "parseOnlyDoubles" parseOnlyDoubles
    , TestLabel "parseMaybeDouble" parseMaybeDouble
    , TestLabel "parseMostlyCorrectlyFormattedInts" parseMostlyCorrectlyFormattedInts
    , TestLabel "parseOnlyDoubles" parseOnlyDoubles
    , TestLabel "parseMixtureOfDoublesAndInts" parseMixtureOfDoublesAndInts
    ]

tests :: Test
tests =
    TestList $
        dimensionsTest
            ++ Operations.Aggregations.tests
            ++ Operations.Apply.tests
            ++ Operations.Core.tests
            ++ Operations.Derive.tests
            ++ Operations.Filter.tests
            ++ Operations.GroupBy.tests
            ++ Operations.InsertColumn.tests
            ++ Operations.Merge.tests
            ++ Operations.ReadCsv.tests
            ++ Operations.Sort.tests
            ++ Operations.Statistics.tests
            ++ Operations.Take.tests
            ++ Functions.tests
            ++ Parquet.tests
            ++ parseTests

main :: IO ()
main = do
    result <- runTestTT tests
    if failures result > 0 || errors result > 0
        then Exit.exitFailure
        else Exit.exitSuccess
