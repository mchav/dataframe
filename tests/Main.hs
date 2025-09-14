{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import qualified Data.Text as T
import qualified Data.Vector as V
import qualified DataFrame as D
import qualified DataFrame.Internal.Column as DI
import qualified DataFrame.Operations.Typing as D
import qualified System.Exit as Exit

import Data.Time
import Test.HUnit

import qualified Functions
import qualified Operations.Apply
import qualified Operations.Derive
import qualified Operations.Filter
import qualified Operations.GroupBy
import qualified Operations.InsertColumn
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
        expected = DI.BoxedColumn (V.fromList [fromGregorian 2020 02 14, fromGregorian 2021 02 14, fromGregorian 2022 02 14])
        actual = D.parseDefault True (DI.fromVector (V.fromList ["2020-02-14" :: T.Text, "2021-02-14", "2022-02-14"]))
     in
        TestCase (assertEqual "Correctly parses gregorian date" expected actual)

incompleteDataParseEither :: Test
incompleteDataParseEither =
    let
        expected = DI.BoxedColumn (V.fromList [Right $ fromGregorian 2020 02 14, Left ("2021-02-" :: T.Text), Right $ fromGregorian 2022 02 14])
        actual = D.parseDefault True (DI.fromVector (V.fromList ["2020-02-14" :: T.Text, "2021-02-", "2022-02-14"]))
     in
        TestCase (assertEqual "Parses Either for gregorian date" expected actual)

incompleteDataParseMaybe :: Test
incompleteDataParseMaybe =
    let
        expected = DI.BoxedColumn (V.fromList [Just $ fromGregorian 2020 02 14, Nothing, Just $ fromGregorian 2022 02 14])
        actual = D.parseDefault True (DI.fromVector (V.fromList ["2020-02-14" :: T.Text, "", "2022-02-14"]))
     in
        TestCase (assertEqual "Parses Maybe for gregorian date with null/empty" expected actual)

parseTests :: [Test]
parseTests =
    [ TestLabel "parseDate" parseDate
    , TestLabel "incompleteDataParseMaybe" incompleteDataParseMaybe
    , TestLabel "incompleteDataParseEither" incompleteDataParseEither
    ]

tests :: Test
tests =
    TestList $
        dimensionsTest
            ++ Operations.Apply.tests
            ++ Operations.Derive.tests
            ++ Operations.Filter.tests
            ++ Operations.GroupBy.tests
            ++ Operations.InsertColumn.tests
            ++ Operations.Sort.tests
            ++ Operations.Statistics.tests
            ++ Operations.Take.tests
            ++ Functions.tests
            ++ Parquet.tests
            ++ parseTests

main :: IO ()
main = do
    result <- runTestTT tests
    if failures result > 0 || errors result > 0 then Exit.exitFailure else Exit.exitSuccess
