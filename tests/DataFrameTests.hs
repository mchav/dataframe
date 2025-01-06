{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Main where

import qualified Data.DataFrame as D
import qualified Data.DataFrame.Internal as DI
import qualified Data.DataFrame.Operations as DO
import qualified Data.List as L
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified System.Exit as Exit

import Control.Exception
import Data.Time
import Test.HUnit

import Assertions

import qualified AddColumn
import qualified Apply
import qualified Filter
import qualified Sort

testData :: D.DataFrame
testData = D.fromList [ ("test1", DI.toColumn ([1..26] :: [Int]))
                      , ("test2", DI.toColumn ['a'..'z'])
                      ]

-- Dimensions
correctDimensions :: Test
correctDimensions = TestCase (assertEqual "should be (26, 2)" (26, 2) (D.dimensions testData))

emptyDataframeDimensions :: Test
emptyDataframeDimensions = TestCase (assertEqual "should be (0, 0)" (0, 0) (D.dimensions D.empty))

dimensionsTest :: [Test]
dimensionsTest = [ TestLabel "dimensions_correctDimensions" correctDimensions
                 , TestLabel "dimensions_emptyDataframeDimensions" emptyDataframeDimensions
                 ]

-- take
lengthEqualsTakeParam :: Test
lengthEqualsTakeParam = TestCase (assertEqual "should be (5, 2)" (5, 2) (D.dimensions $ D.take 5 testData))

lengthGreaterThanTakeParam :: Test
lengthGreaterThanTakeParam = TestCase (assertEqual "should be (26, 2)" (26, 2) (D.dimensions $ D.take 30 testData))

emptyIsZero :: Test
emptyIsZero = TestCase (assertEqual "should be (0, 0)" (0, 0) (D.dimensions $ D.take 5 D.empty))

negativeIsZero :: Test
negativeIsZero = TestCase (assertEqual "should be (0, 2)" (0, 2) (D.dimensions $ D.take (-1) testData))

takeTest :: [Test]
takeTest = [ TestLabel "lengthEqualsTakeParam" lengthEqualsTakeParam
           , TestLabel "lengthGreaterThanTakeParam" lengthGreaterThanTakeParam
           , TestLabel "emptyIsZero" emptyIsZero
           , TestLabel "negativeIsZero" negativeIsZero
           ]

-- parsing.
parseDate :: Test
parseDate = let
    expected = Just $ DI.BoxedColumn (V.fromList [fromGregorian 2020 02 14, fromGregorian 2021 02 14, fromGregorian 2022 02 14])
    actual = DO.parseDefault True $ Just $ DI.toColumn' (V.fromList ["2020-02-14" :: T.Text, "2021-02-14", "2022-02-14"])
  in TestCase (assertEqual "Correctly parses gregorian date" expected actual)

incompleteDataParseMaybe :: Test
incompleteDataParseMaybe = let
    expected = Just $ DI.BoxedColumn (V.fromList [Just $ fromGregorian 2020 02 14, Nothing, Just $ fromGregorian 2022 02 14])
    actual = DO.parseDefault True $ Just $ DI.toColumn' (V.fromList ["2020-02-14" :: T.Text, "2021-02-", "2022-02-14"])
  in TestCase (assertEqual "Parses optional for gregorian date" expected actual)

parseTests :: [Test]
parseTests = [
             TestLabel "parseDate" parseDate,
             TestLabel "incompleteDataParseMaybe" incompleteDataParseMaybe
           ]

tests :: Test
tests = TestList $ dimensionsTest
                ++ takeTest
                ++ AddColumn.tests
                ++ Apply.tests
                ++ Filter.tests
                ++ Sort.tests
                ++ parseTests

main :: IO ()
main = do
    result <- runTestTT tests
    if failures result > 0 || errors result > 0 then Exit.exitFailure else Exit.exitSuccess
