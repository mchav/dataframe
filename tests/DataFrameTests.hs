{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE OverloadedStrings #-}
module Main where

import qualified Data.DataFrame as D
import qualified Data.DataFrame.Internal as DI
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified System.Exit as Exit

import Test.HUnit

testData :: D.DataFrame
testData = D.addColumn "test1" (V.fromList ([1..26] :: [Int]))
         . D.addColumn "test2" (V.fromList ['a'..'z'])
         $ D.empty

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

-- addColumn
-- Adding a boxed vector to an empty dataframe creates a new column boxed containing the vector elements.
addBoxedColumn :: Test
addBoxedColumn = TestCase (assertEqual "Two columns should be equal"
                            (Just $ DI.BoxedColumn (V.fromList ["Thuba" :: T.Text, "Zodwa", "Themba"]))
                            (DI.getColumn "new" $ D.addColumn "new" (V.fromList ["Thuba" :: T.Text, "Zodwa", "Themba"]) D.empty))

-- Adding an boxed vector with an unboxable type (Int/Double) to an empty dataframe creates a new column boxed containing the vector elements.
addUnboxedColumn :: Test
addUnboxedColumn = TestCase (assertEqual "Value should be boxed"
                            (Just $ DI.UnboxedColumn (VU.fromList [1 :: Int, 2, 3]))
                            (DI.getColumn "new" $ D.addColumn "new" (V.fromList [1 :: Int, 2, 3]) D.empty))

dimensionsChangeAfterAdd :: Test
dimensionsChangeAfterAdd = TestCase (assertEqual "should be (26, 3)"
                                     (26, 3)
                                     (D.dimensions $ D.addColumn @Int "new" (V.fromList [1..26]) testData))

dimensionsNotChangedAfterDuplicate :: Test
dimensionsNotChangedAfterDuplicate = TestCase (assertEqual "should be (26, 3)"
                                     (26, 3)
                                     (D.dimensions $ D.addColumn @Int "new" (V.fromList [1..26])
                                                   $ D.addColumn @Int "new" (V.fromList [1..26]) testData))


addColumnTest :: [Test]
addColumnTest = [ 
             TestLabel "dimensionsChangeAfterAdd" dimensionsChangeAfterAdd
           , TestLabel "dimensionsNotChangedAfterDuplicate" dimensionsNotChangedAfterDuplicate
           , TestLabel "addBoxedColunmToEmpty" addBoxedColumn
           , TestLabel "addBoxedColumnAutoUnboxes" addBoxedColumn
           ]

tests :: Test
tests = TestList $  dimensionsTest
                ++ takeTest
                ++ addColumnTest

main :: IO ()
main = do
    result <- runTestTT tests
    if failures result > 0 then Exit.exitFailure else Exit.exitSuccess
