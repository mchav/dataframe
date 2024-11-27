{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE OverloadedStrings #-}
module Main where

import qualified Data.DataFrame as D
import qualified Data.Vector as V
import qualified System.Exit as Exit

import Test.HUnit
 
testData :: D.DataFrame
testData = D.addColumn "test1" (V.fromList ([1..10] :: [Int]))
         . D.addColumn "test2" (V.fromList ['a'..'z'])
         $ D.empty

dimensions_correctDimensions :: Test
dimensions_correctDimensions = TestCase (assertEqual "should be (26, 2)" (26, 2) (D.dimensions testData))

take_DfLengthEqualsTakeParam :: Test
take_DfLengthEqualsTakeParam = TestCase (assertEqual "should be (5, 2)" (5, 2) (D.dimensions $ D.take 5 testData))

take_DfLengthGreaterThanTakeParam :: Test
take_DfLengthGreaterThanTakeParam = TestCase (assertEqual "should be (26, 2)" (26, 2) (D.dimensions $ D.take 30 testData))

take_EmptyDfZeroes :: Test
take_EmptyDfZeroes = TestCase (assertEqual "should be (0, 0)" (0, 0) (D.dimensions $ D.take 5 D.empty))
 
tests :: Test
tests = TestList [ TestLabel "dimensions_correctDimensions" dimensions_correctDimensions
                 , TestLabel "take_DfLengthEqualsTakeParam" take_DfLengthEqualsTakeParam
                 , TestLabel "take_EmptyDfZeroes" take_EmptyDfZeroes
                 , TestLabel "take_DfLengthGreaterThanTakeParam" take_DfLengthGreaterThanTakeParam ]
 
main :: IO ()
main = do
    result <- runTestTT tests
    if failures result > 0 then Exit.exitFailure else Exit.exitSuccess
 