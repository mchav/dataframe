{-# LANGUAGE OverloadedStrings #-}
module Take where

import qualified Data.DataFrame as D
import qualified Data.DataFrame.Internal as DI

import Test.HUnit

testData :: D.DataFrame
testData = D.fromList [ ("test1", DI.toColumn ([1..26] :: [Int]))
                      , ("test2", DI.toColumn ['a'..'z'])
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

tests :: [Test]
tests = [ TestLabel "lengthEqualsTakeParam" lengthEqualsTakeParam
        , TestLabel "lengthGreaterThanTakeParam" lengthGreaterThanTakeParam
        , TestLabel "emptyIsZero" emptyIsZero
        , TestLabel "negativeIsZero" negativeIsZero
        ]
