{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE OverloadedStrings #-}
module Apply where

import qualified Data.DataFrame as D
import qualified Data.DataFrame.Internal as DI
import qualified Data.DataFrame.Errors as DE
import qualified Data.DataFrame.Operations as DO
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import Assertions
import Test.HUnit
import Type.Reflection (typeRep)

testData :: D.DataFrame
testData = D.fromList [ ("test1", DI.toColumn ([1..26] :: [Int]))
                      , ("test2", DI.toColumn (map show ['a'..'z']))
                      , ("test3", DI.toColumn ([1..26] :: [Int]))
                      , ("test4", DI.toColumn ['a'..'z'])
                      , ("test5", DI.toColumn ([1..26] :: [Int]))
                      , ("test6", DI.toColumn ['a'..'z'])
                      , ("test7", DI.toColumn ([1..26] :: [Int]))
                      , ("test8", DI.toColumn ['a'..'z'])
                      ]

applyBoxedToUnboxed :: Test
applyBoxedToUnboxed = TestCase (assertEqual "should be (26, 3)"
                                (Just $ DI.UnboxedColumn (VU.fromList (replicate 26 (1 :: Int))))
                                (DI.getColumn "test2" $ D.apply @String (const (1::Int)) "test2" testData))

applyWrongType :: Test
applyWrongType = TestCase (assertExpectException "[Error Case]"
                                (DE.typeMismatchError (typeRep @Char) (typeRep @[Char]))
                                (print $ DI.getColumn "test2" $ D.apply @Char (const (1::Int)) "test2" testData))

applyUnknownColumn :: Test
applyUnknownColumn = TestCase (assertExpectException "[Error Case]"
                                (DE.columnNotFound "test9" "apply" (D.columnNames testData))
                                (print $ D.apply @[Char] (const (1::Int)) "test9" testData))

tests :: [Test]
tests = [ TestLabel "applyBoxedToUnboxed" applyBoxedToUnboxed
        , TestLabel "applyWrongType" applyWrongType
        , TestLabel "applyUnknownColumn" applyUnknownColumn
        ]
