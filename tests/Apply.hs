{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}
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

values :: [(T.Text, DI.Column)]
values = [ ("test1", DI.toColumn ([1..26] :: [Int]))
         , ("test2", DI.toColumn (map show ['a'..'z']))
         , ("test3", DI.toColumn ([1..26] :: [Int]))
         , ("test4", DI.toColumn ['a'..'z'])
         , ("test5", DI.toColumn ([1..26] :: [Int]))
         , ("test6", DI.toColumn ['a'..'z'])
         , ("test7", DI.toColumn ([1..26] :: [Int]))
         , ("test8", DI.toColumn ['a'..'z'])
         ]

testData :: D.DataFrame
testData = D.fromList values

applyBoxedToUnboxed :: Test
applyBoxedToUnboxed = TestCase (assertEqual "should be (26, 3)"
                                (Just $ DI.UnboxedColumn (VU.fromList (replicate 26 (1 :: Int))))
                                (DI.getColumn "test2" $ D.apply @String (const (1::Int)) "test2" testData))

applyBoxedToBoxed :: Test
applyBoxedToBoxed = TestCase (assertEqual "should be (26, 3)"
                                (Just $ DI.BoxedColumn (V.fromList (replicate 26 (1 :: Integer))))
                                (DI.getColumn "test2" $ D.apply @String (const (1::Integer)) "test2" testData))

applyWrongType :: Test
applyWrongType = TestCase (assertExpectException "[Error Case]"
                                (DE.typeMismatchError (typeRep @Char) (typeRep @[Char]))
                                (print $ DI.getColumn "test2" $ D.apply @Char (const (1::Int)) "test2" testData))

applyUnknownColumn :: Test
applyUnknownColumn = TestCase (assertExpectException "[Error Case]"
                                (DE.columnNotFound "test9" "apply" (D.columnNames testData))
                                (print $ D.apply @[Char] (const (1::Int)) "test9" testData))

applyManyOnlyGivenFields :: Test
applyManyOnlyGivenFields = TestCase (assertEqual "Applies function to many fields"
                                (D.fromList (map (, D.toColumn $ replicate 26 (1 :: Integer)) ["test4", "test6"] ++
                                            -- All other fields should have their original values.
                                            filter (\(name, col) -> name /= "test4" && name /= "test6") values))
                                (D.applyMany @Char (const (1::Integer))
                                    ["test4", "test6"] testData))

applyManyBoxedToBoxed :: Test
applyManyBoxedToBoxed = TestCase (assertEqual "Applies function to many fields"
                                (D.fromList (map (, D.toColumn $ replicate 26 (1 :: Integer)) ["test4", "test6", "test8"]))
                                (D.select ["test4", "test6", "test8"] $ D.applyMany @Char (const (1::Integer))
                                    ["test4", "test6", "test8"] testData))

applyManyBoxedToUnboxed :: Test
applyManyBoxedToUnboxed = TestCase (assertEqual "Unboxes fields when necessary"
                                (D.fromList (map (, D.toColumn $ replicate 26 (1 :: Int)) ["test4", "test6", "test8"]))
                                (D.select ["test4", "test6", "test8"] $ D.applyMany @Char (const (1::Int))
                                    ["test4", "test6", "test8"] testData))

applyManyColumnNotFound :: Test
applyManyColumnNotFound = TestCase (assertExpectException "[Error Case]"
                                (DE.columnNotFound "test0" "apply" (D.columnNames testData))
                                (print $ D.applyMany @Char (const (1::Integer))
                                    ["test0", "test6", "test8"] testData))

applyManyWrongType :: Test
applyManyWrongType = TestCase (assertExpectException "[Error Case]"
                                (DE.typeMismatchError (typeRep @Char) (typeRep @[Char]))
                                (print $ DI.getColumn "test2" $ D.applyMany @Char (const (1::Int)) ["test2"] testData))

tests :: [Test]
tests = [ TestLabel "applyBoxedToUnboxed" applyBoxedToUnboxed
        , TestLabel "applyWrongType" applyWrongType
        , TestLabel "applyUnknownColumn" applyUnknownColumn
        , TestLabel "applyBoxedToBoxed" applyBoxedToBoxed
        , TestLabel "applyManyBoxedToBoxed" applyManyBoxedToBoxed
        , TestLabel "applyManyOnlyGivenFields" applyManyOnlyGivenFields
        , TestLabel "applyManyBoxedToUnboxed" applyManyBoxedToUnboxed
        , TestLabel "applyManyColumnNotFound" applyManyColumnNotFound
        , TestLabel "applyManyWrongType" applyManyWrongType
        ]
