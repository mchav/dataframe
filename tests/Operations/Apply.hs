{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}
module Operations.Apply where

import qualified Data.DataFrame as D
import qualified Data.DataFrame as DI
import qualified Data.DataFrame as DE
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
applyBoxedToUnboxed = TestCase (assertEqual "Boxed apply unboxed when result is unboxed"
                                (Just $ DI.UnboxedColumn (VU.fromList (replicate 26 (1 :: Int))))
                                (DI.getColumn "test2" $ D.apply @String (const (1::Int)) "test2" testData))

applyBoxedToBoxed :: Test
applyBoxedToBoxed = TestCase (assertEqual "Boxed apply remains in boxed vector"
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

applyWhereWrongConditionType :: Test
applyWhereWrongConditionType = TestCase (assertExpectException "[Error Case]"
                                (DE.typeMismatchError (typeRep @Integer) (typeRep @Int))
                                (print $ D.applyWhere (even @Integer) "test1" ((+1) :: Int -> Int) "test5" testData))

applyWhereWrongTargetType :: Test
applyWhereWrongTargetType = TestCase (assertExpectException "[Error Case]"
                                (DE.typeMismatchError (typeRep @Float) (typeRep @Int))
                                (print $ D.applyWhere (even @Int) "test1" ((+1) :: Float -> Float) "test5" testData))

applyWhereConditionColumnNotFound :: Test
applyWhereConditionColumnNotFound = TestCase (assertExpectException "[Error Case]"
                                (DE.columnNotFound "test0" "applyWhere" (D.columnNames testData))
                                (print $ D.applyWhere (even @Int) "test0" ((+1) :: Int -> Int) "test5" testData))

applyWhereTargetColumnNotFound :: Test
applyWhereTargetColumnNotFound = TestCase (assertExpectException "[Error Case]"
                                (DE.columnNotFound "test0" "applyAtIndex" (D.columnNames testData))
                                (print $ D.applyWhere (even @Int) "test1" ((+1) :: Int -> Int) "test0" testData))

applyWhereWAI :: Test
applyWhereWAI = TestCase (assertEqual "applyWhere works as intended"
                                (Just $ DI.UnboxedColumn (VU.fromList (zipWith ($) (cycle [id, (+1)]) [(1 :: Int)..26])))
                                (D.getColumn "test5" $ D.applyWhere (even @Int) "test1" ((+1) :: Int -> Int) "test5" testData))

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
        , TestLabel "applyWhereWrongConditionType" applyWhereWrongConditionType
        , TestLabel "applyWhereWrongTargetType" applyWhereWrongTargetType
        , TestLabel "applyWhereConditionColumnNotFound" applyWhereConditionColumnNotFound
        , TestLabel "applyWhereTargetColumnNotFound" applyWhereTargetColumnNotFound
        , TestLabel "applyWhereWAI" applyWhereWAI
        ]
