{-# LANGUAGE OverloadedStrings #-}
module Operations.GroupBy where

import qualified DataFrame as D
import qualified DataFrame as DI
import qualified DataFrame as DE
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import Assertions
import Test.HUnit

values :: [(T.Text, DI.Column)]
values = [ ("test1", DI.fromList (concatMap (replicate 10) [1 :: Int, 2, 3, 4]))
         , ("test2", DI.fromList (take 40 $ cycle [1 :: Int,2]))
         , ("test3", DI.fromList [(1 :: Int)..40])
         , ("test4", DI.fromList (reverse [(1 :: Int)..40]))
         ]

testData :: D.DataFrame
testData = D.fromNamedColumns values

groupBySingleRowWAI :: Test
groupBySingleRowWAI = TestCase (assertEqual "Groups by single column"
                (D.fromNamedColumns [("test1", DI.fromList [(1::Int)..4]),
                                        -- This just makes rows with [1, 2] for every unique test1 row
                                        ("test2", DI.GroupedUnboxedColumn (V.replicate 4 $ VU.fromList (take 10 $ cycle [1 :: Int, 2]))),
                                        ("test3", DI.GroupedUnboxedColumn (V.generate 4 (\i -> VU.fromList [(i * 10 + 1)..((i + 1) * 10)]))),
                                        ("test4", DI.GroupedUnboxedColumn (V.generate 4 (\i -> VU.fromList [(((3 - i) + 1) * 10),(((3 - i) + 1) * 10 - 1)..((3 - i) * 10 + 1)])))
                                        ])
                (D.groupBy ["test1"] testData D.|> D.sortBy D.Ascending ["test1"]))

groupByMultipleRowsWAI :: Test
groupByMultipleRowsWAI = TestCase (assertEqual "Groups by single column"
                (D.fromNamedColumns [("test1", DI.fromList$ concatMap (replicate 2) [(1::Int)..4]),
                                        ("test2", DI.fromList (take 8 $ cycle [1 :: Int, 2])),
                                        ("test3", DI.GroupedUnboxedColumn (V.fromList [
                                                        VU.fromList [1 :: Int,3..9],
                                                        VU.fromList [2,4..10],
                                                        VU.fromList [11,13..19],
                                                        VU.fromList [12,14..20],
                                                        VU.fromList [21,23..29],
                                                        VU.fromList [22,24..30],
                                                        VU.fromList [31,33..39],
                                                        VU.fromList [32,34..40]
                                                ])),
                                        ("test4", DI.GroupedUnboxedColumn (V.fromList $ reverse [
                                                        VU.fromList [1 :: Int,3..9],
                                                        VU.fromList [2,4..10],
                                                        VU.fromList [11,13..19],
                                                        VU.fromList [12,14..20],
                                                        VU.fromList [21,23..29],
                                                        VU.fromList [22,24..30],
                                                        VU.fromList [31,33..39],
                                                        VU.fromList [32,34..40]
                                                ]))
                                        ])
                (D.groupBy ["test1", "test2"] testData D.|> D.sortBy D.Ascending ["test1", "test2"]))

groupByColumnDoesNotExist :: Test
groupByColumnDoesNotExist = TestCase (assertExpectException "[Error Case]"
                                (DE.columnNotFound "[\"test0\"]" "groupBy" (D.columnNames testData))
                                (print $ D.groupBy ["test0"] testData))

tests :: [Test]
tests = [ TestLabel "groupBySingleRowWAI" groupBySingleRowWAI
        , TestLabel "groupByMultipleRowsWAI" groupByMultipleRowsWAI
        , TestLabel "groupByColumnDoesNotExist" groupByColumnDoesNotExist
        ]

