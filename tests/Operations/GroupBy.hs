{-# LANGUAGE OverloadedStrings #-}
module Operations.GroupBy where

import qualified Data.DataFrame as D
import qualified Data.DataFrame.Internal as DI
import qualified Data.DataFrame.Errors as DE
import qualified Data.DataFrame.Operations as DO
import qualified Data.DataFrame.Util as DU
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import Assertions
import Test.HUnit

values :: [(T.Text, DI.Column)]
values = [ ("test1", DI.toColumn (concatMap (replicate 10) [1 :: Int, 2, 3, 4]))
         , ("test2", DI.toColumn (take 40 $ cycle [1 :: Int,2]))
         , ("test3", DI.toColumn [(1 :: Int)..40])
         , ("test4", DI.toColumn (reverse [(1 :: Int)..40]))
         ]

testData :: D.DataFrame
testData = D.fromList values

groupBySingleRowWAI :: Test
groupBySingleRowWAI = TestCase (assertEqual "Groups by single column"
                (D.fromList [("test1", DI.toColumn [(1::Int)..4]),
                             -- This just makes rows with [1, 2] for every unique test1 row
                             ("test2", DI.GroupedUnboxedColumn (V.replicate 4 $ VU.fromList (take 10 $ cycle [1 :: Int, 2]))),
                             ("test3", DI.GroupedUnboxedColumn (V.generate 4 (\i -> VU.fromList [(i * 10 + 1)..((i + 1) * 10)]))),
                             ("test4", DI.GroupedUnboxedColumn (V.generate 4 (\i -> VU.fromList [(((3 - i) + 1) * 10),(((3 - i) + 1) * 10 - 1)..((3 - i) * 10 + 1)])))
                            ])
                (D.groupBy ["test1"] testData D.|> D.sortBy D.Ascending "test1"))

tests :: [Test]
tests = [ TestLabel "groupBySingleRowWAI" groupBySingleRowWAI
        , TestLabel "groupBySingleRowWAI" groupBySingleRowWAI
        ]

