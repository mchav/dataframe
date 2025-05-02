{-# LANGUAGE OverloadedStrings #-}
module Operations.Sort where

import qualified DataFrame as D
import qualified DataFrame as DI
import qualified DataFrame as DE
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import Assertions
import Control.Monad
import Data.Char
import System.Random
import System.Random.Shuffle (shuffle')
import Test.HUnit

values :: [(T.Text, DI.Column)]
values = let
        ns = shuffle' [(1::Int)..26] 26 $ mkStdGen 252
    in [ ("test1", DI.toColumn ns)
       , ("test2", DI.toColumn (map (chr . (+96)) ns))
       ]

testData :: D.DataFrame
testData = D.fromList values

sortByAscendingWAI :: Test
sortByAscendingWAI = TestCase (assertEqual "Sorting rows by ascending works as intended"
                    (D.fromList [("test1", DI.toColumn [(1::Int)..26]),
                                 ("test2", DI.toColumn ['a'..'z'])])
                    (D.sortBy D.Ascending ["test1"] testData))

sortByDescendingWAI :: Test
sortByDescendingWAI = TestCase (assertEqual "Sorting rows by descending works as intended"
                    (D.fromList [("test1", DI.toColumn $ reverse [(1::Int)..26]),
                                 ("test2", DI.toColumn $ reverse ['a'..'z'])])
                    (D.sortBy D.Descending ["test1"] testData))

sortByColumnDoesNotExist :: Test
sortByColumnDoesNotExist = TestCase (assertExpectException "[Error Case]"
                                (DE.columnNotFound "[\"test0\"]" "sortBy" (D.columnNames testData))
                                (print $ D.sortBy D.Ascending ["test0"] testData))

tests :: [Test]
tests = [ TestLabel "sortByAscendingWAI" sortByAscendingWAI
        , TestLabel "sortByDescendingWAI" sortByDescendingWAI
        , TestLabel "sortByColumnDoesNotExist" sortByColumnDoesNotExist
        ]

