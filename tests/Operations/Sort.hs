{-# LANGUAGE OverloadedStrings #-}

module Operations.Sort where

import Assertions
import Data.Char
import qualified Data.Text as T
import qualified DataFrame as D
import qualified DataFrame.Internal.Column as DI
import System.Random
import System.Random.Shuffle (shuffle')
import Test.HUnit

values :: [(T.Text, DI.Column)]
values =
    let
        ns = shuffle' [(1 :: Int) .. 26] 26 $ mkStdGen 252
     in
        [ ("test1", DI.fromList ns)
        , ("test2", DI.fromList (map (chr . (+ 96)) ns))
        ]

testData :: D.DataFrame
testData = D.fromNamedColumns values

sortByAscendingWAI :: Test
sortByAscendingWAI =
    TestCase
        ( assertEqual
            "Sorting rows by ascending works as intended"
            ( D.fromNamedColumns
                [ ("test1", DI.fromList [(1 :: Int) .. 26])
                , ("test2", DI.fromList ['a' .. 'z'])
                ]
            )
            (D.sortBy D.Ascending ["test1"] testData)
        )

sortByDescendingWAI :: Test
sortByDescendingWAI =
    TestCase
        ( assertEqual
            "Sorting rows by descending works as intended"
            ( D.fromNamedColumns
                [ ("test1", DI.fromList $ reverse [(1 :: Int) .. 26])
                , ("test2", DI.fromList $ reverse ['a' .. 'z'])
                ]
            )
            (D.sortBy D.Descending ["test1"] testData)
        )

sortByColumnDoesNotExist :: Test
sortByColumnDoesNotExist =
    TestCase
        ( assertExpectException
            "[Error Case]"
            (D.columnNotFound "[\"test0\"]" "sortBy" (D.columnNames testData))
            (print $ D.sortBy D.Ascending ["test0"] testData)
        )

tests :: [Test]
tests =
    [ TestLabel "sortByAscendingWAI" sortByAscendingWAI
    , TestLabel "sortByDescendingWAI" sortByDescendingWAI
    , TestLabel "sortByColumnDoesNotExist" sortByColumnDoesNotExist
    ]
