{-# LANGUAGE OverloadedStrings #-}

module Operations.Core where

import qualified DataFrame as D
import qualified DataFrame.Internal.Column as DI

import Test.HUnit

testData :: D.DataFrame
testData =
    D.fromNamedColumns
        [ ("A", DI.fromList ([1 .. 3] :: [Int]))
        , ("B", DI.fromList ['a' .. 'c'])
        ]

createsDataFrameFromRows :: Test
createsDataFrameFromRows =
    TestCase
        ( assertEqual
            "dataframe created from rows"
            testData
            ( D.fromRows
                ["A", "B"]
                [ [D.toAny (1 :: Int), D.toAny ('a' :: Char)]
                , [D.toAny (2 :: Int), D.toAny ('b' :: Char)]
                , [D.toAny (3 :: Int), D.toAny ('c' :: Char)]
                ]
            )
        )

tests :: [Test]
tests = [TestLabel "createsDataFrameFromRows" createsDataFrameFromRows]
