{-# LANGUAGE OverloadedStrings #-}

module Parquet where

import qualified DataFrame as D

import Assertions
import Data.Int
import Data.Text (Text)
import Data.Time
import Data.Time.Calendar
import GHC.IO (unsafePerformIO)
import Test.HUnit

allTypes :: D.DataFrame
allTypes =
    D.fromNamedColumns
        [ ("id", D.fromList [(4 :: Int32), 5, 6, 7, 2, 3, 0, 1])
        , ("bool_col", D.fromList [True, False, True, False, True, False, True, False])
        , ("tinyint_col", D.fromList [(0 :: Int32), 1, 0, 1, 0, 1, 0, 1])
        , ("smallint_col", D.fromList [(0 :: Int32), 1, 0, 1, 0, 1, 0, 1])
        , ("int_col", D.fromList [(0 :: Int32), 1, 0, 1, 0, 1, 0, 1])
        , ("bigint_col", D.fromList [(0 :: Int32), 10, 0, 10, 0, 10, 0, 10])
        , ("float_col", D.fromList [(0 :: Float), 1.1, 0, 1.1, 0, 1.1, 0, 1.1])
        , ("double_col", D.fromList [(0 :: Float), 10.1, 0, 10.1, 0, 10.1, 0, 10.1])
        , ("date_string_col", D.fromList [("30 33 2F 30 31 2F 30 39" :: Text), "30 33 2F 30 31 2F 30 39", "30 34 2F 30 31 2F 30 39", "30 34 2F 30 31 2F 30 39", "30 32 2F 30 31 2F 30 39", "30 32 2F 30 31 2F 30 39", "30 31 2F 30 31 2F 30 39", "30 31 2F 30 31 2F 30 39"])
        , ("string_col", D.fromList (take 8 (cycle [("30" :: Text), "31"])))
        ,
            ( "timestamp_col"
            , D.fromList
                [ UTCTime (fromGregorian 2009 3 1) (secondsToDiffTime 0)
                , UTCTime (fromGregorian 2009 3 1) (secondsToDiffTime 60)
                , UTCTime (fromGregorian 2009 4 1) (secondsToDiffTime 0)
                , UTCTime (fromGregorian 2009 4 1) (secondsToDiffTime 60)
                , UTCTime (fromGregorian 2009 2 1) (secondsToDiffTime 0)
                , UTCTime (fromGregorian 2009 2 1) (secondsToDiffTime 60)
                , UTCTime (fromGregorian 2009 1 1) (secondsToDiffTime 0)
                , UTCTime (fromGregorian 2009 1 1) (secondsToDiffTime 60)
                ]
            )
        ]

-- TODO: This currently fails
allTypesPlain :: Test
allTypesPlain =
    TestCase
        ( assertEqual
            "allTypesPlain"
            allTypes
            (unsafePerformIO (D.readParquet "./tests/parquet-testing/data/alltypes_plain.parquet"))
        )

tests :: [Test]
tests = []
