{-# LANGUAGE OverloadedStrings #-}
module Parquet where

import qualified DataFrame as D

import           Assertions
import           GHC.IO (unsafePerformIO)
import           Test.HUnit

-- TODO: This currently fails
allTypesPlain :: Test
allTypesPlain = TestCase (assertEqual "allTypesPlain"
                            (unsafePerformIO (D.readCsv "./data/mtcars.csv"))
                            (unsafePerformIO (D.readParquet "./data/mtcars.parquet")))

tests :: [Test]
tests = []
