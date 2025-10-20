{-# LANGUAGE OverloadedStrings #-}

module Operations.ReadCsv where

import qualified DataFrame as D
import Test.HUnit

testReadCsvFunctions :: FilePath -> Test
testReadCsvFunctions csvPath = TestCase $ do
    df1 <- D.readCsv csvPath
    df2 <- D.readCsvUnstable csvPath
    df3 <- D.fastReadCsvUnstable csvPath

    assertEqual
        ("readCsvUnstable should produce same result as readCsv for " <> csvPath)
        df1
        df2
    assertEqual
        ("fastReadCsvUnstable should produce same result as readCsv for " <> csvPath)
        df1
        df3

testArbuthnot :: Test
testArbuthnot = testReadCsvFunctions "data/arbuthnot.csv"

testCity :: Test
testCity = testReadCsvFunctions "data/city.csv"

testHousing :: Test
testHousing = testReadCsvFunctions "data/housing.csv"

testPresent :: Test
testPresent = testReadCsvFunctions "data/present.csv"

testStarwars :: Test
testStarwars = testReadCsvFunctions "data/starwars.csv"

testStation :: Test
testStation = testReadCsvFunctions "data/station.csv"

-- Regression test for files without trailing newlines
-- station.csv and city.csv don't end with newlines
-- This test ensures all rows are parsed, including the last one
testNoTrailingNewlineStation :: Test
testNoTrailingNewlineStation = TestCase $ do
    df <- D.readCsvUnstable "data/station.csv"
    -- station.csv has 499 newlines (1 header + 498 data rows with newlines + 1 final data row without)
    -- = 499 data rows total (excluding header)
    -- The file ends with "455,Granger,IA,33,102" without a trailing newline
    assertEqual "station.csv should have 499 rows" (499, 5) (D.dimensions df)

testNoTrailingNewlineCity :: Test
testNoTrailingNewlineCity = TestCase $ do
    df <- D.readCsvUnstable "data/city.csv"
    -- city.csv has 83 newlines (1 header + 82 data rows with newlines + 1 final data row without)
    -- = 83 data rows total (excluding header)
    -- The file ends with "4061,Fall River,USA,Massachusetts,90555" without a trailing newline
    assertEqual "city.csv should have 83 rows" (83, 5) (D.dimensions df)

tests :: [Test]
tests =
    [ TestLabel "readCsv_arbuthnot" testArbuthnot
    , TestLabel "readCsv_city" testCity
    , TestLabel "readCsv_housing" testHousing
    , TestLabel "readCsv_present" testPresent
    , TestLabel "readCsv_starwars" testStarwars
    , TestLabel "readCsv_station" testStation
    , TestLabel "readCsv_noTrailingNewline_station" testNoTrailingNewlineStation
    , TestLabel "readCsv_noTrailingNewline_city" testNoTrailingNewlineCity
    ]
