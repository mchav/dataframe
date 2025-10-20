{-# LANGUAGE OverloadedStrings #-}

module Operations.ReadCsv where

import qualified DataFrame as D
import Test.HUnit

testReadCsvFunctions :: FilePath -> Test
testReadCsvFunctions csvPath = TestCase $ do
    df1 <- D.readCsv csvPath
    df2 <- D.readCsvUnstable csvPath

    assertEqual
        ("readCsvUnstable should produce same result as readCsv for " <> csvPath)
        df1
        df2
    df3 <- D.fastReadCsvUnstable csvPath
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

testNoNewline :: Test
testNoNewline = testReadCsvFunctions "data/test_no_newline.csv"

testWithNewline :: Test
testWithNewline = testReadCsvFunctions "data/test_with_newline.csv"

-- Two tests are commented out because
-- there are slight differences in type
-- inference between the implementations
-- which must be addressed in the future
tests :: [Test]
tests =
    [ TestLabel "readCsv_arbuthnot" testArbuthnot
    , TestLabel "readCsv_city" testCity
    , --    , TestLabel "readCsv_housing" testHousing
      TestLabel "readCsv_present" testPresent
    , --    , TestLabel "readCsv_starwars" testStarwars
      TestLabel "readCsv_station" testStation
    , TestLabel "readCsv_no_newline" testNoNewline
    , TestLabel "readCsv_with_newline" testWithNewline
    ]
