{-# LANGUAGE OverloadedStrings #-}

module IO.JSON where

import qualified Data.ByteString.Lazy.Char8 as LBSC
import DataFrame.IO.JSON (readJSONEither)
import qualified DataFrame.Internal.Column as DI
import qualified DataFrame.Internal.DataFrame as DI
import qualified DataFrame.Operations.Core as D
import Test.HUnit (
    Test (TestCase, TestLabel),
    assertBool,
    assertEqual,
    assertFailure,
 )

-- | Happy path: array of objects with string and number columns.
jsonHappyPath :: Test
jsonHappyPath =
    TestCase
        ( case readJSONEither
            (LBSC.pack "[{\"name\":\"Alice\",\"age\":30},{\"name\":\"Bob\",\"age\":25}]") of
            Left err -> assertFailure $ "Unexpected Left: " ++ err
            Right df -> do
                assertEqual "Happy path: 2 rows" 2 (D.nRows df)
                assertEqual "Happy path: 2 columns" 2 (D.nColumns df)
        )

-- | Boolean column is preserved correctly.
jsonBoolColumn :: Test
jsonBoolColumn =
    TestCase
        ( case readJSONEither (LBSC.pack "[{\"flag\":true},{\"flag\":false}]") of
            Left err -> assertFailure $ "Unexpected Left: " ++ err
            Right df -> do
                assertEqual "Bool column: 2 rows" 2 (D.nRows df)
                assertEqual "Bool column: 1 column" 1 (D.nColumns df)
        )

-- | A key absent from some objects produces an Optional column.
jsonMissingKeyBecomesOptional :: Test
jsonMissingKeyBecomesOptional =
    TestCase
        ( case readJSONEither (LBSC.pack "[{\"a\":1,\"b\":2},{\"a\":3}]") of
            Left err -> assertFailure $ "Unexpected Left: " ++ err
            Right df -> do
                assertEqual "Missing key: 2 rows" 2 (D.nRows df)
                assertEqual "Missing key: 2 columns" 2 (D.nColumns df)
                -- 'b' is absent from the second row, so it is missing
                assertBool
                    "b column should have missing values"
                    (maybe False DI.hasMissing (DI.getColumn "b" df))
        )

-- | Column values of different types across rows make the column generic.
jsonMixedTypeColumn :: Test
jsonMixedTypeColumn =
    TestCase
        ( case readJSONEither (LBSC.pack "[{\"x\":1},{\"x\":\"hello\"}]") of
            Left err -> assertFailure $ "Unexpected Left: " ++ err
            Right df -> do
                assertEqual "Mixed type: 2 rows" 2 (D.nRows df)
                assertEqual "Mixed type: 1 column" 1 (D.nColumns df)
        )

-- | An empty top-level JSON array is rejected.
jsonEmptyArray :: Test
jsonEmptyArray =
    TestCase
        ( case readJSONEither (LBSC.pack "[]") of
            Left _ -> return ()
            Right _ -> assertFailure "Expected Left for empty array"
        )

-- | A non-array top-level JSON value is rejected.
jsonNonArray :: Test
jsonNonArray =
    TestCase
        ( case readJSONEither (LBSC.pack "{\"name\":\"Alice\"}") of
            Left _ -> return ()
            Right _ -> assertFailure "Expected Left for non-array top-level value"
        )

-- | Array elements that are not objects are rejected.
jsonNonObjectElement :: Test
jsonNonObjectElement =
    TestCase
        ( case readJSONEither (LBSC.pack "[1, 2, 3]") of
            Left _ -> return ()
            Right _ -> assertFailure "Expected Left for non-object array elements"
        )

-- | Completely invalid JSON is rejected.
jsonInvalidJSON :: Test
jsonInvalidJSON =
    TestCase
        ( case readJSONEither (LBSC.pack "not valid json at all") of
            Left _ -> return ()
            Right _ -> assertFailure "Expected Left for invalid JSON"
        )

tests :: [Test]
tests =
    [ TestLabel "jsonHappyPath" jsonHappyPath
    , TestLabel "jsonBoolColumn" jsonBoolColumn
    , TestLabel "jsonMissingKeyBecomesOptional" jsonMissingKeyBecomesOptional
    , TestLabel "jsonMixedTypeColumn" jsonMixedTypeColumn
    , TestLabel "jsonEmptyArray" jsonEmptyArray
    , TestLabel "jsonNonArray" jsonNonArray
    , TestLabel "jsonNonObjectElement" jsonNonObjectElement
    , TestLabel "jsonInvalidJSON" jsonInvalidJSON
    ]
