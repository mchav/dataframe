{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Functions where

import DataFrame.Functions (
    col,
    generatePrograms,
    geq,
    ifThenElse,
    lit,
    max,
    mean,
    median,
    min,
    percentile,
    pow,
    relu,
    sanitize,
    stddev,
 )
import DataFrame.Internal.Expression (add, divide, mult)
import Test.HUnit
import Prelude hiding (max, min)

-- Test cases for the sanitize function
sanitizeIdentifiers :: Test
sanitizeIdentifiers =
    TestList
        [ TestCase $
            assertEqual
                "Reserved word 'Data' should become '_data_'"
                "_data_"
                (sanitize "Data")
        , TestCase $
            assertEqual
                "Spaces should become underscores"
                "my_data"
                (sanitize "My Data")
        , TestCase $
            assertEqual
                "Punctuation and parentheses should be handled"
                "distance_km_h"
                (sanitize "Distance (km/h)")
        , TestCase $
            assertEqual
                "Leading digit should be wrapped"
                "_0_age_"
                (sanitize "0 Age")
        , TestCase $
            assertEqual
                "Valid camelCase should be unchanged"
                "camelCaseStr"
                (sanitize "camelCaseStr")
        , TestCase $
            assertEqual
                "Valid camelCase with invalid characters mixed in"
                "camelcase_str"
                (sanitize "camelCase$Str")
        , TestCase $
            assertEqual
                "Valid snake_case should remain unchanged"
                "snake_case_str"
                (sanitize "snake_case_str")
        , TestCase $
            assertEqual
                "Leading digit with snake_case should be wrapped"
                "_12_snake_case_"
                (sanitize "12_snake_case")
        , TestCase $
            assertEqual
                "All symbols should become underscores"
                "_____"
                (sanitize "***")
        ]

generateProgramsCalledWithNoExistingPrograms :: Test
generateProgramsCalledWithNoExistingPrograms =
    TestCase
        ( assertEqual
            "generatePrograms called with no existing programs"
            [ col @Double "x"
            , divide
                (add (col @Double "x") (negate (mean (col @Double "x"))))
                (stddev (col @Double "x"))
            , abs (col @Double "x")
            , exp (mult (log (col @Double "x")) (lit 0.5))
            , log (add (col @Double "x") (lit 1.0))
            , exp (col @Double "x")
            , mean (col @Double "x")
            , median (col @Double "x")
            , stddev (col @Double "x")
            , sin (col @Double "x")
            , cos (col @Double "x")
            , relu (col @Double "x")
            , signum (col @Double "x")
            , pow 2 (col @Double "x")
            , pow 3 (col @Double "x")
            , pow 4 (col @Double "x")
            , pow 5 (col @Double "x")
            , pow 6 (col @Double "x")
            , max (col @Double "x") (col @Double "x")
            , mult (col @Double "x") (col @Double "x")
            ]
            (generatePrograms [col "x"] [] [])
        )

tests :: [Test]
tests =
    [ TestLabel "sanitizeIdentifiers" sanitizeIdentifiers
    , TestLabel
        "generateProgramsCalledWithNoExistingPrograms"
        generateProgramsCalledWithNoExistingPrograms
    ]
