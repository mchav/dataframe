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
            , mean (col @Double "x")
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
            (generatePrograms [col "x"] [])
        )

generateProgramsCalledWithSingleExistingPrograms :: Test
generateProgramsCalledWithSingleExistingPrograms =
    TestCase
        ( assertEqual
            "generatePrograms called with single existing programs"
            [ col @Double "x"
            , col @Double "y"
            , divide
                    (add (col @Double "x") (negate (mean (col @Double "x"))))
                    (stddev (col @Double "x"))
            , exp (mult (log (col @Double "x")) (lit 0.5))
            , abs (col @Double "x")
            , log (add (col @Double "x") (lit 1.0))
            , exp (col @Double "x")
            , mean (col @Double "x")
            , mean (col @Double "x")
            , stddev (col @Double "x")
            , sin (col @Double "x")
            , cos (col @Double "x")
            , relu (col @Double "x")
            , signum (col @Double "x")
            , divide
                    (add (col @Double "y") (negate (mean (col @Double "y"))))
                    (stddev (col @Double "y"))
            , exp (mult (log (col @Double "y")) (lit 0.5))
            , abs (col @Double "y")
            , log (add (col @Double "y") (lit 1.0))
            , exp (col @Double "y")
            , mean (col @Double "y")
            , mean (col @Double "y")
            , stddev (col @Double "y")
            , sin (col @Double "y")
            , cos (col @Double "y")
            , relu (col @Double "y")
            , signum (col @Double "y")
            , pow 2 (col @Double "x")
            , pow 3 (col @Double "x")
            , pow 4 (col @Double "x")
            , pow 5 (col @Double "x")
            , pow 6 (col @Double "x")
            , pow 2 (col @Double "y")
            , pow 3 (col @Double "y")
            , pow 4 (col @Double "y")
            , pow 5 (col @Double "y")
            , pow 6 (col @Double "y")
            , add (col @Double "x") (col @Double "x")
            , add (col @Double "y") (col @Double "x")
            , add (col @Double "y") (col @Double "y")
            , min (col @Double "y") (col @Double "x")
            , max (col @Double "y") (col @Double "x")
            , ifThenElse
                    (geq (col @Double "x") (percentile 1 (col @Double "x")))
                    (col @Double "x")
                    (col @Double "y")
            , ifThenElse
                    (geq (col @Double "x") (percentile 25 (col @Double "x")))
                    (col @Double "x")
                    (col @Double "y")
            , ifThenElse
                    (geq (col @Double "x") (percentile 50 (col @Double "x")))
                    (col @Double "x")
                    (col @Double "y")
            , ifThenElse
                    (geq (col @Double "x") (percentile 75 (col @Double "x")))
                    (col @Double "x")
                    (col @Double "y")
            , ifThenElse
                    (geq (col @Double "x") (percentile 99 (col @Double "x")))
                    (col @Double "x")
                    (col @Double "y")
            , ifThenElse
                    (geq (col @Double "y") (percentile 1 (col @Double "y")))
                    (col @Double "y")
                    (col @Double "x")
            , ifThenElse
                    (geq (col @Double "y") (percentile 25 (col @Double "y")))
                    (col @Double "y")
                    (col @Double "x")
            , ifThenElse
                    (geq (col @Double "y") (percentile 50 (col @Double "y")))
                    (col @Double "y")
                    (col @Double "x")
            , ifThenElse
                    (geq (col @Double "y") (percentile 75 (col @Double "y")))
                    (col @Double "y")
                    (col @Double "x")
            , ifThenElse
                    (geq (col @Double "y") (percentile 99 (col @Double "y")))
                    (col @Double "y")
                    (col @Double "x")
            , ifThenElse
                    (geq (col @Double "x") (percentile 1 (col @Double "x")))
                    (col @Double "x")
                    (lit 1.0)
            , ifThenElse
                    (geq (col @Double "x") (percentile 25 (col @Double "x")))
                    (col @Double "x")
                    (lit 1.0)
            , ifThenElse
                    (geq (col @Double "x") (percentile 50 (col @Double "x")))
                    (col @Double "x")
                    (lit 1.0)
            , ifThenElse
                    (geq (col @Double "x") (percentile 75 (col @Double "x")))
                    (col @Double "x")
                    (lit 1.0)
            , ifThenElse
                    (geq (col @Double "x") (percentile 99 (col @Double "x")))
                    (col @Double "x")
                    (lit 1.0)
            , ifThenElse
                    (geq (col @Double "x") (percentile 1 (col @Double "x")))
                    (col @Double "x")
                    (lit 0.0)
            , ifThenElse
                    (geq (col @Double "x") (percentile 25 (col @Double "x")))
                    (col @Double "x")
                    (lit 0.0)
            , ifThenElse
                    (geq (col @Double "x") (percentile 50 (col @Double "x")))
                    (col @Double "x")
                    (lit 0.0)
            , ifThenElse
                    (geq (col @Double "x") (percentile 75 (col @Double "x")))
                    (col @Double "x")
                    (lit 0.0)
            , ifThenElse
                    (geq (col @Double "x") (percentile 99 (col @Double "x")))
                    (col @Double "x")
                    (lit 0.0)
            , ifThenElse
                    (geq (col @Double "x") (percentile 1 (col @Double "x")))
                    (col @Double "x")
                    (lit (-1.0))
            , ifThenElse
                    (geq (col @Double "x") (percentile 25 (col @Double "x")))
                    (col @Double "x")
                    (lit (-1.0))
            , ifThenElse
                    (geq (col @Double "x") (percentile 50 (col @Double "x")))
                    (col @Double "x")
                    (lit (-1.0))
            , ifThenElse
                    (geq (col @Double "x") (percentile 75 (col @Double "x")))
                    (col @Double "x")
                    (lit (-1.0))
            , ifThenElse
                    (geq (col @Double "x") (percentile 99 (col @Double "x")))
                    (col @Double "x")
                    (lit (-1.0))
            , ifThenElse
                    (geq (col @Double "y") (percentile 1 (col @Double "y")))
                    (col @Double "y")
                    (lit 1.0)
            , ifThenElse
                    (geq (col @Double "y") (percentile 25 (col @Double "y")))
                    (col @Double "y")
                    (lit 1.0)
            , ifThenElse
                    (geq (col @Double "y") (percentile 50 (col @Double "y")))
                    (col @Double "y")
                    (lit 1.0)
            , ifThenElse
                    (geq (col @Double "y") (percentile 75 (col @Double "y")))
                    (col @Double "y")
                    (lit 1.0)
            , ifThenElse
                    (geq (col @Double "y") (percentile 99 (col @Double "y")))
                    (col @Double "y")
                    (lit 1.0)
            , ifThenElse
                    (geq (col @Double "y") (percentile 1 (col @Double "y")))
                    (col @Double "y")
                    (lit 0.0)
            , ifThenElse
                    (geq (col @Double "y") (percentile 25 (col @Double "y")))
                    (col @Double "y")
                    (lit 0.0)
            , ifThenElse
                    (geq (col @Double "y") (percentile 50 (col @Double "y")))
                    (col @Double "y")
                    (lit 0.0)
            , ifThenElse
                    (geq (col @Double "y") (percentile 75 (col @Double "y")))
                    (col @Double "y")
                    (lit 0.0)
            , ifThenElse
                    (geq (col @Double "y") (percentile 99 (col @Double "y")))
                    (col @Double "y")
                    (lit 0.0)
            , ifThenElse
                    (geq (col @Double "y") (percentile 1 (col @Double "y")))
                    (col @Double "y")
                    (lit (-1.0))
            , ifThenElse
                    (geq (col @Double "y") (percentile 25 (col @Double "y")))
                    (col @Double "y")
                    (lit (-1.0))
            , ifThenElse
                    (geq (col @Double "y") (percentile 50 (col @Double "y")))
                    (col @Double "y")
                    (lit (-1.0))
            , ifThenElse
                    (geq (col @Double "y") (percentile 75 (col @Double "y")))
                    (col @Double "y")
                    (lit (-1.0))
            , ifThenElse
                    (geq (col @Double "y") (percentile 99 (col @Double "y")))
                    (col @Double "y")
                    (lit (-1.0))
            , add (col @Double "x") (negate (col @Double "y"))
            , add (col @Double "y") (negate (col @Double "x"))
            , mult (col @Double "x") (col @Double "x")
            , mult (col @Double "y") (col @Double "x")
            , mult (col @Double "y") (col @Double "y")
            , divide (col @Double "x") (col @Double "y")
            , divide (col @Double "y") (col @Double "x")
            ]
            (generatePrograms [col "x"] [col "y"])
        )

generateProgramsCalledWithCompoundExistingPrograms :: Test
generateProgramsCalledWithCompoundExistingPrograms =
    TestCase
        ( assertEqual
            "generatePrograms called with compound existing programs"
            [ col @Double "x"
            , col @Double "y"
            , add (lit 1.0) (col @Double "z")
            , divide
                    (add (col @Double "x") (negate (mean (col @Double "x"))))
                    (stddev (col @Double "x"))
            , exp (mult (log (col @Double "x")) (lit 0.5))
            , abs (col @Double "x")
            , log (add (col @Double "x") (lit 1.0))
            , exp (col @Double "x")
            , mean (col @Double "x")
            , mean (col @Double "x")
            , stddev (col @Double "x")
            , sin (col @Double "x")
            , cos (col @Double "x")
            , relu (col @Double "x")
            , signum (col @Double "x")
            , divide
                    (add (col @Double "y") (negate (mean (col @Double "y"))))
                    (stddev (col @Double "y"))
            , exp (mult (log (col @Double "y")) (lit 0.5))
            , abs (col @Double "y")
            , log (add (col @Double "y") (lit 1.0))
            , exp (col @Double "y")
            , mean (col @Double "y")
            , mean (col @Double "y")
            , stddev (col @Double "y")
            , sin (col @Double "y")
            , cos (col @Double "y")
            , relu (col @Double "y")
            , signum (col @Double "y")
            , divide
                    ( add
                        (add (lit 1.0) (col @Double "z"))
                        (negate (mean (add (lit 1.0) (col @Double "z"))))
                    )
                    (stddev (add (lit 1.0) (col @Double "z")))
            , exp (mult (log (add (lit 1.0) (col @Double "z"))) (lit 0.5))
            , abs (add (lit 1.0) (col @Double "z"))
            , log (add (add (lit 1.0) (col @Double "z")) (lit 1.0))
            , exp (add (lit 1.0) (col @Double "z"))
            , mean (add (lit 1.0) (col @Double "z"))
            , mean (add (lit 1.0) (col @Double "z"))
            , stddev (add (lit 1.0) (col @Double "z"))
            , sin (add (lit 1.0) (col @Double "z"))
            , cos (add (lit 1.0) (col @Double "z"))
            , relu (add (lit 1.0) (col @Double "z"))
            , signum (add (lit 1.0) (col @Double "z"))
            , pow 2 (col @Double "x")
            , pow 3 (col @Double "x")
            , pow 4 (col @Double "x")
            , pow 5 (col @Double "x")
            , pow 6 (col @Double "x")
            , pow 2 (col @Double "y")
            , pow 3 (col @Double "y")
            , pow 4 (col @Double "y")
            , pow 5 (col @Double "y")
            , pow 6 (col @Double "y")
            , pow 2 (add (lit 1.0) (col @Double "z"))
            , pow 3 (add (lit 1.0) (col @Double "z"))
            , pow 4 (add (lit 1.0) (col @Double "z"))
            , pow 5 (add (lit 1.0) (col @Double "z"))
            , pow 6 (add (lit 1.0) (col @Double "z"))
            , add (col @Double "x") (col @Double "x")
            , add (col @Double "y") (col @Double "x")
            , add (col @Double "y") (col @Double "y")
            , add (add (lit 1.0) (col @Double "z")) (col @Double "x")
            , add (add (lit 1.0) (col @Double "z")) (col @Double "y")
            , add (add (lit 1.0) (col @Double "z")) (add (lit 1.0) (col @Double "z"))
            , min (col @Double "y") (col @Double "x")
            , min (add (lit 1.0) (col @Double "z")) (col @Double "x")
            , min (add (lit 1.0) (col @Double "z")) (col @Double "y")
            , max (col @Double "y") (col @Double "x")
            , max (add (lit 1.0) (col @Double "z")) (col @Double "x")
            , max (add (lit 1.0) (col @Double "z")) (col @Double "y")
            , ifThenElse
                    (geq (col @Double "x") (percentile 1 (col @Double "x")))
                    (col @Double "x")
                    (col @Double "y")
            , ifThenElse
                    (geq (col @Double "x") (percentile 25 (col @Double "x")))
                    (col @Double "x")
                    (col @Double "y")
            , ifThenElse
                    (geq (col @Double "x") (percentile 50 (col @Double "x")))
                    (col @Double "x")
                    (col @Double "y")
            , ifThenElse
                    (geq (col @Double "x") (percentile 75 (col @Double "x")))
                    (col @Double "x")
                    (col @Double "y")
            , ifThenElse
                    (geq (col @Double "x") (percentile 99 (col @Double "x")))
                    (col @Double "x")
                    (col @Double "y")
            , ifThenElse
                    (geq (col @Double "x") (percentile 1 (col @Double "x")))
                    (col @Double "x")
                    (add (lit 1.0) (col @Double "z"))
            , ifThenElse
                    (geq (col @Double "x") (percentile 25 (col @Double "x")))
                    (col @Double "x")
                    (add (lit 1.0) (col @Double "z"))
            , ifThenElse
                    (geq (col @Double "x") (percentile 50 (col @Double "x")))
                    (col @Double "x")
                    (add (lit 1.0) (col @Double "z"))
            , ifThenElse
                    (geq (col @Double "x") (percentile 75 (col @Double "x")))
                    (col @Double "x")
                    (add (lit 1.0) (col @Double "z"))
            , ifThenElse
                    (geq (col @Double "x") (percentile 99 (col @Double "x")))
                    (col @Double "x")
                    (add (lit 1.0) (col @Double "z"))
            , ifThenElse
                    (geq (col @Double "y") (percentile 1 (col @Double "y")))
                    (col @Double "y")
                    (col @Double "x")
            , ifThenElse
                    (geq (col @Double "y") (percentile 25 (col @Double "y")))
                    (col @Double "y")
                    (col @Double "x")
            , ifThenElse
                    (geq (col @Double "y") (percentile 50 (col @Double "y")))
                    (col @Double "y")
                    (col @Double "x")
            , ifThenElse
                    (geq (col @Double "y") (percentile 75 (col @Double "y")))
                    (col @Double "y")
                    (col @Double "x")
            , ifThenElse
                    (geq (col @Double "y") (percentile 99 (col @Double "y")))
                    (col @Double "y")
                    (col @Double "x")
            , ifThenElse
                    (geq (col @Double "y") (percentile 1 (col @Double "y")))
                    (col @Double "y")
                    (add (lit 1.0) (col @Double "z"))
            , ifThenElse
                    (geq (col @Double "y") (percentile 25 (col @Double "y")))
                    (col @Double "y")
                    (add (lit 1.0) (col @Double "z"))
            , ifThenElse
                    (geq (col @Double "y") (percentile 50 (col @Double "y")))
                    (col @Double "y")
                    (add (lit 1.0) (col @Double "z"))
            , ifThenElse
                    (geq (col @Double "y") (percentile 75 (col @Double "y")))
                    (col @Double "y")
                    (add (lit 1.0) (col @Double "z"))
            , ifThenElse
                    (geq (col @Double "y") (percentile 99 (col @Double "y")))
                    (col @Double "y")
                    (add (lit 1.0) (col @Double "z"))
            , ifThenElse
                    ( geq
                        (add (lit 1.0) (col @Double "z"))
                        (percentile 1 (add (lit 1.0) (col @Double "z")))
                    )
                    (add (lit 1.0) (col @Double "z"))
                    (col @Double "x")
            , ifThenElse
                    ( geq
                        (add (lit 1.0) (col @Double "z"))
                        (percentile 25 (add (lit 1.0) (col @Double "z")))
                    )
                    (add (lit 1.0) (col @Double "z"))
                    (col @Double "x")
            , ifThenElse
                    ( geq
                        (add (lit 1.0) (col @Double "z"))
                        (percentile 50 (add (lit 1.0) (col @Double "z")))
                    )
                    (add (lit 1.0) (col @Double "z"))
                    (col @Double "x")
            , ifThenElse
                    ( geq
                        (add (lit 1.0) (col @Double "z"))
                        (percentile 75 (add (lit 1.0) (col @Double "z")))
                    )
                    (add (lit 1.0) (col @Double "z"))
                    (col @Double "x")
            , ifThenElse
                    ( geq
                        (add (lit 1.0) (col @Double "z"))
                        (percentile 99 (add (lit 1.0) (col @Double "z")))
                    )
                    (add (lit 1.0) (col @Double "z"))
                    (col @Double "x")
            , ifThenElse
                    ( geq
                        (add (lit 1.0) (col @Double "z"))
                        (percentile 1 (add (lit 1.0) (col @Double "z")))
                    )
                    (add (lit 1.0) (col @Double "z"))
                    (col @Double "y")
            , ifThenElse
                    ( geq
                        (add (lit 1.0) (col @Double "z"))
                        (percentile 25 (add (lit 1.0) (col @Double "z")))
                    )
                    (add (lit 1.0) (col @Double "z"))
                    (col @Double "y")
            , ifThenElse
                    ( geq
                        (add (lit 1.0) (col @Double "z"))
                        (percentile 50 (add (lit 1.0) (col @Double "z")))
                    )
                    (add (lit 1.0) (col @Double "z"))
                    (col @Double "y")
            , ifThenElse
                    ( geq
                        (add (lit 1.0) (col @Double "z"))
                        (percentile 75 (add (lit 1.0) (col @Double "z")))
                    )
                    (add (lit 1.0) (col @Double "z"))
                    (col @Double "y")
            , ifThenElse
                    ( geq
                        (add (lit 1.0) (col @Double "z"))
                        (percentile 99 (add (lit 1.0) (col @Double "z")))
                    )
                    (add (lit 1.0) (col @Double "z"))
                    (col @Double "y")
            , ifThenElse
                    (geq (col @Double "x") (percentile 1 (col @Double "x")))
                    (col @Double "x")
                    (lit 1.0)
            , ifThenElse
                    (geq (col @Double "x") (percentile 25 (col @Double "x")))
                    (col @Double "x")
                    (lit 1.0)
            , ifThenElse
                    (geq (col @Double "x") (percentile 50 (col @Double "x")))
                    (col @Double "x")
                    (lit 1.0)
            , ifThenElse
                    (geq (col @Double "x") (percentile 75 (col @Double "x")))
                    (col @Double "x")
                    (lit 1.0)
            , ifThenElse
                    (geq (col @Double "x") (percentile 99 (col @Double "x")))
                    (col @Double "x")
                    (lit 1.0)
            , ifThenElse
                    (geq (col @Double "x") (percentile 1 (col @Double "x")))
                    (col @Double "x")
                    (lit 0.0)
            , ifThenElse
                    (geq (col @Double "x") (percentile 25 (col @Double "x")))
                    (col @Double "x")
                    (lit 0.0)
            , ifThenElse
                    (geq (col @Double "x") (percentile 50 (col @Double "x")))
                    (col @Double "x")
                    (lit 0.0)
            , ifThenElse
                    (geq (col @Double "x") (percentile 75 (col @Double "x")))
                    (col @Double "x")
                    (lit 0.0)
            , ifThenElse
                    (geq (col @Double "x") (percentile 99 (col @Double "x")))
                    (col @Double "x")
                    (lit 0.0)
            , ifThenElse
                    (geq (col @Double "x") (percentile 1 (col @Double "x")))
                    (col @Double "x")
                    (lit (-1.0))
            , ifThenElse
                    (geq (col @Double "x") (percentile 25 (col @Double "x")))
                    (col @Double "x")
                    (lit (-1.0))
            , ifThenElse
                    (geq (col @Double "x") (percentile 50 (col @Double "x")))
                    (col @Double "x")
                    (lit (-1.0))
            , ifThenElse
                    (geq (col @Double "x") (percentile 75 (col @Double "x")))
                    (col @Double "x")
                    (lit (-1.0))
            , ifThenElse
                    (geq (col @Double "x") (percentile 99 (col @Double "x")))
                    (col @Double "x")
                    (lit (-1.0))
            , ifThenElse
                    (geq (col @Double "y") (percentile 1 (col @Double "y")))
                    (col @Double "y")
                    (lit 1.0)
            , ifThenElse
                    (geq (col @Double "y") (percentile 25 (col @Double "y")))
                    (col @Double "y")
                    (lit 1.0)
            , ifThenElse
                    (geq (col @Double "y") (percentile 50 (col @Double "y")))
                    (col @Double "y")
                    (lit 1.0)
            , ifThenElse
                    (geq (col @Double "y") (percentile 75 (col @Double "y")))
                    (col @Double "y")
                    (lit 1.0)
            , ifThenElse
                    (geq (col @Double "y") (percentile 99 (col @Double "y")))
                    (col @Double "y")
                    (lit 1.0)
            , ifThenElse
                    (geq (col @Double "y") (percentile 1 (col @Double "y")))
                    (col @Double "y")
                    (lit 0.0)
            , ifThenElse
                    (geq (col @Double "y") (percentile 25 (col @Double "y")))
                    (col @Double "y")
                    (lit 0.0)
            , ifThenElse
                    (geq (col @Double "y") (percentile 50 (col @Double "y")))
                    (col @Double "y")
                    (lit 0.0)
            , ifThenElse
                    (geq (col @Double "y") (percentile 75 (col @Double "y")))
                    (col @Double "y")
                    (lit 0.0)
            , ifThenElse
                    (geq (col @Double "y") (percentile 99 (col @Double "y")))
                    (col @Double "y")
                    (lit 0.0)
            , ifThenElse
                    (geq (col @Double "y") (percentile 1 (col @Double "y")))
                    (col @Double "y")
                    (lit (-1.0))
            , ifThenElse
                    (geq (col @Double "y") (percentile 25 (col @Double "y")))
                    (col @Double "y")
                    (lit (-1.0))
            , ifThenElse
                    (geq (col @Double "y") (percentile 50 (col @Double "y")))
                    (col @Double "y")
                    (lit (-1.0))
            , ifThenElse
                    (geq (col @Double "y") (percentile 75 (col @Double "y")))
                    (col @Double "y")
                    (lit (-1.0))
            , ifThenElse
                    (geq (col @Double "y") (percentile 99 (col @Double "y")))
                    (col @Double "y")
                    (lit (-1.0))
            , ifThenElse
                    ( geq
                        (add (lit 1.0) (col @Double "z"))
                        (percentile 1 (add (lit 1.0) (col @Double "z")))
                    )
                    (add (lit 1.0) (col @Double "z"))
                    (lit 1.0)
            , ifThenElse
                    ( geq
                        (add (lit 1.0) (col @Double "z"))
                        (percentile 25 (add (lit 1.0) (col @Double "z")))
                    )
                    (add (lit 1.0) (col @Double "z"))
                    (lit 1.0)
            , ifThenElse
                    ( geq
                        (add (lit 1.0) (col @Double "z"))
                        (percentile 50 (add (lit 1.0) (col @Double "z")))
                    )
                    (add (lit 1.0) (col @Double "z"))
                    (lit 1.0)
            , ifThenElse
                    ( geq
                        (add (lit 1.0) (col @Double "z"))
                        (percentile 75 (add (lit 1.0) (col @Double "z")))
                    )
                    (add (lit 1.0) (col @Double "z"))
                    (lit 1.0)
            , ifThenElse
                    ( geq
                        (add (lit 1.0) (col @Double "z"))
                        (percentile 99 (add (lit 1.0) (col @Double "z")))
                    )
                    (add (lit 1.0) (col @Double "z"))
                    (lit 1.0)
            , ifThenElse
                    ( geq
                        (add (lit 1.0) (col @Double "z"))
                        (percentile 1 (add (lit 1.0) (col @Double "z")))
                    )
                    (add (lit 1.0) (col @Double "z"))
                    (lit 0.0)
            , ifThenElse
                    ( geq
                        (add (lit 1.0) (col @Double "z"))
                        (percentile 25 (add (lit 1.0) (col @Double "z")))
                    )
                    (add (lit 1.0) (col @Double "z"))
                    (lit 0.0)
            , ifThenElse
                    ( geq
                        (add (lit 1.0) (col @Double "z"))
                        (percentile 50 (add (lit 1.0) (col @Double "z")))
                    )
                    (add (lit 1.0) (col @Double "z"))
                    (lit 0.0)
            , ifThenElse
                    ( geq
                        (add (lit 1.0) (col @Double "z"))
                        (percentile 75 (add (lit 1.0) (col @Double "z")))
                    )
                    (add (lit 1.0) (col @Double "z"))
                    (lit 0.0)
            , ifThenElse
                    ( geq
                        (add (lit 1.0) (col @Double "z"))
                        (percentile 99 (add (lit 1.0) (col @Double "z")))
                    )
                    (add (lit 1.0) (col @Double "z"))
                    (lit 0.0)
            , ifThenElse
                    ( geq
                        (add (lit 1.0) (col @Double "z"))
                        (percentile 1 (add (lit 1.0) (col @Double "z")))
                    )
                    (add (lit 1.0) (col @Double "z"))
                    (lit (-1.0))
            , ifThenElse
                    ( geq
                        (add (lit 1.0) (col @Double "z"))
                        (percentile 25 (add (lit 1.0) (col @Double "z")))
                    )
                    (add (lit 1.0) (col @Double "z"))
                    (lit (-1.0))
            , ifThenElse
                    ( geq
                        (add (lit 1.0) (col @Double "z"))
                        (percentile 50 (add (lit 1.0) (col @Double "z")))
                    )
                    (add (lit 1.0) (col @Double "z"))
                    (lit (-1.0))
            , ifThenElse
                    ( geq
                        (add (lit 1.0) (col @Double "z"))
                        (percentile 75 (add (lit 1.0) (col @Double "z")))
                    )
                    (add (lit 1.0) (col @Double "z"))
                    (lit (-1.0))
            , ifThenElse
                    ( geq
                        (add (lit 1.0) (col @Double "z"))
                        (percentile 99 (add (lit 1.0) (col @Double "z")))
                    )
                    (add (lit 1.0) (col @Double "z"))
                    (lit (-1.0))
            , add (col @Double "x") (negate (col @Double "y"))
            , add (col @Double "x") (negate (add (lit 1.0) (col @Double "z")))
            , add (col @Double "y") (negate (col @Double "x"))
            , add (col @Double "y") (negate (add (lit 1.0) (col @Double "z")))
            , add (add (lit 1.0) (col @Double "z")) (negate (col @Double "x"))
            , add (add (lit 1.0) (col @Double "z")) (negate (col @Double "y"))
            , mult (col @Double "x") (col @Double "x")
            , mult (col @Double "y") (col @Double "x")
            , mult (col @Double "y") (col @Double "y")
            , mult (add (lit 1.0) (col @Double "z")) (col @Double "x")
            , mult (add (lit 1.0) (col @Double "z")) (col @Double "y")
            , mult (add (lit 1.0) (col @Double "z")) (add (lit 1.0) (col @Double "z"))
            , divide (col @Double "x") (col @Double "y")
            , divide (col @Double "x") (add (lit 1.0) (col @Double "z"))
            , divide (col @Double "y") (col @Double "x")
            , divide (col @Double "y") (add (lit 1.0) (col @Double "z"))
            , divide (add (lit 1.0) (col @Double "z")) (col @Double "x")
            , divide (add (lit 1.0) (col @Double "z")) (col @Double "y")
            ]
            (generatePrograms [col "x"] [col "y", lit 1 + col "z"])
        )

tests :: [Test]
tests =
    [ TestLabel "sanitizeIdentifiers" sanitizeIdentifiers
    , TestLabel
        "generateProgramsCalledWithNoExistingPrograms"
        generateProgramsCalledWithNoExistingPrograms
    , TestLabel
        "generateProgramsCalledWithSingleExistingPrograms"
        generateProgramsCalledWithSingleExistingPrograms
    , TestLabel
        "generateProgramsCalledWithCompoundExistingPrograms"
        generateProgramsCalledWithCompoundExistingPrograms
    ]
