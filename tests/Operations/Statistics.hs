{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Operations.Statistics where

import qualified Data.Vector.Unboxed as VU
import qualified DataFrame as D
import qualified DataFrame as DE

import Assertions
import Test.HUnit

medianOfOddLengthDataSet :: Test
medianOfOddLengthDataSet =
    TestCase
        ( assertEqual
            "Median of an odd length data set"
            (D.median' (VU.fromList [179.94, 231.94, 839.06, 534.23, 248.94]))
            248.94
        )

medianOfEvenLengthDataSet :: Test
medianOfEvenLengthDataSet =
    TestCase
        ( assertEqual
            "Median of an even length data set"
            (D.median' (VU.fromList [179.94, 231.94, 839.06, 534.23, 248.94, 276.37]))
            262.655
        )

medianOfEmptyDataSet :: Test
medianOfEmptyDataSet =
    TestCase
        ( assertExpectException
            "[Error Case]"
            (DE.emptyDataSetError "median")
            (print $ D.median' (VU.fromList []))
        )

tests :: [Test]
tests =
    [ TestLabel "medianOfOddLengthDataSet" medianOfOddLengthDataSet
    , TestLabel "medianOfEvenLengthDataSet" medianOfEvenLengthDataSet
    , TestLabel "medianOfEmptyDataSet" medianOfEmptyDataSet
    ]
