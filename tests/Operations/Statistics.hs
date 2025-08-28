{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE NumericUnderscores #-}

module Operations.Statistics where

import qualified Data.Vector.Unboxed as VU
import qualified DataFrame as D
import qualified DataFrame.Internal.Column as D
import qualified DataFrame.Internal.DataFrame as D
import qualified DataFrame.Operations.Statistics as D

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
            (D.emptyDataSetError "median")
            (print $ D.median' (VU.fromList []))
        )

skewnessOfDataSetWithSameElements :: Test
skewnessOfDataSetWithSameElements =
    TestCase
        ( assertBool
            "Skewness of a data set with the same elements"
            (isNaN (D.skewness' (VU.fromList $ replicate 10 42.0)))
        )

skewnessOfSymmetricDataSet :: Test
skewnessOfSymmetricDataSet =
    TestCase
        ( assertEqual
            "Skewness of a symmetric data set"
            (D.skewness' (VU.fromList [-3.0, -2.0, -1.5, 0, 1.5, 2.0, 3.0]))
            0
        )

skewnessOfSimpleDataSet :: Test
skewnessOfSimpleDataSet =
    TestCase
        ( assertBool
            "Skewness of a simple data set"
            (abs (D.skewness' (VU.fromList [25, 28, 26, 30, 40, 50, 40]) - 0.566_731_633_676) < 1e-12)
        )

skewnessOfEmptyDataSet :: Test
skewnessOfEmptyDataSet =
    TestCase
        ( assertEqual
            "Skewness of an empty data set"
            (D.skewness' (VU.fromList []))
            0
        )

tests :: [Test]
tests =
    [ TestLabel "medianOfOddLengthDataSet" medianOfOddLengthDataSet
    , TestLabel "medianOfEvenLengthDataSet" medianOfEvenLengthDataSet
    , TestLabel "medianOfEmptyDataSet" medianOfEmptyDataSet
    , TestLabel "skewnessOfDataSetWithSameElements" skewnessOfDataSetWithSameElements
    , TestLabel "skewnessOfSymmetricDataSet" skewnessOfSymmetricDataSet
    , TestLabel "skewnessOfSimpleDataSet" skewnessOfSimpleDataSet
    , TestLabel "skewnessOfEmptyDataSet" skewnessOfEmptyDataSet
    ]
