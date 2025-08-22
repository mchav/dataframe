{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Operations.Statistics where

import qualified Data.Vector.Unboxed as VU
import qualified DataFrame as D
import qualified DataFrame as DE

import Assertions
import Test.HUnit

-- median'
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

-- standardDeviation'
standardDeviationOfSingleElementDataSet :: Test
standardDeviationOfSingleElementDataSet =
    TestCase
        ( assertEqual
            "Standard deviation of a data set with a single element"
            (D.standardDeviation' (VU.fromList [-3.5]))
            0
        )

standardDeviationOfSameElementsDataSet :: Test
standardDeviationOfSameElementsDataSet =
    TestCase
        ( assertEqual
            "Standard deviation of a data set with the same elements"
            (D.standardDeviation' (VU.fromList [3.5, 3.5, 3.5, 3.5]))
            0
        )

standardDeviationOfSimpleDataSet :: Test
standardDeviationOfSimpleDataSet =
    TestCase
        ( assertEqual
            "Standard deviation of a simple data set"
            (D.standardDeviation' (VU.fromList [2, 4, 4, 4, 5, 5, 7, 9]))
            2
        )

standardDeviationOfEmptyDataSet :: Test
standardDeviationOfEmptyDataSet =
    TestCase
        ( assertExpectException
            "[Error Case]"
            (DE.emptyDataSetError "standardDeviation")
            (print $ D.standardDeviation' (VU.fromList []))
        )

tests :: [Test]
tests =
    [ TestLabel "medianOfOddLengthDataSet" medianOfOddLengthDataSet
    , TestLabel "medianOfEvenLengthDataSet" medianOfEvenLengthDataSet
    , TestLabel "medianOfEmptyDataSet" medianOfEmptyDataSet
    , TestLabel "standardDeviationOfSingleElementDataSet" standardDeviationOfSingleElementDataSet
    , TestLabel "standardDeviationOfSameElementsDataSet" standardDeviationOfSameElementsDataSet
    , TestLabel "standardDeviationOfSimpleDataSet" standardDeviationOfSimpleDataSet
    , TestLabel "standardDeviationOfEmptyDataSet" standardDeviationOfEmptyDataSet
    ]
