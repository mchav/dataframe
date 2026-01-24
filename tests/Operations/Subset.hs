{-# LANGUAGE GADTs #-}

module Operations.Subset where

import DataFrame.Internal.DataFrame
import qualified DataFrame.Operations.Core as D
import qualified DataFrame.Operations.Subset as D
import System.Random

prop_dropZero :: DataFrame -> Bool
prop_dropZero df = D.drop 0 df == df

prop_takeZero :: DataFrame -> Bool
prop_takeZero df = fst (dataframeDimensions (D.take 0 df)) == 0

prop_takeAll :: DataFrame -> Bool
prop_takeAll df =
    let n = fst (dataframeDimensions df)
     in D.take n df == df

prop_dropAll :: DataFrame -> Bool
prop_dropAll df =
    let n = fst (dataframeDimensions df)
     in fst (dataframeDimensions (D.drop n df)) == 0

prop_takeLastZero :: DataFrame -> Bool
prop_takeLastZero df = fst (dataframeDimensions (D.takeLast 0 df)) == 0

prop_dropLastZero :: DataFrame -> Bool
prop_dropLastZero df = D.dropLast 0 df == df

prop_takeLastAll :: DataFrame -> Bool
prop_takeLastAll df =
    let n = fst (dataframeDimensions df)
     in D.takeLast n df == df

prop_dropLastAll :: DataFrame -> Bool
prop_dropLastAll df =
    let n = fst (dataframeDimensions df)
     in fst (dataframeDimensions (D.dropLast n df)) == 0

prop_rangeEmpty :: DataFrame -> Bool
prop_rangeEmpty df =
    fst (dataframeDimensions (D.range (5, 5) df)) == 0

prop_rangeFull :: DataFrame -> Bool
prop_rangeFull df =
    let rows = fst (dataframeDimensions df)
     in D.range (0, rows) df == df

prop_selectAll :: DataFrame -> Bool
prop_selectAll df = D.select (D.columnNames df) df == df

prop_selectEmpty :: DataFrame -> Bool
prop_selectEmpty df =
    let result = D.select [] df
     in dataframeDimensions result == (0, 0)

prop_excludeEmpty :: DataFrame -> Bool
prop_excludeEmpty df = D.exclude [] df == df

prop_excludeAll :: DataFrame -> Bool
prop_excludeAll df =
    let result = D.exclude (D.columnNames df) df
     in snd (dataframeDimensions result) == 0

prop_cubePreservesSmall :: DataFrame -> Bool
prop_cubePreservesSmall df =
    let (rows, cols) = dataframeDimensions df
     in D.cube (rows + 100, cols + 100) df == df

prop_sampleEmptyApprox :: DataFrame -> Bool
prop_sampleEmptyApprox df =
    let gen = mkStdGen 42
        sampled = D.sample gen 0.0 df
     in fst (dataframeDimensions sampled) == 0

tests =
    [ prop_dropZero
    , prop_takeZero
    , prop_takeAll
    , prop_dropAll
    , prop_takeLastZero
    , prop_dropLastZero
    , prop_takeLastAll
    , prop_dropLastAll
    , prop_rangeEmpty
    , prop_rangeFull
    , prop_selectAll
    , prop_selectEmpty
    , prop_excludeEmpty
    , prop_excludeAll
    , prop_cubePreservesSmall
    , prop_sampleEmptyApprox
    ]
