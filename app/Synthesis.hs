{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}

import qualified Data.Text as T
import qualified DataFrame as D
import qualified DataFrame.Functions as F

import Data.Char
import DataFrame ((|>))
import DataFrame.DecisionTree
import DataFrame.Functions ((.&&), (.=), (.==))

import System.Random

$(F.declareColumnsFromCsvFile "./data/titanic/train.csv")

main :: IO ()
main = do
    train <- D.readCsv "./data/titanic/train.csv"
    test <- D.readCsv "./data/titanic/test.csv"

    -- Apply the same transformations to training and test.
    let combined =
            (train <> test)
                |> D.deriveMany
                    [ "Ticket" .= F.lift (T.filter isAlpha) ticket
                    , "Name" .= F.match "\\s*([A-Za-z]+)\\." name
                    , "Cabin" .= F.whenPresent (T.take 1) cabin
                    ]
                |> D.renameMany
                    [ (F.name name, "title")
                    , (F.name cabin, "cabin_prefix")
                    , (F.name pclass, "passenger_class")
                    , (F.name sibsp, "number_of_siblings_and_spouses")
                    , (F.name parch, "number_of_parents_and_children")
                    ]
    print combined

    let (train', validation) =
            D.take
                (D.nRows train)
                combined
                |> D.filterJust (F.name survived)
                |> D.randomSplit (mkStdGen 4232) 0.7
        -- Split the test out again.
        test' =
            D.drop
                (D.nRows train)
                combined

        model =
            fitDecisionTree
                ( defaultTreeConfig
                    { maxTreeDepth = 5
                    , minSamplesSplit = 10
                    , minLeafSize = 3
                    , taoIterations = 100
                    , synthConfig =
                        defaultSynthConfig
                            { complexityPenalty = 0.00
                            , maxExprDepth = 2
                            , disallowedCombinations =
                                [ (F.name age, F.name fare)
                                , ("passenger_class", "number_of_siblings_and_spouses")
                                , ("passenger_class", "number_of_parents_and_children")
                                ]
                            }
                    }
                )
                survived -- Label to predict
                ( train'
                    |> D.exclude [F.name passengerid]
                )

    print model

    putStrLn "Training accuracy: "
    print $
        computeAccuracy
            (train' |> D.derive (F.name prediction) model)

    putStrLn "Validation accuracy: "
    print $
        computeAccuracy
            ( validation
                |> D.derive (F.name prediction) model
            )

    let predictions = D.derive (F.name survived) model test'
    D.writeCsv
        "./predictions.csv"
        (predictions |> D.select [F.name passengerid, F.name survived])

prediction :: D.Expr Int
prediction = F.col @Int "prediction"

computeAccuracy :: D.DataFrame -> Double
computeAccuracy df =
    let
        tp =
            fromIntegral $ D.nRows (D.filterWhere (survived .== 1 .&& prediction .== 1) df)
        tn =
            fromIntegral $ D.nRows (D.filterWhere (survived .== 0 .&& prediction .== 0) df)
        fp =
            fromIntegral $ D.nRows (D.filterWhere (survived .== 0 .&& prediction .== 1) df)
        fn =
            fromIntegral $ D.nRows (D.filterWhere (survived .== 1 .&& prediction .== 0) df)
     in
        (tp + tn) / (tp + tn + fp + fn)
