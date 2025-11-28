{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

import Data.Text (Text)
import Data.Time
import qualified DataFrame as D
import qualified DataFrame.Functions as F
import qualified DataFrame.Lazy as LD

import DataFrame ((|>))

main :: IO ()
main = do
    let df = LD.scanCsv "../1brc/measurements.txt"
    res <- df |> LD.groupByAggregate ["names"]
                        [ F.minimum (F.col @Double "temperature") `F.as` "min"
                        , F.mean (F.col @Double "temperature") `F.as` "mean"
                        , F.maximum (F.col @Double "temperature") `F.as` "max"]
              |> LD.runDataFrame
    print res
    {- df <-
        D.readCsvWithOpts
            ( D.defaultReadOptions
                { D.typeSpec =
                    D.SpecifyTypes
                        [ D.schemaType @Text
                        , D.schemaType @Text
                        , D.schemaType @Text
                        , D.schemaType @Int
                        , D.schemaType @Int
                        , D.schemaType @Int
                        , D.schemaType @Int
                        , D.schemaType @Int
                        , D.schemaType @Double
                        ]
                }
            )
            "../db-benchmark/data/G1_1e7_1e2_0_0.csv"
    print df
    start <- getCurrentTime
    print $
        df
            |> D.groupBy ["id1"]
            |> D.aggregate [F.sum (F.col @Int "v1") `F.as` "v1_sum"]
    end <- getCurrentTime
    let computeTime = diffUTCTime end start
    putStrLn $ "Compute Time: " ++ show computeTime -}
