{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Main where

import Control.Exception (throw)
import Control.Monad (when)
import Data.Either
import qualified Data.Map as M
import Data.Maybe
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified DataFrame as D
import qualified DataFrame.Functions as F
import DataFrame.Hasktorch (toTensor)
import Torch

import DataFrame ((|>))

main :: IO ()
main = do
    {- Feature ingestion and engineering -}
    df <- D.readCsv "../data/housing.csv"

    let cleaned =
            df
                |> D.impute "total_bedrooms" (fromMaybe 0 (D.mean "total_bedrooms" df))
                |> D.exclude ["median_house_value"]
                |> D.derive "ocean_proximity" (F.lift oceanProximity (F.col "ocean_proximity"))
                |> D.derive
                    "rooms_per_household"
                    (F.col @Double "total_rooms" / F.col "households")
                |> normalizeFeatures

        -- Convert to hasktorch tensor
        features = toTensor cleaned
        labels = toTensor (D.select ["median_house_value"] df)

    {- Train the model -}
    putStrLn "Training linear regression model..."
    init <-
        sample $ LinearSpec{in_features = snd (D.dimensions cleaned), out_features = 1}
    trained <- foldLoop init 100_000 $ \state i -> do
        let labels' = model state features
            loss = mseLoss labels labels'
        when (i `mod` 10_000 == 0) $ do
            putStrLn $ "Iteration: " ++ show i ++ " | Loss: " ++ show loss
        (state', _) <- runStep state GD loss 0.1
        pure state'

    {- Show predictions -}
    let predictions =
            D.insertUnboxedVector
                "predicted_house_value"
                (asValue @(VU.Vector Float) (model trained features))
                df
    print $
        D.select ["median_house_value", "predicted_house_value"] predictions
            |> D.take 10

normalizeFeatures :: D.DataFrame -> D.DataFrame
normalizeFeatures df =
    df
        |> D.fold
            ( \name d ->
                let
                    -- Convenience reference to the column.
                    col = F.col @Double name
                 in
                    D.derive name ((col - F.minimum col) / (F.maximum col - F.minimum col)) d
            )
            (D.columnNames df)

model :: Linear -> Tensor -> Tensor
model state input = squeezeAll $ linear state input

oceanProximity :: T.Text -> Double
oceanProximity op = case op of
    "ISLAND" -> 0
    "NEAR OCEAN" -> 1
    "NEAR BAY" -> 2
    "<1H OCEAN" -> 3
    "INLAND" -> 4
    _ -> error ("Unknown ocean proximity value: " ++ T.unpack op)
