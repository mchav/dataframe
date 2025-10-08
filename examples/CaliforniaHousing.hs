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
import qualified Data.Vector.Unboxed.Mutable as VUM
import qualified DataFrame as D
import qualified DataFrame.Functions as F
import qualified DataFrame.Internal.Expression as F
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
        features = either throw id (toTensor cleaned)
        labels = either throw id (toTensor (df |> D.select ["median_house_value"]))

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

toTensor :: D.DataFrame -> Either D.DataFrameException Tensor
toTensor df = case D.toMatrix df of
    Left e -> Left e
    Right m ->
        let
            (r, c) = D.dimensions df
            dims' = if c == 1 then [r] else [r, c]
         in
            Right (reshape dims' (asTensor (flattenFeatures m)))

flattenFeatures :: V.Vector (VU.Vector Float) -> VU.Vector Float
flattenFeatures rows =
    let
        total = V.foldl' (\s v -> s + VU.length v) 0 rows
     in
        VU.create $ do
            mv <- VUM.unsafeNew total
            let go !i !off
                    | i == V.length rows = pure ()
                    | otherwise = do
                        let v = rows V.! i
                            len = VU.length v
                        VU.unsafeCopy (VUM.unsafeSlice off len mv) v
                        go (i + 1) (off + len)
            go 0 0
            pure mv
