{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE NumericUnderscores #-}
module Main where

import Control.Monad (when)
import Data.Maybe
import qualified Data.Map as M
import qualified Data.Text as T
import qualified DataFrame as D
import qualified DataFrame.Functions as F
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector as V
import Torch

import DataFrame ((|>))

main :: IO ()
main = do
    {- Feature ingestion and engineering -}
    df <- fmap (D.apply (\(op :: T.Text) -> oceanProximity M.! op) "ocean_proximity") (D.readCsv "./data/housing.csv")
    -- This column has nulls so we:
    --  * Remove all nulls with filterJust
    --  * Calculate the mean of total_bedrooms
    --  * impute the mean.
    -- This could probably be a utility function.
    let meanTotalBedrooms = fromMaybe 0 $ df |> D.filterJust "total_bedrooms"
                                             |> D.mean "total_bedrooms"
        imputed = df |> D.impute "total_bedrooms" meanTotalBedrooms
                     |> D.exclude ["median_house_value"]
                     |> normalizeFeatures
        (r, c) = D.dimensions imputed
        features = reshape [r,c] $ asTensor (flattenFeatures imputed)
        labels = asTensor ((VU.map realToFrac . VU.convert) (D.columnAsVector @Double "median_house_value" df) :: VU.Vector Float)
    
    {- Train the model -}
    putStrLn "Training linear regression model..."
    init <- sample $ LinearSpec{in_features = (snd (D.dimensions df) - 1), out_features = 1}
    trained <- foldLoop init 100_000 $ \state i -> do
        let labels' = model state features
            loss = mseLoss labels labels'
        when (i `mod` 10_000 == 0) $ do
            putStrLn $ "Iteration: " ++ show i ++ " | Loss: " ++ show loss
        (state', _) <- runStep state GD loss 0.1
        pure state'
    
    {- Show predictions -}
    let predictions = D.insertUnboxedVector "predicted_house_value" (asValue @(VU.Vector Float) (model trained features)) df
    print $ D.select ["median_house_value", "predicted_house_value"] predictions |> D.take 10

normalizeFeatures :: D.DataFrame -> D.DataFrame
normalizeFeatures df = df |> D.fold (\name d -> let
    m      = fromMaybe 0 (D.mean name d)
    stdDev = fromMaybe 0.01 (D.standardDeviation name d)
    col    = F.col @Double name
  in D.derive name ((col - (F.minimum col)) / (F.maximum col - F.minimum col)) d) (D.columnNames df)


model :: Linear -> Tensor -> Tensor
model state input = squeezeAll $ linear state input

oceanProximity :: M.Map T.Text Double
oceanProximity = M.fromList [("ISLAND", 0), ("NEAR OCEAN", 1), ("NEAR BAY", 2), ("<1H OCEAN", 3), ("INLAND", 4)] 

flattenFeatures :: D.DataFrame -> VU.Vector Float
flattenFeatures df = V.foldl' (\acc v -> acc VU.++ v) VU.empty (D.toMatrix df)