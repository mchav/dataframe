{-# LANGUAGE NumericUnderscores #-}
-- Useful Haskell extensions.
-- Allow string literal to be interpreted as any other string type.
{-# LANGUAGE OverloadedStrings #-}
-- Convenience syntax for specifiying the type `sum a b :: Int` vs `sum @Int a b'.
{-# LANGUAGE TypeApplications #-}

import qualified DataFrame as D -- import for general functionality.
import qualified DataFrame.Functions as F -- import for column expressions.

import DataFrame ((|>)) -- import chaining operator with unqualified.

import Control.Monad
import qualified Data.List as L
import Data.Maybe
import Data.Function
import Data.Text (Text)
import qualified Data.Text as T
import Data.Text.Read

readDouble :: Text -> Maybe Double
readDouble s =
    case signed double s of
        Left _ -> Nothing
        Right (value, "") -> Just value
        Right (value, _) -> Nothing

reformatOptionalDoubles :: [Text] -> D.DataFrame -> D.DataFrame
reformatOptionalDoubles cols df = L.foldl' (\d name -> D.derive name (F.lift (fromMaybe 0 . (\v -> if isNothing v then D.mean name df else v) . ((readDouble . T.replace "E" "e") =<<)) (F.col @(Maybe Text) name)) d) df cols

imputeDoubles cols df = L.foldl' (\d name -> D.impute name (fromMaybe 0 (D.mean name df)) d) df cols

intsAsDouble cols df = L.foldl' (flip convertToDouble) df cols
    where convertToDouble name = D.derive name (F.lift (fromIntegral @Int @Double) (F.col @Int name))

correlations d = map (`correlationWithFraud` d) (D.columnNames d)
    where
        correlationWithFraud columnName d = (columnName, fromMaybe 0 (D.correlation columnName "is_fraud" d))

trainPipeline df = let
        withFraud = df
                    |> D.impute "TRANSACTION_TAG_falcon" ("" :: Text)
                    |> D.derive "is_fraud" (F.lift (\t -> if t == "" then (0 :: Int) else 1) (F.col @Text "TRANSACTION_TAG_falcon"))

        formatAsDoubles = [ -- "features_merchant_frd_txn_count_30d_txn_count_30d_ratio"
                          -- , "features_merchant_frd_cardholder_count_30d_cardholder_count_30d_ratio"
                          -- , "features_transaction_customer_merchant_customer_merchant_txn_count_1d_30d_growth_rate"
                          -- , "features_merchant_frd_txn_amt_sum_30d_txn_amt_sum_30d_ratio"
                          "features_transaction_customer_sic_spending_deviation_sic_risk_weighted"
                          , "features_transaction_customer_trn_amt_repeated_windowed_customer_trn_amt_repeated_txn_count__86400__"
                          ]
        optionalDoubles = [ --"features_transaction_customer_merchant_spending_deviation_merchant_risk_weighted"
                          -- , "features_transaction_customer_spending_deviation_merchant_risk_weighted"
                          -- , "features_transaction_customer_sic_spending_deviation_merchant_risk_weighted"
                          -- , "features_transaction_customer_point_of_sale_spending_deviation_merchant_risk_weighted"
                          -- , "features_transaction_customer_trn_type_spending_deviation_merchant_risk_weighted"
                          "features_transaction_customer_spending_deviation_sic_risk_weighted"
                          , "features_customer_windowed_customer_transaction_avg__2592000__"]

        intColumns = [ "features_transaction_trn_typ_frd_ordinal"
                     , "features_transaction_trn_pos_ent_cd_frd_ordinal"
                     , "features_transaction_trn_pin_vrfy_cd_frd_ordinal"
                     , "features_transaction_trans_category_frd_ordinal"
                     , "features_transaction_crd_psnt_ind_frd_ordinal"
                     , "features_transaction_cryptogram_valid_frd_ordinal"
                     , "features_transaction_atc_crd_counter"
                     , "trn_amt"
                     , "is_fraud"
                     , "FRD_SCOR_falcon"]

    in withFraud
            |> reformatOptionalDoubles formatAsDoubles
            |> imputeDoubles optionalDoubles
            |> intsAsDouble intColumns
            |> D.select (formatAsDoubles ++ optionalDoubles ++ intColumns)

testPipeline df = let
        withFraud = df
                    |> D.impute "TRANSACTION_TAG_falcon" ("" :: Text)
                    |> D.derive "is_fraud" (F.lift (\t -> if t == "" then (0 :: Int) else 1) (F.col @Text "TRANSACTION_TAG_falcon"))

        formatAsDoubles = [ "features_merchant_frd_txn_count_30d_txn_count_30d_ratio"
                          , "features_merchant_frd_cardholder_count_30d_cardholder_count_30d_ratio"
                          , "features_transaction_customer_merchant_customer_merchant_txn_count_1d_30d_growth_rate"
                          , "features_merchant_frd_txn_amt_sum_30d_txn_amt_sum_30d_ratio"
                          , "features_transaction_customer_merchant_spending_deviation_merchant_risk_weighted"
                          , "features_transaction_customer_spending_deviation_merchant_risk_weighted"
                          , "features_transaction_customer_sic_spending_deviation_merchant_risk_weighted"
                          , "features_transaction_customer_point_of_sale_spending_deviation_merchant_risk_weighted"
                          , "features_transaction_customer_trn_type_spending_deviation_merchant_risk_weighted"
                          , "features_transaction_customer_trn_amt_repeated_windowed_customer_trn_amt_repeated_txn_count__86400__"
                          ]
        optionalDoubles = [ "features_transaction_customer_spending_deviation_sic_risk_weighted"
                          , "features_transaction_customer_sic_spending_deviation_sic_risk_weighted"
                          , "features_customer_windowed_customer_transaction_avg__2592000__"]

        intColumns = [ "features_transaction_trn_typ_frd_ordinal"
                     , "features_transaction_trn_pos_ent_cd_frd_ordinal"
                     , "features_transaction_trn_pin_vrfy_cd_frd_ordinal"
                     , "features_transaction_trans_category_frd_ordinal"
                     , "features_transaction_crd_psnt_ind_frd_ordinal"
                     , "features_transaction_cryptogram_valid_frd_ordinal"
                     , "features_transaction_atc_crd_counter"
                     , "trn_amt"
                     , "is_fraud"
                     , "FRD_SCOR_falcon"]

    in withFraud
            |> reformatOptionalDoubles formatAsDoubles
            |> imputeDoubles optionalDoubles
            |> intsAsDouble intColumns
            |> D.select (formatAsDoubles ++ optionalDoubles ++ intColumns)

main :: IO ()
main = do
    train <- fmap trainPipeline (D.readCsv "../../Downloads/transactions.csv")

    mapM_ print (L.sortBy (flip compare `on` snd) (correlations train))
    putStrLn ""

    let (Right e) = F.search "is_fraud" 3 (D.exclude ["FRD_SCOR_falcon"] train)

    print e

    mapM_ print (L.sortBy (flip compare `on` snd) (correlations (train |> D.derive "n" e)))

    putStrLn ""

    test <- fmap testPipeline (D.readCsv "../../Downloads/testset.csv")

    let df' = test |> D.derive "n" e

    mapM_ print (L.sortBy (flip compare `on` snd) (correlations df'))

