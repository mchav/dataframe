{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.DecisionTree where

import qualified DataFrame.Functions as F
import DataFrame.Internal.Column
import DataFrame.Internal.DataFrame (DataFrame (..), unsafeGetColumn)
import DataFrame.Internal.Expression (Expr (..), interpret)
import DataFrame.Internal.Statistics (percentileOrd')
import DataFrame.Operations.Core (columnNames, nRows)
import DataFrame.Operations.Subset (exclude, filterWhere)

import Control.Exception (throw)
import Data.Function (on)
import Data.List (foldl', maximumBy)
import qualified Data.Map.Strict as M
import qualified Data.Text as T
import Data.Type.Equality
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import Type.Reflection (typeRep)

import DataFrame.Functions ((.<=), (.==))

data TreeConfig = TreeConfig
    { maxDepth :: Int
    , minSamplesSplit :: Int
    }

fitDecisionTree ::
    forall a.
    (Columnable a) =>
    TreeConfig ->
    Expr a ->
    DataFrame ->
    Expr a
fitDecisionTree cfg (Col target) df =
    buildTree @a
        cfg
        (maxDepth cfg)
        target
        (generateConditions (exclude [target] df))
        df
fitDecisionTree _ expr _ = error $ "Cannot create tree for compound expression: " ++ show expr

buildTree ::
    forall a.
    (Columnable a) =>
    TreeConfig ->
    Int ->
    T.Text ->
    [Expr Bool] ->
    DataFrame ->
    Expr a
buildTree cfg depth target conds df
    | depth <= 0 || nRows df <= minSamplesSplit cfg =
        Lit (majorityValue @a target df)
    | otherwise =
        case findBestSplit @a target conds df of
            Nothing -> Lit (majorityValue @a target df)
            Just bestCond ->
                let (dfTrue, dfFalse) = partitionDataFrame bestCond df
                 in if nRows dfTrue == 0 || nRows dfFalse == 0
                        then Lit (majorityValue @a target df)
                        else
                            pruneTree
                                ( F.ifThenElse
                                    bestCond
                                    (buildTree @a cfg (depth - 1) target conds dfTrue)
                                    (buildTree @a cfg (depth - 1) target conds dfFalse)
                                )

pruneTree :: forall a. (Columnable a, Eq a) => Expr a -> Expr a
pruneTree (If cond trueBranch falseBranch) =
    let
        t = pruneTree trueBranch
        f = pruneTree falseBranch
     in
        if t == f
            then t
            else case (t, f) of
                -- Nested simplification: `if C1 then (if C1 then X else Y) else Z`
                -- becomes:     if C1 then X else Z`
                (If condInner tInner fInner, _) | cond == condInner -> If cond tInner f
                (_, If condInner tInner fInner) | cond == condInner -> If cond t fInner
                _ -> If cond t f
pruneTree (UnaryOp name op e) = UnaryOp name op (pruneTree e)
pruneTree (BinaryOp name op l r) = BinaryOp name op (pruneTree l) (pruneTree r)
pruneTree e = e

generateConditions :: DataFrame -> [Expr Bool]
generateConditions df =
    let
        genConds :: T.Text -> [Expr Bool]
        genConds colName = case unsafeGetColumn colName df of
            (BoxedColumn (col :: V.Vector a)) ->
                let
                    percentiles = map (Lit . (`percentileOrd'` col)) [1, 25, 75, 99]
                 in
                    map (Col @a colName .<=) percentiles
                        ++ map (Col @a colName .==) percentiles
            (OptionalColumn (col :: V.Vector a)) ->
                let
                    percentiles = map (Lit . (`percentileOrd'` col)) [1, 25, 75, 99]
                 in
                    map (Col @a colName .<=) percentiles
                        ++ map (Col @a colName .==) percentiles
            (UnboxedColumn (col :: VU.Vector a)) ->
                let
                    percentiles = map (Lit . (`percentileOrd'` VU.convert col)) [1, 25, 75, 99]
                 in
                    map (Col @a colName .<=) percentiles
                        ++ map (Col @a colName .==) percentiles
        columnConds = concatMap colConds [(l, r) | l <- columnNames df, r <- columnNames df]
          where
            colConds (!l, !r) = case (unsafeGetColumn l df, unsafeGetColumn r df) of
                (BoxedColumn (col1 :: V.Vector a), BoxedColumn (col2 :: V.Vector b)) -> case testEquality (typeRep @a) (typeRep @b) of
                    Nothing -> []
                    Just Refl -> [Col @a l .== Col @a r]
                (UnboxedColumn (col1 :: VU.Vector a), UnboxedColumn (col2 :: VU.Vector b)) -> case testEquality (typeRep @a) (typeRep @b) of
                    Nothing -> []
                    Just Refl -> [Col @a l .<= Col @a r, Col @a l .== Col @a r]
                (OptionalColumn (col1 :: V.Vector a), OptionalColumn (col2 :: V.Vector b)) -> case testEquality (typeRep @a) (typeRep @b) of
                    Nothing -> []
                    Just Refl -> [Col @a l .<= Col @a r, Col @a l .== Col @a r]
                _ -> []
     in
        concatMap genConds (columnNames df) ++ columnConds

partitionDataFrame :: Expr Bool -> DataFrame -> (DataFrame, DataFrame)
partitionDataFrame cond df = (filterWhere cond df, filterWhere (F.not cond) df)

findBestSplit ::
    forall a.
    (Columnable a) =>
    T.Text -> [Expr Bool] -> DataFrame -> Maybe (Expr Bool)
findBestSplit target conds df =
    let
        initialImpurity = calculateGini @a target df
        evalGain cond =
            let (t, f) = partitionDataFrame cond df
                n = fromIntegral @Int @Double (nRows df)
                weightT = fromIntegral @Int @Double (nRows t) / n
                weightF = fromIntegral @Int @Double (nRows f) / n
                newImpurity =
                    (weightT * calculateGini @a target t)
                        + (weightF * calculateGini @a target f)
             in initialImpurity - newImpurity

        validConds = filter (\c -> nRows (filterWhere c df) > 0) conds
     in
        if null validConds
            then Nothing
            else Just $ maximumBy (compare `on` evalGain) validConds

calculateGini ::
    forall a.
    (Columnable a) =>
    T.Text -> DataFrame -> Double
calculateGini target df =
    let n = fromIntegral $ nRows df
        counts = getCounts @a target df
        probs = map (\c -> fromIntegral c / n) (M.elems counts)
     in if n == 0 then 0 else 1 - sum (map (^ 2) probs)

majorityValue ::
    forall a.
    (Columnable a) =>
    T.Text -> DataFrame -> a
majorityValue target df =
    let counts = getCounts @a target df
     in if M.null counts
            then error "Empty DataFrame in leaf"
            else fst $ maximumBy (compare `on` snd) (M.toList counts)

getCounts ::
    forall a.
    (Columnable a) =>
    T.Text -> DataFrame -> M.Map a Int
getCounts target df =
    case interpret @a df (Col target) of
        Left e -> throw e
        Right (TColumn col) ->
            case toVector @a col of
                Left e -> throw e
                Right vals -> foldl' (\acc x -> M.insertWith (+) x 1 acc) M.empty (V.toList vals)
