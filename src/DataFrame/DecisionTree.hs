{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.DecisionTree where

import qualified DataFrame.Functions as F
import DataFrame.Internal.Column
import DataFrame.Internal.DataFrame (DataFrame (..), unsafeGetColumn)
import DataFrame.Internal.Expression (Expr (..), eSize)
import DataFrame.Internal.Interpreter (interpret)
import DataFrame.Internal.Statistics (percentile', percentileOrd')
import DataFrame.Internal.Types
import DataFrame.Operations.Core (columnNames, nRows)
import DataFrame.Operations.Statistics (percentile)
import DataFrame.Operations.Subset (exclude, filterWhere)

import Control.Exception (throw)
import Control.Monad (guard)
import Data.Containers.ListUtils (nubOrd)
import Data.Function (on)
import Data.List (foldl', maximumBy, sortBy)
import qualified Data.Map.Strict as M
import Data.Maybe
import qualified Data.Text as T
import Data.Type.Equality
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import Type.Reflection (typeRep)

import DataFrame.Functions ((.<), (.<=), (.==), (.>), (.>=))

data TreeConfig
    = TreeConfig
    { maxTreeDepth :: Int
    , minSamplesSplit :: Int
    , minLeafSize :: Int
    , synthConfig :: SynthConfig
    }
    deriving (Eq, Show)

data SynthConfig = SynthConfig
    { maxExprDepth :: Int
    , boolExpansion :: Int
    , percentiles :: [Int]
    , complexityPenalty :: Double
    , enableStringOps :: Bool
    , enableCrossCols :: Bool
    , enableArithOps :: Bool
    }
    deriving (Eq, Show)

defaultSynthConfig :: SynthConfig
defaultSynthConfig =
    SynthConfig
        { maxExprDepth = 2
        , boolExpansion = 2
        , percentiles = [0, 10 .. 100]
        , complexityPenalty = 0.05
        , enableStringOps = True
        , enableCrossCols = True
        , enableArithOps = True
        }

defaultTreeConfig :: TreeConfig
defaultTreeConfig =
    TreeConfig
        { maxTreeDepth = 4
        , minSamplesSplit = 5
        , minLeafSize = 1
        , synthConfig = defaultSynthConfig
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
        (maxTreeDepth cfg)
        target
        ( numericConditions (synthConfig cfg) (exclude [target] df)
            ++ generateConditionsOld (synthConfig cfg) (exclude [target] df)
        )
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
        case findBestSplit @a cfg target conds df of
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
                -- Generalize this with hegg later.
                (If condInner tInner fInner, _) | cond == condInner -> If cond tInner f
                (_, If condInner tInner fInner) | cond == condInner -> If cond t fInner
                _ -> If cond t f
pruneTree (UnaryOp name op e) = UnaryOp name op (pruneTree e)
pruneTree (BinaryOp name op l r) = BinaryOp name op (pruneTree l) (pruneTree r)
pruneTree e = e

type CondGen = SynthConfig -> DataFrame -> [Expr Bool]

numericConditions :: CondGen
numericConditions = generateNumericConds

generateNumericConds ::
    SynthConfig -> DataFrame -> [Expr Bool]
generateNumericConds cfg df = do
    expr <- numericExprsWithTerms cfg df
    let thresholds = map (\p -> percentile p expr df) (percentiles cfg)
    threshold <- thresholds
    [ expr .<= F.lit threshold
        , expr .>= F.lit threshold
        , expr .< F.lit threshold
        , expr .> F.lit threshold
        ]

numericExprsWithTerms ::
    SynthConfig -> DataFrame -> [Expr Double]
numericExprsWithTerms cfg df =
    concatMap (numericExprs cfg df [] 0) [0 .. maxExprDepth cfg]

numericCols :: DataFrame -> [Expr Double]
numericCols df = concatMap extract (columnNames df)
  where
    extract col = case unsafeGetColumn col df of
        UnboxedColumn (_ :: VU.Vector b) ->
            case testEquality (typeRep @b) (typeRep @Double) of
                Just Refl -> [Col col]
                Nothing -> case sIntegral @b of
                    STrue -> [F.toDouble (Col @b col)]
                    SFalse -> []
        _ -> []

numericExprs ::
    SynthConfig -> DataFrame -> [Expr Double] -> Int -> Int -> [Expr Double]
numericExprs cfg df prevExprs depth maxDepth
    | depth == 0 = baseExprs ++ numericExprs cfg df baseExprs (depth + 1) maxDepth
    | depth >= maxDepth = []
    | otherwise =
        combinedExprs ++ numericExprs cfg df combinedExprs (depth + 1) maxDepth
  where
    baseExprs = numericCols df

    combinedExprs
        | not (enableArithOps cfg) = []
        | otherwise = do
            e1 <- prevExprs
            e2 <- baseExprs
            guard (e1 /= e2)
            [e1 + e2, e1 - e2, e1 * e2, F.ifThenElse (e2 .>= 0) (e1 / e2) 0]

boolExprs ::
    DataFrame -> [Expr Bool] -> [Expr Bool] -> Int -> Int -> [Expr Bool]
boolExprs df baseExprs prevExprs depth maxDepth
    | depth == 0 =
        baseExprs ++ boolExprs df baseExprs prevExprs (depth + 1) maxDepth
    | depth >= maxDepth = []
    | otherwise =
        combinedExprs ++ boolExprs df baseExprs combinedExprs (depth + 1) maxDepth
  where
    combinedExprs = do
        e1 <- prevExprs
        e2 <- baseExprs
        guard (e1 /= e2)
        [F.and e1 e2, F.or e1 e2]

generateConditionsOld :: SynthConfig -> DataFrame -> [Expr Bool]
generateConditionsOld cfg df =
    let
        genConds :: T.Text -> [Expr Bool]
        genConds colName = case unsafeGetColumn colName df of
            (BoxedColumn (col :: V.Vector a)) ->
                let
                    percentiles = map (Lit . (`percentileOrd'` col)) [1, 25, 75, 99]
                 in
                    map (Col @a colName .==) percentiles
            (OptionalColumn (col :: V.Vector (Maybe a))) -> case sFloating @a of
                STrue ->
                    let
                        doubleCol =
                            VU.convert
                                (V.map fromJust (V.filter isJust (V.map (fmap (realToFrac @a @Double)) col)))
                     in
                        zipWith
                            ($)
                            [ (Col @(Maybe a) colName .==)
                            , (Col @(Maybe a) colName .<=)
                            , (Col @(Maybe a) colName .>=)
                            ]
                            ( Lit Nothing
                                : map
                                    ( Lit
                                        . Just
                                        . realToFrac
                                        . (`percentile'` doubleCol)
                                    )
                                    (percentiles cfg)
                            )
                SFalse -> case sIntegral @a of
                    STrue ->
                        let
                            doubleCol =
                                VU.convert
                                    (V.map fromJust (V.filter isJust (V.map (fmap (fromIntegral @a @Double)) col)))
                         in
                            zipWith
                                ($)
                                [ (Col @(Maybe a) colName .==)
                                , (Col @(Maybe a) colName .<=)
                                , (Col @(Maybe a) colName .>=)
                                ]
                                ( Lit Nothing
                                    : map
                                        ( Lit
                                            . Just
                                            . round
                                            . (`percentile'` doubleCol)
                                        )
                                        (percentiles cfg)
                                )
                    SFalse ->
                        map
                            ((Col @(Maybe a) colName .==) . Lit . (`percentileOrd'` col))
                            [1, 25, 75, 99]
            (UnboxedColumn (col :: VU.Vector a)) -> []
        columnConds = concatMap colConds [(l, r) | l <- columnNames df, r <- columnNames df]
          where
            colConds (!l, !r) = case (unsafeGetColumn l df, unsafeGetColumn r df) of
                (BoxedColumn (col1 :: V.Vector a), BoxedColumn (col2 :: V.Vector b)) -> case testEquality (typeRep @a) (typeRep @b) of
                    Nothing -> []
                    Just Refl -> [Col @a l .== Col @a r]
                (UnboxedColumn (col1 :: VU.Vector a), UnboxedColumn (col2 :: VU.Vector b)) -> []
                ( OptionalColumn (col1 :: V.Vector (Maybe a))
                    , OptionalColumn (col2 :: V.Vector (Maybe b))
                    ) -> case testEquality (typeRep @a) (typeRep @b) of
                        Nothing -> []
                        Just Refl -> case testEquality (typeRep @a) (typeRep @T.Text) of
                            Nothing -> [Col @(Maybe a) l .<= Col r, Col @(Maybe a) l .== Col r]
                            Just Refl -> [Col @(Maybe a) l .== Col r]
                _ -> []
     in
        concatMap genConds (columnNames df) ++ columnConds

partitionDataFrame :: Expr Bool -> DataFrame -> (DataFrame, DataFrame)
partitionDataFrame cond df = (filterWhere cond df, filterWhere (F.not cond) df)

findBestSplit ::
    forall a.
    (Columnable a) =>
    TreeConfig -> T.Text -> [Expr Bool] -> DataFrame -> Maybe (Expr Bool)
findBestSplit cfg target conds df =
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
             in ( (initialImpurity - newImpurity)
                    - complexityPenalty (synthConfig cfg) * fromIntegral (eSize cond)
                , negate (eSize cond)
                )

        validConds =
            filter
                ( \c ->
                    let
                        (t, f) = partitionDataFrame c df
                     in
                        nRows t >= minLeafSize cfg && nRows f >= minLeafSize cfg
                )
                (nubOrd conds)
        sortedConditions = take 10 (sortBy (flip compare `on` evalGain) validConds)
     in
        if null validConds
            then Nothing
            else
                Just $
                    maximumBy
                        (compare `on` evalGain)
                        ( boolExprs
                            df
                            sortedConditions
                            sortedConditions
                            0
                            (boolExpansion (synthConfig cfg))
                        )

calculateGini ::
    forall a.
    (Columnable a) =>
    T.Text -> DataFrame -> Double
calculateGini target df =
    let n = fromIntegral $ nRows df
        counts = getCounts @a target df
        numClasses = fromIntegral $ M.size counts
        probs = map (\c -> (fromIntegral c + 1) / (n + numClasses)) (M.elems counts)
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
