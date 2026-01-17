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
import DataFrame.Internal.Expression (Expr (..), eSize, getColumns, prettyPrint)
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
import Data.List (foldl', maximumBy, sort, sortBy)
import qualified Data.Map.Strict as M
import Data.Maybe
import Data.Ollama.Generate
import qualified Data.Text as T
import Data.Type.Equality
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import Type.Reflection (typeRep)

import Data.Text.Read
import DataFrame.Functions ((./=), (.<), (.<=), (.==), (.>), (.>=))
import Debug.Trace (trace)
import GHC.IO.Unsafe (unsafePerformIO)

data TreeConfig
    = TreeConfig
    { maxTreeDepth :: Int
    , minSamplesSplit :: Int
    , minLeafSize :: Int
    , percentiles :: [Int]
    , expressionPairs :: Int
    , synthConfig :: SynthConfig
    }
    deriving (Eq, Show)

data SynthConfig = SynthConfig
    { maxExprDepth :: Int
    , boolExpansion :: Int
    , disallowedCombinations :: [(T.Text, T.Text)]
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
        , disallowedCombinations = []
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
        , percentiles = [0, 10 .. 100]
        , expressionPairs = 10
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
        ( nubOrd $
            numericConditions cfg (exclude [target] df)
                ++ generateConditionsOld cfg (exclude [target] df)
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

type CondGen = TreeConfig -> DataFrame -> [Expr Bool]

numericConditions :: CondGen
numericConditions = generateNumericConds

generateNumericConds ::
    TreeConfig -> DataFrame -> [Expr Bool]
generateNumericConds cfg df = do
    expr <- numericExprsWithTerms (synthConfig cfg) df
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

instructions :: String
instructions =
    "You must output ONLY a single digit 0-10, nothing else. No explanation, no text, just the number. \n"
        ++ "Rate whether this expression produces a meaningful real-world quantity: \n"
        ++ "Evaluate if this expression produces a meaningful quantity by checking:\n"
        ++ "- Do the units/types match for the operation?\n"
        ++ "- Is either operand a categorical code rather than a true quantity?\n"
        ++ "- Would the result be useful to actually calculate?\n"
        ++ "Scoring: \n"
        ++ " - 0-3: Result is meaningless (e.g., \"fare + age\" = dollars+years, \"height + weight\" = meters+kg)\n"
        ++ " - 4-5: Unclear or context-dependent meaning\n"
        ++ " - 6-7: Makes sense in specific domains\n"
        ++ " - 8-9: Clear, commonly useful quantity\n"
        ++ " - 10: Fundamental/universal quantity\n"
        ++ "Guidelines:\n"
        ++ "- Addition/subtraction: operands must represent the same kind of thing\n"
        ++ "- Multiplication/division: can create meaningful derived quantities\n"
        ++ "- Consider: would this result be useful to calculate in practice?\n"
        ++ "- `toDouble` is just a function that converts any number to a decimal and is semantically unimportant.\n"
        ++ "Examples:\n"
        ++ "toDouble(fare) + toDouble(age) = 2 (adding money to years)\n"
        ++ "toDouble(price) / toDouble(area) = 9 (price per sq ft)\n"
        ++ "toDouble(distance) / toDouble(time) = 10 (speed)\n"
        ++ "toDouble(num_people) * toDouble(rejection_rate) = 9 (expected rejections)\n"
        ++ "toDouble(revenue) - toDouble(costs) = 10 (profit)\n"
        ++ "toDouble(height) + toDouble(weight) = 2 (adding length to mass)\n"
        ++ "Output format: Just the digit, e.g., 2\n"
        ++ "Think very carefully about each but only give me the final answer.\n"
        ++ "Expression: "

score :: Expr a -> Int
score expr =
    let
        genOp =
            defaultGenerateOps
                { modelName = "llama3"
                , prompt = T.pack (instructions ++ prettyPrint expr)
                }
        llamaConfig = Just (defaultOllamaConfig{hostUrl = "http://127.0.0.1:8080"})
        llmResponse = genResponse $ either throw id $ unsafePerformIO (generate genOp llamaConfig)
        s = fst $ either error id $ decimal @Int $ llmResponse
     in
        trace (prettyPrint expr ++ ": " ++ show s) s

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
            let cols = getColumns e1 <> getColumns e2
            guard
                ( e1 /= e2
                    && not
                        ( any
                            (\(l, r) -> l `elem` cols && r `elem` cols)
                            (disallowedCombinations cfg)
                        )
                )
            concat [[e1 + e2, e1 - e2] | score (e1 + e2) > 5]
                ++ concat [[e1 * e2, F.ifThenElse (e2 ./= 0) (e1 / e2) 0] | score (e1 * e2) > 5]

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

generateConditionsOld :: TreeConfig -> DataFrame -> [Expr Bool]
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
        columnConds =
            concatMap
                colConds
                [ (l, r)
                | l <- columnNames df
                , r <- columnNames df
                , not
                    ( any
                        (\(l', r') -> sort [l', r'] == sort [l, r])
                        (disallowedCombinations (synthConfig cfg))
                    )
                ]
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
        calculateComplexity c = complexityPenalty (synthConfig cfg) * fromIntegral (eSize c)
        evalGain :: Expr Bool -> (Double, Int)
        evalGain cond =
            let (t, f) = partitionDataFrame cond df
                n = fromIntegral @Int @Double (nRows df)
                weightT = fromIntegral @Int @Double (nRows t) / n
                weightF = fromIntegral @Int @Double (nRows f) / n
                newImpurity =
                    (weightT * calculateGini @a target t)
                        + (weightF * calculateGini @a target f)
             in ( (initialImpurity - newImpurity)
                    - calculateComplexity cond
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
                conds
        sortedConditions =
            map fst $
                take
                    (expressionPairs cfg)
                    ( filter
                        ( \(c, v) ->
                            ((> negate (calculateComplexity c)) . fst)
                                v
                        )
                        (sortBy (flip compare `on` snd) (map (\c -> (c, evalGain c)) validConds))
                    )
     in
        if null sortedConditions
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
