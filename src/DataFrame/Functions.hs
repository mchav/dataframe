{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE UndecidableInstances #-}

module DataFrame.Functions where

import DataFrame.Internal.Column
import DataFrame.Internal.DataFrame (
    DataFrame (..),
    columnAsDoubleVector,
    unsafeGetColumn,
 )
import DataFrame.Internal.Expression (
    Expr (..),
    NamedExpr,
    UExpr (..),
    eSize,
    interpret,
    replaceExpr,
 )
import DataFrame.Internal.Statistics
import qualified DataFrame.Operations.Statistics as Stats
import DataFrame.Operations.Subset (exclude, select)

import Control.Exception (throw)
import Control.Monad
import qualified Data.Char as Char
import Data.Containers.ListUtils
import Data.Function
import qualified Data.List as L
import qualified Data.Map as M
import Data.Maybe (listToMaybe)
import qualified Data.Set as S
import qualified Data.Text as T
import Data.Time
import Data.Type.Equality
import qualified Data.Vector as V
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU
import qualified DataFrame.Operations.Core as D
import qualified DataFrame.Operations.Transformations as D
import Debug.Trace (trace, traceShow)
import Language.Haskell.TH
import qualified Language.Haskell.TH.Syntax as TH
import Text.Regex.TDFA
import Type.Reflection (typeRep)
import Prelude hiding (maximum, minimum, sum)

name :: (Show a) => Expr a -> T.Text
name (Col n) = n
name other =
    error $
        "You must call `name` on a column reference. Not the expression: " ++ show other

col :: (Columnable a) => T.Text -> Expr a
col = Col

as :: (Columnable a) => Expr a -> T.Text -> NamedExpr
as expr name = (name, Wrap expr)

infixr 0 .=
(.=) :: (Columnable a) => T.Text -> Expr a -> NamedExpr
(.=) = flip as

ifThenElse :: (Columnable a) => Expr Bool -> Expr a -> Expr a -> Expr a
ifThenElse = If

lit :: (Columnable a) => a -> Expr a
lit = Lit

lift :: (Columnable a, Columnable b) => (a -> b) -> Expr a -> Expr b
lift = UnaryOp "udf"

lift2 ::
    (Columnable c, Columnable b, Columnable a) =>
    (c -> b -> a) -> Expr c -> Expr b -> Expr a
lift2 = BinaryOp "udf"

toDouble :: (Columnable a, Real a) => Expr a -> Expr Double
toDouble = UnaryOp "toDouble" realToFrac

div :: (Integral a, Columnable a) => Expr a -> Expr a -> Expr a
div = BinaryOp "div" Prelude.div

mod :: (Integral a, Columnable a) => Expr a -> Expr a -> Expr a
mod = BinaryOp "mod" Prelude.mod

(.==) :: (Columnable a, Eq a) => Expr a -> Expr a -> Expr Bool
(.==) = BinaryOp "eq" (==)

eq :: (Columnable a, Eq a) => Expr a -> Expr a -> Expr Bool
eq = BinaryOp "eq" (==)

(.<) :: (Columnable a, Ord a) => Expr a -> Expr a -> Expr Bool
(.<) = BinaryOp "lt" (<)

lt :: (Columnable a, Ord a) => Expr a -> Expr a -> Expr Bool
lt = BinaryOp "lt" (<)

(.>) :: (Columnable a, Ord a) => Expr a -> Expr a -> Expr Bool
(.>) = BinaryOp "gt" (>)

gt :: (Columnable a, Ord a) => Expr a -> Expr a -> Expr Bool
gt = BinaryOp "gt" (>)

(.<=) :: (Columnable a, Ord a, Eq a) => Expr a -> Expr a -> Expr Bool
(.<=) = BinaryOp "leq" (<=)

leq :: (Columnable a, Ord a, Eq a) => Expr a -> Expr a -> Expr Bool
leq = BinaryOp "leq" (<=)

(.>=) :: (Columnable a, Ord a, Eq a) => Expr a -> Expr a -> Expr Bool
(.>=) = BinaryOp "geq" (>=)

geq :: (Columnable a, Ord a, Eq a) => Expr a -> Expr a -> Expr Bool
geq = BinaryOp "geq" (>=)

and :: Expr Bool -> Expr Bool -> Expr Bool
and = BinaryOp "and" (&&)

(.&&) :: Expr Bool -> Expr Bool -> Expr Bool
(.&&) = BinaryOp "and" (&&)

or :: Expr Bool -> Expr Bool -> Expr Bool
or = BinaryOp "or" (||)

(.||) :: Expr Bool -> Expr Bool -> Expr Bool
(.||) = BinaryOp "or" (||)

not :: Expr Bool -> Expr Bool
not = UnaryOp "not" Prelude.not

count :: (Columnable a) => Expr a -> Expr Int
count expr = AggFold expr "count" 0 (\acc _ -> acc + 1)

collect :: (Columnable a) => Expr a -> Expr [a]
collect expr = AggFold expr "collect" [] (flip (:))

mode :: (Columnable a, Eq a) => Expr a -> Expr a
mode expr =
    AggVector
        expr
        "mode"
        ( fst
            . L.maximumBy (compare `on` snd)
            . M.toList
            . V.foldl' (\m e -> M.insertWith (+) e 1 m) M.empty
        )

minimum :: (Columnable a, Ord a) => Expr a -> Expr a
minimum expr = AggReduce expr "minimum" Prelude.min

maximum :: (Columnable a, Ord a) => Expr a -> Expr a
maximum expr = AggReduce expr "maximum" Prelude.max

sum :: forall a. (Columnable a, Num a, VU.Unbox a) => Expr a -> Expr a
sum expr = AggNumericVector expr "sum" VG.sum

mean :: (Columnable a, Real a, VU.Unbox a) => Expr a -> Expr Double
mean expr = AggNumericVector expr "mean" mean'

variance :: (Columnable a, Real a, VU.Unbox a) => Expr a -> Expr Double
variance expr = AggNumericVector expr "variance" variance'

median :: (Columnable a, Real a, VU.Unbox a) => Expr a -> Expr Double
median expr = AggNumericVector expr "median" median'

percentile :: Int -> Expr Double -> Expr Double
percentile n expr =
    AggNumericVector
        expr
        (T.pack $ "percentile " ++ show n)
        (percentile' n)

stddev :: (Columnable a, Real a, VU.Unbox a) => Expr a -> Expr Double
stddev expr = AggNumericVector expr "stddev" (sqrt . variance')

zScore :: Expr Double -> Expr Double
zScore c = (c - mean c) / stddev c

pow :: (Columnable a, Num a) => Int -> Expr a -> Expr a
pow 0 _ = Lit 1
pow 1 expr = expr
pow i expr = UnaryOp ("pow " <> T.pack (show i)) (^ i) expr

relu :: (Columnable a, Num a) => Expr a -> Expr a
relu = UnaryOp "relu" (Prelude.max 0)

min :: (Columnable a, Ord a) => Expr a -> Expr a -> Expr a
min = BinaryOp "min" Prelude.min

max :: (Columnable a, Ord a) => Expr a -> Expr a -> Expr a
max = BinaryOp "max" Prelude.max

reduce ::
    forall a b.
    (Columnable a, Columnable b) => Expr b -> a -> (a -> b -> a) -> Expr a
reduce expr = AggFold expr "foldUdf"

whenPresent ::
    forall a b.
    (Columnable a, Columnable b) => (a -> b) -> Expr (Maybe a) -> Expr (Maybe b)
whenPresent f = lift (fmap f)

whenBothPresent ::
    forall a b c.
    (Columnable a, Columnable b, Columnable c) =>
    (a -> b -> c) -> Expr (Maybe a) -> Expr (Maybe b) -> Expr (Maybe c)
whenBothPresent f = lift2 (\l r -> f <$> l <*> r)

recode ::
    forall a b.
    (Columnable a, Columnable b) => [(a, b)] -> Expr a -> Expr (Maybe b)
recode mapping = UnaryOp (T.pack ("recode " ++ show mapping)) (`lookup` mapping)

firstOrNothing :: (Columnable a) => Expr [a] -> Expr (Maybe a)
firstOrNothing = lift listToMaybe

lastOrNothing :: (Columnable a) => Expr [a] -> Expr (Maybe a)
lastOrNothing = lift (listToMaybe . reverse)

splitOn :: T.Text -> Expr T.Text -> Expr [T.Text]
splitOn delim = lift (T.splitOn delim)

match :: T.Text -> Expr T.Text -> Expr (Maybe T.Text)
match regex = lift ((\r -> if T.null r then Nothing else Just r) . (=~ regex))

matchAll :: T.Text -> Expr T.Text -> Expr [T.Text]
matchAll regex = lift (getAllTextMatches . (=~ regex))

parseDate :: T.Text -> Expr T.Text -> Expr (Maybe Day)
parseDate format = lift (parseTimeM True defaultTimeLocale (T.unpack format) . T.unpack)

daysBetween :: Expr Day -> Expr Day -> Expr Int
daysBetween d1 d2 = lift fromIntegral (lift2 diffDays d1 d2)

bind ::
    forall a m.
    (Columnable a, Columnable (m a), Monad m, Columnable b, Columnable (m b)) =>
    (a -> m b) -> Expr (m a) -> Expr (m b)
bind f = lift (>>= f)

generateConditions ::
    TypedColumn Double -> [Expr Bool] -> [Expr Double] -> DataFrame -> [Expr Bool]
generateConditions labels conds ps df =
    let
        newConds =
            [ p .<= q
            | p <- ps
            , q <- ps
            , p /= q
            ]
                ++ [ DataFrame.Functions.not p
                   | p <- conds
                   ]
        expandedConds =
            conds
                ++ newConds
                ++ [p .&& q | p <- newConds, q <- conds, p /= q]
                ++ [p .|| q | p <- newConds, q <- conds, p /= q]
     in
        pickTopNBool df labels (deduplicate df expandedConds)

generatePrograms ::
    Bool ->
    [Expr Bool] ->
    [Expr Double] ->
    [Expr Double] ->
    [Expr Double] ->
    [Expr Double]
generatePrograms _ _ vars' constants [] = vars' ++ constants
generatePrograms includeConds conds vars constants ps =
    let
        existingPrograms = ps ++ vars ++ constants
     in
        existingPrograms
            ++ [ transform p
               | p <- ps ++ vars
               , transform <-
                    [ sqrt
                    , abs
                    , log . (+ Lit 1)
                    , exp
                    , sin
                    , cos
                    , relu
                    , signum
                    ]
               ]
            ++ [ pow i p
               | p <- existingPrograms
               , i <- [2 .. 6]
               ]
            ++ [ p + q
               | (i, p) <- zip [0 ..] existingPrograms
               , (j, q) <- zip [0 ..] existingPrograms
               , Prelude.not (isLiteral p && isLiteral q)
               , i >= j
               ]
            ++ ( if includeConds
                    then
                        [ DataFrame.Functions.min p q
                        | (i, p) <- zip [0 ..] existingPrograms
                        , (j, q) <- zip [0 ..] existingPrograms
                        , Prelude.not (isLiteral p && isLiteral q)
                        , p /= q
                        , i > j
                        ]
                            ++ [ DataFrame.Functions.max p q
                               | (i, p) <- zip [0 ..] existingPrograms
                               , (j, q) <- zip [0 ..] existingPrograms
                               , Prelude.not (isLiteral p && isLiteral q)
                               , p /= q
                               , i > j
                               ]
                            ++ [ ifThenElse cond r s
                               | cond <- conds
                               , r <- existingPrograms
                               , s <- existingPrograms
                               , r /= s
                               ]
                    else []
               )
            ++ [ p - q
               | (i, p) <- zip [0 ..] existingPrograms
               , (j, q) <- zip [0 ..] existingPrograms
               , Prelude.not (isLiteral p && isLiteral q)
               , i /= j
               ]
            ++ [ p * q
               | (i, p) <- zip [0 ..] existingPrograms
               , (j, q) <- zip [0 ..] existingPrograms
               , Prelude.not (isLiteral p && isLiteral q)
               , i >= j
               ]
            ++ [ p / q
               | p <- existingPrograms
               , q <- existingPrograms
               , Prelude.not (isLiteral p && isLiteral q)
               , p /= q
               ]

isLiteral :: Expr a -> Bool
isLiteral (Lit _) = True
isLiteral _ = False

deduplicate ::
    forall a.
    (Columnable a) =>
    DataFrame ->
    [Expr a] ->
    [(Expr a, TypedColumn a)]
deduplicate df = go S.empty . nubOrd . L.sortBy (\e1 e2 -> compare (eSize e1) (eSize e2))
  where
    go _ [] = []
    go seen (x : xs)
        | hasInvalid = go seen xs
        | S.member res seen = go seen xs
        | otherwise = (x, res) : go (S.insert res seen) xs
      where
        res = case interpret @a df x of
            Left e -> throw e
            Right v -> v
        hasInvalid = case res of
            (TColumn (UnboxedColumn (col :: VU.Vector b))) -> case testEquality (typeRep @Double) (typeRep @b) of
                Just Refl -> VU.any (\n -> isNaN n || isInfinite n) col
                Nothing -> False
            _ -> False

-- | Checks if two programs generate the same outputs given all the same inputs.
equivalent :: DataFrame -> Expr Double -> Expr Double -> Bool
equivalent df p1 p2 = case (==) <$> interpret df p1 <*> interpret df p2 of
    Left e -> throw e
    Right v -> v

synthesizeFeatureExpr ::
    -- | Target expression
    T.Text ->
    BeamConfig ->
    DataFrame ->
    Either String (Expr Double)
synthesizeFeatureExpr target cfg df =
    let
        df' = exclude [target] df
        t = case interpret df (Col target) of
            Left e -> throw e
            Right v -> v
     in
        case beamSearch
            df'
            cfg
            t
            (percentiles df')
            []
            [] of
            Nothing -> Left "No programs found"
            Just p -> Right p

f1FromBinary :: VU.Vector Double -> VU.Vector Double -> Maybe Double
f1FromBinary trues preds =
    let (!tp, !fp, !fn) =
            VU.foldl' step (0 :: Int, 0 :: Int, 0 :: Int) $
                VU.zip (VU.map (> 0) preds) (VU.map (> 0) trues)
     in f1FromCounts tp fp fn
  where
    step (!tp, !fp, !fn) (!p, !t) =
        case (p, t) of
            (True, True) -> (tp + 1, fp, fn)
            (True, False) -> (tp, fp + 1, fn)
            (False, True) -> (tp, fp, fn + 1)
            (False, False) -> (tp, fp, fn)

f1FromCounts :: Int -> Int -> Int -> Maybe Double
f1FromCounts tp fp fn =
    let tp' = fromIntegral tp
        fp' = fromIntegral fp
        fn' = fromIntegral fn
        precision = if tp' + fp' == 0 then 0 else tp' / (tp' + fp')
        recall = if tp' + fn' == 0 then 0 else tp' / (tp' + fn')
     in if precision + recall == 0
            then Nothing
            else Just (2 * precision * recall / (precision + recall))

fitClassifier ::
    -- | Target expression
    T.Text ->
    -- | Depth of search (Roughly, how many terms in the final expression)
    Int ->
    -- | Beam size - the number of candidate expressions to consider at a time.
    Int ->
    DataFrame ->
    Either String (Expr Int)
fitClassifier target d b df =
    let
        df' = exclude [target] df
        t = case interpret df (Col target) of
            Left e -> throw e
            Right v -> v
     in
        case beamSearch
            df'
            (BeamConfig d b F1 True)
            t
            (percentiles df' ++ [lit 1, lit 0, lit (-1)])
            []
            [] of
            Nothing -> Left "No programs found"
            Just p -> Right (ifThenElse (p .> 0) 1 0)

percentiles :: DataFrame -> [Expr Double]
percentiles df =
    let
        doubleColumns = map (either throw id . (`columnAsDoubleVector` df)) (D.columnNames df)
     in
        concatMap
            (\c -> map (lit . roundTo2SigDigits . (`percentile'` c)) [1, 25, 75, 99])
            doubleColumns
            ++ map (lit . roundTo2SigDigits . variance') doubleColumns
            ++ map (lit . roundTo2SigDigits . sqrt . variance') doubleColumns

roundToSigDigits :: Int -> Double -> Double
roundToSigDigits n x
    | x == 0 = 0
    | otherwise =
        let magnitude = floor (logBase 10 (abs x))
            scale = 10 ** fromIntegral (n - 1 - magnitude)
         in fromIntegral (round (x * scale)) / scale

roundTo2SigDigits :: Double -> Double
roundTo2SigDigits = roundToSigDigits 2

fitRegression ::
    -- | Target expression
    T.Text ->
    -- | Depth of search (Roughly, how many terms in the final expression)
    Int ->
    -- | Beam size - the number of candidate expressions to consider at a time.
    Int ->
    DataFrame ->
    Either String (Expr Double)
fitRegression target d b df =
    let
        df' = exclude [target] df
        targetMean = Stats.mean (Col @Double target) df
        t = case interpret df (Col target) of
            Left e -> throw e
            Right v -> v
     in
        case beamSearch
            df'
            ( BeamConfig
                d
                b
                MutualInformation
                False
            )
            t
            (percentiles df')
            []
            [] of
            Nothing -> Left "No programs found"
            Just p ->
                trace (show p) $
                    let
                     in case beamSearch
                            ( D.derive "_generated_regression_feature_" p df
                                & select ["_generated_regression_feature_"]
                            )
                            (BeamConfig d b MeanSquaredError False)
                            t
                            (percentiles df' ++ [lit targetMean, lit 10])
                            []
                            [Col "_generated_regression_feature_"] of
                            Nothing -> Left "Could not find coefficients"
                            Just p' -> Right (replaceExpr p (Col @Double "_generated_regression_feature_") p')

data LossFunction
    = PearsonCorrelation
    | MutualInformation
    | MeanSquaredError
    | F1

getLossFunction ::
    LossFunction -> (VU.Vector Double -> VU.Vector Double -> Maybe Double)
getLossFunction f = case f of
    MutualInformation ->
        ( \l r ->
            mutualInformationBinned
                (Prelude.max 10 (ceiling (sqrt (fromIntegral (VU.length l)))))
                l
                r
        )
    PearsonCorrelation -> (\l r -> (^ 2) <$> correlation' l r)
    MeanSquaredError -> (\l r -> fmap negate (meanSquaredError l r))
    F1 -> f1FromBinary

data BeamConfig = BeamConfig
    { searchDepth :: Int
    , beamLength :: Int
    , lossFunction :: LossFunction
    , includeConditionals :: Bool
    }

defaultBeamConfig :: BeamConfig
defaultBeamConfig = BeamConfig 2 100 PearsonCorrelation False

beamSearch ::
    DataFrame ->
    -- | Parameters of the beam search.
    BeamConfig ->
    -- | Examples
    TypedColumn Double ->
    -- | Constants
    [Expr Double] ->
    -- | Conditions
    [Expr Bool] ->
    -- | Programs
    [Expr Double] ->
    Maybe (Expr Double)
beamSearch df cfg outputs constants conds programs
    | searchDepth cfg == 0 = case ps of
        [] -> Nothing
        (x : _) -> Just x
    | otherwise =
        beamSearch
            df
            (cfg{searchDepth = searchDepth cfg - 1})
            outputs
            constants
            conditions
            (generatePrograms (includeConditionals cfg) conditions vars constants ps)
  where
    vars = map col names
    conditions = generateConditions outputs conds (vars ++ constants ++ ps) df
    ps = pickTopN df outputs cfg $ deduplicate df programs
    names = (map fst . L.sortBy (compare `on` snd) . M.toList . columnIndices) df

pickTopN ::
    DataFrame ->
    TypedColumn Double ->
    BeamConfig ->
    [(Expr Double, TypedColumn a)] ->
    [Expr Double]
pickTopN _ _ _ [] = []
pickTopN df (TColumn col) cfg ps =
    let
        l = case toVector @Double @VU.Vector col of
            Left e -> throw e
            Right v -> v
        ordered =
            Prelude.take
                (beamLength cfg)
                ( map fst $
                    L.sortBy
                        ( \(_, c2) (_, c1) ->
                            if maybe False isInfinite c1
                                || maybe False isInfinite c2
                                || maybe False isNaN c1
                                || maybe False isNaN c2
                                then LT
                                else compare c1 c2
                        )
                        ( map
                            (\(e, res) -> (e, getLossFunction (lossFunction cfg) l (asDoubleVector res)))
                            ps
                        )
                )
        asDoubleVector c =
            let
                (TColumn col') = c
             in
                case toVector @Double @VU.Vector col' of
                    Left e -> throw e
                    Right v -> VU.convert v
        interpretDoubleVector e =
            let
                (TColumn col') = case interpret df e of
                    Left e -> throw e
                    Right v -> v
             in
                case toVector @Double @VU.Vector col' of
                    Left e -> throw e
                    Right v -> VU.convert v
     in
        trace
            ( "Best loss: "
                ++ show
                    ( getLossFunction (lossFunction cfg) l . interpretDoubleVector
                        <$> listToMaybe ordered
                    )
                ++ " "
                ++ (if null ordered then "empty" else show (listToMaybe ordered))
            )
            ordered

pickTopNBool ::
    DataFrame ->
    TypedColumn Double ->
    [(Expr Bool, TypedColumn Bool)] ->
    [Expr Bool]
pickTopNBool _ _ [] = []
pickTopNBool df (TColumn col) ps =
    let
        l = case toVector @Double @VU.Vector col of
            Left e -> throw e
            Right v -> v
        ordered =
            Prelude.take
                10
                ( map fst $
                    L.sortBy
                        ( \(_, c2) (_, c1) ->
                            if maybe False isInfinite c1
                                || maybe False isInfinite c2
                                || maybe False isNaN c1
                                || maybe False isNaN c2
                                then LT
                                else compare c1 c2
                        )
                        ( map
                            (\(e, res) -> (e, getLossFunction MutualInformation l (asDoubleVector res)))
                            ps
                        )
                )
        asDoubleVector c =
            let
                (TColumn col') = c
             in
                case toVector @Bool @VU.Vector col' of
                    Left e -> throw e
                    Right v -> VU.map (fromIntegral @Int @Double . fromEnum) v
     in
        ordered

satisfiesExamples :: DataFrame -> TypedColumn Double -> Expr Double -> Bool
satisfiesExamples df col expr =
    let
        result = case interpret df expr of
            Left e -> throw e
            Right v -> v
     in
        result == col

-- See Section 2.4 of the Haskell Report https://www.haskell.org/definition/haskell2010.pdf
isReservedId :: T.Text -> Bool
isReservedId t = case t of
    "case" -> True
    "class" -> True
    "data" -> True
    "default" -> True
    "deriving" -> True
    "do" -> True
    "else" -> True
    "foreign" -> True
    "if" -> True
    "import" -> True
    "in" -> True
    "infix" -> True
    "infixl" -> True
    "infixr" -> True
    "instance" -> True
    "let" -> True
    "module" -> True
    "newtype" -> True
    "of" -> True
    "then" -> True
    "type" -> True
    "where" -> True
    _ -> False

isVarId :: T.Text -> Bool
isVarId t = case T.uncons t of
    -- We might want to check  c == '_' || Char.isLower c
    -- since the haskell report considers '_' a lowercase character
    -- However, to prevent an edge case where a user may have a
    -- "Name" and an "_Name_" in the same scope, wherein we'd end up
    -- with duplicate "_Name_"s, we eschew the check for '_' here.
    Just (c, _) -> Char.isLower c && Char.isAlpha c
    Nothing -> False

isHaskellIdentifier :: T.Text -> Bool
isHaskellIdentifier t = Prelude.not (isVarId t) || isReservedId t

sanitize :: T.Text -> T.Text
sanitize t
    | isValid = t
    | isHaskellIdentifier t' = "_" <> t' <> "_"
    | otherwise = t'
  where
    isValid =
        Prelude.not (isHaskellIdentifier t)
            && isVarId t
            && T.all Char.isAlphaNum t
    t' = T.map replaceInvalidCharacters . T.filter (Prelude.not . parentheses) $ t
    replaceInvalidCharacters c
        | Char.isUpper c = Char.toLower c
        | Char.isSpace c = '_'
        | Char.isPunctuation c = '_' -- '-' will also become a '_'
        | Char.isSymbol c = '_'
        | Char.isAlphaNum c = c -- Blanket condition
        | otherwise = '_' -- If we're unsure we'll default to an underscore
    parentheses c = case c of
        '(' -> True
        ')' -> True
        '{' -> True
        '}' -> True
        '[' -> True
        ']' -> True
        _ -> False

typeFromString :: [String] -> Q Type
typeFromString [] = fail "No type specified"
typeFromString [t] = do
    maybeType <- lookupTypeName t
    case maybeType of
        Just name -> return (ConT name)
        Nothing -> fail $ "Unsupported type: " ++ t
typeFromString [tycon, t1] = do
    outer <- typeFromString [tycon]
    inner <- typeFromString [t1]
    return (AppT outer inner)
typeFromString [tycon, t1, t2] = do
    outer <- typeFromString [tycon]
    lhs <- typeFromString [t1]
    rhs <- typeFromString [t2]
    return (AppT (AppT outer lhs) rhs)
typeFromString s = fail $ "Unsupported types: " ++ unwords s

declareColumns :: DataFrame -> DecsQ
declareColumns df =
    let
        names = (map fst . L.sortBy (compare `on` snd) . M.toList . columnIndices) df
        types = map (columnTypeString . (`unsafeGetColumn` df)) names
        specs = zipWith (\name type_ -> (name, sanitize name, type_)) names types
     in
        fmap concat $ forM specs $ \(raw, nm, tyStr) -> do
            ty <- typeFromString (words tyStr)
            traceShow (nm <> " :: Expr " <> T.pack tyStr) (pure ())
            let n = mkName (T.unpack nm)
            sig <- sigD n [t|Expr $(pure ty)|]
            val <- valD (varP n) (normalB [|col $(TH.lift raw)|]) []
            pure [sig, val]
