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
import DataFrame.Internal.DataFrame (DataFrame (..), unsafeGetColumn)
import DataFrame.Internal.Expression (
    Expr (..),
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
import Data.Function
import qualified Data.List as L
import qualified Data.Map as M
import Data.Maybe (fromMaybe, listToMaybe)
import qualified Data.Set as S
import qualified Data.Text as T
import Data.Type.Equality
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU
import qualified DataFrame.Operations.Transformations as D
import Debug.Trace (trace, traceShow)
import Language.Haskell.TH
import qualified Language.Haskell.TH.Syntax as TH
import Type.Reflection (typeRep)
import Prelude hiding (maximum, minimum, sum)

name :: (Show a) => Expr a -> T.Text
name (Col n) = n
name other =
    error $
        "You must call `name` on a column reference. Not the expression: " ++ show other

col :: (Columnable a) => T.Text -> Expr a
col = Col

as :: (Columnable a) => Expr a -> T.Text -> (T.Text, UExpr)
as expr name = (name, Wrap expr)

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

(==) :: (Columnable a, Eq a) => Expr a -> Expr a -> Expr Bool
(==) = BinaryOp "eq" (Prelude.==)

eq :: (Columnable a, Eq a) => Expr a -> Expr a -> Expr Bool
eq = BinaryOp "eq" (Prelude.==)

(<) :: (Columnable a, Ord a) => Expr a -> Expr a -> Expr Bool
(<) = BinaryOp "lt" (Prelude.<)

lt :: (Columnable a, Ord a) => Expr a -> Expr a -> Expr Bool
lt = BinaryOp "lt" (Prelude.<)

(>) :: (Columnable a, Ord a) => Expr a -> Expr a -> Expr Bool
(>) = BinaryOp "gt" (Prelude.>)

gt :: (Columnable a, Ord a) => Expr a -> Expr a -> Expr Bool
gt = BinaryOp "gt" (Prelude.>)

(<=) :: (Columnable a, Ord a, Eq a) => Expr a -> Expr a -> Expr Bool
(<=) = BinaryOp "leq" (Prelude.<=)

leq :: (Columnable a, Ord a, Eq a) => Expr a -> Expr a -> Expr Bool
leq = BinaryOp "leq" (Prelude.<=)

(>=) :: (Columnable a, Ord a, Eq a) => Expr a -> Expr a -> Expr Bool
(>=) = BinaryOp "geq" (Prelude.>=)

geq :: (Columnable a, Ord a, Eq a) => Expr a -> Expr a -> Expr Bool
geq = BinaryOp "geq" (Prelude.>=)

and :: Expr Bool -> Expr Bool -> Expr Bool
and = BinaryOp "and" (&&)

or :: Expr Bool -> Expr Bool -> Expr Bool
or = BinaryOp "or" (||)

not :: Expr Bool -> Expr Bool
not = UnaryOp "not" Prelude.not

count :: (Columnable a) => Expr a -> Expr Int
count expr = AggFold expr "foldUdf" 0 (\acc _ -> acc + 1)

minimum :: (Columnable a, Ord a) => Expr a -> Expr a
minimum expr = AggReduce expr "minimum" Prelude.min

maximum :: (Columnable a, Ord a) => Expr a -> Expr a
maximum expr = AggReduce expr "maximum" Prelude.max

sum :: forall a. (Columnable a, Num a, VU.Unbox a) => Expr a -> Expr a
sum expr = AggNumericVector expr "sum" VG.sum

mean :: (Columnable a, Real a, VU.Unbox a) => Expr a -> Expr Double
mean expr = AggNumericVector expr "mean" mean'

median :: Expr Double -> Expr Double
median expr = AggNumericVector expr "mean" median'

percentile :: Int -> Expr Double -> Expr Double
percentile n expr =
    AggNumericVector
        expr
        (T.pack $ "percentile " ++ show n)
        ((VU.! 0) . quantiles' (VU.fromList [n]) 100)

stddev :: (Columnable a, Real a, VU.Unbox a) => Expr a -> Expr Double
stddev expr = AggNumericVector expr "stddev" (sqrt . variance')

zScore :: Expr Double -> Expr Double
zScore c = (c - mean c) / stddev c

pow :: (Columnable a, Num a) => Int -> Expr a -> Expr a
pow i = UnaryOp ("pow " <> T.pack (show i)) (^ i)

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

generatePrograms :: [Expr Double] -> [Expr Double] -> [Expr Double]
generatePrograms vars [] =
    vars
        ++ [ transform p
           | p <- vars
           , transform <-
                [ zScore
                , abs
                , sqrt
                , log . (+ Lit 1)
                , exp
                , mean
                , median
                , stddev
                , sin
                , cos
                , relu
                , signum
                ]
           ]
        ++ [ pow i p
           | p <- vars
           , i <- [2 .. 6]
           ]
        ++ [ p + q
           | (i, p) <- zip [0 ..] vars
           , (j, q) <- zip [0 ..] vars
           , i Prelude.> j
           ]
        ++ [ DataFrame.Functions.min p q
           | (i, p) <- zip [0 ..] vars
           , (j, q) <- zip [0 ..] vars
           , i Prelude.> j
           ]
        ++ [ DataFrame.Functions.max p q
           | (i, p) <- zip [0 ..] vars
           , (j, q) <- zip [0 ..] vars
           , i Prelude.>= j
           ]
        ++ [ p - q
           | (i, p) <- zip [0 ..] vars
           , (j, q) <- zip [0 ..] vars
           , i /= j
           ]
        ++ [ p * q
           | (i, p) <- zip [0 ..] vars
           , (j, q) <- zip [0 ..] vars
           , i Prelude.>= j
           ]
        ++ [ p / q
           | (i, p) <- zip [0 ..] vars
           , (j, q) <- zip [0 ..] vars
           , i /= j
           ]
generatePrograms vars ps =
    let
        existingPrograms = vars ++ ps
     in
        existingPrograms
            ++ [ transform p
               | p <- existingPrograms
               , transform <-
                    [ zScore
                    , sqrt
                    , abs
                    , log . (+ Lit 1)
                    , exp
                    , mean
                    , median
                    , stddev
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
               , i Prelude.>= j
               ]
            ++ [ DataFrame.Functions.min p q
               | (i, p) <- zip [0 ..] existingPrograms
               , (j, q) <- zip [0 ..] existingPrograms
               , i Prelude.> j
               ]
            ++ [ DataFrame.Functions.max p q
               | (i, p) <- zip [0 ..] existingPrograms
               , (j, q) <- zip [0 ..] existingPrograms
               , i Prelude.> j
               ]
            ++ [ ifThenElse (p DataFrame.Functions.>= percentile n p) p q
               | (i, p) <- zip [0 ..] existingPrograms
               , (j, q) <- zip [0 ..] existingPrograms
               , i /= j
               , n <- [1, 25, 50, 75, 99]
               ]
            ++ [ ifThenElse (p DataFrame.Functions.>= percentile n p) p q
               | p <- existingPrograms
               , q <- [lit 1, lit 0, lit (-1)]
               , n <- [1, 25, 50, 75, 99]
               ]
            ++ [ p - q
               | (i, p) <- zip [0 ..] existingPrograms
               , (j, q) <- zip [0 ..] existingPrograms
               , i /= j
               ]
            ++ [ p * q
               | (i, p) <- zip [0 ..] existingPrograms
               , (j, q) <- zip [0 ..] existingPrograms
               , i Prelude.>= j
               ]
            ++ [ p / q
               | (i, p) <- zip [0 ..] existingPrograms
               , (j, q) <- zip [0 ..] existingPrograms
               , i /= j
               ]

-- | Deduplicate programs pick the least smallest one by size.
deduplicate ::
    DataFrame ->
    [Expr Double] ->
    [(Expr Double, TypedColumn Double)]
deduplicate df = go S.empty . L.sortBy (\e1 e2 -> compare (eSize e1) (eSize e2))
  where
    go _ [] = []
    go seen (x : xs)
        | hasInvalid = go seen xs
        | S.member res seen = go seen xs
        | otherwise = (x, res) : go (S.insert res seen) xs
      where
        res = case interpret df x of
            Left e -> throw e
            Right v -> v
        hasInvalid = case res of
            (TColumn (UnboxedColumn (col :: VU.Vector a))) -> case testEquality (typeRep @Double) (typeRep @a) of
                Just Refl -> VU.any (\n -> isNaN n || isInfinite n) col
                Nothing -> False
            _ -> False

-- | Checks if two programs generate the same outputs given all the same inputs.
equivalent :: DataFrame -> Expr Double -> Expr Double -> Bool
equivalent df p1 p2 = case (Prelude.==) <$> interpret df p1 <*> interpret df p2 of
    Left e -> throw e
    Right v -> v

synthesizeFeatureExpr ::
    -- | Target expression
    T.Text ->
    -- | Depth of search (Roughly, how many terms in the final expression)
    Int ->
    -- | Beam size - the number of candidate expressions to consider at a time.
    Int ->
    DataFrame ->
    Either String (Expr Double)
synthesizeFeatureExpr target d b df =
    let
        df' = exclude [target] df
        t = case interpret df (Col target) of
            Left e -> throw e
            Right v -> v
     in
        case beamSearch
            df'
            (BeamConfig d b (\l r -> (^ 2) <$> correlation' l r))
            t
            [] of
            Nothing -> Left "No programs found"
            Just p -> Right p

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
        targetMean = fromMaybe 0 $ Stats.mean target df
        t = case interpret df (Col target) of
            Left e -> throw e
            Right v -> v
     in
        case beamSearch
            df'
            ( BeamConfig
                d
                b
                ( \l r ->
                    mutualInformationBinned
                        (Prelude.max 10 (ceiling (sqrt (fromIntegral (VU.length l)))))
                        l
                        r
                )
            )
            t
            [] of
            Nothing -> Left "No programs found"
            Just p ->
                trace (show p) $
                    let
                     in case beamSearch
                            ( D.derive "_generated_regression_feature_" p df
                                & select ["_generated_regression_feature_"]
                            )
                            (BeamConfig d b (\l r -> fmap negate (meanSquaredError l r)))
                            t
                            [Col "_generated_regression_feature_", lit targetMean, lit 10] of
                            Nothing -> Left "Could not find coefficients"
                            Just p' -> Right (replaceExpr p (Col @Double "_generated_regression_feature_") p')

data BeamConfig = BeamConfig
    { searchDepth :: Int
    , beamLength :: Int
    , rankingFunction :: VU.Vector Double -> VU.Vector Double -> Maybe Double
    }

beamSearch ::
    DataFrame ->
    -- | Parameters of the beam search.
    BeamConfig ->
    -- | Examples
    TypedColumn Double ->
    -- | Programs
    [Expr Double] ->
    Maybe (Expr Double)
beamSearch df cfg outputs programs
    | searchDepth cfg Prelude.== 0 = case ps of
        [] -> Nothing
        (x : _) -> Just x
    | otherwise =
        beamSearch
            df
            (cfg{searchDepth = searchDepth cfg - 1})
            outputs
            (generatePrograms (map col names) ps)
  where
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
                        (map (\(e, res) -> (e, rankingFunction cfg l (asDoubleVector res))) ps)
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
                    (rankingFunction cfg l <$> (interpretDoubleVector <$> (listToMaybe ordered)))
                ++ " "
                ++ (if null ordered then "empty" else show (listToMaybe ordered))
            )
            ordered

satisfiesExamples :: DataFrame -> TypedColumn Double -> Expr Double -> Bool
satisfiesExamples df col expr =
    let
        result = case interpret df expr of
            Left e -> throw e
            Right v -> v
     in
        result Prelude.== col

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
typeFromString s = fail $ "Unsupported type: " ++ unwords s

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
