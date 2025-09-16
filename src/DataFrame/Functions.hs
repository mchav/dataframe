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
import DataFrame.Internal.Expression (Expr (..), UExpr (..), eSize, interpret)
import DataFrame.Internal.Statistics
import DataFrame.Operations.Subset (exclude)

import Control.Monad
import qualified Data.Char as Char
import Data.Function
import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU
import Debug.Trace ( traceShow )
import Language.Haskell.TH
import qualified Language.Haskell.TH.Syntax as TH
import Type.Reflection (typeRep)
import Prelude hiding (sum, minimum, maximum)
import Data.Type.Equality

name :: (Show a) => Expr a -> T.Text
name (Col n) = n
name other = error $ "You must call `name` on a column reference. Not the expression: " ++ show other

col :: (Columnable a) => T.Text -> Expr a
col = Col

as :: (Columnable a) => Expr a -> T.Text -> (T.Text, UExpr)
as expr name = (name, Wrap expr)

lit :: (Columnable a) => a -> Expr a
lit = Lit

lift :: (Columnable a, Columnable b) => (a -> b) -> Expr a -> Expr b
lift = UnaryOp "udf"

lift2 :: (Columnable c, Columnable b, Columnable a) => (c -> b -> a) -> Expr c -> Expr b -> Expr a
lift2 = BinaryOp "udf"

(==) :: (Columnable a, Eq a) => Expr a -> Expr a -> Expr Bool
(==) = BinaryOp "eq" (Prelude.==)

(<) :: (Columnable a, Ord a) => Expr a -> Expr a -> Expr Bool
(<) = BinaryOp "lt" (Prelude.<)

(>) :: (Columnable a, Ord a) => Expr a -> Expr a -> Expr Bool
(>) = BinaryOp "gt" (Prelude.>)

(<=) :: (Columnable a, Ord a, Eq a) => Expr a -> Expr a -> Expr Bool
(<=) = BinaryOp "leq" (Prelude.<=)

(>=) :: (Columnable a, Ord a, Eq a) => Expr a -> Expr a -> Expr Bool
(>=) = BinaryOp "geq" (Prelude.>=)

and :: Expr Bool -> Expr Bool -> Expr Bool
and = BinaryOp "and" (&&)

or :: Expr Bool -> Expr Bool -> Expr Bool
or = BinaryOp "or" (||)

not :: Expr Bool -> Expr Bool
not = UnaryOp "not" Prelude.not

count :: (Columnable a) => Expr a -> Expr Int
count expr = AggFold expr "foldUdf" 0 (\acc _ -> acc + 1)

minimum :: (Columnable a, Ord a) => Expr a -> Expr a
minimum expr = AggReduce expr "minimum" min

maximum :: (Columnable a, Ord a) => Expr a -> Expr a
maximum expr = AggReduce expr "maximum" max

sum :: forall a. (Columnable a, Num a, VU.Unbox a) => Expr a -> Expr a
sum expr = AggNumericVector expr "sum" VG.sum

mean :: (Columnable a, Real a, VU.Unbox a) => Expr a -> Expr Double
mean expr = AggNumericVector expr "mean" mean'

standardDeviation :: (Columnable a, Real a, VU.Unbox a) => Expr a -> Expr Double
standardDeviation expr = AggNumericVector expr "stddev" (sqrt . variance')

zScore :: Expr Double -> Expr Double
zScore c = (c - mean c) / standardDeviation c

reduce :: forall a b. (Columnable a, Columnable b) => Expr b -> a -> (a -> b -> a) -> Expr a
reduce expr = AggFold expr "foldUdf"

generatePrograms :: [Expr Double] -> [Expr Double] -> [Expr Double]
generatePrograms vars existingPrograms =
    existingPrograms ++
    [ transform p
    | p <- existingPrograms
    , transform <- [zScore, abs, log . (+ Lit 1), exp]
    ] ++
    [ p + q
    | (i, p) <- zip [0..] existingPrograms
    , (j, q) <- zip [0..] existingPrograms
    , i Prelude.>= j
    ] ++
    [ p - q
    | p <- existingPrograms
    , q <- existingPrograms
    ] ++
    [ p * q
    | (i, p) <- zip [0..] existingPrograms
    , (j, q) <- zip [0..] existingPrograms
    , i Prelude.>= j
    ] ++
    [ p / q
    | p <- existingPrograms
    , q <- existingPrograms
    ] ++
    [ t p
    | v <- vars
    , p <- existingPrograms
    , t <- transformsWithVar v
    ]
  where
    transformsWithVar v =
        [ (v +)
        , (* v)
        , (/ v)
        , (v /)
        , \v' -> v' - v
        , (v -)
        ]

-- | Deduplicate programs pick the least smallest one by size.
deduplicate :: DataFrame
            -> [Expr Double]
            -> [Expr Double]
deduplicate df = go S.empty . L.sortBy (\e1 e2 -> compare (eSize e1) (eSize e2))
  where
    go _ [] = []
    go seen (x : xs)
        | hasNaN = go seen xs
        | S.member res seen = go seen xs
        | otherwise = x : go (S.insert res seen) xs
            where
                res = interpret df x
                hasNaN = case res of
                    (TColumn (UnboxedColumn (col :: VU.Vector a))) -> case testEquality (typeRep @Double) (typeRep @a) of
                                    Just Refl -> VU.any isNaN col
                                    Nothing -> False
                    _ -> False

-- | Checks if two programs generate the same outputs given all the same inputs.
equivalent :: DataFrame -> Expr Double -> Expr Double -> Bool
equivalent df p1 p2 = interpret df p1 Prelude.== interpret df p2

search :: T.Text -> Int -> DataFrame -> Either String (Expr Double)
search target d df = let
        df' = exclude [target] df
        names = (map fst . L.sortBy (compare `on` snd) . M.toList . columnIndices) df'
        variables = map col names
    in case searchStream df' (interpret df (Col target)) variables d of
            Nothing -> Left "No programs found"
            Just p -> Right p

searchStream ::
    DataFrame ->
    -- | Examples
    TypedColumn Double ->
    -- | Programs
    [Expr Double] ->
    -- | Search depth
    Int ->
    Maybe (Expr Double)
searchStream df outputs programs d
    | d Prelude.== 0 = case ps of
        []    -> Nothing
        (x:_) -> Just x
    | otherwise =
        case findFirst ps of
            Just p -> Just p
            Nothing -> searchStream df outputs (generatePrograms (map col names) ps) (d - 1)
  where
    ps = pickTopN df outputs 100 $ deduplicate df programs
    names = (map fst . L.sortBy (compare `on` snd) . M.toList . columnIndices) df
    findFirst [] = Nothing
    findFirst (p : ps')
        | satisfiesExamples df outputs p = Just p
        | otherwise = findFirst ps'

pickTopN :: DataFrame
         -> TypedColumn Double
         -> Int
         -> [Expr Double]
         -> [Expr Double]
pickTopN df (TColumn col) n ps = let
        l = VU.convert (toVector @Double col)
        ordered = take n (L.sortBy (\e e' -> compare (abs <$> correlation' l (asDoubleVector e')) (abs <$> correlation' l (asDoubleVector e))) ps)
        asDoubleVector e = let
                (TColumn col') = interpret df e
            in VU.convert (toVector @Double col')
    in ordered

satisfiesExamples :: DataFrame -> TypedColumn Double -> Expr Double -> Bool
satisfiesExamples df col expr = let
        result = interpret df expr
    in result Prelude.== col

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
