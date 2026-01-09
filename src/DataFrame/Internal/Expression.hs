{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE UndecidableInstances #-}

module DataFrame.Internal.Expression where

import Data.String
import qualified Data.Text as T
import Data.Type.Equality (TestEquality (testEquality), type (:~:) (Refl))
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU
import DataFrame.Internal.Column
import Type.Reflection (Typeable, typeOf, typeRep)

data Expr a where
    Col :: (Columnable a) => T.Text -> Expr a
    Lit :: (Columnable a) => a -> Expr a
    UnaryOp ::
        (Columnable a, Columnable b) => T.Text -> (b -> a) -> Expr b -> Expr a
    BinaryOp ::
        (Columnable c, Columnable b, Columnable a) =>
        T.Text -> (c -> b -> a) -> Expr c -> Expr b -> Expr a
    If :: (Columnable a) => Expr Bool -> Expr a -> Expr a -> Expr a
    AggVector ::
        (VG.Vector v b, Typeable v, Columnable a, Columnable b) =>
        Expr b -> T.Text -> (v b -> a) -> Expr a
    AggReduce :: (Columnable a) => Expr a -> T.Text -> (a -> a -> a) -> Expr a
    -- TODO(mchav): Numeric reduce might be superfluous since expressions are already type checked.
    AggNumericVector ::
        (Columnable a, Columnable b, VU.Unbox a, VU.Unbox b, Num a, Num b) =>
        Expr b -> T.Text -> (VU.Vector b -> a) -> Expr a
    AggFold ::
        forall a b.
        (Columnable a, Columnable b) => Expr b -> T.Text -> a -> (a -> b -> a) -> Expr a

data UExpr where
    Wrap :: (Columnable a) => Expr a -> UExpr

instance Show UExpr where
    show :: UExpr -> String
    show (Wrap expr) = show expr

type NamedExpr = (T.Text, UExpr)

instance (Num a, Columnable a) => Num (Expr a) where
    (+) :: Expr a -> Expr a -> Expr a
    (+) (Lit x) (Lit y) = Lit (x + y)
    (+) e1 e2
        | e1 == e2 = BinaryOp "mult" (*) e1 (Lit 2)
        | otherwise = BinaryOp "add" (+) e1 e2

    (-) :: Expr a -> Expr a -> Expr a
    (-) (Lit x) (Lit y) = Lit (x - y)
    (-) e1 e2 = BinaryOp "sub" (-) e1 e2

    (*) :: Expr a -> Expr a -> Expr a
    (*) (Lit 0) _ = Lit 0
    (*) _ (Lit 0) = Lit 0
    (*) (Lit 1) e = e
    (*) e (Lit 1) = e
    (*) (Lit x) (Lit y) = Lit (x * y)
    (*) e1 e2
        | e1 == e2 = UnaryOp "pow 2" (^ 2) e1
        | otherwise = BinaryOp "mult" (*) e1 e2

    fromInteger :: Integer -> Expr a
    fromInteger = Lit . fromInteger

    negate :: Expr a -> Expr a
    negate (Lit n) = Lit (negate n)
    negate expr = UnaryOp "negate" negate expr

    abs :: (Num a) => Expr a -> Expr a
    abs (Lit n) = Lit (abs n)
    abs expr = UnaryOp "abs" abs expr

    signum :: (Num a) => Expr a -> Expr a
    signum (Lit n) = Lit (signum n)
    signum expr = UnaryOp "signum" signum expr

add :: (Num a, Columnable a) => Expr a -> Expr a -> Expr a
add = (+)

sub :: (Num a, Columnable a) => Expr a -> Expr a -> Expr a
sub = (-)

mult :: (Num a, Columnable a) => Expr a -> Expr a -> Expr a
mult = (*)

instance (Fractional a, Columnable a) => Fractional (Expr a) where
    fromRational :: (Fractional a, Columnable a) => Rational -> Expr a
    fromRational = Lit . fromRational

    (/) :: (Fractional a, Columnable a) => Expr a -> Expr a -> Expr a
    (/) (Lit l1) (Lit l2) = Lit (l1 / l2)
    (/) e1 e2 = BinaryOp "divide" (/) e1 e2

divide :: (Fractional a, Columnable a) => Expr a -> Expr a -> Expr a
divide = (/)

instance (IsString a, Columnable a) => IsString (Expr a) where
    fromString :: String -> Expr a
    fromString s = Lit (fromString s)

instance (Floating a, Columnable a) => Floating (Expr a) where
    pi :: (Floating a, Columnable a) => Expr a
    pi = Lit pi
    exp :: (Floating a, Columnable a) => Expr a -> Expr a
    exp = UnaryOp "exp" exp
    log :: (Floating a, Columnable a) => Expr a -> Expr a
    log = UnaryOp "log" log
    sin :: (Floating a, Columnable a) => Expr a -> Expr a
    sin = UnaryOp "sin" sin
    cos :: (Floating a, Columnable a) => Expr a -> Expr a
    cos = UnaryOp "cos" cos
    asin :: (Floating a, Columnable a) => Expr a -> Expr a
    asin = UnaryOp "asin" asin
    acos :: (Floating a, Columnable a) => Expr a -> Expr a
    acos = UnaryOp "acos" acos
    atan :: (Floating a, Columnable a) => Expr a -> Expr a
    atan = UnaryOp "atan" atan
    sinh :: (Floating a, Columnable a) => Expr a -> Expr a
    sinh = UnaryOp "sinh" sinh
    cosh :: (Floating a, Columnable a) => Expr a -> Expr a
    cosh = UnaryOp "cosh" cosh
    asinh :: (Floating a, Columnable a) => Expr a -> Expr a
    asinh = UnaryOp "asinh" sinh
    acosh :: (Floating a, Columnable a) => Expr a -> Expr a
    acosh = UnaryOp "acosh" acosh
    atanh :: (Floating a, Columnable a) => Expr a -> Expr a
    atanh = UnaryOp "atanh" atanh

instance (Show a) => Show (Expr a) where
    show :: forall a. (Show a) => Expr a -> String
    show (Col name) = "(col @" ++ show (typeRep @a) ++ " " ++ show name ++ ")"
    show (Lit value) = "(lit (" ++ show value ++ "))"
    show (If cond l r) = "(ifThenElse " ++ show cond ++ " " ++ show l ++ " " ++ show r ++ ")"
    show (UnaryOp name f value) = "(" ++ T.unpack name ++ " " ++ show value ++ ")"
    show (BinaryOp name f a b) = "(" ++ T.unpack name ++ " " ++ show a ++ " " ++ show b ++ ")"
    show (AggNumericVector expr op _) = "(" ++ T.unpack op ++ " " ++ show expr ++ ")"
    show (AggVector expr op _) = "(" ++ T.unpack op ++ " " ++ show expr ++ ")"
    show (AggReduce expr op _) = "(" ++ T.unpack op ++ " " ++ show expr ++ ")"
    show (AggFold expr op _ _) = "(" ++ T.unpack op ++ " " ++ show expr ++ ")"

normalize :: (Eq a, Ord a, Show a, Typeable a) => Expr a -> Expr a
normalize expr = case expr of
    Col name -> Col name
    Lit val -> Lit val
    If cond th el -> If (normalize cond) (normalize th) (normalize el)
    UnaryOp name f e -> UnaryOp name f (normalize e)
    BinaryOp name f e1 e2
        | isCommutative name ->
            let n1 = normalize e1
                n2 = normalize e2
             in case testEquality (typeOf n1) (typeOf n2) of
                    Nothing -> expr
                    Just Refl ->
                        if compareExpr n1 n2 == GT
                            then BinaryOp name f n2 n1 -- Swap to canonical order
                            else BinaryOp name f n1 n2
        | otherwise -> BinaryOp name f (normalize e1) (normalize e2)
    AggVector e name f -> AggVector (normalize e) name f
    AggReduce e name f -> AggReduce (normalize e) name f
    AggNumericVector e name f -> AggNumericVector (normalize e) name f
    AggFold e name init f -> AggFold (normalize e) name init f

isCommutative :: T.Text -> Bool
isCommutative name =
    name
        `elem` [ "add"
               , "mult"
               , "min"
               , "max"
               , "eq"
               , "and"
               , "or"
               ]

-- Compare expressions for ordering (used in normalization)
compareExpr :: Expr a -> Expr a -> Ordering
compareExpr e1 e2 = compare (exprKey e1) (exprKey e2)
  where
    exprKey :: Expr a -> String
    exprKey (Col name) = "0:" ++ T.unpack name
    exprKey (Lit val) = "1:" ++ show val
    exprKey (If c t e) = "2:" ++ exprKey c ++ exprKey t ++ exprKey e
    exprKey (UnaryOp name _ e) = "3:" ++ T.unpack name ++ exprKey e
    exprKey (BinaryOp name _ e1 e2) = "4:" ++ T.unpack name ++ exprKey e1 ++ exprKey e2
    exprKey (AggVector e name _) = "5:" ++ T.unpack name ++ exprKey e
    exprKey (AggReduce e name _) = "6:" ++ T.unpack name ++ exprKey e
    exprKey (AggNumericVector e name _) = "7:" ++ T.unpack name ++ exprKey e
    exprKey (AggFold e name _ _) = "8:" ++ T.unpack name ++ exprKey e

instance (Eq a, Columnable a) => Eq (Expr a) where
    (==) l r = eqNormalized (normalize l) (normalize r)
      where
        exprEq :: (Columnable b, Columnable c) => Expr b -> Expr c -> Bool
        exprEq e1 e2 = case testEquality (typeOf e1) (typeOf e2) of
            Just Refl -> e1 == e2
            Nothing -> False
        eqNormalized :: Expr a -> Expr a -> Bool
        eqNormalized (Col n1) (Col n2) = n1 == n2
        eqNormalized (Lit v1) (Lit v2) = v1 == v2
        eqNormalized (If c1 t1 e1) (If c2 t2 e2) =
            c1 == c2 && t1 `exprEq` t2 && e1 `exprEq` e2
        eqNormalized (UnaryOp n1 _ e1) (UnaryOp n2 _ e2) =
            n1 == n2 && e1 `exprEq` e2
        eqNormalized (BinaryOp n1 _ e1a e1b) (BinaryOp n2 _ e2a e2b) =
            n1 == n2 && e1a `exprEq` e2a && e1b `exprEq` e2b
        eqNormalized (AggVector e1 n1 _) (AggVector e2 n2 _) =
            n1 == n2 && e1 `exprEq` e2
        eqNormalized (AggReduce e1 n1 _) (AggReduce e2 n2 _) =
            n1 == n2 && e1 `exprEq` e2
        eqNormalized (AggNumericVector e1 n1 _) (AggNumericVector e2 n2 _) =
            n1 == n2 && e1 `exprEq` e2
        eqNormalized (AggFold e1 n1 i1 _) (AggFold e2 n2 i2 _) =
            n1 == n2 && e1 `exprEq` e2 && i1 == i2
        eqNormalized _ _ = False

instance (Ord a, Columnable a) => Ord (Expr a) where
    compare :: Expr a -> Expr a -> Ordering
    compare e1 e2 = case (e1, e2) of
        (Col n1, Col n2) -> compare n1 n2
        (Lit v1, Lit v2) -> compare v1 v2
        (If c1 t1 e1', If c2 t2 e2') ->
            compare c1 c2 <> exprComp t1 t2 <> exprComp e1' e2'
        (UnaryOp n1 _ e1', UnaryOp n2 _ e2') ->
            compare n1 n2 <> exprComp e1' e2'
        (BinaryOp n1 _ a1 b1, BinaryOp n2 _ a2 b2) ->
            compare n1 n2 <> exprComp a1 a2 <> exprComp b1 b2
        (AggVector e1' n1 _, AggVector e2' n2 _) ->
            compare n1 n2 <> exprComp e1' e2'
        (AggReduce e1' n1 _, AggReduce e2' n2 _) ->
            compare n1 n2 <> exprComp e1' e2'
        (AggNumericVector e1' n1 _, AggNumericVector e2' n2 _) ->
            compare n1 n2 <> exprComp e1' e2'
        (AggFold e1' n1 i1 _, AggFold e2' n2 i2 _) ->
            compare n1 n2 <> exprComp e1' e2' <> compare i1 i2
        -- Different constructors - compare by priority
        (Col _, _) -> LT
        (_, Col _) -> GT
        (Lit _, _) -> LT
        (_, Lit _) -> GT
        (UnaryOp{}, _) -> LT
        (_, UnaryOp{}) -> GT
        (BinaryOp{}, _) -> LT
        (_, BinaryOp{}) -> GT
        (If{}, _) -> LT
        (_, If{}) -> GT
        (AggVector{}, _) -> LT
        (_, AggVector{}) -> GT
        (AggReduce{}, _) -> LT
        (_, AggReduce{}) -> GT
        (AggNumericVector{}, _) -> LT
        (_, AggNumericVector{}) -> GT

exprComp :: (Columnable b, Columnable c) => Expr b -> Expr c -> Ordering
exprComp e1 e2 = case testEquality (typeOf e1) (typeOf e2) of
    Just Refl -> e1 `compare` e2
    Nothing -> LT

replaceExpr ::
    forall a b c.
    (Columnable a, Columnable b, Columnable c) =>
    Expr a -> Expr b -> Expr c -> Expr c
replaceExpr new old expr = case testEquality (typeRep @b) (typeRep @c) of
    Just Refl -> case testEquality (typeRep @a) (typeRep @c) of
        Just Refl -> if old == expr then new else replace'
        Nothing -> expr
    Nothing -> replace'
  where
    replace' = case expr of
        (Col _) -> expr
        (Lit _) -> expr
        (If cond l r) ->
            If (replaceExpr new old cond) (replaceExpr new old l) (replaceExpr new old r)
        (UnaryOp name f value) -> UnaryOp name f (replaceExpr new old value)
        (BinaryOp name f l r) -> BinaryOp name f (replaceExpr new old l) (replaceExpr new old r)
        (AggNumericVector expr op f) -> AggNumericVector (replaceExpr new old expr) op f
        (AggVector expr op f) -> AggVector (replaceExpr new old expr) op f
        (AggReduce expr op f) -> AggReduce (replaceExpr new old expr) op f
        (AggFold expr op acc f) -> AggFold (replaceExpr new old expr) op acc f

eSize :: Expr a -> Int
eSize (Col _) = 1
eSize (Lit _) = 1
eSize (If c l r) = 1 + eSize c + eSize l + eSize r
eSize (UnaryOp _ _ e) = 1 + eSize e
eSize (BinaryOp _ _ l r) = 1 + eSize l + eSize r
eSize (AggNumericVector expr op _) = eSize expr + 1
eSize (AggVector expr op _) = eSize expr + 1
eSize (AggReduce expr op _) = eSize expr + 1
eSize (AggFold expr op _ _) = eSize expr + 1
