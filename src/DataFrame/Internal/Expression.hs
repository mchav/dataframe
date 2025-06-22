{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
module DataFrame.Internal.Expression where

import qualified Data.Map as M
import Data.Type.Equality (type (:~:)(Refl), TestEquality (testEquality))
import Data.Data (Typeable)
import DataFrame.Internal.Column
import DataFrame.Internal.DataFrame
import DataFrame.Internal.Types
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import Type.Reflection (typeRep)
import DataFrame.Errors (DataFrameException(ColumnNotFoundException))
import Control.Exception (throw)
import Data.Maybe (fromMaybe)

data Expr a where
    Col :: Columnable a => T.Text -> Expr a 
    Lit :: Columnable a => a -> Expr a
    Apply :: (Columnable a, Columnable b) => T.Text -> (b -> a) -> Expr b -> Expr a
    BinOp :: (Columnable c, Columnable b, Columnable a) => T.Text -> (c -> b -> a) -> Expr c -> Expr b -> Expr a

interpret :: forall a . (Columnable a) => DataFrame -> Expr a -> TypedColumn a
interpret df (Lit value) = TColumn $ toColumn' $ V.replicate (fst $ dataframeDimensions df) value
interpret df (Col name) = case getColumn name df of
    Nothing -> throw $ ColumnNotFoundException name "" (map fst $ M.toList $ columnIndices df)
    Just col -> TColumn col
interpret df (Apply _ (f :: c -> d) value) = let
        (TColumn value') = interpret @c df value
    in TColumn $ fromMaybe (error "transform returned nothing") (transform f value')
interpret df (BinOp _ (f :: c -> d -> e) left right) = let
        (TColumn left') = interpret @c df left
        (TColumn right') = interpret @d df right
    in TColumn $ zipWithColumns f left' right'

instance (Num a, Columnable a) => Num (Expr a) where
    (+) :: Expr a -> Expr a -> Expr a
    (+) = BinOp "add" (+)

    (*) :: Expr a -> Expr a -> Expr a
    (*) = BinOp "mult" (*)

    fromInteger :: Integer -> Expr a
    fromInteger = Lit . fromInteger 

    negate :: Expr a -> Expr a
    negate = Apply "negate" negate

    abs :: Num a => Expr a -> Expr a
    abs = Apply "abs" abs

    signum :: Num a => Expr a -> Expr a
    signum = Apply "signum" signum

instance (Fractional a, Columnable a) => Fractional (Expr a) where
    fromRational :: (Fractional a, Columnable a) => Rational -> Expr a
    fromRational = Lit . fromRational

    (/) :: (Fractional a, Columnable a) => Expr a -> Expr a -> Expr a
    (/) = BinOp "divide" (/)

instance (Floating a, Columnable a) => Floating (Expr a) where
    pi :: (Floating a, Columnable a) => Expr a
    pi = Lit pi
    exp :: (Floating a, Columnable a) => Expr a -> Expr a
    exp = Apply "exp" exp
    log :: (Floating a, Columnable a) => Expr a -> Expr a
    log = Apply "log" log
    sin :: (Floating a, Columnable a) => Expr a -> Expr a
    sin = Apply "sin" sin
    cos :: (Floating a, Columnable a) => Expr a -> Expr a
    cos = Apply "cos" cos
    asin :: (Floating a, Columnable a) => Expr a -> Expr a
    asin = Apply "asin" asin
    acos :: (Floating a, Columnable a) => Expr a -> Expr a
    acos = Apply "acos" acos 
    atan :: (Floating a, Columnable a) => Expr a -> Expr a
    atan = Apply "atan" atan
    sinh :: (Floating a, Columnable a) => Expr a -> Expr a
    sinh = Apply "sinh" sinh
    cosh :: (Floating a, Columnable a) => Expr a -> Expr a
    cosh = Apply "cosh" cosh
    asinh :: (Floating a, Columnable a) => Expr a -> Expr a
    asinh = Apply "asinh" sinh
    acosh :: (Floating a, Columnable a) => Expr a -> Expr a
    acosh = Apply "acosh" acosh
    atanh :: (Floating a, Columnable a) => Expr a -> Expr a
    atanh = Apply "atanh" atanh


instance (Show a) => Show (Expr a) where
    show :: forall a . Show a => Expr a -> String
    show (Col name) = "col@" ++ show (typeRep @a) ++ "(" ++ T.unpack name ++ ")"
    show (Lit value) = show value
    show (Apply name f value) = T.unpack name ++ "(" ++ show value ++ ")"
    show (BinOp name f a b) = T.unpack name ++ "(" ++ show a ++ ", " ++ show b ++ ")" 

col :: Columnable a => T.Text -> Expr a
col = Col

lit :: Columnable a => a -> Expr a
lit = Lit

lift :: (Columnable a, Columnable b) => (a -> b) -> Expr a -> Expr b
lift = Apply "udf"

lift2 :: (Columnable c, Columnable b, Columnable a) => (c -> b -> a) -> Expr c -> Expr b -> Expr a 
lift2 = BinOp "udf"

eq :: (Columnable a, Eq a) => Expr a -> Expr a -> Expr Bool
eq = BinOp "eq" (==)

lt :: (Columnable a, Ord a) => Expr a -> Expr a -> Expr Bool
lt = BinOp "lt" (<)

gt :: (Columnable a, Ord a) => Expr a -> Expr a -> Expr Bool
gt = BinOp "gt" (>)

leq :: (Columnable a, Ord a, Eq a) => Expr a -> Expr a -> Expr Bool
leq = BinOp "leq" (<=)

geq :: (Columnable a, Ord a, Eq a) => Expr a -> Expr a -> Expr Bool
geq = BinOp "geq" (>=)
