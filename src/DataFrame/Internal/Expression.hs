{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
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
import qualified Data.Vector.Generic as VG
import Type.Reflection (typeRep)
import DataFrame.Errors (DataFrameException(ColumnNotFoundException))
import Control.Exception (throw)
import Data.Maybe (fromMaybe)

data Expr a where
    Col :: Columnable a => T.Text -> Expr a 
    Lit :: Columnable a => a -> Expr a
    Apply :: (Columnable a,
              Columnable b)
          => T.Text -- Operation name
         -> (b -> a)
         -> Expr b
         -> Expr a
    BinOp :: (Columnable c, 
              Columnable b, 
              Columnable a)
          => T.Text -- operation name
          -> (c -> b -> a)
          -> Expr c
          -> Expr b 
          -> Expr a
    GeneralAggregate :: (Columnable a)
              => T.Text     -- Column name
              -> T.Text     -- Operation name
              -> (forall v b. (VG.Vector v b, Columnable b) => v b -> a)
              -> Expr a
    ReductionAggregate :: (Columnable a)
              => T.Text     -- Column name
              -> T.Text     -- Operation name
              -> (forall v a. (VG.Vector v a, Columnable a) => v a -> a)
              -> Expr a
    NumericAggregate :: (Columnable a,
                         Columnable b,
                         Num a,
                         Num b)
                     => T.Text     -- Column name
                     -> T.Text     -- Operation name
                     -> (VU.Vector b -> a) 
                     -> Expr a

data UExpr where
    Wrap :: Columnable a => Expr a -> UExpr

interpret :: forall a . (Columnable a) => DataFrame -> Expr a -> TypedColumn a
interpret df (Lit value) = TColumn $ fromVector $ V.replicate (fst $ dataframeDimensions df) value
interpret df (Col name) = case getColumn name df of
    Nothing -> throw $ ColumnNotFoundException name "" (map fst $ M.toList $ columnIndices df)
    Just col -> TColumn col
interpret df (Apply _ (f :: c -> d) value) = let
        (TColumn value') = interpret @c df value
    -- TODO: Handle this gracefully.
    in TColumn $ fromMaybe (error "mapColumn returned nothing") (mapColumn f value')
interpret df (BinOp _ (f :: c -> d -> e) left right) = let
        (TColumn left') = interpret @c df left
        (TColumn right') = interpret @d df right
    in TColumn $ fromMaybe (error "mapColumn returned nothing") (zipWithColumns f left' right')
interpret df (GeneralAggregate name op (f :: forall v b. (VG.Vector v b, Columnable b) => v b -> c)) = case getColumn name df of
    Nothing -> throw $ ColumnNotFoundException name "" (map fst $ M.toList $ columnIndices df)
    Just (GroupedBoxedColumn col) -> TColumn $ fromVector $ VG.map f col
    Just (GroupedUnboxedColumn col) -> TColumn $ fromVector $ VG.map f col
    Just (GroupedOptionalColumn col) -> TColumn $ fromVector $ VG.map f col
    _ -> error ""
interpret df (ReductionAggregate name op (f :: forall v a. (VG.Vector v a, Columnable a) => v a -> a)) = case getColumn name df of
    Nothing -> throw $ ColumnNotFoundException name "" (map fst $ M.toList $ columnIndices df)
    Just (GroupedBoxedColumn col) -> TColumn $ fromVector $ VG.map f col
    Just (GroupedUnboxedColumn col) -> TColumn $ fromVector $ VG.map f col
    Just (GroupedOptionalColumn col) -> TColumn $ fromVector $ VG.map f col
    _ -> error ""
interpret df (NumericAggregate name op (f :: VU.Vector b -> c)) = case getColumn name df of
    Nothing -> throw $ ColumnNotFoundException name "" (map fst $ M.toList $ columnIndices df)
    Just (GroupedUnboxedColumn (col :: V.Vector (VU.Vector d))) -> case testEquality (typeRep @b) (typeRep @d) of
        Just Refl -> TColumn $ fromVector $ VG.map f col
        -- Do the matching trick here.
        Nothing -> case testEquality (typeRep @d) (typeRep @Int) of
            Just Refl -> case testEquality (typeRep @b) (typeRep @Double) of
                Just Refl -> TColumn $ fromVector $ VG.map (f . (VG.map fromIntegral)) col
                Nothing -> error $ "Column not a number: " ++ (T.unpack name)
            Nothing -> case testEquality (typeRep @d) (typeRep @Double) of
                Just Refl -> case testEquality (typeRep @b) (typeRep @Int) of
                    Just Refl -> TColumn $ fromVector $ VG.map (f . (VG.map round)) col
                    Nothing -> error $ "Column not a number: " ++ (T.unpack name)
    _ -> error "Cannot apply numeric aggregation to boxed column"

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
