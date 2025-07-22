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
{-# LANGUAGE BangPatterns #-}
module DataFrame.Functions where

import DataFrame.Internal.Column (Columnable)
import DataFrame.Internal.Expression (Expr(..), UExpr(..))

import qualified Data.Text as T
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU

col :: Columnable a => T.Text -> Expr a
col = Col

as :: Columnable a => Expr a -> T.Text -> (T.Text, UExpr)
as expr name = (name, Wrap expr)

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

count :: T.Text -> Expr Int
count name = GeneralAggregate name "count" VG.length

minimum :: Columnable a => T.Text -> Expr a
minimum name = ReductionAggregate name "minimum" VG.minimum

maximum :: Columnable a => T.Text -> Expr a
maximum name = ReductionAggregate name "maximum" VG.maximum

sum :: (Columnable a, Num a, VU.Unbox a) => T.Text -> Expr a
sum name = NumericAggregate name "sum" VG.sum

mean :: T.Text -> Expr Double
mean name = let
        mean' samp = let
                (!total, !n) = VG.foldl' (\(!total, !n) v -> (total + v, n + 1))  (0 :: Double, 0 :: Int) samp
            in total / fromIntegral n
    in NumericAggregate name "mean" mean'
