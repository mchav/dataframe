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
{-# LANGUAGE TemplateHaskell #-}
module DataFrame.Functions where

import DataFrame.Internal.Column
import DataFrame.Internal.DataFrame (DataFrame(..), unsafeGetColumn)
import DataFrame.Internal.Expression (Expr(..), UExpr(..))

import           Control.Monad
import           Data.Function
import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector as VB
import           Language.Haskell.TH
import qualified Language.Haskell.TH.Syntax as TH

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

count :: Columnable a => Expr a -> Expr Int
count (Col name) = GeneralAggregate name "count" VG.length
count _ = error "Argument can only be a column reference not an unevaluated expression"

minimum :: Columnable a => Expr a -> Expr a
minimum (Col name) = ReductionAggregate name "minimum" VG.minimum

maximum :: Columnable a => Expr a -> Expr a
maximum (Col name) = ReductionAggregate name "maximum" VG.maximum

sum :: (Columnable a, Num a, VU.Unbox a) => Expr a -> Expr a
sum (Col name) = NumericAggregate name "sum" VG.sum

mean :: (Columnable a, Num a, VU.Unbox a) => Expr a -> Expr Double
mean (Col name) = let
        mean' samp = let
                (!total, !n) = VG.foldl' (\(!total, !n) v -> (total + v, n + 1))  (0 :: Double, 0 :: Int) samp
            in total / fromIntegral n
    in NumericAggregate name "mean" mean'

-- TODO: Enumerate universe of primitives.
-- Maybe this should go in a separate module where people can add their
-- universe of types.
-- Or maybe make it easier to insert your on types here.
typeFromString :: String -> Q Type
typeFromString s = case s of
  "Int"    -> [t| Int |]
  "Double" -> [t| Double |]
  "Bool"   -> [t| Bool |]
  "Text"   -> [t| T.Text |]
  "Maybe Int"    -> [t| Maybe Int |]
  "Maybe Double" -> [t| Maybe Double |]
  "Maybe Bool"   -> [t| Maybe Bool |]
  "Maybe Text"   -> [t| Maybe T.Text |]
  _        -> fail $ "Unsupported type: " ++ s

declareColumns :: DataFrame -> DecsQ
declareColumns df = let
        names = (map fst . L.sortBy (compare `on` snd). M.toList . columnIndices) df
        types = map (columnTypeString . (`unsafeGetColumn` df)) names
        specs = zip names types
    in fmap concat $ forM specs $ \(nm, tyStr) -> do
        ty  <- typeFromString tyStr
        let n  = mkName (T.unpack nm)
        sig <- sigD n [t| Expr $(pure ty) |]
        val <- valD (varP n) (normalB [| col $(TH.lift nm) |]) []
        pure [sig, val]
