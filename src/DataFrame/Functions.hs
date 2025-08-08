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
import qualified Data.Char as Char
import Debug.Trace (traceShow)
import Type.Reflection (typeRep)

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
minimum (Col name) = ReductionAggregate name "minimum" min

maximum :: Columnable a => Expr a -> Expr a
maximum (Col name) = ReductionAggregate name "maximum" max

sum :: forall a . (Columnable a, Num a, VU.Unbox a) => Expr a -> Expr a
sum (Col name) = NumericAggregate name "sum" VG.sum

mean :: (Columnable a, Num a, VU.Unbox a) => Expr a -> Expr Double
mean (Col name) = let
        mean' samp = let
                (!total, !n) = VG.foldl' (\(!total, !n) v -> (total + v, n + 1))  (0 :: Double, 0 :: Int) samp
            in total / fromIntegral n
    in NumericAggregate name "mean" mean'

-- See Section 2.4 of the Haskell Report https://www.haskell.org/definition/haskell2010.pdf
isReservedId :: T.Text -> Bool
isReservedId t = case t of
  "case"     -> True
  "class"    -> True
  "data"     -> True
  "default"  -> True
  "deriving" -> True
  "do"       -> True
  "else"     -> True
  "foreign"  -> True
  "if"       -> True
  "import"   -> True
  "in"       -> True
  "infix"    -> True
  "infixl"   -> True
  "infixr"   -> True
  "instance" -> True
  "let"      -> True
  "module"   -> True
  "newtype"  -> True
  "of"       -> True
  "then"     -> True
  "type"     -> True
  "where"    -> True
  _          -> False

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
isHaskellIdentifier t =  not (isVarId t) || isReservedId t

sanitize :: T.Text -> T.Text
sanitize t
  | isValid = t
  | isHaskellIdentifier t' = "_" <> t' <> "_"
  | otherwise = t'
  where
    isValid
      =  not (isHaskellIdentifier t)
      && isVarId t
      && T.all Char.isAlphaNum t
    t' = T.map replaceInvalidCharacters . T.filter (not . parentheses) $ t
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
      _   -> False

typeFromString :: [String] -> Q Type
typeFromString []  = fail "No type specified"
typeFromString [t] = do
  maybeType <- lookupTypeName t
  case maybeType of
    Just name -> return (ConT name)
    Nothing -> fail $ "Unsupported type: " ++ t
typeFromString [tycon,t1] = do
  outer <- typeFromString [tycon]
  inner <- typeFromString [t1]
  return (AppT outer inner)
typeFromString [tycon,t1,t2] = do
  outer <- typeFromString [tycon]
  lhs <- typeFromString [t1]
  rhs <- typeFromString [t2]
  return (AppT (AppT outer lhs) rhs)
typeFromString s = fail $ "Unsupported type: " ++ (unwords s)

declareColumns :: DataFrame -> DecsQ
declareColumns df = let
        names = (map fst . L.sortBy (compare `on` snd). M.toList . columnIndices) df
        types = map (columnTypeString . (`unsafeGetColumn` df)) names
        specs = zipWith (\name type_ -> (sanitize name, type_)) names types
    in fmap concat $ forM specs $ \(nm, tyStr) -> do
        traceShow nm (pure ())
        ty  <- typeFromString (words tyStr)
        let n  = mkName (T.unpack nm)
        sig <- sigD n [t| Expr $(pure ty) |]
        val <- valD (varP n) (normalB [| col $(TH.lift nm) |]) []
        pure [sig, val]
