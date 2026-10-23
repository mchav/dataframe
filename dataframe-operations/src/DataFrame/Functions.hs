{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DisambiguateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE IncoherentInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}

module DataFrame.Functions (
    module DataFrame.Functions,
    module DataFrame.Expression.Operators,
    add,
    sub,
    mult,
    divide,
    prettyPrint,
) where

import DataFrame.Internal.Column
import DataFrame.Internal.Expression
import DataFrame.Internal.Statistics

import Control.Applicative
import Data.Either
import Data.Function (on)
import Data.Int
import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Maybe as Maybe
import qualified Data.Set as S
import qualified Data.Text as T
import Data.Time
import Data.Type.Equality (testEquality, type (:~:) (Refl))
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import DataFrame.Expression.Operators
import DataFrame.Internal.Expression.Operators.Nullable (
    BaseType,
    NullLift1Op (applyNull1),
    NullLift1Result,
    NullLift2Op (applyNull2),
    NullLift2Result,
 )
import Text.Regex.TDFA
import Type.Reflection (typeRep)
import Prelude hiding (maximum, minimum)
import Prelude as P

lift :: (Columnable a, Columnable b) => (a -> b) -> Expr a -> Expr b
lift f =
    Unary
        (MkUnaryOp{unaryFn = f, unaryId = Anonymous "unaryUdf", unarySymbol = Nothing})

lift2 ::
    (Columnable c, Columnable b, Columnable a) =>
    (c -> b -> a) ->
    Expr c ->
    Expr b ->
    Expr a
lift2 f =
    Binary
        ( MkBinaryOp
            { binaryFn = f
            , binaryId = Anonymous "binaryUdf"
            , binarySymbol = Nothing
            , binaryCommutative = False
            , binaryPrecedence = 0
            }
        )

-- | Lift a unary function over a nullable or non-nullable column expression.
nullLift ::
    (NullLift1Op a r (NullLift1Result a r), Columnable (NullLift1Result a r)) =>
    (BaseType a -> r) ->
    Expr a ->
    Expr (NullLift1Result a r)
nullLift f =
    Unary
        ( MkUnaryOp
            { unaryFn = applyNull1 f
            , unaryId = Anonymous "nullLift"
            , unarySymbol = Nothing
            }
        )

-- | Lift a binary function over nullable or non-nullable column expressions.
nullLift2 ::
    (NullLift2Op a b r (NullLift2Result a b r), Columnable (NullLift2Result a b r)) =>
    (BaseType a -> BaseType b -> r) ->
    Expr a ->
    Expr b ->
    Expr (NullLift2Result a b r)
nullLift2 f =
    Binary
        ( MkBinaryOp
            { binaryFn = applyNull2 f
            , binaryId = Anonymous "nullLift2"
            , binarySymbol = Nothing
            , binaryCommutative = False
            , binaryPrecedence = 0
            }
        )

-- | Lenient coercion of a column to @Maybe a@; failures become 'Nothing'.
cast :: forall a. (Columnable a, Read a) => T.Text -> Expr (Maybe a)
cast colName = CastWith colName (Builtin CCast) (either (const Nothing) Just)

-- | Lenient coercion that substitutes a default for unconvertible values.
castWithDefault :: forall a. (Columnable a, Read a) => a -> T.Text -> Expr a
castWithDefault def colName =
    CastWith
        colName
        (Builtin (CCastWithDefault (T.pack (show def))))
        (fromRight def)

-- | Lenient coercion to @Either T.Text a@, keeping unparsed values as 'Left'.
castEither ::
    forall a. (Columnable a, Read a) => T.Text -> Expr (Either T.Text a)
castEither colName = CastWith colName (Builtin CCastEither) (either (Left . T.pack) Right)

-- | Lenient coercion that crashes at evaluation if a @Nothing@ is found.
unsafeCast :: forall a. (Columnable a, Read a) => T.Text -> Expr a
unsafeCast colName =
    CastWith
        colName
        (Builtin CUnsafeCast)
        (fromRight (error "unsafeCast: unexpected Nothing in column"))

castExpr ::
    forall b src.
    (Columnable b, Columnable src, Read b) =>
    Expr src ->
    Expr (Maybe b)
castExpr = CastExprWith @b @(Maybe b) @src (Builtin CCast) (either (const Nothing) Just)

castExprWithDefault ::
    forall b src. (Columnable b, Columnable src, Read b) => b -> Expr src -> Expr b
castExprWithDefault def =
    CastExprWith @b @b @src
        (Builtin (CCastWithDefault (T.pack (show def))))
        (fromRight def)

castExprEither ::
    forall b src.
    (Columnable b, Columnable src, Read b) =>
    Expr src ->
    Expr (Either T.Text b)
castExprEither =
    CastExprWith @b @(Either T.Text b) @src
        (Builtin CCastEither)
        (either (Left . T.pack) Right)

unsafeCastExpr ::
    forall b src. (Columnable b, Columnable src, Read b) => Expr src -> Expr b
unsafeCastExpr =
    CastExprWith @b @b @src
        (Builtin CUnsafeCast)
        (fromRight (error "unsafeCastExpr: unexpected Nothing in column"))

toDouble :: (Columnable a, Real a) => Expr a -> Expr Double
toDouble =
    Unary
        ( MkUnaryOp
            { unaryFn = realToFrac
            , unaryId = Builtin UToDouble
            , unarySymbol = Nothing
            }
        )

infix 8 `div`
div :: (Integral a, Columnable a) => Expr a -> Expr a -> Expr a
div = lift2Builtin BDiv Prelude.div (Just "//") False 7

mod :: (Integral a, Columnable a) => Expr a -> Expr a -> Expr a
mod = lift2Builtin BMod Prelude.mod Nothing False 7

eq :: (Columnable a, Eq a, a ~ BaseType a) => Expr a -> Expr a -> Expr Bool
eq = lift2Builtin BEq (==) (Just "==") True 4

lt :: (Columnable a, Ord a, a ~ BaseType a) => Expr a -> Expr a -> Expr Bool
lt = lift2Builtin BLt (<) (Just "<") False 4

gt :: (Columnable a, Ord a, a ~ BaseType a) => Expr a -> Expr a -> Expr Bool
gt = lift2Builtin BGt (>) (Just ">") False 4

leq ::
    (Columnable a, Ord a, Eq a, a ~ BaseType a) => Expr a -> Expr a -> Expr Bool
leq = lift2Builtin BLeq (<=) (Just "<=") False 4

geq ::
    (Columnable a, Ord a, Eq a, a ~ BaseType a) => Expr a -> Expr a -> Expr Bool
geq = lift2Builtin BGeq (>=) (Just ">=") False 4

and :: Expr Bool -> Expr Bool -> Expr Bool
and = (.&&)

or :: Expr Bool -> Expr Bool -> Expr Bool
or = (.||)

not :: Expr Bool -> Expr Bool
not =
    Unary
        (MkUnaryOp{unaryFn = Prelude.not, unaryId = Builtin UNot, unarySymbol = Just "~"})

count :: (Columnable a) => Expr a -> Expr Int
count = Agg (MergeAgg (Builtin ACount) (0 :: Int) (\c _ -> c + 1) (+) id)
{-# SPECIALIZE count :: Expr Double -> Expr Int #-}
{-# SPECIALIZE count :: Expr Float -> Expr Int #-}
{-# SPECIALIZE count :: Expr Int -> Expr Int #-}
{-# SPECIALIZE count :: Expr Int8 -> Expr Int #-}
{-# SPECIALIZE count :: Expr Int16 -> Expr Int #-}
{-# SPECIALIZE count :: Expr Int32 -> Expr Int #-}
{-# SPECIALIZE count :: Expr Int64 -> Expr Int #-}
{-# INLINEABLE count #-}

-- | Number of distinct values, the equivalent of SQL's @COUNT(DISTINCT x)@.
countDistinct :: forall a. (Columnable a, Ord a) => Expr a -> Expr Int
countDistinct =
    Agg
        ( MergeAgg
            (Builtin ACountDistinct)
            (S.empty :: S.Set a)
            (flip S.insert)
            S.union
            S.size
        )
{-# SPECIALIZE countDistinct :: Expr Double -> Expr Int #-}
{-# SPECIALIZE countDistinct :: Expr Float -> Expr Int #-}
{-# SPECIALIZE countDistinct :: Expr Int -> Expr Int #-}
{-# SPECIALIZE countDistinct :: Expr Int8 -> Expr Int #-}
{-# SPECIALIZE countDistinct :: Expr Int16 -> Expr Int #-}
{-# SPECIALIZE countDistinct :: Expr Int32 -> Expr Int #-}
{-# SPECIALIZE countDistinct :: Expr Int64 -> Expr Int #-}

-- | Row count, the equivalent of SQL's @COUNT(*)@.
countAll :: Expr Int
countAll = count (Lit (0 :: Int))
{-# INLINE countAll #-}

collect :: (Columnable a) => Expr a -> Expr [a]
collect = Agg (FoldAgg (Builtin ACollect) (Just []) (flip (:)))
{-# SPECIALIZE collect :: Expr Double -> Expr [Double] #-}
{-# SPECIALIZE collect :: Expr Float -> Expr [Float] #-}
{-# SPECIALIZE collect :: Expr Int -> Expr [Int] #-}
{-# INLINEABLE collect #-}

mode :: (Ord a, Columnable a, Eq a) => Expr a -> Expr a
mode =
    Agg
        ( CollectAgg
            (Builtin AMode)
            ( fst
                . L.maximumBy (compare `on` snd)
                . M.toList
                . V.foldl' (\m e -> M.insertWith (+) e (1 :: Int) m) M.empty
            )
        )
{-# SPECIALIZE mode :: Expr Double -> Expr Double #-}
{-# SPECIALIZE mode :: Expr Float -> Expr Float #-}
{-# SPECIALIZE mode :: Expr Int -> Expr Int #-}
{-# SPECIALIZE mode :: Expr Int8 -> Expr Int8 #-}
{-# SPECIALIZE mode :: Expr Int16 -> Expr Int16 #-}
{-# SPECIALIZE mode :: Expr Int32 -> Expr Int32 #-}
{-# SPECIALIZE mode :: Expr Int64 -> Expr Int64 #-}
{-# INLINEABLE mode #-}

minimum :: (Columnable a, Ord a) => Expr a -> Expr a
minimum = Agg (FoldAgg (Builtin AMinimum) Nothing Prelude.min)
{-# SPECIALIZE DataFrame.Functions.minimum :: Expr Double -> Expr Double #-}
{-# SPECIALIZE DataFrame.Functions.minimum :: Expr Float -> Expr Float #-}
{-# SPECIALIZE DataFrame.Functions.minimum :: Expr Int -> Expr Int #-}
{-# SPECIALIZE DataFrame.Functions.minimum :: Expr Int8 -> Expr Int8 #-}
{-# SPECIALIZE DataFrame.Functions.minimum :: Expr Int16 -> Expr Int16 #-}
{-# SPECIALIZE DataFrame.Functions.minimum :: Expr Int32 -> Expr Int32 #-}
{-# SPECIALIZE DataFrame.Functions.minimum :: Expr Int64 -> Expr Int64 #-}
{-# INLINEABLE DataFrame.Functions.minimum #-}

maximum :: (Columnable a, Ord a) => Expr a -> Expr a
maximum = Agg (FoldAgg (Builtin AMaximum) Nothing Prelude.max)
{-# SPECIALIZE DataFrame.Functions.maximum :: Expr Double -> Expr Double #-}
{-# SPECIALIZE DataFrame.Functions.maximum :: Expr Float -> Expr Float #-}
{-# SPECIALIZE DataFrame.Functions.maximum :: Expr Int -> Expr Int #-}
{-# SPECIALIZE DataFrame.Functions.maximum :: Expr Int8 -> Expr Int8 #-}
{-# SPECIALIZE DataFrame.Functions.maximum :: Expr Int16 -> Expr Int16 #-}
{-# SPECIALIZE DataFrame.Functions.maximum :: Expr Int32 -> Expr Int32 #-}
{-# SPECIALIZE DataFrame.Functions.maximum :: Expr Int64 -> Expr Int64 #-}
{-# INLINEABLE DataFrame.Functions.maximum #-}

sum :: forall a. (Columnable a, Num a) => Expr a -> Expr a
sum = Agg (FoldAgg (Builtin ASum) Nothing (+))
{-# SPECIALIZE DataFrame.Functions.sum :: Expr Double -> Expr Double #-}
{-# SPECIALIZE DataFrame.Functions.sum :: Expr Float -> Expr Float #-}
{-# SPECIALIZE DataFrame.Functions.sum :: Expr Int -> Expr Int #-}
{-# SPECIALIZE DataFrame.Functions.sum :: Expr Int8 -> Expr Int8 #-}
{-# SPECIALIZE DataFrame.Functions.sum :: Expr Int16 -> Expr Int16 #-}
{-# SPECIALIZE DataFrame.Functions.sum :: Expr Int32 -> Expr Int32 #-}
{-# SPECIALIZE DataFrame.Functions.sum :: Expr Int64 -> Expr Int64 #-}
{-# INLINEABLE DataFrame.Functions.sum #-}

sumMaybe :: forall a. (Columnable a, Num a) => Expr (Maybe a) -> Expr a
sumMaybe = Agg (CollectAgg (Builtin ASumMaybe) (P.sum . Maybe.catMaybes . V.toList))
{-# SPECIALIZE sumMaybe :: Expr (Maybe Double) -> Expr Double #-}
{-# SPECIALIZE sumMaybe :: Expr (Maybe Float) -> Expr Float #-}
{-# SPECIALIZE sumMaybe :: Expr (Maybe Int) -> Expr Int #-}
{-# SPECIALIZE sumMaybe :: Expr (Maybe Int8) -> Expr Int8 #-}
{-# SPECIALIZE sumMaybe :: Expr (Maybe Int16) -> Expr Int16 #-}
{-# SPECIALIZE sumMaybe :: Expr (Maybe Int32) -> Expr Int32 #-}
{-# SPECIALIZE sumMaybe :: Expr (Maybe Int64) -> Expr Int64 #-}
{-# INLINEABLE sumMaybe #-}

mean :: (Columnable a, Real a) => Expr a -> Expr Double
mean =
    Agg
        ( MergeAgg
            (Builtin AMean)
            ((0.0, 0) :: (Double, Int))
            (\(!s, !c) x -> (s + realToFrac x, c + 1))
            (\(!s1, !c1) (!s2, !c2) -> (s1 + s2, c1 + c2))
            (\(s, c) -> if c == 0 then 0 / 0 else s / fromIntegral c)
        )
{-# SPECIALIZE mean :: Expr Double -> Expr Double #-}
{-# SPECIALIZE mean :: Expr Float -> Expr Double #-}
{-# SPECIALIZE mean :: Expr Int -> Expr Double #-}
{-# SPECIALIZE mean :: Expr Int8 -> Expr Double #-}
{-# SPECIALIZE mean :: Expr Int16 -> Expr Double #-}
{-# SPECIALIZE mean :: Expr Int32 -> Expr Double #-}
{-# SPECIALIZE mean :: Expr Int64 -> Expr Double #-}
{-# INLINEABLE mean #-}

-- | The k largest values per group.
topK :: (Columnable a, Ord a) => Int -> Expr a -> Expr [a]
topK = kExtremes True
{-# INLINEABLE topK #-}

-- | The k smallest values per group.
bottomK :: (Columnable a, Ord a) => Int -> Expr a -> Expr [a]
bottomK = kExtremes False
{-# INLINEABLE bottomK #-}

-- | The k largest (True) or smallest (False) values, dropping NaNs.
kExtremes ::
    forall a. (Columnable a, Ord a) => Bool -> Int -> Expr a -> Expr [a]
kExtremes largest k =
    Agg
        ( MergeAgg
            (Builtin (if largest then ATopK k else ABottomK k))
            []
            step
            (L.foldl' step)
            id
        )
  where
    wins = if largest then (>) else (<)
    step acc x
        | isNaNLike x = acc
        | otherwise = insert x acc
    insert x = P.take k . go
      where
        go (z : zs) | wins z x = z : go zs
        go zs = x : zs

    isNaNLike :: a -> Bool
    isNaNLike x
        | Just Refl <- testEquality (typeRep @a) (typeRep @Double) = isNaN x
        | Just Refl <- testEquality (typeRep @a) (typeRep @Float) = isNaN x
        | otherwise = False

meanMaybe :: forall a. (Columnable a, Real a) => Expr (Maybe a) -> Expr Double
meanMaybe = Agg (CollectAgg (Builtin AMeanMaybe) (mean' . optionalToDoubleVector))
{-# SPECIALIZE meanMaybe :: Expr (Maybe Double) -> Expr Double #-}
{-# SPECIALIZE meanMaybe :: Expr (Maybe Float) -> Expr Double #-}
{-# SPECIALIZE meanMaybe :: Expr (Maybe Int) -> Expr Double #-}
{-# SPECIALIZE meanMaybe :: Expr (Maybe Int8) -> Expr Double #-}
{-# SPECIALIZE meanMaybe :: Expr (Maybe Int16) -> Expr Double #-}
{-# SPECIALIZE meanMaybe :: Expr (Maybe Int32) -> Expr Double #-}
{-# SPECIALIZE meanMaybe :: Expr (Maybe Int64) -> Expr Double #-}
{-# INLINEABLE meanMaybe #-}

variance :: (Columnable a, Real a, VU.Unbox a) => Expr a -> Expr Double
variance = Agg (CollectAgg (Builtin AVariance) variance')
{-# SPECIALIZE variance :: Expr Double -> Expr Double #-}
{-# SPECIALIZE variance :: Expr Float -> Expr Double #-}
{-# SPECIALIZE variance :: Expr Int -> Expr Double #-}
{-# SPECIALIZE variance :: Expr Int8 -> Expr Double #-}
{-# SPECIALIZE variance :: Expr Int16 -> Expr Double #-}
{-# SPECIALIZE variance :: Expr Int32 -> Expr Double #-}
{-# SPECIALIZE variance :: Expr Int64 -> Expr Double #-}
{-# INLINEABLE variance #-}

median :: (Columnable a, Real a, VU.Unbox a) => Expr a -> Expr Double
median = Agg (CollectAgg (Builtin AMedian) median')
{-# SPECIALIZE median :: Expr Double -> Expr Double #-}
{-# SPECIALIZE median :: Expr Float -> Expr Double #-}
{-# SPECIALIZE median :: Expr Int -> Expr Double #-}
{-# SPECIALIZE median :: Expr Int8 -> Expr Double #-}
{-# SPECIALIZE median :: Expr Int16 -> Expr Double #-}
{-# SPECIALIZE median :: Expr Int32 -> Expr Double #-}
{-# SPECIALIZE median :: Expr Int64 -> Expr Double #-}
{-# INLINEABLE median #-}

medianMaybe :: (Columnable a, Real a) => Expr (Maybe a) -> Expr Double
medianMaybe = Agg (CollectAgg (Builtin AMedianMaybe) (median' . optionalToDoubleVector))
{-# SPECIALIZE medianMaybe :: Expr (Maybe Double) -> Expr Double #-}
{-# SPECIALIZE medianMaybe :: Expr (Maybe Float) -> Expr Double #-}
{-# SPECIALIZE medianMaybe :: Expr (Maybe Int) -> Expr Double #-}
{-# SPECIALIZE medianMaybe :: Expr (Maybe Int8) -> Expr Double #-}
{-# SPECIALIZE medianMaybe :: Expr (Maybe Int16) -> Expr Double #-}
{-# SPECIALIZE medianMaybe :: Expr (Maybe Int32) -> Expr Double #-}
{-# SPECIALIZE medianMaybe :: Expr (Maybe Int64) -> Expr Double #-}
{-# INLINEABLE medianMaybe #-}

percentile :: Int -> Expr Double -> Expr Double
percentile n =
    Agg
        ( CollectAgg
            (Builtin (APercentile n))
            (percentile' n)
        )

stddev :: (Columnable a, Real a, VU.Unbox a) => Expr a -> Expr Double
stddev = Agg (CollectAgg (Builtin AStddev) (sqrt . variance'))
{-# SPECIALIZE stddev :: Expr Double -> Expr Double #-}
{-# SPECIALIZE stddev :: Expr Float -> Expr Double #-}
{-# SPECIALIZE stddev :: Expr Int -> Expr Double #-}
{-# SPECIALIZE stddev :: Expr Int8 -> Expr Double #-}
{-# SPECIALIZE stddev :: Expr Int16 -> Expr Double #-}
{-# SPECIALIZE stddev :: Expr Int32 -> Expr Double #-}
{-# SPECIALIZE stddev :: Expr Int64 -> Expr Double #-}
{-# INLINEABLE stddev #-}

stddevMaybe :: forall a. (Columnable a, Real a) => Expr (Maybe a) -> Expr Double
stddevMaybe =
    Agg
        (CollectAgg (Builtin AStddevMaybe) (sqrt . variance' . optionalToDoubleVector))
{-# SPECIALIZE stddevMaybe :: Expr (Maybe Double) -> Expr Double #-}
{-# SPECIALIZE stddevMaybe :: Expr (Maybe Float) -> Expr Double #-}
{-# SPECIALIZE stddevMaybe :: Expr (Maybe Int) -> Expr Double #-}
{-# SPECIALIZE stddevMaybe :: Expr (Maybe Int8) -> Expr Double #-}
{-# SPECIALIZE stddevMaybe :: Expr (Maybe Int16) -> Expr Double #-}
{-# SPECIALIZE stddevMaybe :: Expr (Maybe Int32) -> Expr Double #-}
{-# SPECIALIZE stddevMaybe :: Expr (Maybe Int64) -> Expr Double #-}
{-# INLINEABLE stddevMaybe #-}

zScore :: Expr Double -> Expr Double
zScore c = (c - mean c) / stddev c

pow :: (Columnable a, Num a) => Expr a -> Int -> Expr a
pow expr i = lift2Builtin BPow (^) (Just "^") False 8 expr (Lit i)
{-# SPECIALIZE pow :: Expr Double -> Int -> Expr Double #-}
{-# SPECIALIZE pow :: Expr Float -> Int -> Expr Float #-}
{-# SPECIALIZE pow :: Expr Int -> Int -> Expr Int #-}
{-# INLINEABLE pow #-}

relu :: (Columnable a, Num a, Ord a) => Expr a -> Expr a
relu = liftBuiltin URelu (Prelude.max 0) Nothing
{-# SPECIALIZE relu :: Expr Double -> Expr Double #-}
{-# SPECIALIZE relu :: Expr Float -> Expr Float #-}
{-# SPECIALIZE relu :: Expr Int -> Expr Int #-}
{-# INLINEABLE relu #-}

min :: (Columnable a, Ord a) => Expr a -> Expr a -> Expr a
min = lift2Builtin BMin Prelude.min Nothing True 1
{-# SPECIALIZE DataFrame.Functions.min ::
    Expr Double -> Expr Double -> Expr Double
    #-}
{-# SPECIALIZE DataFrame.Functions.min ::
    Expr Float -> Expr Float -> Expr Float
    #-}
{-# SPECIALIZE DataFrame.Functions.min :: Expr Int -> Expr Int -> Expr Int #-}
{-# INLINEABLE DataFrame.Functions.min #-}

max :: (Columnable a, Ord a) => Expr a -> Expr a -> Expr a
max = lift2Builtin BMax Prelude.max Nothing True 1
{-# SPECIALIZE DataFrame.Functions.max ::
    Expr Double -> Expr Double -> Expr Double
    #-}
{-# SPECIALIZE DataFrame.Functions.max ::
    Expr Float -> Expr Float -> Expr Float
    #-}
{-# SPECIALIZE DataFrame.Functions.max :: Expr Int -> Expr Int -> Expr Int #-}
{-# INLINEABLE DataFrame.Functions.max #-}

reduce ::
    forall a b.
    (Columnable a, Columnable b) =>
    Expr b ->
    a ->
    (a -> b -> a) ->
    Expr a
reduce expr start f = Agg (FoldAgg (Anonymous "foldUdf") (Just start) f) expr
{-# INLINEABLE reduce #-}

toMaybe :: (Columnable a) => Expr a -> Expr (Maybe a)
toMaybe = liftBuiltin UToMaybe Just Nothing
{-# SPECIALIZE toMaybe :: Expr Double -> Expr (Maybe Double) #-}
{-# SPECIALIZE toMaybe :: Expr Float -> Expr (Maybe Float) #-}
{-# SPECIALIZE toMaybe :: Expr Int -> Expr (Maybe Int) #-}
{-# INLINEABLE toMaybe #-}

fromMaybe :: (Columnable a) => a -> Expr (Maybe a) -> Expr a
fromMaybe d = liftBuiltin (UFromMaybe (T.pack (show d))) (Maybe.fromMaybe d) Nothing
{-# SPECIALIZE fromMaybe :: Double -> Expr (Maybe Double) -> Expr Double #-}
{-# SPECIALIZE fromMaybe :: Float -> Expr (Maybe Float) -> Expr Float #-}
{-# SPECIALIZE fromMaybe :: Int -> Expr (Maybe Int) -> Expr Int #-}
{-# INLINEABLE fromMaybe #-}

isJust :: (Columnable a) => Expr (Maybe a) -> Expr Bool
isJust = liftBuiltin UIsJust Maybe.isJust Nothing
{-# SPECIALIZE isJust :: Expr (Maybe Double) -> Expr Bool #-}
{-# SPECIALIZE isJust :: Expr (Maybe Int) -> Expr Bool #-}
{-# INLINEABLE isJust #-}

isNothing :: (Columnable a) => Expr (Maybe a) -> Expr Bool
isNothing = liftBuiltin UIsNothing Maybe.isNothing Nothing
{-# SPECIALIZE isNothing :: Expr (Maybe Double) -> Expr Bool #-}
{-# SPECIALIZE isNothing :: Expr (Maybe Int) -> Expr Bool #-}
{-# INLINEABLE isNothing #-}

-- | SQL spelling of 'isNothing'.
isNull :: (Columnable a) => Expr (Maybe a) -> Expr Bool
isNull = isNothing
{-# INLINEABLE isNull #-}

-- | SQL spelling of 'isJust'.
isNotNull :: (Columnable a) => Expr (Maybe a) -> Expr Bool
isNotNull = isJust
{-# INLINEABLE isNotNull #-}

fromJust :: (Columnable a) => Expr (Maybe a) -> Expr a
fromJust = liftBuiltin UFromJust Maybe.fromJust Nothing
{-# SPECIALIZE fromJust :: Expr (Maybe Double) -> Expr Double #-}
{-# SPECIALIZE fromJust :: Expr (Maybe Int) -> Expr Int #-}
{-# INLINEABLE fromJust #-}

whenPresent ::
    forall a b.
    (Columnable a, Columnable b) =>
    (a -> b) ->
    Expr (Maybe a) ->
    Expr (Maybe b)
whenPresent f = Unary (MkUnaryOp (fmap f) (Anonymous "whenPresent") Nothing)
{-# INLINEABLE whenPresent #-}

whenBothPresent ::
    forall a b c.
    (Columnable a, Columnable b, Columnable c) =>
    (a -> b -> c) ->
    Expr (Maybe a) ->
    Expr (Maybe b) ->
    Expr (Maybe c)
whenBothPresent f =
    Binary
        (MkBinaryOp (\l r -> f <$> l <*> r) (Anonymous "whenBothPresent") Nothing False 0)
{-# INLINEABLE whenBothPresent #-}

recode ::
    forall a b.
    (Columnable a, Columnable b, Show (a, b)) =>
    [(a, b)] ->
    Expr a ->
    Expr (Maybe b)
recode mapping =
    Unary
        ( MkUnaryOp
            { unaryFn = (`lookup` mapping)
            , unaryId = Builtin (URecode (T.pack (show mapping)))
            , unarySymbol = Nothing
            }
        )

recodeWithCondition ::
    forall a b.
    (Columnable a, Columnable b) =>
    Expr b ->
    [(Expr a -> Expr Bool, b)] ->
    Expr a ->
    Expr b
recodeWithCondition fallback [] _val = fallback
recodeWithCondition fallback ((cond, val) : rest) expr = ifThenElse (cond expr) (lit val) (recodeWithCondition fallback rest expr)

recodeWithDefault ::
    forall a b.
    (Columnable a, Columnable b, Show (a, b)) =>
    b ->
    [(a, b)] ->
    Expr a ->
    Expr b
recodeWithDefault d mapping =
    Unary
        ( MkUnaryOp
            { unaryFn = Maybe.fromMaybe d . (`lookup` mapping)
            , unaryId =
                Builtin (URecodeWithDefault (T.pack (show d)) (T.pack (show mapping)))
            , unarySymbol = Nothing
            }
        )

firstOrNothing :: (Columnable a) => Expr [a] -> Expr (Maybe a)
firstOrNothing = liftBuiltin UFirstOrNothing Maybe.listToMaybe Nothing

lastOrNothing :: (Columnable a) => Expr [a] -> Expr (Maybe a)
lastOrNothing = liftBuiltin ULastOrNothing (Maybe.listToMaybe . reverse) Nothing

-- | Whether the text contains the literal substring.
contains :: T.Text -> Expr T.Text -> Expr Bool
contains needle expr =
    lift2Builtin BContains (flip T.isInfixOf) Nothing False 4 expr (Lit needle)

-- | Character count.
length :: Expr T.Text -> Expr Int
length = liftBuiltin UTextLength T.length Nothing

splitOn :: T.Text -> Expr T.Text -> Expr [T.Text]
splitOn delim = liftBuiltin (USplitOn delim) (T.splitOn delim) Nothing

match :: T.Text -> Expr T.Text -> Expr (Maybe T.Text)
match regex =
    liftBuiltin
        (UMatch regex)
        ((\r -> if T.null r then Nothing else Just r) . (=~ regex))
        Nothing

matchAll :: T.Text -> Expr T.Text -> Expr [T.Text]
matchAll regex =
    liftBuiltin
        (UMatchAll regex)
        (getAllTextMatches . (=~ regex))
        Nothing

parseDate ::
    (ParseTime t, Columnable t) => T.Text -> Expr T.Text -> Expr (Maybe t)
parseDate format =
    liftBuiltin
        (UParseDate format)
        (parseTimeM True defaultTimeLocale (T.unpack format) . T.unpack)
        Nothing

daysBetween :: Expr Day -> Expr Day -> Expr Int
daysBetween =
    lift2Builtin
        BDaysBetween
        (\d1 d2 -> fromIntegral (diffDays d1 d2))
        Nothing
        False
        2

bind ::
    forall a b m.
    (Columnable a, Columnable (m a), Monad m, Columnable b, Columnable (m b)) =>
    (a -> m b) ->
    Expr (m a) ->
    Expr (m b)
bind f = Unary (MkUnaryOp (>>= f) (Anonymous "bind") Nothing)

-- | Window function: evaluate an expression partitioned by the given columns.
over :: (Columnable a) => [T.Text] -> Expr a -> Expr a
over = Over
