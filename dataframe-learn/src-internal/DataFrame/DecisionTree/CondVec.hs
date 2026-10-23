{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- | Cached condition truth vectors and the per-fit cache keyed by form.
module DataFrame.DecisionTree.CondVec (
    CondVec (..),
    materializeCondVec,
    CondCache,
    condCacheKey,
    condCacheFromVecs,
    addTreeCondsToCache,
    lookupCondVec,
    partitionByVec,
    countErrorsByVec,
    consolidateThreshold,
    combineAndVec,
    combineOrVec,
) where

import DataFrame.DecisionTree.Types (CarePoint (..), Direction (..), Tree (..))
import qualified DataFrame.Functions as F
import DataFrame.Internal.Column (TypedColumn (..), toVector)
import DataFrame.Internal.DataFrame (DataFrame)
import DataFrame.Internal.Expression (
    BinOpId (..),
    BinaryOp (binaryId),
    Expr (..),
    builtinOf,
    eqExpr,
    structuralKey,
 )
import DataFrame.Internal.Interpreter (interpret)

import qualified Data.Map.Strict as M
import Data.Maybe (fromMaybe)
import qualified Data.Text as T
import Data.Type.Equality (testEquality, (:~:) (..))
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import Type.Reflection (typeRep)

-- | A boolean condition paired with its truth vector over the full DataFrame.
data CondVec = CondVec
    { cvExpr :: !(Expr Bool)
    , cvVec :: !(VU.Vector Bool)
    }

-- | Interpret a condition once over the DataFrame; Nothing on failure.
materializeCondVec :: DataFrame -> Expr Bool -> Maybe CondVec
materializeCondVec df cond = case interpret @Bool df cond of
    Left _ -> Nothing
    Right (TColumn column) -> CondVec cond <$> eitherToMaybe (toVector @Bool @VU.Vector column)

eitherToMaybe :: Either e a -> Maybe a
eitherToMaybe = either (const Nothing) Just

-- | Truth vectors keyed by structural form, read-only once built.
type CondCache = M.Map T.Text (VU.Vector Bool)

-- | Structural key; Nothing for conditions with anonymous operators.
condCacheKey :: Expr Bool -> Maybe T.Text
condCacheKey = fmap T.pack . structuralKey

-- | Seed a cache from already-materialized candidate 'CondVec's (no interpret).
condCacheFromVecs :: [CondVec] -> CondCache
condCacheFromVecs cvs =
    M.fromList [(k, cvVec cv) | cv <- cvs, Just k <- [condCacheKey (cvExpr cv)]]

-- | Add a tree's branch-condition vectors to a cache.
addTreeCondsToCache :: DataFrame -> Tree a -> CondCache -> CondCache
addTreeCondsToCache df = go
  where
    go (Leaf _) c = c
    go (Branch cond l r) c = go r (go l (insertCond df cond c))

insertCond :: DataFrame -> Expr Bool -> CondCache -> CondCache
insertCond df cond c = case condCacheKey cond of
    Nothing -> c
    Just k
        | M.member k c -> c
        | otherwise ->
            maybe c (\cv -> M.insert k (cvVec cv) c) (materializeCondVec df cond)

-- | A condition's truth vector: a cache hit, else interpret.
lookupCondVec :: CondCache -> DataFrame -> Expr Bool -> Maybe (VU.Vector Bool)
lookupCondVec cache df cond = case (`M.lookup` cache) =<< condCacheKey cond of
    hit@(Just _) -> hit
    Nothing -> cvVec <$> materializeCondVec df cond

-- | Partition row indices by a truth vector: True goes left.
partitionByVec :: VU.Vector Bool -> V.Vector Int -> (V.Vector Int, V.Vector Int)
partitionByVec boolVals = V.partition (boolVals VU.!)

-- | Count care points the truth vector routes to the wrong child.
countErrorsByVec :: VU.Vector Bool -> [CarePoint] -> Int
countErrorsByVec boolVals = length . filter misrouted
  where
    misrouted cp = (boolVals VU.! cpIndex cp) /= (cpCorrectDir cp == GoLeft)

-- | A same-column same-direction Double threshold comparison.
data ThreshCmp = ThreshCmp
    { tcCol :: !T.Text
    , tcOp :: !BinOpId
    , tcThr :: !Double
    , tcRebuild :: Double -> Expr Bool
    }

asDoubleThreshold :: Expr Bool -> Maybe ThreshCmp
asDoubleThreshold (Binary op (Col c :: Expr cc) (Lit (t :: tt))) =
    case ( testEquality (typeRep @cc) (typeRep @Double)
         , testEquality (typeRep @tt) (typeRep @Double)
         , builtinOf (binaryId op)
         ) of
        (Just Refl, Just Refl, Just tag) ->
            Just (ThreshCmp c tag t (Binary op (Col c) . Lit))
        _ -> Nothing
asDoubleThreshold _ = Nothing

directionalOps :: [BinOpId]
directionalOps = [BLt, BLeq, BGt, BGeq]

-- | Tighter (AND) or looser (OR) of two same-direction thresholds.
chooseThreshold :: Bool -> BinOpId -> Double -> Double -> Double
chooseThreshold isAnd op t1 t2
    | leftDir = if isAnd then min t1 t2 else max t1 t2
    | otherwise = if isAnd then max t1 t2 else min t1 t2
  where
    leftDir = op == BLt || op == BLeq

-- | Collapse two same-direction Double comparisons into one, if possible.
consolidateThreshold :: Bool -> Expr Bool -> Expr Bool -> Maybe (Expr Bool)
consolidateThreshold isAnd ea eb = do
    a <- asDoubleThreshold ea
    b <- asDoubleThreshold eb
    if tcCol a == tcCol b && tcOp a == tcOp b && tcOp a `elem` directionalOps
        then Just (tcRebuild a (chooseThreshold isAnd (tcOp a) (tcThr a) (tcThr b)))
        else Nothing

-- | AND-combine two cached conditions, consolidating thresholds if possible.
combineAndVec :: CondVec -> CondVec -> CondVec
combineAndVec a b
    | eqExpr (cvExpr a) (cvExpr b) = a
    | otherwise = CondVec expr (VU.zipWith (&&) (cvVec a) (cvVec b))
  where
    expr =
        fromMaybe
            (F.and (cvExpr a) (cvExpr b))
            (consolidateThreshold True (cvExpr a) (cvExpr b))

-- | OR-combine two cached conditions (see combineAndVec).
combineOrVec :: CondVec -> CondVec -> CondVec
combineOrVec a b
    | eqExpr (cvExpr a) (cvExpr b) = a
    | otherwise = CondVec expr (VU.zipWith (||) (cvVec a) (cvVec b))
  where
    expr =
        fromMaybe
            (F.or (cvExpr a) (cvExpr b))
            (consolidateThreshold False (cvExpr a) (cvExpr b))
