{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- | The aggregation fast-path planner: recognises supported aggregate shapes.
module DataFrame.Internal.Aggregation.Plan (
    AggPlan (..),
    planAgg,
    MomentPlan (..),
    planMoments,
) where

import qualified Data.Map.Strict as M
import qualified Data.Text as T
import Data.Type.Equality (TestEquality (..), type (:~:) (Refl))
import qualified Data.Vector.Unboxed as VU

import DataFrame.Internal.Aggregation.Kernel.Distinct (distinctSupported)
import DataFrame.Internal.Aggregation.Reduction (Reduction (..))
import DataFrame.Internal.Column (Column (..))
import DataFrame.Internal.DataFrame (
    DataFrame (derivingExpressions),
    GroupedDataFrame (..),
    getColumn,
 )
import DataFrame.Internal.Expression (
    AggId (..),
    AggStrategy (..),
    BinOpId (..),
    BinaryOp (binaryId),
    Expr (..),
    OpRef (..),
    SomeExpr (..),
    UnOpId (..),
    UnaryOp (unaryId),
 )
import Type.Reflection (Typeable, typeRep)

-- | The plan 'planAgg' produces for a recognised output expression.
data AggPlan
    = -- | A single scatter reduction over one named column.
      PlanScatter Reduction T.Text
    | -- | @max a - min b@ (Q7): two scatters then a vectorized combine.
      PlanMaxMinusMin T.Text T.Text
    | -- | Holistic median over one named column.
      PlanMedian T.Text
    | -- | Exact distinct count over one named column.
      PlanCountDistinct T.Text

-- | Plan a named output expression, or 'Nothing' if the shape is unsupported.
planAgg :: GroupedDataFrame -> SomeExpr -> Maybe AggPlan
planAgg gdf (SomeExpr (expr :: Expr a)) = case expr of
    Agg (FoldAgg (Builtin tag) _ _) (Col name) -> foldPlan tag name
    Agg (MergeAgg (Builtin tag) _ _ _ _) (Col name) -> mergePlan tag name
    Agg (CollectAgg (Builtin tag) _) (Col name) -> collectPlan tag name
    Binary
        op
        (Agg (FoldAgg (Builtin AMaximum) Nothing _) (Col a))
        (Agg (FoldAgg (Builtin AMinimum) Nothing _) (Col b))
            | Builtin BSub <- binaryId op -> requireBoth a b (PlanMaxMinusMin a b)
    _ -> Nothing
  where
    foldPlan tag name = case tag of
        ASum -> require name (PlanScatter RSum name)
        AMinimum -> require name (PlanScatter RMin name)
        AMaximum -> require name (PlanScatter RMax name)
        _ -> Nothing
    mergePlan tag name = case tag of
        AMean -> outputType @Double >> require name (PlanScatter RMean name)
        ACount -> outputType @Int >> require name (PlanScatter RCount name)
        ACountDistinct -> do
            outputType @Int
            c <- getColumn name (fullDataframe gdf)
            if distinctSupported c then Just (PlanCountDistinct name) else Nothing
        _ -> Nothing
    outputType :: forall t. (Typeable t) => Maybe ()
    outputType = case testEquality (typeRep @a) (typeRep @t) of
        Just Refl -> Just ()
        Nothing -> Nothing
    collectPlan tag name = case tag of
        AStddev -> require name (PlanScatter RStd name)
        AVariance -> require name (PlanScatter RVar name)
        AMedian -> require name (PlanMedian name)
        _ -> Nothing
    require name plan = colUnboxedNumeric name >> Just plan
    requireBoth a b plan = colUnboxedNumeric a >> colUnboxedNumeric b >> Just plan
    colUnboxedNumeric name = case getColumn name (fullDataframe gdf) of
        Just c | isUnboxedNumeric c -> Just ()
        _ -> Nothing

-- | The matcher only fires on non-null unboxed Int/Double columns.
isUnboxedNumeric :: Column -> Bool
isUnboxedNumeric = \case
    UnboxedColumn Nothing (_ :: VU.Vector a) ->
        case testEquality (typeRep @a) (typeRep @Int) of
            Just Refl -> True
            Nothing -> case testEquality (typeRep @a) (typeRep @Double) of
                Just Refl -> True
                Nothing -> False
    _ -> False

-- | A recognised moment aggregate group: six sufficient-statistic columns.
data MomentPlan = MomentPlan
    { mpColX :: T.Text
    , mpColY :: T.Text
    , mpNName :: T.Text
    , mpSxName :: T.Text
    , mpSyName :: T.Text
    , mpSxxName :: T.Text
    , mpSyyName :: T.Text
    , mpSxyName :: T.Text
    }

-- | The shape of a sum's argument: linear in one base column, or a product.
data Term
    = Lin T.Text
    | Prod T.Text T.Text
    deriving (Eq, Ord, Show)

-- | Recognise the moment shape across a whole @aggregate@ list.
planMoments :: GroupedDataFrame -> [(T.Text, SomeExpr)] -> Maybe MomentPlan
planMoments gdf aggs
    | length aggs /= 6 = Nothing
    | otherwise = do
        let exprs = derivingExpressions (fullDataframe gdf)
        roles <- traverse (classify exprs) aggs
        let names = M.fromList [(r, nm) | (nm, r) <- roles]
        nName <- M.lookup RoleN names
        (x, y) <- pickBaseColumns roles
        sxName <- M.lookup (RoleLin x) names
        syName <- M.lookup (RoleLin y) names
        sxxName <- M.lookup (RoleProd x x) names
        syyName <- M.lookup (RoleProd y y) names
        sxyName <- M.lookup (RoleProd x y) names
        _ <- if x /= y then Just () else Nothing
        _ <- colUnboxedNumeric x
        _ <- colUnboxedNumeric y
        pure
            MomentPlan
                { mpColX = x
                , mpColY = y
                , mpNName = nName
                , mpSxName = sxName
                , mpSyName = syName
                , mpSxxName = sxxName
                , mpSyyName = syyName
                , mpSxyName = sxyName
                }
  where
    colUnboxedNumeric name = case getColumn name (fullDataframe gdf) of
        Just c | isUnboxedNumeric c -> Just ()
        _ -> Nothing

-- | The output role each named aggregation plays in the moment shape.
data Role
    = RoleN
    | RoleLin T.Text
    | RoleProd T.Text T.Text
    deriving (Eq, Ord, Show)

-- | Tag a single named aggregation with its moment role, or reject the group.
classify :: M.Map T.Text SomeExpr -> (T.Text, SomeExpr) -> Maybe (T.Text, Role)
classify exprs (name, SomeExpr expr) = case expr of
    Agg (MergeAgg (Builtin ACount) _ _ _ _) _ -> Just (name, RoleN)
    Agg (FoldAgg (Builtin ASum) _ _) arg -> (\t -> (name, termRole t)) <$> resolveTerm exprs (SomeExpr arg)
    _ -> Nothing

termRole :: Term -> Role
termRole (Lin a) = RoleLin a
termRole (Prod a b) = RoleProd a b

-- | Resolve a sum-argument expression to its 'Term'.
resolveTerm :: M.Map T.Text SomeExpr -> SomeExpr -> Maybe Term
resolveTerm exprs = go (8 :: Int)
  where
    go 0 _ = Nothing
    go fuel (SomeExpr e) = case e of
        Col nm -> case M.lookup nm exprs of
            Just ue -> go (fuel - 1) ue
            Nothing -> Just (Lin nm)
        Unary op inner
            | Builtin UToDouble <- unaryId op -> go (fuel - 1) (SomeExpr inner)
        Binary op l r
            | Builtin BMult <- binaryId op -> do
                Lin a <- go (fuel - 1) (SomeExpr l)
                Lin b <- go (fuel - 1) (SomeExpr r)
                Just (sortProd a b)
        _ -> Nothing

-- | Products are unordered: store the pair sorted so @x*y@ and @y*x@ unify.
sortProd :: T.Text -> T.Text -> Term
sortProd a b
    | a <= b = Prod a b
    | otherwise = Prod b a

-- | Find the unordered pair of base columns that the linear sums name.
pickBaseColumns :: [(T.Text, Role)] -> Maybe (T.Text, T.Text)
pickBaseColumns roles =
    case lins of
        [a, b] | a /= b -> Just (a, b)
        _ -> Nothing
  where
    lins = M.keys (M.fromList [(c, ()) | (_, RoleLin c) <- roles])
