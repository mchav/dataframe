{-# LANGUAGE DisambiguateRecordFields #-}
{-# LANGUAGE OverloadedStrings #-}

module DataFrame.Lazy.Internal.Optimizer (optimize) where

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Set as S
import qualified Data.Text as T
import qualified DataFrame.Internal.Expression as E
import DataFrame.Lazy.Internal.LogicalPlan
import DataFrame.Lazy.Internal.PhysicalPlan
import DataFrame.Operations.Permutation (sortOrderColumns)
import DataFrame.Schema (Schema (..), elements)

-- | Optimise a logical plan and lower it to a physical plan.
optimize :: Int -> LogicalPlan -> PhysicalPlan
optimize batchSz =
    toPhysical batchSz
        . eliminateDeadColumns
        . pushPredicates
        . fuseFilters

-- | Merge @Filter p1 (Filter p2 child)@ into @Filter (p1 && p2) child@.
fuseFilters :: LogicalPlan -> LogicalPlan
fuseFilters (Filter p1 (Filter p2 child)) =
    fuseFilters (Filter (andExpr p1 p2) (fuseFilters child))
fuseFilters (Filter p child) = Filter p (fuseFilters child)
fuseFilters (Project cols child) = Project cols (fuseFilters child)
fuseFilters (Derive name expr child) = Derive name expr (fuseFilters child)
fuseFilters (Join jt ks left right) =
    Join jt ks (fuseFilters left) (fuseFilters right)
fuseFilters (Aggregate keys aggs child) =
    Aggregate keys aggs (fuseFilters child)
fuseFilters (Sort cols child) = Sort cols (fuseFilters child)
fuseFilters (Limit n child) = Limit n (fuseFilters child)
fuseFilters plan = mapChildren fuseFilters plan

-- | Logical AND of two @Bool@ expressions.
andExpr :: E.Expr Bool -> E.Expr Bool -> E.Expr Bool
andExpr =
    E.Binary
        ( E.MkBinaryOp
            { E.binaryFn = (&&)
            , E.binaryId = E.Builtin E.BAnd
            , E.binarySymbol = Just "&&"
            , E.binaryCommutative = True
            , E.binaryPrecedence = 3
            }
        )

-- | Push Filter nodes as close to the Scan as possible.
pushPredicates :: LogicalPlan -> LogicalPlan
pushPredicates (Filter p (Derive name expr child))
    | name `notElem` E.getColumns p =
        Derive name expr (pushPredicates (Filter p child))
    | otherwise =
        Filter p (Derive name expr (pushPredicates child))
pushPredicates (Filter p (Project cols child))
    | all (`elem` cols) (E.getColumns p) =
        Project cols (pushPredicates (Filter p child))
    | otherwise =
        Filter p (Project cols (pushPredicates child))
pushPredicates (Filter p (MapBatch lbl eff f child))
    | commutes p eff = MapBatch lbl eff f (pushPredicates (Filter p child))
pushPredicates (Filter p child) = Filter p (pushPredicates child)
pushPredicates (Project cols child) = Project cols (pushPredicates child)
pushPredicates (Derive name expr child) = Derive name expr (pushPredicates child)
pushPredicates (Join jt ks left right) =
    Join jt ks (pushPredicates left) (pushPredicates right)
pushPredicates (Aggregate keys aggs child) =
    Aggregate keys aggs (pushPredicates child)
pushPredicates (Sort cols child) = Sort cols (pushPredicates child)
pushPredicates (Limit n child) = Limit n (pushPredicates child)
pushPredicates plan = mapChildren pushPredicates plan

-- | A filter moves below a row-local node it neither reads from nor feeds.
commutes :: E.Expr Bool -> ColumnEffect -> Bool
commutes p eff = case effectReads eff of
    Nothing -> False
    Just _ -> all (`notElem` effectWrites eff) (E.getColumns p)

-- | Rebuild a node with an optimisation applied to each of its children.
mapChildren :: (LogicalPlan -> LogicalPlan) -> LogicalPlan -> LogicalPlan
mapChildren f plan = case plan of
    Project cols c -> Project cols (f c)
    Filter p c -> Filter p (f c)
    Derive name e c -> Derive name e (f c)
    Join jt ks l r -> Join jt ks (f l) (f r)
    Aggregate ks aggs c -> Aggregate ks aggs (f c)
    Sort ords c -> Sort ords (f c)
    Limit n c -> Limit n (f c)
    Rename pairs c -> Rename pairs (f c)
    Offset n c -> Offset n (f c)
    MapBatch lbl eff g c -> MapBatch lbl eff g (f c)
    Barrier lbl eff g c -> Barrier lbl eff g (f c)
    Barrier2 lbl g l r -> Barrier2 lbl g (f l) (f r)
    Scan{} -> plan
    SourceDF{} -> plan

-- | Collect every column name referenced anywhere in the plan.
referencedCols :: LogicalPlan -> Maybe (S.Set T.Text)
referencedCols (Scan _ schema) = Just (S.fromList (M.keys (elements schema)))
referencedCols (Project cols _) = Just (S.fromList cols)
referencedCols (Filter p child) =
    fmap (S.union (S.fromList (E.getColumns p))) (referencedCols child)
referencedCols (Derive _ expr child) =
    fmap (S.union (S.fromList (someExprCols expr))) (referencedCols child)
referencedCols (Join _ ks left right) =
    let keySet = S.fromList (map fst ks <> map snd ks)
        lRef = fmap (S.union keySet) (referencedCols left)
        rRef = fmap (S.union keySet) (referencedCols right)
     in S.union <$> lRef <*> rRef
referencedCols (Aggregate keys aggs child) =
    let aggCols = S.fromList (keys <> concatMap (someExprCols . snd) aggs)
     in fmap (S.union aggCols) (referencedCols child)
referencedCols (Sort ords child) =
    fmap
        (S.union (S.fromList (concatMap sortOrderColumns ords)))
        (referencedCols child)
referencedCols (Limit _ child) = referencedCols child
referencedCols (Rename pairs child) =
    fmap
        (S.union (S.fromList (concatMap (\(o, n) -> [o, n]) pairs)))
        (referencedCols child)
referencedCols (Offset _ child) = referencedCols child
referencedCols (MapBatch _ eff _ child) = effectCols eff child
referencedCols (Barrier _ eff _ child) = effectCols eff child
referencedCols (Barrier2{}) = Nothing
referencedCols (SourceDF _) = Nothing

effectCols :: ColumnEffect -> LogicalPlan -> Maybe (S.Set T.Text)
effectCols eff child = do
    rs <- effectReads eff
    S.union (S.fromList (rs <> effectWrites eff)) <$> referencedCols child

someExprCols :: E.SomeExpr -> [T.Text]
someExprCols (E.SomeExpr expr) = E.getColumns expr

-- | Drop @Derive@ nodes whose output column is never consumed downstream.
eliminateDeadColumns :: LogicalPlan -> LogicalPlan
eliminateDeadColumns plan = go (referencedCols plan) plan
  where
    go needed (Derive name expr child) =
        case needed of
            Nothing -> Derive name expr (go needed child)
            Just cols
                | name `S.notMember` cols -> go needed child
                | otherwise ->
                    Derive
                        name
                        expr
                        (go (Just (S.union cols (S.fromList (someExprCols expr)))) child)
    go needed (Filter p child) =
        Filter p (go (fmap (S.union (S.fromList (E.getColumns p))) needed) child)
    go _needed (Project cols child) =
        Project cols (go (Just (S.fromList cols)) child)
    go needed (Join jt ks left right) =
        let leftNeeded = fmap (S.union (S.fromList (map fst ks))) needed
            rightNeeded = fmap (S.union (S.fromList (map snd ks))) needed
         in Join jt ks (go leftNeeded left) (go rightNeeded right)
    go needed (Aggregate keys aggs child) =
        let aggCols =
                fmap (S.union (S.fromList (keys <> concatMap (someExprCols . snd) aggs))) needed
         in Aggregate keys aggs (go aggCols child)
    go needed (Sort ords child) =
        Sort
            ords
            (go (fmap (S.union (S.fromList (concatMap sortOrderColumns ords))) needed) child)
    go needed (Limit n child) = Limit n (go needed child)
    go needed (Rename pairs child) =
        let back c = maybe c fst (L.find ((== c) . snd) pairs)
         in Rename pairs (go (fmap (S.map back) needed) child)
    go needed (Offset n child) = Offset n (go needed child)
    go needed (MapBatch lbl eff f child) =
        MapBatch lbl eff f (go (throughEffect eff needed) child)
    go needed (Barrier lbl eff f child) =
        Barrier lbl eff f (go (throughEffect eff needed) child)
    go _needed (Barrier2 lbl f l r) = Barrier2 lbl f (go Nothing l) (go Nothing r)
    go needed (Scan ds schema) =
        case needed of
            Nothing -> Scan ds schema
            Just cols ->
                Scan ds (Schema (M.filterWithKey (\k _ -> k `S.member` cols) (elements schema)))
    go _ (SourceDF df) = SourceDF df

-- | Columns needed below a generic node, given those needed above it.
throughEffect :: ColumnEffect -> Maybe (S.Set T.Text) -> Maybe (S.Set T.Text)
throughEffect eff needed = do
    rs <- effectReads eff
    above <- needed
    pure
        ((above `S.difference` S.fromList (effectWrites eff)) `S.union` S.fromList rs)

-- | Lower the optimised logical plan to a physical plan.
toPhysical :: Int -> LogicalPlan -> PhysicalPlan
-- A Filter directly on a Scan is folded into the scan's pushdown predicate.
toPhysical batchSz (Filter p (Scan (CsvSource path sep reader) schema)) =
    PhysicalScan
        (CsvSource path sep reader)
        (ScanConfig batchSz sep schema (Just p))
toPhysical batchSz (Scan (CsvSource path sep reader) schema) =
    PhysicalScan
        (CsvSource path sep reader)
        (ScanConfig batchSz sep schema Nothing)
toPhysical batchSz (Filter p (Scan (CsvSourceStreaming path sep reader) schema)) =
    PhysicalScan
        (CsvSourceStreaming path sep reader)
        (ScanConfig batchSz sep schema (Just p))
toPhysical batchSz (Scan (CsvSourceStreaming path sep reader) schema) =
    PhysicalScan
        (CsvSourceStreaming path sep reader)
        (ScanConfig batchSz sep schema Nothing)
toPhysical batchSz (Filter p (Scan (CsvSourceStreamingBytes path sep reader) schema)) =
    PhysicalScan
        (CsvSourceStreamingBytes path sep reader)
        (ScanConfig batchSz sep schema (Just p))
toPhysical batchSz (Scan (CsvSourceStreamingBytes path sep reader) schema) =
    PhysicalScan
        (CsvSourceStreamingBytes path sep reader)
        (ScanConfig batchSz sep schema Nothing)
toPhysical batchSz (Filter p (Scan (ParquetSource path) schema)) =
    PhysicalScan
        (ParquetSource path)
        (ScanConfig batchSz ',' schema (Just p))
toPhysical batchSz (Scan (ParquetSource path) schema) =
    PhysicalScan
        (ParquetSource path)
        (ScanConfig batchSz ',' schema Nothing)
toPhysical batchSz (Project cols child) =
    PhysicalProject cols (toPhysical batchSz child)
toPhysical batchSz (Filter p child) =
    PhysicalFilter p (toPhysical batchSz child)
toPhysical batchSz (Derive name expr child) =
    PhysicalDerive name expr (toPhysical batchSz child)
toPhysical batchSz (Join jt ks left right) =
    PhysicalHashJoin jt ks (toPhysical batchSz left) (toPhysical batchSz right)
toPhysical batchSz (Aggregate keys aggs child) =
    PhysicalHashAggregate keys aggs (toPhysical batchSz child)
toPhysical batchSz (Sort ords child) =
    PhysicalSort ords (toPhysical batchSz child)
toPhysical batchSz (Limit n child) =
    PhysicalLimit n (toPhysical batchSz child)
toPhysical batchSz (Rename pairs child) =
    PhysicalRename pairs (toPhysical batchSz child)
toPhysical batchSz (Offset n child) = PhysicalOffset n (toPhysical batchSz child)
toPhysical batchSz (MapBatch lbl _ f child) =
    PhysicalMapBatch lbl f (toPhysical batchSz child)
toPhysical batchSz (Barrier lbl _ f child) =
    PhysicalBarrier lbl f (toPhysical batchSz child)
toPhysical batchSz (Barrier2 lbl f l r) =
    PhysicalBarrier2 lbl f (toPhysical batchSz l) (toPhysical batchSz r)
toPhysical batchSz (SourceDF df) = PhysicalSourceDF batchSz df
