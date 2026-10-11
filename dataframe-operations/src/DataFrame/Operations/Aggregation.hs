{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE Strict #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Operations.Aggregation (
    module DataFrame.Operations.Aggregation,
    groupBy,
    buildRowToGroup,
    changingPoints,
) where

import qualified Data.List as L
import qualified Data.Map.Strict as MS
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import Control.Exception (throw)
import DataFrame.Errors
import DataFrame.Internal.Aggregation.Kernel.Fused (
    mkFusedAgg,
    mkGatherAgg,
    runFusedAggs,
    runGatherAggs,
 )
import DataFrame.Internal.Aggregation.Kernel.Scatter (streamGroupCap)
import DataFrame.Internal.Aggregation.Plan (
    AggPlan (..),
    MomentPlan,
    planAgg,
    planMoments,
 )
import DataFrame.Internal.Aggregation.Reduction (Reduction (..))
import DataFrame.Internal.Column (
    Column (..),
    TypedColumn (..),
    atIndicesStable,
    atIndicesStableMulti,
 )
import DataFrame.Internal.DataFrame (
    DataFrame (..),
    GroupedDataFrame (..),
    columnNames,
    getColumn,
    insertColumn,
 )
import DataFrame.Internal.Expression
import DataFrame.Internal.Grouping (buildRowToGroup, changingPoints, groupBy)
import DataFrame.Internal.Interpreter
import DataFrame.Internal.Row.RowHash (computeRowHashesIO)
import DataFrame.Operations.Aggregation.Run (
    runMedianVarFused,
    runMomentPlan,
    runPlan,
 )
import DataFrame.Operations.Core
import DataFrame.Operations.Subset
import System.IO.Unsafe (unsafePerformIO)

-- | Per-row key hash over the selected key columns, computed in parallel.
computeRowHashes :: [Int] -> DataFrame -> VU.Vector Int
computeRowHashes indices df =
    let n = fst (dimensions df)
        selectedCols = map (columns df V.!) indices
     in unsafePerformIO (computeRowHashesIO n selectedCols)
{-# NOINLINE computeRowHashes #-}

-- | Aggregate a grouped dataframe using the expressions given.
aggregate :: [NamedExpr] -> GroupedDataFrame -> DataFrame
aggregate aggs gdf =
    let
        df = fullDataframe gdf
        offs = offsets gdf

        -- Key columns: one fused parallel gather over the representative rows.
        df' =
            let sub = select (groupedColumns gdf) df
             in sub
                    { columns =
                        V.fromList
                            ( atIndicesStableMulti
                                (groupRepRows gdf)
                                (V.toList (columns sub))
                            )
                    , dataframeDimensions = (nGroups, snd (dataframeDimensions sub))
                    }

        !nGroups = VU.length offs - 1
        !nRows' = fst (dataframeDimensions df)

        -- Under -XStrict this must stay cheap when the moment path covers it.
        fusedScatterCols :: MS.Map T.Text Column
        fusedScatterCols = case fusedMoments of
            Just _ -> MS.empty
            Nothing
                | nGroups <= streamGroupCap ->
                    let cands =
                            [ (name, fa)
                            | (name, ue) <- aggs
                            , Just (PlanScatter red cname) <- [planAgg gdf ue]
                            , Just c <- [getColumn cname df]
                            , Just fa <- [mkFusedAgg nGroups (rowToGroup gdf) red c]
                            ]
                     in if length cands >= 2
                            then
                                MS.fromList
                                    (zip (map fst cands) (runFusedAggs nRows' nGroups (map snd cands)))
                            else MS.empty
                | otherwise ->
                    let cands =
                            [ (name, ga)
                            | (name, ue) <- aggs
                            , Just (PlanScatter red cname) <- [planAgg gdf ue]
                            , Just c <- [getColumn cname df]
                            , Just ga <-
                                [mkGatherAgg nGroups (valueIndices gdf) offs red c]
                            ]
                     in -- Even a single candidate uses the gather path.
                        if not (null cands)
                            then
                                MS.fromList
                                    ( zip
                                        (map fst cands)
                                        ( runGatherAggs
                                            (valueIndices gdf)
                                            offs
                                            nGroups
                                            (map snd cands)
                                        )
                                    )
                            else MS.empty

        -- Median and std/var on one column share a single gather.
        medianVarCols :: MS.Map T.Text Column
        medianVarCols = case fusedMoments of
            Just _ -> MS.empty
            Nothing ->
                let plans = [(name, plan) | (name, ue) <- aggs, Just plan <- [planAgg gdf ue]]
                    medCols = L.nub [c | (_, PlanMedian c) <- plans]
                 in MS.fromList
                        [ kv
                        | cname <- medCols
                        , let stds = [nm | (nm, PlanScatter RStd c) <- plans, c == cname]
                        , let vars = [nm | (nm, PlanScatter RVar c) <- plans, c == cname]
                        , not (null stds && null vars)
                        , Just c <- [getColumn cname df]
                        , Just (medC, varC, stdC) <- [runMedianVarFused gdf nGroups c]
                        , kv <-
                            [(nm, medC) | (nm, PlanMedian c') <- plans, c' == cname]
                                ++ [(nm, stdC) | nm <- stds]
                                ++ [(nm, varC) | nm <- vars]
                        ]

        -- Fast path: a recognised reduction scatters in one unboxed pass.
        f ne@(name, uexpr) d =
            let value = case MS.lookup name medianVarCols of
                    Just c -> c
                    Nothing -> case MS.lookup name fusedScatterCols of
                        Just c -> c
                        Nothing -> case planAgg gdf uexpr of
                            Just plan -> runPlan gdf (rowToGroup gdf) nGroups plan
                            Nothing -> interpretNamed gdf ne
             in insertColumn name value d

        -- Fused fast path: the Q9 regression family becomes one scatter.
        fusedMoments = do
            mp <- planMoments gdf aggs :: Maybe MomentPlan
            runMomentPlan gdf nGroups mp
     in
        case fusedMoments of
            Just cols -> fold (uncurry insertColumn) cols df'
            Nothing -> fold f aggs df'

-- | The fall-back path: evaluate one named aggregation via the interpreter.
interpretNamed :: GroupedDataFrame -> NamedExpr -> Column
interpretNamed gdf (_, SomeExpr (expr :: Expr a)) =
    case interpretAggregation @a gdf expr of
        Left e -> throw e
        Right (UnAggregated _) -> throw $ UnaggregatedException (T.pack $ show expr)
        Right (Aggregated (TColumn col)) -> col

selectIndices :: VU.Vector Int -> DataFrame -> DataFrame
selectIndices xs df =
    df
        { columns = V.map (atIndicesStable xs) (columns df)
        , dataframeDimensions = (VU.length xs, V.length (columns df))
        }

-- | Filter out all non-unique values in a dataframe.
distinct :: DataFrame -> DataFrame
distinct df = selectIndices (VU.map (indices VU.!) (VU.init os)) df
  where
    -- Wildcard last field: a named pattern would force the deferred thunk.
    (Grouped _ _ indices os _) = groupBy (columnNames df) df
