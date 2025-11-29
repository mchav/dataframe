{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module DataFrame.Lazy.Internal.DataFrame where

import Control.Monad (foldM)
import qualified Data.List as L
import qualified Data.Text as T
import System.Directory (createDirectoryIfMissing, getTemporaryDirectory)
import System.FilePath ((</>))

import qualified DataFrame.IO.Unstable.CSV as CSV
import qualified DataFrame.Internal.Column as C
import qualified DataFrame.Internal.DataFrame as D
import qualified DataFrame.Internal.Expression as E
import qualified DataFrame.Lazy.IO.CSV as LCSV
import qualified DataFrame.Operations.Aggregation as A
import DataFrame.Operations.Merge ()
import qualified DataFrame.Operations.Subset as D
import qualified DataFrame.Operations.Transformations as D

data LazyOperation where
    Derive :: (C.Columnable a) => T.Text -> E.Expr a -> LazyOperation
    Select :: [T.Text] -> LazyOperation
    Filter :: E.Expr Bool -> LazyOperation
    GroupByAggregate :: [T.Text] -> [E.NamedExpr] -> LazyOperation

instance Show LazyOperation where
    show :: LazyOperation -> String
    show (Derive name expr) = T.unpack name ++ " := " ++ show expr
    show (Select columns) = "select(" ++ show columns ++ ")"
    show (Filter expr) = "filter(" ++ show expr ++ ")"
    show (GroupByAggregate keys aggs) =
        "groupBy(" ++ show keys ++ ").aggregate(" ++ show aggs ++ ")"

data InputType = ICSV deriving (Show)

data SpillConfig = SpillConfig
    { spillThresholdRows :: !Int
    , spillDirectory :: !(Maybe FilePath)
    }
    deriving (Show)

data LazyDataFrame = LazyDataFrame
    { inputPath :: FilePath
    , inputType :: InputType
    , operations :: [LazyOperation]
    , batchSize :: Int
    , spillConfig :: !(Maybe SpillConfig)
    }
    deriving (Show)

eval :: LazyOperation -> D.DataFrame -> D.DataFrame
eval (Derive name expr) = D.derive name expr
eval (Select columns) = D.select columns
eval (Filter expr) = D.filterWhere expr
eval (GroupByAggregate _ _) =
    error "GroupByAggregate should not be evaluated per batch"

applyOps :: [LazyOperation] -> D.DataFrame -> D.DataFrame
applyOps ops df = L.foldl' (flip eval) df ops

runDataFrame :: LazyDataFrame -> IO D.DataFrame
runDataFrame df = do
    let path = inputPath df

    (totalRows, shards) <- LCSV.shardCsv (batchSize df) path

    let (perBatchOps, mAggPlan) = splitAggregation (operations df)

    (baseDf, spilledFiles) <-
        if null perBatchOps
            then do
                case shards of
                    (first : rest) -> do
                        firstDf <- CSV.readCsvUnstable first
                        pure (firstDf, rest)
                    [] -> pure (D.empty, [])
            else streamAndMaybeSpill df perBatchOps totalRows shards

    case mAggPlan of
        Nothing -> pure baseDf
        Just (keys, namedAggs) -> do
            let aggregated = A.aggregate namedAggs (A.groupBy keys baseDf)
            groupAllSpilled (keys, namedAggs) spilledFiles aggregated

groupAllSpilled ::
    ([T.Text], [E.NamedExpr]) -> [String] -> D.DataFrame -> IO D.DataFrame
groupAllSpilled _ [] df = pure df
groupAllSpilled (keys, namedAggs) (path : rest) aggregated = do
    df <- CSV.readCsvUnstable path
    putStrLn $ "Aggregating shard: " ++ path
    let otherAggregated = A.aggregate namedAggs (A.groupBy keys df)
    let combined = aggregated <> otherAggregated

    -- Crude logic here to replace old column names with new aggregated names.
    -- Sadly, this also assumes that grouping can be done stepwise
    -- which is true for reductions/folds
    -- but isn't true in general of aggregations.
    let combinedAggFunctions = replaceWithAccumulators namedAggs
    let combinedAggregated = A.aggregate combinedAggFunctions (A.groupBy keys combined)
    groupAllSpilled (keys, namedAggs) rest combinedAggregated

replaceWithAccumulators :: [E.NamedExpr] -> [E.NamedExpr]
replaceWithAccumulators [] = []
replaceWithAccumulators ((accName, E.Wrap expr) : rest) =
    let
        colName = case E.getColumns expr of
            [c] -> c
            _ -> error "Aggregations must only be in terms of one column"
     in
        (accName, E.Wrap (E.renameColumn accName colName expr))
            : replaceWithAccumulators rest

splitAggregation ::
    [LazyOperation] ->
    ([LazyOperation], Maybe ([T.Text], [E.NamedExpr]))
splitAggregation ops =
    case reverse ops of
        (GroupByAggregate keys aggs : restRev) ->
            (reverse restRev, Just (keys, aggs))
        _ ->
            (ops, Nothing)

streamAndMaybeSpill ::
    LazyDataFrame ->
    -- | per-batch ops (no GroupByAggregate)
    [LazyOperation] ->
    -- | total rows
    Int ->
    -- | shards
    [String] ->
    IO (D.DataFrame, [FilePath])
streamAndMaybeSpill df ops totalRows shards = do
    let cfg = spillConfig df

    (finalAcc, spillFiles, _) <-
        foldM
            (stepBatch df cfg ops totalRows)
            (D.empty, [], 0 :: Int)
            shards

    pure (finalAcc, spillFiles)

stepBatch ::
    LazyDataFrame ->
    Maybe SpillConfig ->
    [LazyOperation] ->
    Int ->
    ( D.DataFrame
    , [FilePath]
    , Int
    ) ->
    String ->
    IO
        ( D.DataFrame
        , [FilePath]
        , Int
        )
stepBatch
    df
    mSpillCfg
    ops
    totalRows
    (accDf, spillFiles, spillCounter)
    shard = do
        putStrLn $
            "Scanning: "
                ++ shard

        sdf <- CSV.readCsvUnstable shard

        let rdf = applyOps ops sdf
            accDf' = accDf <> rdf

        (accDf'', spillFiles', spillCounter') <-
            maybe
                (pure (accDf', spillFiles, spillCounter))
                (\spillCfg -> enforceSpillThreshold spillCfg accDf' spillFiles spillCounter)
                mSpillCfg

        pure (accDf'', spillFiles', spillCounter')

enforceSpillThreshold ::
    SpillConfig ->
    D.DataFrame ->
    [FilePath] ->
    Int ->
    IO (D.DataFrame, [FilePath], Int)
enforceSpillThreshold spillCfg accDf spillFiles spillCounter = do
    let (rows, _) = D.dataframeDimensions accDf
        threshold = spillThresholdRows spillCfg

    if rows < threshold
        then pure (accDf, spillFiles, spillCounter)
        else do
            dir <- ensureSpillDir (spillDirectory spillCfg)
            let path = dir </> ("lazy_chunk_" ++ show spillCounter ++ ".csv")
            putStrLn $ "Spilling df to: " ++ path
            LCSV.writeCsv path accDf
            pure (D.empty, path : spillFiles, spillCounter + 1)

flushAccumulatorIfNeeded ::
    SpillConfig ->
    D.DataFrame ->
    [FilePath] ->
    Int ->
    IO (D.DataFrame, [FilePath])
flushAccumulatorIfNeeded spillCfg accDf spillFiles spillCounter = do
    let (rows, _) = D.dataframeDimensions accDf
    if rows == 0
        then pure (D.empty, spillFiles)
        else do
            dir <- ensureSpillDir (spillDirectory spillCfg)
            let path = dir </> ("lazy_chunk_" ++ show spillCounter ++ ".csv")
            LCSV.writeCsv path accDf
            pure (D.empty, path : spillFiles)

ensureSpillDir :: Maybe FilePath -> IO FilePath
ensureSpillDir mdir = do
    dir <- maybe getTemporaryDirectory pure mdir
    createDirectoryIfMissing True dir
    pure dir

scanCsv :: T.Text -> LazyDataFrame
scanCsv path =
    LazyDataFrame
        { inputPath = T.unpack path
        , inputType = ICSV
        , operations = []
        , batchSize = 512_000
        , spillConfig = Nothing
        }

addOperation :: LazyOperation -> LazyDataFrame -> LazyDataFrame
addOperation op df = df{operations = operations df ++ [op]}

derive ::
    (C.Columnable a) =>
    T.Text -> E.Expr a -> LazyDataFrame -> LazyDataFrame
derive name expr = addOperation (Derive name expr)

select :: (C.Columnable a) => [T.Text] -> LazyDataFrame -> LazyDataFrame
select columns = addOperation (Select columns)

filter :: (C.Columnable a) => E.Expr Bool -> LazyDataFrame -> LazyDataFrame
filter cond = addOperation (Filter cond)

groupByAggregate ::
    [T.Text] ->
    [E.NamedExpr] ->
    LazyDataFrame ->
    LazyDataFrame
groupByAggregate keys aggs = addOperation (GroupByAggregate keys aggs)

withSpill :: Int -> LazyDataFrame -> LazyDataFrame
withSpill threshold df =
    df{spillConfig = Just (SpillConfig threshold Nothing)}

withSpillIn :: FilePath -> Int -> LazyDataFrame -> LazyDataFrame
withSpillIn dir threshold df =
    df{spillConfig = Just (SpillConfig threshold (Just dir))}
