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
    show (Select columns)   = "select(" ++ show columns ++ ")"
    show (Filter expr)      = "filter(" ++ show expr ++ ")"
    show (GroupByAggregate keys aggs) =
        "groupBy(" ++ show keys ++ ").aggregate(" ++ show aggs ++ ")"

data InputType = ICSV deriving (Show)

data SpillConfig = SpillConfig
    { spillThresholdRows :: !Int
    , spillDirectory     :: !(Maybe FilePath)
    } deriving (Show)

data LazyDataFrame = LazyDataFrame
    { inputPath  :: FilePath
    , inputType  :: InputType
    , operations :: [LazyOperation]
    , batchSize  :: Int
    , spillConfig :: !(Maybe SpillConfig)
    } deriving (Show)

eval :: LazyOperation -> D.DataFrame -> D.DataFrame
eval (Derive name expr) = D.derive name expr
eval (Select columns)   = D.select columns
eval (Filter expr)      = D.filterWhere expr
eval (GroupByAggregate _ _) =
    error "GroupByAggregate should not be evaluated per batch"

applyOps :: [LazyOperation] -> D.DataFrame -> D.DataFrame
applyOps ops df = L.foldl' (flip eval) df ops

runDataFrame :: LazyDataFrame -> IO D.DataFrame
runDataFrame df = do
    let path = inputPath df

    totalRows <- LCSV.countRows ',' path

    let (perBatchOps, mAggPlan) = splitAggregation (operations df)
        batches                  = batchRanges totalRows (batchSize df)

    (baseDf, spilledFiles) <-
        streamAndMaybeSpill df perBatchOps totalRows batches

    case mAggPlan of
        Nothing -> pure baseDf
        Just (keys, namedAggs) -> do
            let grouped = A.groupBy keys baseDf
            pure $ A.aggregate namedAggs grouped

splitAggregation
    :: [LazyOperation]
    -> ([LazyOperation], Maybe ([T.Text], [E.NamedExpr]))
splitAggregation ops =
    case reverse ops of
        (GroupByAggregate keys aggs : restRev) ->
            (reverse restRev, Just (keys, aggs))
        _ ->
            (ops, Nothing)

streamAndMaybeSpill
    :: LazyDataFrame
    -> [LazyOperation]          -- ^ per-batch ops (no GroupByAggregate)
    -> Int                      -- ^ total rows
    -> [(Int, Int)]             -- ^ batches
    -> IO (D.DataFrame, [FilePath])
streamAndMaybeSpill df ops totalRows batches = do
    let cfg = spillConfig df

    (finalAcc, _, spillFiles, _) <-
        foldM
            (stepBatch df cfg ops totalRows)
            (D.empty, (Nothing, "", 0), [], (0 :: Int))
            batches

    case cfg of
        Nothing -> pure (finalAcc, [])
        Just spillCfg -> do
            (acc0, spillFiles') <-
                flushAccumulatorIfNeeded spillCfg finalAcc spillFiles 999999
            stitched <- stitchSpilled acc0 spillFiles'
            pure (stitched, spillFiles')

stepBatch
    :: LazyDataFrame
    -> Maybe SpillConfig
    -> [LazyOperation]
    -> Int
    -> ( D.DataFrame
       , (Maybe Integer, T.Text, Int)
       , [FilePath]
       , Int
       )
    -> (Int, Int)
    -> IO
        ( D.DataFrame
        , (Maybe Integer, T.Text, Int)
        , [FilePath]
        , Int
        )
stepBatch df mSpillCfg ops totalRows
          (accDf, (pos, unused, r), spillFiles, spillCounter)
          (start, _end) = do

    putStr $
        "Scanning: "
        ++ show start
        ++ " to "
        ++ show (start + batchSize df)
        ++ " rows out of "
        ++ show totalRows
        ++ "\n"

    (sdf, (pos', unconsumed, rowsRead)) <-
        LCSV.readSeparated
            ','
            ( LCSV.defaultOptions
                { LCSV.rowRange  = Just (start, batchSize df)
                , LCSV.totalRows = Just totalRows
                , LCSV.seekPos   = pos
                , LCSV.rowsRead  = r
                , LCSV.leftOver  = unused
                }
            )
            (inputPath df)

    let rdf   = applyOps ops sdf
        accDf' = accDf <> rdf

    (accDf'', spillFiles', spillCounter') <-
        maybe
            (pure (accDf', spillFiles, spillCounter))
            (\spillCfg -> enforceSpillThreshold spillCfg accDf' spillFiles spillCounter)
            mSpillCfg

    pure (accDf'', (Just pos', unconsumed, rowsRead + r), spillFiles', spillCounter')

enforceSpillThreshold
    :: SpillConfig
    -> D.DataFrame
    -> [FilePath]
    -> Int
    -> IO (D.DataFrame, [FilePath], Int)
enforceSpillThreshold spillCfg accDf spillFiles spillCounter = do
    let (rows, _) = D.dataframeDimensions accDf
        threshold = spillThresholdRows spillCfg

    if rows < threshold
        then pure (accDf, spillFiles, spillCounter)
        else do
            dir <- ensureSpillDir (spillDirectory spillCfg)
            let path = dir </> ("lazy_chunk_" ++ show spillCounter ++ ".csv")
            LCSV.writeCsv path accDf
            pure (D.empty, path : spillFiles, spillCounter + 1)

flushAccumulatorIfNeeded
    :: SpillConfig
    -> D.DataFrame
    -> [FilePath]
    -> Int
    -> IO (D.DataFrame, [FilePath])
flushAccumulatorIfNeeded spillCfg accDf spillFiles spillCounter = do
    let (rows, _) = D.dataframeDimensions accDf
    if rows == 0
        then pure (D.empty, spillFiles)
        else do
            dir <- ensureSpillDir (spillDirectory spillCfg)
            let path = dir </> ("lazy_chunk_" ++ show spillCounter ++ ".csv")
            LCSV.writeCsv path accDf
            pure (D.empty, path : spillFiles)

stitchSpilled :: D.DataFrame -> [FilePath] -> IO D.DataFrame
stitchSpilled acc spillFiles = do
    foldM
        (\df path -> do
            chunk <- LCSV.readCsv path
            pure (df <> chunk)
        )
        acc
        (reverse spillFiles)

ensureSpillDir :: Maybe FilePath -> IO FilePath
ensureSpillDir mdir = do
    dir <- maybe getTemporaryDirectory pure mdir
    createDirectoryIfMissing True dir
    pure dir

batchRanges :: Int -> Int -> [(Int, Int)]
batchRanges n inc = go n [0, inc .. n]
  where
    go _ []           = []
    go n' [x]         = [(x, n')]
    go n' (f : s : r) = (f, s) : go n' (s : r)

scanCsv :: T.Text -> LazyDataFrame
scanCsv path =
    LazyDataFrame
        { inputPath   = T.unpack path
        , inputType   = ICSV
        , operations  = []
        , batchSize   = 512_000
        , spillConfig = Nothing
        }

addOperation :: LazyOperation -> LazyDataFrame -> LazyDataFrame
addOperation op df = df {operations = operations df ++ [op]}

derive
    :: (C.Columnable a)
    => T.Text -> E.Expr a -> LazyDataFrame -> LazyDataFrame
derive name expr = addOperation (Derive name expr)

select :: (C.Columnable a) => [T.Text] -> LazyDataFrame -> LazyDataFrame
select columns = addOperation (Select columns)

filter :: (C.Columnable a) => E.Expr Bool -> LazyDataFrame -> LazyDataFrame
filter cond = addOperation (Filter cond)

groupByAggregate
    :: [T.Text]
    -> [E.NamedExpr]
    -> LazyDataFrame
    -> LazyDataFrame
groupByAggregate keys aggs = addOperation (GroupByAggregate keys aggs)

withSpill :: Int -> LazyDataFrame -> LazyDataFrame
withSpill threshold df =
    df { spillConfig = Just (SpillConfig threshold Nothing) }

withSpillIn :: FilePath -> Int -> LazyDataFrame -> LazyDataFrame
withSpillIn dir threshold df =
    df { spillConfig = Just (SpillConfig threshold (Just dir)) }
