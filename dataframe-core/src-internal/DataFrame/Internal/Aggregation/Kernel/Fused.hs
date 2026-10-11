{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- | Fused multi-reduction kernels: several reductions in one pass.
module DataFrame.Internal.Aggregation.Kernel.Fused (
    FusedAgg,
    mkFusedAgg,
    runFusedAggs,
    GatherAgg,
    mkGatherAgg,
    runGatherAggs,
) where

import Control.Exception (evaluate)
import Control.Monad (replicateM)
import Data.Type.Equality (TestEquality (..), type (:~:) (Refl))
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import System.IO.Unsafe (unsafePerformIO)
import Type.Reflection (typeRep)

import DataFrame.Internal.Aggregation.Kernel.Scatter (
    groupRangeBounds,
    groupSlices,
    overGroupsAcc,
    streamGroupCap,
 )
import DataFrame.Internal.Aggregation.Reduction (Reduction (..))
import DataFrame.Internal.Column (Column (..), fromUnboxedVector)
import DataFrame.Internal.Control.Concurrent (
    capabilities,
    forkJoin,
    parThreshold,
    parallelBounds_,
    shouldParallelize,
 )

-- | Whether to fan out at this row count.
shouldPar :: Int -> Bool
shouldPar = shouldParallelize parThreshold

-- | One fused reduction of a multi-reduction streaming pass.
data FusedAgg
    = forall s.
        FusedAgg
        (IO s)
        -- \^ allocate one worker's accumulator
        (s -> Int -> Int -> IO ())
        -- \^ accumulate rows [lo, hi) in ascending order
        (s -> s -> Int -> Int -> IO ())
        -- \^ merge the second accumulator into the first over groups [lo, hi)
        (s -> IO Column)

-- \^ finalize the merged accumulator

-- | Below this many groups a Double sum/mean skips the fused streaming pass.
fusedSeqFloatGroups :: Int
fusedSeqFloatGroups = 65536

-- | Build the fused-pass reduction for a pair, or 'Nothing' if it can't stream.
mkFusedAgg :: Int -> VU.Vector Int -> Reduction -> Column -> Maybe FusedAgg
mkFusedAgg nGroups rtg red col
    | nGroups <= 0 || nGroups > streamGroupCap = Nothing
    | otherwise = case col of
        UnboxedColumn Nothing (v :: VU.Vector a)
            | Just Refl <- testEquality (typeRep @a) (typeRep @Int) ->
                case red of
                    RCount -> Just (countFusedAgg nGroups rtg)
                    RSum -> Just (sumIntFusedAgg nGroups rtg v)
                    RMean -> Just (meanIntFusedAgg nGroups rtg v)
                    RMin -> Just (extremaIntFusedAgg True nGroups rtg v)
                    RMax -> Just (extremaIntFusedAgg False nGroups rtg v)
                    _ -> Nothing
            | Just Refl <- testEquality (typeRep @a) (typeRep @Double) ->
                case red of
                    RCount -> Just (countFusedAgg nGroups rtg)
                    RSum
                        | nGroups > fusedSeqFloatGroups ->
                            Just (sumDblFusedAgg nGroups rtg v)
                    RMean
                        | nGroups > fusedSeqFloatGroups ->
                            Just (meanDblFusedAgg nGroups rtg v)
                    RMin -> Just (extremaDblFusedAgg True nGroups rtg v)
                    RMax -> Just (extremaDblFusedAgg False nGroups rtg v)
                    _ -> Nothing
        _ -> Nothing

-- | Execute all fused reductions in one pass over @rowToGroup@ and values.
runFusedAggs :: Int -> Int -> [FusedAgg] -> [Column]
runFusedAggs n nGroups aggs = unsafePerformIO $ do
    let !caps' = if shouldPar n then capabilities else 1
        !per = (max 1 n + caps' - 1) `div` caps'
    opened <- mapM (openFusedAgg caps' nGroups) aggs

    let stepsFor w = map (\(steps, _) -> steps !! w) opened
    _ <-
        forkJoin
            [ blockRun (stepsFor w) lo hi
            | w <- [0 .. caps' - 1]
            , let lo = min n (w * per)
            , let hi = min n (lo + per)
            ]
    mapM snd opened
{-# NOINLINE runFusedAggs #-}

-- | Open one fused reduction for @caps'@ workers: step functions and merge.
openFusedAgg :: Int -> Int -> FusedAgg -> IO ([Int -> Int -> IO ()], IO Column)
openFusedAgg caps' nGroups (FusedAgg new step mergeR fin) = do
    ss <- replicateM caps' new
    let finish = case ss of
            [] -> error "runFusedAggs: no workers"
            (s0 : rest) -> do
                _ <-
                    forkJoin
                        [ mapM_ (\s -> mergeR s0 s lo hi) rest
                        | (lo, hi) <- groupSlices nGroups
                        ]
                fin s0
    pure (map step ss, finish)

-- | Rows per fused block: the block's rowToGroup slice (32KB) stays in L1.
fusedBlock :: Int
fusedBlock = 4096

blockRun :: [Int -> Int -> IO ()] -> Int -> Int -> IO ()
blockRun steps lo0 hi = go lo0
  where
    go !lo
        | lo >= hi = pure ()
        | otherwise = do
            let !e = min hi (lo + fusedBlock)
            mapM_ (\s -> s lo e) steps
            go e

-- Individual fused reductions. Each step loop is monomorphic.

countFusedAgg :: Int -> VU.Vector Int -> FusedAgg
countFusedAgg nGroups rtg =
    FusedAgg
        (VUM.replicate nGroups (0 :: Int))
        (countStepK rtg)
        addIntRange
        (fmap fromUnboxedVector . VU.unsafeFreeze)

countStepK :: VU.Vector Int -> VUM.IOVector Int -> Int -> Int -> IO ()
countStepK rtg acc lo hi = go lo
  where
    go !i
        | i >= hi = pure ()
        | otherwise = do
            let !k = VU.unsafeIndex rtg i
            c <- VUM.unsafeRead acc k
            VUM.unsafeWrite acc k (c + 1)
            go (i + 1)

sumIntFusedAgg :: Int -> VU.Vector Int -> VU.Vector Int -> FusedAgg
sumIntFusedAgg nGroups rtg v =
    FusedAgg
        (VUM.replicate nGroups (0 :: Int))
        (sumStepInt rtg v)
        addIntRange
        (fmap fromUnboxedVector . VU.unsafeFreeze)

sumStepInt ::
    VU.Vector Int -> VU.Vector Int -> VUM.IOVector Int -> Int -> Int -> IO ()
sumStepInt rtg v acc lo hi = go lo
  where
    go !i
        | i >= hi = pure ()
        | otherwise = do
            let !k = VU.unsafeIndex rtg i
            c <- VUM.unsafeRead acc k
            VUM.unsafeWrite acc k (c + VU.unsafeIndex v i)
            go (i + 1)

sumDblFusedAgg :: Int -> VU.Vector Int -> VU.Vector Double -> FusedAgg
sumDblFusedAgg nGroups rtg v =
    FusedAgg
        (VUM.replicate nGroups (0 :: Double))
        (sumStepDbl rtg v)
        addDblRange
        (fmap fromUnboxedVector . VU.unsafeFreeze)

sumStepDbl ::
    VU.Vector Int -> VU.Vector Double -> VUM.IOVector Double -> Int -> Int -> IO ()
sumStepDbl rtg v acc lo hi = go lo
  where
    go !i
        | i >= hi = pure ()
        | otherwise = do
            let !k = VU.unsafeIndex rtg i
            c <- VUM.unsafeRead acc k
            VUM.unsafeWrite acc k (c + VU.unsafeIndex v i)
            go (i + 1)

-- | Mean aggs keep their (sum, count) pair interleaved in one array.
meanIntFusedAgg :: Int -> VU.Vector Int -> VU.Vector Int -> FusedAgg
meanIntFusedAgg nGroups rtg v =
    FusedAgg
        (VUM.replicate (2 * nGroups) (0 :: Int))
        (meanStepInt rtg v)
        (\a b lo hi -> addIntRange a b (2 * lo) (2 * hi))
        ( \s -> do
            sv <- VU.unsafeFreeze s
            pure
                ( fromUnboxedVector
                    ( VU.generate
                        nGroups
                        ( \g ->
                            let !sx = VU.unsafeIndex sv (2 * g)
                                !cx = VU.unsafeIndex sv (2 * g + 1)
                             in if cx == 0
                                    then 0 / 0
                                    else fromIntegral sx / fromIntegral cx :: Double
                        )
                    )
                )
        )

meanStepInt ::
    VU.Vector Int ->
    VU.Vector Int ->
    VUM.IOVector Int ->
    Int ->
    Int ->
    IO ()
meanStepInt rtg v s lo hi = go lo
  where
    go !i
        | i >= hi = pure ()
        | otherwise = do
            let !k2 = 2 * VU.unsafeIndex rtg i
            sv <- VUM.unsafeRead s k2
            VUM.unsafeWrite s k2 (sv + VU.unsafeIndex v i)
            cv <- VUM.unsafeRead s (k2 + 1)
            VUM.unsafeWrite s (k2 + 1) (cv + 1)
            go (i + 1)

-- | See 'meanIntFusedAgg' for the interleaved accumulator layout.
meanDblFusedAgg :: Int -> VU.Vector Int -> VU.Vector Double -> FusedAgg
meanDblFusedAgg nGroups rtg v =
    FusedAgg
        (VUM.replicate (2 * nGroups) (0 :: Double))
        (meanStepDbl rtg v)
        (\a b lo hi -> addDblRange a b (2 * lo) (2 * hi))
        ( \s -> do
            sv <- VU.unsafeFreeze s
            pure
                ( fromUnboxedVector
                    ( VU.generate
                        nGroups
                        ( \g ->
                            let !sx = VU.unsafeIndex sv (2 * g)
                                !cx = VU.unsafeIndex sv (2 * g + 1)
                             in if cx == 0 then 0 / 0 else sx / cx
                        )
                    )
                )
        )

meanStepDbl ::
    VU.Vector Int ->
    VU.Vector Double ->
    VUM.IOVector Double ->
    Int ->
    Int ->
    IO ()
meanStepDbl rtg v s lo hi = go lo
  where
    go !i
        | i >= hi = pure ()
        | otherwise = do
            let !k2 = 2 * VU.unsafeIndex rtg i
            sv <- VUM.unsafeRead s k2
            VUM.unsafeWrite s k2 (sv + VU.unsafeIndex v i)
            cv <- VUM.unsafeRead s (k2 + 1)
            VUM.unsafeWrite s (k2 + 1) (cv + 1)
            go (i + 1)

extremaIntFusedAgg :: Bool -> Int -> VU.Vector Int -> VU.Vector Int -> FusedAgg
extremaIntFusedAgg isMin nGroups rtg v =
    FusedAgg
        (VUM.replicate nGroups (if isMin then maxBound else minBound :: Int))
        (extremaStepInt isMin rtg v)
        (combineIntRange isMin)
        (fmap fromUnboxedVector . VU.unsafeFreeze)

extremaStepInt ::
    Bool ->
    VU.Vector Int ->
    VU.Vector Int ->
    VUM.IOVector Int ->
    Int ->
    Int ->
    IO ()
extremaStepInt isMin rtg v acc lo hi = go lo
  where
    go !i
        | i >= hi = pure ()
        | otherwise = do
            let !k = VU.unsafeIndex rtg i
                !x = VU.unsafeIndex v i
            c <- VUM.unsafeRead acc k
            VUM.unsafeWrite acc k (if isMin then min c x else max c x)
            go (i + 1)

extremaDblFusedAgg ::
    Bool -> Int -> VU.Vector Int -> VU.Vector Double -> FusedAgg
extremaDblFusedAgg isMin nGroups rtg v =
    FusedAgg
        (VUM.replicate nGroups (if isMin then 1 / 0 else negate (1 / 0) :: Double))
        (extremaStepDbl isMin rtg v)
        (combineDblRange isMin)
        (fmap fromUnboxedVector . VU.unsafeFreeze)

extremaStepDbl ::
    Bool ->
    VU.Vector Int ->
    VU.Vector Double ->
    VUM.IOVector Double ->
    Int ->
    Int ->
    IO ()
extremaStepDbl isMin rtg v acc lo hi = go lo
  where
    go !i
        | i >= hi = pure ()
        | otherwise = do
            let !k = VU.unsafeIndex rtg i
                !x = VU.unsafeIndex v i
            c <- VUM.unsafeRead acc k
            VUM.unsafeWrite acc k (if isMin then min c x else max c x)
            go (i + 1)

addIntRange :: VUM.IOVector Int -> VUM.IOVector Int -> Int -> Int -> IO ()
addIntRange a b lo hi = go lo
  where
    go !g
        | g >= hi = pure ()
        | otherwise = do
            x <- VUM.unsafeRead a g
            y <- VUM.unsafeRead b g
            VUM.unsafeWrite a g (x + y)
            go (g + 1)

addDblRange :: VUM.IOVector Double -> VUM.IOVector Double -> Int -> Int -> IO ()
addDblRange a b lo hi = go lo
  where
    go !g
        | g >= hi = pure ()
        | otherwise = do
            x <- VUM.unsafeRead a g
            y <- VUM.unsafeRead b g
            VUM.unsafeWrite a g (x + y)
            go (g + 1)

combineIntRange ::
    Bool -> VUM.IOVector Int -> VUM.IOVector Int -> Int -> Int -> IO ()
combineIntRange isMin a b lo hi = go lo
  where
    go !g
        | g >= hi = pure ()
        | otherwise = do
            x <- VUM.unsafeRead a g
            y <- VUM.unsafeRead b g
            VUM.unsafeWrite a g (if isMin then min x y else max x y)
            go (g + 1)

combineDblRange ::
    Bool -> VUM.IOVector Double -> VUM.IOVector Double -> Int -> Int -> IO ()
combineDblRange isMin a b lo hi = go lo
  where
    go !g
        | g >= hi = pure ()
        | otherwise = do
            x <- VUM.unsafeRead a g
            y <- VUM.unsafeRead b g
            VUM.unsafeWrite a g (if isMin then min x y else max x y)
            go (g + 1)

-- | One fused reduction of a multi-reduction gather pass.
data GatherAgg
    = forall s.
        GatherAgg
        (IO s)
        (s -> Int -> Int -> IO ())
        (s -> IO Column)

-- | Build the fused gather reduction for a pair, or 'Nothing' if it can't fuse.
mkGatherAgg ::
    Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    Reduction ->
    Column ->
    Maybe GatherAgg
mkGatherAgg nGroups vis offs red col = case col of
    UnboxedColumn Nothing (v :: VU.Vector a)
        | Just Refl <- testEquality (typeRep @a) (typeRep @Int) ->
            case red of
                RCount -> Just countGather
                RSum -> Just (outGather (gatherSumInt vis offs v))
                RMean -> Just (outGatherD (gatherMeanInt vis offs v))
                RMin -> Just (outGather (gatherExtremaInt True vis offs v))
                RMax -> Just (outGather (gatherExtremaInt False vis offs v))
                _ -> Nothing
        | Just Refl <- testEquality (typeRep @a) (typeRep @Double) ->
            case red of
                RCount -> Just countGather
                RSum -> Just (outGatherD (gatherSumDbl vis offs v))
                RMean -> Just (outGatherD (gatherMeanDbl vis offs v))
                RMin -> Just (outGatherD (gatherExtremaDbl True vis offs v))
                RMax -> Just (outGatherD (gatherExtremaDbl False vis offs v))
                _ -> Nothing
    _ -> Nothing
  where
    countGather =
        GatherAgg
            (VUM.new nGroups)
            (gatherCount offs)
            (fmap fromUnboxedVector . VU.unsafeFreeze)
    outGather step =
        GatherAgg
            (VUM.new nGroups :: IO (VUM.IOVector Int))
            step
            (fmap fromUnboxedVector . VU.unsafeFreeze)
    outGatherD step =
        GatherAgg
            (VUM.new nGroups :: IO (VUM.IOVector Double))
            step
            (fmap fromUnboxedVector . VU.unsafeFreeze)

-- | Groups each fused-gather block hands to every reduction before moving on.
gatherBlock :: Int
gatherBlock = 32

-- | Execute all fused gather reductions in one traversal over group ranges.
runGatherAggs ::
    VU.Vector Int -> VU.Vector Int -> Int -> [GatherAgg] -> [Column]
runGatherAggs vis offs nGroups aggs = unsafePerformIO $ do
    _ <- evaluate (VU.length vis)
    let !caps = capabilities
        !bounds = groupRangeBounds offs nGroups caps
    opened <-
        mapM (\(GatherAgg new step fin) -> do s <- new; pure (step s, fin s)) aggs
    parallelBounds_ caps bounds $ \gs ge ->
        let go !g
                | g >= ge = pure ()
                | otherwise = do
                    let !e = min ge (g + gatherBlock)
                    mapM_ (\(st, _) -> st g e) opened
                    go e
         in go gs
    mapM snd opened
{-# NOINLINE runGatherAggs #-}

-- Gather folds replicate their unfused kernels' per-group recurrence exactly.

gatherCount :: VU.Vector Int -> VUM.IOVector Int -> Int -> Int -> IO ()
gatherCount offs out gs ge = go gs
  where
    go !g
        | g >= ge = pure ()
        | otherwise = do
            let !c = VU.unsafeIndex offs (g + 1) - VU.unsafeIndex offs g
            VUM.unsafeWrite out g c
            go (g + 1)

gatherSumInt ::
    VU.Vector Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    VUM.IOVector Int ->
    Int ->
    Int ->
    IO ()
gatherSumInt vis offs v out gs ge =
    overGroupsAcc vis offs gs ge 0 (\acc row -> acc + VU.unsafeIndex v row) $
        VUM.unsafeWrite out

gatherSumDbl ::
    VU.Vector Int ->
    VU.Vector Int ->
    VU.Vector Double ->
    VUM.IOVector Double ->
    Int ->
    Int ->
    IO ()
gatherSumDbl vis offs v out gs ge =
    overGroupsAcc vis offs gs ge 0 (\acc row -> acc + VU.unsafeIndex v row) $
        VUM.unsafeWrite out

gatherExtremaInt ::
    Bool ->
    VU.Vector Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    VUM.IOVector Int ->
    Int ->
    Int ->
    IO ()
gatherExtremaInt isMin vis offs v out gs ge =
    overGroupsAcc
        vis
        offs
        gs
        ge
        (if isMin then maxBound else minBound)
        ( \acc row ->
            let !x = VU.unsafeIndex v row
             in if isMin then min acc x else max acc x
        )
        (VUM.unsafeWrite out)

gatherExtremaDbl ::
    Bool ->
    VU.Vector Int ->
    VU.Vector Int ->
    VU.Vector Double ->
    VUM.IOVector Double ->
    Int ->
    Int ->
    IO ()
gatherExtremaDbl isMin vis offs v out gs ge =
    overGroupsAcc
        vis
        offs
        gs
        ge
        (if isMin then 1 / 0 else negate (1 / 0))
        ( \acc row ->
            let !x = VU.unsafeIndex v row
             in if isMin then min acc x else max acc x
        )
        (VUM.unsafeWrite out)

-- | Exact replica of 'meanPar''s per-group loop (Int element type).
gatherMeanInt ::
    VU.Vector Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    VUM.IOVector Double ->
    Int ->
    Int ->
    IO ()
gatherMeanInt vis offs v out gs ge = grp gs
  where
    grp !g
        | g >= ge = pure ()
        | otherwise = do
            let !e = VU.unsafeIndex offs (g + 1)
                inner !pos !acc
                    | pos >= e = acc
                    | otherwise =
                        inner
                            (pos + 1)
                            (acc + fromIntegral (VU.unsafeIndex v (VU.unsafeIndex vis pos)))
                !s0 = VU.unsafeIndex offs g
                !total = inner s0 0
                !c = e - s0
            VUM.unsafeWrite out g (if c == 0 then 0 / 0 else total / fromIntegral c)
            grp (g + 1)

-- | Exact replica of 'meanPar''s per-group loop (Double element type).
gatherMeanDbl ::
    VU.Vector Int ->
    VU.Vector Int ->
    VU.Vector Double ->
    VUM.IOVector Double ->
    Int ->
    Int ->
    IO ()
gatherMeanDbl vis offs v out gs ge = grp gs
  where
    grp !g
        | g >= ge = pure ()
        | otherwise = do
            let !e = VU.unsafeIndex offs (g + 1)
                inner !pos !acc
                    | pos >= e = acc
                    | otherwise =
                        inner
                            (pos + 1)
                            (acc + VU.unsafeIndex v (VU.unsafeIndex vis pos))
                !s0 = VU.unsafeIndex offs g
                !total = inner s0 0
                !c = e - s0
            VUM.unsafeWrite out g (if c == 0 then 0 / 0 else total / fromIntegral c)
            grp (g + 1)
