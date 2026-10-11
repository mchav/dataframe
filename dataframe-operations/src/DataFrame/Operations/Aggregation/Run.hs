{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- | Execute a recognised aggregation plan, producing one result column.
module DataFrame.Operations.Aggregation.Run (
    runPlan,
    runMomentPlan,
    runMedianVarFused,
) where

import qualified Data.Text as T
import qualified Data.Vector.Algorithms.Intro as VA
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Concurrent (getNumCapabilities)
import Data.Type.Equality (TestEquality (..), type (:~:) (Refl))
import DataFrame.Internal.Aggregation.Kernel.Dense (
    denseMaxMinusMin,
    denseReduce,
 )
import DataFrame.Internal.Aggregation.Kernel.Distinct (countDistinctGroups)
import DataFrame.Internal.Aggregation.Kernel.Moments (
    Moments (..),
    momentScatterPar,
    momentStreamPar,
 )
import DataFrame.Internal.Aggregation.Kernel.Scatter (
    maxMinusMinScatterPar,
    scatterReducePar,
    streamGroupCap,
 )
import DataFrame.Internal.Aggregation.Plan (
    AggPlan (..),
    MomentPlan (..),
 )
import DataFrame.Internal.Aggregation.Reduction (
    Reduction (..),
    cleanDoubleVector,
 )
import DataFrame.Internal.Column (Column (..), fromUnboxedVector)
import DataFrame.Internal.Control.Concurrent (parallelBounds_)
import DataFrame.Internal.DataFrame (GroupedDataFrame (..), getColumn)
import System.IO.Unsafe (unsafePerformIO)
import Type.Reflection (typeRep)

-- | Group-domain size at or below which the dense kernel is chosen.
denseThreshold :: Int
denseThreshold = 262144

runPlan :: GroupedDataFrame -> VU.Vector Int -> Int -> AggPlan -> Column
runPlan gdf rtg nGroups plan = case plan of
    PlanScatter red name -> scatterColumn red name
    PlanMaxMinusMin a b ->
        -- min/max are order-independent, so the fused kernels are exact.
        let ca = col a
            cb = col b
            direct
                | nGroups <= streamGroupCap = denseMaxMinusMin rtg nGroups ca cb
                | otherwise = maxMinusMinScatterPar vis offs nGroups ca cb
         in case direct of
                Just out -> out
                Nothing -> maxMinusMin vis offs nGroups ca cb
    PlanMedian name -> groupedMedian vis offs nGroups (col name)
    PlanCountDistinct name -> case countDistinctGroups rtg nGroups (col name) of
        Just counts -> fromUnboxedVector counts
        Nothing -> error "runPlan: countDistinctGroups rejected a planned column"
  where
    vis = valueIndices gdf
    offs = offsets gdf
    -- Dense fast path: scatter straight off the eager @rowToGroup@.
    scatterColumn red name =
        let c = col name
            dense
                | nGroups <= denseThreshold = denseReduce red rtg nGroups c
                | otherwise = Nothing
         in case dense of
                Just out -> out
                Nothing -> case scatterReducePar red vis offs nGroups c of
                    Just out -> out
                    Nothing -> error "runPlan: scatterReducePar rejected a planned column"
    col name = case getColumn name (fullDataframe gdf) of
        Just c -> c
        Nothing -> error ("runPlan: planned column missing: " ++ T.unpack name)

-- | Run a recognised moment plan as one fused scatter over two columns.
runMomentPlan ::
    GroupedDataFrame -> Int -> MomentPlan -> Maybe [(T.Text, Column)]
runMomentPlan gdf nGroups mp = do
    let cx = col (mpColX mp)
        cy = col (mpColY mp)
        -- Preferred: the streaming kernel, one fused pass over rowToGroup.
        streamed = momentStreamPar (rowToGroup gdf) nGroups cx cy
    ms <- case streamed of
        Just m -> Just m
        Nothing -> momentScatterPar vis offs nGroups cx cy
    pure
        [ (mpNName mp, mN ms)
        , (mpSxName mp, mSx ms)
        , (mpSyName mp, mSy ms)
        , (mpSxxName mp, mSxx ms)
        , (mpSyyName mp, mSyy ms)
        , (mpSxyName mp, mSxy ms)
        ]
  where
    vis = valueIndices gdf
    offs = offsets gdf
    col name = case getColumn name (fullDataframe gdf) of
        Just c -> c
        Nothing -> error ("runMomentPlan: planned column missing: " ++ T.unpack name)

-- | @max a - min b@ over the small per-group arrays.
maxMinusMin ::
    VU.Vector Int -> VU.Vector Int -> Int -> Column -> Column -> Column
maxMinusMin vis offs nGroups ca cb =
    case (ca, cb) of
        ( UnboxedColumn Nothing (_ :: VU.Vector x)
            , UnboxedColumn Nothing (_ :: VU.Vector y)
            )
                | Just Refl <- testEquality (typeRep @x) (typeRep @Int)
                , Just Refl <- testEquality (typeRep @y) (typeRep @Int) ->
                    let mx = scatterExtremaInt RMax vis offs nGroups ca
                        mn = scatterExtremaInt RMin vis offs nGroups cb
                     in fromUnboxedVector (VU.zipWith (-) mx mn)
        _ ->
            let mx = scatterExtremaDbl RMax vis offs nGroups ca
                mn = scatterExtremaDbl RMin vis offs nGroups cb
             in fromUnboxedVector (VU.zipWith (-) mx mn)

scatterExtremaInt ::
    Reduction -> VU.Vector Int -> VU.Vector Int -> Int -> Column -> VU.Vector Int
scatterExtremaInt red vis offs nGroups c = case scatterReducePar red vis offs nGroups c of
    Just (UnboxedColumn _ (v :: VU.Vector a))
        | Just Refl <- testEquality (typeRep @a) (typeRep @Int) -> v
    _ -> error "scatterExtremaInt"

scatterExtremaDbl ::
    Reduction -> VU.Vector Int -> VU.Vector Int -> Int -> Column -> VU.Vector Double
scatterExtremaDbl red vis offs nGroups c =
    case scatterReducePar red vis offs nGroups c of
        Just (UnboxedColumn _ (v :: VU.Vector a))
            | Just Refl <- testEquality (typeRep @a) (typeRep @Double) -> v
            | Just Refl <- testEquality (typeRep @a) (typeRep @Int) -> VU.map fromIntegral v
        _ -> error "scatterExtremaDbl"

-- | Fused grouped median and variance family over the same column.
runMedianVarFused ::
    GroupedDataFrame -> Int -> Column -> Maybe (Column, Column, Column)
runMedianVarFused gdf nGroups c = do
    vals <- cleanDoubleVector c
    let (med, var) =
            medianVarByGroup (valueIndices gdf) (offsets gdf) nGroups vals
    pure
        ( fromUnboxedVector med
        , fromUnboxedVector var
        , fromUnboxedVector (VU.map sqrt var)
        )

medianVarByGroup ::
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    VU.Vector Double ->
    (VU.Vector Double, VU.Vector Double)
medianVarByGroup vis offs nGroups vals = unsafePerformIO $ do
    let !n = VU.length vis
    buf <- VUM.new (max 1 n)
    medOut <- VUM.new (max 1 nGroups)
    varOut <- VUM.new (max 1 nGroups)
    caps <- getNumCapabilities
    let !bounds = groupRangeBounds offs nGroups caps
    parallelBounds_ caps bounds $ \gs ge ->
        let grp !g
                | g >= ge = pure ()
                | otherwise = do
                    let !s = VU.unsafeIndex offs g
                        !e = VU.unsafeIndex offs (g + 1)
                        !len = e - s
                        fill !pos
                            | pos >= e = pure ()
                            | otherwise = do
                                VUM.unsafeWrite buf pos (VU.unsafeIndex vals (VU.unsafeIndex vis pos))
                                fill (pos + 1)
                    fill s
                    -- Welford over the gathered slice (still in row order).
                    let welford !pos !c !mu !mm
                            | pos >= e =
                                pure (if c < 2 then 0 else mm / fromIntegral (c - 1))
                            | otherwise = do
                                x <- VUM.unsafeRead buf pos
                                let !c' = c + 1
                                    !delta = x - mu
                                    !mu' = mu + delta / fromIntegral c'
                                    !mm' = mm + delta * (x - mu')
                                welford (pos + 1) c' mu' mm'
                    var <- welford s (0 :: Int) 0 0
                    VUM.unsafeWrite varOut g var
                    -- Median selection, as in 'medianByGroup'.
                    let slice = VUM.unsafeSlice s len buf
                        !mid = len `div` 2
                    VA.select slice (mid + 1)
                    let scan !i !hi !lo
                            | i > mid = pure (hi, lo)
                            | otherwise = do
                                x <- VUM.unsafeRead slice i
                                if x > hi
                                    then scan (i + 1) x hi
                                    else scan (i + 1) hi (max lo x)
                    (hi, lo) <- scan 0 (negate (1 / 0)) (negate (1 / 0))
                    let med = if odd len then hi else (hi + lo) / 2
                    VUM.unsafeWrite medOut g med
                    grp (g + 1)
         in grp gs
    med <- VU.unsafeFreeze (VUM.unsafeSlice 0 nGroups medOut)
    var <- VU.unsafeFreeze (VUM.unsafeSlice 0 nGroups varOut)
    pure (med, var)
{-# NOINLINE medianVarByGroup #-}

-- | Holistic per-group median over a single unboxed Int or Double column.
groupedMedian :: VU.Vector Int -> VU.Vector Int -> Int -> Column -> Column
groupedMedian vis offs nGroups c = case cleanDoubleVector c of
    Nothing -> error "groupedMedian: non-numeric planned column"
    Just vals -> fromUnboxedVector (medianByGroup vis offs nGroups vals)

medianByGroup ::
    VU.Vector Int -> VU.Vector Int -> Int -> VU.Vector Double -> VU.Vector Double
medianByGroup vis offs nGroups vals = unsafePerformIO $ do
    let !n = VU.length vis
    buf <- VUM.new (max 1 n)
    out <- VUM.new (max 1 nGroups)
    caps <- getNumCapabilities
    let !bounds = groupRangeBounds offs nGroups caps
    -- Each worker sorts and takes medians of its own disjoint group range.
    parallelBounds_ caps bounds $ \gs ge ->
        let grp !g
                | g >= ge = pure ()
                | otherwise = do
                    let !s = VU.unsafeIndex offs g
                        !e = VU.unsafeIndex offs (g + 1)
                        !len = e - s
                        fill !pos
                            | pos >= e = pure ()
                            | otherwise = do
                                VUM.unsafeWrite buf pos (VU.unsafeIndex vals (VU.unsafeIndex vis pos))
                                fill (pos + 1)
                    fill s
                    let slice = VUM.unsafeSlice s len buf
                        !mid = len `div` 2
                    -- Move the least mid+1 values to the front, unordered.
                    VA.select slice (mid + 1)
                    let scan !i !hi !lo
                            | i > mid = pure (hi, lo)
                            | otherwise = do
                                x <- VUM.unsafeRead slice i
                                if x > hi
                                    then scan (i + 1) x hi
                                    else scan (i + 1) hi (max lo x)
                    (hi, lo) <- scan 0 (negate (1 / 0)) (negate (1 / 0))
                    let med = if odd len then hi else (hi + lo) / 2
                    VUM.unsafeWrite out g med
                    grp (g + 1)
         in grp gs
    VU.unsafeFreeze (VUM.unsafeSlice 0 nGroups out)
{-# NOINLINE medianByGroup #-}

-- | Split the groups into contiguous ranges balanced by row count.
groupRangeBounds :: VU.Vector Int -> Int -> Int -> VU.Vector Int
groupRangeBounds offs nGroups caps = VU.create $ do
    b <- VUM.new (caps + 1)
    let !nRows = VU.unsafeIndex offs nGroups
        !per = max 1 ((nRows + caps - 1) `div` caps)
        adv !target !gg
            | gg >= nGroups = nGroups
            | VU.unsafeIndex offs gg >= target = gg
            | otherwise = adv target (gg + 1)
        go !w !prev
            | w >= caps = VUM.unsafeWrite b caps nGroups
            | otherwise = do
                let !target = min nRows (w * per)
                    !g = adv target prev
                VUM.unsafeWrite b w g
                go (w + 1) g
    VUM.unsafeWrite b 0 0
    go 1 0
    pure b
