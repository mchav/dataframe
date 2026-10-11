{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- | The group-range scatter-accumulate reduction kernel over grouped layout.
module DataFrame.Internal.Aggregation.Kernel.Scatter (
    scatterReduce,
    scatterReducePar,
    maxMinusMinScatterPar,

    -- * Group-range helpers
    -- $shared
    groupRangeBounds,
    rtgFromVis,
    overGroupsAcc,
    groupSlices,
    streamGroupCap,
) where

import Control.Monad.ST (ST, runST)
import Data.Type.Equality (TestEquality (..), type (:~:) (Refl))
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import System.IO.Unsafe (unsafePerformIO)
import Type.Reflection (typeRep)

import DataFrame.Internal.Aggregation.Reduction (Reduction (..))
import DataFrame.Internal.Column (
    Column (..),
    Columnable,
    fromUnboxedVector,
    materializePacked,
 )
import DataFrame.Internal.Control.Concurrent (
    capabilities,
    chunksFor,
    parThreshold,
    parallelBounds_,
    shouldParallelize,
 )

-- $shared Also used by the Moments kernel, which partitions groups alike.

-- | Whether to fan out at this row count.
shouldPar :: Int -> Bool
shouldPar = shouldParallelize parThreshold

scatterReduce ::
    Reduction -> VU.Vector Int -> Int -> Column -> Maybe Column
scatterReduce red g nGroups col = case col of
    UnboxedColumn Nothing (v :: VU.Vector a) ->
        case testEquality (typeRep @a) (typeRep @Int) of
            Just Refl -> Just (reduceTyped red g nGroups v intIdent)
            Nothing -> case testEquality (typeRep @a) (typeRep @Double) of
                Just Refl -> Just (reduceTyped red g nGroups v dblIdent)
                Nothing -> Nothing
    p@(PackedText _ _) -> scatterReduce red g nGroups (materializePacked p)
    _ -> Nothing
{-# INLINEABLE scatterReduce #-}

-- | Per-type seed identities for the order-preserving reductions.
data Idents a = Idents {minSeed :: !a, maxSeed :: !a}

intIdent :: Idents Int
intIdent = Idents maxBound minBound

dblIdent :: Idents Double
dblIdent = Idents (1 / 0) (negate (1 / 0))

reduceTyped ::
    forall a.
    (Columnable a, VU.Unbox a, Num a, Ord a, Real a) =>
    Reduction -> VU.Vector Int -> Int -> VU.Vector a -> Idents a -> Column
reduceTyped red g nGroups v idents = case red of
    RCount -> fromUnboxedVector (countScatter g nGroups)
    RSum -> fromUnboxedVector (sumScatter g nGroups v)
    RMin -> fromUnboxedVector (extremaScatter min (minSeed idents) g nGroups v)
    RMax -> fromUnboxedVector (extremaScatter max (maxSeed idents) g nGroups v)
    RMean -> fromUnboxedVector (meanScatter g nGroups v)
    RVar -> fromUnboxedVector (varScatter False g nGroups v)
    RStd -> fromUnboxedVector (varScatter True g nGroups v)
{-# INLINE reduceTyped #-}

countScatter :: VU.Vector Int -> Int -> VU.Vector Int
countScatter g nGroups = runST $ do
    cnt <- VUM.replicate nGroups (0 :: Int)
    let n = VU.length g
        go !i
            | i >= n = pure ()
            | otherwise = do
                let !k = VU.unsafeIndex g i
                c <- VUM.unsafeRead cnt k
                VUM.unsafeWrite cnt k (c + 1)
                go (i + 1)
    go 0
    VU.unsafeFreeze cnt

sumScatter ::
    (VU.Unbox a, Num a) => VU.Vector Int -> Int -> VU.Vector a -> VU.Vector a
sumScatter g nGroups v = runST $ do
    s <- VUM.replicate nGroups 0
    let n = VU.length v
        go !i
            | i >= n = pure ()
            | otherwise = do
                let !k = VU.unsafeIndex g i
                cur <- VUM.unsafeRead s k
                VUM.unsafeWrite s k (cur + VU.unsafeIndex v i)
                go (i + 1)
    go 0
    VU.unsafeFreeze s
{-# INLINE sumScatter #-}

extremaScatter ::
    (VU.Unbox a) =>
    (a -> a -> a) -> a -> VU.Vector Int -> Int -> VU.Vector a -> VU.Vector a
extremaScatter combine seed g nGroups v = runST $ do
    m <- VUM.replicate nGroups seed
    let n = VU.length v
        go !i
            | i >= n = pure ()
            | otherwise = do
                let !k = VU.unsafeIndex g i
                cur <- VUM.unsafeRead m k
                VUM.unsafeWrite m k (combine cur (VU.unsafeIndex v i))
                go (i + 1)
    go 0
    VU.unsafeFreeze m
{-# INLINE extremaScatter #-}

meanScatter ::
    (VU.Unbox a, Real a) => VU.Vector Int -> Int -> VU.Vector a -> VU.Vector Double
meanScatter g nGroups v = runST $ do
    s <- VUM.replicate nGroups (0 :: Double)
    cnt <- VUM.replicate nGroups (0 :: Int)
    scatterSumCount g v s cnt
    finalizeMean nGroups s cnt
{-# INLINE meanScatter #-}

scatterSumCount ::
    (VU.Unbox a, Real a) =>
    VU.Vector Int ->
    VU.Vector a ->
    VUM.MVector s Double ->
    VUM.MVector s Int ->
    ST s ()
scatterSumCount g v s cnt = go 0
  where
    n = VU.length v
    go !i
        | i >= n = pure ()
        | otherwise = do
            let !k = VU.unsafeIndex g i
                !x = realToFrac (VU.unsafeIndex v i)
            curS <- VUM.unsafeRead s k
            VUM.unsafeWrite s k (curS + x)
            curC <- VUM.unsafeRead cnt k
            VUM.unsafeWrite cnt k (curC + 1)
            go (i + 1)
{-# INLINE scatterSumCount #-}

finalizeMean ::
    Int -> VUM.MVector s Double -> VUM.MVector s Int -> ST s (VU.Vector Double)
finalizeMean nGroups s cnt = do
    out <- VUM.new nGroups
    let go !k
            | k >= nGroups = pure ()
            | otherwise = do
                sv <- VUM.unsafeRead s k
                c <- VUM.unsafeRead cnt k
                VUM.unsafeWrite out k (if c == 0 then 0 / 0 else sv / fromIntegral c)
                go (k + 1)
    go 0
    VU.unsafeFreeze out

varScatter ::
    (VU.Unbox a, Real a) =>
    Bool -> VU.Vector Int -> Int -> VU.Vector a -> VU.Vector Double
varScatter takeSqrt g nGroups v = runST $ do
    cnt <- VUM.replicate nGroups (0 :: Int)
    meanV <- VUM.replicate nGroups (0 :: Double)
    m2 <- VUM.replicate nGroups (0 :: Double)
    let n = VU.length v
        go !i
            | i >= n = pure ()
            | otherwise = do
                let !k = VU.unsafeIndex g i
                    !x = realToFrac (VU.unsafeIndex v i)
                c <- VUM.unsafeRead cnt k
                mu <- VUM.unsafeRead meanV k
                mm <- VUM.unsafeRead m2 k
                let !c' = c + 1
                    !delta = x - mu
                    !mu' = mu + delta / fromIntegral c'
                    !mm' = mm + delta * (x - mu')
                VUM.unsafeWrite cnt k c'
                VUM.unsafeWrite meanV k mu'
                VUM.unsafeWrite m2 k mm'
                go (i + 1)
    go 0
    out <- VUM.new nGroups
    let fin !k
            | k >= nGroups = pure ()
            | otherwise = do
                c <- VUM.unsafeRead cnt k
                mm <- VUM.unsafeRead m2 k
                let var = if c < 2 then 0 else mm / fromIntegral (c - 1)
                VUM.unsafeWrite out k (if takeSqrt then sqrt var else var)
                fin (k + 1)
    fin 0
    VU.unsafeFreeze out
{-# INLINE varScatter #-}

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

scatterReducePar ::
    Reduction -> VU.Vector Int -> VU.Vector Int -> Int -> Column -> Maybe Column
scatterReducePar red vis offs nGroups col
    | not (shouldParallelize parThreshold (VU.length vis)) || nGroups <= 1 =
        scatterReduce red (rtgFromVis vis offs nGroups) nGroups col
    | otherwise = case col of
        UnboxedColumn Nothing (v :: VU.Vector a) ->
            case testEquality (typeRep @a) (typeRep @Int) of
                Just Refl -> Just (reduceParInt red vis offs nGroups v)
                Nothing -> case testEquality (typeRep @a) (typeRep @Double) of
                    Just Refl -> Just (reduceParDouble red vis offs nGroups v)
                    Nothing -> Nothing
        p@(PackedText _ _) -> scatterReducePar red vis offs nGroups (materializePacked p)
        _ -> Nothing
{-# NOINLINE scatterReducePar #-}

-- | Monomorphic entry points for the 'testEquality' dispatch above.
reduceParInt ::
    Reduction -> VU.Vector Int -> VU.Vector Int -> Int -> VU.Vector Int -> Column
reduceParInt red vis offs nGroups v = reduceParTyped red vis offs nGroups v intIdent

reduceParDouble ::
    Reduction -> VU.Vector Int -> VU.Vector Int -> Int -> VU.Vector Double -> Column
reduceParDouble red vis offs nGroups v = reduceParTyped red vis offs nGroups v dblIdent

rtgFromVis :: VU.Vector Int -> VU.Vector Int -> Int -> VU.Vector Int
rtgFromVis vis offs nGroups = VU.create $ do
    let n = VU.length vis
    rtg <- VUM.new (max 1 n)
    let go !g
            | g >= nGroups = pure ()
            | otherwise = do
                let !e = VU.unsafeIndex offs (g + 1)
                    inner !pos
                        | pos >= e = pure ()
                        | otherwise = do
                            VUM.unsafeWrite rtg (VU.unsafeIndex vis pos) g
                            inner (pos + 1)
                inner (VU.unsafeIndex offs g)
                go (g + 1)
    go 0
    pure rtg

reduceParTyped ::
    forall a.
    (Columnable a, VU.Unbox a, Num a, Ord a, Real a) =>
    Reduction ->
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    VU.Vector a ->
    Idents a ->
    Column
-- SPECIALIZE lets @realToFrac@ resolve at a concrete type (~4x faster).
{-# SPECIALIZE reduceParTyped ::
    Reduction ->
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    VU.Vector Int ->
    Idents Int ->
    Column
    #-}
{-# SPECIALIZE reduceParTyped ::
    Reduction ->
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    VU.Vector Double ->
    Idents Double ->
    Column
    #-}
reduceParTyped red vis offs nGroups v idents =
    let !caps = capabilities
        !bounds = groupRangeBounds offs nGroups caps
     in case red of
            RCount -> fromUnboxedVector (unsafePerformIO (countPar vis offs nGroups caps bounds))
            RSum -> fromUnboxedVector (unsafePerformIO (sumPar vis offs nGroups v caps bounds))
            RMin ->
                fromUnboxedVector
                    (unsafePerformIO (extremaPar min (minSeed idents) vis offs nGroups v caps bounds))
            RMax ->
                fromUnboxedVector
                    (unsafePerformIO (extremaPar max (maxSeed idents) vis offs nGroups v caps bounds))
            RMean -> fromUnboxedVector (unsafePerformIO (meanPar vis offs nGroups v caps bounds))
            RVar ->
                fromUnboxedVector
                    (unsafePerformIO (varPar False vis offs nGroups v caps bounds))
            RStd ->
                fromUnboxedVector (unsafePerformIO (varPar True vis offs nGroups v caps bounds))
{-# INLINEABLE reduceParTyped #-}

-- | Fold each group's rows into a register accumulator, then hand it on.
overGroupsAcc ::
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    Int ->
    acc ->
    (acc -> Int -> acc) ->
    (Int -> acc -> IO ()) ->
    IO ()
overGroupsAcc vis offs gs ge seed step done = grp gs
  where
    grp !g
        | g >= ge = pure ()
        | otherwise = do
            let !e = VU.unsafeIndex offs (g + 1)
                inner !pos !acc
                    | pos >= e = pure acc
                    | otherwise = inner (pos + 1) (step acc (VU.unsafeIndex vis pos))
            acc <- inner (VU.unsafeIndex offs g) seed
            done g acc
            grp (g + 1)
{-# INLINE overGroupsAcc #-}

countPar ::
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    Int ->
    VU.Vector Int ->
    IO (VU.Vector Int)
countPar _vis offs nGroups caps bounds = do
    out <- VUM.replicate nGroups (0 :: Int)
    parallelBounds_ caps bounds $ \gs ge ->
        let grp !g
                | g >= ge = pure ()
                | otherwise = do
                    let !c = VU.unsafeIndex offs (g + 1) - VU.unsafeIndex offs g
                    VUM.unsafeWrite out g c
                    grp (g + 1)
         in grp gs
    VU.unsafeFreeze out

sumPar ::
    (VU.Unbox a, Num a) =>
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    VU.Vector a ->
    Int ->
    VU.Vector Int ->
    IO (VU.Vector a)
sumPar vis offs nGroups v caps bounds = do
    out <- VUM.replicate nGroups 0
    parallelBounds_ caps bounds $ \gs ge ->
        overGroupsAcc vis offs gs ge 0 (\acc row -> acc + VU.unsafeIndex v row) $
            VUM.unsafeWrite out
    VU.unsafeFreeze out
{-# INLINE sumPar #-}

extremaPar ::
    (VU.Unbox a) =>
    (a -> a -> a) ->
    a ->
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    VU.Vector a ->
    Int ->
    VU.Vector Int ->
    IO (VU.Vector a)
extremaPar combine seed vis offs nGroups v caps bounds = do
    out <- VUM.replicate nGroups seed
    parallelBounds_ caps bounds $ \gs ge ->
        overGroupsAcc
            vis
            offs
            gs
            ge
            seed
            (\acc row -> combine acc (VU.unsafeIndex v row))
            $ VUM.unsafeWrite out
    VU.unsafeFreeze out
{-# INLINE extremaPar #-}

meanPar ::
    (VU.Unbox a, Real a) =>
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    VU.Vector a ->
    Int ->
    VU.Vector Int ->
    IO (VU.Vector Double)
meanPar vis offs nGroups v caps bounds = do
    out <- VUM.replicate nGroups (0 :: Double)
    parallelBounds_ caps bounds $ \gs ge ->
        let grp !g
                | g >= ge = pure ()
                | otherwise = do
                    let !e = VU.unsafeIndex offs (g + 1)
                        inner !pos !acc
                            | pos >= e = acc
                            | otherwise =
                                inner
                                    (pos + 1)
                                    (acc + realToFrac (VU.unsafeIndex v (VU.unsafeIndex vis pos)))
                        !s0 = VU.unsafeIndex offs g
                        !total = inner s0 0
                        !c = e - s0
                    VUM.unsafeWrite out g (if c == 0 then 0 / 0 else total / fromIntegral c)
                    grp (g + 1)
         in grp gs
    VU.unsafeFreeze out
{-# INLINE meanPar #-}

varPar ::
    (VU.Unbox a, Real a) =>
    Bool ->
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    VU.Vector a ->
    Int ->
    VU.Vector Int ->
    IO (VU.Vector Double)
varPar takeSqrt vis offs nGroups v caps bounds = do
    out <- VUM.replicate nGroups (0 :: Double)
    parallelBounds_ caps bounds $ \gs ge ->
        -- Per-group Welford state (count, mean, M2) is carried in registers.
        let grp !g
                | g >= ge = pure ()
                | otherwise = do
                    let !e = VU.unsafeIndex offs (g + 1)
                        inner !pos !c !mu !mm
                            | pos >= e =
                                let var = if c < 2 then 0 else mm / fromIntegral (c - 1)
                                 in if takeSqrt then sqrt var else var
                            | otherwise =
                                let !x = realToFrac (VU.unsafeIndex v (VU.unsafeIndex vis pos))
                                    !c' = c + 1
                                    !delta = x - mu
                                    !mu' = mu + delta / fromIntegral c'
                                    !mm' = mm + delta * (x - mu')
                                 in inner (pos + 1) c' mu' mm'
                        !res = inner (VU.unsafeIndex offs g) (0 :: Int) 0 0
                    VUM.unsafeWrite out g res
                    grp (g + 1)
         in grp gs
    VU.unsafeFreeze out
{-# INLINE varPar #-}

-- | Fused @max a - min b@ over the group-range layout in one traversal.
maxMinusMinScatterPar ::
    VU.Vector Int -> VU.Vector Int -> Int -> Column -> Column -> Maybe Column
maxMinusMinScatterPar vis offs nGroups ca cb
    | not (shouldPar (VU.length vis)) || nGroups <= 1 = Nothing
    | otherwise = case (ca, cb) of
        ( UnboxedColumn Nothing (va :: VU.Vector x)
            , UnboxedColumn Nothing (vb :: VU.Vector y)
            )
                | Just Refl <- testEquality (typeRep @x) (typeRep @Int)
                , Just Refl <- testEquality (typeRep @y) (typeRep @Int) ->
                    Just (maxMinusMinParInt vis offs nGroups va vb caps bounds)
                | Just Refl <- testEquality (typeRep @x) (typeRep @Double)
                , Just Refl <- testEquality (typeRep @y) (typeRep @Double) ->
                    Just (maxMinusMinParDbl vis offs nGroups va vb caps bounds)
        _ -> Nothing
  where
    !caps = capabilities
    !bounds = groupRangeBounds offs nGroups caps
{-# NOINLINE maxMinusMinScatterPar #-}

-- | Monomorphic entry points for the 'testEquality' dispatch.
maxMinusMinParInt ::
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    VU.Vector Int ->
    Column
maxMinusMinParInt vis offs nGroups va vb caps bounds =
    fromUnboxedVector
        ( unsafePerformIO
            (maxMinusMinPar minBound maxBound vis offs nGroups va vb caps bounds)
        )
{-# NOINLINE maxMinusMinParInt #-}

maxMinusMinParDbl ::
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    VU.Vector Double ->
    VU.Vector Double ->
    Int ->
    VU.Vector Int ->
    Column
maxMinusMinParDbl vis offs nGroups va vb caps bounds =
    fromUnboxedVector
        ( unsafePerformIO
            (maxMinusMinPar (negate (1 / 0)) (1 / 0) vis offs nGroups va vb caps bounds)
        )
{-# NOINLINE maxMinusMinParDbl #-}

maxMinusMinPar ::
    (VU.Unbox a, Num a, Ord a) =>
    a ->
    a ->
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    VU.Vector a ->
    VU.Vector a ->
    Int ->
    VU.Vector Int ->
    IO (VU.Vector a)
{-# SPECIALIZE maxMinusMinPar ::
    Int ->
    Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    VU.Vector Int ->
    IO (VU.Vector Int)
    #-}
{-# SPECIALIZE maxMinusMinPar ::
    Double ->
    Double ->
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    VU.Vector Double ->
    VU.Vector Double ->
    Int ->
    VU.Vector Int ->
    IO (VU.Vector Double)
    #-}
maxMinusMinPar maxSeed minSeed vis offs nGroups va vb caps bounds = do
    out <- VUM.new nGroups
    parallelBounds_ caps bounds $ \gs ge ->
        -- Both extrema live in registers; one traversal reads both columns.
        let grp !g
                | g >= ge = pure ()
                | otherwise = do
                    let !e = VU.unsafeIndex offs (g + 1)
                        inner !pos !mx !mn
                            | pos >= e = mx - mn
                            | otherwise =
                                let !row = VU.unsafeIndex vis pos
                                 in inner
                                        (pos + 1)
                                        (max mx (VU.unsafeIndex va row))
                                        (min mn (VU.unsafeIndex vb row))
                        !res = inner (VU.unsafeIndex offs g) maxSeed minSeed
                    VUM.unsafeWrite out g res
                    grp (g + 1)
         in grp gs
    VU.unsafeFreeze out

-- | Group-count cap for the fused streaming scatter kernels.
streamGroupCap :: Int
streamGroupCap = 1048576

-- | Near-equal contiguous slices of the group domain for parallel merges.
groupSlices :: Int -> [(Int, Int)]
groupSlices = chunksFor 4096
