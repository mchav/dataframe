{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- | The low-cardinality dense reduction kernel (direct-indexed by group).
module DataFrame.Internal.Aggregation.Kernel.Dense (
    denseReduce,
    denseMaxMinusMin,
) where

import Data.Type.Equality (TestEquality (..), type (:~:) (Refl))
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import System.IO.Unsafe (unsafePerformIO)
import Type.Reflection (typeRep)

import DataFrame.Internal.Aggregation.Reduction (Reduction (..))
import DataFrame.Internal.Column (
    Column (..),
    fromUnboxedVector,
    materializePacked,
 )
import DataFrame.Internal.Control.Concurrent (
    capabilities,
    parThreshold,
    parallelChunks,
    shouldParallelize,
 )

-- | Group count at or below which float reductions run one sequential pass.
seqFloatGroups :: Int
seqFloatGroups = 65536

-- | Run a recognised reduction through the direct-indexed path, if admitted.
denseReduce :: Reduction -> VU.Vector Int -> Int -> Column -> Maybe Column
denseReduce red g nGroups col = case col of
    UnboxedColumn Nothing (v :: VU.Vector a) ->
        case testEquality (typeRep @a) (typeRep @Int) of
            Just Refl -> denseInt red g nGroups v
            Nothing -> case testEquality (typeRep @a) (typeRep @Double) of
                Just Refl -> denseDouble red g nGroups v
                Nothing -> Nothing
    p@(PackedText _ _) -> denseReduce red g nGroups (materializePacked p)
    _ -> Nothing
{-# INLINEABLE denseReduce #-}

-- | The reductions admitted over an Int column.
denseInt :: Reduction -> VU.Vector Int -> Int -> VU.Vector Int -> Maybe Column
denseInt red g nGroups v = case red of
    RCount -> Just (fromUnboxedVector (countDense g nGroups (VU.length v)))
    RSum -> Just (fromUnboxedVector (sumIntDense g nGroups v))
    RMin -> Just (fromUnboxedVector (extremaIntDense True g nGroups v))
    RMax -> Just (fromUnboxedVector (extremaIntDense False g nGroups v))
    RMean -> Just (fromUnboxedVector (meanIntDense g nGroups v))
    _ -> Nothing

-- | The reductions admitted over a Double column.
denseDouble ::
    Reduction -> VU.Vector Int -> Int -> VU.Vector Double -> Maybe Column
denseDouble red g nGroups v = case red of
    RCount -> Just (fromUnboxedVector (countDense g nGroups (VU.length v)))
    RSum -> Just (fromUnboxedVector (sumDblDense g nGroups v))
    RMean -> Just (fromUnboxedVector (meanDblDense g nGroups v))
    RMin -> Just (fromUnboxedVector (extremaDblDense True g nGroups v))
    RMax -> Just (fromUnboxedVector (extremaDblDense False g nGroups v))
    _ -> Nothing

-- | The fused @max a - min b@ direct pass over both extrema at once.
denseMaxMinusMin :: VU.Vector Int -> Int -> Column -> Column -> Maybe Column
denseMaxMinusMin g nGroups ca cb = case (ca, cb) of
    ( UnboxedColumn Nothing (va :: VU.Vector x)
        , UnboxedColumn Nothing (vb :: VU.Vector y)
        )
            | Just Refl <- testEquality (typeRep @x) (typeRep @Int)
            , Just Refl <- testEquality (typeRep @y) (typeRep @Int) ->
                Just (fromUnboxedVector (maxMinusMinDenseInt g nGroups va vb))
            | Just Refl <- testEquality (typeRep @x) (typeRep @Double)
            , Just Refl <- testEquality (typeRep @y) (typeRep @Double) ->
                Just (fromUnboxedVector (maxMinusMinDenseDbl g nGroups va vb))
    _ -> Nothing
{-# INLINEABLE denseMaxMinusMin #-}

-- | Monomorphic entry points for the 'testEquality' dispatch above.
maxMinusMinDenseInt ::
    VU.Vector Int -> Int -> VU.Vector Int -> VU.Vector Int -> VU.Vector Int
maxMinusMinDenseInt = maxMinusMinDense minBound maxBound
{-# NOINLINE maxMinusMinDenseInt #-}

maxMinusMinDenseDbl ::
    VU.Vector Int -> Int -> VU.Vector Double -> VU.Vector Double -> VU.Vector Double
maxMinusMinDenseDbl = maxMinusMinDense (negate (1 / 0)) (1 / 0)
{-# NOINLINE maxMinusMinDenseDbl #-}

-- | Whether to fan out at this row count.
shouldPar :: Int -> Bool
shouldPar = shouldParallelize parThreshold

-- | Fork workers over disjoint row ranges, each with a private accumulator.
runPartialsOver ::
    Int -> Int -> (Int -> Int -> IO acc) -> IO [acc]
runPartialsOver n _caps = parallelChunks parThreshold n

countDense :: VU.Vector Int -> Int -> Int -> VU.Vector Int
countDense g nGroups n = unsafePerformIO $ do
    parts <- runPartialsOver n capabilities (countChunk g nGroups)
    mergeIntSum nGroups parts
{-# NOINLINE countDense #-}

countChunk :: VU.Vector Int -> Int -> Int -> Int -> IO (VUM.IOVector Int)
countChunk g nGroups lo hi = do
    acc <- VUM.replicate nGroups (0 :: Int)
    let go !i
            | i >= hi = pure ()
            | otherwise = do
                let !k = VU.unsafeIndex g i
                c <- VUM.unsafeRead acc k
                VUM.unsafeWrite acc k (c + 1)
                go (i + 1)
    go lo
    pure acc

sumIntDense :: VU.Vector Int -> Int -> VU.Vector Int -> VU.Vector Int
sumIntDense g nGroups v = unsafePerformIO $ do
    parts <- runPartialsOver (VU.length v) capabilities (sumIntChunk g v nGroups)
    mergeIntSum nGroups parts
{-# NOINLINE sumIntDense #-}

sumIntChunk ::
    VU.Vector Int -> VU.Vector Int -> Int -> Int -> Int -> IO (VUM.IOVector Int)
sumIntChunk g v nGroups lo hi = do
    acc <- VUM.replicate nGroups (0 :: Int)
    let go !i
            | i >= hi = pure ()
            | otherwise = do
                let !k = VU.unsafeIndex g i
                c <- VUM.unsafeRead acc k
                VUM.unsafeWrite acc k (c + VU.unsafeIndex v i)
                go (i + 1)
    go lo
    pure acc

-- | Double group sums, sequential at small group counts.
sumDblDense :: VU.Vector Int -> Int -> VU.Vector Double -> VU.Vector Double
sumDblDense g nGroups v
    | nGroups <= seqFloatGroups || not (shouldPar n) =
        unsafePerformIO (sumDblChunk g v nGroups 0 n >>= VU.unsafeFreeze)
    | otherwise = unsafePerformIO $ do
        parts <- runPartialsOver n capabilities (sumDblChunk g v nGroups)
        mergeDblSum nGroups parts
  where
    !n = VU.length v
{-# NOINLINE sumDblDense #-}

sumDblChunk ::
    VU.Vector Int ->
    VU.Vector Double ->
    Int ->
    Int ->
    Int ->
    IO (VUM.IOVector Double)
sumDblChunk g v nGroups lo hi = do
    acc <- VUM.replicate nGroups (0 :: Double)
    let go !i
            | i >= hi = pure ()
            | otherwise = do
                let !k = VU.unsafeIndex g i
                c <- VUM.unsafeRead acc k
                VUM.unsafeWrite acc k (c + VU.unsafeIndex v i)
                go (i + 1)
    go lo
    pure acc

-- | Double mean: fused (sum, count) per group, divided once at finalize.
meanDblDense :: VU.Vector Int -> Int -> VU.Vector Double -> VU.Vector Double
meanDblDense g nGroups v
    | nGroups <= seqFloatGroups || not (shouldPar n) = unsafePerformIO $ do
        (s, c) <- meanDblChunk g v nGroups 0 n
        finalizeMeanDbl nGroups s c
    | otherwise = unsafePerformIO $ do
        parts <- runPartialsOver n capabilities (meanDblChunk g v nGroups)
        (s, c) <- mergeMeanDbl nGroups parts
        finalizeMeanDbl nGroups s c
  where
    !n = VU.length v
{-# NOINLINE meanDblDense #-}

meanDblChunk ::
    VU.Vector Int ->
    VU.Vector Double ->
    Int ->
    Int ->
    Int ->
    IO (VUM.IOVector Double, VUM.IOVector Int)
meanDblChunk g v nGroups lo hi = do
    s <- VUM.replicate nGroups (0 :: Double)
    c <- VUM.replicate nGroups (0 :: Int)
    let go !i
            | i >= hi = pure ()
            | otherwise = do
                let !k = VU.unsafeIndex g i
                sv <- VUM.unsafeRead s k
                VUM.unsafeWrite s k (sv + VU.unsafeIndex v i)
                cv <- VUM.unsafeRead c k
                VUM.unsafeWrite c k (cv + 1)
                go (i + 1)
    go lo
    pure (s, c)

finalizeMeanDbl ::
    Int -> VUM.IOVector Double -> VUM.IOVector Int -> IO (VU.Vector Double)
finalizeMeanDbl nGroups s c = do
    out <- VUM.new nGroups
    let go !k
            | k >= nGroups = pure ()
            | otherwise = do
                sv <- VUM.unsafeRead s k
                cv <- VUM.unsafeRead c k
                VUM.unsafeWrite
                    out
                    k
                    (if cv == 0 then 0 / 0 else sv / fromIntegral cv)
                go (k + 1)
    go 0
    VU.unsafeFreeze out

extremaIntDense ::
    Bool -> VU.Vector Int -> Int -> VU.Vector Int -> VU.Vector Int
extremaIntDense isMin g nGroups v = unsafePerformIO $ do
    parts <-
        runPartialsOver (VU.length v) capabilities (extremaIntChunk isMin g v nGroups)
    mergeExtremaInt isMin nGroups parts
{-# NOINLINE extremaIntDense #-}

extremaIntChunk ::
    Bool ->
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    Int ->
    Int ->
    IO (VUM.IOVector Int)
extremaIntChunk isMin g v nGroups lo hi = do
    let !seed = if isMin then maxBound else minBound
        combine a b = if isMin then min a b else max a b
    acc <- VUM.replicate nGroups seed
    let go !i
            | i >= hi = pure ()
            | otherwise = do
                let !k = VU.unsafeIndex g i
                c <- VUM.unsafeRead acc k
                VUM.unsafeWrite acc k (combine c (VU.unsafeIndex v i))
                go (i + 1)
    go lo
    pure acc

extremaDblDense ::
    Bool -> VU.Vector Int -> Int -> VU.Vector Double -> VU.Vector Double
extremaDblDense isMin g nGroups v
    | not (shouldPar n) =
        unsafePerformIO (extremaDblChunk isMin g v nGroups 0 n >>= VU.unsafeFreeze)
    | otherwise = unsafePerformIO $ do
        parts <- runPartialsOver n capabilities (extremaDblChunk isMin g v nGroups)
        mergeExtremaDbl isMin nGroups parts
  where
    !n = VU.length v
{-# NOINLINE extremaDblDense #-}

extremaDblChunk ::
    Bool ->
    VU.Vector Int ->
    VU.Vector Double ->
    Int ->
    Int ->
    Int ->
    IO (VUM.IOVector Double)
extremaDblChunk isMin g v nGroups lo hi = do
    let !seed = if isMin then 1 / 0 else negate (1 / 0)
        combine a b = if isMin then min a b else max a b
    acc <- VUM.replicate nGroups seed
    let go !i
            | i >= hi = pure ()
            | otherwise = do
                let !k = VU.unsafeIndex g i
                c <- VUM.unsafeRead acc k
                VUM.unsafeWrite acc k (combine c (VU.unsafeIndex v i))
                go (i + 1)
    go lo
    pure acc

-- | One streaming pass accumulating @max a@ and @min b@ together.
maxMinusMinDense ::
    (VU.Unbox a, Num a, Ord a) =>
    a ->
    a ->
    VU.Vector Int ->
    Int ->
    VU.Vector a ->
    VU.Vector a ->
    VU.Vector a
{-# SPECIALIZE maxMinusMinDense ::
    Int ->
    Int ->
    VU.Vector Int ->
    Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    VU.Vector Int
    #-}
{-# SPECIALIZE maxMinusMinDense ::
    Double ->
    Double ->
    VU.Vector Int ->
    Int ->
    VU.Vector Double ->
    VU.Vector Double ->
    VU.Vector Double
    #-}
maxMinusMinDense maxSeed minSeed g nGroups va vb
    | not (shouldPar n) = unsafePerformIO $ do
        (mx, mn) <- maxMinusMinChunk maxSeed minSeed g va vb nGroups 0 n
        finalizeMaxMinusMin nGroups mx mn
    | otherwise = unsafePerformIO $ do
        parts <-
            runPartialsOver
                n
                capabilities
                (maxMinusMinChunk maxSeed minSeed g va vb nGroups)
        (mx, mn) <- mergeMaxMin nGroups parts
        finalizeMaxMinusMin nGroups mx mn
  where
    !n = VU.length va
-- INLINEABLE (not NOINLINE) so the SPECIALIZE pragmas above take effect.
{-# INLINEABLE maxMinusMinDense #-}

maxMinusMinChunk ::
    (VU.Unbox a, Ord a) =>
    a ->
    a ->
    VU.Vector Int ->
    VU.Vector a ->
    VU.Vector a ->
    Int ->
    Int ->
    Int ->
    IO (VUM.IOVector a, VUM.IOVector a)
maxMinusMinChunk maxSeed minSeed g va vb nGroups lo hi = do
    mx <- VUM.replicate nGroups maxSeed
    mn <- VUM.replicate nGroups minSeed
    let go !i
            | i >= hi = pure ()
            | otherwise = do
                let !k = VU.unsafeIndex g i
                cx <- VUM.unsafeRead mx k
                VUM.unsafeWrite mx k (max cx (VU.unsafeIndex va i))
                cn <- VUM.unsafeRead mn k
                VUM.unsafeWrite mn k (min cn (VU.unsafeIndex vb i))
                go (i + 1)
    go lo
    pure (mx, mn)

mergeMaxMin ::
    (VU.Unbox a, Ord a) =>
    Int ->
    [(VUM.IOVector a, VUM.IOVector a)] ->
    IO (VUM.IOVector a, VUM.IOVector a)
mergeMaxMin nGroups parts = case parts of
    [] -> error "mergeMaxMin: no partials"
    ((mx0, mn0) : rest) -> do
        let add (mx, mn) = do
                let go !k
                        | k >= nGroups = pure ()
                        | otherwise = do
                            xa <- VUM.unsafeRead mx0 k
                            xb <- VUM.unsafeRead mx k
                            VUM.unsafeWrite mx0 k (max xa xb)
                            na <- VUM.unsafeRead mn0 k
                            nb <- VUM.unsafeRead mn k
                            VUM.unsafeWrite mn0 k (min na nb)
                            go (k + 1)
                go 0
        mapM_ add rest
        pure (mx0, mn0)

finalizeMaxMinusMin ::
    (VU.Unbox a, Num a) =>
    Int ->
    VUM.IOVector a ->
    VUM.IOVector a ->
    IO (VU.Vector a)
finalizeMaxMinusMin nGroups mx mn = do
    out <- VUM.new nGroups
    let go !k
            | k >= nGroups = pure ()
            | otherwise = do
                a <- VUM.unsafeRead mx k
                b <- VUM.unsafeRead mn k
                VUM.unsafeWrite out k (a - b)
                go (k + 1)
    go 0
    VU.unsafeFreeze out

-- | Integer mean in one fused pass: running sum and count per group.
meanIntDense :: VU.Vector Int -> Int -> VU.Vector Int -> VU.Vector Double
meanIntDense g nGroups v
    | not (shouldPar n) = unsafePerformIO $ do
        (s, c) <- meanIntChunk g v nGroups 0 n
        finalizeMeanInt nGroups s c
    | otherwise = unsafePerformIO $ do
        parts <- runPartialsOver n capabilities (meanIntChunk g v nGroups)
        (s, c) <- mergePair nGroups parts
        finalizeMeanInt nGroups s c
  where
    !n = VU.length v
{-# NOINLINE meanIntDense #-}

meanIntChunk ::
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    Int ->
    Int ->
    IO (VUM.IOVector Int, VUM.IOVector Int)
meanIntChunk g v nGroups lo hi = do
    s <- VUM.replicate nGroups (0 :: Int)
    c <- VUM.replicate nGroups (0 :: Int)
    let go !i
            | i >= hi = pure ()
            | otherwise = do
                let !k = VU.unsafeIndex g i
                sv <- VUM.unsafeRead s k
                VUM.unsafeWrite s k (sv + VU.unsafeIndex v i)
                cv <- VUM.unsafeRead c k
                VUM.unsafeWrite c k (cv + 1)
                go (i + 1)
    go lo
    pure (s, c)

finalizeMeanInt ::
    Int -> VUM.IOVector Int -> VUM.IOVector Int -> IO (VU.Vector Double)
finalizeMeanInt nGroups s c = do
    out <- VUM.new nGroups
    let go !k
            | k >= nGroups = pure ()
            | otherwise = do
                sv <- VUM.unsafeRead s k
                cv <- VUM.unsafeRead c k
                VUM.unsafeWrite
                    out
                    k
                    (if cv == 0 then 0 / 0 else fromIntegral sv / fromIntegral cv)
                go (k + 1)
    go 0
    VU.unsafeFreeze out

mergeIntSum :: Int -> [VUM.IOVector Int] -> IO (VU.Vector Int)
mergeIntSum nGroups parts = case parts of
    [] -> VU.unsafeFreeze =<< VUM.replicate nGroups 0
    (p0 : rest) -> do
        let add !p = do
                let go !k
                        | k >= nGroups = pure ()
                        | otherwise = do
                            a <- VUM.unsafeRead p0 k
                            b <- VUM.unsafeRead p k
                            VUM.unsafeWrite p0 k (a + b)
                            go (k + 1)
                go 0
        mapM_ add rest
        VU.unsafeFreeze p0

-- | Sum per-worker Double partials into the first worker's, in worker order.
mergeDblSum :: Int -> [VUM.IOVector Double] -> IO (VU.Vector Double)
mergeDblSum nGroups parts = case parts of
    [] -> VU.unsafeFreeze =<< VUM.replicate nGroups 0
    (p0 : rest) -> do
        let add !p = do
                let go !k
                        | k >= nGroups = pure ()
                        | otherwise = do
                            a <- VUM.unsafeRead p0 k
                            b <- VUM.unsafeRead p k
                            VUM.unsafeWrite p0 k (a + b)
                            go (k + 1)
                go 0
        mapM_ add rest
        VU.unsafeFreeze p0

-- | Merge per-worker (sum, count) partials by exact integer addition.
mergePair ::
    Int ->
    [(VUM.IOVector Int, VUM.IOVector Int)] ->
    IO (VUM.IOVector Int, VUM.IOVector Int)
mergePair nGroups parts = case parts of
    [] -> (,) <$> VUM.replicate nGroups 0 <*> VUM.replicate nGroups 0
    ((s0, c0) : rest) -> do
        let add (s, c) = do
                let go !k
                        | k >= nGroups = pure ()
                        | otherwise = do
                            sa <- VUM.unsafeRead s0 k
                            sb <- VUM.unsafeRead s k
                            VUM.unsafeWrite s0 k (sa + sb)
                            ca <- VUM.unsafeRead c0 k
                            cb <- VUM.unsafeRead c k
                            VUM.unsafeWrite c0 k (ca + cb)
                            go (k + 1)
                go 0
        mapM_ add rest
        pure (s0, c0)

-- | As 'mergePair' but for the Double (sum, count) partials of the mean.
mergeMeanDbl ::
    Int ->
    [(VUM.IOVector Double, VUM.IOVector Int)] ->
    IO (VUM.IOVector Double, VUM.IOVector Int)
mergeMeanDbl nGroups parts = case parts of
    [] -> (,) <$> VUM.replicate nGroups 0 <*> VUM.replicate nGroups 0
    ((s0, c0) : rest) -> do
        let add (s, c) = do
                let go !k
                        | k >= nGroups = pure ()
                        | otherwise = do
                            sa <- VUM.unsafeRead s0 k
                            sb <- VUM.unsafeRead s k
                            VUM.unsafeWrite s0 k (sa + sb)
                            ca <- VUM.unsafeRead c0 k
                            cb <- VUM.unsafeRead c k
                            VUM.unsafeWrite c0 k (ca + cb)
                            go (k + 1)
                go 0
        mapM_ add rest
        pure (s0, c0)

mergeExtremaInt :: Bool -> Int -> [VUM.IOVector Int] -> IO (VU.Vector Int)
mergeExtremaInt isMin nGroups parts = case parts of
    [] ->
        VU.unsafeFreeze =<< VUM.replicate nGroups (if isMin then maxBound else minBound)
    (p0 : rest) -> do
        let combine a b = if isMin then min a b else max a b
            add !p = do
                let go !k
                        | k >= nGroups = pure ()
                        | otherwise = do
                            a <- VUM.unsafeRead p0 k
                            b <- VUM.unsafeRead p k
                            VUM.unsafeWrite p0 k (combine a b)
                            go (k + 1)
                go 0
        mapM_ add rest
        VU.unsafeFreeze p0

mergeExtremaDbl :: Bool -> Int -> [VUM.IOVector Double] -> IO (VU.Vector Double)
mergeExtremaDbl isMin nGroups parts = case parts of
    [] ->
        VU.unsafeFreeze
            =<< VUM.replicate nGroups (if isMin then 1 / 0 else negate (1 / 0))
    (p0 : rest) -> do
        let combine a b = if isMin then min a b else max a b
            add !p = do
                let go !k
                        | k >= nGroups = pure ()
                        | otherwise = do
                            a <- VUM.unsafeRead p0 k
                            b <- VUM.unsafeRead p k
                            VUM.unsafeWrite p0 k (combine a b)
                            go (k + 1)
                go 0
        mapM_ add rest
        VU.unsafeFreeze p0
