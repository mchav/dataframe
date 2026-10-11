{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Parallel chunked-probe join kernels over a shared 'CompactIndex'.
module DataFrame.Operations.Join.Parallel (
    ProbeTable (..),
    parInnerProbe,
    parLeftProbe,
    shouldParallelizeJoin,
    shouldParallelizeSmallBuildProbe,
    parJoinThreshold,
    parBuildThreshold,
    parProbeThreshold,
) where

import Control.Concurrent (getNumCapabilities)
import Data.Bits (popCount, unsafeShiftR, (.&.))
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import Data.Word (Word64)
import DataFrame.Internal.Control.Concurrent (capabilities, forkJoin_)

-- | Below this many probe rows the fork overhead outweighs parallel probing.
parJoinThreshold :: Int
parJoinThreshold = 200000

-- | Below this many build rows the sequential hash probe beats forking.
parBuildThreshold :: Int
parBuildThreshold = 500000

-- | Whether a join should take the parallel probe path.
shouldParallelizeJoin :: Int -> Int -> Bool
shouldParallelizeJoin probeRows buildRows =
    probeRows >= parJoinThreshold
        && buildRows >= parBuildThreshold
        && capabilities > 1
{-# NOINLINE shouldParallelizeJoin #-}

-- | Probe-row count above which parallel probing wins even for small builds.
parProbeThreshold :: Int
parProbeThreshold = 1000000

-- | Whether a small-build join should take the parallel probe path.
shouldParallelizeSmallBuildProbe :: Int -> Bool
shouldParallelizeSmallBuildProbe probeRows =
    probeRows >= parProbeThreshold
        && capabilities > 1
{-# NOINLINE shouldParallelizeSmallBuildProbe #-}

-- | A read-only view of the build-side index fields needed by the probe.
data ProbeTable = ProbeTable
    { ptSorted :: !(VU.Vector Int)
    , ptKeys :: !(VU.Vector Int)
    , ptRuns :: !(VU.Vector Int)
    -- ^ @(start, len)@ packed 32\/32 per slot; @-1@ = empty (see @ciRuns@).
    , ptMask :: {-# UNPACK #-} !Int
    }

-- | Parallel inner-join probe returning (probeIxs, buildIxs).
parInnerProbe ::
    ProbeTable ->
    VU.Vector Int ->
    IO (VU.Vector Int, VU.Vector Int)
parInnerProbe = runProbe False

-- | Parallel left-join probe; unmatched rows carry a @-1@ build sentinel.
parLeftProbe ::
    ProbeTable ->
    VU.Vector Int ->
    IO (VU.Vector Int, VU.Vector Int)
parLeftProbe = runProbe True

-- | Shared two-pass parallel probe covering inner and left join semantics.
runProbe ::
    Bool ->
    ProbeTable ->
    VU.Vector Int ->
    IO (VU.Vector Int, VU.Vector Int)
runProbe keepUnmatched pt probeHashes = do
    caps <- getNumCapabilities
    let !probeN = VU.length probeHashes
        !nChunks = max 1 (min caps probeN)
        !sorted = ptSorted pt
        !keys = ptKeys pt
        !runs = ptRuns pt
        !mask = ptMask pt
        !shift = 64 - popCount mask
        -- Packed (start,len) run for hash @h@, or -1 on a miss.
        findRun !h =
            go
                ( fromIntegral
                    ((fromIntegral h * (0x9E3779B97F4A7C15 :: Word64)) `unsafeShiftR` shift)
                )
          where
            go !slot =
                let !w = runs `VU.unsafeIndex` slot
                 in if w < 0
                        then -1
                        else
                            if keys `VU.unsafeIndex` slot == h
                                then w
                                else go ((slot + 1) .&. mask)
        chunkBounds k = (lo, hi)
          where
            !lo = (probeN * k) `div` nChunks
            !hi = (probeN * (k + 1)) `div` nChunks
        -- Count pass: output rows produced by probe range [lo, hi).
        countRange !lo !hi =
            let go !i !acc
                    | i >= hi = acc
                    | otherwise =
                        let !w = findRun (VU.unsafeIndex probeHashes i)
                         in if w < 0
                                then go (i + 1) (if keepUnmatched then acc + 1 else acc)
                                else go (i + 1) (acc + (w .&. 0xFFFFFFFF))
             in go lo 0
    chunkCounts <- VUM.new (nChunks + 1)
    forkRanges nChunks $ \k ->
        let (lo, hi) = chunkBounds k
         in VUM.unsafeWrite chunkCounts k (countRange lo hi)
    -- Exclusive prefix sum gives per-chunk start offsets; total at [nChunks].
    let scan !k !acc
            | k > nChunks = pure acc
            | otherwise = do
                c <- if k < nChunks then VUM.unsafeRead chunkCounts k else pure 0
                VUM.unsafeWrite chunkCounts k acc
                scan (k + 1) (acc + c)
    !total <- scan 0 0
    pv <- VUM.unsafeNew (max 1 total)
    bv <- VUM.unsafeNew (max 1 total)
    -- Fill pass: each chunk writes from its prefix-sum offset.
    offs <- VU.unsafeFreeze chunkCounts
    forkRanges nChunks $ \k -> do
        let (lo, hi) = chunkBounds k
            !base = VU.unsafeIndex offs k
            fill !i !p
                | i >= hi = pure ()
                | otherwise = do
                    let !w = findRun (VU.unsafeIndex probeHashes i)
                    if w < 0
                        then
                            if keepUnmatched
                                then do
                                    VUM.unsafeWrite pv p i
                                    VUM.unsafeWrite bv p (-1)
                                    fill (i + 1) (p + 1)
                                else fill (i + 1) p
                        else do
                            let !start = w `unsafeShiftR` 32
                                !len = w .&. 0xFFFFFFFF
                                writeMatch !j !q
                                    | j >= len = pure ()
                                    | otherwise = do
                                        VUM.unsafeWrite pv q i
                                        VUM.unsafeWrite bv q (VU.unsafeIndex sorted (start + j))
                                        writeMatch (j + 1) (q + 1)
                            writeMatch 0 p
                            fill (i + 1) (p + len)
        fill lo base
    pf <- VU.unsafeFreeze (VUM.slice 0 total pv)
    bf <- VU.unsafeFreeze (VUM.slice 0 total bv)
    pure (pf, bf)

-- | Run @body k@ for each chunk on its own forked thread; rethrow failures.
forkRanges :: Int -> (Int -> IO ()) -> IO ()
forkRanges nChunks body = forkJoin_ (map body [0 .. nChunks - 1])
