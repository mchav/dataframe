{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Parallel stable sort of row indices by unsigned per-row hash.
module DataFrame.Internal.Algorithms.Sort.Radix.Parallel (
    parSortByHash,
    parSortThreshold,
) where

import Control.Concurrent (getNumCapabilities)
import Control.Monad (when)
import Data.Bits (countLeadingZeros, unsafeShiftR, (.&.))
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import Data.Word (Word64)
import DataFrame.Internal.Algorithms.Rank.Radix (sortKey)
import DataFrame.Internal.Control.Concurrent (
    capabilities,
    forkJoin,
    forkJoin_,
    pooledIndices,
 )
import System.IO.Unsafe (unsafePerformIO)

-- | Below this many rows the sequential radix path is used instead.
parSortThreshold :: Int
parSortThreshold = 500000

-- | Top-bits partition index of a hash, in ascending key order.
partIx :: Int -> Int -> Int
partIx shift h = fromIntegral ((fromIntegral (sortKey h) :: Word64) `unsafeShiftR` shift)
{-# INLINE partIx #-}

-- | Number of partitions: a power of two, at least @4 * caps@, floored at 256.
numPartitionsFor :: Int -> Int
numPartitionsFor caps = go 1
  where
    target = max 256 (4 * caps)
    go p
        | p >= target = p
        | otherwise = go (p * 2)

-- | @floor (log2 x)@ for a power-of-two @x@.
intLog2 :: Int -> Int
intLog2 x = 63 - countLeadingZeros x
{-# INLINE intLog2 #-}

-- | Parallel stable sort of @[0, n)@ by ascending unsigned hash order.
parSortByHash :: Int -> VU.Vector Int -> (VU.Vector Int, VU.Vector Int)
parSortByHash n hashes
    | n <= 1 =
        (hashes, VU.enumFromN 0 n)
    | n < parSortThreshold || capabilities <= 1 =
        seqSortByHash n hashes
    | otherwise = unsafePerformIO (parSortByHashIO n hashes)
{-# NOINLINE parSortByHash #-}

-- | Stable LSD radix sort of @[0, n)@ by ascending 'sortKey' of their hash.
seqSortByHash :: Int -> VU.Vector Int -> (VU.Vector Int, VU.Vector Int)
seqSortByHash n hashes = unsafePerformIO $ do
    keysA <- VUM.new n
    orderA <- VUM.new n
    let seed !i
            | i >= n = pure ()
            | otherwise = do
                VUM.unsafeWrite keysA i (sortKey (VU.unsafeIndex hashes i))
                VUM.unsafeWrite orderA i i
                seed (i + 1)
    seed 0
    keysB <- VUM.new n
    orderB <- VUM.new n
    radixPasses n keysA orderA keysB orderB
    order <- VU.unsafeFreeze orderA
    pure (VU.unsafeBackpermute hashes order, order)

-- | Run all eight stable 8-bit LSD passes, ping-ponging between buffers.
radixPasses ::
    Int ->
    VUM.IOVector Int ->
    VUM.IOVector Int ->
    VUM.IOVector Int ->
    VUM.IOVector Int ->
    IO ()
radixPasses = radixPassesN 8

-- | Run the first @np@ stable 8-bit LSD passes, ping-ponging between buffers.
radixPassesN ::
    Int ->
    Int ->
    VUM.IOVector Int ->
    VUM.IOVector Int ->
    VUM.IOVector Int ->
    VUM.IOVector Int ->
    IO ()
radixPassesN np n keysA orderA keysB orderB = do
    counts <- VUM.new 256
    let pass ::
            Int ->
            VUM.IOVector Int ->
            VUM.IOVector Int ->
            VUM.IOVector Int ->
            VUM.IOVector Int ->
            IO ()
        pass !shiftBits !srcK !srcO !dstK !dstO = do
            VUM.set counts 0
            let count !i
                    | i >= n = pure ()
                    | otherwise = do
                        k <- VUM.unsafeRead srcK i
                        let !b = (k `unsafeShiftR` shiftBits) .&. 0xff
                        VUM.unsafeRead counts b >>= VUM.unsafeWrite counts b . (+ 1)
                        count (i + 1)
            count 0
            let scan !b !acc
                    | b >= 256 = pure ()
                    | otherwise = do
                        c <- VUM.unsafeRead counts b
                        VUM.unsafeWrite counts b acc
                        scan (b + 1) (acc + c)
            scan 0 0
            let place !i
                    | i >= n = pure ()
                    | otherwise = do
                        k <- VUM.unsafeRead srcK i
                        o <- VUM.unsafeRead srcO i
                        let !b = (k `unsafeShiftR` shiftBits) .&. 0xff
                        pos <- VUM.unsafeRead counts b
                        VUM.unsafeWrite counts b (pos + 1)
                        VUM.unsafeWrite dstK pos k
                        VUM.unsafeWrite dstO pos o
                        place (i + 1)
            place 0
        run !k
            | k >= np = pure ()
            | even k = pass (8 * k) keysA orderA keysB orderB >> run (k + 1)
            | otherwise = pass (8 * k) keysB orderB keysA orderA >> run (k + 1)
    run 0

parSortByHashIO :: Int -> VU.Vector Int -> IO (VU.Vector Int, VU.Vector Int)
parSortByHashIO n hashes = do
    caps <- getNumCapabilities
    let !p = numPartitionsFor caps
        !shift = 64 - intLog2 p
    (partStart, partRows, partHashes) <- partitionRows n hashes p shift
    outOrder <- VUM.new n
    outKeys <- VUM.new n
    sortPartitions caps p partStart partRows partHashes outOrder outKeys
    order <- VU.unsafeFreeze outOrder
    sortedHashes <- VU.unsafeFreeze outKeys
    pure (sortedHashes, order)

-- | Bucket every row index into its top-bits partition by counting sort.
partitionRows ::
    Int ->
    VU.Vector Int ->
    Int ->
    Int ->
    IO (VU.Vector Int, VU.Vector Int, VU.Vector Int)
partitionRows n hashes p shift = do
    caps <- getNumCapabilities
    let chunks = rowChunks caps n
    cursors <- forkJoin [histChunk hashes p shift lo hi | (lo, hi) <- chunks]
    -- Exclusive prefix over partitions and chunks; histograms become cursors.
    partStartM <- VUM.new (p + 1)
    let seed !pp !acc
            | pp >= p = VUM.unsafeWrite partStartM p acc
            | otherwise = do
                VUM.unsafeWrite partStartM pp acc
                let inner [] !a = pure a
                    inner (cur : rest) !a = do
                        t <- VUM.unsafeRead cur pp
                        VUM.unsafeWrite cur pp a
                        inner rest (a + t)
                acc' <- inner cursors acc
                seed (pp + 1) acc'
    seed 0 0
    rowsM <- VUM.new (max 1 n)
    rowHashM <- VUM.new (max 1 n)
    forkJoin_
        [ scatterChunk hashes shift cur rowsM rowHashM lo hi
        | ((lo, hi), cur) <- zip chunks cursors
        ]
    partStart <- VU.unsafeFreeze partStartM
    partRows <- VU.unsafeFreeze rowsM
    partHashes <- VU.unsafeFreeze rowHashM
    pure (partStart, partRows, partHashes)

-- | Contiguous near-equal row chunks, one per capability; empties dropped.
rowChunks :: Int -> Int -> [(Int, Int)]
rowChunks caps n =
    [ (lo, hi)
    | w <- [0 .. caps - 1]
    , let lo = min n (w * per)
    , let hi = min n (lo + per)
    , lo < hi
    ]
  where
    !per = (n + max 1 caps - 1) `div` max 1 caps

-- | Per-partition counts of one row chunk.
histChunk :: VU.Vector Int -> Int -> Int -> Int -> Int -> IO (VUM.IOVector Int)
histChunk hashes p shift lo hi = do
    acc <- VUM.replicate p (0 :: Int)
    let go !i
            | i >= hi = pure acc
            | otherwise = do
                let !pp = partIx shift (VU.unsafeIndex hashes i)
                c <- VUM.unsafeRead acc pp
                VUM.unsafeWrite acc pp (c + 1)
                go (i + 1)
    go lo

-- | Scatter one row chunk into the partitioned layout using its cursor.
scatterChunk ::
    VU.Vector Int ->
    Int ->
    VUM.IOVector Int ->
    VUM.IOVector Int ->
    VUM.IOVector Int ->
    Int ->
    Int ->
    IO ()
scatterChunk hashes shift cursor rowsM rowHashM lo hi = go lo
  where
    go !i
        | i >= hi = pure ()
        | otherwise = do
            let !h = VU.unsafeIndex hashes i
                !pp = partIx shift h
            pos <- VUM.unsafeRead cursor pp
            VUM.unsafeWrite rowsM pos i
            VUM.unsafeWrite rowHashM pos h
            VUM.unsafeWrite cursor pp (pos + 1)
            go (i + 1)

-- | Stable-sort each partition by full key into the output slots.
sortPartitions ::
    Int ->
    Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    VUM.IOVector Int ->
    VUM.IOVector Int ->
    IO ()
sortPartitions caps p partStart partRows partHashes outOrder outKeys =
    pooledIndices caps p sortOne
  where
    sortOne !pp = do
        let !s = VU.unsafeIndex partStart pp
            !e = VU.unsafeIndex partStart (pp + 1)
            !sz = e - s
        when (sz > 0) $
            if sz == 1
                then do
                    VUM.unsafeWrite outOrder s (VU.unsafeIndex partRows s)
                    VUM.unsafeWrite outKeys s (VU.unsafeIndex partHashes s)
                else do
                    keysA <- VUM.new sz
                    orderA <- VUM.new sz
                    let seed !i
                            | i >= sz = pure ()
                            | otherwise = do
                                VUM.unsafeWrite keysA i (sortKey (VU.unsafeIndex partHashes (s + i)))
                                VUM.unsafeWrite orderA i (VU.unsafeIndex partRows (s + i))
                                seed (i + 1)
                    seed 0
                    keysB <- VUM.new sz
                    orderB <- VUM.new sz
                    radixPassesN 7 sz keysA orderA keysB orderB
                    let emit !i
                            | i >= sz = pure ()
                            | otherwise = do
                                o <- VUM.unsafeRead orderB i
                                k <- VUM.unsafeRead keysB i
                                VUM.unsafeWrite outOrder (s + i) o
                                -- sortKey is an involution: recover the hash.
                                VUM.unsafeWrite outKeys (s + i) (sortKey k)
                                emit (i + 1)
                    emit 0
