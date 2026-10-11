{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE Strict #-}

-- | Partitioned group-by: counting-sort rows by hash bits, group per partition.
module DataFrame.Internal.Grouping.Partitioned (
    parallelAssignGroups,
    rtgFromVisOffs,
    numPartitionsFor,
) where

import Control.Concurrent (getNumCapabilities)
import Control.Monad (when)
import Data.Bits (countLeadingZeros, unsafeShiftL, unsafeShiftR)
import qualified Data.List as L
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as VM
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import Data.Word (Word64)
import DataFrame.Internal.Algorithms.Rank.Radix (rankByHash)
import DataFrame.Internal.Control.Concurrent (
    forkJoin,
    forkJoin_,
    parThreshold,
    parallelChunks_,
    pooledIndices,
 )
import DataFrame.Internal.Data.HashTable (
    htInsert,
    newHashTable,
 )
import System.IO.Unsafe (unsafePerformIO)

-- | Sign-preserving unsigned remap of a hash, ordering like signed 'Int'.
key :: Int -> Word64
key h = fromIntegral h + 0x8000000000000000
{-# INLINE key #-}

-- | Partition index of a hash: the top @log2 p@ bits of its unsigned key.
partIx :: Int -> Int -> Int
partIx shift h = fromIntegral (key h `unsafeShiftR` shift)
{-# INLINE partIx #-}

-- | Number of partitions: a power of two scaled by capabilities and rows.
numPartitionsFor :: Int -> Int -> Int
numPartitionsFor caps n = go 1
  where
    base = max 256 (4 * caps)
    go p
        | p < base = go (p * 2)
        | p < maxPartitions && n > p * partRowTarget = go (p * 2)
        | otherwise = p

-- | Cap on partition count (scatter-pass stream count stays manageable).
maxPartitions :: Int
maxPartitions = 4096

-- | Target rows per partition (~24k rows -> 64k-slot table, ~1.5MB).
partRowTarget :: Int
partRowTarget = 24576

-- | @floor (log2 x)@ for a power-of-two @x@.
intLog2 :: Int -> Int
intLog2 x = 63 - countLeadingZeros x
{-# INLINE intLog2 #-}

-- | Parallel group assignment, returning @(valueIndices, offsets)@ canonically.
parallelAssignGroups ::
    Int ->
    VU.Vector Int ->
    (Int -> Int -> Bool) ->
    IO (VU.Vector Int, VU.Vector Int)
parallelAssignGroups n hashes eqRow = do
    caps <- getNumCapabilities
    let !p = numPartitionsFor caps n
        !shift = 64 - intLog2 p
    link <- VUM.new (max 1 n)
    (partStart, sortedRows, sortedHash, sortedLink) <-
        partitionRows n hashes eqRow link p shift
    localGid <- VUM.new (max 1 n)
    gidByRow <- VUM.new (max 1 n)
    canonBoxes <- VM.replicate p (VU.empty :: VU.Vector Int)
    nLocalGroups <- VUM.replicate p (0 :: Int)
    runPartitions
        caps
        p
        partStart
        sortedRows
        sortedHash
        eqRow
        sortedLink
        gidByRow
        localGid
        canonBoxes
        nLocalGroups
    (globalBase, canonOf, nGroups) <- canonicalize p canonBoxes nLocalGroups
    assemble n p partStart sortedRows localGid globalBase canonOf nGroups

-- | Bucket every row index into its partition by a counting sort.
partitionRows ::
    Int ->
    VU.Vector Int ->
    (Int -> Int -> Bool) ->
    VUM.IOVector Int ->
    Int ->
    Int ->
    IO (VU.Vector Int, VU.Vector Int, VU.Vector Int, VU.Vector Int)
partitionRows n hashes eqRow link p shift = do
    caps <- getNumCapabilities
    let chunks = rowChunks caps n
    cursors <-
        forkJoin [histChunk hashes eqRow link p shift lo hi | (lo, hi) <- chunks]
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
    sortedM <- VUM.new (max 1 n)
    sortedHashM <- VUM.new (max 1 n)
    sortedLinkM <- VUM.new (max 1 n)
    forkJoin_
        [ scatterChunk hashes link shift cur sortedM sortedHashM sortedLinkM lo hi
        | ((lo, hi), cur) <- zip chunks cursors
        ]
    partStart <- VU.unsafeFreeze partStartM
    sortedRows <- VU.unsafeFreeze sortedM
    sortedHash <- VU.unsafeFreeze sortedHashM
    sortedLink <- VU.unsafeFreeze sortedLinkM
    pure (partStart, sortedRows, sortedHash, sortedLink)

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

-- | Per-partition counts of one row chunk, linking rows to recent equal keys.
histChunk ::
    VU.Vector Int ->
    (Int -> Int -> Bool) ->
    VUM.IOVector Int ->
    Int ->
    Int ->
    Int ->
    Int ->
    IO (VUM.IOVector Int)
histChunk hashes eqRow link p shift lo hi = do
    acc <- VUM.replicate p (0 :: Int)
    -- Two ways per set, so two hot keys sharing a set do not evict each other.
    cacheHash <- VUM.new (2 * linkCacheSets)
    cacheRow <- VUM.replicate (2 * linkCacheSets) (-1 :: Int)
    let hit !w !h !i = do
            r <- VUM.unsafeRead cacheRow w
            h' <- VUM.unsafeRead cacheHash w
            pure (if r >= 0 && h' == h && eqRow r i then r else -1)
        go !i
            | i >= hi = pure acc
            | otherwise = do
                let !h = VU.unsafeIndex hashes i
                    !pp = partIx shift h
                    !w0 = 2 * cacheSet h
                r0 <- hit w0 h i
                r <- if r0 >= 0 then pure r0 else hit (w0 + 1) h i
                VUM.unsafeWrite link i r
                when (r < 0) $ do
                    -- Way 0 holds the newest key; the older one moves to way 1.
                    VUM.unsafeRead cacheHash w0 >>= VUM.unsafeWrite cacheHash (w0 + 1)
                    VUM.unsafeRead cacheRow w0 >>= VUM.unsafeWrite cacheRow (w0 + 1)
                    VUM.unsafeWrite cacheHash w0 h
                    VUM.unsafeWrite cacheRow w0 i
                c <- VUM.unsafeRead acc pp
                VUM.unsafeWrite acc pp (c + 1)
                go (i + 1)
    go lo

-- | Cache set of a hash, mixing the weak low bits first.
cacheSet :: Int -> Int
cacheSet h =
    fromIntegral
        ( (fromIntegral h * 0x9E3779B97F4A7C15 :: Word64)
            `unsafeShiftR` (64 - linkCacheBits)
        )
{-# INLINE cacheSet #-}

linkCacheBits :: Int
linkCacheBits = 12

-- | Sets in each chunk's cache of recent keys (a power of two).
linkCacheSets :: Int
linkCacheSets = 1 `unsafeShiftL` linkCacheBits

-- | Scatter one row chunk through the chunk's private cursor.
scatterChunk ::
    VU.Vector Int ->
    VUM.IOVector Int ->
    Int ->
    VUM.IOVector Int ->
    VUM.IOVector Int ->
    VUM.IOVector Int ->
    VUM.IOVector Int ->
    Int ->
    Int ->
    IO ()
scatterChunk hashes link shift cursor sortedM sortedHashM sortedLinkM lo hi = go lo
  where
    go !i
        | i >= hi = pure ()
        | otherwise = do
            let !h = VU.unsafeIndex hashes i
                !pp = partIx shift h
            pos <- VUM.unsafeRead cursor pp
            VUM.unsafeWrite sortedM pos i
            VUM.unsafeWrite sortedHashM pos h
            VUM.unsafeRead link i >>= VUM.unsafeWrite sortedLinkM pos
            VUM.unsafeWrite cursor pp (pos + 1)
            go (i + 1)

-- | Partition indices, largest first, so skewed partitions start early.
largestFirst :: VU.Vector Int -> VU.Vector Int
largestFirst partStart =
    let !p = VU.length partStart - 1
        size i = VU.unsafeIndex partStart (i + 1) - VU.unsafeIndex partStart i
     in VU.fromList
            (map snd (L.sortBy (flip compare) [(size i, i) | i <- [0 .. p - 1]]))

-- | Group each partition with its own hash table and rank its local groups.
runPartitions ::
    Int ->
    Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    (Int -> Int -> Bool) ->
    VU.Vector Int ->
    VUM.IOVector Int ->
    VUM.IOVector Int ->
    VM.IOVector (VU.Vector Int) ->
    VUM.IOVector Int ->
    IO ()
runPartitions caps p partStart sortedRows sortedHash eqRow sortedLink gidByRow localGid canonBoxes nLocalGroups =
    pooledIndices caps p (groupPartition . VU.unsafeIndex order)
  where
    !order = largestFirst partStart
    groupPartition !pp = do
        let !s = VU.unsafeIndex partStart pp
            !e = VU.unsafeIndex partStart (pp + 1)
            !sz = e - s
        when (sz > 0) $ do
            -- Linked rows skip the table, so size it by the unlinked ones.
            let countUnlinked !pos !k
                    | pos >= e = pure k
                    | otherwise =
                        countUnlinked (pos + 1) (if VU.unsafeIndex sortedLink pos < 0 then k + 1 else k)
            nUnlinked <- countUnlinked s 0
            ht <- newHashTable nUnlinked
            repHashM <- VUM.new nUnlinked
            let loop !pos !nextGid
                    | pos >= e = pure nextGid
                    | otherwise = do
                        let !row = VU.unsafeIndex sortedRows pos
                            !h = VU.unsafeIndex sortedHash pos
                            !r = VU.unsafeIndex sortedLink pos
                        if r >= 0
                            then do
                                -- r: an earlier unlinked row of this partition.
                                gid <- VUM.unsafeRead gidByRow r
                                VUM.unsafeWrite localGid pos gid
                                loop (pos + 1) nextGid
                            else do
                                (gid, isNew) <- htInsert ht eqRow nextGid row h
                                VUM.unsafeWrite localGid pos gid
                                VUM.unsafeWrite gidByRow row gid
                                if isNew
                                    then do
                                        VUM.unsafeWrite repHashM nextGid h
                                        loop (pos + 1) (nextGid + 1)
                                    else loop (pos + 1) nextGid
            ng <- loop s 0
            VUM.unsafeWrite nLocalGroups pp ng
            canon <- rankByHash (VUM.unsafeRead repHashM) ng
            VM.unsafeWrite canonBoxes pp canon

-- | Exclusive prefix sum of per-partition group counts into @globalBase@.
canonicalize ::
    Int ->
    VM.IOVector (VU.Vector Int) ->
    VUM.IOVector Int ->
    IO (VU.Vector Int, V.Vector (VU.Vector Int), Int)
canonicalize p canonBoxes nLocalGroups = do
    globalBaseM <- VUM.new (p + 1)
    let go !pp !base
            | pp >= p = VUM.unsafeWrite globalBaseM p base >> pure base
            | otherwise = do
                VUM.unsafeWrite globalBaseM pp base
                ng <- VUM.unsafeRead nLocalGroups pp
                go (pp + 1) (base + ng)
    total <- go 0 0
    globalBase <- VU.unsafeFreeze globalBaseM
    canonOf <- V.unsafeFreeze canonBoxes
    pure (globalBase, canonOf, total)

-- | Build the final @(valueIndices, offsets)@ from global group ids.
assemble ::
    Int ->
    Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    VUM.IOVector Int ->
    VU.Vector Int ->
    V.Vector (VU.Vector Int) ->
    Int ->
    IO (VU.Vector Int, VU.Vector Int)
assemble n p partStart sortedRows localGid globalBase canonOf nGroups = do
    caps <- getNumCapabilities
    gidAt <- VUM.new (max 1 n)
    counts <- VUM.new (max 1 nGroups)
    offsM <- VUM.new (nGroups + 1)
    visM <- VUM.new (max 1 n)
    let doPartition !pp = do
            let !s = VU.unsafeIndex partStart pp
                !e = VU.unsafeIndex partStart (pp + 1)
                !base = VU.unsafeIndex globalBase pp
                !gEnd = VU.unsafeIndex globalBase (pp + 1)
                !canon = V.unsafeIndex canonOf pp
            let zero !g
                    | g >= gEnd = pure ()
                    | otherwise = VUM.unsafeWrite counts g 0 >> zero (g + 1)
            zero base
            -- Pass 1: global group ids and per-group counts.
            let pass1 !pos
                    | pos >= e = pure ()
                    | otherwise = do
                        lg <- VUM.unsafeRead localGid pos
                        let !g = base + VU.unsafeIndex canon lg
                        VUM.unsafeWrite gidAt pos g
                        c <- VUM.unsafeRead counts g
                        VUM.unsafeWrite counts g (c + 1)
                        pass1 (pos + 1)
            pass1 s
            -- Offsets for our group range; counts becomes the write cursor.
            let offsLoop !g !acc
                    | g >= gEnd = pure ()
                    | otherwise = do
                        VUM.unsafeWrite offsM g acc
                        c <- VUM.unsafeRead counts g
                        VUM.unsafeWrite counts g acc
                        offsLoop (g + 1) (acc + c)
            offsLoop base s
            -- Pass 2: stable placement into valueIndices.
            let pass2 !pos
                    | pos >= e = pure ()
                    | otherwise = do
                        g <- VUM.unsafeRead gidAt pos
                        let !row = VU.unsafeIndex sortedRows pos
                        c <- VUM.unsafeRead counts g
                        VUM.unsafeWrite visM c row
                        VUM.unsafeWrite counts g (c + 1)
                        pass2 (pos + 1)
            pass2 s
    let !order = largestFirst partStart
    pooledIndices caps p (doPartition . VU.unsafeIndex order)
    VUM.unsafeWrite offsM nGroups n
    offs <- VU.unsafeFreeze offsM
    vis <- VU.unsafeFreeze visM
    pure (vis, offs)

-- | Deferred @rowToGroup@ from @(valueIndices, offsets)@.
rtgFromVisOffs :: Int -> VU.Vector Int -> VU.Vector Int -> VU.Vector Int
rtgFromVisOffs n vis offs = unsafePerformIO $ do
    let !nGroups = VU.length offs - 1
    rtgM <- VUM.new (max 1 n)
    let
        -- Largest g with offs[g] <= i (offsets are non-decreasing).
        findGroup !i = go2 0 nGroups
          where
            go2 !lo !hi
                | lo >= hi = lo - 1
                | otherwise =
                    let !mid = (lo + hi) `div` 2
                     in if VU.unsafeIndex offs mid <= i
                            then go2 (mid + 1) hi
                            else go2 lo mid
        fill !i !hi !g
            | i >= hi = pure ()
            | otherwise = do
                let !g' = advance g
                    advance !gg =
                        if VU.unsafeIndex offs (gg + 1) <= i
                            then advance (gg + 1)
                            else gg
                VUM.unsafeWrite rtgM (VU.unsafeIndex vis i) g'
                fill (i + 1) hi g'
    parallelChunks_ parThreshold n (\lo hi -> fill lo hi (findGroup lo))
    VU.unsafeFreeze rtgM
{-# NOINLINE rtgFromVisOffs #-}
