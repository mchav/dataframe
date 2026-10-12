{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE UnboxedTuples #-}

module DataFrame.Internal.Aggregation.Kernel.Distinct (
    distinctSupported,
    countDistinctColumn,
    countDistinctGroups,
) where

import Control.Monad (forM_, replicateM, void, when)
import Data.Bits (
    countLeadingZeros,
    finiteBitSize,
    unsafeShiftL,
    unsafeShiftR,
    xor,
    (.&.),
 )
import Data.Char (ord)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef, writeIORef)
import Data.Int (Int16, Int32, Int64, Int8)
import Data.Primitive.ByteArray (MutableByteArray (..))
import qualified Data.Text.Array as A
import Data.Text.Internal (Text (Text))
import Data.Type.Equality (TestEquality (..), type (:~:) (Refl))
import qualified Data.Vector as V
import qualified Data.Vector.Primitive as VP
import qualified Data.Vector.Primitive.Mutable as VPM
import qualified Data.Vector.Unboxed as VU
import Data.Vector.Unboxed.Base (
    MVector (MV_Int),
    Vector (V_Int, V_Int64, V_Word64),
 )
import qualified Data.Vector.Unboxed.Mutable as VUM
import Data.Word (Word16, Word32, Word64, Word8)
import GHC.Exts (Int (I#), fetchAddIntArray#, (+#))
import GHC.Float (castDoubleToWord64)
import GHC.IO (IO (IO))
import System.IO.Unsafe (unsafePerformIO)
import Type.Reflection (typeRep)

import DataFrame.Internal.Algorithms.Hash (fnvOffset, mixBytes)
import DataFrame.Internal.Column (Column (..))
import DataFrame.Internal.Control.Concurrent (
    capabilities,
    forkJoin_,
    parThreshold,
    shouldParallelize,
    splitChunkRange,
 )
import DataFrame.Internal.Data.PackedText (
    PackedOffsets (..),
    PackedTextData,
    packedLength,
    packedRowOffsets,
    packedSlice,
 )

-------------------------------------------------------------------------------
-- Admission
-------------------------------------------------------------------------------

-- | How a column's rows become set keys.
data Keys
    = -- | One 64-bit word per row; equal words iff equal values.
      WordKeys !(VU.Vector Int)
    | -- | A UTF-8 byte slice per row.
      ByteKeys !Int !TextRows

{- | Where a text row's bytes live. The contiguous layouts get their own kernel
instances so the row accessor inlines; the rest go through a closure.
-}
data TextRows
    = Contig32 !A.Array !(VU.Vector Int32)
    | Contig64 !A.Array !(VU.Vector Int)
    | AnyRows (Int -> (A.Array, Int, Int))

-- | Whether 'countDistinctColumn' / 'countDistinctGroups' accept this column.
distinctSupported :: Column -> Bool
distinctSupported c = case columnKeys c of
    Just _ -> True
    Nothing -> False

columnKeys :: Column -> Maybe Keys
columnKeys col = case col of
    UnboxedColumn Nothing (v :: VU.Vector a)
        | Just Refl <- testEquality (typeRep @a) (typeRep @Int) -> Just (WordKeys v)
        | Just Refl <- testEquality (typeRep @a) (typeRep @Int64) ->
            Just (WordKeys (int64Words v))
        | Just Refl <- testEquality (typeRep @a) (typeRep @Word64) ->
            Just (WordKeys (word64Words v))
        | Just Refl <- testEquality (typeRep @a) (typeRep @Int32) -> widen v
        | Just Refl <- testEquality (typeRep @a) (typeRep @Int16) -> widen v
        | Just Refl <- testEquality (typeRep @a) (typeRep @Int8) -> widen v
        | Just Refl <- testEquality (typeRep @a) (typeRep @Word) -> widen v
        | Just Refl <- testEquality (typeRep @a) (typeRep @Word32) -> widen v
        | Just Refl <- testEquality (typeRep @a) (typeRep @Word16) -> widen v
        | Just Refl <- testEquality (typeRep @a) (typeRep @Word8) -> widen v
        | Just Refl <- testEquality (typeRep @a) (typeRep @Double) ->
            Just (WordKeys (VU.map doubleWord v))
        | Just Refl <- testEquality (typeRep @a) (typeRep @Char) ->
            Just (WordKeys (VU.map ord v))
        | Just Refl <- testEquality (typeRep @a) (typeRep @Bool) ->
            Just (WordKeys (VU.map fromEnum v))
    BoxedColumn Nothing (v :: V.Vector a)
        | Just Refl <- testEquality (typeRep @a) (typeRep @Text) ->
            Just
                ( ByteKeys
                    (V.length v)
                    (AnyRows (\i -> case V.unsafeIndex v i of Text arr o l -> (arr, o, l)))
                )
    PackedText Nothing p -> Just (ByteKeys (packedLength p) (packedRows p))
    _ -> Nothing
  where
    widen :: (Integral b, VU.Unbox b) => VU.Vector b -> Maybe Keys
    widen = Just . WordKeys . VU.map fromIntegral

packedRows :: PackedTextData -> TextRows
packedRows p = case packedRowOffsets p of
    Just (arr, Offs32 offs) -> Contig32 arr offs
    Just (arr, Offs64 offs) -> Contig64 arr offs
    Nothing -> AnyRows (packedSlice p)

-- | Reinterpret 64-bit words as 'Int' without copying (same width and layout).
int64Words :: VU.Vector Int64 -> VU.Vector Int
int64Words v@(V_Int64 (VP.Vector o l ba))
    | finiteBitSize (0 :: Int) == 64 = V_Int (VP.Vector o l ba)
    | otherwise = VU.map fromIntegral v

word64Words :: VU.Vector Word64 -> VU.Vector Int
word64Words v@(V_Word64 (VP.Vector o l ba))
    | finiteBitSize (0 :: Int) == 64 = V_Int (VP.Vector o l ba)
    | otherwise = VU.map fromIntegral v

-- | Equal doubles share a word: @-0.0@ joins @0.0@ and every NaN is one value.
doubleWord :: Double -> Int
doubleWord d
    | isNaN d = 0x7ff8000000000000
    | d == 0 = 0
    | otherwise = fromIntegral (castDoubleToWord64 d)

-------------------------------------------------------------------------------
-- Entry points
-------------------------------------------------------------------------------

-- | Number of distinct values in a column, or 'Nothing' if not admitted.
countDistinctColumn :: Column -> Maybe Int
countDistinctColumn col = case columnKeys col of
    Nothing -> Nothing
    Just (WordKeys v) -> Just $! unsafePerformIO (wordGlobal v)
    Just (ByteKeys n rows) -> Just $! unsafePerformIO $ case rows of
        Contig32 arr offs -> bytesGlobal n (contigSlice arr offs)
        Contig64 arr offs -> bytesGlobal n (contigSlice arr offs)
        AnyRows slice -> bytesGlobal n slice
{-# NOINLINE countDistinctColumn #-}

{- | Distinct values per group, given each row's group id (@rowToGroup@) and the
group count. 'Nothing' if the column is not admitted.
-}
countDistinctGroups :: VU.Vector Int -> Int -> Column -> Maybe (VU.Vector Int)
countDistinctGroups rtg nGroups col = case columnKeys col of
    Nothing -> Nothing
    Just (WordKeys v) -> Just $! unsafePerformIO (wordGrouped rtg nGroups v)
    Just (ByteKeys _ rows) -> Just $! unsafePerformIO $ case rows of
        Contig32 arr offs -> bytesGrouped rtg nGroups (contigSlice arr offs)
        Contig64 arr offs -> bytesGrouped rtg nGroups (contigSlice arr offs)
        AnyRows slice -> bytesGrouped rtg nGroups slice
{-# NOINLINE countDistinctGroups #-}

contigSlice ::
    (Integral o, VU.Unbox o) => A.Array -> VU.Vector o -> Int -> (A.Array, Int, Int)
contigSlice arr offs i =
    let !o = fromIntegral (VU.unsafeIndex offs i)
     in (arr, o, fromIntegral (VU.unsafeIndex offs (i + 1)) - o)
{-# INLINE contigSlice #-}

-------------------------------------------------------------------------------
-- The four variants
-------------------------------------------------------------------------------

wordGlobal :: VU.Vector Int -> IO Int
wordGlobal v = do
    let !n = VU.length v
        !pb = partitionBits n
        key = VU.unsafeIndex v
    Scattered w0 _ starts <-
        scatterRows
            n
            pb
            False
            False
            (fmix . key)
            (\r r' -> key r == key r')
            (\i _ -> key i)
            (const 0)
    counts <- forPartitions (VU.length starts - 1) (newIORef (0 :: Int)) $ \acc p -> do
        c <- dedupWords pb w0 (VU.unsafeIndex starts p) (VU.unsafeIndex starts (p + 1))
        modifyStrict acc (+ c)
    sum <$> mapM readIORef counts

wordGrouped :: VU.Vector Int -> Int -> VU.Vector Int -> IO (VU.Vector Int)
wordGrouped rtg nGroups v = do
    let !n = VU.length v
        !pb = partitionBits n
        key = VU.unsafeIndex v
        gid = VU.unsafeIndex rtg
    Scattered w0 w1 starts <-
        scatterRows
            n
            pb
            False
            True
            (\i -> pairHash (gid i) (key i))
            (\r r' -> key r == key r' && gid r == gid r')
            (\i _ -> key i)
            gid
    groupCounts nGroups (VU.length starts - 1) $ \counts p ->
        void $
            dedupPairs
                pb
                w0
                w1
                (VU.unsafeIndex starts p)
                (VU.unsafeIndex starts (p + 1))
                pairHash'
                (\a0 a1 b0 b1 -> a0 == b0 && a1 == b1)
                (\_ g -> bumpGroup counts g)
  where
    pairHash' k g = pairHash g k

bytesGlobal :: Int -> (Int -> (A.Array, Int, Int)) -> IO Int
bytesGlobal n slice = do
    let !pb = partitionBits n
        hashRow i = case slice i of (arr, o, l) -> fmix (mixBytes fnvOffset arr o l)
    Scattered w0 w1 starts <-
        scatterRows n pb True True hashRow (rowsEq slice) (\_ h -> h) id
    counts <- forPartitions (VU.length starts - 1) (newIORef (0 :: Int)) $ \acc p -> do
        c <-
            dedupPairs
                pb
                w0
                w1
                (VU.unsafeIndex starts p)
                (VU.unsafeIndex starts (p + 1))
                const
                (\h r h' r' -> h == h' && rowsEq slice r r')
                (\_ _ -> pure ())
        modifyStrict acc (+ c)
    sum <$> mapM readIORef counts

bytesGrouped ::
    VU.Vector Int -> Int -> (Int -> (A.Array, Int, Int)) -> IO (VU.Vector Int)
bytesGrouped rtg nGroups slice = do
    let !n = VU.length rtg
        !pb = partitionBits n
        gid = VU.unsafeIndex rtg
        hashRow i = case slice i of
            (arr, o, l) -> pairHash (gid i) (mixBytes fnvOffset arr o l)
    Scattered w0 w1 starts <-
        scatterRows
            n
            pb
            True
            True
            hashRow
            (\r r' -> gid r == gid r' && rowsEq slice r r')
            (\_ h -> h)
            id
    groupCounts nGroups (VU.length starts - 1) $ \counts p ->
        void $
            dedupPairs
                pb
                w0
                w1
                (VU.unsafeIndex starts p)
                (VU.unsafeIndex starts (p + 1))
                const
                (\h r h' r' -> h == h' && gid r == gid r' && rowsEq slice r r')
                (\_ r -> bumpGroup counts (gid r))

rowsEq :: (Int -> (A.Array, Int, Int)) -> Int -> Int -> Bool
rowsEq slice r r' = case slice r of
    (a, o, l) -> case slice r' of
        (a', o', l') -> l == l' && A.compare a o a' o' l == EQ
{-# INLINE rowsEq #-}

{-# INLINE bytesGlobal #-}
{-# INLINE bytesGrouped #-}

-------------------------------------------------------------------------------
-- Hashing
-------------------------------------------------------------------------------

-- | Murmur3's 64-bit finaliser: every input bit reaches every output bit.
fmix :: Int -> Int
fmix k =
    let !h0 = fromIntegral k :: Word64
        !h1 = (h0 `xor` (h0 `unsafeShiftR` 33)) * 0xff51afd7ed558ccd
        !h2 = (h1 `xor` (h1 `unsafeShiftR` 33)) * 0xc4ceb9fe1a85ec53
     in fromIntegral (h2 `xor` (h2 `unsafeShiftR` 33))
{-# INLINE fmix #-}

pairHash :: Int -> Int -> Int
pairHash g k = fmix (k `xor` (g * 0x9E3779B97F4A7C1))
{-# INLINE pairHash #-}

-- | Partition of a hash: its top @pb@ bits (@pb >= 1@).
partOf :: Int -> Int -> Int
partOf pb h = fromIntegral ((fromIntegral h :: Word64) `unsafeShiftR` (64 - pb))
{-# INLINE partOf #-}

-- | Slot of a hash in a @2^bits@ table: the bits just below the partition bits.
slotOf :: Int -> Int -> Int -> Int
slotOf pb bits h =
    fromIntegral
        (((fromIntegral h :: Word64) `unsafeShiftL` pb) `unsafeShiftR` (64 - bits))
{-# INLINE slotOf #-}

-------------------------------------------------------------------------------
-- Passes 1 and 2: histogram and scatter
-------------------------------------------------------------------------------

{- | Partition count as a bit width: about 64K rows per partition so the sets
stay cache-sized, at least four partitions per worker for balance, capped so
the scatter's write heads stay within cache and TLB reach.
-}
partitionBits :: Int -> Int
partitionBits n
    | not (shouldParallelize parThreshold n) = 1
    | otherwise =
        max
            1
            ( min
                maxPartitionBits
                (max (ceilLog2 (n `div` rowsPerPartition)) (ceilLog2 (4 * capabilities)))
            )

maxPartitionBits :: Int
maxPartitionBits = 8

rowsPerPartition :: Int
rowsPerPartition = 65536

ceilLog2 :: Int -> Int
ceilLog2 x
    | x <= 1 = 0
    | otherwise = finiteBitSize x - countLeadingZeros (x - 1)

-- | Entries in each chunk's cache of recently emitted keys.
cacheSize :: Int
cacheSize = 4096

-- | Partition mark of a row dropped as a cached duplicate.
droppedRow :: Word16
droppedRow = maxBound

-- | Rows scattered by partition: key words, optional second words, @np+1@ starts.
data Scattered = Scattered !(VU.Vector Int) !(VU.Vector Int) !(VU.Vector Int)

{- | Hash every row to histogram partitions, then write each row's words into
its partition. With @keepHash@ the hash is stored between the passes (text
hashing is too costly to repeat).

Each chunk keeps a small direct-mapped cache of the keys it has emitted; a row
equal to a cached key is a duplicate of a row already headed for the same
partition, so it is dropped. Skewed keys (an empty string on most rows, a few
very active users) then cost one cache probe per row instead of piling into
one partition and one worker.
-}
scatterRows ::
    Int ->
    Int ->
    -- | keep the hash between passes
    Bool ->
    -- | write a second word per row
    Bool ->
    -- | row hash
    (Int -> Int) ->
    -- | do two rows hold the same key?
    (Int -> Int -> Bool) ->
    -- | first word, from row and hash
    (Int -> Int -> Int) ->
    -- | second word, from row
    (Int -> Int) ->
    IO Scattered
scatterRows n pb keepHash twoWords rowHash rowEq rowW0 rowW1 = do
    let !np = 1 `unsafeShiftL` pb
        chunks
            | shouldParallelize parThreshold n = splitChunkRange capabilities n
            | otherwise = [(0, n)]
        !nc = length chunks
    hashes <- if keepHash then VUM.unsafeNew n else VUM.unsafeNew 0
    parts <- VUM.unsafeNew n :: IO (VUM.IOVector Word16)
    hist <- VUM.replicate (nc * np) (0 :: Int)
    forkJoin_
        [ do
            cacheHash <- VUM.unsafeNew cacheSize
            cacheRow <- VUM.replicate cacheSize (-1 :: Int)
            let !base = ci * np
                go !i
                    | i >= hi = pure ()
                    | otherwise = do
                        let !h = rowHash i
                            !c = h .&. (cacheSize - 1)
                        when keepHash (VUM.unsafeWrite hashes i h)
                        r <- VUM.unsafeRead cacheRow c
                        dup <-
                            if r < 0
                                then pure False
                                else do
                                    h' <- VUM.unsafeRead cacheHash c
                                    pure (h' == h && rowEq r i)
                        if dup
                            then VUM.unsafeWrite parts i droppedRow
                            else do
                                VUM.unsafeWrite cacheHash c h
                                VUM.unsafeWrite cacheRow c i
                                let !p = partOf pb h
                                VUM.unsafeWrite parts i (fromIntegral p)
                                VUM.unsafeModify hist (+ 1) (base + p)
                        go (i + 1)
            go lo
        | (ci, (lo, hi)) <- zip [0 ..] chunks
        ]
    -- Exclusive prefix sum, partition-major, turns counts into write cursors.
    starts <- VUM.unsafeNew (np + 1)
    let prefix !p !run
            | p >= np = VUM.unsafeWrite starts np run
            | otherwise = do
                VUM.unsafeWrite starts p run
                let walk !ci !r
                        | ci >= nc = pure r
                        | otherwise = do
                            c <- VUM.unsafeRead hist (ci * np + p)
                            VUM.unsafeWrite hist (ci * np + p) r
                            walk (ci + 1) (r + c)
                run' <- walk 0 run
                prefix (p + 1) run'
    prefix 0 0
    kept <- VUM.unsafeRead starts np
    w0 <- VUM.unsafeNew kept
    w1 <- if twoWords then VUM.unsafeNew kept else VUM.unsafeNew 0
    forkJoin_
        [ let !base = ci * np
              go !i
                | i >= hi = pure ()
                | otherwise = do
                    p <- VUM.unsafeRead parts i
                    when (p /= droppedRow) $ do
                        h <- if keepHash then VUM.unsafeRead hashes i else pure 0
                        let !slot = base + fromIntegral p
                        pos <- VUM.unsafeRead hist slot
                        VUM.unsafeWrite hist slot (pos + 1)
                        VUM.unsafeWrite w0 pos (rowW0 i h)
                        when twoWords (VUM.unsafeWrite w1 pos (rowW1 i))
                    go (i + 1)
           in go lo
        | (ci, (lo, hi)) <- zip [0 ..] chunks
        ]
    Scattered
        <$> VU.unsafeFreeze w0
        <*> VU.unsafeFreeze w1
        <*> VU.unsafeFreeze starts
{-# INLINE scatterRows #-}

-------------------------------------------------------------------------------
-- Pass 3: per-partition dedup
-------------------------------------------------------------------------------

{- | Run @body@ over partitions @[0, np)@ on a pool of workers pulling the next
partition from a shared counter. Each worker gets its own state; all states are
returned for the caller to merge.
-}
forPartitions :: Int -> IO s -> (s -> Int -> IO ()) -> IO [s]
forPartitions np newState body = do
    next <- newIORef 0
    let !width = if np > 1 && capabilities > 1 then min capabilities np else 1
    states <- replicateM width newState
    let worker st = do
            p <- atomicModifyIORef' next (\j -> (j + 1, j))
            when (p < np) (body st p >> worker st)
    forkJoin_ (map worker states)
    pure states
{-# INLINE forPartitions #-}

{- | Per-group counts from @body counts p@, which calls @bumpGroup counts g@
once per new pair of group @g@ in partition @p@. Small group domains count into
per-worker arrays summed at the end; wide ones share one array with atomic
increments, as replicating it per worker would cost more than the rare
contention.
-}
groupCounts :: Int -> Int -> (Counts -> Int -> IO ()) -> IO (VU.Vector Int)
groupCounts nGroups np body
    | nGroups * capabilities <= 1 `unsafeShiftL` 22 = do
        arrs <- forPartitions np (VUM.replicate nGroups 0) $ \arr p ->
            body (Counts False arr) p
        total <- VUM.replicate nGroups 0
        forM_ arrs $ \arr ->
            forM_ [0 .. nGroups - 1] $ \g -> do
                c <- VUM.unsafeRead arr g
                VUM.unsafeModify total (+ c) g
        VU.unsafeFreeze total
    | otherwise = do
        shared <- VUM.replicate nGroups 0
        _ <- forPartitions np (pure ()) $ \() p -> body (Counts True shared) p
        VU.unsafeFreeze shared
{-# INLINE groupCounts #-}

{- | A group-count array and whether other workers share it. Passed as data, not
as an increment closure, so the dedup loop's increment is inlined code.
-}
data Counts = Counts !Bool !(VUM.IOVector Int)

bumpGroup :: Counts -> Int -> IO ()
bumpGroup (Counts shared arr) g
    | shared = atomicIncr arr g
    | otherwise = VUM.unsafeModify arr (+ 1) g
{-# INLINE bumpGroup #-}

atomicIncr :: VUM.IOVector Int -> Int -> IO ()
atomicIncr (MV_Int (VPM.MVector (I# off) _ (MutableByteArray mba))) (I# i) =
    IO
        (\s -> case fetchAddIntArray# mba (off +# i) 1# s of (# s', _ #) -> (# s', () #))
{-# INLINE atomicIncr #-}

modifyStrict :: IORef Int -> (Int -> Int) -> IO ()
modifyStrict ref f = readIORef ref >>= \x -> writeIORef ref $! f x
{-# INLINE modifyStrict #-}

-- | Starting table width for a partition of @rows@ entries; it doubles as needed.
initialBits :: Int -> Int
initialBits rows = max 4 (min 12 (ceilLog2 (2 * rows)))

{- | Distinct words in @w0[s, e)@. Slots hold the word itself; 'minBound' marks
an empty slot, so a real 'minBound' key is tracked by a flag instead.
-}
dedupWords :: Int -> VU.Vector Int -> Int -> Int -> IO Int
dedupWords pb w0 s e = do
    let !bits0 = initialBits (e - s)
    t0 <- VUM.replicate (1 `unsafeShiftL` bits0) emptyWord
    let go !t !bits !cnt !sawEmpty !i
            | i >= e = pure (if sawEmpty then cnt + 1 else cnt)
            | otherwise = do
                let !k = VU.unsafeIndex w0 i
                if k == emptyWord
                    then go t bits cnt True (i + 1)
                    else do
                        let !mask = (1 `unsafeShiftL` bits) - 1
                            probe !sl = do
                                x <- VUM.unsafeRead t sl
                                if x == emptyWord
                                    then VUM.unsafeWrite t sl k >> pure True
                                    else
                                        if x == k
                                            then pure False
                                            else probe ((sl + 1) .&. mask)
                        new <- probe (slotOf pb bits (fmix k))
                        if not new
                            then go t bits cnt sawEmpty (i + 1)
                            else
                                if 2 * (cnt + 1) > mask + 1
                                    then do
                                        t' <- growWords pb t bits
                                        go t' (bits + 1) (cnt + 1) sawEmpty (i + 1)
                                    else go t bits (cnt + 1) sawEmpty (i + 1)
    go t0 bits0 0 False s

emptyWord :: Int
emptyWord = minBound

growWords :: Int -> VUM.IOVector Int -> Int -> IO (VUM.IOVector Int)
growWords pb t bits = do
    let !bits' = bits + 1
        !mask' = (1 `unsafeShiftL` bits') - 1
    t' <- VUM.replicate (mask' + 1) emptyWord
    forM_ [0 .. VUM.length t - 1] $ \j -> do
        k <- VUM.unsafeRead t j
        when (k /= emptyWord) $ do
            let place !sl = do
                    x <- VUM.unsafeRead t' sl
                    if x == emptyWord
                        then VUM.unsafeWrite t' sl k
                        else place ((sl + 1) .&. mask')
            place (slotOf pb bits' (fmix k))
    pure t'

{- | Distinct @(w0, w1)@ entries in @[s, e)@ under @eq@, slotted by @hashOf@.
An empty slot has a negative second word (group ids and row indices are not).
@onNew@ sees each first sighting.
-}
dedupPairs ::
    Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    Int ->
    Int ->
    (Int -> Int -> Int) ->
    (Int -> Int -> Int -> Int -> Bool) ->
    (Int -> Int -> IO ()) ->
    IO Int
dedupPairs pb w0 w1 s e hashOf eq onNew = do
    let !bits0 = initialBits (e - s)
    a0 <- VUM.unsafeNew (1 `unsafeShiftL` bits0)
    a1 <- VUM.replicate (1 `unsafeShiftL` bits0) (-1)
    let go !t0 !t1 !bits !cnt !i
            | i >= e = pure cnt
            | otherwise = do
                let !k0 = VU.unsafeIndex w0 i
                    !k1 = VU.unsafeIndex w1 i
                    !mask = (1 `unsafeShiftL` bits) - 1
                    probe !sl = do
                        y <- VUM.unsafeRead t1 sl
                        if y < 0
                            then do
                                VUM.unsafeWrite t0 sl k0
                                VUM.unsafeWrite t1 sl k1
                                pure True
                            else do
                                x <- VUM.unsafeRead t0 sl
                                if eq x y k0 k1
                                    then pure False
                                    else probe ((sl + 1) .&. mask)
                new <- probe (slotOf pb bits (hashOf k0 k1))
                if not new
                    then go t0 t1 bits cnt (i + 1)
                    else do
                        onNew k0 k1
                        if 2 * (cnt + 1) > mask + 1
                            then do
                                (t0', t1') <- growPairs pb hashOf t0 t1 bits
                                go t0' t1' (bits + 1) (cnt + 1) (i + 1)
                            else go t0 t1 bits (cnt + 1) (i + 1)
    go a0 a1 bits0 0 s
{-# INLINE dedupPairs #-}

growPairs ::
    Int ->
    (Int -> Int -> Int) ->
    VUM.IOVector Int ->
    VUM.IOVector Int ->
    Int ->
    IO (VUM.IOVector Int, VUM.IOVector Int)
growPairs pb hashOf t0 t1 bits = do
    let !bits' = bits + 1
        !mask' = (1 `unsafeShiftL` bits') - 1
    n0 <- VUM.unsafeNew (mask' + 1)
    n1 <- VUM.replicate (mask' + 1) (-1)
    forM_ [0 .. VUM.length t1 - 1] $ \j -> do
        y <- VUM.unsafeRead t1 j
        when (y >= 0) $ do
            x <- VUM.unsafeRead t0 j
            let place !sl = do
                    y' <- VUM.unsafeRead n1 sl
                    if y' < 0
                        then VUM.unsafeWrite n0 sl x >> VUM.unsafeWrite n1 sl y
                        else place ((sl + 1) .&. mask')
            place (slotOf pb bits' (hashOf x y))
    pure (n0, n1)
{-# INLINE growPairs #-}
