{-# LANGUAGE BangPatterns #-}

-- | Packed-text payload and byte-slice primitives over one shared buffer.
module DataFrame.Internal.Data.PackedText (
    PackedTextData (..),
    PackedOffsets (..),
    PackedSel (..),
    offAt,
    offCount,
    selAt,
    selLength,
    mkPackedContiguous,
    mkPackedContiguous32,
    mkOffsets,
    mkSel,
    packedGather,
    packedTake,
    packedRowOffsets,
    packedLength,
    packedSlice,
    packedIndexText,
    sliceEqBytes,
    sliceCmpBytes,
) where

import qualified Data.Text as T
import qualified Data.Text.Array as A
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Data.Int (Int32)
import Data.Ord (comparing)
import Data.Text.Internal (Text (Text))
import DataFrame.Internal.Control.Concurrent (
    parThreshold,
    parallelChunks_,
    shouldParallelize,
 )
import DataFrame.Internal.Data.PackedText.Utf8 (
    isValidUtf8Slice,
    lenientDecodeSlice,
 )
import System.IO.Unsafe (unsafePerformIO)

-- | Row byte-offsets, 'Int32' when every value fits and 'Int' otherwise.
data PackedOffsets
    = Offs32 {-# UNPACK #-} !(VU.Vector Int32)
    | Offs64 {-# UNPACK #-} !(VU.Vector Int)

-- | Offset at index @i@, widened to 'Int'.
offAt :: PackedOffsets -> Int -> Int
offAt (Offs32 v) i = fromIntegral (VU.unsafeIndex v i)
offAt (Offs64 v) i = VU.unsafeIndex v i
{-# INLINE offAt #-}

-- | Number of offset entries (row count + 1).
offCount :: PackedOffsets -> Int
offCount (Offs32 v) = VU.length v
offCount (Offs64 v) = VU.length v
{-# INLINE offCount #-}

-- | A selection layer mapping logical rows to base rows; @-1@ marks null.
data PackedSel
    = Sel32 {-# UNPACK #-} !(VU.Vector Int32)
    | Sel64 {-# UNPACK #-} !(VU.Vector Int)

-- | Base row for logical row @i@ (may be @-1@).
selAt :: PackedSel -> Int -> Int
selAt (Sel32 v) i = fromIntegral (VU.unsafeIndex v i)
selAt (Sel64 v) i = VU.unsafeIndex v i
{-# INLINE selAt #-}

selLength :: PackedSel -> Int
selLength (Sel32 v) = VU.length v
selLength (Sel64 v) = VU.length v
{-# INLINE selLength #-}

-- | A shared UTF-8 byte buffer plus @n+1@ row offsets and optional selection.
data PackedTextData = PackedTextData
    { ptBytes :: {-# UNPACK #-} !A.Array
    , ptOffsets :: !PackedOffsets
    , ptSel :: !(Maybe PackedSel)
    , ptCanonicalSel :: !Bool
    }

int32Max :: Int
int32Max = fromIntegral (maxBound :: Int32)

-- | Narrow an 'Int' offset vector when the final offset (total bytes) fits.
mkOffsets :: VU.Vector Int -> PackedOffsets
mkOffsets offs
    | not (VU.null offs) && VU.last offs <= int32Max =
        Offs32 (VU.map fromIntegral offs)
    | otherwise = Offs64 offs
{-# INLINE mkOffsets #-}

-- | Narrow an 'Int' base-row vector to 'Int32' when the row count fits.
mkSel :: Int -> VU.Vector Int -> PackedSel
mkSel base rows
    | base <= int32Max = Sel32 (VU.map fromIntegral rows)
    | otherwise = Sel64 rows
{-# INLINE mkSel #-}

-- | Build a contiguous packed payload (no selection): the freeze-path shape.
mkPackedContiguous :: A.Array -> VU.Vector Int -> PackedTextData
mkPackedContiguous arr offs = PackedTextData arr (mkOffsets offs) Nothing False
{-# INLINE mkPackedContiguous #-}

-- | 'mkPackedContiguous' from offsets already produced at 'Int32' width.
mkPackedContiguous32 :: A.Array -> VU.Vector Int32 -> PackedTextData
mkPackedContiguous32 arr offs = PackedTextData arr (Offs32 offs) Nothing False
{-# INLINE mkPackedContiguous32 #-}

-- | Reindex a packed payload by a selection vector, sharing the byte buffer.
packedGather :: VU.Vector Int -> PackedTextData -> PackedTextData
packedGather indices (PackedTextData arr offs msel canon) =
    let !base = offCount offs - 1
        canon' = case msel of
            Nothing -> False
            Just _ -> canon
        -- Only the selector is rebuilt; bytes and offsets stay shared.
        sel' = case msel of
            Nothing
                | base <= int32Max -> Sel32 (gatherBaseTo32 indices base)
                | otherwise -> Sel64 (gatherBaseTo64 indices base)
            Just (Sel32 s)
                | base <= int32Max -> Sel32 (gatherSel32To32 indices s base)
                | otherwise -> Sel64 (gatherSel32To64 indices s base)
            Just (Sel64 s)
                | base <= int32Max -> Sel32 (gatherSel64To32 indices s base)
                | otherwise -> Sel64 (gatherSel64To64 indices s base)
     in PackedTextData arr offs (Just sel') canon'

-- Monomorphic 'packedGather' selector kernels; out-of-range picks give -1.

gatherBaseTo32 :: VU.Vector Int -> Int -> VU.Vector Int32
gatherBaseTo32 indices !base =
    parGenSelInline (VU.length indices) $ \i ->
        let !r = VU.unsafeIndex indices i
         in if r >= 0 && r < base then fromIntegral r else -1
{-# NOINLINE gatherBaseTo32 #-}

gatherBaseTo64 :: VU.Vector Int -> Int -> VU.Vector Int
gatherBaseTo64 indices !base =
    parGenSelInline (VU.length indices) $ \i ->
        let !r = VU.unsafeIndex indices i
         in if r >= 0 && r < base then r else -1
{-# NOINLINE gatherBaseTo64 #-}

gatherSel32To32 :: VU.Vector Int -> VU.Vector Int32 -> Int -> VU.Vector Int32
gatherSel32To32 indices s !base =
    let !sn = VU.length s
     in parGenSelInline (VU.length indices) $ \i ->
            let !j = VU.unsafeIndex indices i
             in if j >= 0 && j < sn
                    then
                        let !r = fromIntegral (VU.unsafeIndex s j) :: Int
                         in if r >= 0 && r < base then fromIntegral r else -1
                    else -1
{-# NOINLINE gatherSel32To32 #-}

gatherSel32To64 :: VU.Vector Int -> VU.Vector Int32 -> Int -> VU.Vector Int
gatherSel32To64 indices s !base =
    let !sn = VU.length s
     in parGenSelInline (VU.length indices) $ \i ->
            let !j = VU.unsafeIndex indices i
             in if j >= 0 && j < sn
                    then
                        let !r = fromIntegral (VU.unsafeIndex s j) :: Int
                         in if r >= 0 && r < base then r else -1
                    else -1
{-# NOINLINE gatherSel32To64 #-}

gatherSel64To32 :: VU.Vector Int -> VU.Vector Int -> Int -> VU.Vector Int32
gatherSel64To32 indices s !base =
    let !sn = VU.length s
     in parGenSelInline (VU.length indices) $ \i ->
            let !j = VU.unsafeIndex indices i
             in if j >= 0 && j < sn
                    then
                        let !r = VU.unsafeIndex s j
                         in if r >= 0 && r < base then fromIntegral r else -1
                    else -1
{-# NOINLINE gatherSel64To32 #-}

gatherSel64To64 :: VU.Vector Int -> VU.Vector Int -> Int -> VU.Vector Int
gatherSel64To64 indices s !base =
    let !sn = VU.length s
     in parGenSelInline (VU.length indices) $ \i ->
            let !j = VU.unsafeIndex indices i
             in if j >= 0 && j < sn
                    then
                        let !r = VU.unsafeIndex s j
                         in if r >= 0 && r < base then r else -1
                    else -1
{-# NOINLINE gatherSel64To64 #-}

-- elector generate over 'parallelChunks_', one chunk per capability.

-- | 'parGenSel' with an INLINE body so each kernel inlines its pick function.
parGenSelInline :: (VU.Unbox c) => Int -> (Int -> c) -> VU.Vector c
parGenSelInline n f
    | not (shouldParallelize parThreshold n) = VU.generate n f
    | otherwise = unsafePerformIO $ do
        mv <- VUM.unsafeNew n
        parallelChunks_ parThreshold n $ \ !lo !hi ->
            let fill !i
                    | i >= hi = pure ()
                    | otherwise = VUM.unsafeWrite mv i (f i) >> fill (i + 1)
             in fill lo
        VU.unsafeFreeze mv
{-# INLINE parGenSelInline #-}

parGenSel :: (VU.Unbox c) => Int -> (Int -> c) -> VU.Vector c
parGenSel n f
    | not (shouldParallelize parThreshold n) = VU.generate n f
    | otherwise = unsafePerformIO $ do
        mv <- VUM.unsafeNew n
        parallelChunks_ parThreshold n $ \ !lo !hi ->
            let fill !i
                    | i >= hi = pure ()
                    | otherwise = VUM.unsafeWrite mv i (f i) >> fill (i + 1)
             in fill lo
        VU.unsafeFreeze mv
{-# NOINLINE parGenSel #-}

-- | Take the first @k@ logical rows, sharing the byte buffer, in O(k).
packedTake :: Int -> PackedTextData -> PackedTextData
packedTake k (PackedTextData arr offs msel canon) =
    let !base = offCount offs - 1
        !k' = max 0 k
        (sel', canon') = case msel of
            Just (Sel32 s) -> (Sel32 (VU.take k' s), canon)
            Just (Sel64 s) -> (Sel64 (VU.take k' s), canon)
            Nothing -> (mkSel base (VU.enumFromN 0 (min k' base)), False)
     in PackedTextData arr offs (Just sel') canon'
{-# INLINE packedTake #-}

-- | Map a logical row index to its base row, honoring any selection layer.
baseRow :: PackedTextData -> Int -> Int
baseRow (PackedTextData _ _ Nothing _) i = i
baseRow (PackedTextData _ _ (Just sel) _) i = selAt sel i
{-# INLINE baseRow #-}

-- | Row count: @length sel@ when selected, else @length offsets - 1@.
packedLength :: PackedTextData -> Int
packedLength (PackedTextData _ offs Nothing _) = offCount offs - 1
packedLength (PackedTextData _ _ (Just sel) _) = selLength sel
{-# INLINE packedLength #-}

-- | Raw byte slice for logical row @i@: @(buffer, offset, length)@.
packedSlice :: PackedTextData -> Int -> (A.Array, Int, Int)
packedSlice p@(PackedTextData arr offs _ _) i =
    let !r = baseRow p i
     in if r < 0
            then (arr, 0, 0)
            else
                let o = offAt offs r in (arr, o, offAt offs (r + 1) - o)
{-# INLINE packedSlice #-}

-- | The shared buffer and contiguous offsets when the payload is unselected.
packedRowOffsets :: PackedTextData -> Maybe (A.Array, PackedOffsets)
packedRowOffsets (PackedTextData arr offs Nothing _) = Just (arr, offs)
packedRowOffsets _ = Nothing
{-# INLINE packedRowOffsets #-}

-- | On-demand single 'Data.Text.Text' for row @i@, similar to freeze.
packedIndexText :: PackedTextData -> Int -> T.Text
packedIndexText p i =
    let (arr, o, l) = packedSlice p i
     in decodeField arr o l
{-# INLINE packedIndexText #-}

-- Decode one field exactly as the boxed freeze path does per row.
decodeField :: A.Array -> Int -> Int -> T.Text
decodeField arr o l
    | l == 0 = T.empty
    | isValidUtf8Slice arr o l = Text arr o l
    | otherwise = lenientDecodeSlice arr o l
{-# INLINE decodeField #-}

-- | Byte-wise equality of two slices, agreeing with 'Text' equality.
sliceEqBytes :: A.Array -> Int -> Int -> A.Array -> Int -> Int -> Bool
sliceEqBytes a ao al b bo bl
    | al /= bl = False
    | otherwise = go 0
  where
    go !k
        | k >= al = True
        | A.unsafeIndex a (ao + k) == A.unsafeIndex b (bo + k) = go (k + 1)
        | otherwise = False
{-# INLINE sliceEqBytes #-}

-- | Unsigned byte-lexicographic comparison of two slices (memcmp semantics).
sliceCmpBytes :: A.Array -> Int -> Int -> A.Array -> Int -> Int -> Ordering
sliceCmpBytes a ao al b bo bl = go 0
  where
    !m = min al bl
    go !k
        | k >= m = compare al bl
        | otherwise = case comparing id (A.unsafeIndex a (ao + k)) (A.unsafeIndex b (bo + k)) of
            EQ -> go (k + 1)
            r -> r
{-# INLINE sliceCmpBytes #-}
