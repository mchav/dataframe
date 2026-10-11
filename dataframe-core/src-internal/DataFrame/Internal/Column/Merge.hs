{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- | Concatenation of per-chunk 'Column's, merging text at the byte level.
module DataFrame.Internal.Column.Merge (
    TextChunk (..),
    concatColumns,
    concatColumnsIO,
    mergeTextChunks,
    mergeTextChunksIO,
    packedFromTextChunk,
    concatValidity,
    tcRows,
) where

import qualified Data.Text.Array as A
import qualified Data.Vector as VB
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Monad (foldM_, forM_, when)
import Control.Monad.ST (runST, stToIO)
import Data.Type.Equality (testEquality, (:~:) (Refl))
import DataFrame.Internal.Column (
    Column (..),
    Columnable,
    isMergedColumn,
    isPackedText,
    materializeMerged,
    materializePacked,
 )
import DataFrame.Internal.Column.Bitmap (
    Bitmap,
    Validity (Validity),
    concatValidity,
 )
import DataFrame.Internal.Control.Concurrent (
    forkJoin_,
    parThreshold,
    shouldParallelize,
 )
import DataFrame.Internal.Data.PackedText (
    PackedOffsets (..),
    mkPackedContiguous,
    offAt,
    offCount,
    packedRowOffsets,
 )
import Type.Reflection (typeRep)

-- | A frozen text-builder chunk: UTF-8 bytes, row offsets and validity.
data TextChunk = TextChunk
    { tcBytes :: !A.Array
    , tcUsed :: !Int
    , tcOffsets :: !(VU.Vector Int)
    , tcBitmap :: !(Maybe Bitmap)
    }

tcRows :: TextChunk -> Int
tcRows c = VU.length (tcOffsets c) - 1

-- | Freeze a builder chunk directly into a packed-text column.
packedFromTextChunk :: TextChunk -> Column
packedFromTextChunk (TextChunk arr _used offs bm) =
    PackedText bm (mkPackedContiguous arr offs)

-- | Merge text chunks into one packed-text 'Column' with one copy per chunk.
mergeTextChunks :: [TextChunk] -> Column
mergeTextChunks [] = error "DataFrame.Internal.ColumnMerge.mergeTextChunks: empty list"
mergeTextChunks [c] = packedFromTextChunk c
mergeTextChunks cs = runST $ do
    let totalBytes = sum (map tcUsed cs)
        totalRows = sum (map tcRows cs)
    arr <- A.new (max 1 totalBytes)
    offs <- VUM.unsafeNew (totalRows + 1)
    VUM.unsafeWrite offs 0 0
    let splice !byteBase !rowBase c = do
            let n = tcRows c
                co = tcOffsets c
            A.copyI (tcUsed c) arr byteBase (tcBytes c) 0
            forM_ [1 .. n] $ \i ->
                VUM.unsafeWrite offs (rowBase + i) (byteBase + VU.unsafeIndex co i)
            pure (byteBase + tcUsed c, rowBase + n)
    foldM_ (\(b, r) c -> splice b r c) (0, 0) cs
    farr <- A.unsafeFreeze arr
    foffs <- VU.unsafeFreeze offs
    let !bm = concatValidity [Validity (tcBitmap c) (tcRows c) | c <- cs]
    pure (PackedText bm (mkPackedContiguous farr foffs))

-- | 'mergeTextChunks' with copies and offset rebases spread over workers.
mergeTextChunksIO :: [TextChunk] -> IO Column
mergeTextChunksIO [] = error "DataFrame.Internal.ColumnMerge.mergeTextChunksIO: empty list"
mergeTextChunksIO [c] = pure (packedFromTextChunk c)
mergeTextChunksIO cs
    | not (shouldParallelize parThreshold totalRows) = pure (mergeTextChunks cs)
    | otherwise = do
        arr <- stToIO (A.new (max 1 totalBytes))
        offs <- VUM.unsafeNew (totalRows + 1)
        VUM.unsafeWrite offs 0 0
        let byteBases = scanl (+) 0 (map tcUsed cs)
            rowBases = scanl (+) 0 (map tcRows cs)
            splice byteBase rowBase c = do
                let n = tcRows c
                    co = tcOffsets c
                    go !i = when (i <= n) $ do
                        VUM.unsafeWrite offs (rowBase + i) (byteBase + VU.unsafeIndex co i)
                        go (i + 1)
                stToIO (A.copyI (tcUsed c) arr byteBase (tcBytes c) 0)
                go 1
        forkJoin_ (zipWith3 splice byteBases rowBases cs)
        farr <- stToIO (A.unsafeFreeze arr)
        foffs <- VU.unsafeFreeze offs
        let !bm = concatValidity [Validity (tcBitmap c) (tcRows c) | c <- cs]
        pure (PackedText bm (mkPackedContiguous farr foffs))
  where
    totalBytes = sum (map tcUsed cs)
    totalRows = sum (map tcRows cs)

-- | 'concatColumns', merging packed text in parallel.
concatColumnsIO :: [Column] -> IO Column
concatColumnsIO cols
    | Just chunks <- mapM packedChunk cols = mergeTextChunksIO chunks
    | otherwise = pure (concatColumns cols)

-- | Merge per-chunk columns into one column.
concatColumns :: [Column] -> Column
concatColumns [] = error "DataFrame.Internal.Column.Builder.concatColumns: empty list"
concatColumns [c] = c
-- Normalize the whole list: a packed or merged chunk demotes all to boxed.
concatColumns cols
    | Just chunks <- mapM packedChunk cols = mergeTextChunks chunks
concatColumns cols@(c0 : _)
    | any isMergedColumn cols = concatColumns (map materializeMerged cols)
    | any isPackedText cols = concatColumns (map materializePacked cols)
concatColumns cols@(c0 : _) = case c0 of
    PackedText _ _ -> concatColumns (map materializePacked cols)
    MergedColumn _ _ -> concatColumns (map materializeMerged cols)
    UnboxedColumn _ (_ :: VU.Vector a) ->
        let parts = map (unboxedPart @a) cols
            !merged = VU.concat (map snd parts)
            !bm = concatValidity [Validity mb (VU.length v) | (mb, v) <- parts]
         in UnboxedColumn bm merged
    BoxedColumn _ (_ :: VB.Vector a) ->
        let parts = map (boxedPart @a) cols
            !merged = VB.concat (map snd parts)
            !bm = concatValidity [Validity mb (VB.length v) | (mb, v) <- parts]
         in BoxedColumn bm merged

unboxedPart ::
    forall a. (Columnable a, VU.Unbox a) => Column -> (Maybe Bitmap, VU.Vector a)
unboxedPart (UnboxedColumn mb (v :: VU.Vector b)) =
    case testEquality (typeRep @a) (typeRep @b) of
        Just Refl -> (mb, v)
        Nothing -> mergeMismatch
unboxedPart _ = mergeMismatch

boxedPart ::
    forall a. (Columnable a) => Column -> (Maybe Bitmap, VB.Vector a)
boxedPart (BoxedColumn mb (v :: VB.Vector b)) =
    case testEquality (typeRep @a) (typeRep @b) of
        Just Refl -> (mb, v)
        Nothing -> mergeMismatch
boxedPart _ = mergeMismatch

mergeMismatch :: a
mergeMismatch =
    error
        "DataFrame.Internal.Column.Builder.concatColumns: chunk column types differ"

-- | A packed-text column without a selection, viewed as a builder chunk.
packedChunk :: Column -> Maybe TextChunk
packedChunk (PackedText bm p) = case packedRowOffsets p of
    Just (arr, offs) ->
        let n = offCount offs - 1
            offsV = case offs of
                Offs64 v -> v
                Offs32 v -> VU.map fromIntegral v
         in Just (TextChunk arr (offAt offs n) offsV bm)
    Nothing -> Nothing
packedChunk _ = Nothing
