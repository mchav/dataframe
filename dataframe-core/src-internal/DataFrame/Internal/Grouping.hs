{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE Strict #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Internal.Grouping (
    groupBy,
    groupBySeq,
    groupByPar,
    buildRowToGroup,
    changingPoints,
) where

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Exception (throw)
import Control.Monad
import Control.Monad.ST (ST, runST)
import Data.Bits (unsafeShiftR, (.&.))
import Data.Type.Equality (TestEquality (..), type (:~:) (Refl))
import DataFrame.Errors
import DataFrame.Internal.Algorithms.Hash
import DataFrame.Internal.Algorithms.Rank.Radix (rankByHash)
import DataFrame.Internal.Column (
    Column (..),
    materializeMerged,
    widenToInt,
 )
import DataFrame.Internal.Column.Bitmap (
    Bitmap,
    bitmapTestBit,
 )
import DataFrame.Internal.Column.Encode (dictEncodeColumnUpTo)
import DataFrame.Internal.Column.Types
import DataFrame.Internal.Control.Concurrent (
    chunksFor,
    forkJoin,
    parThreshold,
    shouldParallelize,
 )
import DataFrame.Internal.Data.HashTable (htInsert, newHashTable)
import DataFrame.Internal.Data.PackedText (
    PackedOffsets (..),
    PackedTextData (..),
    offCount,
    packedLength,
    packedSlice,
    selAt,
    sliceEqBytes,
 )
import DataFrame.Internal.DataFrame (DataFrame (..), GroupedDataFrame (..))
import DataFrame.Internal.Grouping.Direct (
    ascendingCodeGroups,
    directGroupThreshold,
    rangeOf,
 )
import qualified DataFrame.Internal.Grouping.Direct as GD
import DataFrame.Internal.Grouping.Partitioned (
    parallelAssignGroups,
    rtgFromVisOffs,
 )
import DataFrame.Internal.Row.RowHash (computeRowHashesWithIO)
import System.IO.Unsafe (unsafePerformIO)
import Type.Reflection (typeRep)

-- | O(k * n) group the dataframe by the given key columns via a hash table.
groupBy ::
    [T.Text] ->
    DataFrame ->
    GroupedDataFrame
groupBy names df
    | any (`notElem` columnNames df) names =
        throw $
            ColumnsNotFoundException
                (names L.\\ columnNames df)
                "groupBy"
                (columnNames df)
    | nRows df == 0 =
        Grouped
            df
            names
            VU.empty
            (VU.fromList [0])
            VU.empty
    | Just dg <- tryDirectGroup names df = dg
    | shouldParallelize parThreshold n = groupByPar names df
    | otherwise = groupBySeq names df
  where
    !n = nRows df

-- | Low-cardinality direct-indexed grouping fast path for multi-key lists.
tryDirectGroup :: [T.Text] -> DataFrame -> Maybe GroupedDataFrame
tryDirectGroup [] _ = Nothing
tryDirectGroup names df = do
    cols <-
        traverse (\nm -> M.lookup nm (columnIndices df) >>= (columns df V.!?)) names
    case traverse fusedKey cols of
        Just keys -> fusedDirectGroup names df keys
        Nothing -> case (names, cols) of
            ([name], [col]) -> tryDictGroup (nRows df) df [name] col
            _ -> Nothing

-- | One key column of a fused multi-key direct grouping.
data FusedKey = FusedKey
    { fkCode :: Int -> Int
    , fkDomain :: !Int
    }

-- | Classify a key column for the fused multi-key direct path.
fusedKey :: Column -> Maybe FusedKey
fusedKey col@(UnboxedColumn Nothing (_ :: VU.Vector a))
    | Just v <- asIntKeys col
    , not (VU.null v) =
        let (!mn, !mx) = rangeOf v
            !range = mx - mn + 1
         in if range >= 1 && range <= directGroupThreshold
                then Just (FusedKey (\i -> VU.unsafeIndex v i - mn) range)
                else Nothing
fusedKey (PackedText Nothing p)
    | Just sel <- ptSel p
    , ptCanonicalSel p =
        let offs = ptOffsets p
            !card = offCount offs - 1
         in if card >= 1 && card <= directGroupThreshold
                then
                    Just
                        ( FusedKey
                            (\i -> let c = selAt sel i in if c >= card then -1 else c)
                            card
                        )
                else Nothing
fusedKey _ = Nothing

-- | An integral key column as 'Int's; floats are refused.
asIntKeys :: Column -> Maybe (VU.Vector Int)
asIntKeys (UnboxedColumn Nothing (v :: VU.Vector a)) = case sFloating @a of
    STrue -> Nothing
    SFalse -> widenToInt v
asIntKeys _ = Nothing

-- | Fuse per-key codes into one mixed-radix code per row and group directly.
fusedDirectGroup ::
    [T.Text] -> DataFrame -> [FusedKey] -> Maybe GroupedDataFrame
fusedDirectGroup names df keys = do
    domain <- fusedDomain (map fkDomain keys)
    let n = nRows df
        codeAt' = fusedCodeAt keys
    if GD.useTwoLevel n domain
        then do
            -- Wide domains use the radix engine; valueIndices stays deferred.
            (rtg, offs, reps, nGroups) <-
                GD.directLayoutLazy codeAt' n domain ascendingCodeGroups
            Just
                ( GroupedInternal
                    df
                    names
                    (GD.visFromRowToGroup n nGroups offs rtg)
                    offs
                    rtg
                    reps
                )
        else do
            (offs, reps, counts, ctg, hists, nGroups) <-
                directLayoutLazy codeAt' n domain ascendingCodeGroups
            Just
                ( GroupedInternal
                    df
                    names
                    (visFromCodes codeAt' counts ctg offs hists n domain)
                    offs
                    (rtgFromCodes codeAt' ctg n)
                    reps
                )

-- | Product of the per-key domains, or 'Nothing' past the direct threshold.
fusedDomain :: [Int] -> Maybe Int
fusedDomain = go 1
  where
    go !acc [] = Just acc
    go !acc (d : ds)
        | d < 1 || d > directGroupThreshold = Nothing
        | acc * d > directGroupThreshold = Nothing
        | otherwise = go (acc * d) ds

-- | Per-row fused mixed-radix code; @-1@ when any component code is invalid.
fusedCodeAt :: [FusedKey] -> (Int -> Int)
fusedCodeAt [] = const (-1)
fusedCodeAt (k0 : ks0) = go (fkCode k0) ks0
  where
    go f [] = f
    go f (k : ks) =
        let !d = fkDomain k
            g = fkCode k
         in go
                ( \i ->
                    let a = f i
                     in if a < 0
                            then -1
                            else let b = g i in if b < 0 then -1 else a * d + b
                )
                ks

-- | Dictionary-encode a single text key, then group by counting sort.
tryDictGroup ::
    Int -> DataFrame -> [T.Text] -> Column -> Maybe GroupedDataFrame
tryDictGroup n df names col
    | dictGroupEnabled && not (shouldParallelize parThreshold n) = do
        (codes, card) <- dictEncodeColumnUpTo dictSingleThreshold col
        let (vis, os) = indicesFromGroups codes card
        Just (Grouped df names vis os codes)
    | otherwise = Nothing

-- | Master switch for the single-key dict-encode grouping path.
dictGroupEnabled :: Bool
dictGroupEnabled = False

-- | Cardinality ceiling for the single-key dict-encode probe.
dictSingleThreshold :: Int
dictSingleThreshold = 4096

-- | The sequential grouping path: one open-addressing table over all rows.
groupBySeq :: [T.Text] -> DataFrame -> GroupedDataFrame
groupBySeq names df =
    let !n = nRows df
        indicesToGroup = keyColIndices names df
        (rtg0, repHash, repRow) = assignGroups df indicesToGroup n
        !nGroups = VU.length repHash
        !remap = canonicalRemap repHash repRow
        !rtg = VU.map (VU.unsafeIndex remap) rtg0
        (vis, os) = indicesFromGroups rtg nGroups
     in Grouped df names vis os rtg

-- | The parallel partitioned grouping path, identical in output to sequential.
groupByPar :: [T.Text] -> DataFrame -> GroupedDataFrame
groupByPar names df =
    let !n = nRows df
        indicesToGroup = keyColIndices names df
        -- Merged key columns are exotic; hash their eager form.
        selectedCols = map (materializeMerged . (columns df V.!)) indicesToGroup
        !eqRow = eqKeyRow df indicesToGroup
        (vis, os) = unsafePerformIO $ do
            hashes <- computeRowHashesWithIO True n selectedCols
            parallelAssignGroups n hashes eqRow
     in -- rowToGroup stays an unforced argument (the module is -XStrict).
        Grouped df names vis os (rtgFromVisOffs n vis os)
{-# NOINLINE groupByPar #-}

-- | Column indices of the requested key columns, in column order.
keyColIndices :: [T.Text] -> DataFrame -> [Int]
keyColIndices names df =
    M.elems $ M.filterWithKey (\k _ -> k `elem` names) (columnIndices df)

-- | Assign every row a dense group id in first-appearance order.
assignGroups ::
    DataFrame -> [Int] -> Int -> (VU.Vector Int, VU.Vector Int, VU.Vector Int)
assignGroups df indicesToGroup n = runST $ do
    hashes <- computeHashes df indicesToGroup n
    let !eqRow = eqKeyRow df indicesToGroup
    ht <- newHashTable n
    rtg <- VUM.new n
    repHashM <- VUM.new n
    repRowM <- VUM.new n
    let go !i !next
            | i >= n = pure next
            | otherwise = do
                let !h = VU.unsafeIndex hashes i
                (gid, isNew) <- htInsert ht eqRow next i h
                VUM.unsafeWrite rtg i gid
                when isNew $ do
                    VUM.unsafeWrite repHashM next h
                    VUM.unsafeWrite repRowM next i
                go (i + 1) (if isNew then next + 1 else next)
    !nGroups <- go 0 0
    frozen <- VU.unsafeFreeze rtg
    repHash <- VU.unsafeFreeze (VUM.slice 0 nGroups repHashM)
    repRow <- VU.unsafeFreeze (VUM.slice 0 nGroups repRowM)
    pure (frozen, repHash, repRow)

-- | Map each first-appearance group id to its canonical id (by hash).
canonicalRemap :: VU.Vector Int -> VU.Vector Int -> VU.Vector Int
canonicalRemap repHash _repRow =
    runST (rankByHash (pure . VU.unsafeIndex repHash) (VU.length repHash))

-- | Compute the FNV row-hash of the key columns into an unboxed vector.
computeHashes :: DataFrame -> [Int] -> Int -> ST s (VU.Vector Int)
computeHashes df indicesToGroup n = do
    mh <- VUM.replicate n fnvOffset
    -- Merged key columns are exotic; hash their eager form.
    let selectedCols = map (materializeMerged . (columns df V.!)) indicesToGroup
    forM_ selectedCols $ \case
        UnboxedColumn ubm (v :: VU.Vector a) ->
            case testEquality (typeRep @a) (typeRep @Int) of
                Just Refl -> hashUnboxed mh ubm mixInt v
                Nothing ->
                    case testEquality (typeRep @a) (typeRep @Double) of
                        Just Refl -> hashUnboxed mh ubm mixDouble v
                        Nothing ->
                            case sIntegral @a of
                                STrue ->
                                    hashUnboxed mh ubm (\h d -> mixInt h (fromIntegral @a @Int d)) v
                                SFalse ->
                                    case sFloating @a of
                                        STrue ->
                                            hashUnboxed mh ubm (\h d -> mixDouble h (realToFrac d :: Double)) v
                                        SFalse ->
                                            hashUnboxed mh ubm mixShow v
        BoxedColumn bm (v :: V.Vector a) ->
            case testEquality (typeRep @a) (typeRep @T.Text) of
                Just Refl ->
                    V.imapM_
                        ( \i t -> do
                            !h <- VUM.unsafeRead mh i
                            let h' = case bm of
                                    Just bm' | not (bitmapTestBit bm' i) -> mixInt h nullSalt
                                    _ -> mixText h t
                            VUM.unsafeWrite mh i h'
                        )
                        v
                Nothing ->
                    V.imapM_
                        ( \i d -> do
                            !h <- VUM.unsafeRead mh i
                            let h' = case bm of
                                    Just bm' | not (bitmapTestBit bm' i) -> mixInt h nullSalt
                                    _ -> mixShow h d
                            VUM.unsafeWrite mh i h'
                        )
                        v
        PackedText bm p -> hashPacked mh bm p
        MergedColumn _ _ ->
            error "computeHashes: MergedColumn is normalized before hashing"
    VU.unsafeFreeze mh

-- | Build the row-key equality predicate over the selected key columns.
eqKeyRow :: DataFrame -> [Int] -> Int -> Int -> Bool
eqKeyRow df indicesToGroup = case map (colEqRow . (columns df V.!)) indicesToGroup of
    [p] -> p
    preds ->
        let go [] _ _ = True
            go (p : ps) a b = p a b && go ps a b
         in go preds

-- | Per-column row equality respecting nulls.
colEqRow :: Column -> (Int -> Int -> Bool)
colEqRow c@(MergedColumn _ _) = colEqRow (materializeMerged c)
colEqRow (UnboxedColumn bm v) =
    let eqV a b = VU.unsafeIndex v a == VU.unsafeIndex v b
     in withNulls bm eqV
colEqRow (BoxedColumn bm v) =
    let eqV a b = V.unsafeIndex v a == V.unsafeIndex v b
     in withNulls bm eqV
colEqRow (PackedText bm p) =
    -- A canonical dictionary selection gives equal strings equal codes.
    let eqV = case ptSel p of
            Just sel
                | ptCanonicalSel p ->
                    \a b -> selAt sel a == selAt sel b
            _ -> packedEqRow p
     in withNulls bm eqV
{-# INLINE colEqRow #-}

-- | Byte equality of two packed-text rows, with the layout matched once.
packedEqRow :: PackedTextData -> (Int -> Int -> Bool)
packedEqRow (PackedTextData arr offs sel _) = case (sel, offs) of
    (Nothing, Offs32 o) -> \a b -> a == b || eqBase (off32 o) a b
    (Nothing, Offs64 o) -> \a b -> a == b || eqBase (VU.unsafeIndex o) a b
    (Just s, Offs32 o) -> eqSel s (off32 o)
    (Just s, Offs64 o) -> eqSel s (VU.unsafeIndex o)
  where
    off32 o i = fromIntegral (VU.unsafeIndex o i)
    -- Base rows; equal ones share bytes, and -1 is the empty string.
    eqSel s off a b =
        let !ra = selAt s a
            !rb = selAt s b
         in ra == rb || eqBase off ra rb
    eqBase off ra rb =
        let span r = if r < 0 then (0, 0) else let o = off r in (o, off (r + 1) - o)
            (!oa, !la) = span ra
            (!ob, !lb) = span rb
         in sliceEqBytes arr oa la arr ob lb
    {-# INLINE eqSel #-}
    {-# INLINE eqBase #-}

-- | Wrap a value-equality with null handling (both null counts as equal).
withNulls :: Maybe Bitmap -> (Int -> Int -> Bool) -> (Int -> Int -> Bool)
withNulls Nothing eqV = eqV
withNulls (Just bm) eqV = \a b ->
    case (bitmapTestBit bm a, bitmapTestBit bm b) of
        (True, True) -> eqV a b
        (False, False) -> True
        _ -> False
{-# INLINE withNulls #-}

-- | Derive @(valueIndices, offsets)@ from @rowToGroup@ by stable counting sort.
indicesFromGroups :: VU.Vector Int -> Int -> (VU.Vector Int, VU.Vector Int)
indicesFromGroups rtg nGroups = runST $ do
    let !n = VU.length rtg
    counts <- VUM.replicate (nGroups + 1) 0
    let countLoop !i
            | i >= n = pure ()
            | otherwise = do
                let !g = VU.unsafeIndex rtg i
                c <- VUM.unsafeRead counts g
                VUM.unsafeWrite counts g (c + 1)
                countLoop (i + 1)
    countLoop 0
    offsM <- VUM.new (nGroups + 1)
    let scan !k !acc
            | k > nGroups = pure ()
            | otherwise = do
                VUM.unsafeWrite offsM k acc
                c <- VUM.unsafeRead counts k
                scan (k + 1) (acc + c)
    scan 0 0
    let seed !k
            | k > nGroups = pure ()
            | otherwise = do
                s <- VUM.unsafeRead offsM k
                VUM.unsafeWrite counts k s
                seed (k + 1)
    seed 0
    vis <- VUM.new n
    let place !i
            | i >= n = pure ()
            | otherwise = do
                let !g = VU.unsafeIndex rtg i
                pos <- VUM.unsafeRead counts g
                VUM.unsafeWrite vis pos i
                VUM.unsafeWrite counts g (pos + 1)
                place (i + 1)
    place 0
    offs <- VU.unsafeFreeze offsM
    frozenVis <- VU.unsafeFreeze vis
    pure (frozenVis, offs)

-- | Contiguous per-worker row ranges, one chunk per capability when parallel.
directRowChunks :: Int -> [(Int, Int)]
directRowChunks = chunksFor parThreshold

-- | Like 'directRowChunks' but over a code/group domain.
directCodeSlices :: Int -> [(Int, Int)]
directCodeSlices = chunksFor 4096

-- | The eager phases of the direct counting-sort grouping.
directLayoutLazy ::
    (Int -> Int) ->
    Int ->
    Int ->
    (VU.Vector Int -> (VU.Vector Int, Int)) ->
    Maybe
        ( VU.Vector Int
        , VU.Vector Int
        , VU.Vector Int
        , VU.Vector Int
        , [VU.Vector Int]
        , Int
        )
directLayoutLazy codeAt n card mkGroups
    | n <= 0 || card <= 0 = Nothing
    | n < packedRowLimit = unsafePerformIO $ do
        -- Packed variant: count and first row share one word per code.
        let chunks = directRowChunks n
        parts <- forkJoin [histFirstChunkPacked codeAt card lo hi | (lo, hi) <- chunks]
        if not (all snd parts)
            then pure Nothing
            else finishLayout card mkGroups (map fst parts) $ \histsM totalsM firstAllM lo hi ->
                sumFirstSlicePacked histsM totalsM firstAllM lo hi
    | otherwise = unsafePerformIO $ do
        -- Fallback for huge frames: separate count and firstOcc arrays.
        let chunks = directRowChunks n
        parts <- forkJoin [histFirstChunk codeAt card lo hi | (lo, hi) <- chunks]
        if not (all (\(_, _, ok) -> ok) parts)
            then pure Nothing
            else
                finishLayout
                    card
                    mkGroups
                    (map (\(h, _, _) -> h) parts)
                    ( \histsM totalsM firstAllM lo hi ->
                        sumFirstSlice histsM (map (\(_, f, _) -> f) parts) totalsM firstAllM lo hi
                    )
{-# NOINLINE directLayoutLazy #-}

-- | Shared tail of 'directLayoutLazy': merge, normalise and build the layout.
finishLayout ::
    Int ->
    (VU.Vector Int -> (VU.Vector Int, Int)) ->
    [VUM.IOVector Int] ->
    ( [VUM.IOVector Int] ->
      VUM.IOVector Int ->
      VUM.IOVector Int ->
      Int ->
      Int ->
      IO ()
    ) ->
    IO
        ( Maybe
            ( VU.Vector Int
            , VU.Vector Int
            , VU.Vector Int
            , VU.Vector Int
            , [VU.Vector Int]
            , Int
            )
        )
finishLayout card mkGroups histsM mergeSlice = do
    totalsM <- VUM.new card
    firstAllM <- VUM.new card
    _ <-
        forkJoin
            [ mergeSlice histsM totalsM firstAllM lo hi
            | (lo, hi) <- directCodeSlices card
            ]
    counts <- VU.unsafeFreeze totalsM
    firstAll <- VU.unsafeFreeze firstAllM
    let (codeToGroup, nGroups) = mkGroups counts
    offs <- scanGroupOffsets counts codeToGroup nGroups
    repsM <- VUM.new nGroups
    _ <-
        forkJoin
            [ scatterRepsSlice counts codeToGroup firstAll repsM lo hi
            | (lo, hi) <- directCodeSlices card
            ]
    reps <- VU.unsafeFreeze repsM
    hists <- mapM VU.unsafeFreeze histsM
    pure (Just (offs, reps, counts, codeToGroup, hists, nGroups))

-- | Row limit for the packed count/first-row encoding (@row + 1 < 2^31@).
packedRowLimit :: Int
packedRowLimit = 0x7FFFFFFF

-- | One unit of count in the packed encoding; also the low-bits mask + 1.
packedCountOne :: Int
packedCountOne = 0x80000000

-- | Whether @codeToGroup@ maps every code to itself.
isIdentityMap :: VU.Vector Int -> Bool
isIdentityMap m = go 0
  where
    !k = VU.length m
    go !i
        | i >= k = True
        | VU.unsafeIndex m i /= i = False
        | otherwise = go (i + 1)

-- | Histogram one row chunk with the packed count and first-row encoding.
histFirstChunkPacked ::
    (Int -> Int) -> Int -> Int -> Int -> IO (VUM.IOVector Int, Bool)
histFirstChunkPacked codeAt card lo hi = do
    acc <- VUM.replicate card (0 :: Int)
    let go !i
            | i >= hi = pure True
            | otherwise = do
                let !c = codeAt i
                if c < 0 || c >= card
                    then pure False
                    else do
                        x <- VUM.unsafeRead acc c
                        VUM.unsafeWrite
                            acc
                            c
                            (if x == 0 then packedCountOne + (i + 1) else x + packedCountOne)
                        go (i + 1)
    ok <- go lo
    pure (acc, ok)

-- | Per-code totals and first occurrences from the packed chunk histograms.
sumFirstSlicePacked ::
    [VUM.IOVector Int] ->
    VUM.IOVector Int ->
    VUM.IOVector Int ->
    Int ->
    Int ->
    IO ()
sumFirstSlicePacked hists totals firstAll lo hi = go lo
  where
    go !c
        | c >= hi = pure ()
        | otherwise = do
            let sumP [] !acc !firstRow = pure (acc, firstRow)
                sumP (h : hs) !acc !firstRow = do
                    x <- VUM.unsafeRead h c
                    let !cnt = x `unsafeShiftR` 31
                    VUM.unsafeWrite h c cnt
                    if firstRow < 0 && x /= 0
                        then sumP hs (acc + cnt) ((x .&. (packedCountOne - 1)) - 1)
                        else sumP hs (acc + cnt) firstRow
            (s, fo) <- sumP hists 0 (-1)
            VUM.unsafeWrite totals c s
            VUM.unsafeWrite firstAll c fo
            go (c + 1)

-- | Histogram one row chunk into a private count plus first occurrences.
histFirstChunk ::
    (Int -> Int) ->
    Int ->
    Int ->
    Int ->
    IO (VUM.IOVector Int, VUM.IOVector Int, Bool)
histFirstChunk codeAt card lo hi = do
    acc <- VUM.replicate card (0 :: Int)
    firstOcc <- VUM.replicate card (-1 :: Int)
    let go !i
            | i >= hi = pure True
            | otherwise = do
                let !c = codeAt i
                if c < 0 || c >= card
                    then pure False
                    else do
                        x <- VUM.unsafeRead acc c
                        VUM.unsafeWrite acc c (x + 1)
                        when (x == 0) (VUM.unsafeWrite firstOcc c i)
                        go (i + 1)
    ok <- go lo
    pure (acc, firstOcc, ok)

-- | Per-code totals over one code slice, plus each code's first occurrence.
sumFirstSlice ::
    [VUM.IOVector Int] ->
    [VUM.IOVector Int] ->
    VUM.IOVector Int ->
    VUM.IOVector Int ->
    Int ->
    Int ->
    IO ()
sumFirstSlice hists firsts totals firstAll lo hi = go lo
  where
    go !c
        | c >= hi = pure ()
        | otherwise = do
            let sumP [] [] !acc !firstRow = pure (acc, firstRow)
                sumP (h : hs) (f : fs) !acc !firstRow = do
                    x <- VUM.unsafeRead h c
                    if firstRow < 0 && x > 0
                        then do
                            fo <- VUM.unsafeRead f c
                            sumP hs fs (acc + x) fo
                        else sumP hs fs (acc + x) firstRow
                sumP _ _ _ _ = error "sumFirstSlice: mismatched partials"
            (s, fo) <- sumP hists firsts 0 (-1)
            VUM.unsafeWrite totals c s
            VUM.unsafeWrite firstAll c fo
            go (c + 1)

-- | Exclusive prefix scan of per-group counts into the offsets array.
scanGroupOffsets :: VU.Vector Int -> VU.Vector Int -> Int -> IO (VU.Vector Int)
scanGroupOffsets counts codeToGroup nGroups = do
    let !card = VU.length counts
    grpCount <- VUM.new nGroups
    let gather !c
            | c >= card = pure ()
            | otherwise = do
                let !cnt = VU.unsafeIndex counts c
                if cnt == 0
                    then gather (c + 1)
                    else do
                        VUM.unsafeWrite grpCount (VU.unsafeIndex codeToGroup c) cnt
                        gather (c + 1)
    gather 0
    offsM <- VUM.new (nGroups + 1)
    let scan !g !acc
            | g >= nGroups = VUM.unsafeWrite offsM nGroups acc
            | otherwise = do
                VUM.unsafeWrite offsM g acc
                c <- VUM.unsafeRead grpCount g
                scan (g + 1) (acc + c)
    scan 0 0
    VU.unsafeFreeze offsM

-- | Scatter representative rows for every occupied code, in parallel.
scatterRepsSlice ::
    VU.Vector Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    VUM.IOVector Int ->
    Int ->
    Int ->
    IO ()
scatterRepsSlice counts codeToGroup firstAll repsM lo hi = go lo
  where
    go !c
        | c >= hi = pure ()
        | VU.unsafeIndex counts c == 0 = go (c + 1)
        | otherwise = do
            VUM.unsafeWrite
                repsM
                (VU.unsafeIndex codeToGroup c)
                (VU.unsafeIndex firstAll c)
            go (c + 1)

-- | Deferred @rowToGroup@: one parallel per-row pass through @codeToGroup@.
rtgFromCodes :: (Int -> Int) -> VU.Vector Int -> Int -> VU.Vector Int
rtgFromCodes codeAt codeToGroup n = unsafePerformIO $ do
    rtgM <- VUM.new n
    let identity = isIdentityMap codeToGroup
    _ <-
        forkJoin
            [ ( if identity
                    then rtgChunkIdentity codeAt rtgM lo hi
                    else rtgChunk codeAt codeToGroup rtgM lo hi
              )
            | (lo, hi) <- directRowChunks n
            ]
    VU.unsafeFreeze rtgM
{-# NOINLINE rtgFromCodes #-}

-- | @rtg[i] = codeToGroup (codeAt i)@ over one row chunk (disjoint writes).
rtgChunk ::
    (Int -> Int) -> VU.Vector Int -> VUM.IOVector Int -> Int -> Int -> IO ()
rtgChunk codeAt codeToGroup rtgM lo hi = go lo
  where
    go !i
        | i >= hi = pure ()
        | otherwise = do
            VUM.unsafeWrite rtgM i (VU.unsafeIndex codeToGroup (codeAt i))
            go (i + 1)

-- | 'rtgChunk' without the remap lookup (codeToGroup is the identity).
rtgChunkIdentity ::
    (Int -> Int) -> VUM.IOVector Int -> Int -> Int -> IO ()
rtgChunkIdentity codeAt rtgM lo hi = go lo
  where
    go !i
        | i >= hi = pure ()
        | otherwise = do
            VUM.unsafeWrite rtgM i (codeAt i)
            go (i + 1)

-- | Deferred stable placement building @valueIndices@ from per-row codes.
visFromCodes ::
    (Int -> Int) ->
    VU.Vector Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    [VU.Vector Int] ->
    Int ->
    Int ->
    VU.Vector Int
visFromCodes codeAt counts codeToGroup offs hists n card = unsafePerformIO $ do
    let chunks = directRowChunks n
    -- Private mutable copies of the histograms, rewritten into write cursors.
    cursors <- mapM VU.thaw hists
    _ <-
        forkJoin
            [ seedCursorSlice counts codeToGroup offs cursors lo hi
            | (lo, hi) <- directCodeSlices card
            ]
    vis <- VUM.new n
    _ <-
        forkJoin
            [ placeVisChunk codeAt cursor vis lo hi
            | ((lo, hi), cursor) <- zip chunks cursors
            ]
    VU.unsafeFreeze vis
{-# NOINLINE visFromCodes #-}

-- | Rewrite each chunk's histogram copy in place into its write cursor.
seedCursorSlice ::
    VU.Vector Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    [VUM.IOVector Int] ->
    Int ->
    Int ->
    IO ()
seedCursorSlice counts codeToGroup offs cursors lo hi = go lo
  where
    go !c
        | c >= hi = pure ()
        | VU.unsafeIndex counts c == 0 = go (c + 1)
        | otherwise = do
            let !g = VU.unsafeIndex codeToGroup c
                loop [] !_ = pure ()
                loop (cur : rest) !acc = do
                    t <- VUM.unsafeRead cur c
                    VUM.unsafeWrite cur c acc
                    loop rest (acc + t)
            loop cursors (VU.unsafeIndex offs g)
            go (c + 1)

-- | Stable placement over one row chunk via its advancing cursors.
placeVisChunk ::
    (Int -> Int) -> VUM.IOVector Int -> VUM.IOVector Int -> Int -> Int -> IO ()
placeVisChunk codeAt cursor vis lo hi = go lo
  where
    go !i
        | i >= hi = pure ()
        | otherwise = do
            let !c = codeAt i
            pos <- VUM.unsafeRead cursor c
            VUM.unsafeWrite vis pos i
            VUM.unsafeWrite cursor c (pos + 1)
            go (i + 1)

-- | Fold a value-mix over an unboxed column into the running hash vector.
hashUnboxed ::
    (VU.Unbox a) =>
    VUM.MVector s Int ->
    Maybe Bitmap ->
    (Int -> a -> Int) ->
    VU.Vector a ->
    ST s ()
hashUnboxed mh ubm mix v = case ubm of
    Nothing ->
        VU.imapM_
            ( \i x -> do
                !h <- VUM.unsafeRead mh i
                VUM.unsafeWrite mh i (mix h x)
            )
            v
    Just bm ->
        VU.imapM_
            ( \i x -> do
                !h <- VUM.unsafeRead mh i
                VUM.unsafeWrite
                    mh
                    i
                    (if bitmapTestBit bm i then mix h x else mixInt h nullSalt)
            )
            v
{-# INLINE hashUnboxed #-}

-- | Hash a packed-text column over its raw UTF-8 byte slices.
hashPacked ::
    VUM.MVector s Int -> Maybe Bitmap -> PackedTextData -> ST s ()
hashPacked mh bm p = case ptSel p of
    Just sel | ptCanonicalSel p -> goCodes sel 0
    _ -> go 0
  where
    !n = packedLength p
    go !i
        | i >= n = pure ()
        | otherwise = do
            !h <- VUM.unsafeRead mh i
            let h' = case bm of
                    Just bm' | not (bitmapTestBit bm' i) -> mixInt h nullSalt
                    _ -> let (arr, o, l) = packedSlice p i in mixBytes h arr o l
            VUM.unsafeWrite mh i h'
            go (i + 1)
    goCodes !sel !i
        | i >= n = pure ()
        | otherwise = do
            !h <- VUM.unsafeRead mh i
            let h' = case bm of
                    Just bm' | not (bitmapTestBit bm' i) -> mixInt h nullSalt
                    _ -> mixInt h (selAt sel i)
            VUM.unsafeWrite mh i h'
            goCodes sel (i + 1)
{-# INLINE hashPacked #-}

-- Inline accessors to avoid depending on Operations.Core

columnNames :: DataFrame -> [T.Text]
columnNames = M.keys . columnIndices

nRows :: DataFrame -> Int
nRows = fst . dataframeDimensions

-- | Build the rowToGroup lookup vector from valueIndices and offsets.
buildRowToGroup :: Int -> VU.Vector Int -> VU.Vector Int -> VU.Vector Int
buildRowToGroup n vis os = runST $ do
    rtg <- VUM.new n
    let nGroups = VU.length os - 1
    forM_ [0 .. nGroups - 1] $ \k ->
        let s = VU.unsafeIndex os k
            e = VU.unsafeIndex os (k + 1)
         in forM_ [s .. e - 1] $ \i ->
                VUM.unsafeWrite rtg (VU.unsafeIndex vis i) k
    VU.unsafeFreeze rtg
{-# NOINLINE buildRowToGroup #-}

changingPoints :: VU.Vector (Int, Int) -> VU.Vector Int
changingPoints vs =
    VU.reverse
        (VU.fromList (VU.length vs : fst (VU.ifoldl' findChangePoints initialState vs)))
  where
    initialState = ([0], snd (VU.head vs))
    findChangePoints (!offs, !currentVal) index (_, !newVal)
        | currentVal == newVal = (offs, currentVal)
        | otherwise = (index : offs, newVal)
