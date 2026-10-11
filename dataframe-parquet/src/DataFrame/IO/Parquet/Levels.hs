{-# LANGUAGE BangPatterns #-}

module DataFrame.IO.Parquet.Levels (
    -- Level readers
    readLevelsV1,
    readLevelsV2,
    -- Stitch functions
    stitchList,
    stitchList2,
    stitchList3,
) where

import Control.Monad.ST (runST)
import qualified Data.ByteString as BS
import Data.Int (Int32)
import qualified Data.Vector as VB
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import Data.Word (Word32)
import DataFrame.IO.Parquet.Encoding (
    bitWidthForMaxLevel,
    decodeRLEBitPackedHybrid,
 )
import DataFrame.Internal.Binary (littleEndianWord32)

-- | Convert a 'Word32' level vector to 'Int', counting @maxDef@ entries.
convertAndCount :: Int -> VU.Vector Word32 -> (VU.Vector Int, Int)
convertAndCount maxDef raw = runST $ do
    let !n = VU.length raw
    mv <- VUM.unsafeNew n
    let !maxDefW = fromIntegral maxDef :: Word32
        go !i !nPresent
            | i >= n = pure nPresent
            | otherwise = do
                let !w = VU.unsafeIndex raw i
                    !d = fromIntegral w :: Int
                VUM.unsafeWrite mv i d
                if w == maxDefW
                    then go (i + 1) (nPresent + 1)
                    else go (i + 1) nPresent
    !nPresent <- go 0 0
    !out <- VU.unsafeFreeze mv
    pure (out, nPresent)

readLevelsV1 ::
    -- | Total number of values in the page
    Int ->
    -- | maxDefinitionLevel
    Int ->
    -- | maxRepetitionLevel
    Int ->
    BS.ByteString ->
    (VU.Vector Int, VU.Vector Int, Int, BS.ByteString)
readLevelsV1 n maxDef maxRep bs =
    let bwRep = bitWidthForMaxLevel maxRep
        bwDef = bitWidthForMaxLevel maxDef
        (repVec, _, afterRep) = decodeLevelBlock bwRep n bs
        (defVec, nPresent, afterDef) = decodeLevelBlock bwDef n afterRep
     in (defVec, repVec, nPresent, afterDef)
  where
    -- No levels to read: non-nullable, non-repeated paths never use them.
    decodeLevelBlock 0 n' buf = (VU.empty, n' * fromEnum (maxDef == 0), buf)
    decodeLevelBlock bw n' buf =
        let blockLen = fromIntegral (littleEndianWord32 (BS.take 4 buf)) :: Int
            blockData = BS.take blockLen (BS.drop 4 buf)
            after = BS.drop (4 + blockLen) buf
            (raw, _) = decodeRLEBitPackedHybrid bw n' blockData
            (out, np) = convertAndCount maxDef raw
         in (out, np, after)

readLevelsV2 ::
    -- | Total number of values
    Int ->
    -- | maxDefinitionLevel
    Int ->
    -- | maxRepetitionLevel
    Int ->
    -- | Repetition-level byte length (from page header)
    Int32 ->
    -- | Definition-level byte length (from page header)
    Int32 ->
    BS.ByteString ->
    (VU.Vector Int, VU.Vector Int, Int, BS.ByteString)
readLevelsV2 n maxDef maxRep repLen defLen bs =
    let (repBytes, afterRepBytes) = BS.splitAt (fromIntegral repLen) bs
        (defBytes, afterDefBytes) = BS.splitAt (fromIntegral defLen) afterRepBytes
        bwRep = bitWidthForMaxLevel maxRep
        bwDef = bitWidthForMaxLevel maxDef
        repVec
            | bwRep == 0 = VU.empty
            | otherwise =
                let (raw, _) = decodeRLEBitPackedHybrid bwRep n repBytes
                    (out, _) = convertAndCount maxDef raw
                 in out
        (defVec, nPresent)
            | bwDef == 0 = (VU.empty, n * fromEnum (maxDef == 0))
            | otherwise =
                let (raw, _) = decodeRLEBitPackedHybrid bwDef n defBytes
                 in convertAndCount maxDef raw
     in (defVec, repVec, nPresent, afterDefBytes)

-- | Stitch a singly-nested list column from levels and present values.
stitchList ::
    Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    VB.Vector a ->
    [Maybe [Maybe a]]
stitchList maxDef repVec defVec values =
    map toRow (splitAtRepBound 0 (pairWithValsV maxDef repVec defVec values))
  where
    toRow [] = Nothing
    toRow ((_, d, _) : _) | d == 0 = Nothing
    toRow grp = Just [v | (_, _, v) <- grp]

-- | Stitch a doubly-nested list column (@maxRep == 2@).
stitchList2 ::
    Int ->
    Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    VB.Vector a ->
    [Maybe [Maybe [Maybe a]]]
stitchList2 defT1 maxDef repVec defVec values =
    map toRow (splitAtRepBound 0 triplets)
  where
    triplets = pairWithValsV maxDef repVec defVec values
    toRow [] = Nothing
    toRow ((_, d, _) : _) | d == 0 = Nothing
    toRow row = Just (map toOuter (splitAtRepBound 1 row))
    toOuter [] = Nothing
    toOuter ((_, d, _) : _) | d < defT1 = Nothing
    toOuter outer = Just (map toLeaf (splitAtRepBound 2 outer))
    toLeaf [] = Nothing
    toLeaf ((_, _, v) : _) = v

-- | Stitch a triply-nested list column (@maxRep == 3@).
stitchList3 ::
    Int ->
    Int ->
    Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    VB.Vector a ->
    [Maybe [Maybe [Maybe [Maybe a]]]]
stitchList3 defT1 defT2 maxDef repVec defVec values =
    map toRow (splitAtRepBound 0 triplets)
  where
    triplets = pairWithValsV maxDef repVec defVec values
    toRow [] = Nothing
    toRow ((_, d, _) : _) | d == 0 = Nothing
    toRow row = Just (map toOuter (splitAtRepBound 1 row))
    toOuter [] = Nothing
    toOuter ((_, d, _) : _) | d < defT1 = Nothing
    toOuter outer = Just (map toMiddle (splitAtRepBound 2 outer))
    toMiddle [] = Nothing
    toMiddle ((_, d, _) : _) | d < defT2 = Nothing
    toMiddle middle = Just (map toLeaf (splitAtRepBound 3 middle))
    toLeaf [] = Nothing
    toLeaf ((_, _, v) : _) = v

-- | Zip rep and def levels with present values into triplets.
pairWithValsV ::
    Int ->
    VU.Vector Int ->
    VU.Vector Int ->
    VB.Vector a ->
    [(Int, Int, Maybe a)]
pairWithValsV maxDef repVec defVec values = go 0 0
  where
    n = VU.length defVec
    go i j
        | i >= n = []
        | otherwise =
            let r = VU.unsafeIndex repVec i
                d = VU.unsafeIndex defVec i
             in if d == maxDef
                    then (r, d, Just (VB.unsafeIndex values j)) : go (i + 1) (j + 1)
                    else (r, d, Nothing) : go (i + 1) j

-- | Group a flat triplet list into rows.
splitAtRepBound :: Int -> [(Int, Int, Maybe a)] -> [[(Int, Int, Maybe a)]]
splitAtRepBound _ [] = []
splitAtRepBound bound (t : ts) =
    let (rest, remaining) = span (\(r, _, _) -> r > bound) ts
     in (t : rest) : splitAtRepBound bound remaining
