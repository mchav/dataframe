module DataFrame.IO.Parquet.Encoding where

import Data.Bits
import Data.List (foldl')
import Data.Word
import DataFrame.IO.Parquet.Binary

ceilLog2 :: Int -> Int
ceilLog2 x
    | x <= 1 = 0
    | otherwise = 1 + ceilLog2 ((x + 1) `div` 2)

bitWidthForMaxLevel :: Int -> Int
bitWidthForMaxLevel maxLevel = ceilLog2 (maxLevel + 1)

bytesForBW :: Int -> Int
bytesForBW bw = (bw + 7) `div` 8

unpackBitPacked :: Int -> Int -> [Word8] -> ([Word32], [Word8])
unpackBitPacked bw count bs
    | count <= 0 = ([], bs)
    | null bs = ([], bs)
    | otherwise =
        let totalBits = bw * count
            totalBytes = (totalBits + 7) `div` 8
            chunk = take totalBytes bs
            rest = drop totalBytes bs
            bits = concatMap (\b -> map (\i -> (b `shiftR` i) .&. 1) [0 .. 7]) chunk
            toN xs = foldl' (\a (i, b) -> a .|. (b `shiftL` i)) 0 (zip [0 ..] xs)

            extractValues _ [] = []
            extractValues n bitsLeft
                | n <= 0 = []
                | length bitsLeft < bw = []
                | otherwise =
                    let (this, bitsLeft') = splitAt bw bitsLeft
                     in toN this : extractValues (n - 1) bitsLeft'

            vals = extractValues count bits
         in (map fromIntegral vals, rest)

decodeRLEBitPackedHybrid :: Int -> Int -> [Word8] -> ([Word32], [Word8])
decodeRLEBitPackedHybrid bw need bs
    | bw == 0 = (replicate need 0, bs)
    | otherwise = go need bs []
  where
    mask :: Word32
    mask = if bw == 32 then maxBound else (1 `shiftL` bw) - 1
    go 0 rest acc = (reverse acc, rest)
    go n rest acc
        | null rest = (reverse acc, rest)
        | otherwise =
            let (hdr64, afterHdr) = readUVarInt rest
                isPacked = (hdr64 .&. 1) == 1
             in if isPacked
                    then
                        let groups = fromIntegral (hdr64 `shiftR` 1) :: Int
                            totalVals = groups * 8
                            (valsAll, afterRun) = unpackBitPacked bw totalVals afterHdr
                            takeN = min n totalVals
                            actualTaken = take takeN valsAll
                         in go (n - takeN) afterRun (reverse actualTaken ++ acc)
                    else
                        let runLen = fromIntegral (hdr64 `shiftR` 1) :: Int
                            nbytes = bytesForBW bw
                            word32 = littleEndianWord32 (take 4 afterHdr)
                            afterV = drop nbytes afterHdr
                            val = word32 .&. mask
                            takeN = min n runLen
                         in go (n - takeN) afterV (replicate takeN val ++ acc)

decodeDictIndicesV1 :: Int -> Int -> [Word8] -> ([Int], [Word8])
decodeDictIndicesV1 need dictCard bs =
    case bs of
        [] -> error "empty dictionary index stream"
        (w0 : rest0) ->
            let widthFromDict = ceilLog2 dictCard
                looksLikeWidth = w0 <= 32 && (w0 /= 0 || dictCard <= 1)
                tryWithWidthByte =
                    let bw = fromIntegral w0
                        (u32s, rest1) = decodeRLEBitPackedHybrid bw need rest0
                     in (map fromIntegral u32s, rest1)
                tryWithoutWidthByte =
                    let bw = widthFromDict
                        (u32s, rest1) = decodeRLEBitPackedHybrid bw need bs
                     in (map fromIntegral u32s, rest1)
                (idxs, rest') =
                    if looksLikeWidth
                        then
                            let (xs, r) = tryWithWidthByte
                             in if length xs == need then (xs, r) else tryWithoutWidthByte
                        else
                            tryWithoutWidthByte
             in (idxs, rest')
