{-# LANGUAGE BangPatterns #-}

module DataFrame.IO.Parquet.Dictionary (
    DictVals (..),
    readDictVals,
    dictText,
    dictInt32,
    dictInt64,
    dictFloat,
    dictDouble,
    word32At,
    word64At,
    decodeRLEBitPackedHybrid,
) where

import Data.Bits
import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BSU
import Data.Int (Int32, Int64)
import qualified Data.Text as T
import Data.Text.Encoding
import Data.Time (UTCTime)
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import Data.Word
import DataFrame.IO.Parquet.Binary (readUVarInt)
import DataFrame.IO.Parquet.Thrift (ThriftType (..))
import DataFrame.IO.Parquet.Time (int96ToUTCTime)
import DataFrame.Internal.Binary (littleEndianWord32)
import GHC.Float

-- | A decoded dictionary page with unboxed numeric entries.
data DictVals
    = DBool !(VU.Vector Bool)
    | DInt32 !(VU.Vector Int32)
    | DInt64 !(VU.Vector Int64)
    | DInt96 !(V.Vector UTCTime)
    | DFloat !(VU.Vector Float)
    | DDouble !(VU.Vector Double)
    | DText !(VU.Vector Int) !(VU.Vector Int) !BS.ByteString
    deriving (Show, Eq)

-- | Decode the values from a dictionary page.
readDictVals :: ThriftType -> BS.ByteString -> Int32 -> Maybe Int32 -> DictVals
readDictVals (BOOLEAN _) bs count _ =
    let n = min (fromIntegral count) (8 * BS.length bs)
     in DBool $ VU.generate n $ \i ->
            (BSU.unsafeIndex bs (i `shiftR` 3) `shiftR` (i .&. 7)) .&. 1 == 1
readDictVals (INT32 _) bs _ _ =
    DInt32 $ VU.generate (BS.length bs `div` 4) $ \i -> fromIntegral (word32At bs (4 * i))
readDictVals (INT64 _) bs _ _ =
    DInt64 $ VU.generate (BS.length bs `div` 8) $ \i -> fromIntegral (word64At bs (8 * i))
readDictVals (INT96 _) bs _ _ =
    DInt96 $ V.generate (BS.length bs `div` 12) $ \i ->
        int96ToUTCTime (BSU.unsafeTake 12 (BSU.unsafeDrop (12 * i) bs))
readDictVals (FLOAT _) bs _ _ =
    DFloat $ VU.generate (BS.length bs `div` 4) $ \i -> castWord32ToFloat (word32At bs (4 * i))
readDictVals (DOUBLE _) bs _ _ =
    DDouble $ VU.generate (BS.length bs `div` 8) $ \i -> castWord64ToDouble (word64At bs (8 * i))
readDictVals (BYTE_ARRAY _) bs _ _ = textDict bs
readDictVals (FIXED_LEN_BYTE_ARRAY _) bs _ (Just len) =
    let w = fromIntegral len
        n = if w <= 0 then 0 else BS.length bs `div` w
     in DText (VU.generate n (* w)) (VU.replicate n w) bs
readDictVals t _ _ _ = error $ "Unsupported dictionary type: " ++ show t

-- | Length-prefixed byte-array entries, in two passes.
textDict :: BS.ByteString -> DictVals
textDict bs = DText starts lens bs
  where
    !len = BS.length bs
    count !off !n
        | off + 4 > len = n
        | otherwise = count (off + 4 + fromIntegral (word32At bs off)) (n + 1)
    !n = count 0 0
    positions = VU.unfoldrExactN n step 0
    step off = let l = fromIntegral (word32At bs off) in ((off + 4, l), off + 4 + l)
    (starts, lens) = VU.unzip positions

-- | Dictionary entry @i@ as 'T.Text' (lenient decode, for the boxed paths).
dictText :: DictVals -> Int -> T.Text
dictText (DText starts lens bs) i =
    decodeUtf8Lenient
        ( BSU.unsafeTake
            (VU.unsafeIndex lens i)
            (BSU.unsafeDrop (VU.unsafeIndex starts i) bs)
        )
dictText d _ = error ("dictText: wrong dict type, got " ++ show d)

dictInt32 :: DictVals -> Maybe (VU.Vector Int32)
dictInt32 (DInt32 ds) = Just ds
dictInt32 _ = Nothing

dictInt64 :: DictVals -> Maybe (VU.Vector Int64)
dictInt64 (DInt64 ds) = Just ds
dictInt64 _ = Nothing

dictFloat :: DictVals -> Maybe (VU.Vector Float)
dictFloat (DFloat ds) = Just ds
dictFloat _ = Nothing

dictDouble :: DictVals -> Maybe (VU.Vector Double)
dictDouble (DDouble ds) = Just ds
dictDouble _ = Nothing

word32At :: BS.ByteString -> Int -> Word32
word32At bs o =
    fromIntegral (BSU.unsafeIndex bs o)
        .|. (fromIntegral (BSU.unsafeIndex bs (o + 1)) `unsafeShiftL` 8)
        .|. (fromIntegral (BSU.unsafeIndex bs (o + 2)) `unsafeShiftL` 16)
        .|. (fromIntegral (BSU.unsafeIndex bs (o + 3)) `unsafeShiftL` 24)
{-# INLINE word32At #-}

word64At :: BS.ByteString -> Int -> Word64
word64At bs o =
    fromIntegral (word32At bs o)
        .|. (fromIntegral (word32At bs (o + 4)) `unsafeShiftL` 32)
{-# INLINE word64At #-}

unpackBitPacked :: Int -> Int -> BS.ByteString -> ([Word32], BS.ByteString)
unpackBitPacked bw count bs
    | count <= 0 = ([], bs)
    | BS.null bs = ([], bs)
    | otherwise =
        let totalBytes = (bw * count + 7) `div` 8
            chunk = BS.take totalBytes bs
            rest = BS.drop totalBytes bs
         in (extractBits bw count chunk, rest)

-- | LSB-first bit accumulator that reads each byte once.
extractBits :: Int -> Int -> BS.ByteString -> [Word32]
extractBits bw count bs = go 0 (0 :: Word64) 0 count
  where
    !mask = if bw == 32 then maxBound else (1 `shiftL` bw) - 1 :: Word64
    !len = BS.length bs
    go !byteIdx !acc !accBits !remaining
        | remaining <= 0 = []
        | accBits >= bw =
            fromIntegral (acc .&. mask)
                : go byteIdx (acc `shiftR` bw) (accBits - bw) (remaining - 1)
        | byteIdx >= len = []
        | otherwise =
            let b = fromIntegral (BSU.unsafeIndex bs byteIdx) :: Word64
             in go (byteIdx + 1) (acc .|. (b `shiftL` accBits)) (accBits + 8) remaining

decodeRLEBitPackedHybrid :: Int -> BS.ByteString -> ([Word32], BS.ByteString)
decodeRLEBitPackedHybrid bitWidth bs
    | bitWidth == 0 = ([0], bs)
    | BS.null bs = ([], bs)
    | otherwise =
        -- Evaluated inside the guard: a where clause would force it too early.
        let (hdr64, afterHdr) = readUVarInt bs
            isPacked = (hdr64 .&. 1) == 1
         in if isPacked
                then
                    let groups = fromIntegral (hdr64 `shiftR` 1) :: Int
                        totalVals = groups * 8
                     in unpackBitPacked bitWidth totalVals afterHdr
                else
                    let mask = if bitWidth == 32 then maxBound else (1 `shiftL` bitWidth) - 1
                        runLen = fromIntegral (hdr64 `shiftR` 1) :: Int
                        nBytes = (bitWidth + 7) `div` 8 :: Int
                        word32 = littleEndianWord32 (BS.take 4 afterHdr)
                        value = word32 .&. mask
                     in (replicate runLen value, BS.drop nBytes afterHdr)
