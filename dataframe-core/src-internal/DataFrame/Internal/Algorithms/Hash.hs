{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE MagicHash #-}

-- | A cheap FxHash-style hash used to bucket rows without @hashable@.
module DataFrame.Internal.Algorithms.Hash (
    fnvOffset,
    nullSalt,
    mixInt,
    mixDouble,
    mixBool,
    mixChar,
    mixText,
    mixBytes,
    mixShow,
) where

import Data.Bits (rotateL, unsafeShiftL, unsafeShiftR, xor)
import Data.Char (ord)
import qualified Data.Text as T
import qualified Data.Text.Array as A
#if MIN_VERSION_text(2,1,0)
import Data.Array.Byte (ByteArray (ByteArray))
#else
import Data.Text.Array (Array (ByteArray))
#endif
import Data.Text.Internal (Text (Text))
import GHC.Exts (Int (I#), indexWord8Array#, indexWord8ArrayAsWord64#)
import GHC.Word (Word64 (W64#), Word8 (W8#))

-- | FNV-1a 64-bit offset basis, the initial accumulator.
fnvOffset :: Int
fnvOffset = fromIntegral (0xcbf29ce484222325 :: Word64)

-- | FNV-1a 64-bit prime.
fnvPrime :: Int
fnvPrime = 0x00000100000001b3

-- | Sentinel mixed in for a null slot so @Nothing@ differs from @Just 0@.
nullSalt :: Int
nullSalt = fromIntegral (0x9E3779B97F4A7C15 :: Word64)

-- | Mix an 'Int' into the accumulator with an FxHash-style step.
mixInt :: Int -> Int -> Int
mixInt acc x = (rotateL acc 13 `xor` x) * fnvPrime
{-# INLINE mixInt #-}

-- | Mix a 'Double' into the accumulator.
mixDouble :: Int -> Double -> Int
mixDouble acc d = mixInt acc (floor (d * 1000))
{-# INLINE mixDouble #-}

mixBool :: Int -> Bool -> Int
mixBool acc b = mixInt acc (if b then 1 else 0)
{-# INLINE mixBool #-}

mixChar :: Int -> Char -> Int
mixChar acc = mixInt acc . ord
{-# INLINE mixChar #-}

-- | Mix a 'T.Text' into the accumulator over its UTF-8 bytes, eight at a time.
mixText :: Int -> T.Text -> Int
mixText !acc (Text arr off len) = mixBytes acc arr off len
{-# INLINE mixText #-}

-- | Mix a raw UTF-8 byte slice of a text array, eight bytes at a time.
mixBytes :: Int -> A.Array -> Int -> Int -> Int
mixBytes !acc arr off len = goBytes (goWords acc off) wordsEnd
  where
    !(ByteArray ba) = arr
    !nWords = len `unsafeShiftR` 3
    !wordsEnd = off + (nWords `unsafeShiftL` 3)
    !end = off + len
    goWords !h !i
        | i >= wordsEnd = h
        | otherwise =
            let !(I# i#) = i
                !w = fromIntegral (W64# (indexWord8ArrayAsWord64# ba i#)) :: Int
             in goWords (mixInt h w) (i + 8)
    goBytes !h !i
        | i >= end = h
        | otherwise =
            let !(I# i#) = i
                !b = fromIntegral (W8# (indexWord8Array# ba i#)) :: Int
             in goBytes (mixInt h b) (i + 1)
{-# INLINE mixBytes #-}

-- | Fallback hash for arbitrary 'Show'-able values; slower.
mixShow :: (Show a) => Int -> a -> Int
mixShow acc = mixText acc . T.pack . show
{-# INLINE mixShow #-}
