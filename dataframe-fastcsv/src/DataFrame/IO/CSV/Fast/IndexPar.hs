{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CApiFFI #-}

-- | Parallel delimiter-index construction over aligned SIMD ranges.
module DataFrame.IO.CSV.Fast.IndexPar (
    ParIndex (..),
    getDelimiterIndicesPar,
) where

import qualified Data.Bits as Bits
import qualified Data.ByteString as BS
import qualified Data.Vector.Storable as VS
import qualified Data.Vector.Storable.Mutable as VSM

import Data.Word (Word8)
import Foreign (Ptr, castPtr)
import Foreign.C.Types (CInt (..), CSize (..), CUChar (..))
import Foreign.Marshal.Alloc (alloca)
import Foreign.Marshal.Array (advancePtr)
import Foreign.Storable (peek)

import Control.Concurrent (getNumCapabilities)
import DataFrame.IO.CSV.Fast.Index (byteStringView, lf, quote, scalarScan)
import DataFrame.Internal.Control.Concurrent (pooledRun)

foreign import capi "process_csv.h count_delimiters_chunk"
    count_delimiters_chunk ::
        Ptr CUChar -> CSize -> CSize -> CUChar -> CInt -> Ptr CSize -> IO CSize

foreign import capi "process_csv.h emit_delimiter_indices_chunk"
    emit_delimiter_indices_chunk ::
        Ptr CUChar ->
        CSize ->
        CSize ->
        CUChar ->
        CInt ->
        CSize ->
        Ptr CSize ->
        Ptr CSize ->
        Ptr CSize ->
        IO CSize

-- | Mirrors @GDI_SIMD_UNAVAILABLE@ in @cbits/process_csv.h@.
simdUnavailable :: CSize
simdUnavailable = maxBound

-- | Result of the parallel scan: the @(delimiters, rowEnds)@ pair.
data ParIndex = ParIndex
    { piDelims :: !(VS.Vector CSize)
    , piRowEnds :: !(VS.Vector Int)
    , piEndsInQuote :: !Bool
    }

-- | Scan with @nWorkers@ ranges, or 'Nothing' when SIMD is missing.
getDelimiterIndicesPar ::
    Int -> Word8 -> Int -> VS.Vector Word8 -> IO (Maybe ParIndex)
getDelimiterIndicesPar nWorkers separator contentLen file =
    VS.unsafeWith file $ \filePtr -> do
        probe <-
            alloca $ \nlPtr ->
                count_delimiters_chunk (castPtr filePtr) 0 0 sep 0 nlPtr
        if probe == simdUnavailable
            then pure Nothing
            else Just <$> scanParallel nWorkers separator contentLen file filePtr
  where
    sep = fromIntegral separator

scanParallel ::
    Int -> Word8 -> Int -> VS.Vector Word8 -> Ptr Word8 -> IO ParIndex
scanParallel nWorkers separator contentLen file filePtr = do
    width <- getNumCapabilities
    let sep = fromIntegral separator :: CUChar
        simdLen = (contentLen `div` 64) * 64
        tailLen = contentLen - simdLen
        ranges = alignedRanges nWorkers simdLen
        contentBS = byteStringView contentLen file
        cBool b = if b then 1 else 0 :: CInt
    -- Quote parity at each range start (prefix-xor of quote counts).
    quoteCounts <-
        pooledRun
            width
            [ pure $! BS.count quote (BS.take len (BS.drop off contentBS))
            | (off, len) <- ranges
            ]
    let parities = scanl Bits.xor False (map odd quoteCounts)
        starts = zip ranges parities
    -- Pass 1 (parallel): exact per-range delimiter / row-end counts.
    counts <-
        pooledRun
            width
            [ alloca $ \nlPtr -> do
                d <-
                    count_delimiters_chunk
                        (castPtr filePtr)
                        (fromIntegral off)
                        (fromIntegral len)
                        sep
                        (cBool p)
                        nlPtr
                nl <- peek nlPtr
                pure (fromIntegral d :: Int, fromIntegral nl :: Int)
            | ((off, len), p) <- starts
            ]
    let dOffs = scanl (+) 0 (map fst counts)
        nlOffs = scanl (+) 0 (map snd counts)
        dTot = last dOffs
        nlTot = last nlOffs
    -- Right-sized flat outputs (+ scalar-tail upper bound + synthetic EOF).
    delimsMV <- VSM.unsafeNew (dTot + tailLen + 1)
    rowEndsMV <- VSM.unsafeNew (nlTot + tailLen + 1) :: IO (VSM.IOVector Int)
    -- Pass 2 (parallel): emit positions + row-end ordinals at exact offsets.
    VSM.unsafeWith delimsMV $ \dPtr ->
        VSM.unsafeWith rowEndsMV $ \rePtr -> do
            _ <-
                pooledRun
                    width
                    [ alloca $ \rcPtr ->
                        emit_delimiter_indices_chunk
                            (castPtr filePtr)
                            (fromIntegral off)
                            (fromIntegral len)
                            sep
                            (cBool p)
                            (fromIntegral dOff)
                            (dPtr `advancePtr` dOff)
                            (castPtr (rePtr `advancePtr` nlOff))
                            rcPtr
                    | (((off, len), p), dOff, nlOff) <- zip3 starts dOffs nlOffs
                    ]
            pure ()
    -- Scalar tail (final partial 64-byte chunk), parity carried in.
    (found, inQuote) <-
        scalarScan separator file delimsMV dTot simdLen contentLen (last parities)
    nlAfterTail <- appendTailRowEnds file rowEndsMV delimsMV nlTot dTot found
    -- Synthetic EOF delimiter when the file does not end in a newline.
    (found', nlFinal) <-
        if contentLen > 0 && VS.unsafeIndex file (contentLen - 1) /= lf
            then do
                VSM.unsafeWrite delimsMV found (fromIntegral contentLen)
                VSM.unsafeWrite rowEndsMV nlAfterTail found
                pure (found + 1, nlAfterTail + 1)
            else pure (found, nlAfterTail)
    delims <- VS.unsafeFreeze (VSM.slice 0 found' delimsMV)
    rowEnds <- VS.unsafeFreeze (VSM.slice 0 nlFinal rowEndsMV)
    pure (ParIndex delims rowEnds inQuote)

-- | Append the ordinal of every newline entry in the scalar tail.
appendTailRowEnds ::
    VS.Vector Word8 ->
    VSM.IOVector Int ->
    VSM.IOVector CSize ->
    Int ->
    Int ->
    Int ->
    IO Int
appendTailRowEnds file rowEndsMV delimsMV = go
  where
    go !w !i !to
        | i >= to = pure w
        | otherwise = do
            p <- VSM.unsafeRead delimsMV i
            if VS.unsafeIndex file (fromIntegral p) == lf
                then VSM.unsafeWrite rowEndsMV w i >> go (w + 1) (i + 1) to
                else go w (i + 1) to

-- | Split @[0, n)@ (a multiple of 64) into 64-byte-aligned ranges.
alignedRanges :: Int -> Int -> [(Int, Int)]
alignedRanges k n = filter ((> 0) . snd) (zip offs sizes)
  where
    blocks = n `div` 64
    (q, r) = blocks `divMod` max 1 k
    sizes = [64 * (q + if i < r then 1 else 0) | i <- [0 .. max 1 k - 1]]
    offs = scanl (+) 0 sizes
