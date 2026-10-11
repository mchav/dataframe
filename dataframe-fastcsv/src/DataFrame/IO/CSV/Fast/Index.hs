{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CApiFFI #-}
{-# LANGUAGE ForeignFunctionInterface #-}

-- | Delimiter-index construction for the fast CSV reader.
module DataFrame.IO.CSV.Fast.Index (
    CsvParseError (..),
    getDelimiterIndices,
    getDelimiterIndicesPolicy,
    scalarScan,
    classifyRowEnds,
    fieldsInRow,
    isBlankRow,
    collectDataRows,
    checkNoRaggedRows,
    byteStringView,
    lf,
    cr,
    comma,
    tab,
    quote,
) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI
import qualified Data.Vector.Storable as VS
import qualified Data.Vector.Storable.Mutable as VSM

import Control.Exception (Exception, throwIO)
import Control.Monad (when)
import Data.Word (Word8)
import Foreign (Ptr, castPtr)
import Foreign.C.Types (CInt (..), CSize (..), CUChar (..))
import Foreign.Marshal.Alloc (alloca)
import Foreign.Storable (peek, poke)

import DataFrame.IO.CSV (UnclosedQuotePolicy (..))

-- | Exceptions raised by the fast CSV parser.
data CsvParseError
    = -- | The input ends with a quoted field that was never closed.
      CsvUnclosedQuote
    | -- | A row has a different number of fields from the header.
      CsvRaggedRow !Int !Int !Int
    deriving (Eq, Show)

instance Exception CsvParseError

lf, cr, comma, tab, quote :: Word8
lf = 0x0A
cr = 0x0D
comma = 0x2C
tab = 0x09
quote = 0x22

-- Status codes of 'get_delimiter_indices', mirroring @GDI_*@ in the C header.
gdiOk, gdiUnclosedQuote :: CInt
gdiOk = 0
gdiUnclosedQuote = 1

-- | Return value meaning SIMD is unavailable and Haskell must scan.
simdUnavailable :: CSize
simdUnavailable = maxBound

foreign import capi "process_csv.h get_delimiter_indices"
    get_delimiter_indices ::
        Ptr CUChar -> -- input
        CSize -> -- input size
        CUChar -> -- separator character
        Ptr CSize -> -- result array
        Ptr CInt -> -- status out-parameter
        IO CSize -- occupancy of result array

-- | Zero-copy 'BS.ByteString' view of the first @n@ bytes of a vector.
byteStringView :: Int -> VS.Vector Word8 -> BS.ByteString
byteStringView n v = BSI.fromForeignPtr (fst (VS.unsafeToForeignPtr0 v)) 0 n
{-# INLINE byteStringView #-}

-- | Locate delimiter byte positions in the first @originalLen@ bytes.
{-# INLINE getDelimiterIndices #-}
getDelimiterIndices ::
    Word8 ->
    Int ->
    VS.Vector Word8 ->
    IO (VS.Vector CSize)
getDelimiterIndices = getDelimiterIndicesPolicy RaiseOnUnclosedQuote

getDelimiterIndicesPolicy ::
    UnclosedQuotePolicy ->
    Word8 ->
    Int ->
    VS.Vector Word8 ->
    IO (VS.Vector CSize)
getDelimiterIndicesPolicy policy separator contentLen csvFile = do
    -- Every index is an unquoted separator or newline, plus one EOF slot.
    let contentBS = byteStringView contentLen csvFile
        capacity = BS.count separator contentBS + BS.count lf contentBS + 1
    resultMV <- VSM.unsafeNew capacity
    -- The C scanner reads whole 64-byte chunks, so stop at the last one.
    let simdLen = (contentLen `div` 64) * 64
    (numFound, status) <- VS.unsafeWith csvFile $ \buffer ->
        alloca $ \statusPtr -> do
            poke statusPtr gdiOk
            n <- VSM.unsafeWith resultMV $ \indicesPtr ->
                get_delimiter_indices
                    (castPtr buffer)
                    (fromIntegral (simdLen + 64))
                    (fromIntegral separator)
                    (castPtr indicesPtr)
                    statusPtr
            s <- peek statusPtr
            pure (n, s)
    -- The status gives the quote parity the scalar tail starts from.
    (found, inQuote) <-
        if numFound == simdUnavailable
            then scalarScan separator csvFile resultMV 0 0 contentLen False
            else
                scalarScan
                    separator
                    csvFile
                    resultMV
                    (fromIntegral numFound)
                    simdLen
                    contentLen
                    (status == gdiUnclosedQuote)
    when (inQuote && policy == RaiseOnUnclosedQuote) (throwIO CsvUnclosedQuote)
    -- Synthetic EOF delimiter when the file does not end in a newline.
    finalLen <-
        if contentLen > 0 && VS.unsafeIndex csvFile (contentLen - 1) /= lf
            then do
                VSM.unsafeWrite resultMV found (fromIntegral contentLen)
                pure (found + 1)
            else pure found
    VS.unsafeFreeze (VSM.slice 0 finalLen resultMV)

-- | RFC 4180 quote-parity state machine over a byte range.
scalarScan ::
    Word8 ->
    VS.Vector Word8 ->
    VSM.IOVector CSize ->
    Int ->
    Int ->
    Int ->
    Bool ->
    IO (Int, Bool)
scalarScan separator file out writeIdx from to = go writeIdx from
  where
    go !idx !i !inQ
        | i >= to = pure (idx, inQ)
        | otherwise =
            let b = VS.unsafeIndex file i
             in if inQ
                    then go idx (i + 1) (b /= quote)
                    else
                        if b == quote
                            then go idx (i + 1) True
                            else
                                if b == lf || b == separator
                                    then do
                                        VSM.unsafeWrite out idx (fromIntegral i)
                                        go (idx + 1) (i + 1) False
                                    else go idx (i + 1) False

-- | Throw 'CsvRaggedRow' on the first row whose field count differs.
checkNoRaggedRows ::
    VS.Vector Int ->
    VS.Vector Int ->
    Int ->
    IO ()
checkNoRaggedRows rowEnds dataRows numCol =
    VS.mapM_
        ( \r ->
            let actual = fieldsInRow rowEnds r
             in when (actual /= numCol) $
                    throwIO (CsvRaggedRow r numCol actual)
        )
        dataRows

-- | Find the indices of every row terminator in the delimiter vector.
{-# INLINE classifyRowEnds #-}
classifyRowEnds :: VS.Vector Word8 -> Int -> VS.Vector CSize -> VS.Vector Int
classifyRowEnds file contentLen =
    VS.findIndices isRowBreak
  where
    isRowBreak pos =
        let p = fromIntegral pos :: Int
         in p >= contentLen || VS.unsafeIndex file p == lf

-- | Number of fields that row @r@ contains.
{-# INLINE fieldsInRow #-}
fieldsInRow :: VS.Vector Int -> Int -> Int
fieldsInRow rowEnds r =
    let endIdx = VS.unsafeIndex rowEnds r
        startIdx = if r == 0 then 0 else VS.unsafeIndex rowEnds (r - 1) + 1
     in endIdx - startIdx + 1

-- | Does row @r@ contain exactly one empty field (a blank line)?
{-# INLINE isBlankRow #-}
isBlankRow ::
    VS.Vector Word8 ->
    VS.Vector CSize ->
    VS.Vector Int ->
    Int ->
    Int ->
    Bool
isBlankRow file delimiters rowEnds contentLen r =
    let endIdx = VS.unsafeIndex rowEnds r
        startIdx = if r == 0 then 0 else VS.unsafeIndex rowEnds (r - 1) + 1
        numFields = endIdx - startIdx + 1
     in numFields == 1
            && let fieldEndRaw =
                    fromIntegral (VS.unsafeIndex delimiters startIdx) :: Int
                   fieldEnd = min fieldEndRaw contentLen
                   fieldStart =
                    if startIdx == 0
                        then 0
                        else
                            fromIntegral
                                (VS.unsafeIndex delimiters (startIdx - 1))
                                + 1
                   fieldLen = fieldEnd - fieldStart
                in fieldLen == 0
                    || ( fieldLen == 1
                            && VS.unsafeIndex file fieldStart == cr
                       )

-- | Select the row indices that contain actual data.
{-# INLINE collectDataRows #-}
collectDataRows ::
    VS.Vector Word8 ->
    VS.Vector CSize ->
    VS.Vector Int ->
    Int ->
    Int ->
    Int ->
    VS.Vector Int
collectDataRows file delimiters rowEnds contentLen skip total =
    VS.filter
        (not . isBlankRow file delimiters rowEnds contentLen)
        (VS.generate (total - skip) (+ skip))
