{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CApiFFI #-}
{-# LANGUAGE ForeignFunctionInterface #-}

module DataFrame.IO.Unstable.CSV (
    fastReadCsvUnstable,
    readCsvUnstable,
) where

import qualified Data.Vector as Vector
import qualified Data.Vector.Storable as VS
import Data.Vector.Storable.Mutable (
    grow,
    unsafeFromForeignPtr,
 )
import qualified Data.Vector.Storable.Mutable as VSM
import System.IO.MMap (
    Mode (WriteCopy),
    mmapFileForeignPtr,
 )

import Foreign (
    Ptr,
    castForeignPtr,
    castPtr,
    mallocArray,
    newForeignPtr_,
 )
import Foreign.C.Types

import qualified Data.ByteString as BS
import Data.ByteString.Internal (ByteString (PS))
import qualified Data.Map as M
import Data.Text (Text)
import qualified Data.Text as Text
import qualified Data.Text.Encoding as TextEncoding
import Data.Word (Word8)

import Control.Parallel.Strategies (parList, rpar, using)
import Data.Array.IArray (array, (!))
import Data.Array.Unboxed (UArray)
import Data.Ix (range)

import DataFrame.IO.CSV (
    HeaderSpec (..),
    ReadOptions (..),
    defaultReadOptions,
    shouldInferFromSample,
    stripQuotes,
    typeInferenceSampleSize,
 )
import DataFrame.Internal.DataFrame (DataFrame (..))
import DataFrame.Operations.Typing (parseFromExamples)

fastReadCsvUnstable :: FilePath -> IO DataFrame
fastReadCsvUnstable =
    readCsvUnstable'
        defaultReadOptions
        getDelimiterIndices

readCsvUnstable :: FilePath -> IO DataFrame
readCsvUnstable =
    readCsvUnstable'
        defaultReadOptions
        ( \originalLen v -> do
            indices <- mallocArray originalLen
            getDelimiterIndices_ originalLen v indices
        )

readCsvUnstable' ::
    ReadOptions ->
    (Int -> VS.Vector Word8 -> IO (VS.Vector CSize)) ->
    FilePath ->
    IO DataFrame
readCsvUnstable' opts delimiterIndices filePath = do
    -- We use write copy mode so that we can append
    -- padding to the end of the memory space
    (bufferPtr, offset, len) <-
        mmapFileForeignPtr
            filePath
            WriteCopy
            Nothing
    let mutableFile = unsafeFromForeignPtr bufferPtr offset len
    paddedMutableFile <- grow mutableFile 64
    paddedCSVFile <- VS.unsafeFreeze paddedMutableFile
    indices <- delimiterIndices len paddedCSVFile
    let numCol = countColumnsInFirstRow paddedCSVFile indices
        totalRows = VS.length indices `div` numCol
        extractField' = extractField paddedCSVFile indices
        (columnNames, dataStartRow) = case headerSpec opts of
            NoHeader ->
                ( Vector.fromList $
                    map (Text.pack . show) [0 .. numCol - 1]
                , 0
                )
            UseFirstRow ->
                ( Vector.fromList $
                    map (stripQuotes . extractField') [0 .. numCol - 1]
                , 1
                )
            ProvideNames ns ->
                (Vector.fromList ns, 0)
        numRow = totalRows - dataStartRow
        parseTypes col =
            let n =
                    if shouldInferFromSample (typeSpec opts)
                        then typeInferenceSampleSize (typeSpec opts)
                        else 0
             in parseFromExamples
                    n
                    (safeRead opts)
                    (dateFormat opts)
                    col
        generateColumn col =
            parseTypes $
                Vector.fromListN
                    numRow
                    ( map
                        ( \row ->
                            (stripQuotes . extractField')
                                (row * numCol + col)
                        )
                        [dataStartRow .. totalRows - 1]
                    )
        columns =
            Vector.fromListN
                numCol
                ( map generateColumn [0 .. numCol - 1]
                    `using` parList rpar
                )
        columnIndices =
            M.fromList $
                zip (Vector.toList columnNames) [0 ..]
        dataframeDimensions = (numRow, numCol)
    return $
        DataFrame columns columnIndices dataframeDimensions

{-# INLINE extractField #-}
extractField ::
    VS.Vector Word8 ->
    VS.Vector CSize ->
    Int ->
    Text
extractField file indices position =
    TextEncoding.decodeUtf8Lenient
        . unsafeToByteString
        $ VS.slice
            previous
            (next - previous)
            file
  where
    previous =
        if position == 0
            then 0
            else 1 + fromIntegral (indices VS.! (position - 1))
    next = fromIntegral $ indices VS.! position
    unsafeToByteString :: VS.Vector Word8 -> BS.ByteString
    unsafeToByteString v = PS (castForeignPtr ptr) 0 len
      where
        (ptr, len) = VS.unsafeToForeignPtr0 v

foreign import capi "process_csv.h get_delimiter_indices"
    get_delimiter_indices ::
        Ptr CUChar -> -- input
        CSize -> -- input size
        Ptr CSize -> -- result array
        IO CSize -- occupancy of result array

{-# INLINE getDelimiterIndices #-}
getDelimiterIndices ::
    Int ->
    VS.Vector Word8 ->
    IO (VS.Vector CSize)
getDelimiterIndices originalLen csvFile =
    VS.unsafeWith csvFile $ \buffer -> do
        let paddedLen = VS.length csvFile
        -- then number of delimiters cannot exceed the size
        -- of the input array (which would be a series of
        -- empty fields)
        indices <- mallocArray paddedLen
        num_fields <-
            get_delimiter_indices
                (castPtr buffer)
                (fromIntegral paddedLen)
                (castPtr indices)
        if num_fields == -1
            then getDelimiterIndices_ originalLen csvFile indices
            else do
                indices' <- newForeignPtr_ indices
                let resultVector = VSM.unsafeFromForeignPtr0 indices' paddedLen
                -- Handle the case where the file doesn't end with a newline
                -- We need to add a final delimiter for the last field
                finalResultLen <-
                    if originalLen > 0 && csvFile VS.! (originalLen - 1) /= lf
                        then do
                            VSM.write resultVector (fromIntegral num_fields) (fromIntegral originalLen)
                            return (fromIntegral num_fields + 1)
                        else return (fromIntegral num_fields)
                VS.unsafeFreeze $ VSM.slice 0 finalResultLen resultVector

-- We have a Native version in case the C version
-- cannot be used. For example if neither ARM_NEON
-- nor AVX2 are available

lf, cr, comma, quote :: Word8
lf = 0x0A
cr = 0x0D
comma = 0x2C
quote = 0x22

-- We parse using a state machine
data State
    = UnEscaped -- non quoted
    | Escaped -- quoted
    deriving (Enum)

{-# INLINE stateTransitionTable #-}
stateTransitionTable :: UArray (Int, Word8) Int
stateTransitionTable = array ((0, 0), (1, 255)) [(i, f i) | i <- range ((0, 0), (1, 255))]
  where
    -- Unescaped newline
    f (0, 0x0A) = fromEnum UnEscaped
    -- Unescaped comma
    f (0, 0x2C) = fromEnum UnEscaped
    -- Unescaped quote
    f (0, 0x22) = fromEnum Escaped
    -- Escaped quote
    -- escaped quote in fields are dealt as
    -- consecutive quoted sections of a field
    -- example: If we have
    -- field1, "abc""def""ghi, field3
    -- we end up processing abc, def, and ghi
    -- as consecutive quoted strings.
    f (1, 0x22) = fromEnum UnEscaped
    -- Everything else
    f (state, _) = state

{-# INLINE getDelimiterIndices_ #-}
getDelimiterIndices_ ::
    Int ->
    VS.Vector Word8 ->
    Ptr CSize ->
    IO (VS.Vector CSize)
getDelimiterIndices_ originalLen csvFile resultPtr = do
    resultVector <- resultVectorM
    (_, resultLen) <-
        VS.ifoldM'
            (processCharacter resultVector)
            (UnEscaped, 0)
            csvFile
    -- Handle the case where the file doesn't end with a newline
    -- We need to add a final delimiter for the last field
    finalResultLen <-
        if originalLen > 0 && csvFile VS.! (originalLen - 1) /= lf
            then do
                VSM.write resultVector resultLen (fromIntegral originalLen)
                return (resultLen + 1)
            else return resultLen
    VS.unsafeFreeze $ VSM.slice 0 finalResultLen resultVector
  where
    paddedLen = VS.length csvFile
    resultVectorM = do
        resultForeignPtr <- newForeignPtr_ resultPtr
        return $ VSM.unsafeFromForeignPtr0 resultForeignPtr paddedLen
    processCharacter ::
        VSM.IOVector CSize ->
        (State, Int) ->
        Int ->
        Word8 ->
        IO (State, Int)
    processCharacter
        resultVector
        (!state, !resultIndex)
        index
        character =
            case state of
                UnEscaped ->
                    if character == lf || character == comma
                        then do
                            VSM.write
                                resultVector
                                resultIndex
                                (fromIntegral index)
                            return (newState, resultIndex + 1)
                        else return (newState, resultIndex)
                Escaped -> return (newState, resultIndex)
          where
            newState =
                toEnum $
                    stateTransitionTable
                        ! (fromEnum state, character)

{-# INLINE countColumnsInFirstRow #-}
countColumnsInFirstRow ::
    VS.Vector Word8 ->
    VS.Vector CSize ->
    Int
countColumnsInFirstRow file indices
    | VS.length indices == 0 = 0
    | otherwise =
        1
            + VS.length
                ( VS.takeWhile
                    (\i -> file VS.! fromIntegral i /= lf)
                    indices
                )
