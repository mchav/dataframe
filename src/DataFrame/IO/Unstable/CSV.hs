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
import Data.Array.IArray (genArray, (!))
import Data.Array.Unboxed (UArray)

import DataFrame.IO.CSV (
    HeaderSpec (..),
    ReadOptions (..),
    defaultReadOptions,
    shouldInferFromSample,
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
        ( \v -> do
            let len = VS.length v
            indices <- mallocArray len
            getDelimiterIndices_ v len indices
        )

readCsvUnstable' ::
    ReadOptions ->
    (VS.Vector Word8 -> IO (VS.Vector CSize)) ->
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
    indices <- delimiterIndices paddedCSVFile
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
                    map extractField' [0 .. numCol - 1]
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
                    (dateFormat opts)
                    (safeRead opts)
                    col
        generateColumn col =
            parseTypes $
                Vector.fromListN
                    numRow
                    ( map
                        ( \row ->
                            extractField'
                                ((row + dataStartRow) * numCol + col)
                        )
                        [0 .. numRow - 1]
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
    VS.Vector Word8 ->
    IO (VS.Vector CSize)
getDelimiterIndices csvFile =
    VS.unsafeWith csvFile $ \buffer -> do
        let len = VS.length csvFile
        -- then number of delimiters cannot exceed the size
        -- of the input array (which would be a series of
        -- empty fields)
        indices <- mallocArray len
        num_fields <-
            get_delimiter_indices
                (castPtr buffer)
                (fromIntegral len)
                (castPtr indices)
        if num_fields == -1
            then getDelimiterIndices_ csvFile len indices
            else do
                indices' <- newForeignPtr_ indices
                return $
                    VS.unsafeFromForeignPtr0 indices' $
                        fromIntegral num_fields

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
stateTransitionTable = genArray ((0, 0), (1, 255)) f
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
    VS.Vector Word8 ->
    Int ->
    Ptr CSize ->
    IO (VS.Vector CSize)
getDelimiterIndices_ csvFile len resultPtr = do
    resultVector <- resultVectorM
    (_, resultLen) <-
        VS.ifoldM'
            (processCharacter resultVector)
            (UnEscaped, 0)
            csvFile
    VS.unsafeFreeze $ VSM.slice 0 (resultLen + 1) resultVector
  where
    resultVectorM = do
        resultForeignPtr <- newForeignPtr_ resultPtr
        return $ VSM.unsafeFromForeignPtr0 resultForeignPtr len
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
