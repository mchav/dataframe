{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE ScopedTypeVariables #-}

module DataFrame.IO.Parquet.Page (
    -- Types
    PageDecoder,
    UnboxedPageDecoder,
    -- Per-type decoders
    boolDecoder,
    int32Decoder,
    int64Decoder,
    int96Decoder,
    floatDecoder,
    doubleDecoder,
    byteArrayDecoder,
    fixedLenByteArrayDecoder,
    -- Page iteration
    foldColumnPagesM,
    foldColumnDataPagesM,
    RawPage,
    decodeRequiredNumericInto,
    appendStringPageIO,
    appendNullableStringPageIO,
) where

import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.ST (RealWorld, stToIO)
import Data.Bits (shiftL, shiftR, (.&.), (.|.))
import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as BSU
import Data.Int (Int32, Int64)
import Data.Maybe (fromJust, fromMaybe)
import qualified Data.Text as T
import Data.Text.Encoding (decodeUtf8Lenient)
import Data.Time (UTCTime)
import qualified Data.Vector as VB
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import Data.Word (Word8)
import DataFrame.IO.Parquet.Decompress (decompressData)
import DataFrame.IO.Parquet.Dictionary (
    DictVals (..),
    dictText,
    readDictVals,
    word32At,
    word64At,
 )
import DataFrame.IO.Parquet.Encoding (
    decodeDictIndices,
    decodeRLEBitPackedHybrid,
    rleHybridFold,
 )
import DataFrame.IO.Parquet.Levels (readLevelsV1, readLevelsV2)
import DataFrame.IO.Parquet.Thrift (
    ColumnChunk (..),
    ColumnMetaData (..),
    CompressionCodec,
    DataPageHeader (..),
    DataPageHeaderV2 (..),
    DictionaryPageHeader (..),
    Encoding (..),
    PageHeader (..),
    PageType (..),
    ThriftType (..),
    unField,
 )
import DataFrame.IO.Parquet.Time (int96ToUTCTime)
import DataFrame.IO.Parquet.Utils (ColumnDescription (..))
import DataFrame.IO.Utils.RandomAccess (RandomAccess (..), Range (Range))
import DataFrame.Internal.Binary (littleEndianInt32)
import DataFrame.Internal.Column.Builder (
    TextBuilder,
    appendNull,
    appendTextSliceFromPtr,
 )
import Foreign.Ptr (Ptr, castPtr, plusPtr)
import Foreign.Storable (peekByteOff)
import GHC.Float (castWord32ToFloat, castWord64ToDouble)
import Pinch (decodeWithLeftovers)
import qualified Pinch

-- ---------------------------------------------------------------------------
-- Types
-- ---------------------------------------------------------------------------

{- | A type-specific page decoder.
Given the optional dictionary, the page encoding, the number of present
values, and the decompressed value bytes, returns exactly @nPresent@ values.
-}
type PageDecoder a =
    Maybe DictVals -> Encoding -> Int -> BS.ByteString -> VB.Vector a

type UnboxedPageDecoder a =
    Maybe DictVals -> Encoding -> Int -> BS.ByteString -> VU.Vector a

-- ---------------------------------------------------------------------------
-- Per-type decoders
-- ---------------------------------------------------------------------------

boolDecoder :: UnboxedPageDecoder Bool
boolDecoder mDict enc nPresent bs = case enc of
    -- PLAIN bools are bit-packed (1 bit/value, LSB-first). Generate the
    -- unboxed vector directly by indexing the bit for each row, avoiding an
    -- intermediate @[Bool]@ list.
    PLAIN _ ->
        VU.generate nPresent $ \i ->
            (BSU.unsafeIndex bs (i `shiftR` 3) `shiftR` (i .&. 7)) .&. 1 == 1
    RLE_DICTIONARY _ -> gatherDict "boolDecoder" getBool mDict nPresent bs
    PLAIN_DICTIONARY _ -> gatherDict "boolDecoder" getBool mDict nPresent bs
    _ -> error ("boolDecoder: unsupported encoding " ++ show enc)
  where
    getBool (DBool ds) = Just ds
    getBool _ = Nothing

int32Decoder :: UnboxedPageDecoder Int32
int32Decoder mDict enc nPresent bs = case enc of
    PLAIN _ -> readNInt32 nPresent bs
    RLE_DICTIONARY _ -> gatherDict "int32Decoder" getInt32 mDict nPresent bs
    PLAIN_DICTIONARY _ -> gatherDict "int32Decoder" getInt32 mDict nPresent bs
    _ -> error ("int32Decoder: unsupported encoding " ++ show enc)
  where
    getInt32 (DInt32 ds) = Just ds
    getInt32 _ = Nothing

int64Decoder :: UnboxedPageDecoder Int64
int64Decoder mDict enc nPresent bs = case enc of
    PLAIN _ -> readNInt64 nPresent bs
    RLE_DICTIONARY _ -> gatherDict "int64Decoder" getInt64 mDict nPresent bs
    PLAIN_DICTIONARY _ -> gatherDict "int64Decoder" getInt64 mDict nPresent bs
    _ -> error ("int64Decoder: unsupported encoding " ++ show enc)
  where
    getInt64 (DInt64 ds) = Just ds
    getInt64 _ = Nothing

int96Decoder :: PageDecoder UTCTime
int96Decoder mDict enc nPresent bs = case enc of
    PLAIN _ -> VB.fromList (readNInt96 nPresent bs)
    RLE_DICTIONARY _ -> lookupDict mDict nPresent bs getInt96
    PLAIN_DICTIONARY _ -> lookupDict mDict nPresent bs getInt96
    _ -> error ("int96Decoder: unsupported encoding " ++ show enc)
  where
    getInt96 (DInt96 ds) i = VB.unsafeIndex ds i
    getInt96 d _ = error ("int96Decoder: wrong dict type, got " ++ show d)

floatDecoder :: UnboxedPageDecoder Float
floatDecoder mDict enc nPresent bs = case enc of
    PLAIN _ -> readNFloat nPresent bs
    RLE_DICTIONARY _ -> gatherDict "floatDecoder" getFloat mDict nPresent bs
    PLAIN_DICTIONARY _ -> gatherDict "floatDecoder" getFloat mDict nPresent bs
    _ -> error ("floatDecoder: unsupported encoding " ++ show enc)
  where
    getFloat (DFloat ds) = Just ds
    getFloat _ = Nothing

doubleDecoder :: UnboxedPageDecoder Double
doubleDecoder mDict enc nPresent bs = case enc of
    PLAIN _ -> readNDouble nPresent bs
    RLE_DICTIONARY _ -> gatherDict "doubleDecoder" getDouble mDict nPresent bs
    PLAIN_DICTIONARY _ -> gatherDict "doubleDecoder" getDouble mDict nPresent bs
    _ -> error ("doubleDecoder: unsupported encoding " ++ show enc)
  where
    getDouble (DDouble ds) = Just ds
    getDouble _ = Nothing

byteArrayDecoder :: PageDecoder T.Text
byteArrayDecoder mDict enc nPresent bs = case enc of
    PLAIN _ -> VB.fromList (readNTexts nPresent bs)
    RLE_DICTIONARY _ -> lookupDict mDict nPresent bs getText
    PLAIN_DICTIONARY _ -> lookupDict mDict nPresent bs getText
    _ -> error ("byteArrayDecoder: unsupported encoding " ++ show enc)
  where
    getText = dictText

fixedLenByteArrayDecoder :: Int -> PageDecoder T.Text
fixedLenByteArrayDecoder len mDict enc nPresent bs = case enc of
    PLAIN _ -> VB.fromList (readNFixedTexts len nPresent bs)
    RLE_DICTIONARY _ -> lookupDict mDict nPresent bs getText
    PLAIN_DICTIONARY _ -> lookupDict mDict nPresent bs getText
    _ -> error ("fixedLenByteArrayDecoder: unsupported encoding " ++ show enc)
  where
    getText = dictText

{- | Shared dictionary-path helper: decode @nPresent@ RLE/bit-packed indices
and look each one up in the dictionary.
-}
lookupDict ::
    Maybe DictVals ->
    Int ->
    BS.ByteString ->
    (DictVals -> Int -> a) ->
    VB.Vector a
lookupDict mDict nPresent bs f = case mDict of
    Nothing -> error "Dictionary-encoded page but no dictionary page seen"
    Just dict ->
        let (idxs, _) = decodeDictIndices nPresent bs
         in VB.generate nPresent (f dict . VU.unsafeIndex idxs)

{- | Decode @nPresent@ dictionary indices and gather the entries from an
unboxed dictionary. Inlined per element type so the gather is a plain loop.
-}
gatherDict ::
    (VU.Unbox a) =>
    String ->
    (DictVals -> Maybe (VU.Vector a)) ->
    Maybe DictVals ->
    Int ->
    BS.ByteString ->
    VU.Vector a
gatherDict who pick mDict nPresent bs = case mDict of
    Nothing -> error (who ++ ": dictionary-encoded page but no dictionary page seen")
    Just dict -> case pick dict of
        Nothing -> error (who ++ ": wrong dict type, got " ++ show dict)
        Just ds -> case BS.uncons bs of
            Nothing -> error (who ++ ": empty index stream")
            Just (w0, rest) ->
                let (idxs, _) = decodeRLEBitPackedHybrid (fromIntegral w0) nPresent rest
                 in VU.map (VU.unsafeIndex ds . fromIntegral) idxs
{-# INLINE gatherDict #-}

-- ---------------------------------------------------------------------------
-- Core page-iteration loop
-- ---------------------------------------------------------------------------

-- | Read the raw (compressed) byte range for a column chunk.
readChunkBytes ::
    (RandomAccess m) =>
    ColumnChunk ->
    m (CompressionCodec, ThriftType, BS.ByteString)
readChunkBytes columnChunk = do
    let meta = fromJust . unField $ columnChunk.cc_meta_data
        codec = unField meta.cmd_codec
        pType = unField meta.cmd_type
        dataOffset = fromIntegral . unField $ meta.cmd_data_page_offset
        dictOffset = fromIntegral <$> unField meta.cmd_dictionary_page_offset
        offset = fromMaybe dataOffset dictOffset
        compLen = fromIntegral . unField $ meta.cmd_total_compressed_size
    rawBytes <- readBytes (Range offset compLen)
    return (codec, pType, rawBytes)

{- | A decoded DATA page handed to a fold step: the running dictionary, the
page encoding, the present-value count, the (decompressed) value bytes, and the
definition/repetition level vectors.
-}
type RawPage =
    (Maybe DictVals, Encoding, Int, BS.ByteString, VU.Vector Int, VU.Vector Int)

{- | Left-fold a monadic step over every DATA page (V1 or V2) of every column
chunk, in order, threading the running dictionary internally and handing each
page to @step@ as a 'RawPage' (no value decoding — the step decides how to
materialize, e.g. into a typed vector or straight into a text buffer).

Dictionary pages update the running dictionary; @INDEX_PAGE@s are skipped. Only
one page's bytes are live at a time.

-- TODO: when a page index is available, use it here to compute which page
-- byte ranges to request from the RandomAccess layer instead of reading the
-- entire column chunk in one contiguous read.

-- TODO: accept an optional row-range and use the column/offset page index
-- (when present in file metadata) to skip pages whose row range does not
-- overlap the requested range, avoiding decompression of irrelevant pages
-- entirely.
-}
{-# INLINEABLE foldColumnDataPagesM #-}
foldColumnDataPagesM ::
    forall m acc.
    (RandomAccess m, MonadIO m) =>
    ColumnDescription ->
    [ColumnChunk] ->
    (acc -> RawPage -> m acc) ->
    acc ->
    m acc
foldColumnDataPagesM description chunks step = goChunks chunks
  where
    maxDef = fromIntegral description.maxDefinitionLevel :: Int
    maxRep = fromIntegral description.maxRepetitionLevel :: Int

    goChunks [] !acc = return acc
    goChunks (cc : ccs) !acc = do
        (codec, pType, rawBytes) <- readChunkBytes cc
        acc' <- goPages Nothing codec pType rawBytes acc
        goChunks ccs acc'

    goPages dict codec pType bs !acc
        | BS.null bs = return acc
        | otherwise = case parsePageHeader bs of
            Left e -> error ("foldColumnDataPagesM: failed to parse page header: " ++ e)
            Right (rest, hdr) -> do
                let compSz = fromIntegral . unField $ hdr.ph_compressed_page_size
                    uncmpSz = fromIntegral . unField $ hdr.ph_uncompressed_page_size
                    (pageData, rest') = BS.splitAt compSz rest
                case unField hdr.ph_type of
                    DICTIONARY_PAGE _ -> do
                        let dictHdr =
                                fromMaybe
                                    (error "DICTIONARY_PAGE: missing dictionary page header")
                                    (unField hdr.ph_dictionary_page_header)
                            numVals = unField dictHdr.diph_num_values
                        decompressed <- liftIO $ decompressData uncmpSz codec pageData
                        let d = readDictVals pType decompressed numVals description.typeLength
                        goPages (Just d) codec pType rest' acc
                    DATA_PAGE _ -> do
                        let dph =
                                fromMaybe
                                    (error "DATA_PAGE: missing data page header")
                                    (unField hdr.ph_data_page_header)
                            n = fromIntegral . unField $ dph.dph_num_values
                            enc = unField dph.dph_encoding
                        decompressed <- liftIO $ decompressData uncmpSz codec pageData
                        let (defLvls, repLvls, nPresent, valBytes) =
                                readLevelsV1 n maxDef maxRep decompressed
                        acc' <- step acc (dict, enc, nPresent, valBytes, defLvls, repLvls)
                        goPages dict codec pType rest' acc'
                    DATA_PAGE_V2 _ -> do
                        let dph2 =
                                fromMaybe
                                    (error "DATA_PAGE_V2: missing data page header v2")
                                    (unField hdr.ph_data_page_header_v2)
                            n = fromIntegral . unField $ dph2.dph2_num_values
                            enc = unField dph2.dph2_encoding
                            defLen = unField dph2.dph2_definition_levels_byte_length
                            repLen = unField dph2.dph2_repetition_levels_byte_length
                            -- V2: levels are never compressed; only the value
                            -- payload is (optionally) compressed.
                            isCompressed = fromMaybe True (unField dph2.dph2_is_compressed)
                            (defLvls, repLvls, nPresent, compValBytes) =
                                readLevelsV2 n maxDef maxRep repLen defLen pageData
                        valBytes <-
                            if isCompressed
                                then liftIO $ decompressData uncmpSz codec compValBytes
                                else pure compValBytes
                        acc' <- step acc (dict, enc, nPresent, valBytes, defLvls, repLvls)
                        goPages dict codec pType rest' acc'
                    INDEX_PAGE _ -> goPages dict codec pType rest' acc

{- | Left-fold over per-page value triples, decoding each page with @decoder@.
A thin wrapper over 'foldColumnDataPagesM' for the typed (numeric/boxed) column
paths.
-}
{-# INLINEABLE foldColumnPagesM #-}
foldColumnPagesM ::
    forall m v a acc.
    (RandomAccess m, MonadIO m, VG.Vector v a) =>
    ColumnDescription ->
    (Maybe DictVals -> Encoding -> Int -> BS.ByteString -> v a) ->
    [ColumnChunk] ->
    (acc -> (v a, VU.Vector Int, VU.Vector Int) -> m acc) ->
    acc ->
    m acc
foldColumnPagesM description decoder chunks step =
    foldColumnDataPagesM description chunks $
        \acc (dict, enc, nPresent, valBytes, defLvls, repLvls) ->
            step acc (decoder dict enc nPresent valBytes, defLvls, repLvls)

{- | Append a non-nullable BYTE_ARRAY page's strings straight into a
'TextBuilder', no intermediate boxed 'Text' vector.

PLAIN pages copy each value's UTF-8 bytes directly from the page buffer pointer
('appendTextSliceFromPtr'), skipping the per-value 'decodeUtf8Lenient' +
'ByteString' slicing + 'Text' allocation that @readNTexts@ would do.
Dictionary pages append the shared dictionary 'Text's by reference.
-}
appendStringPageIO ::
    TextBuilder RealWorld ->
    Maybe DictVals ->
    Encoding ->
    Int ->
    BS.ByteString ->
    IO ()
appendStringPageIO builder mDict enc nPresent bs = case enc of
    PLAIN _ -> BSU.unsafeUseAsCStringLen bs $ \(cptr, _len) -> do
        let p = castPtr cptr :: Ptr Word8
            go !_ !i | i >= nPresent = pure ()
            go !off !i = do
                len <- readLenLE p off
                stToIO (appendTextSliceFromPtr builder (p `plusPtr` (off + 4)) len)
                go (off + 4 + len) (i + 1)
        go 0 0
    RLE_DICTIONARY _ -> dictAppend
    PLAIN_DICTIONARY _ -> dictAppend
    _ -> error ("appendStringPageIO: unsupported encoding " ++ show enc)
  where
    dictAppend = case mDict of
        Just (DText starts lens dictBytes) ->
            BSU.unsafeUseAsCStringLen dictBytes $ \(cptr, _) -> do
                let p = castPtr cptr :: Ptr Word8
                    (idxs, _) = decodeDictIndices nPresent bs
                VU.forM_ idxs $ \i ->
                    stToIO
                        ( appendTextSliceFromPtr
                            builder
                            (p `plusPtr` VU.unsafeIndex starts i)
                            (VU.unsafeIndex lens i)
                        )
        Just d -> error ("appendStringPageIO: wrong dict type, got " ++ show d)
        Nothing -> error "appendStringPageIO: dictionary-encoded page but no dictionary seen"

{- | Read a little-endian 4-byte length prefix at byte @off@ from a raw page
pointer.
-}
readLenLE :: Ptr Word8 -> Int -> IO Int
readLenLE p off = do
    b0 <- peekByteOff p off :: IO Word8
    b1 <- peekByteOff p (off + 1) :: IO Word8
    b2 <- peekByteOff p (off + 2) :: IO Word8
    b3 <- peekByteOff p (off + 3) :: IO Word8
    pure $
        fromIntegral b0
            .|. (fromIntegral b1 `shiftL` 8)
            .|. (fromIntegral b2 `shiftL` 16)
            .|. (fromIntegral b3 `shiftL` 24)
{-# INLINE readLenLE #-}

{- | Append a NULLABLE BYTE_ARRAY page into a 'TextBuilder', interleaving nulls
by walking the definition levels: a present row (@def == maxDef@) consumes the
next stored value (dictionary entry by reference, or the next length-prefixed
PLAIN slice by memcpy); a null row ('appendNull') writes an offset and clears a
validity bit. Builds 'PackedText' + validity bitmap with no per-row 'Text' or
@[Maybe a]@ allocation. Only present values are stored in the page payload.
-}
appendNullableStringPageIO ::
    TextBuilder RealWorld ->
    Int ->
    Maybe DictVals ->
    Encoding ->
    Int ->
    BS.ByteString ->
    VU.Vector Int ->
    IO ()
appendNullableStringPageIO builder maxDef mDict enc nPresent bs defs = case enc of
    PLAIN _ -> BSU.unsafeUseAsCStringLen bs $ \(cptr, _len) -> do
        let p = castPtr cptr :: Ptr Word8
            go !_ !i | i >= nDefs = pure ()
            go !off !i
                | VU.unsafeIndex defs i == maxDef = do
                    len <- readLenLE p off
                    stToIO (appendTextSliceFromPtr builder (p `plusPtr` (off + 4)) len)
                    go (off + 4 + len) (i + 1)
                | otherwise = stToIO (appendNull builder) >> go off (i + 1)
        go 0 0
    RLE_DICTIONARY _ -> dictAppend
    PLAIN_DICTIONARY _ -> dictAppend
    _ -> error ("appendNullableStringPageIO: unsupported encoding " ++ show enc)
  where
    nDefs = VU.length defs
    dictAppend = case mDict of
        Just (DText starts lens dictBytes) -> BSU.unsafeUseAsCStringLen dictBytes $ \(cptr, _) -> do
            let p = castPtr cptr :: Ptr Word8
                (idxs, _) = decodeDictIndices nPresent bs
                go !_ !i | i >= nDefs = pure ()
                go !j !i
                    | VU.unsafeIndex defs i == maxDef = do
                        let d = VU.unsafeIndex idxs j
                        stToIO
                            ( appendTextSliceFromPtr
                                builder
                                (p `plusPtr` VU.unsafeIndex starts d)
                                (VU.unsafeIndex lens d)
                            )
                        go (j + 1) (i + 1)
                    | otherwise = stToIO (appendNull builder) >> go j (i + 1)
            go 0 0
        Just d -> error ("appendNullableStringPageIO: wrong dict type, got " ++ show d)
        Nothing ->
            error
                "appendNullableStringPageIO: dictionary-encoded page but no dictionary seen"

-- ---------------------------------------------------------------------------
-- Page header parsing
-- ---------------------------------------------------------------------------

parsePageHeader :: BS.ByteString -> Either String (BS.ByteString, PageHeader)
parsePageHeader = decodeWithLeftovers Pinch.compactProtocol

-- ---------------------------------------------------------------------------
-- Batch value readers
-- ---------------------------------------------------------------------------

readNInt32 :: Int -> BS.ByteString -> VU.Vector Int32
readNInt32 n bs = VU.generate n $ \i -> fromIntegral (word32At bs (4 * i))

readNInt64 :: Int -> BS.ByteString -> VU.Vector Int64
readNInt64 n bs = VU.generate n $ \i -> fromIntegral (word64At bs (8 * i))

readNInt96 :: Int -> BS.ByteString -> [UTCTime]
readNInt96 0 _ = []
readNInt96 n bs = int96ToUTCTime (BS.take 12 bs) : readNInt96 (n - 1) (BS.drop 12 bs)

readNFloat :: Int -> BS.ByteString -> VU.Vector Float
readNFloat n bs = VU.generate n $ \i -> castWord32ToFloat (word32At bs (4 * i))

readNDouble :: Int -> BS.ByteString -> VU.Vector Double
readNDouble n bs = VU.generate n $ \i -> castWord64ToDouble (word64At bs (8 * i))

readNTexts :: Int -> BS.ByteString -> [T.Text]
readNTexts 0 _ = []
readNTexts n bs =
    let len = fromIntegral . littleEndianInt32 . BS.take 4 $ bs
        text = decodeUtf8Lenient . BS.take len . BS.drop 4 $ bs
     in text : readNTexts (n - 1) (BS.drop (4 + len) bs)

readNFixedTexts :: Int -> Int -> BS.ByteString -> [T.Text]
readNFixedTexts _ 0 _ = []
readNFixedTexts len n bs =
    decodeUtf8Lenient (BS.take len bs)
        : readNFixedTexts len (n - 1) (BS.drop len bs)

{- | Decode a required numeric column chunk (no levels) straight into
@out[off ..]@ and return the next free index. Each page is one loop from the
decompressed bytes to the destination: no per-page vectors, no concatenation.
Inlined so each element type gets its own copy.
-}
decodeRequiredNumericInto ::
    forall a m.
    (VU.Unbox a, RandomAccess m, MonadIO m) =>
    (DictVals -> Maybe (VU.Vector a)) ->
    (BS.ByteString -> Int -> a) ->
    Int ->
    ColumnDescription ->
    ColumnChunk ->
    VUM.IOVector a ->
    Int ->
    m Int
decodeRequiredNumericInto pick plainAt width description chunk out off0 = do
    (codec, pType, rawBytes) <- readChunkBytes chunk
    goPages Nothing codec pType rawBytes off0
  where
    goPages ::
        Maybe (VU.Vector a) ->
        CompressionCodec ->
        ThriftType ->
        BS.ByteString ->
        Int ->
        m Int
    goPages dict codec pType bs !off
        | BS.null bs = return off
        | otherwise = case parsePageHeader bs of
            Left e -> error ("decodeRequiredNumericInto: failed to parse page header: " ++ e)
            Right (rest, hdr) -> do
                let compSz = fromIntegral . unField $ hdr.ph_compressed_page_size
                    uncmpSz = fromIntegral . unField $ hdr.ph_uncompressed_page_size
                    (pageData, rest') = BS.splitAt compSz rest
                case unField hdr.ph_type of
                    DICTIONARY_PAGE _ -> do
                        let dictHdr =
                                fromMaybe
                                    (error "DICTIONARY_PAGE: missing dictionary page header")
                                    (unField hdr.ph_dictionary_page_header)
                            numVals = unField dictHdr.diph_num_values
                        decompressed <- liftIO $ decompressData uncmpSz codec pageData
                        let d = readDictVals pType decompressed numVals description.typeLength
                        case pick d of
                            Nothing -> error "decodeRequiredNumericInto: dictionary page mistyped"
                            Just !ds -> goPages (Just ds) codec pType rest' off
                    DATA_PAGE _ -> do
                        let dph =
                                fromMaybe
                                    (error "DATA_PAGE: missing data page header")
                                    (unField hdr.ph_data_page_header)
                            n = fromIntegral . unField $ dph.dph_num_values
                            enc = unField dph.dph_encoding
                        decompressed <- liftIO $ decompressData uncmpSz codec pageData
                        liftIO (writePage dict enc n decompressed off)
                        goPages dict codec pType rest' (off + n)
                    DATA_PAGE_V2 _ -> do
                        let dph2 =
                                fromMaybe
                                    (error "DATA_PAGE_V2: missing data page header v2")
                                    (unField hdr.ph_data_page_header_v2)
                            n = fromIntegral . unField $ dph2.dph2_num_values
                            enc = unField dph2.dph2_encoding
                            levelBytes =
                                fromIntegral (unField dph2.dph2_definition_levels_byte_length)
                                    + fromIntegral (unField dph2.dph2_repetition_levels_byte_length)
                            isCompressed = fromMaybe True (unField dph2.dph2_is_compressed)
                            compValBytes = BS.drop levelBytes pageData
                        valBytes <-
                            if isCompressed
                                then liftIO $ decompressData (uncmpSz - levelBytes) codec compValBytes
                                else pure compValBytes
                        liftIO (writePage dict enc n valBytes off)
                        goPages dict codec pType rest' (off + n)
                    INDEX_PAGE _ -> goPages dict codec pType rest' off

    writePage ::
        Maybe (VU.Vector a) -> Encoding -> Int -> BS.ByteString -> Int -> IO ()
    writePage dict enc n vals off = case enc of
        PLAIN _ ->
            let go !i
                    | i >= n = pure ()
                    | otherwise = do
                        VUM.unsafeWrite out (off + i) (plainAt vals (width * i))
                        go (i + 1)
             in go 0
        RLE_DICTIONARY _ -> gather
        PLAIN_DICTIONARY _ -> gather
        _ -> error ("decodeRequiredNumericInto: unsupported encoding " ++ show enc)
      where
        gather = case dict of
            Nothing -> error "decodeRequiredNumericInto: dictionary page missing"
            Just ds -> case BS.uncons vals of
                Nothing -> error "decodeRequiredNumericInto: empty index stream"
                Just (w0, rest) ->
                    stToIO $
                        rleHybridFold (fromIntegral w0) n rest $ \i w ->
                            VUM.unsafeWrite out (off + i) (VU.unsafeIndex ds (fromIntegral w))
{-# INLINE decodeRequiredNumericInto #-}
