{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.IO.Parquet.Thrift where

import Control.Monad
import Data.Bits
import qualified Data.ByteString as BS
import Data.Char
import Data.IORef
import Data.Int
import qualified Data.Map as M
import Data.Maybe
import qualified Data.Text as T
import Data.Word
import DataFrame.IO.Parquet.Binary
import DataFrame.IO.Parquet.Types

data SchemaElement = SchemaElement
    { elementName :: T.Text
    , elementType :: TType
    , typeLength :: Int32
    , numChildren :: Int32
    , fieldId :: Int32
    , repetitionType :: RepetitionType
    , convertedType :: Int32
    , scale :: Int32
    , precision :: Int32
    , logicalType :: LogicalType
    }
    deriving (Show, Eq)

data KeyValue = KeyValue
    { key :: String
    , value :: String
    }
    deriving (Show, Eq)

data FileMetadata = FileMetaData
    { version :: Int32
    , schema :: [SchemaElement]
    , numRows :: Integer
    , rowGroups :: [RowGroup]
    , keyValueMetadata :: [KeyValue]
    , createdBy :: Maybe String
    , columnOrders :: [ColumnOrder]
    , encryptionAlgorithm :: EncryptionAlgorithm
    , footerSigningKeyMetadata :: [Word8]
    }
    deriving (Show, Eq)

data TType
    = STOP
    | BOOL
    | BYTE
    | I16
    | I32
    | I64
    | DOUBLE
    | STRING
    | LIST
    | SET
    | MAP
    | STRUCT
    | UUID
    deriving (Show, Eq)

defaultMetadata :: FileMetadata
defaultMetadata =
    FileMetaData
        { version = 0
        , schema = []
        , numRows = 0
        , rowGroups = []
        , keyValueMetadata = []
        , createdBy = Nothing
        , columnOrders = []
        , encryptionAlgorithm = ENCRYPTION_ALGORITHM_UNKNOWN
        , footerSigningKeyMetadata = []
        }

data ColumnMetaData = ColumnMetaData
    { columnType :: ParquetType
    , columnEncodings :: [ParquetEncoding]
    , columnPathInSchema :: [String]
    , columnCodec :: CompressionCodec
    , columnNumValues :: Int64
    , columnTotalUncompressedSize :: Int64
    , columnTotalCompressedSize :: Int64
    , columnKeyValueMetadata :: [KeyValue]
    , columnDataPageOffset :: Int64
    , columnIndexPageOffset :: Int64
    , columnDictionaryPageOffset :: Int64
    , columnStatistics :: ColumnStatistics
    , columnEncodingStats :: [PageEncodingStats]
    , bloomFilterOffset :: Int64
    , bloomFilterLength :: Int32
    , columnSizeStatistics :: SizeStatistics
    , columnGeospatialStatistics :: GeospatialStatistics
    }
    deriving (Show, Eq)

data ColumnChunk = ColumnChunk
    { columnChunkFilePath :: String
    , columnChunkMetadataFileOffset :: Int64
    , columnMetaData :: ColumnMetaData
    , columnChunkOffsetIndexOffset :: Int64
    , columnChunkOffsetIndexLength :: Int32
    , columnChunkColumnIndexOffset :: Int64
    , columnChunkColumnIndexLength :: Int32
    , cryptoMetadata :: ColumnCryptoMetadata
    , encryptedColumnMetadata :: [Word8]
    }
    deriving (Show, Eq)

data RowGroup = RowGroup
    { rowGroupColumns :: [ColumnChunk]
    , totalByteSize :: Int64
    , rowGroupNumRows :: Int64
    , rowGroupSortingColumns :: [SortingColumn]
    , fileOffset :: Int64
    , totalCompressedSize :: Int64
    , ordinal :: Int16
    }
    deriving (Show, Eq)

defaultSchemaElement :: SchemaElement
defaultSchemaElement =
    SchemaElement
        ""
        STOP
        0
        0
        (-1)
        UNKNOWN_REPETITION_TYPE
        0
        0
        0
        LOGICAL_TYPE_UNKNOWN

emptyColumnMetadata :: ColumnMetaData
emptyColumnMetadata =
    ColumnMetaData
        PARQUET_TYPE_UNKNOWN
        []
        []
        COMPRESSION_CODEC_UNKNOWN
        0
        0
        0
        []
        0
        0
        0
        emptyColumnStatistics
        []
        0
        0
        emptySizeStatistics
        emptyGeospatialStatistics

emptyColumnChunk :: ColumnChunk
emptyColumnChunk =
    ColumnChunk "" 0 emptyColumnMetadata 0 0 0 0 COLUMN_CRYPTO_METADATA_UNKNOWN []

emptyKeyValue :: KeyValue
emptyKeyValue = KeyValue{key = "", value = ""}

emptyRowGroup :: RowGroup
emptyRowGroup = RowGroup [] 0 0 [] 0 0 0

compactBooleanTrue
    , compactI32
    , compactI64
    , compactDouble
    , compactBinary
    , compactList
    , compactStruct ::
        Word8
compactBooleanTrue = 0x01
compactI32 = 0x05
compactI64 = 0x06
compactDouble = 0x07
compactBinary = 0x08
compactList = 0x09
compactStruct = 0x0C

toTType :: Word8 -> TType
toTType t =
    fromMaybe STOP $
        M.lookup (t .&. 0x0f) $
            M.fromList
                [ (compactBooleanTrue, BOOL)
                , (compactI32, I32)
                , (compactI64, I64)
                , (compactDouble, DOUBLE)
                , (compactBinary, STRING)
                , (compactList, LIST)
                , (compactStruct, STRUCT)
                ]

readField ::
    BS.ByteString -> IORef Int -> Int16 -> [Int16] -> IO (Maybe (TType, Int16))
readField buf pos lastFieldId fieldStack = do
    t <- readAndAdvance pos buf
    if t .&. 0x0f == 0
        then return Nothing
        else do
            let modifier = fromIntegral ((t .&. 0xf0) `shiftR` 4) :: Int16
            identifier <-
                if modifier == 0
                    then readIntFromBuffer @Int16 buf pos
                    else return (lastFieldId + modifier)
            let elemType = toTType (t .&. 0x0f)
            pure $ Just (elemType, identifier)

skipToStructEnd :: BS.ByteString -> IORef Int -> IO ()
skipToStructEnd buf pos = do
    t <- readAndAdvance pos buf
    if t .&. 0x0f == 0
        then return ()
        else do
            let modifier = fromIntegral ((t .&. 0xf0) `shiftR` 4) :: Int16
            identifier <-
                if modifier == 0
                    then readIntFromBuffer @Int16 buf pos
                    else return 0
            let elemType = toTType (t .&. 0x0f)
            skipFieldData elemType buf pos
            skipToStructEnd buf pos

skipFieldData :: TType -> BS.ByteString -> IORef Int -> IO ()
skipFieldData fieldType buf pos = case fieldType of
    BOOL -> return ()
    I32 -> void (readIntFromBuffer @Int32 buf pos)
    I64 -> void (readIntFromBuffer @Int64 buf pos)
    DOUBLE -> void (readIntFromBuffer @Int64 buf pos)
    STRING -> void (readByteString buf pos)
    LIST -> skipList buf pos
    STRUCT -> skipToStructEnd buf pos
    _ -> return ()

skipList :: BS.ByteString -> IORef Int -> IO ()
skipList buf pos = do
    sizeAndType <- readAndAdvance pos buf
    let sizeOnly = fromIntegral ((sizeAndType `shiftR` 4) .&. 0x0f) :: Int
    let elemType = toTType sizeAndType
    replicateM_ sizeOnly (skipFieldData elemType buf pos)

readMetadata :: BS.ByteString -> Int -> IO FileMetadata
readMetadata contents size = do
    let metadataStartPos = BS.length contents - footerSize - size
    let metadataBytes =
            BS.pack $
                map (BS.index contents) [metadataStartPos .. (metadataStartPos + size - 1)]
    let lastFieldId = 0
    let fieldStack = []
    bufferPos <- newIORef (0 :: Int)
    metadata <-
        readFileMetaData defaultMetadata metadataBytes bufferPos lastFieldId fieldStack
    return metadata

readFileMetaData ::
    FileMetadata ->
    BS.ByteString ->
    IORef Int ->
    Int16 ->
    [Int16] ->
    IO FileMetadata
readFileMetaData metadata metaDataBuf bufferPos lastFieldId fieldStack = do
    fieldContents <- readField metaDataBuf bufferPos lastFieldId fieldStack
    case fieldContents of
        Nothing -> return metadata
        Just (elemType, identifier) -> case identifier of
            1 -> do
                version <- readIntFromBuffer @Int32 metaDataBuf bufferPos
                readFileMetaData
                    (metadata{version = version})
                    metaDataBuf
                    bufferPos
                    identifier
                    fieldStack
            2 -> do
                sizeAndType <- readAndAdvance bufferPos metaDataBuf
                listSize <-
                    if (sizeAndType `shiftR` 4) .&. 0x0f == 15
                        then readVarIntFromBuffer @Int metaDataBuf bufferPos
                        else return $ fromIntegral ((sizeAndType `shiftR` 4) .&. 0x0f)
                let _elemType = toTType sizeAndType
                schemaElements <-
                    replicateM
                        listSize
                        (readSchemaElement defaultSchemaElement metaDataBuf bufferPos 0 [])
                readFileMetaData
                    (metadata{schema = schemaElements})
                    metaDataBuf
                    bufferPos
                    identifier
                    fieldStack
            3 -> do
                numRows <- readIntFromBuffer @Int64 metaDataBuf bufferPos
                readFileMetaData
                    (metadata{numRows = fromIntegral numRows})
                    metaDataBuf
                    bufferPos
                    identifier
                    fieldStack
            4 -> do
                sizeAndType <- readAndAdvance bufferPos metaDataBuf
                listSize <-
                    if (sizeAndType `shiftR` 4) .&. 0x0f == 15
                        then readVarIntFromBuffer @Int metaDataBuf bufferPos
                        else return $ fromIntegral ((sizeAndType `shiftR` 4) .&. 0x0f)

                -- TODO actually check elemType agrees (also for all the other underscored _elemType in this module)
                let _elemType = toTType sizeAndType
                rowGroups <-
                    replicateM listSize (readRowGroup emptyRowGroup metaDataBuf bufferPos 0 [])
                readFileMetaData
                    (metadata{rowGroups = rowGroups})
                    metaDataBuf
                    bufferPos
                    identifier
                    fieldStack
            5 -> do
                sizeAndType <- readAndAdvance bufferPos metaDataBuf
                listSize <-
                    if (sizeAndType `shiftR` 4) .&. 0x0f == 15
                        then readVarIntFromBuffer @Int metaDataBuf bufferPos
                        else return $ fromIntegral ((sizeAndType `shiftR` 4) .&. 0x0f)

                let _elemType = toTType sizeAndType
                keyValueMetadata <-
                    replicateM listSize (readKeyValue emptyKeyValue metaDataBuf bufferPos 0 [])
                readFileMetaData
                    (metadata{keyValueMetadata = keyValueMetadata})
                    metaDataBuf
                    bufferPos
                    identifier
                    fieldStack
            6 -> do
                createdBy <- readString metaDataBuf bufferPos
                readFileMetaData
                    (metadata{createdBy = Just createdBy})
                    metaDataBuf
                    bufferPos
                    identifier
                    fieldStack
            7 -> do
                sizeAndType <- readAndAdvance bufferPos metaDataBuf
                listSize <-
                    if (sizeAndType `shiftR` 4) .&. 0x0f == 15
                        then readVarIntFromBuffer @Int metaDataBuf bufferPos
                        else return $ fromIntegral ((sizeAndType `shiftR` 4) .&. 0x0f)

                let _elemType = toTType sizeAndType
                columnOrders <- replicateM listSize (readColumnOrder metaDataBuf bufferPos 0 [])
                readFileMetaData
                    (metadata{columnOrders = columnOrders})
                    metaDataBuf
                    bufferPos
                    identifier
                    fieldStack
            8 -> do
                encryptionAlgorithm <- readEncryptionAlgorithm metaDataBuf bufferPos 0 []
                readFileMetaData
                    (metadata{encryptionAlgorithm = encryptionAlgorithm})
                    metaDataBuf
                    bufferPos
                    identifier
                    fieldStack
            9 -> do
                footerSigningKeyMetadata <- readByteString metaDataBuf bufferPos
                readFileMetaData
                    (metadata{footerSigningKeyMetadata = footerSigningKeyMetadata})
                    metaDataBuf
                    bufferPos
                    identifier
                    fieldStack
            n -> return $ error $ "UNIMPLEMENTED " ++ show n

readSchemaElement ::
    SchemaElement ->
    BS.ByteString ->
    IORef Int ->
    Int16 ->
    [Int16] ->
    IO SchemaElement
readSchemaElement schemaElement buf pos lastFieldId fieldStack = do
    fieldContents <- readField buf pos lastFieldId fieldStack
    case fieldContents of
        Nothing -> return schemaElement
        Just (STOP, _) -> return schemaElement
        Just (elemType, identifier) -> case identifier of
            1 -> do
                schemaElemType <- toIntegralType <$> readInt32FromBuffer buf pos
                readSchemaElement
                    (schemaElement{elementType = schemaElemType})
                    buf
                    pos
                    identifier
                    fieldStack
            2 -> do
                typeLength <- readInt32FromBuffer buf pos
                readSchemaElement
                    (schemaElement{typeLength = typeLength})
                    buf
                    pos
                    identifier
                    fieldStack
            3 -> do
                fieldRepetitionType <- readInt32FromBuffer buf pos
                readSchemaElement
                    (schemaElement{repetitionType = repetitionTypeFromInt fieldRepetitionType})
                    buf
                    pos
                    identifier
                    fieldStack
            4 -> do
                nameSize <- readVarIntFromBuffer @Int buf pos
                if nameSize <= 0
                    then readSchemaElement schemaElement buf pos identifier fieldStack
                    else do
                        contents <- replicateM nameSize (readAndAdvance pos buf)
                        readSchemaElement
                            (schemaElement{elementName = T.pack (map (chr . fromIntegral) contents)})
                            buf
                            pos
                            identifier
                            fieldStack
            5 -> do
                numChildren <- readInt32FromBuffer buf pos
                readSchemaElement
                    (schemaElement{numChildren = numChildren})
                    buf
                    pos
                    identifier
                    fieldStack
            6 -> do
                convertedType <- readInt32FromBuffer buf pos
                readSchemaElement
                    (schemaElement{convertedType = convertedType})
                    buf
                    pos
                    identifier
                    fieldStack
            7 -> do
                scale <- readInt32FromBuffer buf pos
                readSchemaElement (schemaElement{scale = scale}) buf pos identifier fieldStack
            8 -> do
                precision <- readInt32FromBuffer buf pos
                readSchemaElement
                    (schemaElement{precision = precision})
                    buf
                    pos
                    identifier
                    fieldStack
            9 -> do
                fieldId <- readInt32FromBuffer buf pos
                readSchemaElement
                    (schemaElement{fieldId = fieldId})
                    buf
                    pos
                    identifier
                    fieldStack
            10 -> do
                logicalType <- readLogicalType buf pos 0 []
                readSchemaElement
                    (schemaElement{logicalType = logicalType})
                    buf
                    pos
                    identifier
                    fieldStack
            _ -> do
                skipFieldData elemType buf pos
                readSchemaElement schemaElement buf pos identifier fieldStack

readRowGroup ::
    RowGroup -> BS.ByteString -> IORef Int -> Int16 -> [Int16] -> IO RowGroup
readRowGroup r buf pos lastFieldId fieldStack = do
    fieldContents <- readField buf pos lastFieldId fieldStack
    case fieldContents of
        Nothing -> return r
        Just (elemType, identifier) -> case identifier of
            1 -> do
                sizeAndType <- readAndAdvance pos buf
                let sizeOnly = fromIntegral ((sizeAndType `shiftR` 4) .&. 0x0f) :: Int
                let _elemType = toTType sizeAndType
                columnChunks <-
                    replicateM sizeOnly (readColumnChunk emptyColumnChunk buf pos 0 [])
                readRowGroup (r{rowGroupColumns = columnChunks}) buf pos identifier fieldStack
            2 -> do
                totalBytes <- readIntFromBuffer @Int64 buf pos
                readRowGroup (r{totalByteSize = totalBytes}) buf pos identifier fieldStack
            3 -> do
                nRows <- readIntFromBuffer @Int64 buf pos
                readRowGroup (r{rowGroupNumRows = nRows}) buf pos identifier fieldStack
            4 -> return r
            5 -> do
                offset <- readIntFromBuffer @Int64 buf pos
                readRowGroup (r{fileOffset = offset}) buf pos identifier fieldStack
            6 -> do
                compressedSize <- readIntFromBuffer @Int64 buf pos
                readRowGroup
                    (r{totalCompressedSize = compressedSize})
                    buf
                    pos
                    identifier
                    fieldStack
            7 -> do
                ordinal <- readIntFromBuffer @Int16 buf pos
                readRowGroup (r{ordinal = ordinal}) buf pos identifier fieldStack
            _ -> error $ "Unknown row group field: " ++ show identifier

readColumnChunk ::
    ColumnChunk -> BS.ByteString -> IORef Int -> Int16 -> [Int16] -> IO ColumnChunk
readColumnChunk c buf pos lastFieldId fieldStack = do
    fieldContents <- readField buf pos lastFieldId fieldStack
    case fieldContents of
        Nothing -> return c
        Just (elemType, identifier) -> case identifier of
            1 -> do
                stringSize <- readVarIntFromBuffer @Int buf pos
                contents <-
                    map (chr . fromIntegral) <$> replicateM stringSize (readAndAdvance pos buf)
                readColumnChunk
                    (c{columnChunkFilePath = contents})
                    buf
                    pos
                    identifier
                    fieldStack
            2 -> do
                columnChunkMetadataFileOffset <- readIntFromBuffer @Int64 buf pos
                readColumnChunk
                    (c{columnChunkMetadataFileOffset = columnChunkMetadataFileOffset})
                    buf
                    pos
                    identifier
                    fieldStack
            3 -> do
                columnMetadata <- readColumnMetadata emptyColumnMetadata buf pos 0 []
                readColumnChunk
                    (c{columnMetaData = columnMetadata})
                    buf
                    pos
                    identifier
                    fieldStack
            4 -> do
                columnOffsetIndexOffset <- readIntFromBuffer @Int64 buf pos
                readColumnChunk
                    (c{columnChunkOffsetIndexOffset = columnOffsetIndexOffset})
                    buf
                    pos
                    identifier
                    fieldStack
            5 -> do
                columnOffsetIndexLength <- readInt32FromBuffer buf pos
                readColumnChunk
                    (c{columnChunkOffsetIndexLength = columnOffsetIndexLength})
                    buf
                    pos
                    identifier
                    fieldStack
            6 -> do
                columnChunkColumnIndexOffset <- readIntFromBuffer @Int64 buf pos
                readColumnChunk
                    (c{columnChunkColumnIndexOffset = columnChunkColumnIndexOffset})
                    buf
                    pos
                    identifier
                    fieldStack
            7 -> do
                columnChunkColumnIndexLength <- readInt32FromBuffer buf pos
                readColumnChunk
                    (c{columnChunkColumnIndexLength = columnChunkColumnIndexLength})
                    buf
                    pos
                    identifier
                    fieldStack
            _ -> return c

readColumnMetadata ::
    ColumnMetaData ->
    BS.ByteString ->
    IORef Int ->
    Int16 ->
    [Int16] ->
    IO ColumnMetaData
readColumnMetadata cm buf pos lastFieldId fieldStack = do
    fieldContents <- readField buf pos lastFieldId fieldStack
    case fieldContents of
        Nothing -> return cm
        Just (elemType, identifier) -> case identifier of
            1 -> do
                cType <- parquetTypeFromInt <$> readInt32FromBuffer buf pos
                readColumnMetadata (cm{columnType = cType}) buf pos identifier []
            2 -> do
                sizeAndType <- readAndAdvance pos buf
                let sizeOnly = fromIntegral ((sizeAndType `shiftR` 4) .&. 0x0f) :: Int
                let _elemType = toTType sizeAndType
                encodings <- replicateM sizeOnly (readParquetEncoding buf pos 0 [])
                readColumnMetadata
                    (cm{columnEncodings = encodings})
                    buf
                    pos
                    identifier
                    fieldStack
            3 -> do
                sizeAndType <- readAndAdvance pos buf
                let sizeOnly = fromIntegral ((sizeAndType `shiftR` 4) .&. 0x0f) :: Int
                let _elemType = toTType sizeAndType
                paths <- replicateM sizeOnly (readString buf pos)
                readColumnMetadata
                    (cm{columnPathInSchema = paths})
                    buf
                    pos
                    identifier
                    fieldStack
            4 -> do
                cType <- compressionCodecFromInt <$> readInt32FromBuffer buf pos
                readColumnMetadata (cm{columnCodec = cType}) buf pos identifier []
            5 -> do
                numValues <- readIntFromBuffer @Int64 buf pos
                readColumnMetadata (cm{columnNumValues = numValues}) buf pos identifier []
            6 -> do
                columnTotalUncompressedSize <- readIntFromBuffer @Int64 buf pos
                readColumnMetadata
                    (cm{columnTotalUncompressedSize = columnTotalUncompressedSize})
                    buf
                    pos
                    identifier
                    []
            7 -> do
                columnTotalCompressedSize <- readIntFromBuffer @Int64 buf pos
                readColumnMetadata
                    (cm{columnTotalCompressedSize = columnTotalCompressedSize})
                    buf
                    pos
                    identifier
                    []
            8 -> do
                sizeAndType <- readAndAdvance pos buf
                let sizeOnly = fromIntegral ((sizeAndType `shiftR` 4) .&. 0x0f) :: Int
                let _elemType = toTType sizeAndType
                columnKeyValueMetadata <-
                    replicateM sizeOnly (readKeyValue emptyKeyValue buf pos 0 [])
                readColumnMetadata
                    (cm{columnKeyValueMetadata = columnKeyValueMetadata})
                    buf
                    pos
                    identifier
                    fieldStack
            9 -> do
                columnDataPageOffset <- readIntFromBuffer @Int64 buf pos
                readColumnMetadata
                    (cm{columnDataPageOffset = columnDataPageOffset})
                    buf
                    pos
                    identifier
                    []
            10 -> do
                columnIndexPageOffset <- readIntFromBuffer @Int64 buf pos
                readColumnMetadata
                    (cm{columnIndexPageOffset = columnIndexPageOffset})
                    buf
                    pos
                    identifier
                    []
            11 -> do
                columnDictionaryPageOffset <- readIntFromBuffer @Int64 buf pos
                readColumnMetadata
                    (cm{columnDictionaryPageOffset = columnDictionaryPageOffset})
                    buf
                    pos
                    identifier
                    []
            12 -> do
                stats <- readStatistics emptyColumnStatistics buf pos 0 []
                readColumnMetadata (cm{columnStatistics = stats}) buf pos identifier fieldStack
            13 -> do
                sizeAndType <- readAndAdvance pos buf
                let sizeOnly = fromIntegral ((sizeAndType `shiftR` 4) .&. 0x0f) :: Int
                let _elemType = toTType sizeAndType
                pageEncodingStats <-
                    replicateM sizeOnly (readPageEncodingStats emptyPageEncodingStats buf pos 0 [])
                readColumnMetadata
                    (cm{columnEncodingStats = pageEncodingStats})
                    buf
                    pos
                    identifier
                    fieldStack
            14 -> do
                bloomFilterOffset <- readIntFromBuffer @Int64 buf pos
                readColumnMetadata
                    (cm{bloomFilterOffset = bloomFilterOffset})
                    buf
                    pos
                    identifier
                    []
            15 -> do
                bloomFilterLength <- readInt32FromBuffer buf pos
                readColumnMetadata
                    (cm{bloomFilterLength = bloomFilterLength})
                    buf
                    pos
                    identifier
                    []
            16 -> do
                stats <- readSizeStatistics emptySizeStatistics buf pos 0 []
                readColumnMetadata
                    (cm{columnSizeStatistics = stats})
                    buf
                    pos
                    identifier
                    fieldStack
            17 -> return $ error "UNIMPLEMENTED"
            _ -> return cm

readEncryptionAlgorithm ::
    BS.ByteString -> IORef Int -> Int16 -> [Int16] -> IO EncryptionAlgorithm
readEncryptionAlgorithm buf pos lastFieldId fieldStack = do
    fieldContents <- readField buf pos lastFieldId fieldStack
    case fieldContents of
        Nothing -> return ENCRYPTION_ALGORITHM_UNKNOWN
        Just (elemType, identifier) -> case identifier of
            1 -> do
                readAesGcmV1
                    (AesGcmV1{aadPrefix = [], aadFileUnique = [], supplyAadPrefix = False})
                    buf
                    pos
                    0
                    []
            2 -> do
                readAesGcmCtrV1
                    (AesGcmCtrV1{aadPrefix = [], aadFileUnique = [], supplyAadPrefix = False})
                    buf
                    pos
                    0
                    []
            n -> return ENCRYPTION_ALGORITHM_UNKNOWN

readColumnOrder ::
    BS.ByteString -> IORef Int -> Int16 -> [Int16] -> IO ColumnOrder
readColumnOrder buf pos lastFieldId fieldStack = do
    fieldContents <- readField buf pos lastFieldId fieldStack
    case fieldContents of
        Nothing -> return COLUMN_ORDER_UNKNOWN
        Just (elemType, identifier) -> case identifier of
            1 -> do
                replicateM_ 2 (readTypeOrder buf pos 0 [])
                return TYPE_ORDER
            _ -> return COLUMN_ORDER_UNKNOWN

readAesGcmCtrV1 ::
    EncryptionAlgorithm ->
    BS.ByteString ->
    IORef Int ->
    Int16 ->
    [Int16] ->
    IO EncryptionAlgorithm
readAesGcmCtrV1 v@(AesGcmCtrV1 aadPrefix aadFileUnique supplyAadPrefix) buf pos lastFieldId fieldStack = do
    fieldContents <- readField buf pos lastFieldId fieldStack
    case fieldContents of
        Nothing -> return v
        Just (elemType, identifier) -> case identifier of
            1 -> do
                aadPrefix <- readByteString buf pos
                readAesGcmCtrV1 (v{aadPrefix = aadPrefix}) buf pos lastFieldId fieldStack
            2 -> do
                aadFileUnique <- readByteString buf pos
                readAesGcmCtrV1
                    (v{aadFileUnique = aadFileUnique})
                    buf
                    pos
                    lastFieldId
                    fieldStack
            3 -> do
                supplyAadPrefix <- readAndAdvance pos buf
                readAesGcmCtrV1
                    (v{supplyAadPrefix = supplyAadPrefix == compactBooleanTrue})
                    buf
                    pos
                    lastFieldId
                    fieldStack
            _ -> return ENCRYPTION_ALGORITHM_UNKNOWN
readAesGcmCtrV1 _ _ _ _ _ =
    error "readAesGcmCtrV1 called with non AesGcmCtrV1"

readAesGcmV1 ::
    EncryptionAlgorithm ->
    BS.ByteString ->
    IORef Int ->
    Int16 ->
    [Int16] ->
    IO EncryptionAlgorithm
readAesGcmV1 v@(AesGcmV1 aadPrefix aadFileUnique supplyAadPrefix) buf pos lastFieldId fieldStack = do
    fieldContents <- readField buf pos lastFieldId fieldStack
    case fieldContents of
        Nothing -> return v
        Just (elemType, identifier) -> case identifier of
            1 -> do
                aadPrefix <- readByteString buf pos
                readAesGcmV1 (v{aadPrefix = aadPrefix}) buf pos lastFieldId fieldStack
            2 -> do
                aadFileUnique <- readByteString buf pos
                readAesGcmV1 (v{aadFileUnique = aadFileUnique}) buf pos lastFieldId fieldStack
            3 -> do
                supplyAadPrefix <- readAndAdvance pos buf
                readAesGcmV1
                    (v{supplyAadPrefix = supplyAadPrefix == compactBooleanTrue})
                    buf
                    pos
                    lastFieldId
                    fieldStack
            _ -> return ENCRYPTION_ALGORITHM_UNKNOWN
readAesGcmV1 _ _ _ _ _ =
    error "readAesGcmV1 called with non AesGcmV1"

readTypeOrder ::
    BS.ByteString -> IORef Int -> Int16 -> [Int16] -> IO ColumnOrder
readTypeOrder buf pos lastFieldId fieldStack = do
    fieldContents <- readField buf pos lastFieldId fieldStack
    case fieldContents of
        Nothing -> return TYPE_ORDER
        Just (elemType, identifier) ->
            if elemType == STOP
                then return TYPE_ORDER
                else readTypeOrder buf pos identifier fieldStack

readKeyValue ::
    KeyValue -> BS.ByteString -> IORef Int -> Int16 -> [Int16] -> IO KeyValue
readKeyValue kv buf pos lastFieldId fieldStack = do
    fieldContents <- readField buf pos lastFieldId fieldStack
    case fieldContents of
        Nothing -> return kv
        Just (elemType, identifier) -> case identifier of
            1 -> do
                k <- readString buf pos
                readKeyValue (kv{key = k}) buf pos identifier fieldStack
            2 -> do
                v <- readString buf pos
                readKeyValue (kv{value = v}) buf pos identifier fieldStack
            _ -> return kv

readPageEncodingStats ::
    PageEncodingStats ->
    BS.ByteString ->
    IORef Int ->
    Int16 ->
    [Int16] ->
    IO PageEncodingStats
readPageEncodingStats pes buf pos lastFieldId fieldStack = do
    fieldContents <- readField buf pos lastFieldId fieldStack
    case fieldContents of
        Nothing -> return pes
        Just (elemType, identifier) -> case identifier of
            1 -> do
                pType <- pageTypeFromInt <$> readInt32FromBuffer buf pos
                readPageEncodingStats (pes{pageEncodingPageType = pType}) buf pos identifier []
            2 -> do
                pEnc <- parquetEncodingFromInt <$> readInt32FromBuffer buf pos
                readPageEncodingStats (pes{pageEncoding = pEnc}) buf pos identifier []
            3 -> do
                encodedCount <- readInt32FromBuffer buf pos
                readPageEncodingStats
                    (pes{pagesWithEncoding = encodedCount})
                    buf
                    pos
                    identifier
                    []
            _ -> pure pes

readParquetEncoding ::
    BS.ByteString -> IORef Int -> Int16 -> [Int16] -> IO ParquetEncoding
readParquetEncoding buf pos lastFieldId fieldStack = parquetEncodingFromInt <$> readInt32FromBuffer buf pos

readStatistics ::
    ColumnStatistics ->
    BS.ByteString ->
    IORef Int ->
    Int16 ->
    [Int16] ->
    IO ColumnStatistics
readStatistics cs buf pos lastFieldId fieldStack = do
    fieldContents <- readField buf pos lastFieldId fieldStack
    case fieldContents of
        Nothing -> return cs
        Just (elemType, identifier) -> case identifier of
            1 -> do
                maxInBytes <- readByteString buf pos
                readStatistics (cs{columnMax = maxInBytes}) buf pos identifier fieldStack
            2 -> do
                minInBytes <- readByteString buf pos
                readStatistics (cs{columnMin = minInBytes}) buf pos identifier fieldStack
            3 -> do
                nullCount <- readIntFromBuffer @Int64 buf pos
                readStatistics (cs{columnNullCount = nullCount}) buf pos identifier fieldStack
            4 -> do
                distinctCount <- readIntFromBuffer @Int64 buf pos
                readStatistics
                    (cs{columnDistictCount = distinctCount})
                    buf
                    pos
                    identifier
                    fieldStack
            5 -> do
                maxInBytes <- readByteString buf pos
                readStatistics (cs{columnMaxValue = maxInBytes}) buf pos identifier fieldStack
            6 -> do
                minInBytes <- readByteString buf pos
                readStatistics (cs{columnMinValue = minInBytes}) buf pos identifier fieldStack
            7 -> do
                isMaxValueExact <- readAndAdvance pos buf
                readStatistics
                    (cs{isColumnMaxValueExact = isMaxValueExact == compactBooleanTrue})
                    buf
                    pos
                    identifier
                    fieldStack
            8 -> do
                isMinValueExact <- readAndAdvance pos buf
                readStatistics
                    (cs{isColumnMinValueExact = isMinValueExact == compactBooleanTrue})
                    buf
                    pos
                    identifier
                    fieldStack
            _ -> pure cs

readSizeStatistics ::
    SizeStatistics ->
    BS.ByteString ->
    IORef Int ->
    Int16 ->
    [Int16] ->
    IO SizeStatistics
readSizeStatistics ss buf pos lastFieldId fieldStack = do
    fieldContents <- readField buf pos lastFieldId fieldStack
    case fieldContents of
        Nothing -> return ss
        Just (elemType, identifier) -> case identifier of
            1 -> do
                unencodedByteArrayDataTypes <- readIntFromBuffer @Int64 buf pos
                readSizeStatistics
                    (ss{unencodedByteArrayDataTypes = unencodedByteArrayDataTypes})
                    buf
                    pos
                    identifier
                    fieldStack
            2 -> do
                sizeAndType <- readAndAdvance pos buf
                let sizeOnly = fromIntegral ((sizeAndType `shiftR` 4) .&. 0x0f) :: Int
                let _elemType = toTType sizeAndType
                repetitionLevelHistogram <-
                    replicateM sizeOnly (readIntFromBuffer @Int64 buf pos)
                readSizeStatistics
                    (ss{repetitionLevelHistogram = repetitionLevelHistogram})
                    buf
                    pos
                    identifier
                    fieldStack
            3 -> do
                sizeAndType <- readAndAdvance pos buf
                let sizeOnly = fromIntegral ((sizeAndType `shiftR` 4) .&. 0x0f) :: Int
                let _elemType = toTType sizeAndType
                definitionLevelHistogram <-
                    replicateM sizeOnly (readIntFromBuffer @Int64 buf pos)
                readSizeStatistics
                    (ss{definitionLevelHistogram = definitionLevelHistogram})
                    buf
                    pos
                    identifier
                    fieldStack
            _ -> pure ss

footerSize :: Int
footerSize = 8

toIntegralType :: Int32 -> TType
toIntegralType n
    | n == 1 = I32
    | n == 2 = I64
    | n == 4 = DOUBLE
    | n == 6 = STRING
    | otherwise = STRING

readLogicalType ::
    BS.ByteString -> IORef Int -> Int16 -> [Int16] -> IO LogicalType
readLogicalType buf pos lastFieldId fieldStack = do
    t <- readAndAdvance pos buf
    if t .&. 0x0f == 0
        then return LOGICAL_TYPE_UNKNOWN
        else do
            let modifier = fromIntegral ((t .&. 0xf0) `shiftR` 4) :: Int16
            identifier <-
                if modifier == 0
                    then readIntFromBuffer @Int16 buf pos
                    else return (lastFieldId + modifier)
            let _elemType = toTType (t .&. 0x0f)
            case identifier of
                1 -> do
                    replicateM_ 2 (readField buf pos 0 [])
                    return STRING_TYPE
                2 -> do
                    replicateM_ 2 (readField buf pos 0 [])
                    return MAP_TYPE
                3 -> do
                    replicateM_ 2 (readField buf pos 0 [])
                    return LIST_TYPE
                4 -> do
                    replicateM_ 2 (readField buf pos 0 [])
                    return ENUM_TYPE
                5 -> do
                    readDecimalType 0 0 buf pos 0 []
                6 -> do
                    replicateM_ 2 (readField buf pos 0 [])
                    return DATE_TYPE
                7 -> do
                    readTimeType False MILLISECONDS buf pos 0 []
                8 -> do
                    readTimestampType False MILLISECONDS buf pos 0 []
                -- Apparently reserved for interval types
                9 -> return LOGICAL_TYPE_UNKNOWN
                10 -> do
                    intType <- readIntType 0 False buf pos 0 []
                    _ <- readField buf pos 0 []
                    pure intType
                11 -> do
                    replicateM_ 2 (readField buf pos 0 [])
                    return LOGICAL_TYPE_UNKNOWN
                12 -> do
                    replicateM_ 2 (readField buf pos 0 [])
                    return JSON_TYPE
                13 -> do
                    replicateM_ 2 (readField buf pos 0 [])
                    return BSON_TYPE
                14 -> do
                    replicateM_ 2 (readField buf pos 0 [])
                    return UUID_TYPE
                15 -> do
                    replicateM_ 2 (readField buf pos 0 [])
                    return FLOAT16_TYPE
                16 -> do
                    return VariantType{specificationVersion = 1}
                17 -> do
                    return GeometryType{crs = ""}
                18 -> do
                    return GeographyType{crs = "", algorithm = SPHERICAL}
                _ -> return LOGICAL_TYPE_UNKNOWN

readIntType ::
    Int8 ->
    Bool ->
    BS.ByteString ->
    IORef Int ->
    Int16 ->
    [Int16] ->
    IO LogicalType
readIntType bitWidth intIsSigned buf pos lastFieldId fieldStack = do
    t <- readAndAdvance pos buf
    if t .&. 0x0f == 0
        then return (IntType bitWidth intIsSigned)
        else do
            let modifier = fromIntegral ((t .&. 0xf0) `shiftR` 4) :: Int16
            identifier <-
                if modifier == 0
                    then readIntFromBuffer @Int16 buf pos
                    else return (lastFieldId + modifier)

            case identifier of
                1 -> do
                    bitWidth' <- readAndAdvance pos buf
                    readIntType (fromIntegral bitWidth') intIsSigned buf pos identifier fieldStack
                2 -> do
                    let intIsSigned' = (t .&. 0x0f) == compactBooleanTrue
                    readIntType bitWidth intIsSigned' buf pos identifier fieldStack
                _ -> error $ "UNKNOWN field ID for IntType: " ++ show identifier

readDecimalType ::
    Int32 ->
    Int32 ->
    BS.ByteString ->
    IORef Int ->
    Int16 ->
    [Int16] ->
    IO LogicalType
readDecimalType precision scale buf pos lastFieldId fieldStack = do
    fieldContents <- readField buf pos lastFieldId fieldStack
    case fieldContents of
        Nothing -> return (DecimalType precision scale)
        Just (elemType, identifier) -> case identifier of
            1 -> do
                scale' <- readInt32FromBuffer buf pos
                readDecimalType precision scale' buf pos lastFieldId fieldStack
            2 -> do
                precision' <- readInt32FromBuffer buf pos
                readDecimalType precision' scale buf pos lastFieldId fieldStack
            _ -> error $ "UNKNOWN field ID for DecimalType" ++ show identifier

readTimeType ::
    Bool ->
    TimeUnit ->
    BS.ByteString ->
    IORef Int ->
    Int16 ->
    [Int16] ->
    IO LogicalType
readTimeType isAdjustedToUTC unit buf pos lastFieldId fieldStack = do
    fieldContents <- readField buf pos lastFieldId fieldStack
    case fieldContents of
        Nothing -> return (TimeType isAdjustedToUTC unit)
        Just (elemType, identifier) -> case identifier of
            1 -> do
                -- TODO: Check for empty
                isAdjustedToUTC' <- (== compactBooleanTrue) <$> readAndAdvance pos buf
                readTimeType isAdjustedToUTC' unit buf pos lastFieldId fieldStack
            2 -> do
                unit' <- readUnit buf pos 0 []
                readTimeType isAdjustedToUTC unit' buf pos lastFieldId fieldStack
            _ -> error $ "UNKNOWN field ID for TimeType" ++ show identifier

readTimestampType ::
    Bool ->
    TimeUnit ->
    BS.ByteString ->
    IORef Int ->
    Int16 ->
    [Int16] ->
    IO LogicalType
readTimestampType isAdjustedToUTC unit buf pos lastFieldId fieldStack = do
    fieldContents <- readField buf pos lastFieldId fieldStack
    case fieldContents of
        Nothing -> return (TimestampType isAdjustedToUTC unit)
        Just (elemType, identifier) -> case identifier of
            1 -> do
                -- TODO: Check for empty
                isAdjustedToUTC' <- (== compactBooleanTrue) <$> readAndAdvance pos buf
                readTimestampType isAdjustedToUTC' unit buf pos lastFieldId fieldStack
            2 -> do
                unit' <- readUnit buf pos 0 []
                readTimestampType isAdjustedToUTC unit' buf pos lastFieldId fieldStack
            _ -> error $ "UNKNOWN field ID for TimestampType" ++ show identifier

readUnit :: BS.ByteString -> IORef Int -> Int16 -> [Int16] -> IO TimeUnit
readUnit buf pos lastFieldId fieldStack = do
    fieldContents <- readField buf pos lastFieldId fieldStack
    case fieldContents of
        Nothing -> return TIME_UNIT_UNKNOWN
        Just (elemType, identifier) -> case identifier of
            1 -> do
                _ <- readField buf pos 0 []
                return MILLISECONDS
            2 -> do
                _ <- readField buf pos 0 []
                return MICROSECONDS
            3 -> do
                _ <- readField buf pos 0 []
                return NANOSECONDS
            _ -> return TIME_UNIT_UNKNOWN
