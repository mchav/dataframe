{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE RecordWildCards #-}

module DataFrame.IO.Parquet where

import Codec.Compression.Zstd.Streaming
import Control.Monad
import qualified Data.ByteString as BSO
import qualified Data.ByteString.Char8 as BS
import Data.Char
import Data.Foldable
import Data.IORef
import qualified Data.Map as M
import Data.Maybe
import qualified Data.Text as T
import DataFrame.Internal.DataFrame (DataFrame)
import qualified DataFrame.Internal.DataFrame as DI
import Foreign
import GHC.IO (unsafePerformIO)
import System.IO

footerSize :: Integer
footerSize = 8

data ParquetType
  = PBOOLEAN
  | PINT32
  | PINT64
  | PINT96
  | PFLOAT
  | PDOUBLE
  | PBYTE_ARRAY
  | PFIXED_LEN_BYTE_ARRAY
  | PARQUET_TYPE_UNKNOWN
  deriving (Show, Eq)

parquetTypeFromInt :: Int32 -> ParquetType
parquetTypeFromInt 0 = PBOOLEAN
parquetTypeFromInt 1 = PINT32
parquetTypeFromInt 2 = PINT64
parquetTypeFromInt 3 = PINT96
parquetTypeFromInt 4 = PFLOAT
parquetTypeFromInt 5 = PDOUBLE
parquetTypeFromInt 6 = PBYTE_ARRAY
parquetTypeFromInt 7 = PFIXED_LEN_BYTE_ARRAY
parquetTypeFromInt _ = PARQUET_TYPE_UNKNOWN

data ParquetEncoding
  = EPLAIN
  | EPLAIN_DICTIONARY
  | ERLE
  | EBIT_PACKED
  | EDELTA_BINARY_PACKED
  | EDELTA_LENGTH_BYTE_ARRAY
  | EDELTA_BYTE_ARRAY
  | ERLE_DICTIONARY
  | EBYTE_STREAM_SPLIT
  | PARQUET_ENCODING_UNKNOWN
  deriving (Show, Eq)

parquetEncodingFromInt :: Int32 -> ParquetEncoding
parquetEncodingFromInt 0 = EPLAIN
parquetEncodingFromInt 2 = EPLAIN_DICTIONARY
parquetEncodingFromInt 3 = ERLE
parquetEncodingFromInt 4 = EBIT_PACKED
parquetEncodingFromInt 5 = EDELTA_BINARY_PACKED
parquetEncodingFromInt 6 = EDELTA_LENGTH_BYTE_ARRAY
parquetEncodingFromInt 7 = EDELTA_BYTE_ARRAY
parquetEncodingFromInt 8 = ERLE_DICTIONARY
parquetEncodingFromInt 9 = EBYTE_STREAM_SPLIT
parquetEncodingFromInt _ = PARQUET_ENCODING_UNKNOWN

data CompressionCodec
  = UNCOMPRESSED
  | SNAPPY
  | GZIP
  | LZO
  | BROTLI
  | LZ4
  | ZSTD
  | LZ4_RAW
  | COMPRESSION_CODEC_UNKNOWN
  deriving (Show, Eq)

compressionCodecFromInt :: Int32 -> CompressionCodec
compressionCodecFromInt 0 = UNCOMPRESSED
compressionCodecFromInt 1 = SNAPPY
compressionCodecFromInt 2 = GZIP
compressionCodecFromInt 3 = LZO
compressionCodecFromInt 4 = BROTLI
compressionCodecFromInt 5 = LZ4
compressionCodecFromInt 6 = ZSTD
compressionCodecFromInt 7 = LZ4_RAW
compressionCodecFromInt _ = COMPRESSION_CODEC_UNKNOWN

data ColumnStatistics = ColumnStatistics
  { columnMin :: [Word8],
    columnMax :: [Word8],
    columnNullCount :: Int64,
    columnDistictCount :: Int64,
    columnMinValue :: [Word8],
    columnMaxValue :: [Word8],
    isColumnMaxValueExact :: Bool,
    isColumnMinValueExact :: Bool
  }
  deriving (Show, Eq)

emptyColumnStatistics :: ColumnStatistics
emptyColumnStatistics = ColumnStatistics [] [] 0 0 [] [] False False

data PageType
  = DATA_PAGE
  | INDEX_PAGE
  | DICTIONARY_PAGE
  | DATA_PAGE_V2
  | PAGE_TYPE_UNKNOWN
  deriving (Show, Eq)

pageTypeFromInt :: Int32 -> PageType
pageTypeFromInt 0 = DATA_PAGE
pageTypeFromInt 1 = INDEX_PAGE
pageTypeFromInt 2 = DICTIONARY_PAGE
pageTypeFromInt 3 = DATA_PAGE_V2
pageTypeFromInt _ = PAGE_TYPE_UNKNOWN

data PageEncodingStats = PageEncodingStats
  { pageEncodingPageType :: PageType,
    pageEncoding :: ParquetEncoding,
    pagesWithEncoding :: Int32
  }
  deriving (Show, Eq)

emptyPageEncodingStats :: PageEncodingStats
emptyPageEncodingStats = PageEncodingStats PAGE_TYPE_UNKNOWN PARQUET_ENCODING_UNKNOWN 0

data SizeStatistics = SizeStatisics
  { unencodedByteArrayDataTypes :: Int64,
    repetitionLevelHistogram :: [Int64],
    definitionLevelHistogram :: [Int64]
  }
  deriving (Show, Eq)

emptySizeStatistics :: SizeStatistics
emptySizeStatistics = SizeStatisics 0 [] []

data BoundingBox = BoundingBox
  { xmin :: Double,
    xmax :: Double,
    ymin :: Double,
    ymax :: Double,
    zmin :: Double,
    zmax :: Double,
    mmin :: Double,
    mmax :: Double
  }
  deriving (Show, Eq)

emptyBoundingBox :: BoundingBox
emptyBoundingBox = BoundingBox 0 0 0 0 0 0 0 0

data GeospatialStatistics = GeospatialStatistics
  { bbox :: BoundingBox,
    geospatialTypes :: [Int32]
  }
  deriving (Show, Eq)

emptyGeospatialStatistics :: GeospatialStatistics
emptyGeospatialStatistics = GeospatialStatistics emptyBoundingBox []

emptyKeyValue :: KeyValue
emptyKeyValue = KeyValue {key = "", value = ""}

data ColumnMetaData = ColumnMetaData
  { columnType :: ParquetType,
    columnEncodings :: [ParquetEncoding],
    columnPathInSchema :: [String],
    columnCodec :: CompressionCodec,
    columnNumValues :: Int64,
    columnTotalUncompressedSize :: Int64,
    columnTotalCompressedSize :: Int64,
    columnKeyValueMetadata :: [KeyValue],
    columnDataPageOffset :: Int64,
    columnIndexPageOffset :: Int64,
    columnDictionaryPageOffset :: Int64,
    columnStatistics :: ColumnStatistics,
    columnEncodingStats :: [PageEncodingStats],
    bloomFilterOffset :: Int64,
    bloomFilterLength :: Int32,
    columnSizeStatistics :: SizeStatistics,
    columnGeospatialStatistics :: GeospatialStatistics
  }
  deriving (Show, Eq)

emptyColumnMetadata :: ColumnMetaData
emptyColumnMetadata = ColumnMetaData PARQUET_TYPE_UNKNOWN [] [] COMPRESSION_CODEC_UNKNOWN 0 0 0 [] 0 0 0 emptyColumnStatistics [] 0 0 emptySizeStatistics emptyGeospatialStatistics

data ColumnCryptoMetadata
  = COLUMN_CRYPTO_METADATA_UNKNOWN
  | ENCRYPTION_WITH_FOOTER_KEY
  | EncryptionWithColumnKey
      { columnCryptPathInSchema :: [String],
        columnKeyMetadata :: [Word8]
      }
  deriving (Show, Eq)

data ColumnChunk = ColumnChunk
  { columnChunkFilePath :: String,
    columnChunkMetadataFileOffset :: Int64,
    columnMetaData :: ColumnMetaData,
    columnChunkOffsetIndexOffset :: Int64,
    columnChunkOffsetIndexLength :: Int32,
    columnChunkColumnIndexOffset :: Int64,
    columnChunkColumnIndexLength :: Int32,
    cryptoMetadata :: ColumnCryptoMetadata,
    encryptedColumnMetadata :: [Word8]
  }
  deriving (Show, Eq)

emptyColumnChunk :: ColumnChunk
emptyColumnChunk = ColumnChunk "" 0 emptyColumnMetadata 0 0 0 0 COLUMN_CRYPTO_METADATA_UNKNOWN []

data SortingColumn = SortingColumn
  { columnIndex :: Int32,
    columnOrderDescending :: Bool,
    nullFirst :: Bool
  }
  deriving (Show, Eq)

emptySortingColumn :: SortingColumn
emptySortingColumn = SortingColumn 0 False False

data RowGroup = RowGroup
  { rowGroupColumns :: [ColumnChunk],
    totalByteSize :: Int64,
    rowGroupNumRows :: Int64,
    rowGroupSortingColumns :: [SortingColumn],
    fileOffset :: Int64,
    totalCompressedSize :: Int64,
    ordinal :: Int16
  }
  deriving (Show, Eq)

emptyRowGroup :: RowGroup
emptyRowGroup = RowGroup [] 0 0 [] 0 0 0

data ColumnOrder
  = TYPE_ORDER
  | COLUMN_ORDER_UNKNOWN
  deriving (Show, Eq)

data EncryptionAlgorithm
  = ENCRYPTION_ALGORITHM_UNKNOWN
  | AesGcmV1
      { aadPrefix :: [Word8],
        aadFileUnique :: [Word8],
        supplyAadPrefix :: Bool
      }
  | AesGcmCtrV1
      { aadPrefix :: [Word8],
        aadFileUnique :: [Word8],
        supplyAadPrefix :: Bool
      }
  deriving (Show, Eq)

data KeyValue = KeyValue
  { key :: String,
    value :: String
  }
  deriving (Show, Eq)

data FileMetadata = FileMetaData
  { version :: Int32,
    schema :: [SchemaElement],
    numRows :: Integer,
    rowGroups :: [RowGroup],
    keyValueMetadata :: [KeyValue],
    createdBy :: Maybe String,
    columnOrders :: [ColumnOrder],
    encryptionAlgorithm :: EncryptionAlgorithm,
    footerSigningKeyMetadata :: [Word8]
  }
  deriving (Show, Eq)

defaultMetadata :: FileMetadata
defaultMetadata =
  FileMetaData
    { version = 0,
      schema = [],
      numRows = 0,
      rowGroups = [],
      keyValueMetadata = [],
      createdBy = Nothing,
      columnOrders = [],
      encryptionAlgorithm = ENCRYPTION_ALGORITHM_UNKNOWN,
      footerSigningKeyMetadata = []
    }

readParquet :: String -> IO DataFrame
readParquet path = withBinaryFile path ReadMode $ \handle -> do
  (size, magicString) <- readMetadataSizeFromFooter handle
  when (magicString /= "PAR1") $ error "Invalid Parquet file"

  metadata <- readMetadata handle size
  -- print metadata
  forM_ (rowGroups metadata) $ \r -> do
    forM_ (rowGroupColumns r) $ \c -> do
      let metadata = columnMetaData c
      let colDataPageOffset = columnDataPageOffset metadata
      let colDictionaryPageOffset = columnDictionaryPageOffset metadata
      let colStart = if colDictionaryPageOffset > 0 && colDataPageOffset > colDictionaryPageOffset
                     then colDictionaryPageOffset
                     else colDataPageOffset
      let colLength = columnTotalCompressedSize metadata
      columnBytes <-readBytes handle colStart colLength
      print metadata
      (maybePage, res) <- readPage columnBytes
      case maybePage of
        Just p -> if isDictionaryPage p
                  then do
                    (maybePage', res') <- readPage res
                    let p' = fromMaybe (error "[UNEXPECTED] No data page found") maybePage'
                    print $ readPageBytes (pageBytes p)
                    print p'
                  else print "Data???"
        Nothing -> pure ()

  return DI.empty

readPageBytes :: [Word8] -> [BSO.ByteString]
readPageBytes [] = []
readPageBytes xs = let
    lenBytes = fromIntegral (littleEndianWord8 $ take 4 xs)
    totalBytesRead = lenBytes + 4
  in BSO.pack (take lenBytes (drop 4 xs)) : readPageBytes (drop totalBytesRead xs)

readPage :: [Word8] -> IO (Maybe Page, [Word8])
readPage [] = pure (Nothing, [])
readPage columnBytes = do
  let (hdr, rem) = readPageHeader emptyPageHeader columnBytes 0
  print hdr
  let compressed = take (fromIntegral $ compressedPageSize hdr) rem
  
  -- Weird round about way to uncompress zstd files compressed using the
  -- streaming API
  Consume dFunc <- decompress
  Consume dFunc' <- dFunc (BSO.pack compressed)
  Done res <- dFunc' BSO.empty
  pure $ (Just $ Page hdr (BSO.unpack res), drop (fromIntegral $ compressedPageSize hdr) rem)

data Page = Page { pageHeader :: PageHeader
                 , pageBytes :: [Word8] } deriving (Show, Eq)

data PageHeader = PageHeader { pageHeaderPageType :: PageType
                             , uncompressedPageSize :: Int32
                             , compressedPageSize ::Int32
                             , pageHeaderCrcChecksum :: Int32
                             , pageTypeHeader :: PageTypeHeader
                             } deriving (Show, Eq)


emptyPageHeader = PageHeader PAGE_TYPE_UNKNOWN 0 0 0  PAGE_TYPE_HEADER_UNKNOWN

isDataPage :: Page -> Bool
isDataPage page = case pageTypeHeader (pageHeader page) of
                    DataPageHeader   {..} -> True
                    DataPageHeaderV2 {..} -> True
                    _                     -> False

isDictionaryPage :: Page -> Bool
isDictionaryPage page = case pageTypeHeader (pageHeader page) of
                          DictionaryPageHeader {..} -> True
                          _                         -> False

data PageTypeHeader = DataPageHeader { dataPageHeaderNumValues :: Int32
                                     , dataPageHeaderEncoding :: ParquetEncoding
                                     , definitionLevelEncoding :: ParquetEncoding
                                     , repetitionLevelEncoding :: ParquetEncoding
                                     , dataPageHeaderStatistics :: ColumnStatistics
                                     }
                    | DataPageHeaderV2 { dataPageHeaderV2NumValues :: Int32
                                         , dataPageHeaderV2NumNulls :: Int32
                                         , dataPageHeaderV2NumRows :: Int32
                                         , dataPageHeaderV2Encoding :: ParquetEncoding
                                         , definitionLevelByteLength :: Int32
                                         , repetitionLevelByteLength :: Int32
                                         , dataPageHeaderV2IsCompressed :: Bool
                                         , dataPageHeaderV2Statistics :: ColumnStatistics
                                         }
                    | DictionaryPageHeader { dictionaryPageHeaderNumValues :: Int32
                                            , dictionaryPageHeaderEncoding :: ParquetEncoding
                                            , dictionaryPageIsSorted :: Bool 
                                            }
                    | INDEX_PAGE_HEADER
                    | PAGE_TYPE_HEADER_UNKNOWN deriving (Show, Eq)

emptyDictionaryPageHeader = DictionaryPageHeader 0 PARQUET_ENCODING_UNKNOWN False
emptyDataPageHeader = DataPageHeader 0 PARQUET_ENCODING_UNKNOWN PARQUET_ENCODING_UNKNOWN PARQUET_ENCODING_UNKNOWN emptyColumnStatistics
emptyDataPageHeaderV2 = DataPageHeaderV2 0 0 0 PARQUET_ENCODING_UNKNOWN 0 0 False emptyColumnStatistics 

readPageHeader :: PageHeader -> [Word8] -> Int16 -> (PageHeader, [Word8])
readPageHeader hdr [] _ = (hdr, [])
readPageHeader hdr xs lastFieldId = let
    fieldContents = readField' xs lastFieldId
  in case fieldContents of
      Nothing -> (hdr, tail xs)
      Just (rem, elemType, identifier) -> case identifier of
        1 -> let
            (pType, rem') = readInt32FromBytes rem
          in readPageHeader (hdr {pageHeaderPageType = pageTypeFromInt pType}) rem' identifier
        2 -> let
            (uncompressedPageSize, rem') = readInt32FromBytes rem
          in readPageHeader (hdr {uncompressedPageSize = uncompressedPageSize}) rem' identifier
        3 -> let
            (compressedPageSize, rem') = readInt32FromBytes rem
          in readPageHeader (hdr {compressedPageSize = compressedPageSize}) rem' identifier
        5 -> let
            (dataPageHeader, rem') = readPageTypeHeader emptyDataPageHeader rem 0
          in readPageHeader (hdr {pageTypeHeader = dataPageHeader}) rem' identifier
        7 -> let
            (dictionaryPageHeader, rem') = readPageTypeHeader emptyDictionaryPageHeader rem 0
          in readPageHeader (hdr {pageTypeHeader = dictionaryPageHeader}) rem' identifier
        n -> error $ show n

readPageTypeHeader :: PageTypeHeader -> [Word8] -> Int16 -> (PageTypeHeader, [Word8])
readPageTypeHeader hdr [] _ = (hdr, [])
readPageTypeHeader hdr@(DictionaryPageHeader {..}) xs lastFieldId = let
    fieldContents = readField' xs lastFieldId
  in case fieldContents of
      Nothing -> (hdr, tail xs)
      Just (rem, elemType, identifier) -> case identifier of
        1 -> let
            (numValues, rem') = readInt32FromBytes rem
          in readPageTypeHeader (hdr {dictionaryPageHeaderNumValues = numValues}) rem' identifier
        2 -> let
            (enc, rem') = readInt32FromBytes rem
          in readPageTypeHeader (hdr {dictionaryPageHeaderEncoding = parquetEncodingFromInt enc}) rem' identifier
        3 -> let
            (isSorted: rem') = rem
          in readPageTypeHeader (hdr {dictionaryPageIsSorted = isSorted == compactBooleanTrue}) rem' identifier
        n -> error $ show n
readPageTypeHeader hdr@(DataPageHeader {..}) xs lastFieldId = let
    fieldContents = readField' xs lastFieldId
  in case fieldContents of
      Nothing -> (hdr, tail xs)
      Just (rem, elemType, identifier) -> case identifier of
        1 -> let
            (numValues, rem') = readInt32FromBytes rem
          in readPageTypeHeader (hdr {dataPageHeaderNumValues = numValues}) rem' identifier
        2 -> let
            (enc, rem') = readInt32FromBytes rem
          in readPageTypeHeader (hdr {dataPageHeaderEncoding = parquetEncodingFromInt enc}) rem' identifier
        3 -> let
            (enc, rem') = readInt32FromBytes rem
          in readPageTypeHeader (hdr {definitionLevelEncoding = parquetEncodingFromInt enc}) rem' identifier
        4 -> let
            (enc, rem') = readInt32FromBytes rem
          in readPageTypeHeader (hdr {repetitionLevelEncoding = parquetEncodingFromInt enc}) rem' identifier
        5 -> let
            (stats, rem') = readStatisticsFromBytes emptyColumnStatistics rem 0
          in readPageTypeHeader (hdr {dataPageHeaderStatistics = stats}) rem' identifier
        n -> error $ show n

readStatisticsFromBytes :: ColumnStatistics -> [Word8] -> Int16 -> (ColumnStatistics, [Word8])
readStatisticsFromBytes cs xs lastFieldId = let
    fieldContents = readField' xs lastFieldId
  in case fieldContents of
      Nothing -> (cs, tail xs)
      Just (rem, elemType, identifier) -> case identifier of
        1 -> let
            (maxInBytes, rem') = readByteStringFromBytes rem
          in readStatisticsFromBytes (cs {columnMax = maxInBytes}) rem' identifier
        2 -> let
            (minInBytes, rem') = readByteStringFromBytes rem
          in readStatisticsFromBytes (cs {columnMin = minInBytes}) rem' identifier
        3 -> let
            (nullCount, rem') = readIntFromBytes @Int64 rem
          in readStatisticsFromBytes (cs {columnNullCount = nullCount}) rem' identifier
        4 -> let
            (distinctCount, rem') = readIntFromBytes @Int64 rem
          in readStatisticsFromBytes (cs {columnDistictCount = distinctCount}) rem' identifier
        5 -> let
            (maxInBytes, rem') = readByteStringFromBytes rem
          in readStatisticsFromBytes (cs {columnMaxValue = maxInBytes}) rem' identifier
        6 -> let
            (minInBytes, rem') = readByteStringFromBytes rem
          in readStatisticsFromBytes (cs {columnMinValue = minInBytes}) rem' identifier
        7 -> let
            (isMaxValueExact: rem') = rem
          in readStatisticsFromBytes (cs {isColumnMaxValueExact = isMaxValueExact == compactBooleanTrue}) rem' identifier
        8 -> let
            (isMinValueExact: rem') = rem
          in readStatisticsFromBytes (cs {isColumnMinValueExact = isMinValueExact == compactBooleanTrue}) rem' identifier
        n -> error $ show n

readBytes :: Handle -> Int64 -> Int64 -> IO [Word8]
readBytes handle colStart colLen = do
  buf <- mallocBytes (fromIntegral colLen) :: IO (Ptr Word8)
  hSeek handle AbsoluteSeek (fromIntegral colStart)
  _ <- hGetBuf handle buf (fromIntegral colLen) 
  columnBytes <- readByteString' buf colLen
  free buf
  pure columnBytes

numBytesInFile :: Handle -> IO Integer
numBytesInFile handle = do
  hSeek handle SeekFromEnd 0
  hTell handle

readMetadataSizeFromFooter :: Handle -> IO (Integer, BS.ByteString)
readMetadataSizeFromFooter handle = do
  footerOffSet <- numBytesInFile handle

  buf <- mallocBytes (fromIntegral footerSize) :: IO (Ptr Word8)

  hSeek handle AbsoluteSeek (fromIntegral $! footerOffSet - footerSize)
  n <- hGetBuf handle buf (fromIntegral footerSize)

  -- The bytes that store the metadata size.
  sizeBytes <- mapM (\i -> fromIntegral <$> (peekElemOff buf i :: IO Word8) :: IO Int32) [0 .. 3]
  let size = fromIntegral $ foldl' (.|.) 0 $! zipWith shift sizeBytes [0, 8, 16, 24]

  magicStringBytes <- mapM (\i -> peekElemOff buf i :: IO Word8) [4 .. 7]
  let magicString = BSO.pack magicStringBytes
  free buf
  return (size, magicString)

readMetadata :: Handle -> Integer -> IO FileMetadata
readMetadata handle size = do
  metaDataBuf <- mallocBytes (fromIntegral size) :: IO (Ptr Word8)
  footerOffSet <- numBytesInFile handle

  hSeek handle AbsoluteSeek (fromIntegral $! footerOffSet - footerSize - size)

  metadataBytesRead <- hGetBuf handle metaDataBuf (fromIntegral size)
  let lastFieldId = 0
  let fieldStack = []
  bufferPos <- newIORef (0 :: Int)
  metadata <- readFileMetaData defaultMetadata metaDataBuf bufferPos lastFieldId fieldStack
  free metaDataBuf
  return metadata

readFileMetaData :: FileMetadata -> Ptr Word8 -> IORef Int -> Int16 -> [Int16] -> IO FileMetadata
readFileMetaData metadata metaDataBuf bufferPos lastFieldId fieldStack = do
  fieldContents <- readField metaDataBuf bufferPos lastFieldId fieldStack
  case fieldContents of
    Nothing -> return metadata
    Just (elemType, identifier) -> case identifier of
      1 -> do
        version <- readIntFromBuffer @Int32 metaDataBuf bufferPos
        readFileMetaData (metadata {version = version}) metaDataBuf bufferPos identifier fieldStack
      2 -> do
        -- We can do some type checking/exception handling here.
        -- Check elemType == List
        sizeAndType <- readAndAdvance bufferPos metaDataBuf
        let sizeOnly = fromIntegral ((sizeAndType `shiftR` 4) .&. 0x0f) :: Int
        -- type of the contents of the list.
        let elemType = toTType sizeAndType
        schemaElements <- replicateM sizeOnly (readSchemaElement defaultSchemaElement metaDataBuf bufferPos 0 [])
        readFileMetaData (metadata {schema = schemaElements}) metaDataBuf bufferPos identifier fieldStack
      3 -> do
        numRows <- readIntFromBuffer @Int64 metaDataBuf bufferPos
        readFileMetaData (metadata {numRows = fromIntegral numRows}) metaDataBuf bufferPos identifier fieldStack
      4 -> do
        -- We can do some type checking/exception handling here.
        -- Check elemType == List
        sizeAndType <- readAndAdvance bufferPos metaDataBuf
        let sizeOnly = fromIntegral ((sizeAndType `shiftR` 4) .&. 0x0f) :: Int
        -- type of the contents of the list.
        let elemType = toTType sizeAndType
        rowGroups <- replicateM sizeOnly (readRowGroup emptyRowGroup metaDataBuf bufferPos 0 [])
        readFileMetaData (metadata {rowGroups = rowGroups}) metaDataBuf bufferPos identifier fieldStack
      5 -> do
        -- We can do some type checking/exception handling here.
        -- Check elemType == List
        sizeAndType <- readAndAdvance bufferPos metaDataBuf
        let sizeOnly = fromIntegral ((sizeAndType `shiftR` 4) .&. 0x0f) :: Int
        -- type of the contents of the list.
        let elemType = toTType sizeAndType
        keyValueMetadata <- replicateM sizeOnly (readKeyValue emptyKeyValue metaDataBuf bufferPos 0 [])
        readFileMetaData (metadata {keyValueMetadata = keyValueMetadata}) metaDataBuf bufferPos identifier fieldStack
      6 -> do
        createdBy <- readString metaDataBuf bufferPos
        readFileMetaData (metadata {createdBy = Just createdBy}) metaDataBuf bufferPos identifier fieldStack
      7 -> do
        sizeAndType <- readAndAdvance bufferPos metaDataBuf
        let sizeOnly = fromIntegral ((sizeAndType `shiftR` 4) .&. 0x0f) :: Int
        let elemType = toTType sizeAndType
        columnOrders <- replicateM sizeOnly (readColumnOrder metaDataBuf bufferPos 0 [])
        readFileMetaData (metadata {columnOrders = columnOrders}) metaDataBuf bufferPos identifier fieldStack
      8 -> do
        encryptionAlgorithm <- readEncryptionAlgorithm metaDataBuf bufferPos 0 []
        readFileMetaData (metadata {encryptionAlgorithm = encryptionAlgorithm}) metaDataBuf bufferPos identifier fieldStack
      9 -> do
        footerSigningKeyMetadata <- readByteString metaDataBuf bufferPos
        readFileMetaData (metadata {footerSigningKeyMetadata = footerSigningKeyMetadata}) metaDataBuf bufferPos identifier fieldStack
      n -> return $ error $ "UNIMPLEMENTED " ++ show n

readEncryptionAlgorithm :: Ptr Word8 -> IORef Int -> Int16 -> [Int16] -> IO EncryptionAlgorithm
readEncryptionAlgorithm buf pos lastFieldId fieldStack = do
  fieldContents <- readField buf pos lastFieldId fieldStack
  case fieldContents of
    Nothing -> return ENCRYPTION_ALGORITHM_UNKNOWN
    Just (elemType, identifier) -> case identifier of
      1 -> do
        readAesGcmV1 (AesGcmV1 {aadPrefix = [], aadFileUnique = [], supplyAadPrefix = False}) buf pos 0 []
      2 -> do
        readAesGcmCtrV1 (AesGcmCtrV1 {aadPrefix = [], aadFileUnique = [], supplyAadPrefix = False}) buf pos 0 []
      n -> return ENCRYPTION_ALGORITHM_UNKNOWN

readAesGcmV1 :: EncryptionAlgorithm -> Ptr Word8 -> IORef Int -> Int16 -> [Int16] -> IO EncryptionAlgorithm
readAesGcmV1 v@(AesGcmV1 aadPrefix aadFileUnique supplyAadPrefix) buf pos lastFieldId fieldStack = do
  fieldContents <- readField buf pos lastFieldId fieldStack
  case fieldContents of
    Nothing -> return v
    Just (elemType, identifier) -> case identifier of
      1 -> do
        aadPrefix <- readByteString buf pos
        readAesGcmV1 (v {aadPrefix = aadPrefix}) buf pos lastFieldId fieldStack
      2 -> do
        aadFileUnique <- readByteString buf pos
        readAesGcmV1 (v {aadFileUnique = aadFileUnique}) buf pos lastFieldId fieldStack
      3 -> do
        supplyAadPrefix <- readAndAdvance pos buf
        readAesGcmV1 (v {supplyAadPrefix = supplyAadPrefix == compactBooleanTrue}) buf pos lastFieldId fieldStack
      _ -> return ENCRYPTION_ALGORITHM_UNKNOWN

readAesGcmCtrV1 :: EncryptionAlgorithm -> Ptr Word8 -> IORef Int -> Int16 -> [Int16] -> IO EncryptionAlgorithm
readAesGcmCtrV1 v@(AesGcmCtrV1 aadPrefix aadFileUnique supplyAadPrefix) buf pos lastFieldId fieldStack = do
  fieldContents <- readField buf pos lastFieldId fieldStack
  case fieldContents of
    Nothing -> return v
    Just (elemType, identifier) -> case identifier of
      1 -> do
        aadPrefix <- readByteString buf pos
        readAesGcmCtrV1 (v {aadPrefix = aadPrefix}) buf pos lastFieldId fieldStack
      2 -> do
        aadFileUnique <- readByteString buf pos
        readAesGcmCtrV1 (v {aadFileUnique = aadFileUnique}) buf pos lastFieldId fieldStack
      3 -> do
        supplyAadPrefix <- readAndAdvance pos buf
        readAesGcmCtrV1 (v {supplyAadPrefix = supplyAadPrefix == compactBooleanTrue}) buf pos lastFieldId fieldStack
      _ -> return ENCRYPTION_ALGORITHM_UNKNOWN

readColumnOrder :: Ptr Word8 -> IORef Int -> Int16 -> [Int16] -> IO ColumnOrder
readColumnOrder buf pos lastFieldId fieldStack = do
  fieldContents <- readField buf pos lastFieldId fieldStack
  case fieldContents of
    Nothing -> return COLUMN_ORDER_UNKNOWN
    Just (elemType, identifier) -> case identifier of
      1 -> do
        _ <- replicateM_ 2 (readTypeOrder buf pos 0 [])
        return TYPE_ORDER
      _ -> return COLUMN_ORDER_UNKNOWN

readTypeOrder :: Ptr Word8 -> IORef Int -> Int16 -> [Int16] -> IO ColumnOrder
readTypeOrder buf pos lastFieldId fieldStack = do
  fieldContents <- readField buf pos lastFieldId fieldStack
  case fieldContents of
    Nothing -> return TYPE_ORDER
    Just (elemType, identifier) -> if elemType == STOP
                                   then return TYPE_ORDER 
                                   else readTypeOrder buf pos identifier fieldStack

data SchemaElement = SchemaElement
  { elementName :: T.Text,
    elementType :: TType,
    typeLength :: Int32,
    numChildren :: Int32,
    fieldId :: Int32,
    repetitionType :: RepetitionType,
    convertedType :: Int32,
    scale :: Int32,
    precision :: Int32,
    logicalType :: LogicalType
  }
  deriving (Show, Eq)

data RepetitionType = REQUIRED | OPTIONAL | REPEATED | UNKNOWN_REPETITION_TYPE deriving (Eq, Show)

data LogicalType
  = STRING_TYPE
  | MAP_TYPE
  | LIST_TYPE
  | ENUM_TYPE
  | DECIMAL_TYPE
  | DATE_TYPE
  | DecimalType {decimalTypePrecision :: Int32, decimalTypeScale :: Int32}
  | TimeType {isAdjustedToUTC :: Bool, unit :: TimeUnit}
  | -- This should probably have a different, more constrained TimeUnit type.
    TimestampType {isAdjustedToUTC :: Bool, unit :: TimeUnit}
  | IntType {bitWidth :: Int8, intIsSigned :: Bool}
  | LOGICAL_TYPE_UNKNOWN
  | JSON_TYPE
  | BSON_TYPE
  | UUID_TYPE
  | FLOAT16_TYPE
  | VariantType {specificationVersion :: Int8}
  | GeometryType {crs :: T.Text}
  | GeographyType {crs :: T.Text, algorithm :: EdgeInterpolationAlgorithm}
  deriving (Eq, Show)

data TimeUnit
  = MILLISECONDS
  | MICROSECONDS
  | NANOSECONDS
  | TIME_UNIT_UNKNOWN
  deriving (Eq, Show)

data EdgeInterpolationAlgorithm
  = SPHERICAL
  | VINCENTY
  | THOMAS
  | ANDOYER
  | KARNEY
  deriving (Eq, Show)

repetitionTypeFromInt :: Int32 -> RepetitionType
repetitionTypeFromInt 0 = REQUIRED
repetitionTypeFromInt 1 = OPTIONAL
repetitionTypeFromInt 2 = REPEATED
repetitionTypeFromInt _ = UNKNOWN_REPETITION_TYPE

defaultSchemaElement :: SchemaElement
defaultSchemaElement = SchemaElement "" STOP 0 0 (-1) UNKNOWN_REPETITION_TYPE 0 0 0 LOGICAL_TYPE_UNKNOWN

toIntegralType :: Int32 -> TType
toIntegralType n
  | n == 0 = BOOL
  | n == 1 = I32
  | n == 2 = I64
  | n == 3 = I64
  | n == 4 = DOUBLE
  | n == 5 = DOUBLE
  | n == 6 = STRING
  | n == 7 = STRING
  | otherwise = error $ "Unknown integral type: " ++ show n

readSchemaElement :: SchemaElement -> Ptr Word8 -> IORef Int -> Int16 -> [Int16] -> IO SchemaElement
readSchemaElement schemaElement buf pos lastFieldId fieldStack = do
  t <- readAndAdvance pos buf
  if t .&. 0x0f == 0
    then return schemaElement
    else do
      let modifier = fromIntegral ((t .&. 0xf0) `shiftR` 4) :: Int16
      identifier <-
        if modifier == 0
          then readIntFromBuffer @Int16 buf pos
          else return (lastFieldId + modifier)
      let elemType = toTType (t .&. 0x0f)
      case identifier of
        1 -> do
          schemaElemType <- toIntegralType <$> readInt32FromBuffer buf pos
          readSchemaElement (schemaElement {elementType = schemaElemType}) buf pos identifier fieldStack
        2 -> do
          typeLength <- readInt32FromBuffer buf pos
          readSchemaElement (schemaElement {typeLength = typeLength}) buf pos identifier fieldStack
        3 -> do
          fieldRepetitionType <- readInt32FromBuffer buf pos
          readSchemaElement (schemaElement {repetitionType = repetitionTypeFromInt fieldRepetitionType}) buf pos identifier fieldStack
        4 -> do
          nameSize <- readVarIntFromBuffer @Int buf pos
          contents <- replicateM nameSize (readAndAdvance pos buf)
          readSchemaElement (schemaElement {elementName = T.pack (map (chr . fromIntegral) contents)}) buf pos identifier fieldStack
        5 -> do
          numChildren <- readInt32FromBuffer buf pos
          readSchemaElement (schemaElement {numChildren = numChildren}) buf pos identifier fieldStack
        6 -> do
          convertedType <- readInt32FromBuffer buf pos
          readSchemaElement (schemaElement {convertedType = convertedType}) buf pos identifier fieldStack
        7 -> do
          scale <- readInt32FromBuffer buf pos
          readSchemaElement (schemaElement {scale = scale}) buf pos identifier fieldStack
        8 -> do
          precision <- readInt32FromBuffer buf pos
          readSchemaElement (schemaElement {precision = precision}) buf pos identifier fieldStack
        9 -> do
          fieldId <- readInt32FromBuffer buf pos
          readSchemaElement (schemaElement {fieldId = fieldId}) buf pos identifier fieldStack
        10 -> do
          logicalType <- readLogicalType buf pos 0 []
          readSchemaElement (schemaElement {logicalType = logicalType}) buf pos identifier fieldStack
        _ -> error $ show identifier -- return schemaElement

readLogicalType :: Ptr Word8 -> IORef Int -> Int16 -> [Int16] -> IO LogicalType
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
      let elemType = toTType (t .&. 0x0f)
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
          _ <- readField buf pos 0 []
          readDecimalType (DecimalType {decimalTypeScale = 0, decimalTypePrecision = 0}) buf pos 0 []
        6 -> do
          replicateM_ 2 (readField buf pos 0 [])
          return DATE_TYPE
        7 -> do
          _ <- readField buf pos 0 []
          readTimeType (TimeType {isAdjustedToUTC = False, unit = MILLISECONDS}) buf pos 0 []
        8 -> do
          _ <- readField buf pos 0 []
          readTimeType (TimestampType {isAdjustedToUTC = False, unit = MILLISECONDS}) buf pos 0 []
        -- Apparently reserved for interval types
        9 -> return LOGICAL_TYPE_UNKNOWN
        10 -> do
          _ <- readField buf pos 0 []
          readIntType (IntType {intIsSigned = False, bitWidth = 0}) buf pos 0 []
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
          _ <- readField buf pos 0 []
          return VariantType {specificationVersion = 1}
        17 -> do
          _ <- readField buf pos 0 []
          return GeometryType {crs = ""}
        18 -> do
          _ <- readField buf pos 0 []
          return GeographyType {crs = "", algorithm = SPHERICAL}
        _ -> return LOGICAL_TYPE_UNKNOWN

readIntType :: LogicalType -> Ptr Word8 -> IORef Int -> Int16 -> [Int16] -> IO LogicalType
readIntType v@(IntType bitWidth intIsSigned) buf pos lastFieldId fieldStack = do
  fieldContents <- readField buf pos lastFieldId fieldStack
  case fieldContents of
    Nothing -> return v
    Just (elemType, identifier) -> case identifier of
      1 -> do
        bitWidth <- readAndAdvance pos buf
        readIntType (v {bitWidth = fromIntegral bitWidth}) buf pos lastFieldId fieldStack
      2 -> do
        -- TODO: Check for empty
        intIsSigned <- readAndAdvance pos buf
        readIntType (v {intIsSigned = intIsSigned == compactBooleanTrue}) buf pos lastFieldId fieldStack
      _ -> error $ "UNKNOWN field ID for IntType" ++ show identifier

readDecimalType :: LogicalType -> Ptr Word8 -> IORef Int -> Int16 -> [Int16] -> IO LogicalType
readDecimalType v@(DecimalType p s) buf pos lastFieldId fieldStack = do
  fieldContents <- readField buf pos lastFieldId fieldStack
  case fieldContents of
    Nothing -> return v
    Just (elemType, identifier) -> case identifier of
      1 -> do
        s' <- readInt32FromBuffer buf pos
        readDecimalType (v {decimalTypeScale = s'}) buf pos lastFieldId fieldStack
      2 -> do
        p' <- readInt32FromBuffer buf pos
        readDecimalType (v {decimalTypePrecision = p'}) buf pos lastFieldId fieldStack
      _ -> error $ "UNKNOWN field ID for DecimalType" ++ show identifier

readTimeType :: LogicalType -> Ptr Word8 -> IORef Int -> Int16 -> [Int16] -> IO LogicalType
readTimeType v@(TimeType _ _) buf pos lastFieldId fieldStack = do
  fieldContents <- readField buf pos lastFieldId fieldStack
  case fieldContents of
    Nothing -> return v
    Just (elemType, identifier) -> case identifier of
      1 -> do
        -- TODO: Check for empty
        isAdjustedToUTC <- readAndAdvance pos buf
        readTimeType (v {isAdjustedToUTC = isAdjustedToUTC == compactBooleanTrue}) buf pos lastFieldId fieldStack
      2 -> do
        u <- readUnit buf pos 0 []
        readTimeType (v {unit = u}) buf pos lastFieldId fieldStack
      _ -> error $ "UNKNOWN field ID for TimeType" ++ show identifier
readTimeType v@(TimestampType _ _) buf pos lastFieldId fieldStack = do
  fieldContents <- readField buf pos lastFieldId fieldStack
  case fieldContents of
    Nothing -> return v
    Just (elemType, identifier) -> case identifier of
      1 -> do
        -- TODO: Check for empty
        isAdjustedToUTC <- readAndAdvance pos buf
        readTimeType (v {isAdjustedToUTC = isAdjustedToUTC == compactBooleanTrue}) buf pos lastFieldId fieldStack
      2 -> do
        u <- readUnit buf pos 0 []
        readTimeType (v {unit = u}) buf pos lastFieldId fieldStack
      _ -> error $ "UNKNOWN field ID for TimestampType" ++ show identifier

readUnit :: Ptr Word8 -> IORef Int -> Int16 -> [Int16] -> IO TimeUnit
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

readRowGroup :: RowGroup -> Ptr Word8 -> IORef Int -> Int16 -> [Int16] -> IO RowGroup
readRowGroup r buf pos lastFieldId fieldStack = do
  fieldContents <- readField buf pos lastFieldId fieldStack
  case fieldContents of
    Nothing -> return r
    Just (elemType, identifier) -> case identifier of
      1 -> do
        sizeAndType <- readAndAdvance pos buf
        let sizeOnly = fromIntegral ((sizeAndType `shiftR` 4) .&. 0x0f) :: Int
        -- type of the contents of the list.
        let elemType = toTType sizeAndType
        columnChunks <- replicateM sizeOnly (readColumnChunk emptyColumnChunk buf pos 0 [])
        readRowGroup (r {rowGroupColumns = columnChunks}) buf pos identifier fieldStack
      2 -> do
        totalBytes <- readIntFromBuffer @Int64 buf pos
        readRowGroup (r {totalByteSize = totalBytes}) buf pos identifier fieldStack
      3 -> do
        nRows <- readIntFromBuffer @Int64 buf pos
        readRowGroup (r {rowGroupNumRows = nRows}) buf pos identifier fieldStack
      4 -> return r
      5 -> do
        offset <- readIntFromBuffer @Int64 buf pos
        readRowGroup (r {fileOffset = offset}) buf pos identifier fieldStack
      6 -> do
        compressedSize <- readIntFromBuffer @Int64 buf pos
        readRowGroup (r {totalCompressedSize = compressedSize}) buf pos identifier fieldStack
      7 -> do
        ordinal <- readIntFromBuffer @Int16 buf pos
        readRowGroup (r {ordinal = ordinal}) buf pos identifier fieldStack
      _ -> error $ "Unknown row group field: " ++ show identifier

readColumnChunk :: ColumnChunk -> Ptr Word8 -> IORef Int -> Int16 -> [Int16] -> IO ColumnChunk
readColumnChunk c buf pos lastFieldId fieldStack = do
  fieldContents <- readField buf pos lastFieldId fieldStack
  case fieldContents of
    Nothing -> return c
    Just (elemType, identifier) -> case identifier of
      1 -> do
        stringSize <- readVarIntFromBuffer @Int buf pos
        contents <- map (chr . fromIntegral) <$> replicateM stringSize (readAndAdvance pos buf)
        readColumnChunk (c {columnChunkFilePath = contents}) buf pos identifier fieldStack
      2 -> do
        columnChunkMetadataFileOffset <- readIntFromBuffer @Int64 buf pos
        readColumnChunk (c {columnChunkMetadataFileOffset = columnChunkMetadataFileOffset}) buf pos identifier fieldStack
      3 -> do
        columnMetadata <- readColumnMetadata emptyColumnMetadata buf pos 0 []
        readColumnChunk (c {columnMetaData = columnMetadata}) buf pos identifier fieldStack 
      4 -> do
        columnOffsetIndexOffset <- readIntFromBuffer @Int64 buf pos
        readColumnChunk (c {columnChunkOffsetIndexOffset = columnOffsetIndexOffset}) buf pos identifier fieldStack
      5 -> do
        columnOffsetIndexLength <- readInt32FromBuffer buf pos
        readColumnChunk (c {columnChunkOffsetIndexLength = columnOffsetIndexLength}) buf pos identifier fieldStack
      6 -> do
        columnChunkColumnIndexOffset <- readIntFromBuffer @Int64 buf pos
        readColumnChunk (c {columnChunkColumnIndexOffset = columnChunkColumnIndexOffset}) buf pos identifier fieldStack
      7 -> do
        columnChunkColumnIndexLength <- readInt32FromBuffer buf pos
        readColumnChunk (c {columnChunkColumnIndexLength = columnChunkColumnIndexLength}) buf pos identifier fieldStack
      _ -> return c

readColumnMetadata :: ColumnMetaData -> Ptr Word8 -> IORef Int -> Int16 -> [Int16] -> IO ColumnMetaData
readColumnMetadata cm buf pos lastFieldId fieldStack = do
  fieldContents <- readField buf pos lastFieldId fieldStack
  case fieldContents of
    Nothing -> return cm
    Just (elemType, identifier) -> case identifier of
      1 -> do
        cType <- parquetTypeFromInt <$> readInt32FromBuffer buf pos
        readColumnMetadata (cm {columnType = cType}) buf pos identifier []
      2 -> do
        sizeAndType <- readAndAdvance pos buf
        let sizeOnly = fromIntegral ((sizeAndType `shiftR` 4) .&. 0x0f) :: Int
        let elemType = toTType sizeAndType
        encodings <- replicateM sizeOnly (readParquetEncoding buf pos 0 [])
        readColumnMetadata (cm {columnEncodings = encodings}) buf pos identifier fieldStack
      3 -> do
        sizeAndType <- readAndAdvance pos buf
        let sizeOnly = fromIntegral ((sizeAndType `shiftR` 4) .&. 0x0f) :: Int
        let elemType = toTType sizeAndType
        paths <- replicateM sizeOnly (readString buf pos)
        readColumnMetadata (cm {columnPathInSchema = paths}) buf pos identifier fieldStack
      4 -> do
        cType <- compressionCodecFromInt <$> readInt32FromBuffer buf pos
        readColumnMetadata (cm {columnCodec = cType}) buf pos identifier []
      5 -> do
        numValues <- readIntFromBuffer @Int64 buf pos
        readColumnMetadata (cm {columnNumValues = numValues}) buf pos identifier []
      6 -> do
        columnTotalUncompressedSize <- readIntFromBuffer @Int64 buf pos
        readColumnMetadata (cm {columnTotalUncompressedSize = columnTotalUncompressedSize}) buf pos identifier []
      7 -> do
        columnTotalCompressedSize <- readIntFromBuffer @Int64 buf pos
        readColumnMetadata (cm {columnTotalCompressedSize = columnTotalCompressedSize}) buf pos identifier []
      8 -> do
        sizeAndType <- readAndAdvance pos buf
        let sizeOnly = fromIntegral ((sizeAndType `shiftR` 4) .&. 0x0f) :: Int
        let elemType = toTType sizeAndType
        columnKeyValueMetadata <- replicateM sizeOnly (readKeyValue emptyKeyValue buf pos 0 [])
        readColumnMetadata (cm {columnKeyValueMetadata = columnKeyValueMetadata}) buf pos identifier fieldStack
      9 -> do
        columnDataPageOffset <- readIntFromBuffer @Int64 buf pos
        readColumnMetadata (cm {columnDataPageOffset = columnDataPageOffset}) buf pos identifier []
      10 -> do
        columnIndexPageOffset <- readIntFromBuffer @Int64 buf pos
        readColumnMetadata (cm {columnIndexPageOffset = columnIndexPageOffset}) buf pos identifier []
      11 -> do
        columnDictionaryPageOffset <- readIntFromBuffer @Int64 buf pos
        readColumnMetadata (cm {columnDictionaryPageOffset = columnDictionaryPageOffset}) buf pos identifier []
      12 -> do
        stats <- readStatistics emptyColumnStatistics buf pos 0 []
        readColumnMetadata (cm {columnStatistics = stats}) buf pos identifier fieldStack
      13 -> do
        sizeAndType <- readAndAdvance pos buf
        let sizeOnly = fromIntegral ((sizeAndType `shiftR` 4) .&. 0x0f) :: Int
        let elemType = toTType sizeAndType
        pageEncodingStats <- replicateM sizeOnly (readPageEncodingStats emptyPageEncodingStats buf pos 0 [])
        readColumnMetadata (cm {columnEncodingStats = pageEncodingStats}) buf pos identifier fieldStack
      14 -> do
        bloomFilterOffset <- readIntFromBuffer @Int64 buf pos
        readColumnMetadata (cm {bloomFilterOffset = bloomFilterOffset}) buf pos identifier []
      15 -> do
        bloomFilterLength <- readInt32FromBuffer buf pos
        readColumnMetadata (cm {bloomFilterLength = bloomFilterLength}) buf pos identifier []
      16 -> do
        stats <- readSizeStatistics emptySizeStatistics buf pos 0 []
        readColumnMetadata (cm {columnSizeStatistics = stats}) buf pos identifier fieldStack
      17 -> return $ error "UNIMPLEMENTED"
      _ -> return cm

readParquetEncoding :: Ptr Word8 -> IORef Int -> Int16 -> [Int16] -> IO ParquetEncoding
readParquetEncoding buf pos lastFieldId fieldStack = parquetEncodingFromInt <$> readInt32FromBuffer buf pos

readPageEncodingStats :: PageEncodingStats -> Ptr Word8 -> IORef Int -> Int16 -> [Int16] -> IO PageEncodingStats
readPageEncodingStats pes buf pos lastFieldId fieldStack = do
  fieldContents <- readField buf pos lastFieldId fieldStack
  case fieldContents of
    Nothing -> return pes
    Just (elemType, identifier) -> case identifier of
      1 -> do
        pType <- pageTypeFromInt <$> readInt32FromBuffer buf pos
        readPageEncodingStats (pes {pageEncodingPageType = pType}) buf pos identifier []
      2 -> do
        pEnc <- parquetEncodingFromInt <$> readInt32FromBuffer buf pos
        readPageEncodingStats (pes {pageEncoding = pEnc}) buf pos identifier []
      3 -> do
        encodedCount <- readInt32FromBuffer buf pos
        readPageEncodingStats (pes {pagesWithEncoding = encodedCount}) buf pos identifier []
      _ -> pure pes

readStatistics :: ColumnStatistics -> Ptr Word8 -> IORef Int -> Int16 -> [Int16] -> IO ColumnStatistics
readStatistics cs buf pos lastFieldId fieldStack = do
  fieldContents <- readField buf pos lastFieldId fieldStack
  case fieldContents of
    Nothing -> return cs
    Just (elemType, identifier) -> case identifier of
      1 -> do
        maxInBytes <- readByteString buf pos
        readStatistics (cs {columnMax = maxInBytes}) buf pos identifier fieldStack
      2 -> do
        minInBytes <- readByteString buf pos
        readStatistics (cs {columnMin = minInBytes}) buf pos identifier fieldStack
      3 -> do
        nullCount <- readIntFromBuffer @Int64 buf pos
        readStatistics (cs {columnNullCount = nullCount}) buf pos identifier fieldStack
      4 -> do
        distinctCount <- readIntFromBuffer @Int64 buf pos
        readStatistics (cs {columnDistictCount = distinctCount}) buf pos identifier fieldStack
      5 -> do
        maxInBytes <- readByteString buf pos
        readStatistics (cs {columnMaxValue = maxInBytes}) buf pos identifier fieldStack
      6 -> do
        minInBytes <- readByteString buf pos
        readStatistics (cs {columnMinValue = minInBytes}) buf pos identifier fieldStack
      7 -> do
        isMaxValueExact <- readAndAdvance pos buf
        readStatistics (cs {isColumnMaxValueExact = isMaxValueExact == compactBooleanTrue}) buf pos identifier fieldStack
      8 -> do
        isMinValueExact <- readAndAdvance pos buf
        readStatistics (cs {isColumnMinValueExact = isMinValueExact == compactBooleanTrue}) buf pos identifier fieldStack
      _ -> pure cs

readSizeStatistics :: SizeStatistics -> Ptr Word8 -> IORef Int -> Int16 -> [Int16] -> IO SizeStatistics
readSizeStatistics ss buf pos lastFieldId fieldStack = do
  fieldContents <- readField buf pos lastFieldId fieldStack
  case fieldContents of
    Nothing -> return ss
    Just (elemType, identifier) -> case identifier of
      1 -> do
        unencodedByteArrayDataTypes <- readIntFromBuffer @Int64 buf pos
        readSizeStatistics (ss {unencodedByteArrayDataTypes = unencodedByteArrayDataTypes}) buf pos identifier fieldStack
      2 -> do
        sizeAndType <- readAndAdvance pos buf
        let sizeOnly = fromIntegral ((sizeAndType `shiftR` 4) .&. 0x0f) :: Int
        let elemType = toTType sizeAndType
        repetitionLevelHistogram <- replicateM sizeOnly (readIntFromBuffer @Int64 buf pos)
        readSizeStatistics (ss {repetitionLevelHistogram = repetitionLevelHistogram}) buf pos identifier fieldStack
      3 -> do
        sizeAndType <- readAndAdvance pos buf
        let sizeOnly = fromIntegral ((sizeAndType `shiftR` 4) .&. 0x0f) :: Int
        let elemType = toTType sizeAndType
        definitionLevelHistogram <- replicateM sizeOnly (readIntFromBuffer @Int64 buf pos)
        readSizeStatistics (ss {definitionLevelHistogram = definitionLevelHistogram}) buf pos identifier fieldStack
      _ -> pure ss

readKeyValue :: KeyValue -> Ptr Word8 -> IORef Int -> Int16 -> [Int16] -> IO KeyValue
readKeyValue kv buf pos lastFieldId fieldStack = do
  fieldContents <- readField buf pos lastFieldId fieldStack
  case fieldContents of
    Nothing -> return kv
    Just (elemType, identifier) -> case identifier of
      1 -> do
        k <- readString buf pos
        readKeyValue (kv {key = k}) buf pos identifier fieldStack
      2 -> do
        v <- readString buf pos
        readKeyValue (kv {key = v}) buf pos identifier fieldStack
      _ -> return kv

readString :: Ptr Word8 -> IORef Int -> IO String
readString buf pos = do
  nameSize <- readVarIntFromBuffer @Int buf pos
  map (chr . fromIntegral) <$> replicateM nameSize (readAndAdvance pos buf)

readByteStringFromBytes :: [Word8] -> ([Word8], [Word8])
readByteStringFromBytes xs = let
    (size, rem) = readVarIntFromBytes @Int xs
  in (take size rem, drop size rem)

readByteString :: Ptr Word8 -> IORef Int -> IO [Word8]
readByteString buf pos = do
  size <- readVarIntFromBuffer @Int buf pos
  replicateM size (readAndAdvance pos buf)

readByteString' :: Ptr Word8 -> Int64 -> IO [Word8]
readByteString' buf size = mapM (`readSingleByte` buf) [0..(size - 1)]

readField :: Ptr Word8 -> IORef Int -> Int16 -> [Int16] -> IO (Maybe (TType, Int16))
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

readField' :: [Word8] -> Int16 -> Maybe ([Word8], TType, Int16)
readField' [] _ = Nothing
readField' (x:xs) lastFieldId
  | x .&. 0x0f == 0 = Nothing
  | otherwise = let
      modifier = fromIntegral ((x .&. 0xf0) `shiftR` 4) :: Int16
      (identifier, rem) = if modifier == 0 then readIntFromBytes @Int16 xs else (lastFieldId + modifier, xs)
      elemType = toTType (x .&. 0x0f)
    in Just (rem, elemType, identifier)

readAndAdvance :: IORef Int -> Ptr b -> IO Word8
readAndAdvance bufferPos buffer = do
  pos <- readIORef bufferPos
  b <- peekByteOff buffer pos :: IO Word8
  modifyIORef bufferPos (+ 1)
  return b

readSingleByte :: Int64 -> Ptr b -> IO Word8
readSingleByte pos buffer = peekByteOff buffer (fromIntegral pos)

readNoAdvance :: IORef Int -> Ptr b -> IO Word8
readNoAdvance bufferPos buffer = do
  pos <- readIORef bufferPos
  peekByteOff buffer pos :: IO Word8

compactBooleanTrue :: Word8
compactBooleanTrue = 0x01

compactBooleanFalse :: Word8
compactBooleanFalse = 0x02

compactByte :: Word8
compactByte = 0x03

compactI16 :: Word8
compactI16 = 0x04

compactI32 :: Word8
compactI32 = 0x05

compactI64 :: Word8
compactI64 = 0x06

compactDouble :: Word8
compactDouble = 0x07

compactBinary :: Word8
compactBinary = 0x08

compactList :: Word8
compactList = 0x09

compactSet :: Word8
compactSet = 0x0A

compactMap :: Word8
compactMap = 0x0B

compactStruct :: Word8
compactStruct = 0x0C

compactUuid :: Word8
compactUuid = 0x0D

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

toTType :: Word8 -> TType
toTType t =
  fromMaybe STOP $
    M.lookup (t .&. 0x0f) $
      M.fromList
        [ (compactBooleanTrue, BOOL),
          (compactBooleanFalse, BOOL),
          (compactByte, BYTE),
          (compactI16, I16),
          (compactI32, I32),
          (compactI64, I64),
          (compactDouble, DOUBLE),
          (compactBinary, STRING),
          (compactList, LIST),
          (compactSet, SET),
          (compactMap, MAP),
          (compactStruct, STRUCT),
          (compactUuid, UUID)
        ]

readIntFromBuffer :: (Integral a) => Ptr b -> IORef Int -> IO a
readIntFromBuffer buf bufferPos = do
  n <- readVarIntFromBuffer buf bufferPos
  let u = fromIntegral n :: Word32
  return $ fromIntegral $ (fromIntegral (u `shiftR` 1) :: Int32) .^. (-(n .&. 1))

readIntFromBytes :: (Integral a) => [Word8] -> (a, [Word8])
readIntFromBytes bs = let
    (n, rem) = readVarIntFromBytes bs
    u = fromIntegral n :: Word32
  in (fromIntegral $ (fromIntegral (u `shiftR` 1) :: Int32) .^. (-(n .&. 1)), rem)

readInt32FromBuffer :: Ptr b -> IORef Int -> IO Int32
readInt32FromBuffer buf bufferPos = do
  n <- (fromIntegral <$> readVarIntFromBuffer @Int64 buf bufferPos) :: IO Int32
  let u = fromIntegral n :: Word32
  return $ (fromIntegral (u `shiftR` 1) :: Int32) .^. (-(n .&. 1))

readInt32FromBytes :: [Word8] -> (Int32, [Word8])
readInt32FromBytes bs = let
    (n', rem) = readVarIntFromBytes @Int64 bs
    n = fromIntegral n' :: Int32
    u = fromIntegral n :: Word32
  in ((fromIntegral (u `shiftR` 1) :: Int32) .^. (-(n .&. 1)), rem)

readVarIntFromBuffer :: (Integral a) => Ptr b -> IORef Int -> IO a
readVarIntFromBuffer buf bufferPos = do
  start <- readIORef bufferPos
  let loop i shift result = do
        b <- readAndAdvance bufferPos buf
        let res = result .|. ((fromIntegral (b .&. 0x7f) :: Integer) `shiftL` shift)
        if (b .&. 0x80) /= 0x80
          then return res
          else loop (i + 1) (shift + 7) res
  fromIntegral <$> loop start 0 0

readVarIntFromBytes :: (Integral a) => [Word8] -> (a, [Word8])
readVarIntFromBytes bs = (fromIntegral n, rem)
  where
    (n, rem) = loop 0 0 bs
    loop _ result [] = (result, [])
    loop shift result (x:xs) = let
        res = result .|. ((fromIntegral (x .&. 0x7f) :: Integer) `shiftL` shift)
      in if (x .&. 0x80) /= 0x80 then (res, xs) else loop (shift + 7) res xs

-- // Uint32 returns the uint32 representation of b[0:4].
-- func (littleEndian) Uint32(b []byte) uint32 {
-- 	_ = b[3] // bounds check hint to compiler; see golang.org/issue/14808
-- 	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
-- }


littleEndianWord8 :: [Word8] -> Word8
littleEndianWord8 bytes
  | length bytes == 4 = foldr (\v acc -> acc .|. v) 0 (zipWith (\b i -> b `shiftL` i) bytes [0,8..])
  | otherwise = error "Expected exactly 4 bytes"