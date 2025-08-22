{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module DataFrame.IO.Parquet (
    readParquet,
) where

import Control.Monad
import qualified Data.ByteString as BSO
import Data.Char
import Data.Foldable
import Data.IORef
import Data.List
import qualified Data.Map as M
import Data.Maybe
import qualified Data.Text as T
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.DataFrame (DataFrame)
import qualified DataFrame.Internal.DataFrame as DI
import qualified DataFrame.Operations.Core as DI
import Foreign
import System.IO

import DataFrame.IO.Parquet.Binary
import DataFrame.IO.Parquet.Dictionary
import DataFrame.IO.Parquet.Levels
import DataFrame.IO.Parquet.Page
import DataFrame.IO.Parquet.Thrift
import DataFrame.IO.Parquet.Types

readParquet :: String -> IO DataFrame
readParquet path = withBinaryFile path ReadMode $ \handle -> do
    (size, magicString) <- readMetadataSizeFromFooter handle
    when (magicString /= "PAR1") $ error "Invalid Parquet file"

    fileMetadata <- readMetadata handle size

    let columnPaths = getColumnPaths (drop 1 $ schema fileMetadata)
    let columnNames = map fst columnPaths

    colMap <- newIORef (M.empty :: M.Map T.Text DI.Column)

    let schemaElements = schema fileMetadata
    let getTypeLength :: [String] -> Maybe Int32
        getTypeLength path = findTypeLength schemaElements path 0
          where
            findTypeLength [] _ _ = Nothing
            findTypeLength (s : ss) targetPath depth
                | map T.unpack (pathToElement s ss depth) == targetPath
                    && elementType s == STRING
                    && typeLength s > 0 =
                    Just (typeLength s)
                | otherwise = findTypeLength ss targetPath (if numChildren s > 0 then depth + 1 else depth)

            pathToElement _ _ _ = []

    forM_ (rowGroups fileMetadata) $ \rowGroup -> do
        forM_ (zip (rowGroupColumns rowGroup) [0 ..]) $ \(colChunk, colIdx) -> do
            let metadata = columnMetaData colChunk
            let colPath = columnPathInSchema metadata
            let colName =
                    if null colPath
                        then T.pack $ "col_" ++ show colIdx
                        else T.pack $ last colPath

            let colDataPageOffset = columnDataPageOffset metadata
            let colDictionaryPageOffset = columnDictionaryPageOffset metadata
            let colStart =
                    if colDictionaryPageOffset > 0 && colDataPageOffset > colDictionaryPageOffset
                        then colDictionaryPageOffset
                        else colDataPageOffset
            let colLength = columnTotalCompressedSize metadata

            columnBytes <- readBytes handle colStart colLength

            pages <- readAllPages (columnCodec metadata) columnBytes

            let maybeTypeLength =
                    if columnType metadata == PFIXED_LEN_BYTE_ARRAY
                        then getTypeLength colPath
                        else Nothing

            let primaryEncoding = fromMaybe EPLAIN (fmap fst (uncons (columnEncodings metadata)))

            let schemaTail = drop 1 (schema fileMetadata)
            let colPath = columnPathInSchema (columnMetaData colChunk)
            let (maxDef, maxRep) = levelsForPath schemaTail colPath
            column <- processColumnPages (maxDef, maxRep) pages (columnType metadata) primaryEncoding maybeTypeLength

            modifyIORef colMap (M.insert colName column)

    finalColMap <- readIORef colMap
    let orderedColumns =
            map
                (\name -> (name, finalColMap M.! name))
                (filter (`M.member` finalColMap) columnNames)

    pure $ DI.fromNamedColumns orderedColumns

readMetadataSizeFromFooter :: Handle -> IO (Integer, BSO.ByteString)
readMetadataSizeFromFooter handle = do
    footerOffSet <- numBytesInFile handle
    buf <- mallocBytes 8 :: IO (Ptr Word8)
    hSeek handle AbsoluteSeek (fromIntegral $ footerOffSet - 8)
    _ <- hGetBuf handle buf 8

    sizeBytes <- mapM (\i -> fromIntegral <$> (peekElemOff buf i :: IO Word8) :: IO Int32) [0 .. 3]
    let size = fromIntegral $ foldl' (.|.) 0 $ zipWith shift sizeBytes [0, 8, 16, 24]

    magicStringBytes <- mapM (\i -> peekElemOff buf i :: IO Word8) [4 .. 7]
    let magicString = BSO.pack magicStringBytes
    free buf
    return (size, magicString)

getColumnPaths :: [SchemaElement] -> [(T.Text, Int)]
getColumnPaths schema = extractLeafPaths schema 0 []
  where
    extractLeafPaths :: [SchemaElement] -> Int -> [T.Text] -> [(T.Text, Int)]
    extractLeafPaths [] _ _ = []
    extractLeafPaths (s : ss) idx path
        | numChildren s == 0 =
            let fullPath = T.intercalate "." (path ++ [elementName s])
             in (fullPath, idx) : extractLeafPaths ss (idx + 1) path
        | otherwise =
            let newPath = if T.null (elementName s) then path else path ++ [elementName s]
                childrenCount = fromIntegral (numChildren s)
                (children, remaining) = splitAt childrenCount ss
                childResults = extractLeafPaths children idx newPath
             in childResults ++ extractLeafPaths remaining (idx + length childResults) path

processColumnPages :: (Int, Int) -> [Page] -> ParquetType -> ParquetEncoding -> Maybe Int32 -> IO DI.Column
processColumnPages (maxDef, maxRep) pages pType _ maybeTypeLength = do
    let dictPages = filter isDictionaryPage pages
    let dataPages = filter isDataPage pages

    let dictValsM =
            case dictPages of
                [] -> Nothing
                (dictPage : _) ->
                    case pageTypeHeader (pageHeader dictPage) of
                        DictionaryPageHeader{..} ->
                            let countForBools =
                                    if pType == PBOOLEAN
                                        then error "is bool" Just dictionaryPageHeaderNumValues
                                        else maybeTypeLength
                             in Just (readDictVals pType (pageBytes dictPage) countForBools)
                        _ -> Nothing

    cols <- forM dataPages $ \page -> do
        case pageTypeHeader (pageHeader page) of
            DataPageHeader{..} -> do
                let n = fromIntegral dataPageHeaderNumValues
                let bs0 = pageBytes page
                let (defLvls, _repLvls, afterLvls) = readLevelsV1 n maxDef maxRep bs0
                let nPresent = length (filter (== maxDef) defLvls)

                case dataPageHeaderEncoding of
                    EPLAIN ->
                        case pType of
                            PBOOLEAN ->
                                let (vals, _) = readNBool nPresent afterLvls
                                 in pure (toMaybeBool maxDef defLvls vals)
                            PINT32 ->
                                let (vals, _) = readNInt32 nPresent afterLvls
                                 in pure (toMaybeInt32 maxDef defLvls vals)
                            PINT64 ->
                                let (vals, _) = readNInt64 nPresent afterLvls
                                 in pure (toMaybeInt64 maxDef defLvls vals)
                            PINT96 ->
                                let (vals, _) = readNInt96Times nPresent afterLvls
                                 in pure (toMaybeUTCTime maxDef defLvls vals)
                            PFLOAT ->
                                let (vals, _) = readNFloat nPresent afterLvls
                                 in pure (toMaybeFloat maxDef defLvls vals)
                            PDOUBLE ->
                                let (vals, _) = readNDouble nPresent afterLvls
                                 in pure (toMaybeDouble maxDef defLvls vals)
                            PBYTE_ARRAY ->
                                let (raws, _) = readNByteArrays nPresent afterLvls
                                    texts = map (T.pack . map (chr . fromIntegral)) raws
                                 in pure (toMaybeText maxDef defLvls texts)
                            PFIXED_LEN_BYTE_ARRAY ->
                                case maybeTypeLength of
                                    Just len ->
                                        let (raws, _) = splitFixed nPresent (fromIntegral len) afterLvls
                                            texts = map (T.pack . map (chr . fromIntegral)) raws
                                         in pure (toMaybeText maxDef defLvls texts)
                                    Nothing -> error "FIXED_LEN_BYTE_ARRAY requires type length"
                            PARQUET_TYPE_UNKNOWN -> error "Cannot read unknown Parquet type"
                    ERLE_DICTIONARY -> decodeDictV1 dictValsM maxDef defLvls nPresent afterLvls
                    EPLAIN_DICTIONARY -> decodeDictV1 dictValsM maxDef defLvls nPresent afterLvls
                    other -> error ("Unsupported v1 encoding: " ++ show other)
            DataPageHeaderV2{..} -> do
                let n = fromIntegral dataPageHeaderV2NumValues
                let bs0 = pageBytes page
                let (defLvls, _repLvls, afterLvls) =
                        readLevelsV2 n maxDef maxRep definitionLevelByteLength repetitionLevelByteLength bs0
                let nPresent =
                        if dataPageHeaderV2NumNulls > 0
                            then fromIntegral (dataPageHeaderV2NumValues - dataPageHeaderV2NumNulls)
                            else length (filter (== maxDef) defLvls)

                case dataPageHeaderV2Encoding of
                    EPLAIN ->
                        case pType of
                            PBOOLEAN ->
                                let (vals, _) = readNBool nPresent afterLvls
                                 in pure (toMaybeBool maxDef defLvls vals)
                            PINT32 ->
                                let (vals, _) = readNInt32 nPresent afterLvls
                                 in pure (toMaybeInt32 maxDef defLvls vals)
                            PINT64 ->
                                let (vals, _) = readNInt64 nPresent afterLvls
                                 in pure (toMaybeInt64 maxDef defLvls vals)
                            PINT96 ->
                                let (vals, _) = readNInt96Times nPresent afterLvls
                                 in pure (toMaybeUTCTime maxDef defLvls vals)
                            PFLOAT ->
                                let (vals, _) = readNFloat nPresent afterLvls
                                 in pure (toMaybeFloat maxDef defLvls vals)
                            PDOUBLE ->
                                let (vals, _) = readNDouble nPresent afterLvls
                                 in pure (toMaybeDouble maxDef defLvls vals)
                            PBYTE_ARRAY ->
                                let (raws, _) = readNByteArrays nPresent afterLvls
                                    texts = map (T.pack . map (chr . fromIntegral)) raws
                                 in pure (toMaybeText maxDef defLvls texts)
                            PFIXED_LEN_BYTE_ARRAY ->
                                case maybeTypeLength of
                                    Just len ->
                                        let (raws, _) = splitFixed nPresent (fromIntegral len) afterLvls
                                            texts = map (T.pack . map (chr . fromIntegral)) raws
                                         in pure (toMaybeText maxDef defLvls texts)
                                    Nothing -> error "FIXED_LEN_BYTE_ARRAY requires type length"
                            PARQUET_TYPE_UNKNOWN -> error "Cannot read unknown Parquet type"
                    other -> error ("Unsupported v2 encoding: " ++ show other)

    case cols of
        [] -> pure $ DI.fromList ([] :: [Maybe Int])
        (c : cs) -> pure $ foldl' (\l r -> fromMaybe (error "concat failed") (DI.concatColumns l r)) c cs
