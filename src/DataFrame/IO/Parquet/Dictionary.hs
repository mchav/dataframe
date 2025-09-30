{-# LANGUAGE OverloadedStrings #-}

module DataFrame.IO.Parquet.Dictionary where

import Control.Monad
import Data.Bits
import Data.Char
import Data.Int
import Data.Maybe
import qualified Data.Text as T
import Data.Time
import Data.Word
import DataFrame.IO.Parquet.Binary
import DataFrame.IO.Parquet.Encoding
import DataFrame.IO.Parquet.Levels
import DataFrame.IO.Parquet.Time
import DataFrame.IO.Parquet.Types
import qualified DataFrame.Internal.Column as DI
import GHC.Float

dictCardinality :: DictVals -> Int
dictCardinality (DBool ds) = length ds
dictCardinality (DInt32 ds) = length ds
dictCardinality (DInt64 ds) = length ds
dictCardinality (DInt96 ds) = length ds
dictCardinality (DFloat ds) = length ds
dictCardinality (DDouble ds) = length ds
dictCardinality (DText ds) = length ds

readDictVals :: ParquetType -> [Word8] -> Maybe Int32 -> DictVals
readDictVals PBOOLEAN bs (Just count) = DBool (take (fromIntegral count) $ readPageBool bs)
readDictVals PINT32 bs _ = DInt32 (readPageInt32 bs)
readDictVals PINT64 bs _ = DInt64 (readPageInt64 bs)
readDictVals PINT96 bs _ = DInt96 (readPageInt96Times bs)
readDictVals PFLOAT bs _ = DFloat (readPageFloat bs)
readDictVals PDOUBLE bs _ = DDouble (readPageWord64 bs)
readDictVals PBYTE_ARRAY bs _ = DText (readPageBytes bs)
readDictVals PFIXED_LEN_BYTE_ARRAY bs (Just len) = DText (readPageFixedBytes bs (fromIntegral len))
readDictVals t _ _ = error $ "Unsupported dictionary type: " ++ show t

readPageInt32 :: [Word8] -> [Int32]
readPageInt32 [] = []
readPageInt32 xs = littleEndianInt32 (take 4 xs) : readPageInt32 (drop 4 xs)

readPageWord64 :: [Word8] -> [Double]
readPageWord64 [] = []
readPageWord64 xs =
    castWord64ToDouble (littleEndianWord64 (take 8 xs)) : readPageWord64 (drop 8 xs)

readPageBytes :: [Word8] -> [T.Text]
readPageBytes [] = []
readPageBytes xs =
    let lenBytes = fromIntegral (littleEndianInt32 $ take 4 xs)
        totalBytesRead = lenBytes + 4
     in T.pack (map (chr . fromIntegral) $ take lenBytes (drop 4 xs))
            : readPageBytes (drop totalBytesRead xs)

readPageBool :: [Word8] -> [Bool]
readPageBool [] = []
readPageBool bs =
    concatMap (\b -> map (\i -> (b `shiftR` i) .&. 1 == 1) [0 .. 7]) bs

readPageInt64 :: [Word8] -> [Int64]
readPageInt64 [] = []
readPageInt64 xs = fromIntegral (littleEndianWord64 (take 8 xs)) : readPageInt64 (drop 8 xs)

readPageFloat :: [Word8] -> [Float]
readPageFloat [] = []
readPageFloat xs =
    castWord32ToFloat (littleEndianWord32 (take 4 xs)) : readPageFloat (drop 4 xs)

readNInt96Times :: Int -> [Word8] -> ([UTCTime], [Word8])
readNInt96Times 0 bs = ([], bs)
readNInt96Times k bs =
    let timestamp96 = take 12 bs
        utcTime = int96ToUTCTime timestamp96
        bs' = drop 12 bs
        (times, rest) = readNInt96Times (k - 1) bs'
     in (utcTime : times, rest)

readPageInt96Times :: [Word8] -> [UTCTime]
readPageInt96Times [] = []
readPageInt96Times bs =
    let (times, _) = readNInt96Times (length bs `div` 12) bs
     in times

readPageFixedBytes :: [Word8] -> Int -> [T.Text]
readPageFixedBytes [] _ = []
readPageFixedBytes xs len =
    let chunk = take len xs
        text = T.pack (map (chr . fromIntegral) chunk)
     in text : readPageFixedBytes (drop len xs) len

decodeDictV1 :: Maybe DictVals -> Int -> [Int] -> Int -> [Word8] -> IO DI.Column
decodeDictV1 dictValsM maxDef defLvls nPresent bytes =
    case dictValsM of
        Nothing -> error "Dictionary-encoded page but dictionary is missing"
        Just dictVals ->
            let (idxs, _rest) = decodeDictIndicesV1 nPresent (dictCardinality dictVals) bytes
             in do
                    when (length idxs /= nPresent) $
                        error $
                            "dict index count mismatch: got "
                                ++ show (length idxs)
                                ++ ", expected "
                                ++ show nPresent
                    case dictVals of
                        DBool ds -> do
                            let values = [ds !! i | i <- idxs]
                            pure (toMaybeBool maxDef defLvls values)
                        DInt32 ds -> do
                            let values = [ds !! i | i <- idxs]
                            pure (toMaybeInt32 maxDef defLvls values)
                        DInt64 ds -> do
                            let values = [ds !! i | i <- idxs]
                            pure (toMaybeInt64 maxDef defLvls values)
                        DInt96 ds -> do
                            let values = [ds !! i | i <- idxs]
                            pure (toMaybeUTCTime maxDef defLvls values)
                        DFloat ds -> do
                            let values = [ds !! i | i <- idxs]
                            pure (toMaybeFloat maxDef defLvls values)
                        DDouble ds -> do
                            let values = [ds !! i | i <- idxs]
                            pure (toMaybeDouble maxDef defLvls values)
                        DText ds -> do
                            let values = [ds !! i | i <- idxs]
                            pure (toMaybeText maxDef defLvls values)

toMaybeInt32 :: Int -> [Int] -> [Int32] -> DI.Column
toMaybeInt32 maxDef def xs =
    let filled = stitchNullable maxDef def xs
     in if all isJust filled
            then DI.fromList (map (fromMaybe 0) filled)
            else DI.fromList filled

toMaybeDouble :: Int -> [Int] -> [Double] -> DI.Column
toMaybeDouble maxDef def xs =
    let filled = stitchNullable maxDef def xs
     in if all isJust filled
            then DI.fromList (map (fromMaybe 0) filled)
            else DI.fromList filled

toMaybeText :: Int -> [Int] -> [T.Text] -> DI.Column
toMaybeText maxDef def xs =
    let filled = stitchNullable maxDef def xs
     in if all isJust filled
            then DI.fromList (map (fromMaybe "") filled)
            else DI.fromList filled

toMaybeBool :: Int -> [Int] -> [Bool] -> DI.Column
toMaybeBool maxDef def xs =
    let filled = stitchNullable maxDef def xs
     in if all isJust filled
            then DI.fromList (map (fromMaybe False) filled)
            else DI.fromList filled

toMaybeInt64 :: Int -> [Int] -> [Int64] -> DI.Column
toMaybeInt64 maxDef def xs =
    let filled = stitchNullable maxDef def xs
     in if all isJust filled
            then DI.fromList (map (fromMaybe 0) filled)
            else DI.fromList filled

toMaybeFloat :: Int -> [Int] -> [Float] -> DI.Column
toMaybeFloat maxDef def xs =
    let filled = stitchNullable maxDef def xs
     in if all isJust filled
            then DI.fromList (map (fromMaybe 0.0) filled)
            else DI.fromList filled

toMaybeUTCTime :: Int -> [Int] -> [UTCTime] -> DI.Column
toMaybeUTCTime maxDef def times =
    let filled = stitchNullable maxDef def times
        defaultTime = UTCTime (fromGregorian 1970 1 1) (secondsToDiffTime 0)
     in if all isJust filled
            then DI.fromList (map (fromMaybe defaultTime) filled)
            else DI.fromList filled
