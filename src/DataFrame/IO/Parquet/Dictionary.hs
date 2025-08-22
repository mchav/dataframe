{-# LANGUAGE OverloadedStrings #-}
module DataFrame.IO.Parquet.Dictionary where

import Control.Monad
import Data.Char
import Data.Int
import Data.Word
import Data.Maybe
import Foreign
import qualified Data.Text as T
import qualified DataFrame.Internal.Column as DI
import DataFrame.IO.Parquet.Types
import DataFrame.IO.Parquet.Encoding
import DataFrame.IO.Parquet.Levels
import DataFrame.IO.Parquet.Binary
import GHC.Float
import GHC.IO (unsafePerformIO)

dictCardinality :: DictVals -> Int
dictCardinality (DInt32  ds) = length ds
dictCardinality (DDouble ds) = length ds
dictCardinality (DText   ds) = length ds

readDictVals :: ParquetType -> [Word8] -> Maybe Int32 -> DictVals
readDictVals PINT32 bs _  = DInt32 (readPageInt32 bs)
readDictVals PDOUBLE bs _ = DDouble (readPageWord64 bs)
readDictVals PBYTE_ARRAY bs _ = DText (readPageBytes bs)
readDictVals t _ _ = error $ "Unsupported dictionary type: " ++ show t

readPageInt32 :: [Word8] -> [Int32]
readPageInt32 [] = []
readPageInt32 xs = littleEndianInt32 (take 4 xs) : readPageInt32 (drop 4 xs)

readPageWord64 :: [Word8] -> [Double]
readPageWord64 [] = []
readPageWord64 xs = castWord64ToDouble (littleEndianWord64 (take 8 xs)) : readPageWord64 (drop 8 xs)

readPageBytes :: [Word8] -> [T.Text]
readPageBytes [] = []
readPageBytes xs =
    let lenBytes = fromIntegral (littleEndianInt32 $ take 4 xs)
        totalBytesRead = lenBytes + 4
    in T.pack (map (chr . fromIntegral) $ take lenBytes (drop 4 xs)) : readPageBytes (drop totalBytesRead xs)

decodeDictV1 :: Maybe DictVals -> Int -> [Int] -> Int -> [Word8] -> IO DI.Column
decodeDictV1 dictValsM maxDef defLvls nPresent bytes =
  case dictValsM of
    Nothing -> error "Dictionary-encoded page but dictionary is missing"
    Just dictVals ->
      let (idxs, _rest) = decodeDictIndicesV1 nPresent (dictCardinality dictVals) bytes
      in do
        when (length idxs /= nPresent) $
          error $ "dict index count mismatch: got " ++ show (length idxs) ++ ", expected " ++ show nPresent
        case dictVals of
          DInt32 ds  -> do
            let values = [ ds !! i | i <- idxs ]
            let result = toMaybeInt32 maxDef defLvls values
            pure result
          DDouble ds -> do
            let values = [ ds !! i | i <- idxs ]
            let result = toMaybeDouble maxDef defLvls values
            pure result
          DText ds   -> do
            let values = [ ds !! i | i <- idxs ]
            let result = toMaybeText maxDef defLvls values
            pure result

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
