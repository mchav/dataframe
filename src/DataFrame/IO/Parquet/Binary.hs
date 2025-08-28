{-# LANGUAGE TypeApplications #-}

module DataFrame.IO.Parquet.Binary where

import           Control.Monad
import           Data.Bits
import qualified Data.ByteString as BS
import           Data.Char
import           Data.IORef
import           Data.Word
import           Foreign
import           System.IO

littleEndianWord32 :: [Word8] -> Word32
littleEndianWord32 bytes
    | length bytes >= 4 = foldr (.|.) 0 (zipWith (\b i -> (fromIntegral b) `shiftL` i) (take 4 bytes) [0, 8, 16, 24])
    | otherwise = littleEndianWord32 (take 4 $ bytes ++ repeat 0)

littleEndianWord64 :: [Word8] -> Word64
littleEndianWord64 bytes = foldr (.|.) 0 (zipWith (\b i -> (fromIntegral b) `shiftL` i) (take 8 bytes) [0, 8 ..])

littleEndianInt32 :: [Word8] -> Int32
littleEndianInt32 = fromIntegral . littleEndianWord32

word64ToLittleEndian :: Word64 -> [Word8]
word64ToLittleEndian w = map (\i -> fromIntegral (w `shiftR` i)) [0, 8, 16, 24, 32, 40, 48, 56]

word32ToLittleEndian :: Word32 -> [Word8]
word32ToLittleEndian w = map (\i -> fromIntegral (w `shiftR` i)) [0, 8, 16, 24]

readUVarInt :: [Word8] -> (Word64, [Word8])
readUVarInt xs = loop xs 0 0 0
  where
    loop bs x _ 10 = (x, bs)
    loop (b : bs) x s i
        | b < 0x80 = (x .|. ((fromIntegral b) `shiftL` s), bs)
        | otherwise = loop bs (x .|. (fromIntegral ((b .&. 0x7f) `shiftL` s))) (s + 7) (i + 1)

readVarIntFromBytes :: (Integral a) => [Word8] -> (a, [Word8])
readVarIntFromBytes bs = (fromIntegral n, rem)
  where
    (n, rem) = loop 0 0 bs
    loop _ result [] = (result, [])
    loop shift result (x : xs) =
        let res = result .|. ((fromIntegral (x .&. 0x7f) :: Integer) `shiftL` shift)
         in if (x .&. 0x80) /= 0x80 then (res, xs) else loop (shift + 7) res xs

readIntFromBytes :: (Integral a) => [Word8] -> (a, [Word8])
readIntFromBytes bs =
    let (n, rem) = readVarIntFromBytes bs
        u = fromIntegral n :: Word32
     in (fromIntegral $ (fromIntegral (u `shiftR` 1) :: Int32) .^. (-(n .&. 1)), rem)

readInt32FromBytes :: [Word8] -> (Int32, [Word8])
readInt32FromBytes bs =
    let (n', rem) = readVarIntFromBytes @Int64 bs
        n = fromIntegral n' :: Int32
        u = fromIntegral n :: Word32
     in ((fromIntegral (u `shiftR` 1) :: Int32) .^. (-(n .&. 1)), rem)

readAndAdvance :: IORef Int -> Ptr b -> IO Word8
readAndAdvance bufferPos buffer = do
    pos <- readIORef bufferPos
    b <- peekByteOff buffer pos :: IO Word8
    modifyIORef bufferPos (+ 1)
    return b

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

readIntFromBuffer :: (Integral a) => Ptr b -> IORef Int -> IO a
readIntFromBuffer buf bufferPos = do
    n <- readVarIntFromBuffer buf bufferPos
    let u = fromIntegral n :: Word32
    return $ fromIntegral $ (fromIntegral (u `shiftR` 1) :: Int32) .^. (-(n .&. 1))

readInt32FromBuffer :: Ptr b -> IORef Int -> IO Int32
readInt32FromBuffer buf bufferPos = do
    n <- (fromIntegral <$> readVarIntFromBuffer @Int64 buf bufferPos) :: IO Int32
    let u = fromIntegral n :: Word32
    return $ (fromIntegral (u `shiftR` 1) :: Int32) .^. (-(n .&. 1))

readString :: Ptr Word8 -> IORef Int -> IO String
readString buf pos = do
    nameSize <- readVarIntFromBuffer @Int buf pos
    map (chr . fromIntegral) <$> replicateM nameSize (readAndAdvance pos buf)

readBytes :: Handle -> Int64 -> Int64 -> IO [Word8]
readBytes h colStart colLen = do
  hSeek h AbsoluteSeek (fromIntegral colStart)
  bs <- BS.hGet h (fromIntegral colLen)
  if BS.length bs /= (fromIntegral colLen)
     then ioError (userError ("short read: wanted "
                              ++ show colLen ++ " bytes, got "
                              ++ show (BS.length bs)))
     else pure (BS.unpack bs)

numBytesInFile :: Handle -> IO Integer
numBytesInFile handle = do
    hSeek handle SeekFromEnd 0
    hTell handle

readByteStringFromBytes :: [Word8] -> ([Word8], [Word8])
readByteStringFromBytes xs =
    let
        (size, rem) = readVarIntFromBytes @Int xs
     in
        (take size rem, drop size rem)

readByteString :: Ptr Word8 -> IORef Int -> IO [Word8]
readByteString buf pos = do
    size <- readVarIntFromBuffer @Int buf pos
    replicateM size (readAndAdvance pos buf)

readByteString' :: Ptr Word8 -> Int64 -> IO [Word8]
readByteString' buf size = mapM (`readSingleByte` buf) [0 .. (size - 1)]

readSingleByte :: Int64 -> Ptr b -> IO Word8
readSingleByte pos buffer = peekByteOff buffer (fromIntegral pos)

readNoAdvance :: IORef Int -> Ptr b -> IO Word8
readNoAdvance bufferPos buffer = do
    pos <- readIORef bufferPos
    peekByteOff buffer pos :: IO Word8
