{-# LANGUAGE OverloadedStrings #-}

module DataFrame.Internal.Parsing where

import qualified Data.ByteString.Char8 as C
import qualified Data.Set as S
import qualified Data.Text as T

import Data.ByteString.Lex.Fractional
import Data.Maybe (fromMaybe)
import Data.Text.Read
import GHC.Stack (HasCallStack)
import Text.Read (readMaybe)

isNullish :: T.Text -> Bool
isNullish =
    ( `S.member`
        S.fromList
            ["Nothing", "NULL", "", " ", "nan", "null", "N/A", "NaN", "NAN", "NA"]
    )

isTrueish :: T.Text -> Bool
isTrueish t = t `elem` ["True", "true", "TRUE"]

isFalseish :: T.Text -> Bool
isFalseish t = t `elem` ["False", "false", "FALSE"]

readValue :: (HasCallStack, Read a) => T.Text -> a
readValue s = case readMaybe (T.unpack s) of
    Nothing -> error $ "Could not read value: " ++ T.unpack s
    Just value -> value

readBool :: (HasCallStack) => T.Text -> Maybe Bool
readBool s
    | isTrueish s = Just True
    | isFalseish s = Just False
    | otherwise = Nothing

readInteger :: (HasCallStack) => T.Text -> Maybe Integer
readInteger s = case signed decimal (T.strip s) of
    Left _ -> Nothing
    Right (value, "") -> Just value
    Right (value, _) -> Nothing

readInt :: (HasCallStack) => T.Text -> Maybe Int
readInt s = case signed decimal (T.strip s) of
    Left _ -> Nothing
    Right (value, "") -> Just value
    Right (value, _) -> Nothing
{-# INLINE readInt #-}

readByteStringInt :: (HasCallStack) => C.ByteString -> Maybe Int
readByteStringInt s = case C.readInt (C.strip s) of
    Nothing -> Nothing
    Just (value, "") -> Just value
    Just (value, _) -> Nothing
{-# INLINE readByteStringInt #-}

readByteStringDouble :: (HasCallStack) => C.ByteString -> Maybe Double
readByteStringDouble s =
    let
        readFunc = if C.any (\c -> c == 'e' || c == 'E') s then readExponential else readDecimal
     in
        case readSigned readFunc (C.strip s) of
            Nothing -> Nothing
            Just (value, "") -> Just value
            Just (value, _) -> Nothing
{-# INLINE readByteStringDouble #-}

readDouble :: (HasCallStack) => T.Text -> Maybe Double
readDouble s =
    case signed double s of
        Left _ -> Nothing
        Right (value, "") -> Just value
        Right (value, _) -> Nothing
{-# INLINE readDouble #-}

readIntegerEither :: (HasCallStack) => T.Text -> Either T.Text Integer
readIntegerEither s = case signed decimal (T.strip s) of
    Left _ -> Left s
    Right (value, "") -> Right value
    Right (value, _) -> Left s
{-# INLINE readIntegerEither #-}

readIntEither :: (HasCallStack) => T.Text -> Either T.Text Int
readIntEither s = case signed decimal (T.strip s) of
    Left _ -> Left s
    Right (value, "") -> Right value
    Right (value, _) -> Left s
{-# INLINE readIntEither #-}

readDoubleEither :: (HasCallStack) => T.Text -> Either T.Text Double
readDoubleEither s =
    case signed double s of
        Left _ -> Left s
        Right (value, "") -> Right value
        Right (value, _) -> Left s
{-# INLINE readDoubleEither #-}

safeReadValue :: (Read a) => T.Text -> Maybe a
safeReadValue s = readMaybe (T.unpack s)

readWithDefault :: (HasCallStack, Read a) => a -> T.Text -> a
readWithDefault v s = fromMaybe v (readMaybe (T.unpack s))
