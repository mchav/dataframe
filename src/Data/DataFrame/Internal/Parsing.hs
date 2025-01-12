{-# LANGUAGE OverloadedStrings #-}
module Data.DataFrame.Internal.Parsing where

import qualified Data.Set as S
import qualified Data.Text as T

import Data.Text.Read
import Data.Maybe (fromMaybe)
import GHC.Stack (HasCallStack)
import Text.Read (readMaybe)

isNullish :: T.Text -> Bool
isNullish s = s `S.member` S.fromList ["Nothing", "NULL", "", " ", "nan"]

readValue :: (HasCallStack, Read a) => T.Text -> a
readValue s = case readMaybe (T.unpack s) of
  Nothing -> error $ "Could not read value: " ++ T.unpack s
  Just value -> value

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

readDouble :: (HasCallStack) => T.Text -> Maybe Double
readDouble s =
  case signed double s of
    Left _ -> Nothing
    Right (value, "") -> Just value
    Right (value, _) -> Nothing

readIntegerEither :: (HasCallStack) => T.Text -> Either T.Text Integer
readIntegerEither s = case signed decimal (T.strip s) of
  Left _ -> Left s
  Right (value, "") -> Right value
  Right (value, _) -> Left s

readIntEither :: (HasCallStack) => T.Text -> Either T.Text Int
readIntEither s = case signed decimal (T.strip s) of
  Left _ -> Left s
  Right (value, "") -> Right value
  Right (value, _) -> Left s

readDoubleEither :: (HasCallStack) => T.Text -> Either T.Text Double
readDoubleEither s =
  case signed double s of
    Left _ -> Left s
    Right (value, "") -> Right value
    Right (value, _) -> Left s

safeReadValue :: (Read a) => T.Text -> Maybe a
safeReadValue s = readMaybe (T.unpack s)

readWithDefault :: (HasCallStack, Read a) => a -> T.Text -> a
readWithDefault v s = fromMaybe v (readMaybe (T.unpack s))
