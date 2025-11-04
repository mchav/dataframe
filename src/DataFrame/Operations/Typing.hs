{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Operations.Typing where

import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import Control.Monad ((<=<))
import Data.Maybe
import Data.Time
import Data.Type.Equality (TestEquality (..), type (:~:) (Refl))
import DataFrame.Internal.Column (Column (..))
import DataFrame.Internal.DataFrame (DataFrame (..))
import DataFrame.Internal.Parsing
import Type.Reflection (typeRep)

parseDefaults :: Int -> Bool -> String -> DataFrame -> DataFrame
parseDefaults n safeRead dateFormat df = df{columns = V.map (parseDefault n safeRead dateFormat) (columns df)}

parseDefault :: Int -> Bool -> String -> Column -> Column
parseDefault n safeRead dateFormat (BoxedColumn (c :: V.Vector a)) =
    case (typeRep @a) `testEquality` (typeRep @T.Text) of
        Nothing -> case (typeRep @a) `testEquality` (typeRep @String) of
            Just Refl -> parseFromExamples n dateFormat safeRead (V.map T.pack c)
            Nothing -> BoxedColumn c
        Just Refl -> parseFromExamples n dateFormat safeRead c
parseDefault n safeRead dateFormat (OptionalColumn (c :: V.Vector (Maybe a))) =
    case (typeRep @a) `testEquality` (typeRep @T.Text) of
        Nothing -> case (typeRep @a) `testEquality` (typeRep @String) of
            Just Refl -> parseFromExamples n dateFormat safeRead (V.map (T.pack . fromMaybe "") c)
            Nothing -> BoxedColumn c
        Just Refl -> parseFromExamples n dateFormat safeRead (V.map (fromMaybe "") c)
parseDefault _ _ _ column = column

parseFromExamples :: Int -> String -> Bool -> V.Vector T.Text -> Column
parseFromExamples n dateFormat safeRead c
    | V.any isJust potentialInts && equalNonEmpty potentialInts potentialDoubles =
        let safelyParsed = V.map (readInt <=< emptyToNothing) c
            hasNulls = V.any isNothing safelyParsed
         in if safeRead && hasNulls
                then OptionalColumn safelyParsed
                else UnboxedColumn (VU.generate (V.length c) (fromMaybe 0 . (safelyParsed V.!)))
    | V.any isJust potentialDoubles =
        let safelyParsed = V.map (readDouble <=< emptyToNothing) c
            hasNulls = V.any isNothing safelyParsed
         in if safeRead && hasNulls
                then OptionalColumn safelyParsed
                else UnboxedColumn (VU.generate (V.length c) (fromMaybe 0 . (safelyParsed V.!)))
    | V.any isJust potentialDates =
        let safelyParsed = V.map (parseTimeOpt dateFormat <=< emptyToNothing) c
            hasNulls = V.any isNothing safelyParsed
         in if safeRead && hasNulls
                then OptionalColumn safelyParsed
                else BoxedColumn (V.map (unsafeParseTime dateFormat) c)
    | otherwise =
        let safelyParsed = V.map (pure <=< emptyToNothing) c
            hasNulls = V.any isNothing safelyParsed
         in if safeRead && hasNulls then OptionalColumn safelyParsed else BoxedColumn c
  where
    examples = V.take n c
    potentialInts = V.map readInt examples
    potentialDoubles = V.map readDouble examples
    potentialDates = V.map (parseTimeOpt dateFormat) examples

emptyToNothing :: T.Text -> Maybe T.Text
emptyToNothing v = if isNullish v then Nothing else Just v

parseTimeOpt :: String -> T.Text -> Maybe Day
parseTimeOpt dateFormat s =
    parseTimeM {- Accept leading/trailing whitespace -}
        True
        defaultTimeLocale
        dateFormat
        (T.unpack s)

unsafeParseTime :: String -> T.Text -> Day
unsafeParseTime dateFormat s =
    parseTimeOrError {- Accept leading/trailing whitespace -}
        True
        defaultTimeLocale
        dateFormat
        (T.unpack s)

equalNonEmpty :: V.Vector (Maybe a) -> V.Vector (Maybe b) -> Bool
equalNonEmpty xs ys = (V.length xs == V.length ys) && V.and (V.zipWith hasSameConstructor xs ys)
  where
    hasSameConstructor :: Maybe a -> Maybe b -> Bool
    hasSameConstructor (Just _) (Just _) = True
    hasSameConstructor Nothing Nothing = True
    hasSameConstructor _ _ = False
