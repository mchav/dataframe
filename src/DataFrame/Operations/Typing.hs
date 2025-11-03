{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Operations.Typing where

import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import Data.Either
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
    | V.any isJust (V.map readInt examples)
        && equalNonEmpty (V.map readInt examples) (V.map readDouble examples) =
        let safeVector = V.map ((=<<) readInt . emptyToNothing) c
            hasNulls = V.elem Nothing safeVector
         in if safeRead && hasNulls
                then OptionalColumn safeVector
                else UnboxedColumn (VU.generate (V.length c) (fromMaybe 0 . (safeVector V.!)))
    | V.any isJust (V.map readDouble examples) =
        let safeVector = V.map ((=<<) readDouble . emptyToNothing) c
            hasNulls = V.elem Nothing safeVector
         in if safeRead && hasNulls
                then OptionalColumn safeVector
                else UnboxedColumn (VU.generate (V.length c) (fromMaybe 0 . (safeVector V.!)))
    | V.any isJust (V.map (parseTimeOpt dateFormat) examples) =
        let
            -- failed parse should be Either, nullish should be Maybe
            emptyToNothing' v = if isNullish v then Left v else Right v
            parseTimeEither v = case parseTimeOpt dateFormat v of
                Just v' -> Right v'
                Nothing -> Left v
            safeVector = V.map ((=<<) parseTimeEither . emptyToNothing') c
            toMaybe (Left _) = Nothing
            toMaybe (Right value) = Just value
            lefts = V.filter isLeft safeVector
            onlyNulls = (not (V.null lefts) && V.all (isNullish . fromLeft "non-null") lefts)
         in
            if safeRead
                then
                    if onlyNulls
                        then OptionalColumn (V.map toMaybe safeVector)
                        else
                            if V.any isLeft safeVector
                                then BoxedColumn safeVector
                                else BoxedColumn (V.map (unsafeParseTime dateFormat) c)
                else BoxedColumn (V.map (unsafeParseTime dateFormat) c)
    | otherwise =
        let
            safeVector = V.map emptyToNothing c
            hasNulls = V.any isNullish c
         in
            if safeRead && hasNulls then OptionalColumn safeVector else BoxedColumn c
  where
    examples = V.take n c

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

countNonEmpty :: V.Vector (Maybe a) -> Int
countNonEmpty xs = V.length (V.filter isJust xs)

equalNonEmpty :: V.Vector (Maybe a) -> V.Vector (Maybe b) -> Bool
equalNonEmpty xs ys = countNonEmpty xs == countNonEmpty ys
