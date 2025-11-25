{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Operations.Typing where

import qualified Data.Text as T
import qualified Data.Vector as V

import Data.Maybe (fromMaybe)
import qualified Data.Proxy as P
import Data.Time
import Data.Type.Equality (TestEquality (..), type (:~:) (Refl))
import DataFrame.Internal.Column (Column (..), fromVector)
import DataFrame.Internal.DataFrame (DataFrame (..))
import DataFrame.Internal.Parsing
import DataFrame.Internal.Schema
import Text.Read
import Type.Reflection (typeRep)

type DateFormat = String

parseDefaults :: Int -> Bool -> DateFormat -> DataFrame -> DataFrame
parseDefaults n safeRead dateFormat df = df{columns = V.map (parseDefault n safeRead dateFormat) (columns df)}

parseDefault :: Int -> Bool -> DateFormat -> Column -> Column
parseDefault n safeRead dateFormat (BoxedColumn (c :: V.Vector a)) =
    case (typeRep @a) `testEquality` (typeRep @T.Text) of
        Nothing -> case (typeRep @a) `testEquality` (typeRep @String) of
            Just Refl -> parseFromExamples n safeRead dateFormat (V.map T.pack c)
            Nothing -> BoxedColumn c
        Just Refl -> parseFromExamples n safeRead dateFormat c
parseDefault n safeRead dateFormat (OptionalColumn (c :: V.Vector (Maybe a))) =
    case (typeRep @a) `testEquality` (typeRep @T.Text) of
        Nothing -> case (typeRep @a) `testEquality` (typeRep @String) of
            Just Refl -> parseFromExamples n safeRead dateFormat (V.map (T.pack . fromMaybe "") c)
            Nothing -> BoxedColumn c
        Just Refl -> parseFromExamples n safeRead dateFormat (V.map (fromMaybe "") c)
parseDefault _ _ _ column = column

parseFromExamples :: Int -> Bool -> DateFormat -> V.Vector T.Text -> Column
parseFromExamples n safeRead dateFormat cols =
    let
        converter = if safeRead then convertNullish else convertOnlyEmpty
        examples = V.map converter (V.take n cols)
        asMaybeText = V.map converter cols
     in
        case makeParsingAssumption dateFormat examples of
            BoolAssumption -> handleBoolAssumption asMaybeText
            IntAssumption -> handleIntAssumption asMaybeText
            DoubleAssumption -> handleDoubleAssumption asMaybeText
            TextAssumption -> handleTextAssumption asMaybeText
            DateAssumption -> handleDateAssumption dateFormat asMaybeText
            NoAssumption -> handleNoAssumption dateFormat asMaybeText

handleBoolAssumption :: V.Vector (Maybe T.Text) -> Column
handleBoolAssumption asMaybeText
    | parsableAsBool =
        maybe (fromVector asMaybeBool) fromVector (sequenceA asMaybeBool)
    | otherwise = maybe (fromVector asMaybeText) fromVector (sequenceA asMaybeText)
  where
    asMaybeBool = V.map (>>= readBool) asMaybeText
    parsableAsBool = vecSameConstructor asMaybeText asMaybeBool

handleIntAssumption :: V.Vector (Maybe T.Text) -> Column
handleIntAssumption asMaybeText
    | parsableAsInt =
        maybe (fromVector asMaybeInt) fromVector (sequenceA asMaybeInt)
    | parsableAsDouble =
        maybe (fromVector asMaybeDouble) fromVector (sequenceA asMaybeDouble)
    | otherwise = maybe (fromVector asMaybeText) fromVector (sequenceA asMaybeText)
  where
    asMaybeInt = V.map (>>= readInt) asMaybeText
    asMaybeDouble = V.map (>>= readDouble) asMaybeText
    parsableAsInt =
        vecSameConstructor asMaybeText asMaybeInt
            && vecSameConstructor asMaybeText asMaybeDouble
    parsableAsDouble = vecSameConstructor asMaybeText asMaybeDouble

handleDoubleAssumption :: V.Vector (Maybe T.Text) -> Column
handleDoubleAssumption asMaybeText
    | parsableAsDouble =
        maybe (fromVector asMaybeDouble) fromVector (sequenceA asMaybeDouble)
    | otherwise = maybe (fromVector asMaybeText) fromVector (sequenceA asMaybeText)
  where
    asMaybeDouble = V.map (>>= readDouble) asMaybeText
    parsableAsDouble = vecSameConstructor asMaybeText asMaybeDouble

handleDateAssumption :: DateFormat -> V.Vector (Maybe T.Text) -> Column
handleDateAssumption dateFormat asMaybeText
    | parsableAsDate =
        maybe (fromVector asMaybeDate) fromVector (sequenceA asMaybeDate)
    | otherwise = maybe (fromVector asMaybeText) fromVector (sequenceA asMaybeText)
  where
    asMaybeDate = V.map (>>= parseTimeOpt dateFormat) asMaybeText
    parsableAsDate = vecSameConstructor asMaybeText asMaybeDate

handleTextAssumption :: V.Vector (Maybe T.Text) -> Column
handleTextAssumption asMaybeText = maybe (fromVector asMaybeText) fromVector (sequenceA asMaybeText)

handleNoAssumption :: DateFormat -> V.Vector (Maybe T.Text) -> Column
handleNoAssumption dateFormat asMaybeText
    -- No need to check for null values. If we are in this condition, that
    -- means that the examples consisted only of null values, so we can
    -- confidently know that this column must be an OptionalColumn
    | V.all (== Nothing) asMaybeText = fromVector asMaybeText
    | parsableAsBool = fromVector asMaybeBool
    | parsableAsInt = fromVector asMaybeInt
    | parsableAsDouble = fromVector asMaybeDouble
    | parsableAsDate = fromVector asMaybeDate
    | otherwise = fromVector asMaybeText
  where
    asMaybeBool = V.map (>>= readBool) asMaybeText
    asMaybeInt = V.map (>>= readInt) asMaybeText
    asMaybeDouble = V.map (>>= readDouble) asMaybeText
    asMaybeDate = V.map (>>= parseTimeOpt dateFormat) asMaybeText
    parsableAsBool = vecSameConstructor asMaybeText asMaybeBool
    parsableAsInt =
        vecSameConstructor asMaybeText asMaybeInt
            && vecSameConstructor asMaybeText asMaybeDouble
    parsableAsDouble = vecSameConstructor asMaybeText asMaybeDouble
    parsableAsDate = vecSameConstructor asMaybeText asMaybeDate

convertNullish :: T.Text -> Maybe T.Text
convertNullish v = if isNullish v then Nothing else Just v

convertOnlyEmpty :: T.Text -> Maybe T.Text
convertOnlyEmpty v = if v == "" then Nothing else Just v

parseTimeOpt :: DateFormat -> T.Text -> Maybe Day
parseTimeOpt dateFormat s =
    parseTimeM {- Accept leading/trailing whitespace -}
        True
        defaultTimeLocale
        dateFormat
        (T.unpack s)

unsafeParseTime :: DateFormat -> T.Text -> Day
unsafeParseTime dateFormat s =
    parseTimeOrError {- Accept leading/trailing whitespace -}
        True
        defaultTimeLocale
        dateFormat
        (T.unpack s)

hasNullValues :: (Eq a) => V.Vector (Maybe a) -> Bool
hasNullValues = V.any (== Nothing)

vecSameConstructor :: V.Vector (Maybe a) -> V.Vector (Maybe b) -> Bool
vecSameConstructor xs ys = (V.length xs == V.length ys) && V.and (V.zipWith hasSameConstructor xs ys)
  where
    hasSameConstructor :: Maybe a -> Maybe b -> Bool
    hasSameConstructor (Just _) (Just _) = True
    hasSameConstructor Nothing Nothing = True
    hasSameConstructor _ _ = False

makeParsingAssumption ::
    DateFormat -> V.Vector (Maybe T.Text) -> ParsingAssumption
makeParsingAssumption dateFormat asMaybeText
    -- All the examples are "NA", "Null", "", so we can't make any shortcut
    -- assumptions and just have to go the long way.
    | V.all (== Nothing) asMaybeText = NoAssumption
    -- After accounting for nulls, parsing for Ints and Doubles results in the
    -- same corresponding positions of Justs and Nothings, so we assume
    -- that the best way to parse is Int
    | vecSameConstructor asMaybeText asMaybeBool = BoolAssumption
    | vecSameConstructor asMaybeText asMaybeInt
        && vecSameConstructor asMaybeText asMaybeDouble =
        IntAssumption
    -- After accounting for nulls, the previous condition fails, so some (or none) can be parsed as Ints
    -- and some can be parsed as Doubles, so we make the assumpotion of doubles.
    | vecSameConstructor asMaybeText asMaybeDouble = DoubleAssumption
    -- After accounting for nulls, parsing for Dates results in the same corresponding
    -- positions of Justs and Nothings, so we assume that the best way to parse is Date.
    | vecSameConstructor asMaybeText asMaybeDate = DateAssumption
    | otherwise = TextAssumption
  where
    asMaybeBool = V.map (>>= readBool) asMaybeText
    asMaybeInt = V.map (>>= readInt) asMaybeText
    asMaybeDouble = V.map (>>= readDouble) asMaybeText
    asMaybeDate = V.map (>>= parseTimeOpt dateFormat) asMaybeText

data ParsingAssumption
    = BoolAssumption
    | IntAssumption
    | DoubleAssumption
    | DateAssumption
    | NoAssumption
    | TextAssumption

parseWithTypes :: [SchemaType] -> DataFrame -> DataFrame
parseWithTypes ts df = df{columns = go 0 ts (columns df)}
  where
    go :: Int -> [SchemaType] -> V.Vector Column -> V.Vector Column
    go n [] xs = xs
    go n (t : rest) xs
        | n >= V.length xs = xs
        | otherwise =
            go (n + 1) rest (V.update xs (V.fromList [(n, asType t (xs V.! n))]))
    asType :: SchemaType -> Column -> Column
    asType (SType (_ :: P.Proxy a)) c@(BoxedColumn (col :: V.Vector b)) = case testEquality (typeRep @a) (typeRep @b) of
        Just Refl -> c
        Nothing -> case testEquality (typeRep @T.Text) (typeRep @b) of
            Just Refl -> fromVector (V.map ((readMaybe @a) . T.unpack) col)
            Nothing -> fromVector (V.map ((readMaybe @a) . show) col)
    asType _ c = c
