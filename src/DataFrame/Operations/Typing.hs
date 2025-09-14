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

parseDefaults :: Bool -> DataFrame -> DataFrame
parseDefaults safeRead df = df{columns = V.map (parseDefault safeRead) (columns df)}

parseDefault :: Bool -> Column -> Column
parseDefault safeRead (BoxedColumn (c :: V.Vector a)) =
    let
        parseTimeOpt s = parseTimeM {- Accept leading/trailing whitespace -} True defaultTimeLocale "%Y-%m-%d" (T.unpack s) :: Maybe Day
        unsafeParseTime s = parseTimeOrError {- Accept leading/trailing whitespace -} True defaultTimeLocale "%Y-%m-%d" (T.unpack s) :: Day
     in
        case (typeRep @a) `testEquality` (typeRep @T.Text) of
            Nothing -> case (typeRep @a) `testEquality` (typeRep @String) of
                Just Refl ->
                    let
                        emptyToNothing v = if isNullish (T.pack v) then Nothing else Just v
                        safeVector = V.map emptyToNothing c
                        hasNulls = V.foldl' (\acc v -> if isNothing v then acc || True else acc) False safeVector
                     in
                        if safeRead && hasNulls then BoxedColumn safeVector else BoxedColumn c
                Nothing -> BoxedColumn c
            Just Refl ->
                let example = T.strip (V.head c)
                    emptyToNothing v = if isNullish v then Nothing else Just v
                 in case readInt example of
                        Just _ ->
                            let safeVector = V.map ((=<<) readInt . emptyToNothing) c
                                hasNulls = V.elem Nothing safeVector
                             in if safeRead && hasNulls then BoxedColumn safeVector else UnboxedColumn (VU.generate (V.length c) (fromMaybe 0 . (safeVector V.!)))
                        Nothing -> case readDouble example of
                            Just _ ->
                                let safeVector = V.map ((=<<) readDouble . emptyToNothing) c
                                    hasNulls = V.elem Nothing safeVector
                                 in if safeRead && hasNulls then BoxedColumn safeVector else UnboxedColumn (VU.generate (V.length c) (fromMaybe 0 . (safeVector V.!)))
                            Nothing -> case parseTimeOpt example of
                                Just d ->
                                    let
                                        -- failed parse should be Either, nullish should be Maybe
                                        emptyToNothing' v = if isNullish v then Left v else Right v
                                        parseTimeEither v = case parseTimeOpt v of
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
                                                    then BoxedColumn (V.map toMaybe safeVector)
                                                    else
                                                        if V.any isLeft safeVector
                                                            then BoxedColumn safeVector
                                                            else BoxedColumn (V.map unsafeParseTime c)
                                            else BoxedColumn (V.map unsafeParseTime c)
                                Nothing ->
                                    let
                                        safeVector = V.map emptyToNothing c
                                        hasNulls = V.any isNullish c
                                     in
                                        if safeRead && hasNulls then BoxedColumn safeVector else BoxedColumn c
parseDefault safeRead column = column
