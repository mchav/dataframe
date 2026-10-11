{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Typed.Freeze (
    -- * Safe boundary
    freeze,
    freezeWithError,
    freezeOrThrow,

    -- * Escape hatches
    thaw,
    unsafeFreeze,

    -- * Frame coercion
    ToDataFrame (..),
) where

import Control.Exception (throwIO)
import qualified Data.Text as T
import Type.Reflection (SomeTypeRep)

import Data.List (stripPrefix)
import DataFrame.Errors (DataFrameException (InternalException))
import qualified DataFrame.Internal.Column as C
import DataFrame.Internal.DataFrame (columnNames)
import qualified DataFrame.Internal.DataFrame as D
import DataFrame.Typed.Schema (KnownSchema (..))
import DataFrame.Typed.Types (TypedDataFrame (..))

-- | Validate an untyped 'DataFrame' against schema @cols@ and wrap it.
freeze ::
    forall cols. (KnownSchema cols) => D.DataFrame -> Maybe (TypedDataFrame cols)
freeze df = case validateSchema @cols df of
    Left _ -> Nothing
    Right _ -> Just (TDF df)

-- | Like 'freeze' but returns a descriptive error message on failure.
freezeWithError ::
    forall cols.
    (KnownSchema cols) =>
    D.DataFrame -> Either T.Text (TypedDataFrame cols)
freezeWithError df = case validateSchema @cols df of
    Left err -> Left err
    Right _ -> Right (TDF df)

-- | Like 'freezeWithError' but throws a 'DataFrameException' on mismatch.
freezeOrThrow ::
    forall cols. (KnownSchema cols) => D.DataFrame -> IO (TypedDataFrame cols)
freezeOrThrow = either (throwIO . InternalException) pure . freezeWithError @cols

-- | Unwrap a typed DataFrame back to the untyped representation.
thaw :: TypedDataFrame cols -> D.DataFrame
thaw (TDF df) = df

class ToDataFrame f where
    toDataFrame :: f -> D.DataFrame

instance ToDataFrame D.DataFrame where
    toDataFrame = id

instance ToDataFrame (TypedDataFrame cols) where
    toDataFrame = thaw

-- | Wrap an untyped DataFrame without any validation.
unsafeFreeze :: D.DataFrame -> TypedDataFrame cols
unsafeFreeze = TDF

validateSchema ::
    forall cols.
    (KnownSchema cols) =>
    D.DataFrame -> Either T.Text ()
validateSchema df = mapM_ checkCol (schemaEvidence @cols)
  where
    checkCol :: (T.Text, SomeTypeRep) -> Either T.Text ()
    checkCol (name, expectedRep) = case D.getColumn name df of
        Nothing ->
            Left $
                "Column '"
                    <> name
                    <> "' not found in DataFrame. "
                    <> "Available columns: "
                    <> T.pack (show (columnNames df))
        Just col ->
            if matchesType expectedRep col
                then Right ()
                else
                    Left $
                        "Type mismatch on column '"
                            <> name
                            <> "': expected "
                            <> T.pack (show expectedRep)
                            <> ", got "
                            <> T.pack (C.columnTypeString col)

-- | Check that a Column's element type matches the expected SomeTypeRep.
matchesType :: SomeTypeRep -> C.Column -> Bool
matchesType expected col =
    let expectedStr = show expected
        colTypeStr = C.columnTypeString col
     in expectedStr == colTypeStr
            -- A nullable column stores "X" under a "Maybe X" schema type.
            || ( C.hasMissing col
                    && Just colTypeStr == stripPrefix "Maybe " expectedStr
               )
