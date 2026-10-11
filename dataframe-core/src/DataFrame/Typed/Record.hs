{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}

-- | Bridge a Haskell record type to a typed dataframe schema.
module DataFrame.Typed.Record (
    -- * Class
    HasSchema (..),

    -- * Untyped helpers
    fromRecords,
    toRecords,

    -- * Typed helpers
    fromRecordsTyped,
    toRecordsTyped,

    -- * Helpers used by generated code
    requireColumn,
) where

import Data.Kind (Type)
import qualified Data.Text as T
import qualified Data.Vector as VB

import qualified DataFrame.Internal.Column as C
import DataFrame.Internal.DataFrame (fromColumns)
import qualified DataFrame.Internal.DataFrame as D
import DataFrame.Typed.Types (TypedDataFrame (..))
import GHC.TypeLits

-- | Bridge a Haskell record type to a typed-dataframe schema.
class HasSchema a where
    type Schema a :: [(Symbol, Type)]
    recordsToColumns :: [a] -> [(T.Text, C.Column)]
    recordsFromFrame :: D.DataFrame -> Either T.Text [a]

-- | Build an untyped 'D.DataFrame' from a list of records.
fromRecords :: (HasSchema a) => [a] -> D.DataFrame
fromRecords = fromColumns . recordsToColumns

-- | Parse a list of records out of an untyped 'D.DataFrame'.
toRecords :: (HasSchema a) => D.DataFrame -> Either T.Text [a]
toRecords = recordsFromFrame

-- | Like 'fromRecords' but returns a 'TypedDataFrame' tagged with the schema.
fromRecordsTyped :: forall a. (HasSchema a) => [a] -> TypedDataFrame (Schema a)
fromRecordsTyped = TDF . fromRecords

-- | Like 'toRecords' but accepts a 'TypedDataFrame'.
toRecordsTyped ::
    forall a. (HasSchema a) => TypedDataFrame (Schema a) -> Either T.Text [a]
toRecordsTyped (TDF df) = recordsFromFrame df

-- | Extract a column as a boxed vector by name, or a 'T.Text' error.
requireColumn ::
    forall a.
    (C.Columnable a) => T.Text -> D.DataFrame -> Either T.Text (VB.Vector a)
requireColumn name df = case D.getColumn name df of
    Nothing ->
        Left $ "Column '" <> name <> "' not found in DataFrame"
    Just col -> case C.toVector col of
        Right v -> Right v
        Left e ->
            Left $
                "Column '" <> name <> "': " <> T.pack (show e)
