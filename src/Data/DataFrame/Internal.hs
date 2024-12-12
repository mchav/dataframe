{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE TypeApplications #-}

module Data.DataFrame.Internal (
    DataFrame(..),
    Column(..),
    toColumn,
    toColumnUnboxed,
    empty,
    asText) where

import qualified Data.Map as M
import qualified Data.Map.Strict as MS
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import Data.DataFrame.Util ( applySnd, showTable, typeMismatchError )
import Data.Function (on)
import GHC.Stack (HasCallStack)
import Data.List (groupBy, sortBy, elemIndex, transpose)
import Data.Map (Map)
import Data.Maybe (fromMaybe)
import Data.Typeable (Typeable)
import Data.Vector (Vector)
import Data.Vector.Unboxed (Unbox)
import Data.Type.Equality
    ( type (:~:)(Refl), TestEquality(testEquality) )
import Type.Reflection ( Typeable, TypeRep, typeRep )

-- | Our representation of a column is a GADT that can store data in either
-- a vector with boxed elements or 
data Column where
    BoxedColumn :: (Typeable a, Show a, Ord a) => Vector a -> Column
    UnboxedColumn :: (Typeable a, Show a, Ord a, Unbox a) => VU.Vector a -> Column

instance Show Column where
    show :: Column -> String
    show (BoxedColumn column) = show column

data DataFrame = DataFrame {
    -- | Our main data structure stores a dataframe as
    -- a map of columns.
    columns :: Map T.Text Column,
    -- | Keeps the column names in the order they were inserted in.
    _columnNames :: [T.Text]
}

-- | Converts a an unboxed vector to a column making sure to put
-- the vector into an appropriate column type by reflection on the
-- vector's type parameter.
toColumn :: forall a . (Typeable a, Show a, Ord a) => Vector a -> Column
toColumn xs = case testEquality (typeRep @a) (typeRep @Int) of
    Just Refl -> UnboxedColumn (VU.convert xs)
    Nothing -> case testEquality (typeRep @a) (typeRep @Double) of
        Just Refl -> UnboxedColumn (VU.convert xs)
        Nothing -> BoxedColumn xs

-- | Converts a an unboxed vector to a column making sure to put
-- the vector into an appropriate column type by reflection on the
-- vector's type parameter.
toColumnUnboxed :: forall a . (Typeable a, Show a, Ord a, Unbox a) => VU.Vector a -> Column
toColumnUnboxed = UnboxedColumn

-- | O(1) Creates an empty dataframe
empty :: DataFrame
empty = DataFrame { columns = M.empty, _columnNames = [] }

instance Show DataFrame where
    show :: DataFrame -> String
    show d = T.unpack (asText d)

asText :: DataFrame -> T.Text
asText d = let
        header = _columnNames d
        -- Separate out cases dynamically so we don't end up making round trip string
        -- copies.
        get (BoxedColumn (column :: Vector a)) = let
                repa :: Type.Reflection.TypeRep a = Type.Reflection.typeRep @a
                repString :: Type.Reflection.TypeRep String = Type.Reflection.typeRep @String
                repText :: Type.Reflection.TypeRep T.Text = Type.Reflection.typeRep @T.Text
            in case testEquality repa repText of
                Just Refl -> column
                Nothing -> case testEquality repa repString of
                    Just Refl -> V.map T.pack column
                    Nothing -> V.map (T.pack . show) column
        get (UnboxedColumn column) = V.map (T.pack . show) (V.convert column)
        getTextColumnFromFrame df name = get $ (MS.!) (columns d) name
        rows = transpose
            $ map (V.toList . getTextColumnFromFrame d) header
    in showTable header rows
