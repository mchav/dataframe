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
    empty) where

import qualified Data.ByteString.Char8 as C
import qualified Data.Map as M
import qualified Data.Map.Strict as MS
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import Data.DataFrame.Util ( applySnd, showTable, typeMismatchError )
import Data.Function (on)
import Data.List (groupBy, sortBy, elemIndex, transpose)
import Data.Map (Map)
import Data.Maybe (fromMaybe)
import Data.Typeable (Typeable)
import Data.Vector (Vector)
import Data.Type.Equality
    ( type (:~:)(Refl), TestEquality(testEquality) )
import Type.Reflection ( Typeable, TypeRep, typeRep )
import GHC.Stack (HasCallStack)
import Data.Vector.Unboxed (Unbox)

data Column where
    BoxedColumn :: (Typeable a, Show a, Ord a) => Vector a -> Column
    UnboxedColumn :: (Typeable a, Show a, Ord a, Unbox a) => VU.Vector a -> Column

instance Show Column where
    show :: Column -> String
    show (BoxedColumn column) = show column

data DataFrame = DataFrame {
    columns :: Map C.ByteString Column,
    _columnNames :: [C.ByteString]
}

toColumn :: forall a . (Typeable a, Show a, Ord a) => Vector a -> Column
toColumn xs = let
        repa :: Type.Reflection.TypeRep a = Type.Reflection.typeRep @a
        repInt :: Type.Reflection.TypeRep Int = Type.Reflection.typeRep @Int
        repDouble :: Type.Reflection.TypeRep Double = Type.Reflection.typeRep @Double
    in case testEquality repa repInt of
        Just Refl -> UnboxedColumn (VU.convert xs)
        Nothing -> case testEquality repa repDouble of
            Just Refl -> UnboxedColumn (VU.convert xs)
            Nothing -> BoxedColumn xs

toColumnUnboxed :: forall a . (Typeable a, Show a) => VU.Vector a -> Column
toColumnUnboxed xs = let
        repa :: Type.Reflection.TypeRep a = Type.Reflection.typeRep @a
        repInt :: Type.Reflection.TypeRep Int = Type.Reflection.typeRep @Int
        repDouble :: Type.Reflection.TypeRep Double = Type.Reflection.typeRep @Double
    in case testEquality repa repInt of
        Just Refl -> UnboxedColumn (VU.convert xs)
        Nothing -> case testEquality repa repDouble of
            Just Refl -> UnboxedColumn (VU.convert xs)
            Nothing -> error "Value isn't boxed"

empty :: DataFrame
empty = DataFrame { columns = M.empty, _columnNames = [] }

instance Show DataFrame where
    show :: DataFrame -> String
    show d = let
                 header = _columnNames d
                 get (BoxedColumn column) = V.map (C.pack . show) column
                 get (UnboxedColumn column) = V.map (C.pack . show) (V.convert column)
                 getByteStringColumnFromFrame df name = get $ (MS.!) (columns d) name
                 rows = transpose
                      $ map (V.toList . getByteStringColumnFromFrame d) header
             in C.unpack $ showTable header rows
