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
    transformColumn,
    fetchColumn,
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

data Column where
    BoxedColumn :: (Typeable a, Show a, Ord a) => Vector a -> Column
    UnboxedColumn :: (Typeable a, Show a, Ord a, Unbox a) => VU.Vector a -> Column

instance Show Column where
    show :: Column -> String
    show (BoxedColumn column) = show column

data DataFrame = DataFrame {
    columns :: Map T.Text Column,
    _columnNames :: [T.Text]
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
    show d = T.unpack (asText d)

asText :: DataFrame -> T.Text
asText d = let
        header = _columnNames d
        get (MkColumn column) = V.map (T.pack . show) column
        getTextColumnFromFrame df name = get $ (MS.!) (columns d) name
        rows = transpose
            $ map (V.toList . getTextColumnFromFrame d) header
    in showTable header rows
