{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Data.DataFrame.Internal
  ( DataFrame (..),
    Column (..),
    toColumn,
    toColumnUnboxed,
    empty,
    asText,
    isEmpty,
    columnLength,
    metadata
  )
where

import qualified Data.Map as M
import qualified Data.Map.Strict as MS
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Mutable as VM
import qualified Data.Vector.Unboxed as VU

import Data.DataFrame.Util (applySnd, showTable, typeMismatchError, readInt)
import Data.Function (on)
import Data.List (elemIndex, groupBy, sortBy, transpose)
import Data.Map (Map)
import Data.Maybe (fromMaybe, isJust)
import Data.Type.Equality
  ( TestEquality (testEquality),
    type (:~:) (Refl),
  )
import Data.Typeable (Typeable)
import Data.Vector (Vector)
import Data.Vector.Unboxed (Unbox)
import GHC.Stack (HasCallStack)
import Type.Reflection (TypeRep, Typeable, typeRep)

initialColumnSize :: Int
initialColumnSize = 128

-- | Our representation of a column is a GADT that can store data in either
-- a vector with boxed elements or
data Column where
  BoxedColumn :: (Typeable a, Show a, Ord a) => Vector a -> Column
  UnboxedColumn :: (Typeable a, Show a, Ord a, Unbox a) => VU.Vector a -> Column

instance Show Column where
  show :: Column -> String
  show (BoxedColumn column) = show column
  show (UnboxedColumn column) = show column

data DataFrame = DataFrame
  { -- | Our main data structure stores a dataframe as
    -- a vector of columns. This improv
    columns :: V.Vector (Maybe Column),
    -- | Keeps the column names in the order they were inserted in.
    columnIndices :: M.Map T.Text Int,
    -- | Next free index that we insert a column into.
    freeIndices :: [Int],
    dataframeDimensions :: (Int, Int)
  }

isEmpty :: DataFrame -> Bool
isEmpty df = dataframeDimensions df == (0, 0)

-- | Converts a an unboxed vector to a column making sure to put
-- the vector into an appropriate column type by reflection on the
-- vector's type parameter.
toColumn :: forall a. (Typeable a, Show a, Ord a) => Vector a -> Column
toColumn xs = case testEquality (typeRep @a) (typeRep @Int) of
  Just Refl -> UnboxedColumn (VU.convert xs)
  Nothing -> case testEquality (typeRep @a) (typeRep @Double) of
    Just Refl -> UnboxedColumn (VU.convert xs)
    Nothing -> BoxedColumn xs

-- | O(1) Gets the number of elements in the column.
columnLength :: Column -> Int
columnLength (BoxedColumn xs) = VG.length xs
columnLength (UnboxedColumn xs) = VG.length xs

-- | Converts a an unboxed vector to a column making sure to put
-- the vector into an appropriate column type by reflection on the
-- vector's type parameter.
toColumnUnboxed :: forall a. (Typeable a, Show a, Ord a, Unbox a) => VU.Vector a -> Column
toColumnUnboxed = UnboxedColumn

-- | O(1) Creates an empty dataframe
empty :: DataFrame
empty = DataFrame {columns = V.replicate initialColumnSize Nothing,
                   columnIndices = M.empty,
                   freeIndices = [0..(initialColumnSize - 1)],
                   dataframeDimensions = (0, 0) }

instance Show DataFrame where
  show :: DataFrame -> String
  show d = T.unpack (asText d)

asText :: DataFrame -> T.Text
asText d =
  let header = "index" : map fst (sortBy (compare `on` snd) $ M.toList (columnIndices d))
      types = V.toList $ V.filter (/= "") $ V.map getType (columns d)
      getType Nothing = ""
      getType (Just (BoxedColumn (column :: Vector a))) = T.pack $ show (Type.Reflection.typeRep @a)
      getType (Just (UnboxedColumn (column :: VU.Vector a))) = T.pack $ show (Type.Reflection.typeRep @a)
      -- Separate out cases dynamically so we don't end up making round trip string
      -- copies.
      get (Just (BoxedColumn (column :: Vector a))) =
        let repa :: Type.Reflection.TypeRep a = Type.Reflection.typeRep @a
            repString :: Type.Reflection.TypeRep String = Type.Reflection.typeRep @String
            repText :: Type.Reflection.TypeRep T.Text = Type.Reflection.typeRep @T.Text
         in case testEquality repa repText of
              Just Refl -> column
              Nothing -> case testEquality repa repString of
                Just Refl -> V.map T.pack column
                Nothing -> V.map (T.pack . show) column
      get (Just (UnboxedColumn column)) = V.map (T.pack . show) (V.convert column)
      getTextColumnFromFrame df (i, name) = if i == 0
                                            then V.fromList (map (T.pack . show) [0..(fst (dataframeDimensions df) - 1)])
                                            else get $ (V.!) (columns d) ((M.!) (columnIndices d) name)
      rows =
        transpose $
          zipWith (curry (V.toList . getTextColumnFromFrame d)) [0..] header
   in showTable header ("Int":types) rows

metadata :: DataFrame -> String
metadata df = show (columnIndices df) ++ "\n" ++
              show (V.map isJust (columns df)) ++ "\n" ++
              show (freeIndices df) ++ "\n" ++
              show (dataframeDimensions df)