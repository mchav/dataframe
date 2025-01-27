{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE GADTs #-}
module Data.DataFrame.Internal.DataFrame where

import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import Control.Monad (join)
import Data.DataFrame.Display.Terminal.PrettyPrint
import Data.DataFrame.Internal.Column
import Data.Function (on)
import Data.List (sortBy, transpose)
import Data.Maybe (isJust)
import Data.Type.Equality (type (:~:)(Refl), TestEquality (testEquality))
import Type.Reflection (typeRep)

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

instance Eq DataFrame where
  (==) :: DataFrame -> DataFrame -> Bool
  a == b = map fst (M.toList $ columnIndices a) == map fst (M.toList $ columnIndices b) &&
           foldr (\(name, index) acc -> acc && (columns a V.!? index == (columns b V.!? (columnIndices b M.! name)))) True (M.toList $ columnIndices a)

instance Show DataFrame where
  show :: DataFrame -> String
  show d = T.unpack (asText d)

asText :: DataFrame -> T.Text
asText d =
  let header = "index" : map fst (sortBy (compare `on` snd) $ M.toList (columnIndices d))
      types = V.toList $ V.filter (/= "") $ V.map getType (columns d)
      getType Nothing = ""
      getType (Just (BoxedColumn (column :: V.Vector a))) = T.pack $ show (typeRep @a)
      getType (Just (UnboxedColumn (column :: VU.Vector a))) = T.pack $ show (typeRep @a)
      getType (Just (OptionalColumn (column :: V.Vector a))) = T.pack $ show (typeRep @a)
      getType (Just (GroupedBoxedColumn (column :: V.Vector a))) = T.pack $ show (typeRep @a)
      getType (Just (GroupedUnboxedColumn (column :: V.Vector a))) = T.pack $ show (typeRep @a)
      -- Separate out cases dynamically so we don't end up making round trip string
      -- copies.
      get (Just (BoxedColumn (column :: V.Vector a))) = case testEquality (typeRep @a) (typeRep @T.Text) of
              Just Refl -> column
              Nothing -> case testEquality (typeRep @a) (typeRep @String) of
                Just Refl -> V.map T.pack column
                Nothing -> V.map (T.pack . show) column
      get (Just (UnboxedColumn column)) = V.map (T.pack . show) (V.convert column)
      get (Just (OptionalColumn column)) = V.map (T.pack . show) column
      get (Just (GroupedBoxedColumn column)) = V.map (T.pack . show) column
      get (Just (GroupedUnboxedColumn column)) = V.map (T.pack . show) column
      getTextColumnFromFrame df (i, name) = if i == 0
                                            then V.fromList (map (T.pack . show) [0..(fst (dataframeDimensions df) - 1)])
                                            else get $ (V.!) (columns d) ((M.!) (columnIndices d) name)
      rows =
        transpose $
          zipWith (curry (V.toList . getTextColumnFromFrame d)) [0..] header
   in showTable header ("Int":types) rows

-- | O(1) Creates an empty dataframe
empty :: DataFrame
empty = DataFrame {columns = V.replicate initialColumnSize Nothing,
                   columnIndices = M.empty,
                   freeIndices = [0..(initialColumnSize - 1)],
                   dataframeDimensions = (0, 0) }

initialColumnSize :: Int
initialColumnSize = 8

getColumn :: T.Text -> DataFrame -> Maybe Column
getColumn name df = do
  i <- columnIndices df M.!? name
  join $ columns df V.!? i

null :: DataFrame -> Bool
null df = dataframeDimensions df == (0, 0)

metadata :: DataFrame -> String
metadata df = show (columnIndices df) ++ "\n" ++
              show (V.map isJust (columns df)) ++ "\n" ++
              show (freeIndices df) ++ "\n" ++
              show (dataframeDimensions df)
