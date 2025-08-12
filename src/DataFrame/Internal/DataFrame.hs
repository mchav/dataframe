{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE FlexibleContexts #-}
module DataFrame.Internal.DataFrame where

import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Generic as VG

import Control.Monad (join)
import DataFrame.Display.Terminal.PrettyPrint
import DataFrame.Internal.Column
import Data.Function (on)
import Data.List (sortBy, transpose, (\\))
import Data.Maybe (isJust)
import Data.Type.Equality (type (:~:)(Refl), TestEquality (testEquality))
import Text.Printf
import Type.Reflection (typeRep)

data DataFrame = DataFrame
  { -- | Our main data structure stores a dataframe as
    -- a vector of columns. This improv
    columns :: V.Vector Column,
    -- | Keeps the column names in the order they were inserted in.
    columnIndices :: M.Map T.Text Int,
    dataframeDimensions :: (Int, Int)
  }

data GroupedDataFrame = Grouped {
  fullDataframe :: DataFrame,
  groupedColumns :: [T.Text],
  valueIndices :: VU.Vector Int,
  offsets :: VU.Vector Int
}

instance Show GroupedDataFrame where
  show (Grouped df cols indices os) = printf "{ keyColumns: %s groupedColumns: %s }" 
                                             (show cols)
                                             (show ((M.keys (columnIndices df)) \\ cols))

instance Eq GroupedDataFrame where
  (==) (Grouped df cols indices os) (Grouped df' cols' indices' os') = (df == df') && (cols == cols') 


instance Eq DataFrame where
  (==) :: DataFrame -> DataFrame -> Bool
  a == b = map fst (M.toList $ columnIndices a) == map fst (M.toList $ columnIndices b) &&
           foldr (\(name, index) acc -> acc && (columns a V.!? index == (columns b V.!? (columnIndices b M.! name)))) True (M.toList $ columnIndices a)

instance Show DataFrame where
  show :: DataFrame -> String
  show d = T.unpack (asText d False)

asText :: DataFrame -> Bool -> T.Text
asText d properMarkdown =
  let header = "index" : map fst (sortBy (compare `on` snd) $ M.toList (columnIndices d))
      types = V.toList $ V.filter (/= "") $ V.map getType (columns d)
      getType :: Column -> T.Text
      getType (BoxedColumn (column :: V.Vector a)) = T.pack $ show (typeRep @a)
      getType (UnboxedColumn (column :: VU.Vector a)) = T.pack $ show (typeRep @a)
      getType (OptionalColumn (column :: V.Vector a)) = T.pack $ show (typeRep @a)
      -- Separate out cases dynamically so we don't end up making round trip string
      -- copies.
      get :: Maybe Column -> V.Vector T.Text
      get (Just (BoxedColumn (column :: V.Vector a))) = case testEquality (typeRep @a) (typeRep @T.Text) of
              Just Refl -> column
              Nothing -> case testEquality (typeRep @a) (typeRep @String) of
                Just Refl -> V.map T.pack column
                Nothing -> V.map (T.pack . show) column
      get (Just (UnboxedColumn column)) = V.map (T.pack . show) (V.convert column)
      get (Just (OptionalColumn column)) = V.map (T.pack . show) column
      getTextColumnFromFrame df (i, name) = if i == 0
                                            then V.fromList (map (T.pack . show) [0..(fst (dataframeDimensions df) - 1)])
                                            else get $ (V.!?) (columns d) ((M.!) (columnIndices d) name)
      rows =
        transpose $
          zipWith (curry (V.toList . getTextColumnFromFrame d)) [0..] header
   in showTable properMarkdown header ("Int":types) rows

-- | O(1) Creates an empty dataframe
empty :: DataFrame
empty = DataFrame {columns = V.empty,
                   columnIndices = M.empty,
                   dataframeDimensions = (0, 0) }

getColumn :: T.Text -> DataFrame -> Maybe Column
getColumn name df = do
  i <- columnIndices df M.!? name
  columns df V.!? i

unsafeGetColumn :: T.Text -> DataFrame -> Column
unsafeGetColumn name df = columns df V.! (columnIndices df M.! name)

null :: DataFrame -> Bool
null df = V.null (columns df)

toMatrix :: DataFrame -> V.Vector (VU.Vector Float)
toMatrix df = let
    m = V.map (toVector @Double) (columns df)
  in V.generate (fst (dataframeDimensions df)) (\i -> foldl (\acc j -> acc `VU.snoc` (realToFrac ((m V.! j) V.! i))) VU.empty [0..(V.length m - 1)])

columnAsVector :: forall a . Columnable a => T.Text -> DataFrame -> V.Vector a
columnAsVector name df = case unsafeGetColumn name df of
  (BoxedColumn (col :: V.Vector b))    -> case testEquality (typeRep @a) (typeRep @b) of
    Nothing   -> error "Type error"
    Just Refl -> col
  (OptionalColumn (col :: V.Vector b)) -> case testEquality (typeRep @a) (typeRep @b) of
    Nothing   -> error "Type error"
    Just Refl -> col
  (UnboxedColumn (col :: VU.Vector b)) -> case testEquality (typeRep @a) (typeRep @b) of
    Nothing   -> error "Type error"
    Just Refl -> VG.convert col
