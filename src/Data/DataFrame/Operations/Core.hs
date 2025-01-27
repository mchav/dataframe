{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE BangPatterns #-}
module Data.DataFrame.Operations.Core where

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Map.Strict as MS
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Vector.Generic as VG
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import Control.Exception ( throw )
import Data.DataFrame.Errors
import Data.DataFrame.Internal.Column ( Column(..), toColumn', toColumn, columnLength, columnTypeString )
import Data.DataFrame.Internal.DataFrame (DataFrame(..), getColumn, null, empty)
import Data.DataFrame.Internal.Parsing (isNullish)
import Data.DataFrame.Internal.Types (Columnable)
import Data.Either
import Data.Function (on, (&))
import Data.Maybe
import Data.Type.Equality (type (:~:)(Refl), TestEquality(..))
import Type.Reflection
import Prelude hiding (null)

-- | O(1) Get DataFrame dimensions i.e. (rows, columns)
dimensions :: DataFrame -> (Int, Int)
dimensions = dataframeDimensions
{-# INLINE dimensions #-}

-- | O(k) Get column names of the DataFrame in order of insertion.
columnNames :: DataFrame -> [T.Text]
columnNames = map fst . L.sortBy (compare `on` snd). M.toList . columnIndices
{-# INLINE columnNames #-}

-- | /O(n)/ Adds a vector to the dataframe.
insertColumn ::
  forall a.
  (Columnable a) =>
  -- | Column Name
  T.Text ->
  -- | Vector to add to column
  V.Vector a ->
  -- | DataFrame to add column to
  DataFrame ->
  DataFrame
insertColumn name xs = insertColumn' name (Just (toColumn' xs))
{-# INLINE insertColumn #-}

cloneColumn :: T.Text -> T.Text -> DataFrame -> DataFrame
cloneColumn original new df = fromMaybe (throw $ ColumnNotFoundException original "cloneColumn" (map fst $ M.toList $ columnIndices df)) $ do
  column <- getColumn original df
  return $ insertColumn' new (Just column) df

-- | /O(n)/ Adds an unboxed vector to the dataframe.
insertUnboxedColumn ::
  forall a.
  (Columnable a, VU.Unbox a) =>
  -- | Column Name
  T.Text ->
  -- | Unboxed vector to add to column
  VU.Vector a ->
  -- | DataFrame to add to column
  DataFrame ->
  DataFrame
insertUnboxedColumn name xs = insertColumn' name (Just (UnboxedColumn xs))

-- -- | /O(n)/ Add a column to the dataframe. Not meant for external use.
insertColumn' ::
  -- | Column Name
  T.Text ->
  -- | Column to add
  Maybe Column ->
  -- | DataFrame to add to column
  DataFrame ->
  DataFrame
insertColumn' name xs d
    | M.member name (columnIndices d) = let
      i = (M.!) (columnIndices d) name
    in d { columns = columns d V.// [(i, xs)] }
  | otherwise = let
      (n:rest) = case freeIndices d of
        [] -> [VG.length (columns d)..(VG.length (columns d) * 2 - 1)]
        lst -> lst
      columns' = if L.null (freeIndices d)
                 then columns d V.++ V.replicate (VG.length (columns d)) Nothing
                 else columns d
      xs'
        | diff == 0 || null d = xs
        | diff > 0 = case xs of
            Nothing -> xs
            Just (BoxedColumn col) -> Just $ BoxedColumn $ V.map Just col <> V.replicate diff Nothing
            Just (UnboxedColumn col) -> Just $ BoxedColumn $ V.map Just (V.convert col) <> V.replicate diff Nothing
            -- TODO: What does this mean when groupby operations are changed to box operations?
            -- Maybe handle these with empty.
            Just (GroupedBoxedColumn col) -> Just $ BoxedColumn $ V.map Just col <> V.replicate diff Nothing
            Just (GroupedUnboxedColumn col) -> Just $ BoxedColumn $ V.map Just (V.convert col) <> V.replicate diff Nothing
        -- Make other columns optional.
        | diff < 0 = error "Column is too large to add"
   in d
        { columns = columns' V.// [(n, xs')],
          columnIndices = M.insert name n (columnIndices d),
          freeIndices = rest,
          dataframeDimensions = (if r == 0 then l else r, c + 1)
        }
        where diff = r - l
              l = maybe 0 columnLength xs
              (r, c) = dataframeDimensions d
{-# INLINE insertColumn' #-}

-- | /O(k)/ Add a column to the dataframe providing a default.
-- This constructs a new vector and also may convert it
-- to an unboxed vector if necessary. Since columns are usually
-- large the runtime is dominated by the length of the list, k.
insertColumnWithDefault ::
  forall a.
  (Columnable a) =>
  -- | Default Value
  a ->
  -- | Column name
  T.Text ->
  -- | Data to add to column
  V.Vector a ->
  -- | DataFrame to add to column
  DataFrame ->
  DataFrame
insertColumnWithDefault defaultValue name xs d =
  let (rows, _) = dataframeDimensions d
      values = xs V.++ V.replicate (rows - V.length xs) defaultValue
   in insertColumn' name (Just $ toColumn' values) d

-- TODO: Add existence check in rename.
rename :: T.Text -> T.Text -> DataFrame -> DataFrame
rename orig new df = fromMaybe (throw $ ColumnNotFoundException orig "rename" (map fst $ M.toList $ columnIndices df)) $ do
  columnIndex <- M.lookup orig (columnIndices df)
  let origRemoved = M.delete orig (columnIndices df)
  let newAdded = M.insert new columnIndex origRemoved
  return df { columnIndices = newAdded }

-- | O(1) Get the number of elements in a given column.
columnSize :: T.Text -> DataFrame -> Maybe Int
columnSize name df = columnLength <$> getColumn name df

data ColumnInfo = ColumnInfo {
    nameOfColumn :: !T.Text,
    nonNullValues :: !Int,
    nullValues :: !Int,
    partiallyParsedValues :: !Int,
    uniqueValues :: !Int,
    typeOfColumn :: !T.Text
  }

-- | O(n) Returns the number of non-null columns in the dataframe and the type associated
-- with each column.
columnInfo :: DataFrame -> DataFrame
columnInfo df = empty & insertColumn' "Column Name" (Just $ toColumn (map nameOfColumn infos))
                      & insertColumn' "# Non-null Values" (Just $ toColumn (map nonNullValues infos))
                      & insertColumn' "# Null Values" (Just $ toColumn (map nullValues infos))
                      & insertColumn' "# Partially parsed" (Just $ toColumn (map partiallyParsedValues infos))
                      & insertColumn' "# Unique Values" (Just $ toColumn (map uniqueValues infos))
                      & insertColumn' "Type" (Just $ toColumn (map typeOfColumn infos))
  where
    infos = L.sortBy (compare `on` nonNullValues) (V.ifoldl' go [] (columns df)) :: [ColumnInfo]
    indexMap = M.fromList (map (\(a, b) -> (b, a)) $ M.toList (columnIndices df))
    columnName i = indexMap M.! i
    go acc i Nothing = acc
    go acc i (Just col@(OptionalColumn (c :: V.Vector a))) = let
        cname = columnName i
        countNulls = nulls col
        countPartial = partiallyParsed col
        columnType = T.pack $ show $ typeRep @a
        unique = S.size $ VG.foldr S.insert S.empty c
      in ColumnInfo cname (columnLength col - countNulls) countNulls countPartial unique columnType : acc
    go acc i (Just col@(BoxedColumn (c :: V.Vector a))) = let
        cname = columnName i
        countNulls = nulls col
        countPartial = partiallyParsed col
        columnType = T.pack $ show $ typeRep @a
        unique = S.size $ VG.foldr S.insert S.empty c
      in ColumnInfo cname (columnLength col - countNulls) countNulls countPartial unique columnType : acc
    go acc i (Just col@(UnboxedColumn c)) = let
        cname = columnName i
        columnType = T.pack $ columnTypeString col
        unique = S.size $ VG.foldr S.insert S.empty c
        -- Unboxed columns cannot have nulls since Maybe
        -- is not an instance of Unbox a
      in ColumnInfo cname (columnLength col) 0 0 unique columnType : acc

nulls :: Column -> Int
nulls (BoxedColumn (xs :: V.Vector a)) = case testEquality (typeRep @a) (typeRep @T.Text) of
  Just Refl -> VG.length $ VG.filter isNullish xs
  Nothing -> case testEquality (typeRep @a) (typeRep @String) of
    Just Refl -> VG.length $ VG.filter (isNullish . T.pack) xs
    Nothing -> case typeRep @a of
      App t1 t2 -> case eqTypeRep t1 (typeRep @Maybe) of
          Just HRefl -> VG.length $ VG.filter isNothing xs
          Nothing -> 0
      _ -> 0
nulls _ = 0

partiallyParsed :: Column -> Int
partiallyParsed (BoxedColumn (xs :: V.Vector a)) =
  case typeRep @a of
    App (App tycon t1) t2 -> case eqTypeRep tycon (typeRep @Either) of
      Just HRefl -> VG.length $ VG.filter isLeft xs
      Nothing -> 0
    _ -> 0
partiallyParsed _ = 0

fromList :: [(T.Text, Column)] -> DataFrame
fromList = L.foldl' (\df (name, column) -> insertColumn' name (Just column) df) empty

-- | O (k * n) Counts the occurences of each value in a given column.
valueCounts :: forall a. (Columnable a) => T.Text -> DataFrame -> [(a, Integer)]
valueCounts columnName df = case getColumn columnName df of
      Nothing -> throw $ ColumnNotFoundException columnName "sortBy" (map fst $ M.toList $ columnIndices df)
      Just (BoxedColumn (column' :: V.Vector c)) ->
        let
          column = V.foldl' (\m v -> MS.insertWith (+) v (1 :: Integer) m) M.empty column'
        in case (typeRep @a) `testEquality` (typeRep @c) of
              Nothing -> throw $ TypeMismatchException (typeRep @a) (typeRep @c) columnName "valueCounts"
              Just Refl -> M.toAscList column
      Just (UnboxedColumn (column' :: VU.Vector c)) -> let
          column = V.foldl' (\m v -> MS.insertWith (+) v (1 :: Integer) m) M.empty (V.convert column')
        in case (typeRep @a) `testEquality` (typeRep @c) of
          Nothing -> throw $ TypeMismatchException (typeRep @a) (typeRep @c) columnName "valueCounts"
          Just Refl -> M.toAscList column

fold :: (a -> DataFrame -> DataFrame) -> [a] -> DataFrame -> DataFrame
fold f xs acc = L.foldl' (flip f) acc xs