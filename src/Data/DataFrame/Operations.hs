{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
module Data.DataFrame.Operations (
    info,
    addColumn,
    addColumnWithDefault,
    dimensions,
    columnNames,
    getColumn,
    getIntColumn,
    getUnindexedColumn,
    apply,
    applyWhere,
    applyAtIndex,
    applyInt,
    applyDouble,
    take,
    sum,
    sumWhere,
    filter,
    valueCounts,
    select,
    dropColumns,
    groupBy,
    reduceBy,
    columnSize
) where

import Data.DataFrame.Internal ( Column(..), DataFrame(..) )
import Data.Type.Equality
    ( type (:~:)(Refl), TestEquality(testEquality) )
import Data.Typeable (Typeable)
import Data.Vector (Vector)
import Prelude hiding (take, sum, filter)
import qualified Prelude as P
import Type.Reflection ( Typeable, TypeRep, typeRep )

import Data.List (sort, group, (\\), delete, sortBy)
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Set as S
import qualified Data.Vector as V
import Data.Maybe (fromMaybe)
import qualified Data.DataFrame.Internal as DI
import Data.DataFrame.Util


addColumn :: forall a. (Typeable a, Show a, Ord a)
          => T.Text            -- Column Name
          -> V.Vector a        -- Data to add to column
          -> DataFrame         -- DataFrame to add to column
          -> DataFrame
addColumn name xs d = let indexedValues = V.zip (V.fromList [0..]) xs
    in d { columns = M.insert name (MkColumn indexedValues) (columns d),
           _columnNames = if name `elem` _columnNames d
                            then _columnNames d
                          else _columnNames d ++ [name] }

addColumnWithDefault :: forall a. (Typeable a, Show a, Ord a)
          => a          -- Default Value
          -> T.Text     -- Column name
          -> V.Vector a -- Data to add to column
          -> DataFrame  -- DataFrame to add to column
          -> DataFrame
addColumnWithDefault defaultValue name xs d = let
        (rows, _) = dimensions d
        values = xs V.++ V.fromList (repeat defaultValue)
    in addColumn name (V.take rows values) d

getColumn' :: forall a. (Typeable a, Show a)
          => Maybe T.Text -- Call point
          -> T.Text     -- Column Name
          -> DataFrame  -- DataFrame to get column from
          -> Vector (Int, a)
getColumn' callPoint name d = case name `M.lookup` columns d of
    Nothing -> error $ columnNotFound name
                                     (fromMaybe "getColumn" callPoint)
                                     (columnNames d)
    Just (MkColumn (column :: Vector (Int, b))) ->
        let repb :: Type.Reflection.TypeRep b = Type.Reflection.typeRep @b
            repa :: Type.Reflection.TypeRep a = Type.Reflection.typeRep @a
        in case repa `testEquality` repb of
            Nothing -> error $ typeMismatchError name
                                         (fromMaybe "getColumn" callPoint)
                                         repa
                                         repb
            Just Refl -> column

getColumn :: forall a. (Typeable a, Show a)
          => T.Text     -- Column Name
          -> DataFrame  -- DataFrame to get column from
          -> Vector (Int, a)
getColumn = getColumn' Nothing

getIntColumn :: T.Text -> DataFrame -> Vector (Int, Int)
getIntColumn = getColumn' Nothing

getUnindexedColumn :: forall a . (Typeable a, Show a) => T.Text -> DataFrame -> Vector a
getUnindexedColumn columnName df = V.map snd (getColumn columnName df)

apply :: forall b c. (Typeable b, Typeable c, Show b, Show c, Ord b, Ord c)
      => T.Text     -- Column name
      -> (b -> c)   -- function to apply
      -> DataFrame  -- DataFrame to apply operation to
      -> DataFrame
apply columnName f d =
    d { columns = M.alter alteration columnName (columns d) }
    where
        alteration :: Maybe Column -> Maybe Column
        alteration c' = case c' of
            Nothing -> error $ columnNotFound columnName "apply" (columnNames d)
            Just (MkColumn (column' :: Vector (Int, a)))  ->
                let repb :: Type.Reflection.TypeRep b = Type.Reflection.typeRep @b
                    repa :: Type.Reflection.TypeRep a = Type.Reflection.typeRep @a
                in case repa `testEquality` repb of
                    Nothing -> error $ typeMismatchError columnName "apply" repb repa
                    Just Refl -> Just (MkColumn (V.map (applySnd f) column'))

applyInt :: (Typeable b, Show b, Ord b)
         => T.Text       -- Column name
         -> (Int -> b)   -- function to apply
         -> DataFrame    -- DataFrame to apply operation to
         -> DataFrame
applyInt = apply

applyDouble :: (Typeable b, Show b, Ord b)
            => T.Text          -- Column name
            -> (Double -> b)   -- function to apply
            -> DataFrame       -- DataFrame to apply operation to
            -> DataFrame
applyDouble = apply

applyWhere :: forall a b c. (Typeable a, Typeable b, Show a, Show b, Ord a, Ord b)
           => T.Text      -- Criterion Column
           -> (a -> Bool) -- Filter condition
           -> T.Text      -- Column name
           -> (b -> b)    -- function to apply
           -> DataFrame   -- DataFrame to apply operation to
           -> DataFrame
applyWhere filterColumnName condition columnName f df = let
        filterColumn = getColumn filterColumnName df
        indexes = V.map fst $ V.filter (condition . snd) filterColumn
    in if V.null indexes
       then df
       else foldr (\i d -> applyAtIndex i columnName f d) df indexes

applyAtIndex :: forall a. (Typeable a, Show a, Ord a)
           => Int         -- Index
           -> T.Text      -- Column name
           -> (a -> a)    -- function to apply
           -> DataFrame   -- DataFrame to apply operation to
           -> DataFrame
applyAtIndex i columnName f df = let
                valueColumn = getColumn columnName df
                updated = V.map (\(index, value) -> if index == i then f value else value) valueColumn
            in addColumn columnName updated df

-- Take the first n rows of a DataFrame.
take :: Int -> DataFrame -> DataFrame
take n d = d { columns = M.map
                       (\(MkColumn column') -> MkColumn (V.take n column')) (columns d) }

-- Get DataFrame dimensions.
dimensions :: DataFrame -> (Int, Int)
dimensions d = (numRows, numColumns)
    where columnSize (MkColumn column') = V.length column'
          numRows = foldr (max . columnSize . snd) 0 (M.toList (columns d))
          numColumns = M.size $ columns d

-- Get column names of the DataFrame in Alphabetical order.
columnNames :: DataFrame -> [T.Text]
columnNames = _columnNames

sum :: (Typeable a, Show a, Num a) => T.Text -> DataFrame -> a
sum name df = V.sum $ V.map snd $ getColumn' (Just "sum") name df

sumWhere :: (Typeable a, Show a, Typeable b, Show b, Num b) => T.Text -> (a -> Bool) -> T.Text -> DataFrame -> b
sumWhere filterColumnName condition columnName df = let
            filterColumn = getColumn filterColumnName df
            indexes = S.fromList $ V.toList $ V.map fst $ V.filter (condition . snd) filterColumn
        in if indexes == S.empty
           then 0
           else V.sum $ V.map snd $ V.filter (\(i, v) -> i `S.member` indexes) $ getColumn' (Just "sum") columnName df

filter :: (Typeable a, Show a)
            => T.Text
            -> (a -> Bool)
            -> DataFrame
            -> DataFrame
filter filterColumnName condition df = let
        filterColumn = getColumn filterColumnName df
        indexes = S.fromList $ V.toList $ V.map fst $ V.filter (condition . snd) filterColumn
        f (MkColumn (column :: Vector (Int, b))) = MkColumn $ V.indexed $ V.map snd $ V.filter (\(i, v) -> i `S.member` indexes) column
    in df { columns = M.map f (columns df) }

columnSize :: T.Text -> DataFrame -> Int
columnSize name df = case name `M.lookup` columns df of
                        Nothing -> error $ columnNotFound name "apply" (columnNames df)
                        Just (MkColumn (column' :: Vector (Int, a)))  -> V.length column'

valueCounts :: (Typeable a, Show a, Ord a) => T.Text -> DataFrame -> [(a, Integer)]
valueCounts columnName df = let
        column = sort $ V.toList $ V.map snd (getColumn columnName df)
    in map (\xs -> (head xs, fromIntegral $ length xs)) (group column)

select :: [T.Text]
              -> DataFrame
              -> DataFrame
select cs df = foldl addKeyValue DI.empty cs
            where addKeyValue d k = d { columns = M.insert k (columns df M.! k) (columns d),
                                        _columnNames = _columnNames d ++ [k] }

dropColumns :: [T.Text]
            -> DataFrame
            -> DataFrame
dropColumns cs df = let
        keysToKeep = columnNames df \\ cs
    in select keysToKeep df


groupBy :: T.Text
        -> DataFrame
        -> DataFrame
groupBy columnName df = case columnName `M.lookup` columns df of
    Nothing -> error $ columnNotFound columnName "groupBy" (columnNames df)
    Just (MkColumn (column :: Vector (Int, b))) ->
        let
            -- Create a map of the values
            valueIndices = foldr (\(v, k) m -> M.insertWith (++) k [v] m) M.empty column
            valueIndicesInitOrder = sortBy (\(_,a) (_,b) -> compare (minimum a) (minimum b)) $ M.toList valueIndices
            keys = map fst valueIndicesInitOrder
            indices = map snd valueIndicesInitOrder
            otherColumns =  columnName `delete` columnNames df
            initDf = addColumn columnName (V.fromList keys) DI.empty
        in foldr (\cname d -> case cname `M.lookup` columns df of
                                Nothing -> error $ columnNotFound cname "groupBy" (columnNames df)
                                Just (MkColumn (column' :: Vector (Int, c))) ->
                                    let
                                        vs = V.fromList $ map (V.fromList . map (snd . (column' V.!))) indices
                                    in addColumn cname vs d) initDf otherColumns

reduceBy :: (Typeable a, Show a, Ord a, Typeable b, Show b, Ord b)
         => T.Text
         -> (Vector a -> b)
         -> DataFrame
         -> DataFrame
reduceBy = apply

-- Counts number of non-empty columns in a Raw text CSV for each row
-- And also gives a best guess of the row's data type.
info :: DataFrame -> [(T.Text, Int, T.Text)]
info df = map
      (\ name
         -> (name,
             V.length $ V.filter (\v -> v /= "" && v /= "NULL")
               $ getUnindexedColumn @T.Text name df,
               inferType $ getUnindexedColumn @T.Text name df)) (columnNames df)