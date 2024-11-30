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
    getIndexedColumn,
    apply,
    applyMany,
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
    columnSize,
    combine,
) where

import Data.DataFrame.Internal ( Column(..), DataFrame(..), transformColumn )
import Data.Function (on)
import Data.Typeable (Typeable)
import Data.Vector (Vector)
import Prelude hiding (take, sum, filter)
import qualified Prelude as P

import Data.List (sort, group, (\\), delete, sortBy, foldl')
import qualified Data.ByteString.Char8 as C
import qualified Data.Map as M
import qualified Data.Map.Strict as MS
import qualified Data.Set as S
import qualified Data.Vector as V
import Data.Maybe (fromMaybe)
import qualified Data.DataFrame.Internal as DI
import Data.DataFrame.Util
    ( columnNotFound, typeMismatchError, inferType, appendWithFrontMin, getIndices )


addColumn :: forall a. (Typeable a, Show a, Ord a)
          => C.ByteString            -- Column Name
          -> V.Vector a        -- Data to add to column
          -> DataFrame         -- DataFrame to add to column
          -> DataFrame
addColumn name xs d = d {
           columns = MS.insert name (MkColumn xs) (columns d),
           _columnNames = if name `elem` _columnNames d
                            then _columnNames d
                          else _columnNames d ++ [name] }

addColumnWithDefault :: forall a. (Typeable a, Show a, Ord a)
          => a          -- Default Value
          -> C.ByteString     -- Column name
          -> V.Vector a -- Data to add to column
          -> DataFrame  -- DataFrame to add to column
          -> DataFrame
addColumnWithDefault defaultValue name xs d = let
        (rows, _) = dimensions d
        values = xs V.++ V.fromList (repeat defaultValue)
    in addColumn name (V.take rows values) d

getColumn' :: forall a. (Typeable a, Show a, Ord a)
          => Maybe C.ByteString -- Call point
          -> C.ByteString     -- Column Name
          -> DataFrame  -- DataFrame to get column from
          -> Vector a
getColumn' callPoint name d = case name `MS.lookup` columns d of
    Nothing -> error $ columnNotFound name
                                     (fromMaybe "getColumn" callPoint)
                                     (columnNames d)
    Just c@(MkColumn (column :: Vector b)) -> DI.fetchColumn @a c

getColumn :: forall a. (Typeable a, Show a, Ord a)
          => C.ByteString     -- Column Name
          -> DataFrame        -- DataFrame to get column from
          -> Vector a
getColumn = getColumn' Nothing

getIntColumn :: C.ByteString -> DataFrame -> Vector Int
getIntColumn = getColumn' Nothing

getIndexedColumn :: forall a . (Typeable a, Show a, Ord a) => C.ByteString -> DataFrame -> Vector (Int, a)
getIndexedColumn columnName df = V.indexed (getColumn columnName df)

apply :: forall b c. (Typeable b, Typeable c, Show b, Show c, Ord b, Ord c)
      => C.ByteString   -- Column name
      -> (b -> c)       -- function to apply
      -> DataFrame      -- DataFrame to apply operation to
      -> DataFrame
apply columnName f d =
    d { columns = MS.alter alteration columnName (columns d) }
    where
        alteration :: Maybe Column -> Maybe Column
        alteration c' = case c' of
            Nothing -> error $ columnNotFound columnName "apply" (columnNames d)
            Just column'  -> Just $ transformColumn (V.map f) column'

applyMany :: (Typeable b, Typeable c, Show b, Show c, Ord b, Ord c)
          => [C.ByteString]
          -> (b -> c)
          -> DataFrame
          -> DataFrame
applyMany names f df = foldl' (\d name -> apply name f d) df names

applyInt :: (Typeable b, Show b, Ord b)
         => C.ByteString       -- Column name
         -> (Int -> b)   -- function to apply
         -> DataFrame    -- DataFrame to apply operation to
         -> DataFrame
applyInt = apply

applyDouble :: (Typeable b, Show b, Ord b)
            => C.ByteString          -- Column name
            -> (Double -> b)   -- function to apply
            -> DataFrame       -- DataFrame to apply operation to
            -> DataFrame
applyDouble = apply

applyWhere :: forall a b c. (Typeable a, Typeable b, Show a, Show b, Ord a, Ord b)
           => C.ByteString      -- Criterion Column
           -> (a -> Bool) -- Filter condition
           -> C.ByteString      -- Column name
           -> (b -> b)    -- function to apply
           -> DataFrame   -- DataFrame to apply operation to
           -> DataFrame
applyWhere filterColumnName condition columnName f df = let
        filterColumn = getIndexedColumn filterColumnName df
        indexes = V.map fst $ V.filter (condition . snd) filterColumn
    in if V.null indexes
       then df
       else foldl' (\d i -> applyAtIndex i columnName f d) df indexes

applyAtIndex :: forall a. (Typeable a, Show a, Ord a)
           => Int         -- Index
           -> C.ByteString      -- Column name
           -> (a -> a)    -- function to apply
           -> DataFrame   -- DataFrame to apply operation to
           -> DataFrame
applyAtIndex i columnName f df = let
                valueColumn = getIndexedColumn columnName df
                updated = V.map (\(index, value) -> if index == i then f value else value) valueColumn
            in update columnName updated df

-- Take the first n rows of a DataFrame.
take :: Int -> DataFrame -> DataFrame
take n d = d { columns = MS.map
                       (\(MkColumn column') -> MkColumn (V.take n column')) (columns d) }

-- Get DataFrame dimensions.
dimensions :: DataFrame -> (Int, Int)
dimensions d = (numRows, numColumns)
    where columnSize (MkColumn column') = V.length column'
          numRows = foldl' (flip (max . columnSize . snd)) 0 (MS.toList (columns d))
          numColumns = MS.size $ columns d

-- Get column names of the DataFrame in order of insertion.
columnNames :: DataFrame -> [C.ByteString]
columnNames = _columnNames

sum :: (Typeable a, Show a, Ord a, Num a) => C.ByteString -> DataFrame -> a
sum name df = V.sum $ getColumn' (Just "sum") name df

sumWhere :: (Typeable a, Show a, Ord a, Typeable b, Show b, Ord b, Num b)
         => C.ByteString
         -> (a -> Bool)
         -> C.ByteString
         -> DataFrame
         -> b
sumWhere filterColumnName condition columnName df = let
            filterColumn = getIndexedColumn filterColumnName df
            indexes = S.fromList $ V.toList $ V.map fst $ V.filter (condition . snd) filterColumn
        in if indexes == S.empty
           then 0
           else V.sum $ V.ifilter (\i v -> i `S.member` indexes) $ getColumn' (Just "sum") columnName df

filter :: (Typeable a, Show a, Ord a)
            => C.ByteString
            -> (a -> Bool)
            -> DataFrame
            -> DataFrame
filter filterColumnName condition df = let
        filterColumn = getIndexedColumn filterColumnName df
        indexes = S.fromList $ V.toList $ V.map fst $ V.filter (condition . snd) filterColumn
        f (MkColumn (column :: Vector b)) = MkColumn $ V.ifilter (\i v -> i `S.member` indexes) column
    in df { columns = MS.map f (columns df) }

columnSize :: C.ByteString -> DataFrame -> Int
columnSize name df = case name `MS.lookup` columns df of
                        Nothing -> error $ columnNotFound name "apply" (columnNames df)
                        Just (MkColumn (column' :: Vector a))  -> V.length column'

valueCounts :: (Typeable a, Show a, Ord a) => C.ByteString -> DataFrame -> [(a, Integer)]
valueCounts columnName df = let
        column = sort $ V.toList (getColumn columnName df)
    in map (\xs -> (head xs, fromIntegral $ length xs)) (group column)

select :: [C.ByteString]
              -> DataFrame
              -> DataFrame
select cs df
    | not $ any (`elem` columnNames df) cs = error $ columnNotFound (C.pack $ show $ cs \\ columnNames df) "select" (columnNames df)
    | otherwise = foldl' addKeyValue DI.empty cs
            where addKeyValue d k = d { columns = MS.insert k (columns df MS.! k) (columns d),
                                        _columnNames = _columnNames d ++ [k] }

dropColumns :: [C.ByteString]
            -> DataFrame
            -> DataFrame
dropColumns cs df = let
        keysToKeep = columnNames df \\ cs
    in select keysToKeep df


groupBy :: [C.ByteString]
        -> DataFrame
        -> DataFrame
groupBy names df
    | not $ any (`elem` columnNames df) names = error $ columnNotFound (head names) "groupBy" (columnNames df)
    | otherwise = case head names `MS.lookup` columns df of
                        Nothing -> error "Nothing"
                        Just c@(MkColumn (column' :: Vector a)) ->
                            let
                                column = DI.fetchColumn @a c
                                valueIndices = V.ifoldl' (\m v k -> MS.insertWith (appendWithFrontMin . head) k [v] m) M.empty column
                                valueIndicesInitOrder = sortBy (compare `on` (head . snd)) $! MS.toList valueIndices
                                keys = map fst valueIndicesInitOrder
                                indices = map snd valueIndicesInitOrder
                                nameSet = S.fromList names
                                otherColumns = P.filter (`S.notMember` nameSet) (columnNames df)
                                initDf = addColumn (head names) (V.fromList keys) DI.empty
                            in foldl' (\d cname -> case cname `MS.lookup` columns df of
                                                    Nothing -> error $ columnNotFound cname "groupBy" (columnNames df)
                                                    Just c'@(MkColumn (column'' :: Vector b)) ->
                                                        let
                                                            vs = V.fromList $ map (`getIndices` DI.fetchColumn @b c') indices
                                                        in addColumn cname vs d) initDf otherColumns



reduceBy :: (Typeable a, Show a, Ord a, Typeable b, Show b, Ord b)
         => C.ByteString
         -> (Vector a -> b)
         -> DataFrame
         -> DataFrame
reduceBy = apply

-- Counts number of non-empty columns in a Raw text CSV for each row
-- And also gives a best guess of the row's data type.
info :: DataFrame -> [(C.ByteString, Int, C.ByteString)]
info df = map
      (\ name
         -> (name,
             V.length $ V.filter (\v -> v /= "" && v /= "NULL")
               $ getColumn @C.ByteString name df,
               inferType $ getColumn @C.ByteString name df)) (columnNames df)

combine :: forall a b c . (Typeable a, Show a, Ord a, Typeable b, Show b, Ord b, Typeable c, Show c)
        => C.ByteString
        -> C.ByteString
        -> (a -> b -> c)
        -> DataFrame
        -> V.Vector c
combine firstColumn secondColumn f df = V.zipWith f (getColumn @a firstColumn df)
                                                    (getColumn @b secondColumn df)

-- Since this potentially changes the length of a column and could break other operations e.g
-- groupBy and filter we do not (and should not) export it.
update :: forall a b. (Typeable a, Show a, Ord a)
       => C.ByteString
       -> V.Vector a
       -> DataFrame
       -> DataFrame
update name xs df = df {columns = MS.adjust (const (MkColumn xs)) name (columns df)}
