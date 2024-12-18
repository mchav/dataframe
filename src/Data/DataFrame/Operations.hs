{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Data.DataFrame.Operations
  ( addColumn,
    addColumn',
    addColumnWithDefault,
    dimensions,
    columnNames,
    apply,
    applyMany,
    applyWhere,
    applyWithAlias,
    applyAtIndex,
    applyInt,
    applyDouble,
    take,
    filter,
    valueCounts,
    select,
    drop,
    groupBy,
    reduceBy,
    columnSize,
    combine,
    parseDefaults,
    parseDefault,
    sortBy,
    SortOrder (..),
    columnInfo,
    fromList
  )
where

import qualified Data.DataFrame.Internal as DI
import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Map.Strict as MS
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Typeable as Typeable
import qualified Data.Vector as V
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Mutable as VM
import qualified Data.Vector.Unboxed as VU
import qualified Prelude as P

import Data.DataFrame.Internal (Column (..), DataFrame (..))
import Data.DataFrame.Util
import Data.Function (on)
import Data.Maybe (fromMaybe, isJust, isNothing)
import Data.Type.Equality
  ( TestEquality (testEquality),
    type (:~:) (Refl),
  )
import Data.Typeable (Typeable)
import Data.Vector (Vector)
import GHC.Stack (HasCallStack)
import Prelude hiding (drop, filter, sum, take)
import Text.Read (readMaybe)
import Type.Reflection (TypeRep, Typeable, typeOf, typeRep)

-- | /O(n)/ Adds a vector to the dataframe.
addColumn ::
  forall a.
  (Typeable a, Show a, Ord a) =>
  -- | Column Name
  T.Text ->
  -- | Vector to add to column
  V.Vector a ->
  -- | DataFrame to add column to
  DataFrame ->
  DataFrame
addColumn name xs = addColumn' name (Just (DI.toColumn' xs))

-- | /O(n)/ Adds an unboxed vector to the dataframe.
addUnboxedColumn ::
  forall a.
  (Typeable a, Show a, Ord a, VU.Unbox a) =>
  -- | Column Name
  T.Text ->
  -- | Unboxed vector to add to column
  VU.Vector a ->
  -- | DataFrame to add to column
  DataFrame ->
  DataFrame
addUnboxedColumn name xs = addColumn' name (Just (UnboxedColumn xs)) 

-- -- | /O(n)/ Add a column to the dataframe. Not meant for external use.
addColumn' ::
  -- | Column Name
  T.Text ->
  -- | Column to add
  Maybe Column ->
  -- | DataFrame to add to column
  DataFrame ->
  DataFrame
addColumn' name xs d
    | M.member name (DI.columnIndices d) = let
      i = (M.!) (DI.columnIndices d) name
    in d { columns = columns d V.// [(i, xs)] }
  | otherwise = let
      (n:rest) = case DI.freeIndices d of
        [] -> [VG.length (columns d)..(VG.length (columns d) * 2 - 1)]
        lst -> lst
      columns' = if null (DI.freeIndices d)
                 then columns d V.++ V.replicate (VG.length (columns d)) Nothing
                 else columns d
      xs'
        | diff == 0 || DI.isEmpty d = xs
        | diff > 0 = case xs of
            Nothing -> xs
            Just (BoxedColumn col) -> Just $ BoxedColumn $ V.map Just col <> V.replicate diff Nothing
            Just (UnboxedColumn col) -> Just $ BoxedColumn $ V.map Just (V.convert col) <> V.replicate diff Nothing
        | diff < 0 = error "Column is too large to add"
   in d
        { columns = columns' V.// [(n, xs')],
          columnIndices = M.insert name n (DI.columnIndices d),
          freeIndices = rest,
          dataframeDimensions = (if r == 0 then l else r, c + 1)
        }
        where diff = r - l
              l = maybe 0 DI.columnLength xs
              (r, c) = DI.dataframeDimensions d

-- | /O(k)/ Add a column to the dataframe providing a default.
-- This constructs a new vector and also may convert it
-- to an unboxed vector if necessary. Since columns are usually
-- large the runtime is dominated by the length of the list, k.
addColumnWithDefault ::
  forall a.
  (Typeable a, Show a, Ord a) =>
  -- | Default Value
  a ->
  -- | Column name
  T.Text ->
  -- | Data to add to column
  V.Vector a ->
  -- | DataFrame to add to column
  DataFrame ->
  DataFrame
addColumnWithDefault defaultValue name xs d =
  let (rows, _) = DI.dataframeDimensions d
      values = xs V.++ V.replicate (rows - (V.length xs)) defaultValue
   in addColumn' name (Just $ DI.toColumn' values) d

-- | O(k) Apply a function to a given column in a dataframe.
apply ::
  forall b c.
  (Typeable b, Typeable c, Show b, Show c, Ord b, Ord c) =>
  -- | Column name
  T.Text ->
  -- | function to apply
  (b -> c) ->
  -- | DataFrame to apply operation to
  DataFrame ->
  DataFrame
apply columnName f d = case columnName `MS.lookup` DI.columnIndices d of
  Nothing -> error $ columnNotFound columnName "apply" (map fst $ M.toList $ DI.columnIndices d)
  Just i -> case DI.columns d V.!? i of
    Nothing -> error "Internal error: Column is empty"
    Just c -> case c of
      Just ((BoxedColumn (column :: V.Vector a))) ->
        let
        in case testEquality (typeRep @a) (typeRep @b) of
              Just Refl -> addColumn' columnName (Just $ DI.toColumn' (V.map f column)) d
              Nothing -> error $ addCallPointInfo columnName (Just "apply") (typeMismatchError (typeRep @b) (typeRep @a))
      Just ((UnboxedColumn (column :: VU.Vector a))) ->
        let
        in case testEquality (typeRep @a) (typeRep @b) of
              Just Refl -> case testEquality (typeRep @c) (typeRep @Double) of
                Just Refl -> addUnboxedColumn columnName (VU.map f column) d
                Nothing -> case testEquality (typeRep @c) (typeRep @Int) of
                  Just Refl -> addUnboxedColumn columnName (VU.map f column) d
                  Nothing -> addColumn' columnName (Just $ DI.toColumn' (V.map f (V.convert column))) d
              Nothing -> error $ addCallPointInfo columnName (Just "apply") (typeMismatchError (typeRep @b) (typeRep @a))

-- | O(k) Apply a function to a given column in a dataframe and
-- add the result into alias column. This function is useful for

applyWithAlias ::
  forall b c.
  (Typeable b, Typeable c, Show b, Show c, Ord b, Ord c) =>
  -- | New name
  T.Text ->
  -- | function to apply
  (b -> c) ->
  -- | Derivative column name
  T.Text ->
  -- | DataFrame to apply operation to
  DataFrame ->
  DataFrame
applyWithAlias alias f columnName d = case columnName `MS.lookup` DI.columnIndices d of
  Nothing -> error $ columnNotFound columnName "applyAt" (map fst $ M.toList $ DI.columnIndices d)
  Just i -> case DI.columns d V.!? i of
    Nothing -> error "Internal error: Column is empty"
    Just c -> case c of
      Just ((BoxedColumn (column :: V.Vector a))) ->
        let
        in case testEquality (typeRep @a) (typeRep @b) of
              Just Refl -> addColumn' alias (Just $ DI.toColumn' (V.map f column)) d
              Nothing -> error $ addCallPointInfo columnName (Just "applyAt") (typeMismatchError (typeRep @a) (typeRep @b))
      Just ((UnboxedColumn (column :: VU.Vector a))) ->
        let
        in case testEquality (typeRep @a) (typeRep @b) of
              Just Refl -> case testEquality (typeRep @c) (typeRep @Double) of
                Just Refl -> addUnboxedColumn columnName (VU.map f column) d
                Nothing -> case testEquality (typeRep @c) (typeRep @Int) of
                  Just Refl -> addUnboxedColumn columnName (VU.map f column) d
                  Nothing -> addColumn' alias (Just $ DI.toColumn' (V.map f (V.convert column))) d
              Nothing -> error $ addCallPointInfo columnName (Just "apply") (typeMismatchError (typeRep @a) (typeRep @b))

-- | O(k * n) Apply a function to given column names in a dataframe.
applyMany ::
  (Typeable b, Typeable c, Show b, Show c, Ord b, Ord c) =>
  [T.Text] ->
  (b -> c) ->
  DataFrame ->
  DataFrame
applyMany names f df = L.foldl' (\d name -> apply name f d) df names

-- | O(k) Convenience function that applies to an int column.
applyInt ::
  (Typeable b, Show b, Ord b) =>
  -- | Column name
  T.Text ->
  -- | function to apply
  (Int -> b) ->
  -- | DataFrame to apply operation to
  DataFrame ->
  DataFrame
applyInt = apply

-- | O(k) Convenience function that applies to an double column.
applyDouble ::
  (Typeable b, Show b, Ord b) =>
  -- | Column name
  T.Text ->
  -- | function to apply
  (Double -> b) ->
  -- | DataFrame to apply operation to
  DataFrame ->
  DataFrame
applyDouble = apply

-- | O(k * n) Apply a function to a column only if there is another column
-- value that matches the given criterion.
--
-- > applyWhere "Age" (<20) "Generation" (const "Gen-Z")
applyWhere ::
  forall a b.
  (Typeable a, Typeable b, Show a, Show b, Ord a, Ord b) =>
  T.Text -> -- Criterion Column
  (a -> Bool) -> -- Filter condition
  T.Text -> -- Column name
  (b -> b) -> -- function to apply
  DataFrame -> -- DataFrame to apply operation to
  DataFrame
applyWhere filterColumnName condition columnName f df = case filterColumnName `MS.lookup` DI.columnIndices df of
  Nothing -> error $ columnNotFound filterColumnName "applyWhere" (map fst $ M.toList $ DI.columnIndices df)
  Just i -> case DI.columns df V.!? i of
    Nothing -> error "Internal error: Column is empty"
    Just c -> case c of
      Just (BoxedColumn (column :: Vector c)) -> case (typeRep @a) `testEquality` (typeRep @c) of
        Nothing -> error $ addCallPointInfo columnName (Just "applyWhere") (typeMismatchError (typeRep @a) (typeRep @b))
        Just Refl ->
          let filterColumn = V.indexed column
              indexes = V.map fst $ V.filter (condition . snd) filterColumn
          in if V.null indexes
                then df
                else L.foldl' (\d i -> applyAtIndex i columnName f d) df indexes
      Just (UnboxedColumn (column :: VU.Vector c)) -> case (typeRep @a) `testEquality` (typeRep @c) of
        Nothing -> error $ addCallPointInfo columnName (Just "applyWhere") (typeMismatchError (typeRep @a) (typeRep @b))
        Just Refl ->
          let filterColumn = VU.indexed column
              indexes = VU.map fst $ VU.filter (condition . snd) filterColumn
          in if VU.null indexes
                then df
                else VU.foldl' (\d i -> applyAtIndex i columnName f d) df indexes

-- | O(k) Apply a function to the column at a given index.
applyAtIndex ::
  forall a.
  (Typeable a, Show a, Ord a) =>
  -- | Index
  Int ->
  -- | Column name
  T.Text ->
  -- | function to apply
  (a -> a) ->
  -- | DataFrame to apply operation to
  DataFrame ->
  DataFrame
applyAtIndex i columnName f df = case columnName `MS.lookup` DI.columnIndices df of
  Nothing -> error $ columnNotFound columnName "applyAtIndex" (map fst $ M.toList $ DI.columnIndices df)
  Just i -> case DI.columns df V.!? i of
    Nothing -> error "Internal error: Column is empty"
    Just c -> case c of
      Just (BoxedColumn (column :: Vector b)) -> case (typeRep @a) `testEquality` (typeRep @b) of
        Nothing -> error $ addCallPointInfo columnName (Just "applyWhere") (typeMismatchError (typeRep @a) (typeRep @b))
        Just Refl ->
          let updated = V.imap (\index value -> if index == i then f value else value) column
          in addColumn columnName updated df
      Just (UnboxedColumn (column :: VU.Vector b)) -> case (typeRep @a) `testEquality` (typeRep @b) of
        Nothing -> error $ addCallPointInfo columnName (Just "applyWhere") (typeMismatchError (typeRep @a) (typeRep @b))
        Just Refl ->
          let updated = VU.imap (\index value -> if index == i then f value else value) column
          in addUnboxedColumn columnName updated df

-- | O(k * n) Take the first n rows of a DataFrame.
take :: Int -> DataFrame -> DataFrame
take n d = d {columns = V.map take' (columns d), dataframeDimensions = (min (max n 0) r, c)}
  where
    (r, c) = DI.dataframeDimensions d
    take' Nothing = Nothing
    take' (Just (BoxedColumn column)) = Just (BoxedColumn (V.take n column))
    take' (Just (UnboxedColumn column)) = Just (UnboxedColumn (VU.take n column))

-- | O(1) Get DataFrame dimensions i.e. (rows, columns)
dimensions :: DataFrame -> (Int, Int)
dimensions = DI.dataframeDimensions

-- | O(k) Get column names of the DataFrame in order of insertion.
columnNames :: DataFrame -> [T.Text]
columnNames = map fst . L.sortBy (compare `on` snd). M.toList . DI.columnIndices

-- | O(n * k) Filter rows by a given condition.
filter ::
  forall a.
  (Typeable a, Show a, Ord a) =>
  -- | Column to filter by
  T.Text ->
  -- | Filter condition
  (a -> Bool) ->
  -- | Dataframe to filter
  DataFrame ->
  DataFrame
filter filterColumnName condition df =
  let pick _ Nothing = Nothing
      pick indexes (Just c@(BoxedColumn column)) = Just $ BoxedColumn $ V.ifilter (\i v -> i `S.member` indexes) column
      pick indexes (Just c@(UnboxedColumn column)) = Just $ UnboxedColumn $ VU.ifilter (\i v -> i `S.member` indexes) column
      (r', c') = DI.dataframeDimensions df
   in case filterColumnName `MS.lookup` DI.columnIndices df of
      Nothing -> error $ columnNotFound filterColumnName "filter" (map fst $ M.toList $ DI.columnIndices df)
      Just i -> case DI.columns df V.!? i of
        Nothing -> error "Internal error: Column is empty"
        Just c -> case c of
          Just (BoxedColumn (column :: Vector c)) -> case (typeRep @a) `testEquality` (typeRep @c) of
            Nothing -> error $ addCallPointInfo filterColumnName (Just "filter") (typeMismatchError (typeRep @a) (typeRep @c))
            Just Refl ->
              let indexes = V.ifoldl' (\s i v -> if condition v then S.insert i s else s) S.empty column
              in df {columns = V.map (pick indexes) (columns df), dataframeDimensions = (S.size indexes, c')}
          Just (UnboxedColumn (column :: VU.Vector c)) -> case (typeRep @a) `testEquality` (typeRep @c) of
            Nothing -> error $ addCallPointInfo filterColumnName (Just "filter") (typeMismatchError (typeRep @a) (typeRep @c))
            Just Refl ->
              let indexes = VU.ifoldl' (\s i v -> if condition v then S.insert i s else s) S.empty column
              in df {columns = V.map (pick indexes) (columns df), dataframeDimensions = (S.size indexes, c')}

-- | Sort order taken as a parameter by the sortby function.
data SortOrder = Ascending | Descending deriving (Eq)

-- | O(k log n) Sorts the dataframe by a given row. Currently uses insertion sort
-- under the hood so can be very inefficient for large data.
--
-- > sortBy "Age" df
sortBy ::
  T.Text ->
  SortOrder ->
  DataFrame ->
  DataFrame
sortBy sortColumnName order df =
  let pick _ Nothing = Nothing
      pick indexes (Just c@(BoxedColumn column)) = Just $ BoxedColumn $ indexes `getIndices` column
      pick indexes (Just c@(UnboxedColumn column)) = Just $ UnboxedColumn $ indexes `getIndicesUnboxed` column
   in case sortColumnName `MS.lookup` DI.columnIndices df of
      Nothing -> error $ columnNotFound sortColumnName "sortBy" (map fst $ M.toList $ DI.columnIndices df)
      Just i -> case DI.columns df V.!? i of
        Nothing -> error "Internal error: Column is empty"
        Just c -> case c of
          Just (BoxedColumn (column :: V.Vector c)) ->
            let indexes = map snd . (if order == Ascending then S.toAscList else S.toDescList) $ VG.ifoldr (\i e acc -> S.insert (e, i) acc) S.empty column
            in df {columns = V.map (pick indexes) (columns df)}
          Just (UnboxedColumn (column :: VU.Vector c)) ->
            let indexes = map snd $ (if order == Ascending then S.toAscList else S.toDescList) $ VU.ifoldr (\i e acc -> S.insert (e, i) acc) S.empty column
            in df {columns = V.map (pick indexes) (columns df)}

-- | O(1) Get the number of elements in a given column.
columnSize :: T.Text -> DataFrame -> Maybe Int
columnSize name df = case name `MS.lookup` DI.columnIndices df of
  Nothing -> error $ columnNotFound name "columnSize" (columnNames df)
  Just i -> case DI.columns df V.!? i of
    Nothing -> Nothing
    Just c -> DI.columnLength <$> c

-- | O (k * n) Counts the occurences of each value in a given column.
valueCounts :: forall a. (Typeable a, Show a, Ord a) => T.Text -> DataFrame -> [(a, Integer)]
valueCounts columnName df =
  let repa :: Type.Reflection.TypeRep a = Type.Reflection.typeRep @a
   in case columnName `MS.lookup` DI.columnIndices df of
      Nothing -> error $ columnNotFound columnName "sortBy" (map fst $ M.toList $ DI.columnIndices df)
      Just i -> case DI.columns df V.!? i of
        Nothing -> error "Internal error: Column is empty"
        Just c -> case c of
          Just (BoxedColumn (column' :: V.Vector c)) ->
            let repc :: Type.Reflection.TypeRep c = Type.Reflection.typeRep @c
                column = L.sortBy (compare `on` snd) $ V.toList $ V.map (\v -> (v, show v)) column'
            in case repa `testEquality` repc of
                  Nothing -> error $ addCallPointInfo columnName (Just "apply") (typeMismatchError (typeRep @a) (typeRep @c))
                  Just Refl -> map (\xs -> (fst (head xs), fromIntegral $ length xs)) (L.groupBy ((==) `on` snd) column)
          Just (UnboxedColumn (column' :: VU.Vector c)) ->
            let repc :: Type.Reflection.TypeRep c = Type.Reflection.typeRep @c
                column = L.sortBy (compare `on` snd) $ V.toList $ V.map (\v -> (v, show v)) (VU.convert column')
            in case repa `testEquality` repc of
                  Nothing -> error $ addCallPointInfo columnName (Just "apply") (typeMismatchError (typeRep @a) (typeRep @c))
                  Just Refl -> map (\xs -> (fst (head xs), fromIntegral $ length xs)) (L.groupBy ((==) `on` snd) column)

-- | O(n) Selects a number of columns in a given dataframe.
--
-- > select ["name", "age"] df
select ::
  [T.Text] ->
  DataFrame ->
  DataFrame
select cs df
  | not $ any (`elem` columnNames df) cs = error $ columnNotFound (T.pack $ show $ cs L.\\ columnNames df) "select" (columnNames df)
  | otherwise = L.foldl' addKeyValue DI.empty cs
  where
    cIndexAssoc = M.toList $ DI.columnIndices df
    remaining = L.filter (\(c, i) -> c `elem` cs) cIndexAssoc
    removed = cIndexAssoc L.\\ remaining
    indexes = map snd remaining
    (r, c) = DI.dataframeDimensions df
    addKeyValue d k =
      d
        { columns = V.imap (\i v -> if i `notElem` indexes then Nothing else v) (columns df),
          columnIndices = M.fromList remaining,
          freeIndices = map snd removed ++ DI.freeIndices df,
          dataframeDimensions = (r, L.length remaining)
        }

-- | O(n) inverse of select
--
-- > drop ["Name"] df
drop ::
  [T.Text] ->
  DataFrame ->
  DataFrame
drop cs df =
  let keysToKeep = columnNames df L.\\ cs
   in select keysToKeep df

-- | O(k * n) groups the dataframe by the given rows aggregating the remaining rows
-- into vector that should be reduced later.
groupBy ::
  [T.Text] ->
  DataFrame ->
  DataFrame
groupBy names df
  | not $ any (`elem` columnNames df) names = error $ columnNotFound (T.pack $ show $ names L.\\ columnNames df) "groupBy" (columnNames df)
  | otherwise = L.foldl' addColumns initDf groupingColumns
  where
    -- Create a string representation of each row.
    values = V.map (mkRowRep df (S.fromList names)) (V.generate (fst (dimensions df)) id)
    -- Create a mapping from the row representation to the list of indices that
    -- have that row representation. This will allow us to combine the indexes
    -- where the rows are the same.
    valueIndices = V.ifoldl' (\m index rowRep -> MS.insertWith (appendWithFrontMin . head) rowRep [index] m) M.empty values
    -- Since the min is at the head this allows us to get the min in constant time and sort by it
    -- That way we can recover the original order of the rows.
    valueIndicesInitOrder = L.sortBy (compare `on` (head . snd)) $! MS.toList valueIndices
    -- For the ungrouped columns these will be the indexes to get for each row.
    -- We rely on this list being in the same order as the rows.
    indices = map snd valueIndicesInitOrder
    -- These are the indexes of the grouping/key rows i.e the minimum elements
    -- of the list.
    keyIndices = map (head . snd) valueIndicesInitOrder
    -- this will be our main worker function in the fold that takes all
    -- indices and replaces each value in a column with a list of
    -- the elements with the indices where the grouped row
    -- values are the same.
    addColumns = groupColumns indices df
    -- Out initial DF will just be all the grouped rows added to an
    -- empty dataframe. The entries are dedued and are in their
    -- initial order.
    initDf = L.foldl' (mkGroupedColumns keyIndices df) DI.empty names
    -- All the rest of the columns that we are grouping by.
    groupingColumns = columnNames df L.\\ names

mkRowRep :: DataFrame -> S.Set T.Text -> Int -> String
mkRowRep df names i = V.ifoldl' go "" (columns df)
  where
    indexMap = M.fromList (map (\(a, b) -> (b, a)) $ M.toList (DI.columnIndices df))
    go acc k Nothing = acc
    go acc k (Just (BoxedColumn c)) =
      if S.notMember (indexMap M.! k) names
        then acc
        else case c V.!? i of
          Just e -> acc ++ show e
          Nothing ->
            error $
              "Column "
                ++ T.unpack (indexMap M.! k)
                ++ " has less items than "
                ++ "the other columns at index "
                ++ show i
    go acc k (Just (UnboxedColumn c)) =
      if S.notMember (indexMap M.! k) names
        then acc
        else case c VU.!? i of
          Just e -> acc ++ show e
          Nothing ->
            error $
              "Column "
                ++ T.unpack (indexMap M.! k)
                ++ " has less items than "
                ++ "the other columns at index "
                ++ show i

mkGroupedColumns :: [Int] -> DataFrame -> DataFrame -> T.Text -> DataFrame
mkGroupedColumns indices df acc name =
  case (V.!) (DI.columns df) (DI.columnIndices df M.! name) of
    Nothing -> error "Unexpected"
    (Just (BoxedColumn column)) ->
      let vs = indices `getIndices` column
       in addColumn name vs acc
    (Just (UnboxedColumn column)) ->
      let vs = indices `getIndicesUnboxed` column
       in addUnboxedColumn name vs acc

groupColumns :: [[Int]] -> DataFrame -> DataFrame -> T.Text -> DataFrame
groupColumns indices df acc name =
  case (V.!) (DI.columns df) (DI.columnIndices df M.! name) of
    Nothing -> df
    (Just (BoxedColumn column)) ->
      let vs = V.fromList $ map (`getIndices` column) indices
       in addColumn name vs acc
    (Just (UnboxedColumn column)) ->
      let vs = V.fromList $ map (`getIndicesUnboxed` column) indices
       in addColumn name vs acc

-- O (k * n) Reduces a vector valued volumn with a given function.
reduceBy ::
  (Typeable a, Show a, Ord a, Typeable b, Show b, Ord b, Typeable (v a), Show (v a), Ord (v a), VG.Vector v a) =>
  T.Text ->
  (v a -> b) ->
  DataFrame ->
  DataFrame
reduceBy = apply

-- O (k) combines two columns into a single column using a given function similar to zipWith.
--
-- > combine "total_price" (*) "unit_price" "quantity" df
combine ::
  forall a b c.
  (Typeable a, Show a, Ord a, Typeable b, Show b, Ord b, Typeable c, Show c, Ord c) =>
  -- | the name of the column where the result will be materialized.
  T.Text ->
  -- | the function to zip the rows with.
  (a -> b -> c) ->
  -- | left-hand-side column
  T.Text ->
  -- | right hand side column
  T.Text ->
  -- | dataframe to apply the combination too.
  DataFrame ->
  DataFrame
combine targetColumn func firstColumn secondColumn df =
  if all isJust [M.lookup firstColumn (DI.columnIndices df), M.lookup secondColumn (DI.columnIndices df)]
    then case ((V.!) (columns df) (DI.columnIndices df M.! firstColumn), (V.!) (columns df) (DI.columnIndices df M.! secondColumn)) of
      (Nothing, Nothing) -> df
      (Nothing, _) -> df
      (_, Nothing) -> df
      (Just (BoxedColumn (f :: Vector d)), Just (BoxedColumn (g :: Vector e))) ->
        case testEquality (typeRep @a) (typeRep @d) of
          Nothing -> error $ addCallPointInfo firstColumn (Just "combine") (typeMismatchError (typeRep @a) (typeRep @d))
          Just Refl -> case testEquality (typeRep @b) (typeRep @e) of
            Nothing -> error $ addCallPointInfo secondColumn (Just "combine") (typeMismatchError (typeRep @b) (typeRep @e))
            Just Refl -> addColumn targetColumn (V.zipWith func f g) df
      (Just (UnboxedColumn (f :: VU.Vector d)), Just (UnboxedColumn (g :: VU.Vector e))) ->
        case testEquality (typeRep @a) (typeRep @d) of
          Nothing -> error $ addCallPointInfo firstColumn (Just "combine") (typeMismatchError (typeRep @a) (typeRep @d))
          Just Refl -> case testEquality (typeRep @b) (typeRep @e) of
            Nothing -> error $ addCallPointInfo secondColumn (Just "combine") (typeMismatchError (typeRep @b) (typeRep @e))
            Just Refl -> case testEquality (typeRep @c) (typeRep @Int) of
              Just Refl -> addUnboxedColumn targetColumn (VU.zipWith func f g) df
              Nothing -> case testEquality (typeRep @c) (typeRep @Double) of
                Just Refl -> addUnboxedColumn targetColumn (VU.zipWith func f g) df
                Nothing -> addColumn targetColumn (V.zipWith func (V.convert f) (V.convert g)) df
      (Just (UnboxedColumn (f :: VU.Vector d)), Just (BoxedColumn (g :: Vector e))) ->
        case testEquality (typeRep @a) (typeRep @d) of
          Nothing -> error $ addCallPointInfo firstColumn (Just "combine") (typeMismatchError (typeRep @a) (typeRep @d))
          Just Refl -> case testEquality (typeRep @b) (typeRep @e) of
            Nothing -> error $ addCallPointInfo secondColumn (Just "combine") (typeMismatchError (typeRep @b) (typeRep @e))
            Just Refl -> case testEquality (typeRep @c) (typeRep @Int) of
              Just Refl -> addUnboxedColumn targetColumn (VU.convert $ V.zipWith func (V.convert f) g) df
              Nothing -> case testEquality (typeRep @c) (typeRep @Double) of
                Just Refl -> addUnboxedColumn targetColumn (VU.convert $ V.zipWith func (V.convert f) g) df
                Nothing -> addColumn targetColumn (V.zipWith func (V.convert f) g) df
      (Just (BoxedColumn (f :: V.Vector d)), Just (UnboxedColumn (g :: VU.Vector e))) ->
        case testEquality (typeRep @a) (typeRep @d) of
          Nothing -> error $ addCallPointInfo firstColumn (Just "combine") (typeMismatchError (typeRep @a) (typeRep @d))
          Just Refl -> case testEquality (typeRep @b) (typeRep @e) of
            Nothing -> error $ addCallPointInfo secondColumn (Just "combine") (typeMismatchError (typeRep @b) (typeRep @e))
            Just Refl -> case testEquality (typeRep @c) (typeRep @Int) of
              Just Refl -> addUnboxedColumn targetColumn (VU.convert $ V.zipWith func f (V.convert g)) df
              Nothing -> case testEquality (typeRep @c) (typeRep @Double) of
                Just Refl -> addUnboxedColumn targetColumn (VU.convert $ V.zipWith func f (V.convert g)) df
                Nothing -> addColumn targetColumn (V.zipWith func f (V.convert g)) df
    else error $ columnNotFound (T.pack $ show $ [targetColumn, firstColumn, secondColumn] L.\\ columnNames df) "combine" (columnNames df)

parseDefaults :: Bool -> DataFrame -> DataFrame
parseDefaults safeRead df = df {columns = V.map (parseDefault safeRead) (columns df)}

parseDefault :: Bool -> Maybe Column -> Maybe Column
parseDefault _ Nothing = Nothing
parseDefault safeRead (Just (BoxedColumn (c :: V.Vector a))) =
  let repa :: Type.Reflection.TypeRep a = Type.Reflection.typeRep @a
      repText :: Type.Reflection.TypeRep T.Text = Type.Reflection.typeRep @T.Text
   in case repa `testEquality` repText of
        Nothing -> Just $ BoxedColumn c
        Just Refl ->
          let example = T.strip (V.head c)
              nullish = S.fromList ["nan", "NULL", "null", "", " "]
              emptyToNothing v = if S.member v nullish then Nothing else Just v
           in case readInt example of
                Just _ ->
                  let safeVector = V.map ((=<<) readInt . emptyToNothing) c
                      hasNulls = V.foldl' (\acc v -> if isNothing v then acc || True else acc) False safeVector
                   in Just (if safeRead && hasNulls then BoxedColumn safeVector else UnboxedColumn (VU.convert $ V.map (fromMaybe 0 . readInt) c))
                Nothing -> case readDouble example of
                  Just _ ->
                    let safeVector = V.map ((=<<) readDouble . emptyToNothing) c
                        hasNulls = V.foldl' (\acc v -> if isNothing v then acc || True else acc) False safeVector
                     in Just $ if safeRead && hasNulls then BoxedColumn safeVector else UnboxedColumn (VU.convert $ V.map (fromMaybe 0 . readDouble) c)
                  Nothing ->
                    let safeVector = V.map emptyToNothing c
                        hasNulls = V.foldl' (\acc v -> if isNothing v then acc || True else acc) False safeVector
                     in Just $ if safeRead && hasNulls then BoxedColumn safeVector else BoxedColumn c

-- | O(n) Returns the number of non-null columns in the dataframe and the type associated
-- with each column.
columnInfo :: DataFrame -> [(String, Int, String)]
columnInfo df = L.sortBy (compare `on` snd') (V.ifoldl' go [] (columns df))
  where
    indexMap = M.fromList (map (\(a, b) -> (b, a)) $ M.toList (DI.columnIndices df))
    go acc i Nothing = acc
    go acc i (Just (BoxedColumn (c :: Vector a))) = (T.unpack (indexMap M.! i), V.length $ V.filter (flip S.member nullish . show) c, show $ typeRep @a) : acc
    go acc i (Just (UnboxedColumn (c :: VU.Vector a))) = (T.unpack (indexMap M.! i), V.length $ V.filter (flip S.member nullish . show) (V.convert c), show $ typeRep @a) : acc
    nullish = S.fromList ["Nothing", "NULL", "", " ", "nan"]
    snd' (_, x, _) = x

fromList :: [(T.Text, Column)] -> DataFrame
fromList = L.foldl' (\df (name, column) -> addColumn' name (Just column) df) DI.empty
