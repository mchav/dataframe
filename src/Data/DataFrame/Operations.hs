{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE FlexibleContexts #-}

module Data.DataFrame.Operations
  ( addColumn,
    addColumn',
    addColumnWithDefault,
    dimensions,
    columnNames,
    rename,
    apply,
    applyMany,
    applyWhere,
    derive,
    deriveF,
    applyAtIndex,
    applyInt,
    applyDouble,
    take,
    range,
    filter,
    filterBy,
    valueCounts,
    select,
    exclude,
    fold,
    groupBy,
    groupByAgg,
    reduceBy,
    reduceByAgg,
    aggregate,
    columnSize,
    parseDefaults,
    parseDefault,
    sortBy,
    SortOrder (..),
    Aggregation(..),
    columnInfo,
    fromList,
    as,
    frequencies,
    mean,
    median,
    standardDeviation,
    variance,
    interQuartileRange,
    sum,
    skewness,
    correlation,
    summarize,
    (|>)
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
import qualified Statistics.Quantile as SS
import qualified Statistics.Sample as SS

import Control.Applicative (asum)
import Control.Exception
import Control.Monad (foldM_)
import Control.Monad.ST ( ST, runST )
import Data.DataFrame.Errors (DataFrameException(..))
import Data.DataFrame.Function
import Data.DataFrame.Internal (Column (..), DataFrame (..), ColumnValue)
import Data.DataFrame.Util
import Data.Either (isLeft, fromLeft)
import Data.Function (on, (&))
import Data.Hashable
import Data.Maybe (fromMaybe, isJust, isNothing)
import Data.Time
import Data.Type.Equality
  ( TestEquality (testEquality),
    type (:~:) (Refl),
  )
import Data.Typeable (Typeable)
import Data.Vector (Vector)
import GHC.Stack (HasCallStack)
import Prelude hiding (drop, filter, sum, take)
import Text.Read (readMaybe)
import Type.Reflection
import GHC.IO.Unsafe (unsafePerformIO)

-- | /O(n)/ Adds a vector to the dataframe.
addColumn ::
  forall a.
  (ColumnValue a) =>
  -- | Column Name
  T.Text ->
  -- | Vector to add to column
  V.Vector a ->
  -- | DataFrame to add column to
  DataFrame ->
  DataFrame
addColumn name xs = addColumn' name (Just (DI.toColumn' xs))

cloneColumn :: T.Text -> T.Text -> DataFrame -> DataFrame
cloneColumn original new df = fromMaybe (throw $ ColumnNotFoundException original "cloneColumn" (map fst $ M.toList $ DI.columnIndices df)) $ do
  column <- DI.getColumn original df
  return $ addColumn' new (Just column) df

-- | /O(n)/ Adds an unboxed vector to the dataframe.
addUnboxedColumn ::
  forall a.
  (ColumnValue a, VU.Unbox a) =>
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
            Just (GroupedBoxedColumn col) -> Just $ BoxedColumn $ V.map Just col <> V.replicate diff Nothing
            Just (GroupedUnboxedColumn col) -> Just $ BoxedColumn $ V.map Just (V.convert col) <> V.replicate diff Nothing
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
  (ColumnValue a) =>
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
      values = xs V.++ V.replicate (rows - V.length xs) defaultValue
   in addColumn' name (Just $ DI.toColumn' values) d

rename :: T.Text -> T.Text -> DataFrame -> DataFrame
rename orig new df = let
    columnIndex = (M.!) (DI.columnIndices df) orig
    newColumnIndices = M.insert new columnIndex (M.delete orig (DI.columnIndices df))
  in df { columnIndices = newColumnIndices}


as :: T.Text
   -> (a -> T.Text -> DataFrame -> DataFrame)
   -> a
   -> T.Text
   -> DataFrame
   -> DataFrame
as alias func f name = rename name alias . func f name

-- | O(k) Apply a function to a given column in a dataframe.
apply ::
  forall b c.
  (ColumnValue b, ColumnValue c) =>
  -- | function to apply
  (b -> c) ->
  -- | Column name
  T.Text ->
  -- | DataFrame to apply operation to
  DataFrame ->
  DataFrame
apply f columnName d = case DI.getColumn columnName d of
  Nothing -> throw $ ColumnNotFoundException columnName "apply" (map fst $ M.toList $ DI.columnIndices d)
  Just column -> case DI.transform f column of
    Nothing -> throw $ TypeMismatchException' (typeRep @b) (DI.columnTypeString column) columnName "apply"
    column' -> addColumn' columnName column' d

-- | O(k) Apply a function to a combination of columns in a dataframe and
-- add the result into `alias` column.
deriveF :: ([T.Text], Function) -> T.Text -> DataFrame -> DataFrame
deriveF (args, f) name df = case f of
  (F4 (f' :: a -> b -> c -> d -> e)) -> let
      xs = VG.map (\row -> funcApply @e row f) $ V.generate (fst (dimensions df)) (mkFuncArgs df args)
    in addColumn name xs df
  (F3 (f' :: a -> b -> c -> d)) -> let
      xs = VG.map (\row -> funcApply @d row f) $ V.generate (fst (dimensions df)) (mkFuncArgs df args)
    in addColumn name xs df
  (F2 (f' :: a -> b -> c)) -> let
      xs = VG.map (\row -> funcApply @c row f) $ V.generate (fst (dimensions df)) (mkFuncArgs df args)
    in addColumn name xs df
  (F1 (f' :: a -> b)) -> let
      xs = VG.map (\row -> funcApply @b row f) $ V.generate (fst (dimensions df)) (mkFuncArgs df args)
    in addColumn name xs df

mkFuncArgs :: DataFrame -> [T.Text] -> Int -> [FuncArg]
mkFuncArgs df names i = foldr go [] names
  where
    go name acc = case DI.getColumn name df of
      Nothing -> throw $ ColumnNotFoundException name "applyF" (map fst $ M.toList $ DI.columnIndices df)
      Just (BoxedColumn column) -> Arg (column V.! i) : acc
      Just (UnboxedColumn column) -> Arg (column VU.! i) : acc

-- | O(k) Apply a function to a given column in a dataframe and
-- add the result into alias column.

derive ::
  forall b c.
  (ColumnValue b, ColumnValue c) =>
  -- | New name
  T.Text ->
  -- | function to apply
  (b -> c) ->
  -- | Derivative column name
  T.Text ->
  -- | DataFrame to apply operation to
  DataFrame ->
  DataFrame
derive alias f columnName d = case DI.getColumn columnName d of
  Nothing -> throw $ ColumnNotFoundException columnName "derive" (map fst $ M.toList $ DI.columnIndices d)
  Just column -> case DI.transform f column of
    Nothing  -> throw $ TypeMismatchException (typeOf column) (typeRep @b) columnName "derive"
    Just res -> addColumn' alias (Just res) d

-- | O(k * n) Apply a function to given column names in a dataframe.
applyMany ::
  (ColumnValue b, ColumnValue c) =>
  (b -> c) ->
  [T.Text] ->
  DataFrame ->
  DataFrame
applyMany f names df = L.foldl' (flip (apply f)) df names

-- | O(k) Convenience function that applies to an int column.
applyInt ::
  (ColumnValue b) =>
  -- | Column name
  -- | function to apply
  (Int -> b) ->
  T.Text ->
  -- | DataFrame to apply operation to
  DataFrame ->
  DataFrame
applyInt = apply

-- | O(k) Convenience function that applies to an double column.
applyDouble ::
  (ColumnValue b) =>
  -- | Column name
  -- | function to apply
  (Double -> b) ->
  T.Text ->
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
  (ColumnValue a, ColumnValue b) =>
  (a -> Bool) -> -- Filter condition
  T.Text -> -- Criterion Column
  (b -> b) -> -- function to apply
  T.Text -> -- Column name
  DataFrame -> -- DataFrame to apply operation to
  DataFrame
applyWhere condition filterColumnName f columnName df = case DI.getColumn filterColumnName df of
  Nothing -> throw $ ColumnNotFoundException filterColumnName "applyWhere" (map fst $ M.toList $ DI.columnIndices df)
  Just column -> case DI.ifoldrColumn (\i val acc -> if condition val then V.cons i acc else acc) V.empty column of
      Nothing -> throw $ TypeMismatchException' (typeRep @a) (DI.columnTypeString column) filterColumnName "applyWhere"
      Just indexes -> if V.null indexes
                      then df
                      else L.foldl' (\d i -> applyAtIndex i f columnName d) df indexes

-- | O(k) Apply a function to the column at a given index.
applyAtIndex ::
  forall a.
  (ColumnValue a) =>
  -- | Index
  Int ->
  -- | function to apply
  (a -> a) ->
  -- | Column name
  T.Text ->
  -- | DataFrame to apply operation to
  DataFrame ->
  DataFrame
applyAtIndex i f columnName df = case DI.getColumn columnName df of
  Nothing -> throw $ ColumnNotFoundException columnName "applyAtIndex" (map fst $ M.toList $ DI.columnIndices df)
  Just column -> case DI.itransform (\index value -> if index == i then f value else value) column of
    Nothing -> throw $ TypeMismatchException' (typeRep @a) (DI.columnTypeString column) columnName "applyAtIndex"
    column' -> addColumn' columnName column' df

-- | O(k * n) Take the first n rows of a DataFrame.
take :: Int -> DataFrame -> DataFrame
take n d = d {columns = V.map (DI.takeColumn n <$>) (columns d), dataframeDimensions = (min (max n 0) r, c)}
  where
    (r, c) = DI.dataframeDimensions d

-- | O(k * n) Take a range of rows of a DataFrame.
range :: (Int, Int) -> DataFrame -> DataFrame
range (start, end) d = d {columns = V.map (DI.sliceColumn start (end - start) <$>) (columns d), dataframeDimensions = (min (max (end - start) 0) r, c)}
  where
    (r, c) = DI.dataframeDimensions d

-- | O(1) Get DataFrame dimensions i.e. (rows, columns)
dimensions :: DataFrame -> (Int, Int)
dimensions = DI.dataframeDimensions

-- | O(k) Get column names of the DataFrame in order of insertion.
columnNames :: DataFrame -> [T.Text]
columnNames = map fst . L.sortBy (compare `on` snd). M.toList . DI.columnIndices

-- | O(n * k) Filter rows by a given condition.
filter ::
  forall a.
  (ColumnValue a) =>
  -- | Column to filter by
  T.Text ->
  -- | Filter condition
  (a -> Bool) ->
  -- | Dataframe to filter
  DataFrame ->
  DataFrame
filter filterColumnName condition df = case DI.getColumn filterColumnName df of
  Nothing -> throw $ ColumnNotFoundException filterColumnName "filter" (map fst $ M.toList $ DI.columnIndices df)
  Just column -> case DI.ifoldlColumn (\s i v -> if condition v then S.insert i s else s) S.empty column of
    Nothing -> throw $ TypeMismatchException' (typeRep @a) (DI.columnTypeString column) filterColumnName "filter"
    Just indexes -> let
        c' = snd $ dimensions df
        pick idxs col = DI.atIndices idxs <$> col
      in df {columns = V.map (pick indexes) (columns df), dataframeDimensions = (S.size indexes, c')}

filterBy :: (ColumnValue a) => (a -> Bool) -> T.Text -> DataFrame -> DataFrame
filterBy = flip filter

-- | Sort order taken as a parameter by the sortby function.
data SortOrder = Ascending | Descending deriving (Eq)

-- | O(k log n) Sorts the dataframe by a given row. Currently uses insertion sort
-- under the hood so can be very inefficient for large data.
--
-- > sortBy "Age" df
sortBy ::
  SortOrder ->
  [T.Text] ->
  DataFrame ->
  DataFrame
sortBy order names df
  | not $ any (`elem` columnNames df) names = throw $ ColumnNotFoundException (T.pack $ show $ names L.\\ columnNames df) "sortBy" (columnNames df)
  | otherwise = let
      -- TODO: Remove the SortOrder defintion from operations so we can share it between here and internal and
      -- we don't have to do this Bool mapping.
      indexes = DI.sortedIndexes' (order == Ascending) (DI.toRowList names df)
      pick idxs col = DI.atIndicesStable idxs <$> col
    in df {columns = V.map (pick indexes) (columns df)}

-- | O(1) Get the number of elements in a given column.
columnSize :: T.Text -> DataFrame -> Maybe Int
columnSize name df = DI.columnLength <$> DI.getColumn name df

-- | O (k * n) Counts the occurences of each value in a given column.
valueCounts :: forall a. (ColumnValue a) => T.Text -> DataFrame -> [(a, Integer)]
valueCounts columnName df =
  let repa :: Type.Reflection.TypeRep a = Type.Reflection.typeRep @a
   in case columnName `MS.lookup` DI.columnIndices df of
      Nothing -> throw $ ColumnNotFoundException columnName "sortBy" (map fst $ M.toList $ DI.columnIndices df)
      Just i -> case DI.columns df V.!? i of
        Nothing -> error "Internal error: Column is empty"
        Just c -> case c of
          Just (BoxedColumn (column' :: V.Vector c)) ->
            let repc :: Type.Reflection.TypeRep c = Type.Reflection.typeRep @c
                column = V.foldl' (\m v -> MS.insertWith (+) v (1 :: Integer) m) M.empty column'
            in case repa `testEquality` repc of
                  Nothing -> throw $ TypeMismatchException (typeRep @a) (typeRep @c) columnName "valueCounts"
                  Just Refl -> M.toAscList column
          Just (UnboxedColumn (column' :: VU.Vector c)) ->
            let repc :: Type.Reflection.TypeRep c = Type.Reflection.typeRep @c
                column = VU.foldl' (\m v -> MS.insertWith (+) v (1 :: Integer) m) M.empty column'
            in case repa `testEquality` repc of
                  Nothing -> throw $ TypeMismatchException (typeRep @a) (typeRep @c) columnName "valueCounts"
                  Just Refl -> M.toAscList column

-- | O(n) Selects a number of columns in a given dataframe.
--
-- > select ["name", "age"] df
select ::
  [T.Text] ->
  DataFrame ->
  DataFrame
select cs df
  | not $ any (`elem` columnNames df) cs = throw $ ColumnNotFoundException (T.pack $ show $ cs L.\\ columnNames df) "select" (columnNames df)
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
exclude ::
  [T.Text] ->
  DataFrame ->
  DataFrame
exclude cs df =
  let keysToKeep = columnNames df L.\\ cs
   in select keysToKeep df

-- | O(k * n) groups the dataframe by the given rows aggregating the remaining rows
-- into vector that should be reduced later.
groupBy ::
  [T.Text] ->
  DataFrame ->
  DataFrame
groupBy names df
  | not $ any (`elem` columnNames df) names = throw $ ColumnNotFoundException (T.pack $ show $ names L.\\ columnNames df) "groupBy" (columnNames df)
  | otherwise = L.foldl' addColumns initDf groupingColumns
  where
    -- Create a string representation of each row.
    values = V.generate (fst (dimensions df)) (mkRowRep df (S.fromList names))
    -- Create a mapping from the row representation to the list of indices that
    -- have that row representation. This will allow us sortedIndexesto combine the indexes
    -- where the rows are the same.
    valueIndices = V.ifoldl' (\m index rowRep -> MS.insertWith (appendWithFrontMin . head) rowRep [index] m) M.empty values
    -- Since the min is at the head this allows us to get the min in constant time and sort by it
    -- That way we can recover the original order of the rows.
    -- valueIndicesInitOrder = L.sortBy (compare `on` snd) $! MS.toList $ MS.map VU.head valueIndices
    valueIndicesInitOrder = runST $ do
      v <- VM.new (MS.size valueIndices)
      foldM_ (\i idxs -> VM.write v i (VU.fromList idxs) >> return (i + 1)) 0 valueIndices
      V.unsafeFreeze v

    -- These are the indexes of the grouping/key rows i.e the minimum elements
    -- of the list.
    keyIndices = VU.generate (VG.length valueIndicesInitOrder) (\i -> VG.head $ valueIndicesInitOrder VG.! i)
    -- this will be our main worker function in the fold that takes all
    -- indices and replaces each value in a column with a list of
    -- the elements with the indices where the grouped row
    -- values are the same.
    addColumns = groupColumns valueIndicesInitOrder df
    -- Out initial DF will just be all the grouped rows added to an
    -- empty dataframe. The entries are dedued and are in their
    -- initial order.
    initDf = L.foldl' (mkGroupedColumns keyIndices df) DI.empty names
    -- All the rest of the columns that we are grouping by.
    groupingColumns = columnNames df L.\\ names

mkRowRep :: DataFrame -> S.Set T.Text -> Int -> Int
mkRowRep df names i = hash $ V.ifoldl' go "" (columns df)
  where
    indexMap = M.fromList (map (\(a, b) -> (b, a)) $ M.toList (DI.columnIndices df))
    go acc k Nothing = acc
    go acc k (Just (BoxedColumn c)) =
      if S.notMember (indexMap M.! k) names
        then acc
        else case c V.!? i of
          Just e -> acc <> (T.pack . show) e
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
          Just e -> acc <> (T.pack . show) e
          Nothing ->
            error $
              "Column "
                ++ T.unpack (indexMap M.! k)
                ++ " has less items than "
                ++ "the other columns at index "
                ++ show i

mkGroupedColumns :: VU.Vector Int -> DataFrame -> DataFrame -> T.Text -> DataFrame
mkGroupedColumns indices df acc name =
  case (V.!) (DI.columns df) (DI.columnIndices df M.! name) of
    Nothing -> error "Unexpected"
    (Just (BoxedColumn column)) ->
      let vs = indices `getIndices` column
       in addColumn name vs acc
    (Just (UnboxedColumn column)) ->
      let vs = indices `getIndicesUnboxed` column
       in addUnboxedColumn name vs acc

groupColumns :: V.Vector (VU.Vector Int) -> DataFrame -> DataFrame -> T.Text -> DataFrame
groupColumns indices df acc name =
  case (V.!) (DI.columns df) (DI.columnIndices df M.! name) of
    Nothing -> df
    (Just (BoxedColumn column)) ->
      let vs = V.map (`getIndices` column) indices
       in addColumn' name (Just $ GroupedBoxedColumn vs) acc
    (Just (UnboxedColumn column)) ->
      let vs = V.map (`getIndicesUnboxed` column) indices
       in addColumn' name (Just $ GroupedUnboxedColumn vs) acc

fold :: Foldable t => (a -> DataFrame -> DataFrame) -> t a -> DataFrame -> DataFrame
fold f = flip (L.foldl' (flip f))

data Aggregation = Count
                 | Mean
                 | Minimum
                 | Median
                 | Maximum
                 | Sum deriving (Show, Eq)

groupByAgg :: Aggregation -> [T.Text] -> DataFrame -> DataFrame
groupByAgg agg columnNames df = let
  in case agg of
    Count -> addColumnWithDefault @Int 1 (T.pack (show agg)) V.empty df
           & groupBy columnNames
           & reduceBy @Int VG.length "Count"
    _ -> error "UNIMPLEMENTED"

-- O (k * n) Reduces a vector valued volumn with a given function.
reduceBy ::
  forall a b . (ColumnValue a, ColumnValue b) =>
  (forall v . (VG.Vector v a) => v a -> b) ->
  T.Text ->
  DataFrame ->
  DataFrame
reduceBy f name df = case DI.getColumn name df of
    Just ((GroupedBoxedColumn (column :: Vector (Vector a')))) -> case testEquality (typeRep @a) (typeRep @a') of
      Just Refl -> addColumn' name (Just $ DI.toColumn' (VG.map f column)) df
      Nothing -> error "Type error"
    Just ((GroupedUnboxedColumn (column :: Vector (VU.Vector a')))) -> case testEquality (typeRep @a) (typeRep @a') of
      Just Refl -> addColumn' name (Just $ DI.toColumn' (VG.map f column)) df
      Nothing -> error "Type error"
    _ -> error "Column is ungrouped"

reduceByAgg :: Aggregation
            -> T.Text
            -> DataFrame
            -> DataFrame
reduceByAgg agg name df = case agg of
  Count   -> case DI.getColumn name df of
    Just ((GroupedBoxedColumn (column :: Vector (Vector a')))) ->  addColumn' name (Just $ DI.toColumn' (VG.map VG.length column)) df
    Just ((GroupedUnboxedColumn (column :: Vector (VU.Vector a')))) ->  addColumn' name (Just $ DI.toColumn' (VG.map VG.length column)) df
  Mean    -> case DI.getColumn name df of
    Just ((GroupedBoxedColumn (column :: Vector (Vector a')))) -> case testEquality (typeRep @a') (typeRep @Int) of
      Just Refl -> addColumn' name (Just $ DI.toColumn' (VG.map (SS.mean . VG.map fromIntegral) column)) df
      Nothing -> case testEquality (typeRep @a') (typeRep @Double) of
        Just Refl -> addColumn' name (Just $ DI.toColumn' (VG.map SS.mean column)) df
        Nothing -> case testEquality (typeRep @a') (typeRep @Float) of
          Just Refl -> addColumn' name (Just $ DI.toColumn' (VG.map (SS.mean . VG.map realToFrac) column)) df
          Nothing -> case testEquality (typeRep @a') (typeRep @(Maybe Int)) of
            Just Refl -> addColumn' name (Just $ DI.toColumn' (VG.map (SS.mean . VG.map (fromIntegral . fromMaybe 0) . VG.filter isJust) column)) df
            Nothing -> case testEquality (typeRep @a') (typeRep @(Maybe Double)) of
              Just Refl -> addColumn' name (Just $ DI.toColumn' (VG.map (SS.mean . VG.map (fromMaybe 0) . VG.filter isJust) column)) df
              Nothing -> case testEquality (typeRep @a') (typeRep @(Maybe Float)) of
                Just Refl -> addColumn' name (Just $ DI.toColumn' (VG.map (SS.mean . VG.map (realToFrac . fromMaybe 0) . VG.filter isJust) column)) df
                Nothing -> error $ "Cannot get mean of non-numeric column: " ++ T.unpack name -- Not sure what to do with no numeric - return nothing???
    Just ((GroupedUnboxedColumn (column :: Vector (VU.Vector a')))) -> case testEquality (typeRep @a') (typeRep @Int) of
      Just Refl -> addColumn' name (Just $ DI.toColumn' (VG.map (SS.mean . VG.map fromIntegral) column)) df
      Nothing -> case testEquality (typeRep @a') (typeRep @Double) of
        Just Refl -> addColumn' name (Just $ DI.toColumn' (VG.map SS.mean column)) df
        Nothing -> case testEquality (typeRep @a') (typeRep @Float) of
          Just Refl -> addColumn' name (Just $ DI.toColumn' (VG.map (SS.mean . VG.map realToFrac) column)) df
          Nothing -> case testEquality (typeRep @a') (typeRep @(Maybe Int)) of
            Just Refl -> addColumn' name (Just $ DI.toColumn' (VG.map (SS.mean . VG.map (fromIntegral . fromMaybe 0) . VG.filter isJust) column)) df
            Nothing -> case testEquality (typeRep @a') (typeRep @(Maybe Double)) of
              Just Refl -> addColumn' name (Just $ DI.toColumn' (VG.map (SS.mean . VG.map (fromMaybe 0) . VG.filter isJust) column)) df
              Nothing -> case testEquality (typeRep @a') (typeRep @(Maybe Float)) of
                Just Refl -> addColumn' name (Just $ DI.toColumn' (VG.map (SS.mean . VG.map (realToFrac . fromMaybe 0) . VG.filter isJust) column)) df
                Nothing -> error $ "Cannot get mean of non-numeric column: " ++ T.unpack name -- Not sure what to do with no numeric - return nothing???
  Minimum -> case DI.getColumn name df of
    Just ((GroupedBoxedColumn (column :: Vector (Vector a')))) ->  addColumn' name (Just $ DI.toColumn' (VG.map VG.minimum column)) df
    Just ((GroupedUnboxedColumn (column :: Vector (VU.Vector a')))) ->  addColumn' name (Just $ DI.toColumn' (VG.map VG.minimum column)) df
  Maximum -> case DI.getColumn name df of
    Just ((GroupedBoxedColumn (column :: Vector (Vector a')))) ->  addColumn' name (Just $ DI.toColumn' (VG.map VG.maximum column)) df
    Just ((GroupedUnboxedColumn (column :: Vector (VU.Vector a')))) ->  addColumn' name (Just $ DI.toColumn' (VG.map VG.maximum column)) df
  Sum -> case DI.getColumn name df of
    Just ((GroupedBoxedColumn (column :: Vector (Vector a')))) -> case testEquality (typeRep @a') (typeRep @Int) of
      Just Refl -> addColumn' name (Just $ DI.toColumn' (VG.map VG.sum column)) df
      Nothing -> case testEquality (typeRep @a') (typeRep @Double) of
        Just Refl -> addColumn' name (Just $ DI.toColumn' (VG.map VG.sum column)) df
        Nothing -> case testEquality (typeRep @a') (typeRep @(Maybe Int)) of
          Just Refl -> addColumn' name (Just $ DI.toColumn' (VG.map (VG.sum . VG.map (fromMaybe 0) . VG.filter isJust) column)) df
          Nothing -> case testEquality (typeRep @a') (typeRep @(Maybe Double)) of
            Just Refl -> addColumn' name (Just $ DI.toColumn' (VG.map (VG.sum . VG.map (fromMaybe 0) . VG.filter isJust) column)) df
            Nothing -> error $ "Cannot get sum of non-numeric column: " ++ T.unpack name -- Not sure what to do with no numeric - return nothing???
    Just ((GroupedUnboxedColumn (column :: Vector (VU.Vector a')))) -> case testEquality (typeRep @a') (typeRep @Int) of
      Just Refl -> addColumn' name (Just $ DI.toColumn' (VG.map VG.sum column)) df
      Nothing -> case testEquality (typeRep @a') (typeRep @Double) of
        Just Refl -> addColumn' name (Just $ DI.toColumn' (VG.map VG.sum column)) df
        Nothing -> case testEquality (typeRep @a') (typeRep @(Maybe Int)) of
          Just Refl -> addColumn' name (Just $ DI.toColumn' (VG.map (VG.sum . VG.map (fromMaybe 0) . VG.filter isJust) column)) df
          Nothing -> case testEquality (typeRep @a') (typeRep @(Maybe Double)) of
            Just Refl -> addColumn' name (Just $ DI.toColumn' (VG.map (VG.sum . VG.map (fromMaybe 0) . VG.filter isJust) column)) df
            Nothing -> error $ "Cannot get sum of non-numeric column: " ++ T.unpack name -- Not sure what to do with no numeric - return nothing???
  _ -> error "UNIMPLEMENTED"

aggregate :: [(T.Text, Aggregation)] -> DataFrame -> DataFrame
aggregate aggs df = fold (\(name, agg) df -> let
    alias = (T.pack . show) agg <> "_" <> name
  in cloneColumn name alias df |> reduceByAgg agg alias) aggs df
  |> fold (\name df -> exclude [name] df) (map fst aggs)

parseDefaults :: Bool -> DataFrame -> DataFrame
parseDefaults safeRead df = df {columns = V.map (parseDefault safeRead) (columns df)}

parseDefault :: Bool -> Maybe Column -> Maybe Column
parseDefault _ Nothing = Nothing
parseDefault safeRead (Just (BoxedColumn (c :: V.Vector a))) =
  let repa :: Type.Reflection.TypeRep a = Type.Reflection.typeRep @a
      repText :: Type.Reflection.TypeRep T.Text = Type.Reflection.typeRep @T.Text
      parseTimeOpt s = parseTimeM {- Accept leading/trailing whitespace -} True defaultTimeLocale "%Y-%m-%d" (T.unpack s) :: Maybe Day
      unsafeParseTime s = parseTimeOrError {- Accept leading/trailing whitespace -} True defaultTimeLocale "%Y-%m-%d" (T.unpack s) :: Day
   in case repa `testEquality` repText of
        Nothing -> case repa `testEquality` (typeRep @String) of
            Just Refl -> let
                nullish = S.fromList ["nan", "NULL", "null", "", " "]
                emptyToNothing v = if S.member v nullish then Nothing else Just v
                safeVector = V.map emptyToNothing c
                hasNulls = V.foldl' (\acc v -> if isNothing v then acc || True else acc) False safeVector
              in Just $ if safeRead && hasNulls then BoxedColumn safeVector else BoxedColumn c
            Nothing -> Just $ BoxedColumn c
        Just Refl ->
          let example = T.strip (V.head c)
              nullish = S.fromList ["nan", "NULL", "null", "", " "]
              emptyToNothing v = if S.member v nullish then Nothing else Just v
           in case readInt example of
                Just _ ->
                  let safeVector = V.map ((=<<) readInt . emptyToNothing) c
                      hasNulls = V.elem Nothing safeVector
                   in Just $ if safeRead && hasNulls then BoxedColumn safeVector else UnboxedColumn (VU.generate (V.length c) (fromMaybe 0  . (safeVector V.!)))
                Nothing -> case readDouble example of
                  Just _ ->
                    let safeVector = V.map ((=<<) readDouble . emptyToNothing) c
                        hasNulls = V.elem Nothing safeVector
                     in Just $ if safeRead && hasNulls then BoxedColumn safeVector else UnboxedColumn (VU.generate (V.length c) (fromMaybe 0 . (safeVector V.!)))
                  Nothing -> case parseTimeOpt example of
                    Just d -> let
                        -- failed parse should be Either, nullish should be Maybe
                        emptyToNothing' v = if S.member v nullish then Left v else Right v
                        parseTimeEither v = case parseTimeOpt v of
                          Just v' -> Right v'
                          Nothing -> Left v
                        safeVector = V.map ((=<<) parseTimeEither . emptyToNothing') c
                        toMaybe (Left _) = Nothing
                        toMaybe (Right value) = Just value
                        lefts = V.filter isLeft safeVector
                        onlyNulls = (not (V.null lefts) && V.all ((`S.member` nullish) . fromLeft "non-null") lefts)
                      in Just $ if safeRead
                        then if onlyNulls
                             then BoxedColumn (V.map toMaybe safeVector)
                             else if V.any isLeft safeVector
                              then BoxedColumn safeVector
                              else BoxedColumn (V.map unsafeParseTime c)
                        else BoxedColumn (V.map unsafeParseTime c)
                    Nothing -> let
                        safeVector = V.map emptyToNothing c
                        hasNulls = isJust $ V.find (`S.member` nullish) c
                      in Just $ if safeRead && hasNulls then BoxedColumn safeVector else BoxedColumn c
parseDefault safeRead column = column

-- | O(n) Returns the number of non-null columns in the dataframe and the type associated
-- with each column.
columnInfo :: DataFrame -> DataFrame
columnInfo df = DI.empty & addColumn' "Column Name" (Just $ DI.toColumn (map fst' triples))
                         & addColumn' "# Non-null Values" (Just $ DI.toColumn (map snd' triples))
                         & addColumn' "# Null Values" (Just $ DI.toColumn (map thd' triples))
                         & addColumn' "Type" (Just $ DI.toColumn (map fth' triples))
  where
    triples = L.sortBy (compare `on` snd') (V.ifoldl' go [] (columns df)) :: [(String, Int,  Int, String)]
    indexMap = M.fromList (map (\(a, b) -> (b, a)) $ M.toList (DI.columnIndices df))
    columnName i = T.unpack (indexMap M.! i)
    numNulls c = VG.length $ VG.filter (`S.member` nullish) c
    go acc i Nothing = acc
    go acc i (Just (BoxedColumn (c :: Vector a))) = (columnName i, VG.length c - numNulls (V.map show c), numNulls (V.map show c), show $ typeRep @a) : acc
    go acc i (Just (UnboxedColumn (c :: VU.Vector a))) = (columnName i, VG.length c - numNulls (VG.map show (V.convert c)), numNulls (VG.map show (V.convert c)), show $ typeRep @a) : acc
    nullish = S.fromList ["Nothing", "NULL", "", " ", "nan"]
    fst' (x, _, _, _) = x
    snd' (_, x, _, _) = x
    thd' (_, _, x, _) = x
    fth' (_, _, _, x) = x


fromList :: [(T.Text, Column)] -> DataFrame
fromList = L.foldl' (\df (name, column) -> addColumn' name (Just column) df) DI.empty

frequencies :: T.Text -> DataFrame -> DataFrame
frequencies name df = case name `MS.lookup` DI.columnIndices df of
    Nothing -> throw $ ColumnNotFoundException name "apply" (map fst $ M.toList $ DI.columnIndices df)
    Just i -> case DI.columns df V.!? i of
      Nothing -> error "Internal error: Column is empty"
      Just c -> case c of
        Just ((BoxedColumn (column :: V.Vector a))) -> let
            counts = valueCounts @a name df
            total = P.sum $ map snd counts
            vText :: forall a . (ColumnValue a) => a -> T.Text
            vText c' = case testEquality (typeRep @a) (typeRep @T.Text) of
              Just Refl -> c'
              Nothing -> case testEquality (typeRep @a) (typeRep @String) of
                Just Refl -> T.pack c'
                Nothing -> (T.pack . show) c'
            initDf = DI.empty & addColumn "Statistic" (V.fromList ["Count" :: T.Text,  "Percentage (%)"])
          in L.foldl' (\df (col, k) -> addColumn (vText col) (V.fromList [k, k * 100 `div` total]) df) initDf counts
        Just ((UnboxedColumn (column :: VU.Vector a))) -> let
            counts = valueCounts @a name df
            total = P.sum $ map snd counts
            vText :: forall a . (ColumnValue a) => a -> T.Text
            vText c' = case testEquality (typeRep @a) (typeRep @T.Text) of
              Just Refl -> c'
              Nothing -> case testEquality (typeRep @a) (typeRep @String) of
                Just Refl -> T.pack c'
                Nothing -> (T.pack . show) c'
            initDf = DI.empty & addColumn "Statistic" (V.fromList ["Count" :: T.Text,  "Percentage (%)"])
          in L.foldl' (\df (col, k) -> addColumn (vText col) (V.fromList [k, k * 100 `div` total]) df) initDf counts

mean :: T.Text -> DataFrame -> Maybe Double
mean = applyStatistic SS.mean

median :: T.Text -> DataFrame -> Maybe Double
median = applyStatistic (SS.median SS.medianUnbiased)

standardDeviation :: T.Text -> DataFrame -> Maybe Double
standardDeviation = applyStatistic SS.fastStdDev

skewness :: T.Text -> DataFrame -> Maybe Double
skewness = applyStatistic SS.skewness

variance :: T.Text -> DataFrame -> Maybe Double
variance = applyStatistic SS.variance

interQuartileRange :: T.Text -> DataFrame -> Maybe Double
interQuartileRange = applyStatistic (SS.midspread SS.medianUnbiased 4)

correlation :: T.Text -> T.Text -> DataFrame -> Maybe Double
correlation first second df = DI.reduceColumn @(VU.Vector (Double, Double)) SS.correlation <$> (DI.zipColumns <$> DI.getColumn first df <*> DI.getColumn second df)

sum :: T.Text -> DataFrame -> Maybe Double
sum name df = case DI.getColumn name df of
  Just ((UnboxedColumn (column :: VU.Vector a'))) -> case testEquality (typeRep @a') (typeRep @Int) of
    Just Refl -> Just $ VG.sum (VU.map fromIntegral column)
    Nothing -> case testEquality (typeRep @a') (typeRep @Double) of
      Just Refl -> Just $ VG.sum column
      Nothing -> Nothing
  Nothing -> Nothing

applyStatistic :: (VU.Vector Double -> Double) -> T.Text -> DataFrame -> Maybe Double
applyStatistic f name df = do
      column <- DI.getColumn name df
      matching <- asum [ DI.transform (fromIntegral :: Int -> Double) column,
                         DI.transform (realToFrac :: Float -> Double) column,
                         Just column ]
      DI.safeReduceColumn f matching

applyStatistics :: (VU.Vector Double -> VU.Vector Double) -> T.Text -> DataFrame -> Maybe (VU.Vector Double)
applyStatistics f name df = case DI.getColumn name df of
  Just ((UnboxedColumn (column :: VU.Vector a'))) -> case testEquality (typeRep @a') (typeRep @Int) of
    Just Refl -> Just $ f (VU.map fromIntegral column)
    Nothing -> case testEquality (typeRep @a') (typeRep @Double) of
      Just Refl -> Just $ f column
      Nothing -> case testEquality (typeRep @a') (typeRep @Float) of
        Just Refl -> Just $ f (VG.map realToFrac column)
        Nothing -> Nothing
  _ -> Nothing

summarize :: DataFrame -> DataFrame
summarize df = fold columnStats (columnNames df) (fromList [("Statistic", DI.toColumn ["Mean" :: T.Text, "Minimum", "25%" ,"Median", "75%", "Max", "StdDev", "IQR", "Skewness"])])
  where columnStats name d = if all isJust (stats name) then addUnboxedColumn name (VU.fromList (map (roundTo 2 . fromMaybe 0) $ stats name)) d else d
        stats name = let
            quantiles = applyStatistics (SS.quantilesVec SS.medianUnbiased (VU.fromList [0,1,2,3,4]) 4) name df
            min' = flip (VG.!) 0 <$> quantiles
            quartile1 = flip (VG.!) 1 <$> quantiles
            median' = flip (VG.!) 2 <$> quantiles
            quartile3 = flip (VG.!) 3 <$> quantiles
            max' = flip (VG.!) 4 <$> quantiles
            iqr = (-) <$> quartile3 <*> quartile1
          in [mean name df,
              min',
              quartile1,
              median',
              quartile3,
              max',
              standardDeviation name df,
              iqr,
              skewness name df]
        roundTo :: Int -> Double -> Double
        roundTo n x = fromInteger (round $ x * (10^n)) / (10.0^^n)

(|>) :: a -> (a -> b) -> b
(|>) = (&)
