{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
module Data.DataFrame.Operations (
    addColumn,
    addColumn',
    addColumnWithDefault,
    dimensions,
    columnNames,
    apply,
    applyMany,
    applyWhere,
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
    SortOrder(..),
    columnInfo
) where

import qualified Data.DataFrame.Internal as DI
import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Map.Strict as MS
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU
import qualified Prelude as P

import Data.DataFrame.Internal ( Column(..), DataFrame(..) )
import Data.DataFrame.Util
import Data.Function (on)
import Data.Maybe (fromMaybe, isJust, isNothing)
import Data.Typeable (Typeable)
import Data.Type.Equality
    ( type (:~:)(Refl), TestEquality(testEquality) )
import Data.Vector (Vector)
import GHC.Stack (HasCallStack)
import Prelude hiding (take, sum, filter, drop)
import Text.Read (readMaybe)
import Type.Reflection ( Typeable, TypeRep, typeRep, typeOf )

-- | /O(log n)/ Adds a vector to the dataframe. 
addColumn :: forall a. (Typeable a, Show a, Ord a)
          => T.Text            -- ^ Column Name
          -> V.Vector a        -- ^ Vector to add to column
          -> DataFrame         -- ^ DataFrame to add column to
          -> DataFrame
addColumn name xs d = d {
           -- TODO: Ensure that the column has the same
           -- length as the other columns.
           columns = MS.insert name (DI.toColumn xs) (columns d),
           _columnNames = if name `elem` _columnNames d
                            then _columnNames d
                          else _columnNames d ++ [name] }

-- | /O(log n)/ Adds an unboxed vector to the dataframe. 
addUnboxedColumn :: forall a. (Typeable a, Show a, Ord a, VU.Unbox a)
                 => T.Text       -- ^ Column Name
                 -> VU.Vector a  -- ^ Unboxed vector to add to column
                 -> DataFrame    -- ^ DataFrame to add to column
                 -> DataFrame
addUnboxedColumn name xs d = d {
           columns = MS.insert name (DI.toColumnUnboxed xs) (columns d),
           _columnNames = if name `elem` _columnNames d
                          then _columnNames d
                          else _columnNames d ++ [name] }

-- | /O(log n)/ Add a column to the dataframe. Not meant for external use.
addColumn' :: T.Text      -- ^ Column Name
           -> Column      -- ^ Column to add
           -> DataFrame   -- ^ DataFrame to add to column
           -> DataFrame
addColumn' name xs d = d {
           columns = MS.insert name xs (columns d),
           _columnNames = if name `elem` _columnNames d
                            then _columnNames d
                          else _columnNames d ++ [name] }

-- | /O(k)/ Add a column to the dataframe providing a default.
-- This constructs a new vector and also may convert it
-- to an unboxed vector if necessary. Since columns usually
-- large the runtime is dominated by the length of the list, k.
addColumnWithDefault :: forall a. (Typeable a, Show a, Ord a)
          => a          -- ^ Default Value
          -> T.Text     -- ^ Column name
          -> V.Vector a -- ^ Data to add to column
          -> DataFrame  -- ^ DataFrame to add to column
          -> DataFrame
addColumnWithDefault defaultValue name xs d = let
        (rows, _) = dimensions d
        values = xs V.++ V.fromList (repeat defaultValue)
    in addColumn' name (DI.toColumn $ V.take rows values) d

-- | O(k) Apply a function to a given column in a dataframe.
apply :: forall b c. (Typeable b, Typeable c, Show b, Show c, Ord b, Ord c)
      => T.Text         -- ^ Column name
      -> (b -> c)       -- ^ function to apply
      -> DataFrame      -- ^ DataFrame to apply operation to
      -> DataFrame
apply columnName f d = case columnName `MS.lookup` columns d of
    Nothing -> error $ columnNotFound columnName "apply" (columnNames d)
    Just ((BoxedColumn (column :: V.Vector a))) -> let
        in case testEquality (typeRep @a) (typeRep @b) of
            Just Refl -> addColumn' columnName (DI.toColumn (V.map f column)) d
            Nothing -> error $ addCallPointInfo columnName (Just "apply") (typeMismatchError (typeRep @a) (typeRep @b))
    Just ((UnboxedColumn (column :: VU.Vector a))) -> let
        in case testEquality (typeRep @a) (typeRep @b) of
            Just Refl -> case testEquality (typeRep @c) (typeRep @Double) of
                        Just Refl -> addUnboxedColumn columnName (VU.map f column) d
                        Nothing -> case testEquality (typeRep @c) (typeRep @Int) of
                            Just Refl -> addUnboxedColumn columnName (VU.map f column) d
                            Nothing -> addColumn' columnName (DI.toColumn (V.map f (V.convert column))) d
            Nothing -> error $ addCallPointInfo columnName (Just "apply") (typeMismatchError (typeRep @a) (typeRep @b))

-- | O(k * n) Apply a function to given column names in a dataframe.
applyMany :: (Typeable b, Typeable c, Show b, Show c, Ord b, Ord c)
          => [T.Text]
          -> (b -> c)
          -> DataFrame
          -> DataFrame
applyMany names f df = L.foldl' (\d name -> apply name f d) df names

-- | O(k) Convenience function that applies to an int column.
applyInt :: (Typeable b, Show b, Ord b)
         => T.Text       -- ^ Column name
         -> (Int -> b)   -- ^ function to apply
         -> DataFrame    -- ^ DataFrame to apply operation to
         -> DataFrame
applyInt = apply

-- | O(k) Convenience function that applies to an double column.
applyDouble :: (Typeable b, Show b, Ord b)
            => T.Text          -- ^ Column name
            -> (Double -> b)   -- ^ function to apply
            -> DataFrame       -- ^ DataFrame to apply operation to
            -> DataFrame
applyDouble = apply

-- | O(k * n) Apply a function to a column only if there is another column
-- value that matches the given criterion.
-- 
-- > applyWhere "Age" (<20) "Generation" (const "Gen-Z")
applyWhere :: forall a b . (Typeable a, Typeable b, Show a, Show b, Ord a, Ord b)
           => T.Text      -- Criterion Column
           -> (a -> Bool) -- Filter condition
           -> T.Text      -- Column name
           -> (b -> b)    -- function to apply
           -> DataFrame   -- DataFrame to apply operation to
           -> DataFrame
applyWhere filterColumnName condition columnName f df = case filterColumnName `M.lookup` columns df of
    Nothing -> error $ columnNotFound columnName "apply" (columnNames df)
    Just (BoxedColumn (column :: Vector c)) -> case (typeRep @a) `testEquality` (typeRep @c) of
        Nothing -> error $ addCallPointInfo columnName (Just "applyWhere") (typeMismatchError (typeRep @a) (typeRep @b))
        Just Refl -> let
                filterColumn = V.indexed column
                indexes = V.map fst $ V.filter (condition . snd) filterColumn
            in if V.null indexes
            then df
            else L.foldl' (\d i -> applyAtIndex i columnName f d) df indexes
    Just (UnboxedColumn (column :: VU.Vector c)) -> case (typeRep @a) `testEquality` (typeRep @c) of
        Nothing -> error $ addCallPointInfo columnName (Just "applyWhere") (typeMismatchError (typeRep @a) (typeRep @b))
        Just Refl -> let
                filterColumn = VU.indexed column
                indexes = VU.map fst $ VU.filter (condition . snd) filterColumn
            in if VU.null indexes
            then df
            else VU.foldl' (\d i -> applyAtIndex i columnName f d) df indexes

-- | O(k) Apply a function to the column at a given index.
applyAtIndex :: forall a. (Typeable a, Show a, Ord a)
           => Int         -- ^ Index
           -> T.Text      -- ^ Column name
           -> (a -> a)    -- ^ function to apply
           -> DataFrame   -- ^ DataFrame to apply operation to
           -> DataFrame
applyAtIndex i columnName f df = case columnName `M.lookup` columns df of
        Nothing -> error $ columnNotFound columnName "apply" (columnNames df)
        Just (BoxedColumn (column :: Vector b)) -> case (typeRep @a) `testEquality` (typeRep @b) of
            Nothing -> error $ addCallPointInfo columnName (Just "applyWhere") (typeMismatchError (typeRep @a) (typeRep @b))
            Just Refl -> let
                    updated = V.imap (\index value -> if index == i then f value else value) column
                in addColumn columnName updated df
        Just (UnboxedColumn (column :: VU.Vector b)) -> case (typeRep @a) `testEquality` (typeRep @b) of
            Nothing -> error $ addCallPointInfo columnName (Just "applyWhere") (typeMismatchError (typeRep @a) (typeRep @b))
            Just Refl -> let
                    updated = VU.imap (\index value -> if index == i then f value else value) column
                in addUnboxedColumn columnName updated df

-- | O(k * n) Take the first n rows of a DataFrame.
take :: Int -> DataFrame -> DataFrame
take n d = d { columns = MS.map take' (columns d) }
    where take' (BoxedColumn column) = BoxedColumn (V.take n column)
          take' (UnboxedColumn column) = UnboxedColumn (VU.take n column)

-- | O(k) Get DataFrame dimensions i.e. (rows, columns)
dimensions :: DataFrame -> (Int, Int)
dimensions d = (numRows, numColumns)
    where columnSize (BoxedColumn column') = V.length column'
          columnSize (UnboxedColumn column') = VU.length column'
          numRows = M.foldr (\c acc -> max acc (columnSize c)) 0 (columns d)
          numColumns = MS.size $ columns d

-- | O(1) Get column names of the DataFrame in order of insertion.
columnNames :: DataFrame -> [T.Text]
columnNames = _columnNames

-- | O(n * k) Filter rows by a given condition.
filter :: forall a . (Typeable a, Show a, Ord a)
       => T.Text       -- ^ Column to filter by
       -> (a -> Bool)  -- ^ Filter condition
       -> DataFrame    -- ^ Dataframe to filter
       -> DataFrame
filter filterColumnName condition df = let
        pick indexes c@(BoxedColumn column) = BoxedColumn $ V.ifilter (\i v -> i `S.member` indexes) column
        pick indexes c@(UnboxedColumn column) = UnboxedColumn $ VU.ifilter (\i v -> i `S.member` indexes) column
    in case filterColumnName `M.lookup` columns df of
        Nothing -> error $ columnNotFound filterColumnName "filter" (columnNames df)
        Just (BoxedColumn (column :: Vector c)) -> case (typeRep @a) `testEquality` (typeRep @c) of
            Nothing -> error $ addCallPointInfo filterColumnName (Just "filter") (typeMismatchError (typeRep @a) (typeRep @c))
            Just Refl -> let
                    indexes = V.ifoldl' (\s i v -> if condition v then S.insert i s else s) S.empty column
                    in df { columns = MS.map (pick indexes) (columns df) }
        Just (UnboxedColumn (column :: VU.Vector c)) -> case (typeRep @a) `testEquality` (typeRep @c) of
            Nothing -> error $ addCallPointInfo filterColumnName (Just "filter") (typeMismatchError (typeRep @a) (typeRep @c))
            Just Refl -> let
                    indexes = VU.ifoldl' (\s i v -> if condition v then S.insert i s else s) S.empty column
                in df { columns = MS.map (pick indexes) (columns df) }

-- | Sort order taken as a parameter by the sortby function.
data SortOrder = Ascending | Descending deriving (Eq)

-- | O(k ^ 2) Sorts the dataframe by a given row. Currently uses insertion sort
-- under the hood so can be very inefficient for large data.
--
-- > sortBy "Age" df 
sortBy :: T.Text
       -> SortOrder
       -> DataFrame
       -> DataFrame
sortBy sortColumnName order df = let
        pick indexes c@(BoxedColumn column) = BoxedColumn $ indexes `getIndices` column
        pick indexes c@(UnboxedColumn column) = UnboxedColumn $ indexes `getIndicesUnboxed` column
        -- TODO: This is a REALLY inefficient sorting algorithm (insertion sort).
        -- Complains about escaping context when you try and use sort by.
        insertSorted _ t [] = [t]
        insertSorted Ascending t@(a, b) lst@(x:xs) = if b < snd x then t:lst else insertSorted Ascending t xs
        insertSorted Descending t@(a, b) lst@(x:xs) = if b > snd x then t:lst else insertSorted Descending t xs
    in case sortColumnName `M.lookup` columns df of
        Nothing -> error $ columnNotFound sortColumnName "valueCounts" (columnNames df)
        Just (BoxedColumn (column :: V.Vector c)) -> let
                indexes = map fst $ V.ifoldr (\i e acc -> insertSorted order (i, e) acc) [] column
            in df { columns = MS.map (pick indexes) (columns df) }
        Just (UnboxedColumn (column :: VU.Vector c)) -> let
                indexes = map fst $ VU.ifoldr (\i e acc -> insertSorted order (i, e) acc) [] column
            in df { columns = MS.map (pick indexes) (columns df) }

-- | O(log n) Get the number of elements in a given column.
columnSize :: T.Text -> DataFrame -> Int
columnSize name df = case name `MS.lookup` columns df of
                        Nothing -> error $ columnNotFound name "apply" (columnNames df)
                        Just (BoxedColumn column')  -> V.length column'
                        Just (UnboxedColumn column')  -> VU.length column'

-- | O (k log n) Counts the occurences of each value in a given column.
valueCounts :: forall a . (Typeable a, Show a, Ord a) => T.Text -> DataFrame -> [(a, Integer)]
valueCounts columnName df = let
        repa :: Type.Reflection.TypeRep a = Type.Reflection.typeRep @a
    in case columnName `MS.lookup` columns df of
        Nothing -> error $ columnNotFound columnName "valueCounts" (columnNames df)
        Just (BoxedColumn (column' :: V.Vector c)) -> let
                repc :: Type.Reflection.TypeRep c = Type.Reflection.typeRep @c
                column = L.sortBy (compare `on` snd) $ V.toList $ V.map (\v -> (v, show v)) column'
            in case repa `testEquality` repc of
                Nothing -> error $ addCallPointInfo columnName (Just "apply") (typeMismatchError (typeRep @a) (typeRep @c))
                Just Refl -> map (\xs -> (fst (head xs), fromIntegral $ length xs)) (L.groupBy ((==) `on` snd) column)
        Just (UnboxedColumn (column' :: VU.Vector c)) -> let
                repc :: Type.Reflection.TypeRep c = Type.Reflection.typeRep @c
                column = L.sortBy (compare `on` snd) $ V.toList $ V.map (\v -> (v, show v)) (VU.convert column')
            in case repa `testEquality` repc of
                Nothing -> error $ addCallPointInfo columnName (Just "apply") (typeMismatchError (typeRep @a) (typeRep @c))
                Just Refl -> map (\xs -> (fst (head xs), fromIntegral $ length xs)) (L.groupBy ((==) `on` snd) column)

-- | O(n) Selects a number of columns in a given dataframe.
--
-- > select ["name", "age"] df
select :: [T.Text]
       -> DataFrame
       -> DataFrame
select cs df
    | not $ any (`elem` columnNames df) cs = error $ columnNotFound (T.pack $ show $ cs L.\\ columnNames df) "select" (columnNames df)
    | otherwise = L.foldl' addKeyValue DI.empty cs
            where addKeyValue d k = d { columns = MS.insert k (columns df MS.! k) (columns d),
                                        _columnNames = _columnNames d ++ [k] }

-- | O(n) inverse of select
--
-- > drop ["Name"] df
drop :: [T.Text]
        -> DataFrame
        -> DataFrame
drop cs df = let
        keysToKeep = columnNames df L.\\ cs
    in select keysToKeep df


-- | O(k * n) groups the dataframe by the given rows aggregating the remaining rows
-- into vector that should be reduced later.
groupBy :: [T.Text]
        -> DataFrame
        -> DataFrame
groupBy names df
    | not $ any (`elem` columnNames df) names = error $ columnNotFound (T.pack $ show $ names L.\\ columnNames df) "groupBy" (columnNames df)
    | otherwise = L.foldl' addColumns initDf groupingColumns
            where -- Create a string representation of each row.
                  values = V.map (mkRowRep df (S.fromList names)) (V.generate (fst (dimensions df) - 1) id)
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
mkRowRep df names i = MS.foldlWithKey go "" (columns df)
    where go acc k (BoxedColumn c) =
            if S.notMember k names
            then acc
            else case c V.!? i of
                Just e -> acc ++ show e
                Nothing -> error $ "Column " ++ T.unpack k ++
                                    " has less items than " ++
                                    "the other columns."
          go acc k (UnboxedColumn c) =
            if S.notMember k names
            then acc
            else case c VU.!? i of
                Just e -> acc ++ show e
                Nothing -> error $ "Column " ++ T.unpack k ++
                                    " has less items than " ++
                                    "the other columns."

mkGroupedColumns :: [Int] -> DataFrame -> DataFrame -> T.Text -> DataFrame
mkGroupedColumns indices df acc name
    = case (MS.!) (columns df) name of
        (BoxedColumn column) ->
            let
                vs = indices `getIndices` column
            in addColumn name vs acc
        (UnboxedColumn column) ->
            let
                vs = indices `getIndicesUnboxed` column
            in addUnboxedColumn name vs acc

groupColumns :: [[Int]] -> DataFrame -> DataFrame -> T.Text -> DataFrame
groupColumns indices df acc name
    = case (MS.!) (columns df) name of
        (BoxedColumn column) ->
            let
                vs = V.fromList $ map (`getIndices` column) indices
            in addColumn name vs acc
        (UnboxedColumn column) ->
            let
                vs = V.fromList $ map (`getIndicesUnboxed` column) indices
            in addColumn name vs acc

-- O (k * n) Reduces a vector valued volumn with a given function.
reduceBy :: (Typeable a, Show a, Ord a, Typeable b, Show b, Ord b, Typeable (v a), Show (v a), Ord (v a), VG.Vector v a)
         => T.Text
         -> (v a -> b)
         -> DataFrame
         -> DataFrame
reduceBy = apply

-- O (k) combines two columns into a single column using a given function similar to zipWith.
--
-- > combine "total_price" (*) "unit_price" "quantity" df
combine :: forall a b c . (Typeable a, Show a, Ord a, Typeable b, Show b, Ord b, Typeable c, Show c, Ord c)
        => T.Text         -- ^ the name of the column where the result will be materialized.
        -> (a -> b -> c)  -- ^ the function to zip the rows with.
        -> T.Text         -- ^ left-hand-side column
        -> T.Text         -- ^ right hand side column
        -> DataFrame      -- ^ dataframe to apply the combination too.
        -> DataFrame
combine targetColumn func firstColumn secondColumn df =
    if all isJust [M.lookup firstColumn (columns df), M.lookup secondColumn (columns df)]
    then case ((M.!) (columns df) firstColumn, (M.!) (columns df) secondColumn) of
        (BoxedColumn (f :: Vector d), BoxedColumn (g :: Vector e)) ->
            case testEquality (typeRep @a) (typeRep @d) of
                Nothing -> error $ addCallPointInfo firstColumn (Just "combine") (typeMismatchError (typeRep @a) (typeRep @d))
                Just Refl -> case testEquality (typeRep @b) (typeRep @e) of
                    Nothing -> error $ addCallPointInfo secondColumn (Just "combine") (typeMismatchError (typeRep @b) (typeRep @e))
                    Just Refl -> addColumn targetColumn (V.zipWith func f g) df
        (UnboxedColumn (f :: VU.Vector d), UnboxedColumn (g :: VU.Vector e)) ->
            case testEquality (typeRep @a) (typeRep @d) of
                Nothing -> error $ addCallPointInfo firstColumn (Just "combine") (typeMismatchError (typeRep @a) (typeRep @d))
                Just Refl -> case testEquality (typeRep @b) (typeRep @e) of
                    Nothing -> error $ addCallPointInfo secondColumn (Just "combine") (typeMismatchError (typeRep @b) (typeRep @e))
                    Just Refl -> case testEquality (typeRep @c) (typeRep @Int) of
                        Just Refl -> addUnboxedColumn targetColumn (VU.zipWith func f g) df
                        Nothing -> case testEquality (typeRep @c) (typeRep @Double) of
                            Just Refl -> addUnboxedColumn targetColumn (VU.zipWith func f g) df
                            Nothing -> addColumn targetColumn (V.zipWith func (V.convert f) (V.convert g)) df
        (UnboxedColumn (f :: VU.Vector d), BoxedColumn (g :: Vector e)) ->
            case testEquality (typeRep @a) (typeRep @d) of
                Nothing -> error $ addCallPointInfo firstColumn (Just "combine") (typeMismatchError (typeRep @a) (typeRep @d))
                Just Refl -> case testEquality (typeRep @b) (typeRep @e) of
                    Nothing -> error $ addCallPointInfo secondColumn (Just "combine") (typeMismatchError (typeRep @b) (typeRep @e))
                    Just Refl -> case testEquality (typeRep @c) (typeRep @Int) of
                        Just Refl -> addUnboxedColumn targetColumn (VU.convert $ V.zipWith func (V.convert f) g) df
                        Nothing -> case testEquality (typeRep @c) (typeRep @Double) of
                            Just Refl -> addUnboxedColumn targetColumn (VU.convert $ V.zipWith func (V.convert f) g) df
                            Nothing -> addColumn targetColumn (V.zipWith func (V.convert f) g) df
        (BoxedColumn (f :: V.Vector d), UnboxedColumn (g :: VU.Vector e)) -> 
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
parseDefaults safeRead df = df { columns = MS.map (parseDefault safeRead) (columns df) }

parseDefault :: Bool -> Column -> Column
parseDefault safeRead (BoxedColumn (c :: V.Vector a)) = let
        repa :: Type.Reflection.TypeRep a = Type.Reflection.typeRep @a
        repText :: Type.Reflection.TypeRep T.Text = Type.Reflection.typeRep @T.Text
    in case repa `testEquality` repText of
        Nothing -> BoxedColumn c
        Just Refl -> let
                example = T.strip (V.head c)
                nullish = S.fromList ["nan", "NULL", "null", "", " "]
                emptyToNothing v = if S.member v nullish then Nothing else Just v
            in case readInt example of
                Just _ -> let
                        safeVector = V.map ((=<<) readInt . emptyToNothing) c
                        hasNulls = V.foldl' (\acc v -> if isNothing v then acc || True else acc) False safeVector
                    in (if safeRead && hasNulls then BoxedColumn safeVector else UnboxedColumn (VU.convert $ V.map (fromMaybe 0 . readInt) c))
                Nothing -> case readDouble example of
                    Just _ -> let
                            safeVector = V.map ((=<<) readDouble . emptyToNothing) c
                            hasNulls = V.foldl' (\acc v -> if isNothing v then acc || True else acc) False safeVector
                        in if safeRead && hasNulls then BoxedColumn safeVector else UnboxedColumn (VU.convert $ V.map (fromMaybe 0 . readDouble) c)
                    Nothing -> let
                                safeVector = V.map emptyToNothing c
                                hasNulls = V.foldl' (\acc v -> if isNothing v then acc || True else acc) False safeVector
                            in if safeRead && hasNulls then BoxedColumn safeVector else BoxedColumn c

-- | O(n) Returns the number of non-null columns in the dataframe and the type associated
-- with each column.
columnInfo :: DataFrame -> [(String, Int, String)]
columnInfo df = L.sortBy (compare `on` snd') (MS.foldlWithKey go [] (columns df))
    where go acc k (BoxedColumn (c :: Vector a)) = (T.unpack k, V.length $ V.filter (flip S.member nullish . show) c, show $ typeRep @a) : acc
          go acc k (UnboxedColumn (c :: VU.Vector a)) = (T.unpack k, V.length $ V.filter (flip S.member nullish . show) (V.convert c), show $ typeRep @a) : acc
          nullish = S.fromList ["Nothing", "NULL", "",  " ", "nan"]
          snd' (_, x, _) = x 
