{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Operations.Core where

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Map.Strict as MS
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU

import Control.Exception (throw)
import Data.Either
import Data.Function (on, (&))
import Data.Maybe
import Data.Type.Equality (TestEquality (..))
import DataFrame.Errors
import DataFrame.Internal.Column (
    Column (..),
    Columnable,
    columnLength,
    columnTypeString,
    expandColumn,
    fromList,
    fromVector,
 )
import DataFrame.Internal.DataFrame (DataFrame (..), empty, getColumn)
import DataFrame.Internal.Parsing (isNullish)
import DataFrame.Internal.Row (Any, mkColumnFromRow)
import Type.Reflection
import Prelude hiding (null)

{- | O(1) Get DataFrame dimensions i.e. (rows, columns)

==== __Example__
@
ghci> D.dimensions df

(100, 3)
@
-}
dimensions :: DataFrame -> (Int, Int)
dimensions = dataframeDimensions
{-# INLINE dimensions #-}

{- | O(k) Get column names of the DataFrame in order of insertion.

==== __Example__
@
ghci> D.columnNames df

["col_a", "col_b", "col_c"]
@
-}
columnNames :: DataFrame -> [T.Text]
columnNames = map fst . L.sortBy (compare `on` snd) . M.toList . columnIndices
{-# INLINE columnNames #-}

{- | Adds a vector to the dataframe. If the vector has less elements than the dataframe and the dataframe is not empty
the vector is converted to type `Maybe a` filled with `Nothing` to match the size of the dataframe. Similarly,
if the vector has more elements than what's currently in the dataframe, the other columns in the dataframe are
change to `Maybe <Type>` and filled with `Nothing`.

==== __Example__
@
ghci> import qualified Data.Vector as V

ghci> D.insertVector "numbers" (V.fromList [1..10]) D.empty

---------------
index | numbers
------|--------
 Int  |   Int
------|--------
0     | 1
1     | 2
2     | 3
3     | 4
4     | 5
5     | 6
6     | 7
7     | 8
8     | 9
9     | 10

@
-}
insertVector ::
    forall a.
    (Columnable a) =>
    -- | Column Name
    T.Text ->
    -- | Vector to add to column
    V.Vector a ->
    -- | DataFrame to add column to
    DataFrame ->
    DataFrame
insertVector name xs = insertColumn name (fromVector xs)
{-# INLINE insertVector #-}

{- | /O(k)/ Add a column to the dataframe providing a default.
This constructs a new vector and also may convert it
to an unboxed vector if necessary. Since columns are usually
large the runtime is dominated by the length of the list, k.
-}
insertVectorWithDefault ::
    forall a.
    (Columnable a) =>
    -- | Default Value
    a ->
    -- | Column name
    T.Text ->
    -- | Data to add to column
    V.Vector a ->
    -- | DataFrame to add the column to
    DataFrame ->
    DataFrame
insertVectorWithDefault defaultValue name xs d =
    let (rows, _) = dataframeDimensions d
        values = xs V.++ V.replicate (rows - V.length xs) defaultValue
     in insertColumn name (fromVector values) d

{- | /O(n)/ Adds an unboxed vector to the dataframe.

Same as insertVector but takes an unboxed vector. If you insert a vector of numbers through insertVector it will either way be converted
into an unboxed vector so this function saves that extra work/conversion.
-}
insertUnboxedVector ::
    forall a.
    (Columnable a, VU.Unbox a) =>
    -- | Column Name
    T.Text ->
    -- | Unboxed vector to add to column
    VU.Vector a ->
    -- | DataFrame to add the column to
    DataFrame ->
    DataFrame
insertUnboxedVector name xs = insertColumn name (UnboxedColumn xs)

{- | /O(n)/ Add a column to the dataframe.

==== __Example__
@
ghci> D.insertColumn "numbers" (D.fromList [1..10]) D.empty

---------------
index | numbers
------|--------
 Int  |   Int
------|--------
0     | 1
1     | 2
2     | 3
3     | 4
4     | 5
5     | 6
6     | 7
7     | 8
8     | 9
9     | 10

@
-}
insertColumn ::
    -- | Column Name
    T.Text ->
    -- | Column to add
    Column ->
    -- | DataFrame to add the column to
    DataFrame ->
    DataFrame
insertColumn name column d =
    let
        (r, c) = dataframeDimensions d
        n = max (columnLength column) r
     in
        case M.lookup name (columnIndices d) of
            Just i ->
                DataFrame
                    (V.map (expandColumn n) (columns d V.// [(i, column)]))
                    (columnIndices d)
                    (n, c)
            Nothing ->
                DataFrame
                    (V.map (expandColumn n) (columns d `V.snoc` column))
                    (M.insert name c (columnIndices d))
                    (n, c + 1)

{- | /O(n)/ Clones a column and places it under a new name in the dataframe.

==== __Example__
@
ghci> import qualified Data.Vector as V

ghci> df = insertVector "numbers" (V.fromList [1..10]) D.empty

ghci> D.cloneColumn "numbers" "others" df

------------------------
index | numbers | others
------|---------|-------
 Int  |   Int   |  Int
------|---------|-------
0     | 1       | 1
1     | 2       | 2
2     | 3       | 3
3     | 4       | 4
4     | 5       | 5
5     | 6       | 6
6     | 7       | 7
7     | 8       | 8
8     | 9       | 9
9     | 10      | 10

@
-}
cloneColumn :: T.Text -> T.Text -> DataFrame -> DataFrame
cloneColumn original new df = fromMaybe
    ( throw $
        ColumnNotFoundException original "cloneColumn" (M.keys $ columnIndices df)
    )
    $ do
        column <- getColumn original df
        return $ insertColumn new column df

{- | /O(n)/ Renames a single column.

==== __Example__
@
ghci> import qualified Data.Vector as V

ghci> df = insertVector "numbers" (V.fromList [1..10]) D.empty

ghci> D.rename "numbers" "others" df

--------------
index | others
------|-------
 Int  |  Int
------|-------
0     | 1
1     | 2
2     | 3
3     | 4
4     | 5
5     | 6
6     | 7
7     | 8
8     | 9
9     | 10

@
-}
rename :: T.Text -> T.Text -> DataFrame -> DataFrame
rename orig new df = either throw id (renameSafe orig new df)

{- | /O(n)/ Renames many columns.

==== __Example__
@
ghci> import qualified Data.Vector as V

ghci> df = D.insertVector "others" (V.fromList [11..20]) (D.insertVector "numbers" (V.fromList [1..10]) D.empty)

ghci> df

------------------------
index | numbers | others
------|---------|-------
 Int  |   Int   |  Int
------|---------|-------
0     | 1       | 11
1     | 2       | 12
2     | 3       | 13
3     | 4       | 14
4     | 5       | 15
5     | 6       | 16
6     | 7       | 17
7     | 8       | 18
8     | 9       | 19
9     | 10      | 20

ghci> D.renameMany [("numbers", "first_10"), ("others", "next_10")] df

--------------------------
index | first_10 | next_10
------|----------|--------
 Int  |   Int    |   Int
------|----------|--------
0     | 1        | 11
1     | 2        | 12
2     | 3        | 13
3     | 4        | 14
4     | 5        | 15
5     | 6        | 16
6     | 7        | 17
7     | 8        | 18
8     | 9        | 19
9     | 10       | 20

@
-}
renameMany :: [(T.Text, T.Text)] -> DataFrame -> DataFrame
renameMany = fold (uncurry rename)

renameSafe ::
    T.Text -> T.Text -> DataFrame -> Either DataFrameException DataFrame
renameSafe orig new df = fromMaybe
    (Left $ ColumnNotFoundException orig "rename" (M.keys $ columnIndices df))
    $ do
        columnIndex <- M.lookup orig (columnIndices df)
        let origRemoved = M.delete orig (columnIndices df)
        let newAdded = M.insert new columnIndex origRemoved
        return (Right df{columnIndices = newAdded})

data ColumnInfo = ColumnInfo
    { nameOfColumn :: !T.Text
    , nonNullValues :: !Int
    , nullValues :: !Int
    , partiallyParsedValues :: !Int
    , uniqueValues :: !Int
    , typeOfColumn :: !T.Text
    }

{- | O(n * k ^ 2) Returns the number of non-null columns in the dataframe and the type associated with each column.

==== __Example__
@
ghci> import qualified Data.Vector as V

ghci> df = D.insertVector "others" (V.fromList [11..20]) (D.insertVector "numbers" (V.fromList [1..10]) D.empty)

ghci> D.describeColumns df

-----------------------------------------------------------------------------------------------------
index | Column Name | # Non-null Values | # Null Values | # Partially parsed | # Unique Values | Type
------|-------------|-------------------|---------------|--------------------|-----------------|-----
 Int  |    Text     |        Int        |      Int      |        Int         |       Int       | Text
------|-------------|-------------------|---------------|--------------------|-----------------|-----
0     | others      | 10                | 0             | 0                  | 10              | Int
1     | numbers     | 10                | 0             | 0                  | 10              | Int

@
-}
describeColumns :: DataFrame -> DataFrame
describeColumns df =
    empty
        & insertColumn "Column Name" (fromList (map nameOfColumn infos))
        & insertColumn "# Non-null Values" (fromList (map nonNullValues infos))
        & insertColumn "# Null Values" (fromList (map nullValues infos))
        & insertColumn "# Partially parsed" (fromList (map partiallyParsedValues infos))
        & insertColumn "# Unique Values" (fromList (map uniqueValues infos))
        & insertColumn "Type" (fromList (map typeOfColumn infos))
  where
    infos =
        L.sortBy (compare `on` nonNullValues) (V.ifoldl' go [] (columns df)) ::
            [ColumnInfo]
    indexMap = M.fromList (map (\(a, b) -> (b, a)) $ M.toList (columnIndices df))
    columnName i = M.lookup i indexMap
    go acc i col@(OptionalColumn (c :: V.Vector a)) =
        let
            cname = columnName i
            countNulls = nulls col
            countPartial = partiallyParsed col
            columnType = T.pack $ show $ typeRep @a
            unique = S.size $ VG.foldr S.insert S.empty c
         in
            if isNothing cname
                then acc
                else
                    ColumnInfo
                        (fromMaybe "" cname)
                        (columnLength col - countNulls)
                        countNulls
                        countPartial
                        unique
                        columnType
                        : acc
    go acc i col@(BoxedColumn (c :: V.Vector a)) =
        let
            cname = columnName i
            countPartial = partiallyParsed col
            columnType = T.pack $ show $ typeRep @a
            unique = S.size $ VG.foldr S.insert S.empty c
         in
            if isNothing cname
                then acc
                else
                    ColumnInfo
                        (fromMaybe "" cname)
                        (columnLength col)
                        0
                        countPartial
                        unique
                        columnType
                        : acc
    go acc i col@(UnboxedColumn c) =
        let
            cname = columnName i
            columnType = T.pack $ columnTypeString col
            unique = S.size $ VG.foldr S.insert S.empty c
         in
            -- Unboxed columns cannot have nulls since Maybe
            -- is not an instance of Unbox a
            if isNothing cname
                then acc
                else
                    ColumnInfo (fromMaybe "" cname) (columnLength col) 0 0 unique columnType : acc

nulls :: Column -> Int
nulls (OptionalColumn xs) = VG.length $ VG.filter isNothing xs
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

{- | Creates a dataframe from a list of tuples with name and column.

==== __Example__
@
ghci> df = D.fromNamedColumns [("numbers", D.fromList [1..10]), ("others", D.fromList [11..20])]

ghci> df

------------------------
index | numbers | others
------|---------|-------
 Int  |   Int   |  Int
------|---------|-------
0     | 1       | 11
1     | 2       | 12
2     | 3       | 13
3     | 4       | 14
4     | 5       | 15
5     | 6       | 16
6     | 7       | 17
7     | 8       | 18
8     | 9       | 19
9     | 10      | 20

@
-}
fromNamedColumns :: [(T.Text, Column)] -> DataFrame
fromNamedColumns = L.foldl' (\df (name, column) -> insertColumn name column df) empty

{- | Create a dataframe from a list of columns. The column names are "0", "1"... etc.
Useful for quick exploration but you should probably always rename the columns after
or drop the ones you don't want.

==== __Example__
@
ghci> df = D.fromUnnamedColumns [D.fromList [1..10], D.fromList [11..20]]

ghci> df

-----------------
index |  0  |  1
------|-----|----
 Int  | Int | Int
------|-----|----
0     | 1   | 11
1     | 2   | 12
2     | 3   | 13
3     | 4   | 14
4     | 5   | 15
5     | 6   | 16
6     | 7   | 17
7     | 8   | 18
8     | 9   | 19
9     | 10  | 20

@
-}
fromUnnamedColumns :: [Column] -> DataFrame
fromUnnamedColumns = fromNamedColumns . zip (map (T.pack . show) [0 ..])

{- | Create a dataframe from a list of column names and rows.

==== __Example__
@
ghci> df = D.fromRows ["A", "B"] [[D.toAny 1, D.toAny 11], [D.toAny 2, D.toAny 12], [D.toAny 3, D.toAny 13]]

ghci> df

-----------------
index |  A  |  B
------|-----|----
 Int  | Int | Int
------|-----|----
0     | 1   | 11
1     | 2   | 12
2     | 3   | 13

@
-}
fromRows :: [T.Text] -> [[Any]] -> DataFrame
fromRows names rows =
    L.foldl'
        (\df i -> insertColumn (names !! i) (mkColumnFromRow i rows) df)
        empty
        [0 .. length names - 1]

{- | O (k * n) Counts the occurences of each value in a given column.

==== __Example__
@
ghci> df = D.fromUnnamedColumns [D.fromList [1..10], D.fromList [11..20]]

ghci> D.valueCounts @Int "0" df

[(1,1),(2,1),(3,1),(4,1),(5,1),(6,1),(7,1),(8,1),(9,1),(10,1)]

@
-}
valueCounts :: forall a. (Columnable a) => T.Text -> DataFrame -> [(a, Int)]
valueCounts columnName df = case getColumn columnName df of
    Nothing ->
        throw $
            ColumnNotFoundException columnName "valueCounts" (M.keys $ columnIndices df)
    Just (BoxedColumn (column' :: V.Vector c)) ->
        let
            column = V.foldl' (\m v -> MS.insertWith (+) v (1 :: Int) m) M.empty column'
         in
            case (typeRep @a) `testEquality` (typeRep @c) of
                Nothing ->
                    throw $
                        TypeMismatchException
                            ( MkTypeErrorContext
                                { userType = Right $ typeRep @a
                                , expectedType = Right $ typeRep @c
                                , errorColumnName = Just (T.unpack columnName)
                                , callingFunctionName = Just "valueCounts"
                                }
                            )
                Just Refl -> M.toAscList column
    Just (OptionalColumn (column' :: V.Vector c)) ->
        let
            column = V.foldl' (\m v -> MS.insertWith (+) v (1 :: Int) m) M.empty column'
         in
            case (typeRep @a) `testEquality` (typeRep @c) of
                Nothing ->
                    throw $
                        TypeMismatchException
                            ( MkTypeErrorContext
                                { userType = Right $ typeRep @a
                                , expectedType = Right $ typeRep @c
                                , errorColumnName = Just (T.unpack columnName)
                                , callingFunctionName = Just "valueCounts"
                                }
                            )
                Just Refl -> M.toAscList column
    Just (UnboxedColumn (column' :: VU.Vector c)) ->
        let
            column =
                V.foldl' (\m v -> MS.insertWith (+) v (1 :: Int) m) M.empty (V.convert column')
         in
            case (typeRep @a) `testEquality` (typeRep @c) of
                Nothing ->
                    throw $
                        TypeMismatchException
                            ( MkTypeErrorContext
                                { userType = Right $ typeRep @a
                                , expectedType = Right $ typeRep @c
                                , errorColumnName = Just (T.unpack columnName)
                                , callingFunctionName = Just "valueCounts"
                                }
                            )
                Just Refl -> M.toAscList column

{- | A left fold for dataframes that takes the dataframe as the last object.
This makes it easier to chain operations.

==== __Example__
@
ghci> D.fold (const id) [1..5] df

-----------------
index |  0  |  1
------|-----|----
 Int  | Int | Int
------|-----|----
0     | 1   | 11
1     | 2   | 12
2     | 3   | 13
3     | 4   | 14
4     | 5   | 15
5     | 6   | 16
6     | 7   | 17
7     | 8   | 18
8     | 9   | 19
9     | 10  | 20

@
-}
fold :: (a -> DataFrame -> DataFrame) -> [a] -> DataFrame -> DataFrame
fold f xs acc = L.foldl' (flip f) acc xs
