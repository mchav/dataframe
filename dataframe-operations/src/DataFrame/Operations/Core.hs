{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Operations.Core (
    -- * Dimensions
    dimensions,
    nRows,
    nColumns,

    -- * Construction
    fromUnnamedColumns,
    fromRows,

    -- * Insertion
    insert,
    insertVector,
    insertUnboxedVector,
    insertWithDefault,
    insertVectorWithDefault,

    -- * Column management
    cloneColumn,
    rename,
    renameMany,

    -- * Inspection
    describeColumns,
    valueCounts,
    valueProportions,
    showDerivedExpressions,

    -- * Folds & matrix/vector conversions
    fold,
    toFloatMatrix,
    toDoubleMatrix,
    toIntMatrix,
    columnAsVector,
    columnAsList,
    columnAsIntVector,
    columnAsDoubleVector,
    columnAsFloatVector,
    columnAsUnboxedVector,
) where

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Map.Strict as MS
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU

import Control.Exception (throw)
import Data.Bits (popCount)
import Data.Either
import qualified Data.Foldable as Fold
import Data.Function (on, (&))
import Data.Maybe
import Data.Type.Equality (TestEquality (..))
import DataFrame.Errors
import DataFrame.Internal.Column (
    Column (..),
    Columnable,
    TypedColumn (..),
    columnLength,
    columnTypeString,
    fromList,
    fromVector,
    materializeMerged,
    materializePacked,
    mergedHead,
    toDoubleVector,
    toFloatVector,
    toIntVector,
    toUnboxedVector,
    toVector,
 )
import DataFrame.Internal.DataFrame (
    DataFrame (..),
    columnIndices,
    columnNames,
    derivingExpressions,
    empty,
    fromColumns,
    getColumn,
    insertColumn,
    null,
 )
import DataFrame.Internal.Expression
import DataFrame.Internal.Interpreter
import DataFrame.Internal.Parsing (isNullish)
import DataFrame.Internal.Row (Any, mkColumnFromRow)
import Type.Reflection
import Prelude hiding (null)

-- | O(1) Get DataFrame dimensions, i.e. (rows, columns).
dimensions :: DataFrame -> (Int, Int)
dimensions = dataframeDimensions
{-# INLINE dimensions #-}

-- | O(1) Get the number of rows in a dataframe.
nRows :: DataFrame -> Int
nRows = fst . dataframeDimensions

-- | O(1) Get the number of columns in a dataframe.
nColumns :: DataFrame -> Int
nColumns = snd . dataframeDimensions

-- 'columnNames' lives in "DataFrame.Internal.DataFrame" (re-exported).

-- | Add a vector as a named column; size mismatches pad with Nothing.
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

-- | Add a foldable as a named column; do not pass infinite collections.
insert ::
    forall a t.
    (Columnable a, Foldable t) =>
    -- | Column Name
    T.Text ->
    -- | Sequence to add to dataframe
    t a ->
    -- | DataFrame to add column to
    DataFrame ->
    DataFrame
insert name xs = insertColumn name (fromList (Fold.foldr' (:) [] xs))
{-# INLINE insert #-}

-- | Add a vector, padding with a default if it has fewer elements than rows.
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

-- | Add a list, padding with a default if it has fewer elements than rows.
insertWithDefault ::
    forall a t.
    (Columnable a, Foldable t) =>
    -- | Default Value
    a ->
    -- | Column name
    T.Text ->
    -- | Data to add to column
    t a ->
    -- | DataFrame to add the column to
    DataFrame ->
    DataFrame
insertWithDefault defaultValue name xs d =
    let (rows, _) = dataframeDimensions d
        xs' = Fold.foldr' (:) [] xs
        values = xs' ++ replicate (rows - length xs') defaultValue
     in insertColumn name (fromList values) d

-- | /O(n)/ Like 'insertVector' but takes an already-unboxed vector.
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
insertUnboxedVector name xs = insertColumn name (UnboxedColumn Nothing xs)

-- 'insertColumn' is now defined in "DataFrame.Internal.DataFrame".

-- | /O(n)/ Clone a column and place it under a new name.
cloneColumn :: T.Text -> T.Text -> DataFrame -> DataFrame
cloneColumn original new df
    | null df = throw (EmptyDataSetException "cloneColumn")
    | otherwise = fromMaybe
        ( throw $
            ColumnsNotFoundException [original] "cloneColumn" (M.keys $ columnIndices df)
        )
        $ do
            column <- getColumn original df
            return $ insertColumn new column df

-- | /O(n)/ Rename a single column.
rename :: T.Text -> T.Text -> DataFrame -> DataFrame
rename orig new df = either throw id (renameSafe orig new df)

-- | /O(n)/ Rename many columns.
renameMany :: [(T.Text, T.Text)] -> DataFrame -> DataFrame
renameMany = fold (uncurry rename)

renameSafe ::
    T.Text -> T.Text -> DataFrame -> Either DataFrameException DataFrame
renameSafe orig new df
    | null df = throw (EmptyDataSetException "rename")
    | otherwise = fromMaybe
        (Left $ ColumnsNotFoundException [orig] "rename" (M.keys $ columnIndices df))
        $ do
            columnIndex <- M.lookup orig (columnIndices df)
            let origRemoved = M.delete orig (columnIndices df)
            let newAdded = M.insert new columnIndex origRemoved
            return (Right df{columnIndices = newAdded})

data ColumnInfo = ColumnInfo
    { nameOfColumn :: !T.Text
    , nonNullValues :: !Int
    , nullValues :: !Int
    , typeOfColumn :: !T.Text
    }

-- | O(n * k ^ 2) Count non-null values and the type of each column.
describeColumns :: DataFrame -> DataFrame
describeColumns df =
    empty
        & insertColumn "Column Name" (fromList (map nameOfColumn infos))
        & insertColumn "# Non-null Values" (fromList (map nonNullValues infos))
        & insertColumn "# Null Values" (fromList (map nullValues infos))
        & insertColumn "Type" (fromList (map typeOfColumn infos))
  where
    infos =
        L.sortBy (compare `on` nonNullValues) (V.ifoldl' go [] (columns df)) ::
            [ColumnInfo]
    indexMap = M.fromList (map (\(a, b) -> (b, a)) $ M.toList (columnIndices df))
    columnName i = M.lookup i indexMap
    go acc i col@(BoxedColumn _bm (_c :: V.Vector a)) =
        let
            cname = columnName i
            countNulls = nulls col
            columnType = T.pack $ columnTypeString col
         in
            if isNothing cname
                then acc
                else
                    ColumnInfo
                        (fromMaybe "" cname)
                        (columnLength col - countNulls)
                        countNulls
                        columnType
                        : acc
    go acc i col@(UnboxedColumn _bm _c) =
        let
            cname = columnName i
            countNulls = nulls col
            columnType = T.pack $ columnTypeString col
         in
            if isNothing cname
                then acc
                else
                    ColumnInfo
                        (fromMaybe "" cname)
                        (columnLength col - countNulls)
                        countNulls
                        columnType
                        : acc
    go acc i col@(PackedText _ _) = go acc i (materializePacked col)
    go acc i col@(MergedColumn _ _) = go acc i (materializeMerged col)

nulls :: Column -> Int
nulls (BoxedColumn (Just bm) xs) =
    let n = VG.length xs
     in n - VU.foldl' (\acc b -> acc + popCount b) 0 bm
nulls (BoxedColumn Nothing (xs :: V.Vector a)) = case testEquality (typeRep @a) (typeRep @T.Text) of
    Just Refl -> VG.length $ VG.filter isNullish xs
    Nothing -> case testEquality (typeRep @a) (typeRep @String) of
        Just Refl -> VG.length $ VG.filter (isNullish . T.pack) xs
        Nothing -> 0
nulls (UnboxedColumn (Just bm) xs) =
    let n = VG.length xs
     in n - VU.foldl' (\acc b -> acc + popCount b) 0 bm
nulls c@(PackedText _ _) = nulls (materializePacked c)
nulls _ = 0

-- 'fromColumns' is now defined in "DataFrame.Internal.DataFrame".

-- | Create a dataframe from a list of columns named "0", "1", and so on.
fromUnnamedColumns :: [Column] -> DataFrame
fromUnnamedColumns = fromColumns . zip (map (T.pack . show) [(0 :: Int) ..])

-- | Create a dataframe from a list of column names and rows.
fromRows :: [T.Text] -> [[Any]] -> DataFrame
fromRows names rows =
    L.foldl'
        (\df (i, name) -> insertColumn name (mkColumnFromRow name i rows) df)
        empty
        (zip [0 ..] names)

-- | O (k * n) Count the occurrences of each value in a given column.
valueCounts ::
    forall a. (Ord a, Columnable a) => Expr a -> DataFrame -> [(a, Int)]
valueCounts expr df
    | null df = throw (EmptyDataSetException "valueCounts")
    | otherwise = case columnAsVector expr df of
        Left e -> throw e
        Right column' ->
            let
                column = V.foldl' (\m v -> MS.insertWith (+) v (1 :: Int) m) M.empty column'
             in
                M.toAscList column

-- | O (k * n) Show the proportion of each value in a given column.
valueProportions ::
    forall a. (Ord a, Columnable a) => Expr a -> DataFrame -> [(a, Double)]
valueProportions expr df
    | null df = throw (EmptyDataSetException "valueCounts")
    | otherwise = case columnAsVector expr df of
        Left e -> throw e
        Right column' ->
            let
                counts =
                    M.toAscList
                        (V.foldl' (\m v -> MS.insertWith (+) v (1 :: Int) m) M.empty column')
                total = fromIntegral (sum (map snd counts))
             in
                map (fmap ((/ total) . fromIntegral)) counts

-- | A left fold for dataframes that takes the dataframe as the last argument.
fold :: (a -> DataFrame -> DataFrame) -> [a] -> DataFrame -> DataFrame
fold f xs acc = L.foldl' (flip f) acc xs

-- | The dataframe as a row-major float matrix, 'Left' if conversion fails.
toFloatMatrix ::
    DataFrame -> Either DataFrameException (V.Vector (VU.Vector Float))
toFloatMatrix df = case V.foldl'
    (\acc c -> V.snoc <$> acc <*> toFloatVector c)
    (Right V.empty :: Either DataFrameException (V.Vector (VU.Vector Float)))
    (columns df) of
    Left e -> Left e
    Right m ->
        pure $
            V.generate
                (fst (dataframeDimensions df))
                (\i -> VU.generate (V.length m) (\j -> (m VG.! j) VG.! i))

-- | The dataframe as a row-major double matrix, 'Left' if conversion fails.
toDoubleMatrix ::
    DataFrame -> Either DataFrameException (V.Vector (VU.Vector Double))
toDoubleMatrix df = case V.foldl'
    (\acc c -> V.snoc <$> acc <*> toDoubleVector c)
    (Right V.empty :: Either DataFrameException (V.Vector (VU.Vector Double)))
    (columns df) of
    Left e -> Left e
    Right m ->
        pure $
            V.generate
                (fst (dataframeDimensions df))
                (\i -> VU.generate (V.length m) (\j -> (m VG.! j) VG.! i))

-- | The dataframe as a row-major int matrix, 'Left' if conversion fails.
toIntMatrix :: DataFrame -> Either DataFrameException (V.Vector (VU.Vector Int))
toIntMatrix df = case V.foldl'
    (\acc c -> V.snoc <$> acc <*> toIntVector c)
    (Right V.empty :: Either DataFrameException (V.Vector (VU.Vector Int)))
    (columns df) of
    Left e -> Left e
    Right m ->
        pure $
            V.generate
                (fst (dataframeDimensions df))
                (\i -> VU.generate (V.length m) (\j -> (m VG.! j) VG.! i))

withColumnName ::
    T.Text -> Either DataFrameException a -> Either DataFrameException a
withColumnName name (Left (TypeMismatchException ctx)) =
    Left
        (TypeMismatchException ctx{errorColumnName = orFallback (errorColumnName ctx)})
  where
    orFallback (Just n) = Just n
    orFallback Nothing = Just (T.unpack name)
withColumnName _ result = result

-- | Get a specific column as a vector; the type comes from type application.
columnAsVector ::
    forall a.
    (Columnable a) => Expr a -> DataFrame -> Either DataFrameException (V.Vector a)
columnAsVector expr df
    | null df = throw (EmptyDataSetException "columnAsVector")
    | otherwise = case expr of
        (Col name) -> case getColumn name df of
            Just col -> withColumnName name (toVector col)
            Nothing ->
                Left $
                    ColumnsNotFoundException [name] "columnAsVector" (M.keys $ columnIndices df)
        _ -> case interpret df expr of
            Left e -> throw e
            Right (TColumn col) -> toVector col

-- | A column as an unboxed vector of 'Int', 'Left' if not convertible.
columnAsIntVector ::
    (Columnable a, Num a) =>
    Expr a -> DataFrame -> Either DataFrameException (VU.Vector Int)
columnAsIntVector (Col name) df = case getColumn name df of
    Just col -> withColumnName name (toIntVector col)
    Nothing ->
        Left $
            ColumnsNotFoundException [name] "columnAsIntVector" (M.keys $ columnIndices df)
columnAsIntVector expr df = case interpret df expr of
    Left e -> throw e
    Right (TColumn col) -> toIntVector col

-- | A column as an unboxed vector of 'Double', 'Left' if not convertible.
columnAsDoubleVector ::
    (Columnable a, Num a) =>
    Expr a -> DataFrame -> Either DataFrameException (VU.Vector Double)
columnAsDoubleVector (Col name) df = case getColumn name df of
    Just col -> withColumnName name (toDoubleVector col)
    Nothing ->
        Left $
            ColumnsNotFoundException
                [name]
                "columnAsDoubleVector"
                (M.keys $ columnIndices df)
columnAsDoubleVector expr df = case interpret df expr of
    Left e -> throw e
    Right (TColumn col) -> toDoubleVector col

-- | A column as an unboxed vector of 'Float', 'Left' if not convertible.
columnAsFloatVector ::
    (Columnable a, Num a) =>
    Expr a -> DataFrame -> Either DataFrameException (VU.Vector Float)
columnAsFloatVector (Col name) df = case getColumn name df of
    Just col -> withColumnName name (toFloatVector col)
    Nothing ->
        Left $
            ColumnsNotFoundException
                [name]
                "columnAsFloatVector"
                (M.keys $ columnIndices df)
columnAsFloatVector expr df = case interpret df expr of
    Left e -> throw e
    Right (TColumn col) -> toFloatVector col

columnAsUnboxedVector ::
    forall a.
    (Columnable a, VU.Unbox a) =>
    Expr a -> DataFrame -> Either DataFrameException (VU.Vector a)
columnAsUnboxedVector (Col name) df = case getColumn name df of
    Just col -> withColumnName name (toUnboxedVector col)
    Nothing ->
        Left $
            ColumnsNotFoundException
                [name]
                "columnAsFloatVector"
                (M.keys $ columnIndices df)
columnAsUnboxedVector expr df = case interpret df expr of
    Left e -> throw e
    Right (TColumn col) -> toUnboxedVector col
{-# SPECIALIZE columnAsUnboxedVector ::
    Expr Double -> DataFrame -> Either DataFrameException (VU.Vector Double)
    #-}
{-# INLINE columnAsUnboxedVector #-}

-- | Get a specific column as a list; the type comes from type application.
columnAsList :: forall a. (Columnable a) => Expr a -> DataFrame -> [a]
columnAsList expr df = either throw V.toList (columnAsVector expr df)

-- TODO: mchavinda - Expand out these expressions if possible.
showDerivedExpressions :: DataFrame -> [NamedExpr]
showDerivedExpressions df =
    let exprs = derivingExpressions df
        names = columnNames df
        toNamedExpr name = case M.lookup name exprs of
            Just uexpr -> (name, uexpr)
            Nothing -> (name, identityExpr name)
     in map toNamedExpr names
  where
    identityExpr name = case getColumn name df of
        Just (BoxedColumn (Just _) (_ :: V.Vector a)) -> SomeExpr (Col @(Maybe a) name)
        Just (BoxedColumn Nothing (_ :: V.Vector a)) -> SomeExpr (Col @a name)
        Just (UnboxedColumn (Just _) (_ :: VU.Vector a)) -> SomeExpr (Col @(Maybe a) name)
        Just (UnboxedColumn Nothing (_ :: VU.Vector a)) -> SomeExpr (Col @a name)
        Just (PackedText (Just _) _) -> SomeExpr (Col @(Maybe T.Text) name)
        Just (PackedText Nothing _) -> SomeExpr (Col @T.Text name)
        Just c@(MergedColumn _ _) -> case mergedHead c of
            BoxedColumn (Just _) (_ :: V.Vector a) -> SomeExpr (Col @(Maybe a) name)
            BoxedColumn Nothing (_ :: V.Vector a) -> SomeExpr (Col @a name)
            _ ->
                error $
                    "showDerivedExpressions: merged column did not materialize boxed: "
                        ++ T.unpack name
        Nothing -> error $ "showDerivedExpressions: column not found: " ++ T.unpack name
