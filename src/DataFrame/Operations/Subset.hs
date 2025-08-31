{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Operations.Subset where

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import qualified Prelude

import Control.Exception (throw)
import Control.Monad.ST
import Data.Function ((&))
import Data.Maybe (fromJust, fromMaybe, isJust)
import Data.Type.Equality (TestEquality (..), type (:~:) (Refl))
import DataFrame.Errors (DataFrameException (..), TypeErrorContext (..))
import DataFrame.Internal.Column
import DataFrame.Internal.DataFrame (DataFrame (..), empty, getColumn)
import DataFrame.Internal.Expression
import DataFrame.Internal.Row (Any, mkRowFromArgs, toAny)
import DataFrame.Operations.Core
import DataFrame.Operations.Transformations (apply)
import Type.Reflection
import Prelude hiding (filter, take)

-- | O(k * n) Take the first n rows of a DataFrame.
take :: Int -> DataFrame -> DataFrame
take n d = d{columns = V.map (takeColumn n') (columns d), dataframeDimensions = (n', c)}
  where
    (r, c) = dataframeDimensions d
    n' = clip n 0 r

-- | O(k * n) Take the last n rows of a DataFrame.
takeLast :: Int -> DataFrame -> DataFrame
takeLast n d = d{columns = V.map (takeLastColumn n') (columns d), dataframeDimensions = (n', c)}
  where
    (r, c) = dataframeDimensions d
    n' = clip n 0 r

-- | O(k * n) Drop the first n rows of a DataFrame.
drop :: Int -> DataFrame -> DataFrame
drop n d = d{columns = V.map (sliceColumn n' (max (r - n') 0)) (columns d), dataframeDimensions = (max (r - n') 0, c)}
  where
    (r, c) = dataframeDimensions d
    n' = clip n 0 r

-- | O(k * n) Drop the last n rows of a DataFrame.
dropLast :: Int -> DataFrame -> DataFrame
dropLast n d = d{columns = V.map (sliceColumn 0 n') (columns d), dataframeDimensions = (n', c)}
  where
    (r, c) = dataframeDimensions d
    n' = clip (r - n) 0 r

-- | O(k * n) Take a range of rows of a DataFrame.
range :: (Int, Int) -> DataFrame -> DataFrame
range (start, end) d = d{columns = V.map (sliceColumn (clip start 0 r) n') (columns d), dataframeDimensions = (n', c)}
  where
    (r, c) = dataframeDimensions d
    n' = clip (end - start) 0 r

clip :: Int -> Int -> Int -> Int
clip n left right = min right $ max n left

{- | O(n * k) Filter rows by a given condition.

filter "x" even df
-}
filter ::
    forall a.
    (Columnable a) =>
    -- | Column to filter by
    T.Text ->
    -- | Filter condition
    (a -> Bool) ->
    -- | Dataframe to filter
    DataFrame ->
    DataFrame
filter filterColumnName condition df = case getColumn filterColumnName df of
    Nothing -> throw $ ColumnNotFoundException filterColumnName "filter" (map fst $ M.toList $ columnIndices df)
    Just (BoxedColumn (column :: V.Vector b)) -> filterByVector filterColumnName column condition df
    Just (OptionalColumn (column :: V.Vector b)) -> filterByVector filterColumnName column condition df
    Just (UnboxedColumn (column :: VU.Vector b)) -> filterByVector filterColumnName column condition df

filterByVector :: forall a b v. (VG.Vector v b, Columnable a, Columnable b) => T.Text -> v b -> (a -> Bool) -> DataFrame -> DataFrame
filterByVector filterColumnName column condition df = case testEquality (typeRep @a) (typeRep @b) of
    Nothing ->
        throw $
            TypeMismatchException
                ( MkTypeErrorContext
                    { userType = Right $ typeRep @a
                    , expectedType = Right $ typeRep @b
                    , errorColumnName = Just (T.unpack filterColumnName)
                    , callingFunctionName = Just "filter"
                    }
                )
    Just Refl ->
        let
            ixs = indexes condition column
         in
            df{columns = V.map (atIndicesStable ixs) (columns df), dataframeDimensions = (VG.length ixs, snd (dataframeDimensions df))}

indexes :: (VG.Vector v a) => (a -> Bool) -> v a -> VU.Vector Int
indexes condition cols = runST $ do
    ixs <- VUM.new 8192
    (!icount, _, _, !ixs') <-
        VG.foldM
            ( \(!icount, !vcount, !cap, mv) v -> do
                if not (condition v)
                    then
                        pure (icount, vcount + 1, cap, mv)
                    else do
                        let shouldGrow = icount == cap
                        mv' <- if shouldGrow then VUM.grow mv cap else pure mv
                        VUM.write mv' icount vcount
                        pure (icount + 1, vcount + 1, cap + (cap * fromEnum shouldGrow), mv')
            )
            (0, 0, 8192, ixs)
            cols
    VU.freeze (VUM.slice 0 icount ixs')

{- | O(k) a version of filter where the predicate comes first.

> filterBy even "x" df
-}
filterBy :: (Columnable a) => (a -> Bool) -> T.Text -> DataFrame -> DataFrame
filterBy = flip filter

{- | O(k) filters the dataframe with a row predicate. The arguments in the function
  must appear in the same order as they do in the list.

> filterWhere (["x", "y"], func (\x y -> x + y > 5)) df
-}
filterWhere :: Expr Bool -> DataFrame -> DataFrame
filterWhere expr df =
    let
        (TColumn col) = interpret @Bool df expr
        (Just indexes) = findIndices (== True) col
        c' = snd $ dataframeDimensions df
        pick idxs col = atIndicesStable idxs col
     in
        df{columns = V.map (pick indexes) (columns df), dataframeDimensions = (VU.length indexes, c')}

{- | O(k) removes all rows with `Nothing` in a given column from the dataframe.

> filterJust df
-}
filterJust :: T.Text -> DataFrame -> DataFrame
filterJust name df = case getColumn name df of
    Nothing -> throw $ ColumnNotFoundException name "filterJust" (map fst $ M.toList $ columnIndices df)
    Just column@(OptionalColumn (col :: V.Vector (Maybe a))) -> filter @(Maybe a) name isJust df & apply @(Maybe a) fromJust name
    Just column -> df

{- | O(n * k) removes all rows with `Nothing` from the dataframe.

> filterJust df
-}
filterAllJust :: DataFrame -> DataFrame
filterAllJust df = foldr filterJust df (columnNames df)

{- | O(k) cuts the dataframe in a cube of size (a, b) where
  a is the length and b is the width.

> cube (10, 5) df
-}
cube :: (Int, Int) -> DataFrame -> DataFrame
cube (length, width) = take length . selectBy [ColumnIndexRange (0, width - 1)]

{- | O(n) Selects a number of columns in a given dataframe.

> select ["name", "age"] df
-}
select ::
    [T.Text] ->
    DataFrame ->
    DataFrame
select cs df
    | L.null cs = empty
    | any (`notElem` columnNames df) cs = throw $ ColumnNotFoundException (T.pack $ show $ cs L.\\ columnNames df) "select" (columnNames df)
    | otherwise = L.foldl' addKeyValue empty cs
  where
    addKeyValue d k = fromMaybe df $ do
        col <- getColumn k df
        pure $ insertColumn k col d

data SelectionCriteria
    = ColumnProperty (Column -> Bool)
    | ColumnNameProperty (T.Text -> Bool)
    | ColumnTextRange (T.Text, T.Text)
    | ColumnIndexRange (Int, Int)
    | ColumnName T.Text

byName :: T.Text -> SelectionCriteria
byName = ColumnName

byProperty :: (T.Text -> Bool) -> SelectionCriteria
byProperty = ColumnNameProperty

byNameProperty :: (T.Text -> Bool) -> SelectionCriteria
byNameProperty = ColumnNameProperty

byNameRange :: (T.Text, T.Text) -> SelectionCriteria
byNameRange = ColumnTextRange

byIndexRange :: (Int, Int) -> SelectionCriteria
byIndexRange = ColumnIndexRange

-- | O(n) select columns by column predicate name.
selectBy :: [SelectionCriteria] -> DataFrame -> DataFrame
selectBy xs df = select columnsWithProperties df
  where
    columnsWithProperties = L.foldl' columnWithProperty [] xs
    columnWithProperty acc (ColumnName name) = acc ++ [name]
    columnWithProperty acc (ColumnNameProperty f) = acc ++ L.filter f (columnNames df)
    columnWithProperty acc (ColumnTextRange (from, to)) = acc ++ reverse (Prelude.dropWhile (to /=) $ reverse $ dropWhile (from /=) (columnNames df))
    columnWithProperty acc (ColumnIndexRange (from, to)) = acc ++ Prelude.take (to - from + 1) (Prelude.drop from (columnNames df))
    columnWithProperty acc (ColumnProperty f) = acc ++ map fst (L.filter (\(k, v) -> v `elem` ixs) (M.toAscList (columnIndices df)))
      where
        ixs = V.ifoldl' (\acc i c -> if f c then i : acc else acc) [] (columns df)

{- | O(n) inverse of select

> exclude ["Name"] df
-}
exclude ::
    [T.Text] ->
    DataFrame ->
    DataFrame
exclude cs df =
    let keysToKeep = columnNames df L.\\ cs
     in select keysToKeep df
