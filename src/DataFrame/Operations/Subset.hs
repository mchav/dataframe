{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE FlexibleContexts #-}
module DataFrame.Operations.Subset where

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Generic as VG
import qualified Prelude

import Control.Exception (throw)
import DataFrame.Errors (DataFrameException(..), TypeErrorContext(..))
import DataFrame.Internal.Column
import DataFrame.Internal.DataFrame (DataFrame(..), getColumn, empty)
import DataFrame.Internal.Expression
import DataFrame.Internal.Row (mkRowFromArgs, RowValue, toRowValue)
import DataFrame.Operations.Core
import DataFrame.Operations.Transformations (apply)
import Data.Function ((&))
import Data.Maybe (isJust, fromJust, fromMaybe)
import Prelude hiding (filter, take)
import Type.Reflection

-- | O(k * n) Take the first n rows of a DataFrame.
take :: Int -> DataFrame -> DataFrame
take n d = d {columns = V.map (takeColumn n') (columns d), dataframeDimensions = (n', c)}
  where
    (r, c) = dataframeDimensions d
    n' = clip n 0 r

takeLast :: Int -> DataFrame -> DataFrame
takeLast n d = d {columns = V.map (takeLastColumn n') (columns d), dataframeDimensions = (n', c)}
  where
    (r, c) = dataframeDimensions d
    n' = clip n 0 r

drop :: Int -> DataFrame -> DataFrame
drop n d = d {columns = V.map (sliceColumn n' (max (r - n') 0)) (columns d), dataframeDimensions = (max (r - n') 0, c)}
  where
    (r, c) = dataframeDimensions d
    n' = clip n 0 r

dropLast :: Int -> DataFrame -> DataFrame
dropLast n d = d {columns = V.map (sliceColumn 0 n') (columns d), dataframeDimensions = (n', c)}
  where
    (r, c) = dataframeDimensions d
    n' = clip (r - n) 0 r

-- | O(k * n) Take a range of rows of a DataFrame.
range :: (Int, Int) -> DataFrame -> DataFrame
range (start, end) d = d {columns = V.map (sliceColumn (clip start 0 r) n') (columns d), dataframeDimensions = (n', c)}
  where
    (r, c) = dataframeDimensions d
    n' = clip (end - start) 0 r

clip :: Int -> Int -> Int -> Int
clip n left right = min right $ max n left

-- | O(n * k) Filter rows by a given condition.
--
-- filter "x" even df
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
  Just column -> case ifoldlColumn (\s i v -> if condition v then S.insert i s else s) S.empty column of
    Nothing -> throw $ TypeMismatchException (MkTypeErrorContext
                                                        { userType = Right $ typeRep @a
                                                        , expectedType = Left (columnTypeString column) :: Either String (TypeRep ()) 
                                                        , errorColumnName = Just (T.unpack filterColumnName)
                                                        , callingFunctionName = Just "filter"})
    Just indexes -> let
        c' = snd $ dataframeDimensions df
        pick idxs col = atIndices idxs col
      in df {columns = V.map (pick indexes) (columns df), dataframeDimensions = (S.size indexes, c')}

-- | O(k) a version of filter where the predicate comes first.
--
-- > filterBy even "x" df
filterBy :: (Columnable a) => (a -> Bool) -> T.Text -> DataFrame -> DataFrame
filterBy = flip filter

-- | O(k) filters the dataframe with a row predicate. The arguments in the function
--   must appear in the same order as they do in the list.
--
-- > filterWhere (["x", "y"], func (\x y -> x + y > 5)) df
filterWhere :: Expr Bool -> DataFrame -> DataFrame
filterWhere expr df = let
    (TColumn col) = interpret @Bool df expr
    (Just indexes) = VU.convert . V.map (fromMaybe 0) . V.filter isJust . toVector @(Maybe Int) <$> imapColumn (\i satisfied -> if satisfied then Just i else Nothing) col
    c' = snd $ dataframeDimensions df
    pick idxs col = atIndicesStable idxs col
  in df {columns = V.map (pick indexes) (columns df), dataframeDimensions = (VU.length indexes, c')}


-- | O(k) removes all rows with `Nothing` in a given column from the dataframe.
--
-- > filterJust df
filterJust :: T.Text -> DataFrame -> DataFrame
filterJust name df = case getColumn name df of
  Nothing -> throw $ ColumnNotFoundException name "filterJust" (map fst $ M.toList $ columnIndices df)
  Just column@(OptionalColumn (col :: V.Vector (Maybe a))) -> filter @(Maybe a) name isJust df & apply @(Maybe a) fromJust name
  Just column -> df

-- | O(n * k) removes all rows with `Nothing` from the dataframe.
--
-- > filterJust df
filterAllJust :: DataFrame -> DataFrame
filterAllJust df = foldr filterJust df (columnNames df)

-- | O(k) cuts the dataframe in a cube of size (a, b) where
--   a is the length and b is the width.   
--
-- > cube (10, 5) df
cube :: (Int, Int) -> DataFrame -> DataFrame
cube (length, width) = take length . selectIntRange (0, width - 1)

-- | O(n) Selects a number of columns in a given dataframe.
--
-- > select ["name", "age"] df
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

-- | O(n) select columns by index range of column names.
selectIntRange :: (Int, Int) -> DataFrame -> DataFrame
selectIntRange (from, to) df = select (Prelude.take (to - from + 1) $ Prelude.drop from (columnNames df)) df

-- | O(n) select columns by index range of column names.
selectRange :: (T.Text, T.Text) -> DataFrame -> DataFrame
selectRange (from, to) df = select (reverse $ Prelude.dropWhile (to /=) $ reverse $ dropWhile (from /=) (columnNames df)) df

-- | O(n) select columns by column predicate name.
selectBy :: (T.Text -> Bool) -> DataFrame -> DataFrame
selectBy f df = select (L.filter f (columnNames df)) df

-- | O(n) inverse of select
--
-- > exclude ["Name"] df
exclude ::
  [T.Text] ->
  DataFrame ->
  DataFrame
exclude cs df =
  let keysToKeep = columnNames df L.\\ cs
   in select keysToKeep df
