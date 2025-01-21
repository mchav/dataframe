{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
module Data.DataFrame.Operations.Subset where

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Generic as VG

import Control.Exception (throw)
import Data.DataFrame.Errors (DataFrameException(..))
import Data.DataFrame.Internal.Column
import Data.DataFrame.Internal.DataFrame (DataFrame(..), getColumn, empty)
import Data.DataFrame.Internal.Row (mkRowFromArgs)
import Data.DataFrame.Internal.Types (Columnable, RowValue, toRowValue)
import Data.DataFrame.Operations.Core
import Prelude hiding (filter)
import Type.Reflection (typeRep)
import Data.DataFrame.Internal.Function

-- | O(k * n) Take the first n rows of a DataFrame.
take :: Int -> DataFrame -> DataFrame
take n d = d {columns = V.map (takeColumn n' <$>) (columns d), dataframeDimensions = (n', c)}
  where
    (r, c) = dataframeDimensions d
    n' = clip n 0 r

takeLast :: Int -> DataFrame -> DataFrame
takeLast n d = d {columns = V.map (takeLastColumn n' <$>) (columns d), dataframeDimensions = (n', c)}
  where
    (r, c) = dataframeDimensions d
    n' = clip n 0 r

drop :: Int -> DataFrame -> DataFrame
drop n d = d {columns = V.map (sliceColumn n' (max (r - n') 0) <$>) (columns d), dataframeDimensions = (max (r - n') 0, c)}
  where
    (r, c) = dataframeDimensions d
    n' = clip n 0 r

dropLast :: Int -> DataFrame -> DataFrame
dropLast n d = d {columns = V.map (sliceColumn 0 n' <$>) (columns d), dataframeDimensions = (n', c)}
  where
    (r, c) = dataframeDimensions d
    n' = clip (r - n) 0 r

-- | O(k * n) Take a range of rows of a DataFrame.
range :: (Int, Int) -> DataFrame -> DataFrame
range (start, end) d = d {columns = V.map (sliceColumn (clip start 0 r) n' <$>) (columns d), dataframeDimensions = (n', c)}
  where
    (r, c) = dataframeDimensions d
    n' = clip (end - start) 0 r

clip :: Int -> Int -> Int -> Int
clip n left right = min right $ max n left

-- | O(n * k) Filter rows by a given condition.
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
    Nothing -> throw $ TypeMismatchException' (typeRep @a) (columnTypeString column) filterColumnName "filter"
    Just indexes -> let
        c' = snd $ dataframeDimensions df
        pick idxs col = atIndices idxs <$> col
      in df {columns = V.map (pick indexes) (columns df), dataframeDimensions = (S.size indexes, c')}

filterBy :: (Columnable a) => (a -> Bool) -> T.Text -> DataFrame -> DataFrame
filterBy = flip filter

filterWhere :: ([T.Text], Function) -> DataFrame -> DataFrame
filterWhere (args, f) df = let
    indexes = VG.ifoldl' (\s i row -> if funcApply @Bool row f then S.insert i s else s) S.empty $ V.generate (fst (dimensions df)) (mkRowFromArgs args df)
    c' = snd $ dataframeDimensions df
    pick idxs col = atIndices idxs <$> col
  in df {columns = V.map (pick indexes) (columns df), dataframeDimensions = (S.size indexes, c')}

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
    cIndexAssoc = M.toList $ columnIndices df
    remaining = L.filter (\(c, i) -> c `elem` cs) cIndexAssoc
    removed = cIndexAssoc L.\\ remaining
    indexes = map snd remaining
    (r, c) = dataframeDimensions df
    addKeyValue d k =
      d
        { columns = V.imap (\i v -> if i `notElem` indexes then Nothing else v) (columns df),
          columnIndices = M.fromList remaining,
          freeIndices = map snd removed ++ freeIndices df,
          dataframeDimensions = (r, L.length remaining)
        }

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
