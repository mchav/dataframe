{-# LANGUAGE OverloadedStrings #-}
module Data.DataFrame.Internal.Row where

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import Control.Exception (throw)
import Data.DataFrame.Errors (DataFrameException(..))
import Data.DataFrame.Internal.Column
import Data.DataFrame.Internal.DataFrame
import Data.DataFrame.Internal.Types

type Row = [RowValue]

toRowList :: [T.Text] -> DataFrame -> [Row]
toRowList names df = let
    nameSet = S.fromList names
  in map (mkRowRep df nameSet) [0..(fst (dataframeDimensions df) - 1)]

mkRowFromArgs :: [T.Text] -> DataFrame -> Int -> [RowValue]
mkRowFromArgs names df i = foldr go [] names
  where
    go name acc = case getColumn name df of
      Nothing -> throw $ ColumnNotFoundException name "[INTERNAL] mkRowFromArgs" (map fst $ M.toList $ columnIndices df)
      Just (BoxedColumn column) -> toRowValue (column V.! i) : acc
      Just (UnboxedColumn column) -> toRowValue (column VU.! i) : acc

mkRowRep :: DataFrame -> S.Set T.Text -> Int -> Row
mkRowRep df names i = reverse $ V.ifoldl' go [] (columns df)
  where
    indexMap = M.fromList (map (\(a, b) -> (b, a)) $ M.toList (columnIndices df))
    go acc k Nothing = acc
    go acc k (Just (BoxedColumn c)) =
      if S.notMember (indexMap M.! k) names
        then acc
        else case c V.!? i of
          Just e -> toRowValue e : acc
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
          Just e -> toRowValue e : acc
          Nothing ->
            error $
              "Column "
                ++ T.unpack (indexMap M.! k)
                ++ " has less items than "
                ++ "the other columns at index "
                ++ show i
    go acc k (Just (GroupedBoxedColumn c)) =
      if S.notMember (indexMap M.! k) names
        then acc
        else case c V.!? i of
          Just e -> toRowValue e : acc
          Nothing ->
            error $
              "Column "
                ++ T.unpack (indexMap M.! k)
                ++ " has less items than "
                ++ "the other columns at index "
                ++ show i
    go acc k (Just (GroupedUnboxedColumn c)) =
      if S.notMember (indexMap M.! k) names
        then acc
        else case c V.!? i of
          Just e -> toRowValue e : acc
          Nothing ->
            error $
              "Column "
                ++ T.unpack (indexMap M.! k)
                ++ " has less items than "
                ++ "the other columns at index "
                ++ show i

sortedIndexes' :: Bool -> [Row] -> VU.Vector Int
sortedIndexes' asc rows = VU.fromList
                        $ map fst
                        $ L.sortBy (\(a, b) (a', b') -> (if asc then compare else flip compare) b b') (zip [0..] rows)
