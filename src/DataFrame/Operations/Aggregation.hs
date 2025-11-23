{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Operations.Aggregation where

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Algorithms.Merge as VA
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Exception (throw)
import Control.Monad
import Control.Monad.ST (runST)
import Data.Hashable
import Data.Type.Equality (TestEquality (..), type (:~:) (Refl))
import DataFrame.Errors
import DataFrame.Internal.Column (
    Column (..),
    TypedColumn (..),
    atIndicesStable,
 )
import DataFrame.Internal.DataFrame (DataFrame (..), GroupedDataFrame (..))
import DataFrame.Internal.Expression
import DataFrame.Internal.Types
import DataFrame.Operations.Core
import DataFrame.Operations.Subset
import Type.Reflection (typeRep)

{- | O(k * n) groups the dataframe by the given rows aggregating the remaining rows
into vector that should be reduced later.
-}
groupBy ::
    [T.Text] ->
    DataFrame ->
    GroupedDataFrame
groupBy names df
    | any (`notElem` columnNames df) names =
        throw $
            ColumnNotFoundException
                (T.pack $ show $ names L.\\ columnNames df)
                "groupBy"
                (columnNames df)
    | otherwise =
        Grouped
            df
            names
            (VG.map fst valueIndices)
            (VU.fromList (reverse (changingPoints valueIndices)))
  where
    indicesToGroup = M.elems $ M.filterWithKey (\k _ -> k `elem` names) (columnIndices df)
    rowRepresentations = computeRowHashes indicesToGroup df

    valueIndices = runST $ do
        withIndexes <- VG.thaw $ VG.indexed rowRepresentations
        VA.sortBy (\(a, b) (a', b') -> compare b' b) withIndexes
        VG.unsafeFreeze withIndexes

changingPoints :: (Eq a, VU.Unbox a) => VU.Vector (Int, a) -> [Int]
changingPoints vs = VG.length vs : fst (VU.ifoldl findChangePoints initialState vs)
  where
    initialState = ([0], snd (VG.head vs))
    findChangePoints (offsets, currentVal) index (_, newVal)
        | currentVal == newVal = (offsets, currentVal)
        | otherwise = (index : offsets, newVal)

computeRowHashes :: [Int] -> DataFrame -> VU.Vector Int
computeRowHashes indices df = runST $ do
    let n = fst (dimensions df)
    mv <- VUM.new n

    let selectedCols = map (columns df V.!) indices

    forM_ selectedCols $ \case
        UnboxedColumn (v :: VU.Vector a) ->
            case testEquality (typeRep @a) (typeRep @Int) of
                Just Refl ->
                    VU.imapM_
                        ( \i (x :: Int) -> do
                            h <- VUM.unsafeRead mv i
                            VUM.unsafeWrite mv i (hashWithSalt h x)
                        )
                        v
                Nothing ->
                    case testEquality (typeRep @a) (typeRep @Double) of
                        Just Refl ->
                            VU.imapM_
                                ( \i (d :: Double) -> do
                                    h <- VUM.unsafeRead mv i
                                    VUM.unsafeWrite mv i (hashWithSalt h (doubleToInt d))
                                )
                                v
                        Nothing ->
                            case sIntegral @a of
                                STrue ->
                                    VU.imapM_
                                        ( \i d -> do
                                            let x :: Int
                                                x = fromIntegral @a @Int d
                                            h <- VUM.unsafeRead mv i
                                            VUM.unsafeWrite mv i (hashWithSalt h x)
                                        )
                                        v
                                SFalse ->
                                    case sFloating @a of
                                        STrue ->
                                            VU.imapM_
                                                ( \i d -> do
                                                    let x :: Int
                                                        x = doubleToInt (realToFrac d :: Double)
                                                    h <- VUM.unsafeRead mv i
                                                    VUM.unsafeWrite mv i (hashWithSalt h x)
                                                )
                                                v
                                        SFalse ->
                                            VU.imapM_
                                                ( \i d -> do
                                                    let x = hash (show d)
                                                    h <- VUM.unsafeRead mv i
                                                    VUM.unsafeWrite mv i (hashWithSalt h x)
                                                )
                                                v
        BoxedColumn (v :: V.Vector a) ->
            case testEquality (typeRep @a) (typeRep @T.Text) of
                Just Refl ->
                    V.imapM_
                        ( \i (t :: T.Text) -> do
                            h <- VUM.unsafeRead mv i
                            VUM.unsafeWrite mv i (hashWithSalt h t)
                        )
                        v
                Nothing ->
                    V.imapM_
                        ( \i d -> do
                            let x = hash (show d)
                            h <- VUM.unsafeRead mv i
                            VUM.unsafeWrite mv i (hashWithSalt h x)
                        )
                        v
        OptionalColumn v ->
            V.imapM_
                ( \i d -> do
                    let x = hash (show d)
                    h <- VUM.unsafeRead mv i
                    VUM.unsafeWrite mv i (hashWithSalt h x)
                )
                v

    VU.unsafeFreeze mv
  where
    doubleToInt :: Double -> Int
    doubleToInt = floor . (* 1000)

{- | Aggregate a grouped dataframe using the expressions given.
All ungrouped columns will be dropped.
-}
aggregate :: [NamedExpr] -> GroupedDataFrame -> DataFrame
aggregate aggs gdf@(Grouped df groupingColumns valueIndices offsets) =
    let
        df' =
            selectIndices
                (VG.map (valueIndices VG.!) (VG.init offsets))
                (select groupingColumns df)

        f (name, Wrap (expr :: Expr a)) d =
            let
                value = case interpretAggregation @a gdf expr of
                    Left e -> throw e
                    Right (UnAggregated _) -> throw $ UnaggregatedException (T.pack $ show expr)
                    Right (Aggregated (TColumn col)) -> col
             in
                insertColumn name value d
     in
        fold f aggs df'

selectIndices :: VU.Vector Int -> DataFrame -> DataFrame
selectIndices xs df =
    df
        { columns = VG.map (atIndicesStable xs) (columns df)
        , dataframeDimensions = (VG.length xs, VG.length (columns df))
        }

-- | Filter out all non-unique values in a dataframe.
distinct :: DataFrame -> DataFrame
distinct df = selectIndices (VG.map (indices VG.!) (VG.init os)) df
  where
    (Grouped _ _ indices os) = groupBy (columnNames df) df
