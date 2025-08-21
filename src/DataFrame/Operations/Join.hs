{-# LANGUAGE OverloadedStrings #-}

module DataFrame.Operations.Join where

import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.Vector as VB
import qualified Data.Vector.Unboxed as VU
import DataFrame.Internal.Column as D
import DataFrame.Internal.DataFrame as D
import DataFrame.Operations.Aggregation as D
import DataFrame.Operations.Core as D

data JoinType
    = INNER
    | LEFT
    | RIGHT
    | FULL_OUTER

join ::
    JoinType ->
    [T.Text] ->
    DataFrame -> -- Right hand side
    DataFrame -> -- Left hand side
    DataFrame
join INNER xs right = innerJoin xs right
join LEFT xs right = error "UNIMPLEMENTED"
join RIGHT xs right = error "UNIMPLEMENTED"
join FULL_OUTER xs right = error "UNIMPLEMENTED"

-- Create row representation for each of the two dataframes
-- get the product of left and right counts for each key
innerJoin :: [T.Text] -> DataFrame -> DataFrame -> DataFrame
innerJoin cs right left =
    let
        leftIndicesToGroup = M.elems $ M.filterWithKey (\k _ -> k `elem` cs) (D.columnIndices left)
        leftRowRepresentations = VU.generate (fst (D.dimensions left)) (D.mkRowRep leftIndicesToGroup left)
        -- key -> [index0, index1]
        leftKeyCountsAndIndices = VU.foldr (\(i, v) acc -> M.insertWith (++) v [i] acc) M.empty (VU.indexed leftRowRepresentations)
        -- key -> [index0, index1]
        rightIndicesToGroup = M.elems $ M.filterWithKey (\k _ -> k `elem` cs) (D.columnIndices right)
        rightRowRepresentations = VU.generate (fst (D.dimensions right)) (D.mkRowRep rightIndicesToGroup right)
        rightKeyCountsAndIndices = VU.foldr (\(i, v) acc -> M.insertWith (++) v [i] acc) M.empty (VU.indexed rightRowRepresentations)
        -- key -> [(left_indexes0, right_indexes1)]
        mergedKeyCountsAndIndices = M.foldrWithKey (\k v m -> if k `M.member` rightKeyCountsAndIndices then M.insert k (VU.fromList v, VU.fromList (rightKeyCountsAndIndices M.! k)) m else m) M.empty leftKeyCountsAndIndices
        -- [(ints, ints)]
        leftAndRightIndicies = M.elems mergedKeyCountsAndIndices
        -- [(ints, ints)] (expanded to n * m)
        expandedIndices = map (\(l, r) -> (mconcat (replicate (VU.length r) l), mconcat (replicate (VU.length l) r))) leftAndRightIndicies
        expandedLeftIndicies = mconcat (map fst expandedIndices)
        expandedRightIndicies = mconcat (map snd expandedIndices)
        -- df
        expandedLeft = left{columns = VB.map (D.atIndicesStable expandedLeftIndicies) (D.columns left), dataframeDimensions = (VU.length expandedLeftIndicies, snd (D.dataframeDimensions left))}
        -- df
        expandedRight = right{columns = VB.map (D.atIndicesStable expandedRightIndicies) (D.columns right), dataframeDimensions = (VU.length expandedRightIndicies, snd (D.dataframeDimensions right))}
        -- [string]
        leftColumns = D.columnNames left
        rightColumns = D.columnNames right
        initDf = expandedLeft
        insertIfPresent _ Nothing df = df
        insertIfPresent name (Just c) df = D.insertColumn name c df
     in
        D.fold (\name df -> if name `elem` cs then df else (if name `elem` leftColumns then insertIfPresent ("Right_" <> name) (D.getColumn name expandedRight) df else insertIfPresent name (D.getColumn name expandedRight) df)) rightColumns initDf
