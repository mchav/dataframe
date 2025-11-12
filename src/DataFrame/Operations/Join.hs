{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

module DataFrame.Operations.Join where

import qualified Data.Map.Strict as M
import qualified Data.Text as T
import qualified Data.Vector as VB
import qualified Data.Vector.Unboxed as VU
import DataFrame.Internal.Column as D
import DataFrame.Internal.DataFrame as D
import DataFrame.Operations.Aggregation as D
import DataFrame.Operations.Core as D

-- | Equivalent to SQL join types.
data JoinType
    = INNER
    | LEFT
    | RIGHT
    | FULL_OUTER

{- | Join two dataframes using SQL join semantics.

Only inner join is implemented for now.
-}
join ::
    JoinType ->
    [T.Text] ->
    DataFrame -> -- Right hand side
    DataFrame -> -- Left hand side
    DataFrame
join INNER xs right = innerJoin xs right
join LEFT xs right = leftJoin xs right
join RIGHT xs right = rightJoin xs right
join FULL_OUTER xs right = fullOuterJoin xs right

{- | Performs an inner join on two dataframes using the specified key columns.
Returns only rows where the key values exist in both dataframes.

==== __Example__
@
ghci> df = D.fromNamedColumns [("key", D.fromList ["K0", "K1", "K2", "K3"]), ("A", D.fromList ["A0", "A1", "A2", "A3"])]
ghci> other = D.fromNamedColumns [("key", D.fromList ["K0", "K1", "K2"]), ("B", D.fromList ["B0", "B1", "B2"])]
ghci> D.innerJoin ["key"] df other

-----------------
 key  |  A  |  B
------|-----|----
 Text | Text| Text
------|-----|----
 K0   | A0  | B0
 K1   | A1  | B1
 K2   | A2  | B2

@
-}
innerJoin ::
    [T.Text] -> DataFrame -> DataFrame -> DataFrame
innerJoin cs right left =
    let
        leftIndicesToGroup = M.elems $ M.filterWithKey (\k _ -> k `elem` cs) (D.columnIndices left)
        leftRowRepresentations = VU.generate (fst (D.dimensions left)) (D.mkRowRep leftIndicesToGroup left)
        -- key -> [index0, index1]
        leftKeyCountsAndIndices =
            VU.foldr
                (\(i, v) acc -> M.insertWith (++) v [i] acc)
                M.empty
                (VU.indexed leftRowRepresentations)
        -- key -> [index0, index1]
        rightIndicesToGroup = M.elems $ M.filterWithKey (\k _ -> k `elem` cs) (D.columnIndices right)
        rightRowRepresentations = VU.generate (fst (D.dimensions right)) (D.mkRowRep rightIndicesToGroup right)
        rightKeyCountsAndIndices =
            VU.foldr
                (\(i, v) acc -> M.insertWith (++) v [i] acc)
                M.empty
                (VU.indexed rightRowRepresentations)
        -- key -> [(left_indexes0, right_indexes1)]
        mergedKeyCountsAndIndices =
            M.foldrWithKey
                ( \k v m ->
                    if k `M.member` rightKeyCountsAndIndices
                        then M.insert k (VU.fromList v, VU.fromList (rightKeyCountsAndIndices M.! k)) m
                        else m
                )
                M.empty
                leftKeyCountsAndIndices
        -- [(ints, ints)]
        leftAndRightIndicies = M.elems mergedKeyCountsAndIndices
        -- [(ints, ints)] (expanded to n * m)
        expandedIndices =
            map
                ( \(l, r) -> (mconcat (replicate (VU.length r) l), mconcat (replicate (VU.length l) r))
                )
                leftAndRightIndicies
        expandedLeftIndicies = mconcat (map fst expandedIndices)
        expandedRightIndicies = mconcat (map snd expandedIndices)
        -- df
        expandedLeft =
            left
                { columns = VB.map (D.atIndicesStable expandedLeftIndicies) (D.columns left)
                , dataframeDimensions =
                    (VU.length expandedLeftIndicies, snd (D.dataframeDimensions left))
                }
        -- df
        expandedRight =
            right
                { columns = VB.map (D.atIndicesStable expandedRightIndicies) (D.columns right)
                , dataframeDimensions =
                    (VU.length expandedRightIndicies, snd (D.dataframeDimensions right))
                }
        -- [string]
        leftColumns = D.columnNames left
        rightColumns = D.columnNames right
        initDf = expandedLeft
        insertIfPresent _ Nothing df = df
        insertIfPresent name (Just c) df = D.insertColumn name c df
     in
        D.fold
            ( \name df ->
                if name `elem` cs
                    then df
                    else
                        ( if name `elem` leftColumns
                            then insertIfPresent ("Right_" <> name) (D.getColumn name expandedRight) df
                            else insertIfPresent name (D.getColumn name expandedRight) df
                        )
            )
            rightColumns
            initDf

{- | Performs a left join on two dataframes using the specified key columns.
Returns all rows from the left dataframe, with matching rows from the right dataframe.
Non-matching rows will have Nothing/null values for columns from the right dataframe.

==== __Example__
@
ghci> df = D.fromNamedColumns [("key", D.fromList ["K0", "K1", "K2", "K3"]), ("A", D.fromList ["A0", "A1", "A2", "A3"])]
ghci> other = D.fromNamedColumns [("key", D.fromList ["K0", "K1", "K2"]), ("B", D.fromList ["B0", "B1", "B2"])]
ghci> D.leftJoin ["key"] df other

------------------------
 key  |  A  |     B
------|-----|----------
 Text | Text| Maybe Text
------|-----|----------
 K0   | A0  | Just "B0"
 K1   | A1  | Just "B1"
 K2   | A2  | Just "B2"
 K3   | A3  | Nothing

@
-}
leftJoin ::
    [T.Text] -> DataFrame -> DataFrame -> DataFrame
leftJoin cs right left =
    let
        leftIndicesToGroup = M.elems $ M.filterWithKey (\k _ -> k `elem` cs) (D.columnIndices left)
        leftRowRepresentations = VU.generate (fst (D.dimensions left)) (D.mkRowRep leftIndicesToGroup left)
        rightIndicesToGroup = M.elems $ M.filterWithKey (\k _ -> k `elem` cs) (D.columnIndices right)
        rightRowRepresentations = VU.generate (fst (D.dimensions right)) (D.mkRowRep rightIndicesToGroup right)
        rightKeyCountsAndIndices =
            VU.foldr
                (\(i, v) acc -> M.insertWith (++) v [i] acc)
                M.empty
                (VU.indexed rightRowRepresentations)
        rightKeyCountsAndIndicesVec = M.map VU.fromList rightKeyCountsAndIndices
        leftRowCount = fst (D.dimensions left)
        pairs =
            [ (i, maybeRight)
            | i <- [0 .. leftRowCount - 1]
            , maybeRight <-
                case M.lookup (leftRowRepresentations VU.! i) rightKeyCountsAndIndicesVec of
                    Nothing -> [Nothing]
                    Just rVec -> map Just (VU.toList rVec)
            ]
        expandedLeftIndicies = VU.fromList (map fst pairs)
        expandedRightIndicies = VB.fromList (map snd pairs)
        expandedLeft =
            left
                { columns = VB.map (D.atIndicesStable expandedLeftIndicies) (D.columns left)
                , dataframeDimensions =
                    (VU.length expandedLeftIndicies, snd (D.dataframeDimensions left))
                }
        expandedRight =
            right
                { columns = VB.map (D.atIndicesWithNulls expandedRightIndicies) (D.columns right)
                , dataframeDimensions =
                    (VB.length expandedRightIndicies, snd (D.dataframeDimensions right))
                }
        leftColumns = D.columnNames left
        rightColumns = D.columnNames right
        initDf = expandedLeft
        insertIfPresent _ Nothing df = df
        insertIfPresent name (Just c) df = D.insertColumn name c df
     in
        D.fold
            ( \name df ->
                if name `elem` cs
                    then df
                    else
                        ( if name `elem` leftColumns
                            then insertIfPresent ("Right_" <> name) (D.getColumn name expandedRight) df
                            else insertIfPresent name (D.getColumn name expandedRight) df
                        )
            )
            rightColumns
            initDf

{- | Performs a right join on two dataframes using the specified key columns.
Returns all rows from the right dataframe, with matching rows from the left dataframe.
Non-matching rows will have Nothing/null values for columns from the left dataframe.

==== __Example__
@
ghci> df = D.fromNamedColumns [("key", D.fromList ["K0", "K1", "K2", "K3"]), ("A", D.fromList ["A0", "A1", "A2", "A3"])]
ghci> other = D.fromNamedColumns [("key", D.fromList ["K0", "K1"]), ("B", D.fromList ["B0", "B1"])]
ghci> D.rightJoin ["key"] df other

-----------------
 key  |  A  |  B
------|-----|----
 Text | Text| Text
------|-----|----
 K0   | A0  | B0
 K1   | A1  | B1

@
-}
rightJoin ::
    [T.Text] -> DataFrame -> DataFrame -> DataFrame
rightJoin cs right left =
    let
        leftIndicesToGroup = M.elems $ M.filterWithKey (\k _ -> k `elem` cs) (D.columnIndices left)
        leftRowRepresentations = VU.generate (fst (D.dimensions left)) (D.mkRowRep leftIndicesToGroup left)
        leftKeyCountsAndIndicesVec =
            M.map VU.fromList $
                VU.foldr
                    (\(i, v) acc -> M.insertWith (++) v [i] acc)
                    M.empty
                    (VU.indexed leftRowRepresentations)
        rightIndicesToGroup = M.elems $ M.filterWithKey (\k _ -> k `elem` cs) (D.columnIndices right)
        rightRowRepresentations = VU.generate (fst (D.dimensions right)) (D.mkRowRep rightIndicesToGroup right)
        rightRowCount = fst (D.dimensions right)
        pairs =
            [ (maybeLeft, j)
            | j <- [0 .. rightRowCount - 1]
            , maybeLeft <-
                case M.lookup (rightRowRepresentations VU.! j) leftKeyCountsAndIndicesVec of
                    Nothing -> [Nothing]
                    Just lVec -> map Just (VU.toList lVec)
            ]
        expandedLeftIndicies = VB.fromList (map fst pairs)
        expandedRightIndicies = VU.fromList (map snd pairs)
        expandedLeft =
            left
                { columns = VB.map (D.atIndicesWithNulls expandedLeftIndicies) (D.columns left)
                , dataframeDimensions =
                    (VB.length expandedLeftIndicies, snd (D.dataframeDimensions left))
                }
        expandedRight =
            right
                { columns = VB.map (D.atIndicesStable expandedRightIndicies) (D.columns right)
                , dataframeDimensions =
                    (VU.length expandedRightIndicies, snd (D.dataframeDimensions right))
                }
        leftColumns = D.columnNames left
        rightColumns = D.columnNames right
        initDf = expandedLeft
        insertIfPresent _ Nothing df = df
        insertIfPresent name (Just c) df = D.insertColumn name c df
     in
        D.fold
            ( \name df ->
                if name `elem` cs
                    then df
                    else
                        ( if name `elem` leftColumns
                            then insertIfPresent ("Right_" <> name) (D.getColumn name expandedRight) df
                            else insertIfPresent name (D.getColumn name expandedRight) df
                        )
            )
            rightColumns
            initDf

fullOuterJoin ::
    [T.Text] -> DataFrame -> DataFrame -> DataFrame
fullOuterJoin cs right left =
    let
        leftIndicesToGroup = M.elems $ M.filterWithKey (\k _ -> k `elem` cs) (D.columnIndices left)
        leftRowRepresentations = VU.generate (fst (D.dimensions left)) (D.mkRowRep leftIndicesToGroup left)
        leftKeyCountsAndIndices =
            VU.foldr
                (\(i, v) acc -> M.insertWith (++) v [i] acc)
                M.empty
                (VU.indexed leftRowRepresentations)
        leftKeyCountsAndIndicesVec = M.map VU.fromList leftKeyCountsAndIndices
        rightIndicesToGroup = M.elems $ M.filterWithKey (\k _ -> k `elem` cs) (D.columnIndices right)
        rightRowRepresentations = VU.generate (fst (D.dimensions right)) (D.mkRowRep rightIndicesToGroup right)
        rightKeyCountsAndIndices =
            VU.foldr
                (\(i, v) acc -> M.insertWith (++) v [i] acc)
                M.empty
                (VU.indexed rightRowRepresentations)
        rightKeyCountsAndIndicesVec = M.map VU.fromList rightKeyCountsAndIndices
        matchedPairs =
            concatMap
                ( \(lVec, rVec) ->
                    [ (Just lIdx, Just rIdx)
                    | lIdx <- VU.toList lVec
                    , rIdx <- VU.toList rVec
                    ]
                )
                ( M.elems
                    (M.intersectionWith (,) leftKeyCountsAndIndicesVec rightKeyCountsAndIndicesVec)
                )
        leftOnlyPairs =
            concatMap
                (map (\lIdx -> (Just lIdx, Nothing)) . VU.toList)
                (M.elems (leftKeyCountsAndIndicesVec `M.difference` rightKeyCountsAndIndicesVec))
        rightOnlyPairs =
            concatMap
                (map (\rIdx -> (Nothing, Just rIdx)) . VU.toList)
                (M.elems (rightKeyCountsAndIndicesVec `M.difference` leftKeyCountsAndIndicesVec))
        pairs = matchedPairs ++ leftOnlyPairs ++ rightOnlyPairs
        expandedLeftIndicies = VB.fromList (map fst pairs)
        expandedRightIndicies = VB.fromList (map snd pairs)
        expandedLeft =
            left
                { columns = VB.map (D.atIndicesWithNulls expandedLeftIndicies) (D.columns left)
                , dataframeDimensions =
                    (VB.length expandedLeftIndicies, snd (D.dataframeDimensions left))
                }
        expandedRight =
            right
                { columns = VB.map (D.atIndicesWithNulls expandedRightIndicies) (D.columns right)
                , dataframeDimensions =
                    (VB.length expandedRightIndicies, snd (D.dataframeDimensions right))
                }
        leftColumns = D.columnNames left
        rightColumns = D.columnNames right
        initDf = expandedLeft
        insertIfPresent _ Nothing df = df
        insertIfPresent name (Just c) df = D.insertColumn name c df
     in
        D.fold
            ( \name df ->
                if name `elem` cs
                    then df
                    else
                        ( if name `elem` leftColumns
                            then insertIfPresent ("Right_" <> name) (D.getColumn name expandedRight) df
                            else insertIfPresent name (D.getColumn name expandedRight) df
                        )
            )
            rightColumns
            initDf
