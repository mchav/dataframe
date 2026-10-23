{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

module DataFrame.Typed.Join (
    -- * Typed joins
    join,
    joinOn,
    JoinSchema,
    JoinOnSchema,
    JoinKeys,
    KnownJoinType (..),
    Fsts,
    SwapPairs,
    innerJoin,
    leftJoin,
    rightJoin,
    fullOuterJoin,
) where

import Data.Kind (Type)
import GHC.TypeLits (Symbol)

import DataFrame.Operations.Join (JoinType (..))
import qualified DataFrame.Operations.Join as DJ

import DataFrame.Typed.Freeze (unsafeFreeze)
import DataFrame.Typed.Operations (AllKnownPairs (..))
import DataFrame.Typed.Schema
import DataFrame.Typed.Types (TypedDataFrame (..))

-- | Typed inner join on one or more key columns.
innerJoin ::
    forall (keys :: [Symbol]) left right.
    ( AllKnownSymbol keys
    , AssertAllPresent keys left
    , AssertAllPresent keys right
    , AssertKeyTypesMatch keys left right
    ) =>
    TypedDataFrame left ->
    TypedDataFrame right ->
    TypedDataFrame (InnerJoinSchema keys left right)
innerJoin (TDF l) (TDF r) =
    unsafeFreeze (DJ.innerJoin keyNames l r)
  where
    keyNames = symbolVals @keys

-- | Typed left join.
leftJoin ::
    forall (keys :: [Symbol]) left right.
    ( AllKnownSymbol keys
    , AssertAllPresent keys left
    , AssertAllPresent keys right
    , AssertKeyTypesMatch keys left right
    ) =>
    TypedDataFrame left ->
    TypedDataFrame right ->
    TypedDataFrame (LeftJoinSchema keys left right)
leftJoin (TDF l) (TDF r) =
    unsafeFreeze (DJ.leftJoin keyNames l r)
  where
    keyNames = symbolVals @keys

-- | Typed right join.
rightJoin ::
    forall (keys :: [Symbol]) left right.
    ( AllKnownSymbol keys
    , AssertAllPresent keys left
    , AssertAllPresent keys right
    , AssertKeyTypesMatch keys left right
    ) =>
    TypedDataFrame left ->
    TypedDataFrame right ->
    TypedDataFrame (RightJoinSchema keys left right)
rightJoin (TDF l) (TDF r) =
    unsafeFreeze (DJ.rightJoin keyNames l r)
  where
    keyNames = symbolVals @keys

-- | Typed full outer join.
fullOuterJoin ::
    forall (keys :: [Symbol]) left right.
    ( AllKnownSymbol keys
    , AssertAllPresent keys left
    , AssertAllPresent keys right
    , AssertKeyTypesMatch keys left right
    ) =>
    TypedDataFrame left ->
    TypedDataFrame right ->
    TypedDataFrame (FullOuterJoinSchema keys left right)
fullOuterJoin (TDF l) (TDF r) =
    unsafeFreeze (DJ.fullOuterJoin keyNames l r)
  where
    keyNames = symbolVals @keys

-- | Keys present on both sides with matching types.
type JoinKeys keys left right =
    ( AllKnownSymbol keys
    , AssertAllPresent keys left
    , AssertAllPresent keys right
    , AssertKeyTypesMatch keys left right
    )

-- | Result schema of a @jt@ join on @keys@.
type family
    JoinSchema
        (jt :: JoinType)
        (keys :: [Symbol])
        (left :: [(Symbol, Type)])
        (right :: [(Symbol, Type)]) ::
        [(Symbol, Type)]
    where
    JoinSchema 'INNER keys left right = InnerJoinSchema keys left right
    JoinSchema 'LEFT keys left right = LeftJoinSchema keys left right
    JoinSchema 'RIGHT keys left right = RightJoinSchema keys left right
    JoinSchema 'FULL_OUTER keys left right = FullOuterJoinSchema keys left right

-- | Reflect a promoted 'JoinType' to its value.
class KnownJoinType (jt :: JoinType) where
    joinTypeVal :: JoinType

instance KnownJoinType 'INNER where joinTypeVal = INNER
instance KnownJoinType 'LEFT where joinTypeVal = LEFT
instance KnownJoinType 'RIGHT where joinTypeVal = RIGHT
instance KnownJoinType 'FULL_OUTER where joinTypeVal = FULL_OUTER

-- | Typed counterpart of untyped @join@: @join \@'INNER \@'["key"] l r@.
join ::
    forall (jt :: JoinType) (keys :: [Symbol]) left right.
    (KnownJoinType jt, JoinKeys keys left right) =>
    TypedDataFrame left ->
    TypedDataFrame right ->
    TypedDataFrame (JoinSchema jt keys left right)
join (TDF l) (TDF r) =
    unsafeFreeze (DJ.join (joinTypeVal @jt) (symbolVals @keys) l r)

-- | Left names of @(leftKey, rightKey)@ pairs.
type family Fsts (pairs :: [(Symbol, Symbol)]) :: [Symbol] where
    Fsts '[] = '[]
    Fsts ('(l, _) ': rest) = l ': Fsts rest

-- | Flip each pair, turning @(left, right)@ into a right-to-left rename.
type family SwapPairs (pairs :: [(Symbol, Symbol)]) :: [(Symbol, Symbol)] where
    SwapPairs '[] = '[]
    SwapPairs ('(l, r) ': rest) = '(r, l) ': SwapPairs rest

-- | Result schema of 'joinOn': right keys take their left names first.
type JoinOnSchema jt pairs left right =
    JoinSchema jt (Fsts pairs) left (RenameManyInSchema (SwapPairs pairs) right)

-- | Join on key pairs: @joinOn \@'INNER \@'[ '("id", "uid")] l r@.
joinOn ::
    forall (jt :: JoinType) (pairs :: [(Symbol, Symbol)]) left right.
    ( KnownJoinType jt
    , AllKnownPairs pairs
    , JoinKeys (Fsts pairs) left (RenameManyInSchema (SwapPairs pairs) right)
    ) =>
    TypedDataFrame left ->
    TypedDataFrame right ->
    TypedDataFrame (JoinOnSchema jt pairs left right)
joinOn (TDF l) (TDF r) =
    unsafeFreeze (DJ.joinOn (joinTypeVal @jt) (pairVals @pairs) l r)
