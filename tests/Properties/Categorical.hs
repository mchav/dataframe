{-# LANGUAGE OverloadedStrings #-}

-- | Property tests pinning the categorical laws of set algebra and migration.
module Properties.Categorical (tests) where

import Data.Text ()

import qualified DataFrame as D
import qualified DataFrame.Internal.Column as DI
import DataFrame.Internal.DataFrame (
    DataFrame,
    columnNames,
    dataframeDimensions,
 )

import Test.QuickCheck

nRows :: DataFrame -> Int
nRows = fst . dataframeDimensions

-- | Equality of the underlying row sets, a sound oracle for the topos laws.
sameRows :: DataFrame -> DataFrame -> Property
sameRows x y = x === y

-- | A schema-compatible pair: two row-subsets of a common base frame.
data Pair = Pair DataFrame DataFrame deriving (Show)

-- | A schema-compatible triple, likewise.
data Triple = Triple DataFrame DataFrame DataFrame deriving (Show)

-- | A base frame of Int and Maybe Int columns, with many duplicate rows.
genBase :: Gen DataFrame
genBase = do
    nCols <- choose (1, 4)
    nRowsG <- choose (0, 60)
    let names = take nCols ["c0", "c1", "c2", "c3"]
    cols <- mapM (const (genCol nRowsG)) names
    pure (D.fromColumns (zip names cols))
  where
    genCol n =
        oneof
            [ DI.fromList <$> vectorOf n (choose (-3, 3) :: Gen Int)
            , DI.fromList <$> vectorOf n genMaybeInt
            ]
    genMaybeInt =
        frequency
            [ (3, Just <$> (choose (-3, 3) :: Gen Int))
            , (1, pure Nothing)
            ]

subset :: DataFrame -> Gen DataFrame
subset df = do
    let n = nRows df
    lo <- choose (0, n)
    hi <- choose (0, n)
    pure (D.range (min lo hi, max lo hi) df)

instance Arbitrary Pair where
    arbitrary = do
        base <- genBase
        Pair <$> subset base <*> subset base

instance Arbitrary Triple where
    arbitrary = do
        base <- genBase
        Triple <$> subset base <*> subset base <*> subset base

-- | A single clean base frame (Int and Maybe Int columns only).
newtype Frame = Frame DataFrame deriving (Show)

instance Arbitrary Frame where
    arbitrary = Frame <$> genBase

-- | An empty frame with the same schema as @df@ (zero rows, same columns).
emptyLike :: DataFrame -> DataFrame
emptyLike = D.range (0, 0)

prop_unionCommutative :: Pair -> Property
prop_unionCommutative (Pair a b) = sameRows (D.union a b) (D.union b a)

prop_unionAssociative :: Triple -> Property
prop_unionAssociative (Triple a b c) =
    sameRows (D.union (D.union a b) c) (D.union a (D.union b c))

prop_unionIdempotent :: Pair -> Property
prop_unionIdempotent (Pair a _) = sameRows (D.union a a) (D.distinct a)

prop_intersectCommutative :: Pair -> Property
prop_intersectCommutative (Pair a b) = sameRows (D.intersect a b) (D.intersect b a)

prop_intersectIdempotent :: Pair -> Property
prop_intersectIdempotent (Pair a _) = sameRows (D.intersect a a) (D.distinct a)

prop_differenceSelfEmpty :: Pair -> Property
prop_differenceSelfEmpty (Pair a _) = nRows (D.difference a a) === 0

prop_differenceEmptyRight :: Pair -> Property
prop_differenceEmptyRight (Pair a _) =
    sameRows (D.difference a (emptyLike a)) (D.distinct a)

prop_unionAlreadyDistinct :: Pair -> Property
prop_unionAlreadyDistinct (Pair a b) =
    sameRows (D.distinct (D.union a b)) (D.union a b)

prop_symmetricDifferenceDef :: Pair -> Property
prop_symmetricDifferenceDef (Pair a b) =
    sameRows
        (D.symmetricDifference a b)
        (D.union (D.difference a b) (D.difference b a))

-- | Topos law: the complement and the intersection partition the left set.
prop_complementPartition :: Pair -> Property
prop_complementPartition (Pair a b) =
    sameRows (D.union (D.difference a b) (D.intersect a b)) (D.distinct a)

-- | The complement is disjoint from the subtrahend.
prop_differenceDisjoint :: Pair -> Property
prop_differenceDisjoint (Pair a b) =
    nRows (D.intersect (D.difference a b) b) === 0

-- | Excluding nothing is the identity.
prop_excludeNothingIdentity :: Frame -> Property
prop_excludeNothingIdentity (Frame df) = D.exclude [] df === df

-- | Renaming a column and back is the identity (functor preserves identities).
prop_renameRoundTrip :: Frame -> Property
prop_renameRoundTrip (Frame df) =
    case columnNames df of
        [] -> property True
        (name : _) ->
            let tmp = name <> "__rt_tmp"
             in notElem tmp (columnNames df) ==>
                    D.rename tmp name (D.rename name tmp df) === df

tests :: [Property]
tests =
    [ property prop_unionCommutative
    , property prop_unionAssociative
    , property prop_unionIdempotent
    , property prop_intersectCommutative
    , property prop_intersectIdempotent
    , property prop_differenceSelfEmpty
    , property prop_differenceEmptyRight
    , property prop_unionAlreadyDistinct
    , property prop_symmetricDifferenceDef
    , property prop_complementPartition
    , property prop_differenceDisjoint
    , property prop_excludeNothingIdentity
    , property prop_renameRoundTrip
    ]
