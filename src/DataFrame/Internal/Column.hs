{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE DataKinds              #-}
{-# LANGUAGE TypeFamilies           #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE LambdaCase #-}
module DataFrame.Internal.Column where

import qualified Data.ByteString.Char8 as C
import qualified Data.List as L
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Vector.Algorithms.Merge as VA
import qualified Data.Vector.Generic as VG
import qualified Data.Vector as VB
import qualified Data.Vector.Mutable as VBM
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Monad.ST (runST)
import DataFrame.Internal.Types
import DataFrame.Internal.Parsing
import Data.Int
import Data.Maybe
import Data.Proxy
import Data.Text.Encoding (decodeUtf8Lenient)
import Data.Type.Equality (type (:~:)(Refl), TestEquality (..))
import Data.Typeable (Typeable, cast)
import Data.Word
import Type.Reflection
import Unsafe.Coerce (unsafeCoerce)
import DataFrame.Errors
import Control.Exception (throw)
import Data.Kind (Type, Constraint)

-- | Our representation of a column is a GADT that can store data based on the underlying data.
-- 
-- This allows us to pattern match on data kinds and limit some operations to only some
-- kinds of vectors. E.g. operations for missing data only happen in an OptionalColumn.
data Column where
  BoxedColumn :: Columnable a => VB.Vector a -> Column
  UnboxedColumn :: (Columnable a, VU.Unbox a) => VU.Vector a -> Column
  OptionalColumn :: Columnable a => VB.Vector (Maybe a) -> Column
  GroupedBoxedColumn :: Columnable a => VB.Vector (VB.Vector a) -> Column
  GroupedUnboxedColumn :: (Columnable a, VU.Unbox a) => VB.Vector (VU.Vector a) -> Column
  GroupedOptionalColumn :: (Columnable a) => VB.Vector (VB.Vector (Maybe a)) -> Column
  -- These are used purely for I/O, not to store live data.
  MutableBoxedColumn :: Columnable a => VBM.IOVector a -> Column
  MutableUnboxedColumn :: (Columnable a, VU.Unbox a) => VUM.IOVector a -> Column

-- | A TypedColumn is a wrapper around our type-erased column.
-- It is used to type check expressions on columns.
data TypedColumn a where
  TColumn :: Columnable a => Column -> TypedColumn a

-- | Gets the underlying value from a TypedColumn.
unwrapTypedColumn :: TypedColumn a -> Column
unwrapTypedColumn (TColumn value) = value

-- | An internal function that checks if a column
-- can be used for aggregation.
isGrouped :: Column -> Bool
isGrouped (GroupedBoxedColumn column) = True
isGrouped (GroupedUnboxedColumn column) = True
isGrouped _ = False

-- | An internal function that checks if a column can
-- be used in missing value operations.
isOptional :: Column -> Bool
isOptional (OptionalColumn column) = True
isOptional _ = False

-- | An internal/debugging function to get the column type of a column.
columnVersionString :: Column -> String
columnVersionString column = case column of
  BoxedColumn _           -> "Boxed"
  UnboxedColumn _         -> "Unboxed"
  OptionalColumn _        -> "Optional"
  GroupedBoxedColumn _    -> "Grouped Boxed"
  GroupedUnboxedColumn _  -> "Grouped Unboxed"
  GroupedOptionalColumn _ -> "Grouped Optional"
  _                       -> "Unknown column string"

-- | An internal/debugging function to get the type stored in the outermost vector
-- of a column.
columnTypeString :: Column -> String
columnTypeString column = case column of
  BoxedColumn (column :: VB.Vector a) -> show (typeRep @a)
  UnboxedColumn (column :: VU.Vector a) -> show (typeRep @a)
  OptionalColumn (column :: VB.Vector a) -> show (typeRep @a)
  GroupedBoxedColumn (column :: VB.Vector a) -> show (typeRep @a)
  GroupedUnboxedColumn (column :: VB.Vector a) -> show (typeRep @a)
  GroupedOptionalColumn (column :: VB.Vector a) -> show (typeRep @a)

instance (Show a) => Show (TypedColumn a) where
  show (TColumn col) = show col

instance Show Column where
  show :: Column -> String
  show (BoxedColumn column) = show column
  show (UnboxedColumn column) = show column
  show (OptionalColumn column) = show column
  show (GroupedBoxedColumn column) = show column
  show (GroupedUnboxedColumn column) = show column

instance Eq Column where
  (==) :: Column -> Column -> Bool
  (==) (BoxedColumn (a :: VB.Vector t1)) (BoxedColumn (b :: VB.Vector t2)) =
    case testEquality (typeRep @t1) (typeRep @t2) of
      Nothing -> False
      Just Refl -> a == b
  (==) (OptionalColumn (a :: VB.Vector t1)) (OptionalColumn (b :: VB.Vector t2)) =
    case testEquality (typeRep @t1) (typeRep @t2) of
      Nothing -> False
      Just Refl -> a == b
  (==) (UnboxedColumn (a :: VU.Vector t1)) (UnboxedColumn (b :: VU.Vector t2)) =
    case testEquality (typeRep @t1) (typeRep @t2) of
      Nothing -> False
      Just Refl -> a == b
  -- Note: comparing grouped columns is expensive. We do this for stable tests
  -- but also you should probably aggregate grouped columns soon after creating them.
  (==) (GroupedBoxedColumn (a :: VB.Vector t1)) (GroupedBoxedColumn (b :: VB.Vector t2)) =
    case testEquality (typeRep @t1) (typeRep @t2) of
      Nothing -> False
      Just Refl -> VB.map (L.sort . VG.toList) a == VB.map (L.sort . VG.toList) b
  (==) (GroupedUnboxedColumn (a :: VB.Vector t1)) (GroupedUnboxedColumn (b :: VB.Vector t2)) =
    case testEquality (typeRep @t1) (typeRep @t2) of
      Nothing -> False
      Just Refl -> VB.map (L.sort . VG.toList) a == VB.map (L.sort . VG.toList) b
  (==) (GroupedOptionalColumn (a :: VB.Vector t1)) (GroupedOptionalColumn (b :: VB.Vector t2)) =
    case testEquality (typeRep @t1) (typeRep @t2) of
      Nothing -> False
      Just Refl -> VB.map (L.sort . VG.toList) a == VB.map (L.sort . VG.toList) b
  (==) _ _ = False

-- | A type with column representations used to select the
-- "right" representation when specializing the `toColumn` function.
data Rep
  = RBoxed
  | RUnboxed
  | ROptional
  | RGBoxed
  | RGUnboxed
  | RGOptional

-- | Type-level if statement.
type family If (cond :: Bool) (yes :: k) (no :: k) :: k where
  If 'True  yes _  = yes
  If 'False _   no = no

-- | All unboxable types (according to the `vector` package).
type family Unboxable (a :: Type) :: Bool where
  Unboxable Int    = 'True
  Unboxable Int8   = 'True
  Unboxable Int16  = 'True
  Unboxable Int32  = 'True
  Unboxable Int64  = 'True
  Unboxable Word   = 'True
  Unboxable Word8  = 'True
  Unboxable Word16 = 'True
  Unboxable Word32 = 'True
  Unboxable Word64 = 'True
  Unboxable Char   = 'True
  Unboxable Bool   = 'True
  Unboxable Double = 'True
  Unboxable Float  = 'True
  Unboxable _      = 'False

-- | Compute the column representation tag for any ‘a’.
type family KindOf a :: Rep where
  KindOf (Maybe a)     = 'ROptional
  KindOf (VB.Vector a) = 'RGBoxed
  KindOf (VU.Vector a) = 'RGUnboxed
  KindOf a             = If (Unboxable a) 'RUnboxed 'RBoxed

-- | A class for converting a vector to a column of the appropriate type.
-- Given each Rep we tell the `toColumnRep` function which Column type to pick.
class ColumnifyRep (r :: Rep) a where
  toColumnRep :: VB.Vector a -> Column

-- | Constraint synonym for what we can put into columns.
type Columnable a = (Columnable' a, ColumnifyRep (KindOf a) a, UnboxIf a, SBoolI (Unboxable a) )

instance (Columnable a, VU.Unbox a)
      => ColumnifyRep 'RUnboxed a where
  toColumnRep = UnboxedColumn . VU.convert

instance Columnable a
      => ColumnifyRep 'RBoxed a where
  toColumnRep = BoxedColumn

instance Columnable a
      => ColumnifyRep 'ROptional (Maybe a) where
  toColumnRep = OptionalColumn

instance Columnable a
      => ColumnifyRep 'RGBoxed (VB.Vector a) where
  toColumnRep = GroupedBoxedColumn

instance (Columnable a, VU.Unbox a)
      => ColumnifyRep 'RGUnboxed (VU.Vector a) where
  toColumnRep = GroupedUnboxedColumn

{- | O(n) Convert a vector to a column. Automatically picks the best representation of a vector to store the underlying data in.

__Examples:__

@
> import qualified Data.Vector as V
> fromVector (V.fromList [(1 :: Int), 2, 3, 4])
[1,2,3,4]
@
-}
fromVector ::
  forall a. (Columnable a, ColumnifyRep (KindOf a) a)
  => VB.Vector a -> Column
fromVector = toColumnRep @(KindOf a)

{- | O(n) Convert an unboxed vector to a column. This avoids the extra conversion if you already have the data in an unboxed vector.

__Examples:__

@
> import qualified Data.Vector.Unboxed as V
> fromUnboxedVector (V.fromList [(1 :: Int), 2, 3, 4])
[1,2,3,4]
@
-}
fromUnboxedVector :: forall a. (Columnable a, VU.Unbox a) => VU.Vector a -> Column
fromUnboxedVector = UnboxedColumn

{- | O(n) Convert a list to a column. Automatically picks the best representation of a vector to store the underlying data in.

__Examples:__

@
> fromList [(1 :: Int), 2, 3, 4]
[1,2,3,4]
@
-}
fromList ::
  forall a. (Columnable a, ColumnifyRep (KindOf a) a)
  => [a] -> Column
fromList = toColumnRep @(KindOf a) . VB.fromList

-- | Type-level boolean for constraint/type comparison.
data SBool (b :: Bool) where
  STrue  :: SBool 'True
  SFalse :: SBool 'False

-- | The runtime witness for our type-level branching.
class SBoolI (b :: Bool) where
  sbool :: SBool b

instance SBoolI 'True  where sbool = STrue
instance SBoolI 'False where sbool = SFalse

-- | Type-level function to determine whether or not a type is unboxa
sUnbox :: forall a. SBoolI (Unboxable a) => SBool (Unboxable a)
sUnbox = sbool @(Unboxable a)

type family When (flag :: Bool) (c :: Constraint) :: Constraint where
  When 'True  c = c
  When 'False c = ()          -- empty constraint

type UnboxIf a = When (Unboxable a) (VU.Unbox a)

-- | An internal function to map a function over the values of a column.
mapColumn
  :: forall b c.
     ( Columnable b
     , Columnable c
     , UnboxIf c)
  => (b -> c)
  -> Column
  -> Maybe Column
mapColumn f = \case
  BoxedColumn (col :: VB.Vector a)
    | Just Refl <- testEquality (typeRep @a) (typeRep @b)
    -> Just (fromVector @c (VB.map f col))
    | otherwise -> Nothing
  OptionalColumn (col :: VB.Vector a)
    | Just Refl <- testEquality (typeRep @a) (typeRep @b)
    -> Just (fromVector @c (VB.map f col))
    | otherwise -> Nothing
  UnboxedColumn (col :: VU.Vector a)
    | Just Refl <- testEquality (typeRep @a) (typeRep @b)
    -> Just $ case sUnbox @c of
                STrue  -> UnboxedColumn (VU.map f col)
                SFalse -> fromVector @c (VB.map f (VB.convert col))
    | otherwise -> Nothing
  GroupedBoxedColumn (col :: VB.Vector (VB.Vector a))
    | Just Refl <- testEquality (typeRep @(VB.Vector a)) (typeRep @b)
    -> Just (fromVector @c (VB.map f col))
    | otherwise -> Nothing
  GroupedUnboxedColumn (col :: VB.Vector (VU.Vector a))
    | Just Refl <- testEquality (typeRep @(VU.Vector a)) (typeRep @b)
    -> Just (fromVector @c (VB.map f col))
    | otherwise -> Nothing
  GroupedOptionalColumn (col :: VB.Vector (VB.Vector a))
    | Just Refl <- testEquality (typeRep @(VB.Vector a)) (typeRep @b)
    -> Just (fromVector @c (VB.map f col))
    | otherwise -> Nothing


-- | O(1) Gets the number of elements in the column.
columnLength :: Column -> Int
columnLength (BoxedColumn xs) = VG.length xs
columnLength (UnboxedColumn xs) = VG.length xs
columnLength (OptionalColumn xs) = VG.length xs
columnLength (GroupedBoxedColumn xs) = VG.length xs
columnLength (GroupedUnboxedColumn xs) = VG.length xs
columnLength (GroupedOptionalColumn xs) = VG.length xs
{-# INLINE columnLength #-}

-- | O(n) Takes the first n values of a column.
takeColumn :: Int -> Column -> Column
takeColumn n (BoxedColumn xs) = BoxedColumn $ VG.take n xs
takeColumn n (UnboxedColumn xs) = UnboxedColumn $ VG.take n xs
takeColumn n (OptionalColumn xs) = OptionalColumn $ VG.take n xs
takeColumn n (GroupedBoxedColumn xs) = GroupedBoxedColumn $ VG.take n xs
takeColumn n (GroupedUnboxedColumn xs) = GroupedUnboxedColumn $ VG.take n xs
takeColumn n (GroupedOptionalColumn xs) = GroupedOptionalColumn $ VG.take n xs
{-# INLINE takeColumn #-}

-- | O(n) Takes the last n values of a column.
takeLastColumn :: Int -> Column -> Column
takeLastColumn n column = sliceColumn (columnLength column - n) n column
{-# INLINE takeLastColumn #-}

-- | O(n) Takes n values after a given column index.
sliceColumn :: Int -> Int -> Column -> Column
sliceColumn start n (BoxedColumn xs) = BoxedColumn $ VG.slice start n xs
sliceColumn start n (UnboxedColumn xs) = UnboxedColumn $ VG.slice start n xs
sliceColumn start n (OptionalColumn xs) = OptionalColumn $ VG.slice start n xs
sliceColumn start n (GroupedBoxedColumn xs) = GroupedBoxedColumn $ VG.slice start n xs
sliceColumn start n (GroupedUnboxedColumn xs) = GroupedUnboxedColumn $ VG.slice start n xs
sliceColumn start n (GroupedOptionalColumn xs) = GroupedOptionalColumn $ VG.slice start n xs
{-# INLINE sliceColumn #-}

-- | O(n) Selects the elements at a given set of indices. May change the order.
atIndices :: S.Set Int -> Column -> Column
atIndices indexes (BoxedColumn column) = BoxedColumn $ VG.ifilter (\i _ -> i `S.member` indexes) column
atIndices indexes (OptionalColumn column) = OptionalColumn $ VG.ifilter (\i _ -> i `S.member` indexes) column
atIndices indexes (UnboxedColumn column) = UnboxedColumn $ VG.ifilter (\i _ -> i `S.member` indexes) column
atIndices indexes (GroupedBoxedColumn column) = GroupedBoxedColumn $ VG.ifilter (\i _ -> i `S.member` indexes) column
atIndices indexes (GroupedUnboxedColumn column) = GroupedUnboxedColumn $ VG.ifilter (\i _ -> i `S.member` indexes) column
atIndices indexes (GroupedOptionalColumn column) = GroupedOptionalColumn $ VG.ifilter (\i _ -> i `S.member` indexes) column
{-# INLINE atIndices #-}

-- | O(n) Selects the elements at a given set of indices. Does not change the order.
atIndicesStable :: VU.Vector Int -> Column -> Column
atIndicesStable indexes (BoxedColumn column) = BoxedColumn $ indexes `getIndices` column
atIndicesStable indexes (UnboxedColumn column) = UnboxedColumn $ indexes `getIndicesUnboxed` column
atIndicesStable indexes (OptionalColumn column) = OptionalColumn $ indexes `getIndices` column
atIndicesStable indexes (GroupedBoxedColumn column) = GroupedBoxedColumn $ indexes `getIndices` column
atIndicesStable indexes (GroupedUnboxedColumn column) = GroupedUnboxedColumn $ indexes `getIndices` column
atIndicesStable indexes (GroupedOptionalColumn column) = GroupedOptionalColumn $ indexes `getIndices` column
{-# INLINE atIndicesStable #-}

-- | Internal helper to get indices in a boxed vector.
getIndices :: VU.Vector Int -> VB.Vector a -> VB.Vector a
getIndices indices xs = VB.generate (VU.length indices) (\i -> xs VB.! (indices VU.! i))
{-# INLINE getIndices #-}

-- | Internal helper to get indices in an unboxed vector.
getIndicesUnboxed :: (VU.Unbox a) => VU.Vector Int -> VU.Vector a -> VU.Vector a
getIndicesUnboxed indices xs = VU.generate (VU.length indices) (\i -> xs VU.! (indices VU.! i))
{-# INLINE getIndicesUnboxed #-}

findIndices :: forall a. (Columnable a)
            => (a -> Bool)
            -> Column
            -> Maybe (VU.Vector Int)
findIndices pred (BoxedColumn (column :: VB.Vector b)) = do
  Refl <- testEquality (typeRep @a) (typeRep @b)
  pure $ VG.convert (VG.findIndices pred column)
findIndices pred (UnboxedColumn (column :: VU.Vector b)) = do
  Refl <- testEquality (typeRep @a) (typeRep @b)
  pure $ VG.findIndices pred column
findIndices pred (OptionalColumn (column :: VB.Vector (Maybe b))) = do
  Refl <- testEquality (typeRep @a) (typeRep @(Maybe b))
  pure $ VG.convert (VG.findIndices pred column)
findIndices pred (GroupedBoxedColumn (column :: VB.Vector b)) = do
  Refl <- testEquality (typeRep @a) (typeRep @b)
  pure $ VG.convert (VG.findIndices pred column)
findIndices pred (GroupedUnboxedColumn (column :: VB.Vector b)) = do
  Refl <- testEquality (typeRep @a) (typeRep @b)
  pure $ VG.convert (VG.findIndices pred column)

-- | An internal function that returns a vector of how indexes change after a column is sorted.
sortedIndexes :: Bool -> Column -> VU.Vector Int
sortedIndexes asc (BoxedColumn column ) = runST $ do
  withIndexes <- VG.thaw $ VG.indexed column
  VA.sortBy (\(a, b) (a', b') -> (if asc then compare else flip compare) b b') withIndexes
  sorted <- VG.unsafeFreeze withIndexes
  return $ VU.generate (VG.length column) (\i -> fst (sorted VG.! i))
sortedIndexes asc (UnboxedColumn column) = runST $ do
  withIndexes <- VG.thaw $ VG.indexed column
  VA.sortBy (\(a, b) (a', b') -> (if asc then compare else flip compare) b b') withIndexes
  sorted <- VG.unsafeFreeze withIndexes
  return $ VU.generate (VG.length column) (\i -> fst (sorted VG.! i))
sortedIndexes asc (OptionalColumn column ) = runST $ do
  withIndexes <- VG.thaw $ VG.indexed column
  VA.sortBy (\(a, b) (a', b') -> (if asc then compare else flip compare) b b') withIndexes
  sorted <- VG.unsafeFreeze withIndexes
  return $ VU.generate (VG.length column) (\i -> fst (sorted VG.! i))
sortedIndexes asc (GroupedBoxedColumn column) = runST $ do
  withIndexes <- VG.thaw $ VG.indexed column
  VA.sortBy (\(a, b) (a', b') -> (if asc then compare else flip compare) b b') withIndexes
  sorted <- VG.unsafeFreeze withIndexes
  return $ VU.generate (VG.length column) (\i -> fst (sorted VG.! i))
sortedIndexes asc (GroupedUnboxedColumn column) = runST $ do
  withIndexes <- VG.thaw $ VG.indexed column
  VA.sortBy (\(a, b) (a', b') -> (if asc then compare else flip compare) b b') withIndexes
  sorted <- VG.unsafeFreeze withIndexes
  return $ VU.generate (VG.length column) (\i -> fst (sorted VG.! i))
sortedIndexes asc (GroupedOptionalColumn column) = runST $ do
  withIndexes <- VG.thaw $ VG.indexed column
  VA.sortBy (\(a, b) (a', b') -> (if asc then compare else flip compare) b b') withIndexes
  sorted <- VG.unsafeFreeze withIndexes
  return $ VU.generate (VG.length column) (\i -> fst (sorted VG.! i))
{-# INLINE sortedIndexes #-}

-- | Applies a function that returns an unboxed result to an unboxed vector, storing the result in a column.
imapColumn
  :: forall b c. (Columnable b, Columnable c)
  => (Int -> b -> c) -> Column -> Maybe Column
imapColumn f = \case
  BoxedColumn (col :: VB.Vector a)
    | Just Refl <- testEquality (typeRep @a) (typeRep @b)
    -> Just (fromVector @c (VB.imap f col))
    | otherwise -> Nothing
  UnboxedColumn (col :: VU.Vector a)
    | Just Refl <- testEquality (typeRep @a) (typeRep @b)
    -> Just $
        case sUnbox @c of
          STrue  -> UnboxedColumn (VU.imap f col)
          SFalse -> fromVector @c (VB.imap f (VB.convert col))
    | otherwise -> Nothing
  OptionalColumn (col :: VB.Vector a)
    | Just Refl <- testEquality (typeRep @a) (typeRep @b)
    -> Just (fromVector @c (VB.imap f col))
    | otherwise -> Nothing
  GroupedBoxedColumn (col :: VB.Vector a)
    | Just Refl <- testEquality (typeRep @a) (typeRep @b)
    -> Just (fromVector @c (VB.imap f col))
    | otherwise -> Nothing
  GroupedUnboxedColumn (col :: VB.Vector a)
    | Just Refl <- testEquality (typeRep @a) (typeRep @b)
    -> Just (fromVector @c (VB.imap f col))
    | otherwise -> Nothing

-- | Filter column with index.
ifilterColumn :: forall a . (Columnable a) => (Int -> a -> Bool) -> Column -> Maybe Column
ifilterColumn f c@(BoxedColumn (column :: VB.Vector b)) = do
  Refl <- testEquality (typeRep @a) (typeRep @b)
  return $ BoxedColumn $ VG.ifilter f column
ifilterColumn f c@(UnboxedColumn (column :: VU.Vector b)) = do
  Refl <- testEquality (typeRep @a) (typeRep @b)
  return $ UnboxedColumn $ VG.ifilter f column
ifilterColumn f c@(GroupedBoxedColumn (column :: VB.Vector b)) = do
  Refl <- testEquality (typeRep @a) (typeRep @b)
  return $ GroupedBoxedColumn $ VG.ifilter f column
ifilterColumn f c@(GroupedUnboxedColumn (column :: VB.Vector b)) = do
  Refl <- testEquality (typeRep @a) (typeRep @b)
  return $ GroupedUnboxedColumn $ VG.ifilter f column

-- | Fold (right) column with index.
ifoldrColumn :: forall a b. (Columnable a, Columnable b) => (Int -> a -> b -> b) -> b -> Column -> Maybe b
ifoldrColumn f acc c@(BoxedColumn (column :: VB.Vector d)) = do
  Refl <- testEquality (typeRep @a) (typeRep @d)
  return $ VG.ifoldr f acc column
ifoldrColumn f acc c@(OptionalColumn (column :: VB.Vector d)) = do
  Refl <- testEquality (typeRep @a) (typeRep @d)
  return $ VG.ifoldr f acc column
ifoldrColumn f acc c@(UnboxedColumn (column :: VU.Vector d)) = do
  Refl <- testEquality (typeRep @a) (typeRep @d)
  return $ VG.ifoldr f acc column
ifoldrColumn f acc c@(GroupedBoxedColumn (column :: VB.Vector d)) = do
  Refl <- testEquality (typeRep @a) (typeRep @d)
  return $ VG.ifoldr f acc column
ifoldrColumn f acc c@(GroupedUnboxedColumn (column :: VB.Vector d)) = do
  Refl <- testEquality (typeRep @a) (typeRep @d)
  return $ VG.ifoldr f acc column

-- | Fold (left) column with index.
ifoldlColumn :: forall a b . (Columnable a, Columnable b) => (b -> Int -> a -> b) -> b -> Column -> Maybe b
ifoldlColumn f acc c@(BoxedColumn (column :: VB.Vector d)) = do
  Refl <- testEquality (typeRep @a) (typeRep @d)
  return $ VG.ifoldl' f acc column
ifoldlColumn f acc c@(OptionalColumn (column :: VB.Vector d)) = do
  Refl <- testEquality (typeRep @a) (typeRep @d)
  return $ VG.ifoldl' f acc column
ifoldlColumn f acc c@(UnboxedColumn (column :: VU.Vector d)) = do
  Refl <- testEquality (typeRep @a) (typeRep @d)
  return $ VG.ifoldl' f acc column
ifoldlColumn f acc c@(GroupedBoxedColumn (column :: VB.Vector d)) = do
  Refl <- testEquality (typeRep @a) (typeRep @d)
  return $ VG.ifoldl' f acc column
ifoldlColumn f acc c@(GroupedUnboxedColumn (column :: VB.Vector d)) = do
  Refl <- testEquality (typeRep @a) (typeRep @d)
  return $ VG.ifoldl' f acc column

-- | Generic reduce function for all Column types.
reduceColumn :: forall a b. Columnable a => (a -> b) -> Column -> Maybe b
{-# SPECIALIZE reduceColumn ::
    (VU.Vector (Double, Double) -> Double) -> Column -> Maybe Double,
    (VU.Vector Double -> Double) -> Column -> Maybe Double #-}
reduceColumn f (BoxedColumn (column :: c)) = do
  Refl <- testEquality (typeRep @c) (typeRep @a)
  pure $ f column
reduceColumn f (UnboxedColumn (column :: c)) = do
  Refl <- testEquality (typeRep @c) (typeRep @a)
  pure $ f column
reduceColumn f (OptionalColumn (column :: c)) = do
  Refl <- testEquality (typeRep @c) (typeRep @a)
  pure $ f column
reduceColumn _ _ = error "UNIMPLEMENTED"
{-# INLINE reduceColumn #-}

-- | An internal, column version of zip.
zipColumns :: Column -> Column -> Column
zipColumns (BoxedColumn column) (BoxedColumn other) = BoxedColumn (VG.zip column other)
zipColumns (BoxedColumn column) (UnboxedColumn other) = BoxedColumn (VB.generate (min (VG.length column) (VG.length other)) (\i -> (column VG.! i, other VG.! i)))
zipColumns (UnboxedColumn column) (BoxedColumn other) = BoxedColumn (VB.generate (min (VG.length column) (VG.length other)) (\i -> (column VG.! i, other VG.! i)))
zipColumns (UnboxedColumn column) (UnboxedColumn other) = UnboxedColumn (VG.zip column other)
{-# INLINE zipColumns #-}

-- | An internal, column version of zipWith.
zipWithColumns :: forall a b c . (Columnable a, Columnable b, Columnable c) => (a -> b -> c) -> Column -> Column -> Maybe Column
zipWithColumns f (UnboxedColumn (column :: VU.Vector d)) (UnboxedColumn (other :: VU.Vector e)) = case testEquality (typeRep @a) (typeRep @d) of
  Just Refl -> case testEquality (typeRep @b) (typeRep @e) of
    Just Refl -> pure $ case sUnbox @c of
                STrue  -> fromUnboxedVector (VU.zipWith f column other)
                SFalse -> fromVector $ VB.zipWith f (VG.convert column) (VG.convert other)
    Nothing -> Nothing
  Nothing -> Nothing
zipWithColumns f left right = let
    left' = toVector @a left
    right' = toVector @b right
  in pure $ fromVector $ VB.zipWith f left' right' 
{-# INLINE zipWithColumns #-}

-- Functions for mutable columns (intended for IO).
writeColumn :: Int -> T.Text -> Column -> IO (Either T.Text Bool)
writeColumn i value (MutableBoxedColumn (col :: VBM.IOVector a)) = let
  in case testEquality (typeRep @a) (typeRep @T.Text) of
      Just Refl -> (if isNullish value
                    then VBM.unsafeWrite col i "" >> return (Left $! value)
                    else VBM.unsafeWrite col i value >> return (Right True))
      Nothing -> return (Left value)
writeColumn i value (MutableUnboxedColumn (col :: VUM.IOVector a)) =
  case testEquality (typeRep @a) (typeRep @Int) of
      Just Refl -> case readInt value of
        Just v -> VUM.unsafeWrite col i v >> return (Right True)
        Nothing -> VUM.unsafeWrite col i 0 >> return (Left value)
      Nothing -> case testEquality (typeRep @a) (typeRep @Double) of
          Nothing -> return (Left $! value)
          Just Refl -> case readDouble value of
            Just v -> VUM.unsafeWrite col i v >> return (Right True)
            Nothing -> VUM.unsafeWrite col i 0 >> return (Left $! value)
{-# INLINE writeColumn #-}

freezeColumn' :: [(Int, T.Text)] -> Column -> IO Column
freezeColumn' nulls (MutableBoxedColumn col)
  | null nulls = BoxedColumn <$> VB.unsafeFreeze col
  | all (isNullish . snd) nulls = OptionalColumn . VB.imap (\i v -> if i `elem` map fst nulls then Nothing else Just v) <$> VB.unsafeFreeze col
  | otherwise  = BoxedColumn . VB.imap (\i v -> if i `elem` map fst nulls then Left (fromMaybe (error "") (lookup i nulls)) else Right v) <$> VB.unsafeFreeze col
freezeColumn' nulls (MutableUnboxedColumn col)
  | null nulls = UnboxedColumn <$> VU.unsafeFreeze col
  | all (isNullish . snd) nulls = VU.unsafeFreeze col >>= \c -> return $ OptionalColumn $ VB.generate (VU.length c) (\i -> if i `elem` map fst nulls then Nothing else Just (c VU.! i))
  | otherwise  = VU.unsafeFreeze col >>= \c -> return $ BoxedColumn $ VB.generate (VU.length c) (\i -> if i `elem` map fst nulls then Left (fromMaybe (error "") (lookup i nulls)) else Right (c VU.! i))
{-# INLINE freezeColumn' #-}

-- | Fills the end of a column, up to n, with Nothing. Does nothing if column has length greater than n.
expandColumn :: Int -> Column -> Column
expandColumn n (OptionalColumn col) = OptionalColumn $ col <> VB.replicate (n - VG.length col) Nothing
expandColumn n column@(BoxedColumn col)
  | n > VG.length col = OptionalColumn $ VB.map Just col <> VB.replicate (n - VG.length col) Nothing
  | otherwise         = column
expandColumn n column@(UnboxedColumn col)
  | n > VG.length col = OptionalColumn $ VB.map Just (VU.convert col) <> VB.replicate (n - VG.length col) Nothing
  | otherwise         = column
expandColumn n column@(GroupedBoxedColumn col)
  | n > VG.length col = GroupedBoxedColumn $ col <> VB.replicate (n - VG.length col) VB.empty
  | otherwise         = column
expandColumn n column@(GroupedUnboxedColumn col)
  | n > VG.length col = GroupedUnboxedColumn $ col <> VB.replicate (n - VG.length col) VU.empty
  | otherwise         = column

-- | Fills the beginning of a column, up to n, with Nothing. Does nothing if column has length greater than n.
leftExpandColumn :: Int -> Column -> Column
leftExpandColumn n column@(OptionalColumn col)
  | n > VG.length col = OptionalColumn $ VG.replicate (n - VG.length col) Nothing <> col
  | otherwise         = column
leftExpandColumn n column@(BoxedColumn col)
  | n > VG.length col = OptionalColumn $ VG.replicate (n - VG.length col) Nothing <> VG.map Just col
  | otherwise         = column
leftExpandColumn n column@(UnboxedColumn col)
  | n > VG.length col = OptionalColumn $ VG.replicate (n - VG.length col) Nothing <> VG.map Just (VU.convert col)
  | otherwise         = column
leftExpandColumn n column@(GroupedBoxedColumn col)
  | n > VG.length col = GroupedBoxedColumn $ VG.replicate (n - VG.length col) VB.empty <> col
  | otherwise         = column
leftExpandColumn n column@(GroupedUnboxedColumn col)
  | n > VG.length col = GroupedUnboxedColumn $ VG.replicate (n - VG.length col) VU.empty <> col
  | otherwise         = column

-- | Concatenates two columns.
concatColumns :: Column -> Column -> Maybe Column
concatColumns (OptionalColumn left) (OptionalColumn right) = case testEquality (typeOf left) (typeOf right) of
  Nothing   -> Nothing
  Just Refl -> Just (OptionalColumn $ left <> right)
concatColumns (BoxedColumn left) (BoxedColumn right) = case testEquality (typeOf left) (typeOf right) of
  Nothing   -> Nothing
  Just Refl -> Just (BoxedColumn $ left <> right)
concatColumns (UnboxedColumn left) (UnboxedColumn right) = case testEquality (typeOf left) (typeOf right) of
  Nothing   -> Nothing
  Just Refl -> Just (UnboxedColumn $ left <> right)
concatColumns (GroupedBoxedColumn left) (GroupedBoxedColumn right) = case testEquality (typeOf left) (typeOf right) of
  Nothing   -> Nothing
  Just Refl -> Just (GroupedBoxedColumn $ left <> right)
concatColumns (GroupedUnboxedColumn left) (GroupedUnboxedColumn right) = case testEquality (typeOf left) (typeOf right) of
  Nothing   -> Nothing
  Just Refl -> Just (GroupedUnboxedColumn $ left <> right)
concatColumns _ _ = Nothing

{- | O(n) Converts a column to a boxed vector. Throws an exception if the wrong type is specified.

__Examples:__

@
> column = fromList [(1 :: Int), 2, 3, 4]
> toVector @Int column
[1,2,3,4]
> toVector @Double column
exception: ...
-}
toVector :: forall a . Columnable a => Column -> VB.Vector a
toVector xs = case toVectorSafe xs of
  Left err  -> throw err
  Right val -> val

{- | O(n) Converts a column to a list. Throws an exception if the wrong type is specified.

__Examples:__

@
> column = fromList [(1 :: Int), 2, 3, 4]
> toList @Int column
[1,2,3,4]
> toList @Double column
exception: ...
-}
toList :: forall a . Columnable a => Column -> [a]
toList xs = case toVectorSafe xs of
  Left err  -> throw err
  Right val -> VG.toList val

-- | A safe version of toVector that returns an Either type.
toVectorSafe :: forall a . Columnable a => Column -> Either DataFrameException (VB.Vector a)
toVectorSafe column@(OptionalColumn (col :: VB.Vector b)) =
  case testEquality (typeRep @a) (typeRep @b) of
    Just Refl -> Right col
    Nothing -> Left $ TypeMismatchException (MkTypeErrorContext { userType = Right (typeRep @a)
                                                                 , expectedType = Right (typeRep @b)
                                                                 , callingFunctionName = Just "toVectorSafe"
                                                                 , errorColumnName = Nothing})
toVectorSafe (BoxedColumn (col :: VB.Vector b)) =
  case testEquality (typeRep @a) (typeRep @b) of
    Just Refl -> Right col
    Nothing -> Left $ TypeMismatchException (MkTypeErrorContext { userType = Right (typeRep @a)
                                                                 , expectedType = Right (typeRep @b)
                                                                 , callingFunctionName = Just "toVectorSafe"
                                                                 , errorColumnName = Nothing})
toVectorSafe (UnboxedColumn (col :: VU.Vector b)) =
  case testEquality (typeRep @a) (typeRep @b) of
    Just Refl -> Right $ VB.convert col
    Nothing -> Left $ TypeMismatchException (MkTypeErrorContext { userType = Right (typeRep @a)
                                                                 , expectedType = Right (typeRep @b)
                                                                 , callingFunctionName = Just "toVectorSafe"
                                                                 , errorColumnName = Nothing})
toVectorSafe (GroupedBoxedColumn (col :: VB.Vector b)) =
  case testEquality (typeRep @a) (typeRep @b) of
    Just Refl -> Right col
    Nothing -> Left $ TypeMismatchException (MkTypeErrorContext { userType = Right (typeRep @a)
                                                                 , expectedType = Right (typeRep @b)
                                                                 , callingFunctionName = Just "toVectorSafe"
                                                                 , errorColumnName = Nothing})
toVectorSafe (GroupedUnboxedColumn (col :: VB.Vector b)) =
  case testEquality (typeRep @a) (typeRep @b) of
    Just Refl -> Right col
    Nothing -> Left $ TypeMismatchException (MkTypeErrorContext { userType = Right (typeRep @a)
                                                                 , expectedType = Right (typeRep @b)
                                                                 , callingFunctionName = Just "toVectorSafe"
                                                                 , errorColumnName = Nothing})
