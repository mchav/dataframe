{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StrictData #-}
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

-- | Our representation of a column is a GADT that can store data in either
-- a vector with boxed elements or
data Column where
  BoxedColumn :: Columnable a => VB.Vector a -> Column
  UnboxedColumn :: (Columnable a, VU.Unbox a) => VU.Vector a -> Column
  OptionalColumn :: Columnable a => VB.Vector (Maybe a) -> Column
  GroupedBoxedColumn :: Columnable a => VB.Vector (VB.Vector a) -> Column
  GroupedUnboxedColumn :: (Columnable a, VU.Unbox a) => VB.Vector (VU.Vector a) -> Column
  GroupedOptionalColumn :: (Columnable a) => VB.Vector (VB.Vector (Maybe a)) -> Column
  MutableBoxedColumn :: Columnable a => VBM.IOVector a -> Column
  MutableUnboxedColumn :: (Columnable a, VU.Unbox a) => VUM.IOVector a -> Column

data TypedColumn a where
  TColumn :: Columnable a => Column -> TypedColumn a

unwrapTypedColumn :: TypedColumn a -> Column
unwrapTypedColumn (TColumn value) = value

-- Functions about column metadata.
isGrouped :: Column -> Bool
isGrouped (GroupedBoxedColumn column) = True
isGrouped (GroupedUnboxedColumn column) = True
isGrouped _ = False

columnVersionString :: Column -> String
columnVersionString column = case column of
  BoxedColumn _ -> "Boxed"
  UnboxedColumn _ -> "Unboxed"
  OptionalColumn _ -> "Optional"
  GroupedBoxedColumn _ -> "Grouped Boxed"
  GroupedUnboxedColumn _ -> "Grouped Unboxed"
  GroupedOptionalColumn _ -> "Grouped Optional"

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

data Rep
  = RBoxed
  | RUnboxed
  | ROptional
  | RGBoxed
  | RGUnboxed
  | RGOptional

type family If (cond :: Bool) (yes :: k) (no :: k) :: k where
  If 'True  yes _  = yes
  If 'False _   no = no

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

class ColumnifyRep (r :: Rep) a where
  toColumnRep :: VB.Vector a -> Column

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

toColumn' ::
  forall a. (Columnable a, ColumnifyRep (KindOf a) a)
  => VB.Vector a -> Column
toColumn' = toColumnRep @(KindOf a)

toColumn ::
  forall a. (Columnable a, ColumnifyRep (KindOf a) a)
  => [a] -> Column
toColumn = toColumnRep @(KindOf a) . VB.fromList

data SBool (b :: Bool) where
  STrue  :: SBool 'True
  SFalse :: SBool 'False

class SBoolI (b :: Bool) where
  sbool :: SBool b          -- the run-time witness

instance SBoolI 'True  where sbool = STrue
instance SBoolI 'False where sbool = SFalse

sUnbox :: forall a. SBoolI (Unboxable a) => SBool (Unboxable a)
sUnbox = sbool @(Unboxable a)

type family When (flag :: Bool) (c :: Constraint) :: Constraint where
  When 'True  c = c
  When 'False c = ()          -- empty constraint

type UnboxIf a = When (Unboxable a) (VU.Unbox a)

-- | Generic column transformation (no index).
transform
  :: forall b c.
     ( Columnable b
     , Columnable c
     , UnboxIf c
     , Typeable b
     , Typeable c )
  => (b -> c)
  -> Column
  -> Maybe Column
transform f = \case
  BoxedColumn (col :: VB.Vector a)
    | Just Refl <- testEquality (typeRep @a) (typeRep @b)
    -> Just (toColumn' @c (VB.map f col))
    | otherwise -> Nothing
  OptionalColumn (col :: VB.Vector a)
    | Just Refl <- testEquality (typeRep @a) (typeRep @b)
    -> Just (toColumn' @c (VB.map f col))
    | otherwise -> Nothing
  UnboxedColumn (col :: VU.Vector a)
    | Just Refl <- testEquality (typeRep @a) (typeRep @b)
    -> Just $ case sUnbox @c of
                STrue  -> UnboxedColumn (VU.map f col)
                SFalse -> toColumn' @c (VB.map f (VB.convert col))
    | otherwise -> Nothing
  GroupedBoxedColumn (col :: VB.Vector (VB.Vector a))
    | Just Refl <- testEquality (typeRep @(VB.Vector a)) (typeRep @b)
    -> Just (toColumn' @c (VB.map f col))
    | otherwise -> Nothing
  GroupedUnboxedColumn (col :: VB.Vector (VU.Vector a))
    | Just Refl <- testEquality (typeRep @(VU.Vector a)) (typeRep @b)
    -> Just (toColumn' @c (VB.map f col))
    | otherwise -> Nothing
  GroupedOptionalColumn (col :: VB.Vector (VB.Vector a))
    | Just Refl <- testEquality (typeRep @(VB.Vector a)) (typeRep @b)
    -> Just (toColumn' @c (VB.map f col))
    | otherwise -> Nothing

-- | Converts a an unboxed vector to a column making sure to put
-- the vector into an appropriate column type by reflection on the
-- vector's type parameter.
toColumnUnboxed :: forall a. (Columnable a, VU.Unbox a) => VU.Vector a -> Column
toColumnUnboxed = UnboxedColumn

-- Functions that don't depend on column type.
-- | O(1) Gets the number of elements in the column.
columnLength :: Column -> Int
columnLength (BoxedColumn xs) = VG.length xs
columnLength (UnboxedColumn xs) = VG.length xs
columnLength (OptionalColumn xs) = VG.length xs
columnLength (GroupedBoxedColumn xs) = VG.length xs
columnLength (GroupedUnboxedColumn xs) = VG.length xs
columnLength (GroupedOptionalColumn xs) = VG.length xs
{-# INLINE columnLength #-}

takeColumn :: Int -> Column -> Column
takeColumn n (BoxedColumn xs) = BoxedColumn $ VG.take n xs
takeColumn n (UnboxedColumn xs) = UnboxedColumn $ VG.take n xs
takeColumn n (OptionalColumn xs) = OptionalColumn $ VG.take n xs
takeColumn n (GroupedBoxedColumn xs) = GroupedBoxedColumn $ VG.take n xs
takeColumn n (GroupedUnboxedColumn xs) = GroupedUnboxedColumn $ VG.take n xs
takeColumn n (GroupedOptionalColumn xs) = GroupedOptionalColumn $ VG.take n xs
{-# INLINE takeColumn #-}

-- TODO: Maybe we can remvoe all this boilerplate and make
-- transform take in a generic vector function.
takeLastColumn :: Int -> Column -> Column
takeLastColumn n column = sliceColumn (columnLength column - n) n column
{-# INLINE takeLastColumn #-}

sliceColumn :: Int -> Int -> Column -> Column
sliceColumn start n (BoxedColumn xs) = BoxedColumn $ VG.slice start n xs
sliceColumn start n (UnboxedColumn xs) = UnboxedColumn $ VG.slice start n xs
sliceColumn start n (OptionalColumn xs) = OptionalColumn $ VG.slice start n xs
sliceColumn start n (GroupedBoxedColumn xs) = GroupedBoxedColumn $ VG.slice start n xs
sliceColumn start n (GroupedUnboxedColumn xs) = GroupedUnboxedColumn $ VG.slice start n xs
sliceColumn start n (GroupedOptionalColumn xs) = GroupedOptionalColumn $ VG.slice start n xs
{-# INLINE sliceColumn #-}

-- TODO: We can probably generalize this to `applyVectorFunction`.
atIndices :: S.Set Int -> Column -> Column
atIndices indexes (BoxedColumn column) = BoxedColumn $ VG.ifilter (\i _ -> i `S.member` indexes) column
atIndices indexes (OptionalColumn column) = OptionalColumn $ VG.ifilter (\i _ -> i `S.member` indexes) column
atIndices indexes (UnboxedColumn column) = UnboxedColumn $ VG.ifilter (\i _ -> i `S.member` indexes) column
atIndices indexes (GroupedBoxedColumn column) = GroupedBoxedColumn $ VG.ifilter (\i _ -> i `S.member` indexes) column
atIndices indexes (GroupedUnboxedColumn column) = GroupedUnboxedColumn $ VG.ifilter (\i _ -> i `S.member` indexes) column
atIndices indexes (GroupedOptionalColumn column) = GroupedOptionalColumn $ VG.ifilter (\i _ -> i `S.member` indexes) column
{-# INLINE atIndices #-}

atIndicesStable :: VU.Vector Int -> Column -> Column
atIndicesStable indexes (BoxedColumn column) = BoxedColumn $ indexes `getIndices` column
atIndicesStable indexes (UnboxedColumn column) = UnboxedColumn $ indexes `getIndicesUnboxed` column
atIndicesStable indexes (OptionalColumn column) = OptionalColumn $ indexes `getIndices` column
atIndicesStable indexes (GroupedBoxedColumn column) = GroupedBoxedColumn $ indexes `getIndices` column
atIndicesStable indexes (GroupedUnboxedColumn column) = GroupedUnboxedColumn $ indexes `getIndices` column
atIndicesStable indexes (GroupedOptionalColumn column) = GroupedOptionalColumn $ indexes `getIndices` column
{-# INLINE atIndicesStable #-}

getIndices :: VU.Vector Int -> VB.Vector a -> VB.Vector a
getIndices indices xs = VB.generate (VU.length indices) (\i -> xs VB.! (indices VU.! i))
{-# INLINE getIndices #-}

getIndicesUnboxed :: (VU.Unbox a) => VU.Vector Int -> VU.Vector a -> VU.Vector a
getIndicesUnboxed indices xs = VU.generate (VU.length indices) (\i -> xs VU.! (indices VU.! i))
{-# INLINE getIndicesUnboxed #-}

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
itransform
  :: forall b c. (Typeable b, Typeable c, Columnable b, Columnable c)
  => (Int -> b -> c) -> Column -> Maybe Column
itransform f = \case
  BoxedColumn (col :: VB.Vector a)
    | Just Refl <- testEquality (typeRep @a) (typeRep @b)
    -> Just (toColumn' @c (VB.imap f col))
    | otherwise -> Nothing
  UnboxedColumn (col :: VU.Vector a)
    | Just Refl <- testEquality (typeRep @a) (typeRep @b)
    -> Just $
        case sUnbox @c of
          STrue  -> UnboxedColumn (VU.imap f col)
          SFalse -> toColumn' @c (VB.imap f (VB.convert col))
    | otherwise -> Nothing
  OptionalColumn (col :: VB.Vector a)
    | Just Refl <- testEquality (typeRep @a) (typeRep @b)
    -> Just (toColumn' @c (VB.imap f col))
    | otherwise -> Nothing
  GroupedBoxedColumn (col :: VB.Vector a)
    | Just Refl <- testEquality (typeRep @a) (typeRep @b)
    -> Just (toColumn' @c (VB.imap f col))
    | otherwise -> Nothing
  GroupedUnboxedColumn (col :: VB.Vector a)
    | Just Refl <- testEquality (typeRep @a) (typeRep @b)
    -> Just (toColumn' @c (VB.imap f col))
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

reduceColumn :: forall a b. Columnable a => (a -> b) -> Column -> b
{-# SPECIALIZE reduceColumn ::
    (VU.Vector (Double, Double) -> Double) -> Column -> Double,
    (VU.Vector Double -> Double) -> Column -> Double #-}
reduceColumn f (BoxedColumn (column :: c)) = case testEquality (typeRep @c) (typeRep @a) of
  Just Refl -> f column
  Nothing -> error $ "Can't reduce. Incompatible types: " ++ show (typeRep @a) ++ " " ++ show (typeRep @a)
reduceColumn f (UnboxedColumn (column :: c)) = case testEquality (typeRep @c) (typeRep @a) of
  Just Refl -> f column
  Nothing -> error $ "Can't reduce. Incompatible types: " ++ show (typeRep @a) ++ " " ++ show (typeRep @a)
reduceColumn f (OptionalColumn (column :: c)) = case testEquality (typeRep @c) (typeRep @a) of
  Just Refl -> f column
  Nothing -> error $ "Can't reduce. Incompatible types: " ++ show (typeRep @a) ++ " " ++ show (typeRep @a)
{-# INLINE reduceColumn #-}

safeReduceColumn :: forall a b. (Typeable a) => (a -> b) -> Column -> Maybe b
safeReduceColumn f (BoxedColumn (column :: c)) = do
  Refl <- testEquality (typeRep @c) (typeRep @a)
  return $! f column
safeReduceColumn f (UnboxedColumn (column :: c)) = do
  Refl <- testEquality (typeRep @c) (typeRep @a)
  return $! f column
safeReduceColumn f (OptionalColumn (column :: c)) = do
  Refl <- testEquality (typeRep @c) (typeRep @a)
  return $! f column
{-# INLINE safeReduceColumn #-}

zipColumns :: Column -> Column -> Column
zipColumns (BoxedColumn column) (BoxedColumn other) = BoxedColumn (VG.zip column other)
zipColumns (BoxedColumn column) (UnboxedColumn other) = BoxedColumn (VB.generate (min (VG.length column) (VG.length other)) (\i -> (column VG.! i, other VG.! i)))
zipColumns (UnboxedColumn column) (BoxedColumn other) = BoxedColumn (VB.generate (min (VG.length column) (VG.length other)) (\i -> (column VG.! i, other VG.! i)))
zipColumns (UnboxedColumn column) (UnboxedColumn other) = UnboxedColumn (VG.zip column other)
{-# INLINE zipColumns #-}

zipWithColumns :: forall a b c . (Columnable a, Columnable b, Columnable c) => (a -> b -> c) -> Column -> Column -> Column
zipWithColumns f (UnboxedColumn (column :: VU.Vector d)) (UnboxedColumn (other :: VU.Vector e)) = case testEquality (typeRep @a) (typeRep @d) of
  Just Refl -> case testEquality (typeRep @b) (typeRep @e) of
    Just Refl -> toColumn' $ VB.zipWith f (VG.convert column) (VG.convert other)
    Nothing -> throw $ TypeMismatchException' (typeRep @b) (show $ typeRep @e) "" "zipWithColumns"
  Nothing -> throw $ TypeMismatchException' (typeRep @a) (show $ typeRep @d) "" "zipWithColumns"
zipWithColumns f left right = let
    left' = toVector @a left
    right' = toVector @b right
  in toColumn' $ VB.zipWith f left' right' 
{-# INLINE zipWithColumns #-}

-- Functions for mutable columns (intended for IO).
-- Clean this up.
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

expandColumn :: Int -> Column -> Column
expandColumn n (OptionalColumn col) = OptionalColumn $ col <> VB.replicate n Nothing
expandColumn n (BoxedColumn col) = OptionalColumn $ VB.map Just col <> VB.replicate n Nothing
expandColumn n (UnboxedColumn col) = OptionalColumn $ VB.map Just (VU.convert col) <> VB.replicate n Nothing
expandColumn n (GroupedBoxedColumn col) = GroupedBoxedColumn $ col <> VB.replicate n VB.empty
expandColumn n (GroupedUnboxedColumn col) = GroupedUnboxedColumn $ col <> VB.replicate n VU.empty

leftExpandColumn :: Int -> Column -> Column
leftExpandColumn n (OptionalColumn col) = OptionalColumn $ VB.replicate n Nothing <> col
leftExpandColumn n (BoxedColumn col) = OptionalColumn $ VB.replicate n Nothing <> VB.map Just col
leftExpandColumn n (UnboxedColumn col) = OptionalColumn $ VB.replicate n Nothing <> VB.map Just (VU.convert col)
leftExpandColumn n (GroupedBoxedColumn col) = GroupedBoxedColumn $ VB.replicate n VB.empty <> col
leftExpandColumn n (GroupedUnboxedColumn col) = GroupedUnboxedColumn $ VB.replicate n VU.empty <> col

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

toVector :: forall a . Columnable a => Column -> VB.Vector a
toVector column@(OptionalColumn (col :: VB.Vector b)) =
  case testEquality (typeRep @a) (typeRep @b) of
    Just Refl -> col
    Nothing -> throw $ TypeMismatchException' (typeRep @b) (show $ typeRep @a) "" "toVector"
toVector (BoxedColumn (col :: VB.Vector b)) =
  case testEquality (typeRep @a) (typeRep @b) of
    Just Refl -> col
    Nothing -> throw $ TypeMismatchException' (typeRep @b) (show $ typeRep @a) "" "toVector"
toVector (UnboxedColumn (col :: VU.Vector b)) =
  case testEquality (typeRep @a) (typeRep @b) of
    Just Refl -> VB.convert col
    Nothing -> throw $ TypeMismatchException' (typeRep @b) (show $ typeRep @a) "" "toVector"
toVector (GroupedBoxedColumn (col :: VB.Vector b)) =
  case testEquality (typeRep @a) (typeRep @b) of
    Just Refl -> col
    Nothing -> throw $ TypeMismatchException' (typeRep @b) (show $ typeRep @a) "" "toVector"
toVector (GroupedUnboxedColumn (col :: VB.Vector b)) =
  case testEquality (typeRep @a) (typeRep @b) of
    Just Refl -> col
    Nothing -> throw $ TypeMismatchException' (typeRep @b) (show $ typeRep @a) "" "toVector"
