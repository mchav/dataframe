{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
module Data.DataFrame.Internal.Column where

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
import Data.DataFrame.Internal.Function
import Data.DataFrame.Internal.Types
import Data.DataFrame.Internal.Parsing
import Data.Int
import Data.Maybe
import Data.Text.Encoding (decodeUtf8Lenient)
import Data.Type.Equality (type (:~:)(Refl), TestEquality (..))
import Data.Typeable (Typeable)
import Data.Word
import Type.Reflection (typeRep)

-- | Our representation of a column is a GADT that can store data in either
-- a vector with boxed elements or
data Column where
  BoxedColumn :: Columnable a => VB.Vector a -> Column
  UnboxedColumn :: (Columnable a, VU.Unbox a) => VU.Vector a -> Column
  GroupedBoxedColumn :: Columnable a => VB.Vector (VB.Vector a) -> Column
  GroupedUnboxedColumn :: (Columnable a, VU.Unbox a) => VB.Vector (VU.Vector a) -> Column
  MutableBoxedColumn :: Columnable a => VBM.IOVector a -> Column
  MutableUnboxedColumn :: (Columnable a, VU.Unbox a) => VUM.IOVector a -> Column

-- Functions about column metadata.
isGrouped :: Column -> Bool
isGrouped (GroupedBoxedColumn column) = True
isGrouped (GroupedUnboxedColumn column) = True
isGrouped _ = False

columnTypeString :: Column -> String
columnTypeString column = case column of
  BoxedColumn (column :: VB.Vector a) -> show (typeRep @a)
  UnboxedColumn (column :: VU.Vector a) -> show (typeRep @a)
  GroupedBoxedColumn (column :: VB.Vector a) -> show (typeRep @a)
  GroupedUnboxedColumn (column :: VB.Vector a) -> show (typeRep @a)

instance Show Column where
  show :: Column -> String
  show (BoxedColumn column) = show column
  show (UnboxedColumn column) = show column
  show (GroupedBoxedColumn column) = show column

instance Eq Column where
  (==) :: Column -> Column -> Bool
  (==) (BoxedColumn (a :: VB.Vector t1)) (BoxedColumn (b :: VB.Vector t2)) =
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
  (==) _ _ = False

-- | Converts a boxed vector to a column making sure to put
-- the vector into an appropriate column type by reflection on the
-- vector's type parameter.
toColumn' :: forall a. (Columnable a) => VB.Vector a -> Column
toColumn' xs = case testEquality (typeRep @a) (typeRep @Int) of
  Just Refl -> UnboxedColumn (VU.convert xs)
  Nothing -> case testEquality (typeRep @a) (typeRep @Double) of
    Just Refl -> UnboxedColumn (VU.convert xs)
    Nothing -> case testEquality (typeRep @a) (typeRep @Float) of
      Just Refl -> UnboxedColumn (VU.convert xs)
      Nothing -> BoxedColumn xs

toColumn :: forall a. (Columnable a) => [a] -> Column
toColumn = toColumn' . VB.fromList

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
columnLength (GroupedBoxedColumn xs) = VG.length xs
columnLength (GroupedUnboxedColumn xs) = VG.length xs
{-# INLINE columnLength #-}

takeColumn :: Int -> Column -> Column
takeColumn n (BoxedColumn xs) = BoxedColumn $ VG.take n xs
takeColumn n (UnboxedColumn xs) = UnboxedColumn $ VG.take n xs
takeColumn n (GroupedBoxedColumn xs) = GroupedBoxedColumn $ VG.take n xs
takeColumn n (GroupedUnboxedColumn xs) = GroupedUnboxedColumn $ VG.take n xs
{-# INLINE takeColumn #-}

-- TODO: Maybe we can remvoe all this boilerplate and make
-- transform take in a generic vector function.
takeLastColumn :: Int -> Column -> Column
takeLastColumn n column = sliceColumn (columnLength column - n) n column
{-# INLINE takeLastColumn #-}

sliceColumn :: Int -> Int -> Column -> Column
sliceColumn start n (BoxedColumn xs) = BoxedColumn $ VG.slice start n xs
sliceColumn start n (UnboxedColumn xs) = UnboxedColumn $ VG.slice start n xs
sliceColumn start n (GroupedBoxedColumn xs) = GroupedBoxedColumn $ VG.slice start n xs
sliceColumn start n (GroupedUnboxedColumn xs) = GroupedUnboxedColumn $ VG.slice start n xs
{-# INLINE sliceColumn #-}

-- TODO: We can probably generalize this to `applyVectorFunction`.
atIndices :: S.Set Int -> Column -> Column
atIndices indexes (BoxedColumn column) = BoxedColumn $ VG.ifilter (\i _ -> i `S.member` indexes) column
atIndices indexes (UnboxedColumn column) = UnboxedColumn $ VG.ifilter (\i _ -> i `S.member` indexes) column
atIndices indexes (GroupedBoxedColumn column) = GroupedBoxedColumn $ VG.ifilter (\i _ -> i `S.member` indexes) column
atIndices indexes (GroupedUnboxedColumn column) = GroupedUnboxedColumn $ VG.ifilter (\i _ -> i `S.member` indexes) column
{-# INLINE atIndices #-}

atIndicesStable :: VU.Vector Int -> Column -> Column
atIndicesStable indexes (BoxedColumn column) = BoxedColumn $ indexes `getIndices` column
atIndicesStable indexes (UnboxedColumn column) = UnboxedColumn $ indexes `getIndicesUnboxed` column
atIndicesStable indexes (GroupedBoxedColumn column) = GroupedBoxedColumn $ indexes `getIndices` column
atIndicesStable indexes (GroupedUnboxedColumn column) = GroupedUnboxedColumn $ indexes `getIndices` column
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
{-# INLINE sortedIndexes #-}

-- Operations on a column that may change its type.

instance Transformable Column where
  transform :: forall b c . (Columnable b, Columnable c) => (b -> c) -> Column -> Maybe Column
  transform f (BoxedColumn (column :: VB.Vector a)) = do
    Refl <- testEquality (typeRep @a) (typeRep @b)
    return (toColumn' (VG.map f column))
  transform f (UnboxedColumn (column :: VU.Vector a)) = do
    Refl <- testEquality (typeRep @a) (typeRep @b)
    return $ if testUnboxable (typeRep @c) then transformUnboxed f column else toColumn' (VG.map f (VB.convert column))
  transform f (GroupedBoxedColumn (column :: VB.Vector (VB.Vector a))) = do
    Refl <- testEquality (typeRep @(VB.Vector a)) (typeRep @b)
    return (toColumn' (VG.map f column))
  transform f (GroupedUnboxedColumn (column :: VB.Vector (VU.Vector a))) = do
    Refl <- testEquality (typeRep @(VU.Vector a)) (typeRep @b)
    return (toColumn' (VG.map f column))

-- | Applies a function that returns an unboxed result to an unboxed vector, storing the result in a column.
transformUnboxed :: forall a b . (Columnable a, VU.Unbox a, Columnable b) => (a -> b) -> VU.Vector a -> Column
transformUnboxed f = itransformUnboxed (const f)

itransformUnboxed :: forall a b . (Columnable a, VU.Unbox a, Columnable b) => (Int -> a -> b) -> VU.Vector a -> Column
itransformUnboxed f column = case testEquality (typeRep @b) (typeRep @Int) of
  Just Refl -> UnboxedColumn $ VG.imap f column
  Nothing -> case testEquality (typeRep @b) (typeRep @Int8) of
    Just Refl -> UnboxedColumn $ VG.imap f column
    Nothing -> case testEquality (typeRep @b) (typeRep @Int16) of
      Just Refl -> UnboxedColumn $ VG.imap f column
      Nothing -> case testEquality (typeRep @b) (typeRep @Int32) of
        Just Refl -> UnboxedColumn $ VG.imap f column
        Nothing -> case testEquality (typeRep @b) (typeRep @Int64) of
          Just Refl -> UnboxedColumn $ VG.imap f column
          Nothing -> case testEquality (typeRep @b) (typeRep @Word8) of
            Just Refl -> UnboxedColumn $ VG.imap f column
            Nothing-> case testEquality (typeRep @b) (typeRep @Word16) of
              Just Refl -> UnboxedColumn $ VG.imap f column
              Nothing -> case testEquality (typeRep @b) (typeRep @Word32) of
                Just Refl -> UnboxedColumn $ VG.imap f column
                Nothing -> case testEquality (typeRep @b) (typeRep @Word64) of
                  Just Refl -> UnboxedColumn $ VG.imap f column
                  Nothing -> case testEquality (typeRep @b) (typeRep @Char) of
                    Just Refl -> UnboxedColumn $ VG.imap f column
                    Nothing -> case testEquality (typeRep @b) (typeRep @Bool) of
                      Just Refl -> UnboxedColumn $ VG.imap f column
                      Nothing -> case testEquality (typeRep @b) (typeRep @Float) of
                        Just Refl -> UnboxedColumn $ VG.imap f column
                        Nothing -> case testEquality (typeRep @b) (typeRep @Double) of
                          Just Refl -> UnboxedColumn $ VG.imap f column
                          Nothing -> case testEquality (typeRep @b) (typeRep @Word) of
                            Just Refl -> UnboxedColumn $ VG.imap f column
                            Nothing -> error "Result type is unboxed" -- since we only call this after confirming 

-- | tranform with index.
itransform :: forall b c. (Columnable b, Columnable c) => (Int -> b -> c) -> Column -> Maybe Column
itransform f (BoxedColumn (column :: VB.Vector a)) = do
  Refl <- testEquality (typeRep @a) (typeRep @b)
  return (toColumn' (VG.imap f column))
itransform f (UnboxedColumn (column :: VU.Vector a)) = do
  Refl <- testEquality (typeRep @a) (typeRep @b)
  return $ if testUnboxable (typeRep @c) then itransformUnboxed f column else toColumn' (VG.imap f (VB.convert column))
itransform f (GroupedBoxedColumn (column :: VB.Vector (VB.Vector a))) = do
  Refl <- testEquality (typeRep @(VB.Vector a)) (typeRep @b)
  return (toColumn' (VG.imap f column))
itransform f (GroupedUnboxedColumn (column :: VB.Vector (VU.Vector a))) = do
  Refl <- testEquality (typeRep @(VU.Vector a)) (typeRep @b)
  return (toColumn' (VG.imap f column))

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

-- TODO: Expand this to use more predicates.
ifilterColumnF :: Function -> Column -> Maybe Column
ifilterColumnF (ICond (f :: Int -> a -> Bool)) c@(BoxedColumn (column :: VB.Vector b)) = do
  Refl <- testEquality (typeRep @a) (typeRep @b)
  return $ BoxedColumn $ VG.ifilter f column
ifilterColumnF (ICond (f :: Int -> a -> Bool)) c@(UnboxedColumn (column :: VU.Vector b)) = do
  Refl <- testEquality (typeRep @a) (typeRep @b)
  return $ UnboxedColumn $ VG.ifilter f column
ifilterColumnF (ICond (f :: Int -> a -> Bool)) c@(GroupedBoxedColumn (column :: VB.Vector b)) = do
  Refl <- testEquality (typeRep @a) (typeRep @b)
  return $ GroupedBoxedColumn $ VG.ifilter f column
ifilterColumnF (ICond (f :: Int -> a -> Bool)) c@(GroupedUnboxedColumn (column :: VB.Vector b)) = do
  Refl <- testEquality (typeRep @a) (typeRep @b)
  return $ GroupedUnboxedColumn $ VG.ifilter f column

ifoldrColumn :: forall a b. (Columnable a, Columnable b) => (Int -> a -> b -> b) -> b -> Column -> Maybe b
ifoldrColumn f acc c@(BoxedColumn (column :: VB.Vector d)) = do
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
{-# INLINE reduceColumn #-}

safeReduceColumn :: forall a b. (Typeable a) => (a -> b) -> Column -> Maybe b
safeReduceColumn f (BoxedColumn (column :: c)) = do
  Refl <- testEquality (typeRep @c) (typeRep @a)
  return $ f column
safeReduceColumn f (UnboxedColumn (column :: c)) = do
  Refl <- testEquality (typeRep @c) (typeRep @a)
  return $ f column
{-# INLINE safeReduceColumn #-}

longZipColumns :: Column -> Column -> Column
longZipColumns (BoxedColumn column) (BoxedColumn other) = BoxedColumn (VB.generate (max (VG.length column) (VG.length other)) (\i -> (column VG.!? i, other VG.!? i)))
longZipColumns (BoxedColumn column) (UnboxedColumn other) = BoxedColumn (VB.generate (max (VG.length column) (VG.length other)) (\i -> (column VG.!? i, other VG.!? i)))
longZipColumns (UnboxedColumn column) (BoxedColumn other) = BoxedColumn (VB.generate (max (VG.length column) (VG.length other)) (\i -> (column VG.!? i, other VG.!? i)))
longZipColumns (UnboxedColumn column) (UnboxedColumn other) = BoxedColumn (VB.generate (max (VG.length column) (VG.length other)) (\i -> (column VG.!? i, other VG.!? i)))

zipColumns :: Column -> Column -> Column
zipColumns (BoxedColumn column) (BoxedColumn other) = BoxedColumn (VG.zip column other)
zipColumns (BoxedColumn column) (UnboxedColumn other) = BoxedColumn (VB.generate (min (VG.length column) (VG.length other)) (\i -> (column VG.! i, other VG.! i)))
zipColumns (UnboxedColumn column) (BoxedColumn other) = BoxedColumn (VB.generate (min (VG.length column) (VG.length other)) (\i -> (column VG.! i, other VG.! i)))
zipColumns (UnboxedColumn column) (UnboxedColumn other) = UnboxedColumn (VG.zip column other)
{-# INLINE zipColumns #-}

-- Functions for mutable columns (intended for IO).
-- Clean this up.
writeColumn :: Int -> C.ByteString -> Column -> IO (Either T.Text Bool)
writeColumn i value' (MutableBoxedColumn (col :: VBM.IOVector a)) = let
    value = (decodeUtf8Lenient . C.strip) value'
  in case testEquality (typeRep @a) (typeRep @T.Text) of
      Just Refl -> (if isNullish value
                    then VBM.unsafeWrite col i "" >> return (Left value)
                    else VBM.unsafeWrite col i value >> return (Right True))
      Nothing -> return (Left (T.pack (show value)))
writeColumn i value (MutableUnboxedColumn (col :: VUM.IOVector a)) =
  case testEquality (typeRep @a) (typeRep @Int) of
      Just Refl -> case readByteStringInt value of
        Just v -> VUM.unsafeWrite col i v >> return (Right True)
        Nothing -> VUM.unsafeWrite col i 0 >> return (Left $ decodeUtf8Lenient value)
      Nothing -> let
          value' = decodeUtf8Lenient value
        in case testEquality (typeRep @a) (typeRep @Double) of
          Nothing -> return (Left value')
          Just Refl -> case readDouble value' of
            Just v -> VUM.unsafeWrite col i v >> return (Right True)
            Nothing -> VUM.unsafeWrite col i 0 >> return (Left value')
{-# INLINE writeColumn #-}

freezeColumn' :: [(Int, T.Text)] -> Column -> IO Column
freezeColumn' nulls (MutableBoxedColumn col)
  | null nulls = BoxedColumn <$> VB.unsafeFreeze col
  | all (isNullish . snd) nulls = BoxedColumn . VB.imap (\i v -> if i `elem` map fst nulls then Nothing else Just v) <$> VB.unsafeFreeze col
  | otherwise  = BoxedColumn . VB.imap (\i v -> if i `elem` map fst nulls then Left (fromMaybe (error "") (lookup i nulls)) else Right v) <$> VB.unsafeFreeze col
freezeColumn' nulls (MutableUnboxedColumn col)
  | null nulls = UnboxedColumn <$> VU.unsafeFreeze col
  | all (isNullish . snd) nulls = VU.unsafeFreeze col >>= \c -> return $ BoxedColumn $ VB.generate (VU.length c) (\i -> if i `elem` map fst nulls then Nothing else Just (c VU.! i))
  | otherwise  = VU.unsafeFreeze col >>= \c -> return $ BoxedColumn $ VB.generate (VU.length c) (\i -> if i `elem` map fst nulls then Left (fromMaybe (error "") (lookup i nulls)) else Right (c VU.! i))
{-# INLINE freezeColumn' #-}
