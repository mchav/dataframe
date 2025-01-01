{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE KindSignatures #-}

module Data.DataFrame.Internal where

import qualified Data.Map as M
import qualified Data.Map.Strict as MS
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Algorithms.Merge as VA
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Mutable as VM
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Data.DataFrame.Function
import Data.DataFrame.Util (applySnd, showTable, readInt, isNullish, readDouble, getIndices, getIndicesUnboxed)
import Data.Function (on)
import Data.Int
import Data.List (elemIndex, groupBy, sortBy, transpose)
import Data.Map (Map)
import Data.Maybe (fromMaybe, isJust, isNothing)
import Data.Type.Equality
  ( TestEquality (testEquality),
    type (:~:) (Refl),
  )
import Data.Typeable (Typeable)
import Data.Vector (Vector)
import Data.Vector.Unboxed (Unbox)
import Data.Word
import GHC.Stack (HasCallStack)
import Type.Reflection (TypeRep, Typeable, typeRep)
import Data.Kind (Type)
import Control.Monad.ST (runST)
import Control.Monad (foldM_, join)

initialColumnSize :: Int
initialColumnSize = 8

type ColumnValue a = (Typeable a, Show a, Ord a)

-- | Our representation of a column is a GADT that can store data in either
-- a vector with boxed elements or
data Column where
  BoxedColumn :: ColumnValue a => Vector a -> Column
  UnboxedColumn :: (ColumnValue a, Unbox a) => VU.Vector a -> Column
  GroupedBoxedColumn :: ColumnValue a => Vector (Vector a) -> Column
  GroupedUnboxedColumn :: (ColumnValue a, Unbox a) => Vector (VU.Vector a) -> Column
  MutableBoxedColumn :: ColumnValue a => VM.IOVector a -> Column
  MutableUnboxedColumn :: (ColumnValue a, Unbox a) => VUM.IOVector a -> Column

unboxableTypes :: TypeRepList '[Int, Int8, Int16, Int32, Int64,
                                Word, Word8, Word16, Word32, Word64,
                                Char, Double, Float, Bool]
unboxableTypes = Cons typeRep (Cons typeRep (Cons typeRep (Cons typeRep (Cons typeRep (Cons typeRep (Cons typeRep (Cons typeRep (Cons typeRep (Cons typeRep (Cons typeRep (Cons typeRep (Cons typeRep (Cons typeRep Nil)))))))))))))

numericTypes :: TypeRepList '[Int, Int8, Int16, Int32, Int64, Double, Float]
numericTypes = Cons typeRep (Cons typeRep (Cons typeRep (Cons typeRep (Cons typeRep (Cons typeRep (Cons typeRep Nil))))))

data TypeRepList (xs :: [Type]) where
  Nil  :: TypeRepList '[]
  Cons :: Typeable x => TypeRep x -> TypeRepList xs -> TypeRepList (x ': xs)

matchesAnyType :: forall a xs. (Typeable a) => TypeRepList xs -> TypeRep a -> Bool
matchesAnyType Nil _ = False
matchesAnyType (Cons ty tys) rep =
  case testEquality ty rep of
    Just Refl -> True
    Nothing   -> matchesAnyType tys rep

testUnboxable :: forall a . Typeable a => TypeRep a -> Bool
testUnboxable x = matchesAnyType unboxableTypes (typeRep @a)

testNumeric :: forall a . Typeable a => TypeRep a -> Bool
testNumeric x = matchesAnyType numericTypes (typeRep @a)

transformUnboxed :: forall a b . (ColumnValue a, Unbox a, ColumnValue b) => (a -> b) -> VU.Vector a -> Column
transformUnboxed f column = case testEquality (typeRep @b) (typeRep @Int) of
  Just Refl -> UnboxedColumn $ VG.map f column
  Nothing -> case testEquality (typeRep @b) (typeRep @Int8) of
    Just Refl -> UnboxedColumn $ VG.map f column
    Nothing -> case testEquality (typeRep @b) (typeRep @Int16) of
      Just Refl -> UnboxedColumn $ VG.map f column
      Nothing -> case testEquality (typeRep @b) (typeRep @Int16) of
        Just Refl -> UnboxedColumn $ VG.map f column
        Nothing -> case testEquality (typeRep @b) (typeRep @Int32) of
          Just Refl -> UnboxedColumn $ VG.map f column
          Nothing -> case testEquality (typeRep @b) (typeRep @Int64) of
            Just Refl -> UnboxedColumn $ VG.map f column
            Nothing -> case testEquality (typeRep @b) (typeRep @Word8) of
              Just Refl -> UnboxedColumn $ VG.map f column
              Nothing-> case testEquality (typeRep @b) (typeRep @Word16) of
                Just Refl -> UnboxedColumn $ VG.map f column
                Nothing -> case testEquality (typeRep @b) (typeRep @Word32) of
                  Just Refl -> UnboxedColumn $ VG.map f column
                  Nothing -> case testEquality (typeRep @b) (typeRep @Word64) of
                    Just Refl -> UnboxedColumn $ VG.map f column
                    Nothing -> case testEquality (typeRep @b) (typeRep @Char) of
                      Just Refl -> UnboxedColumn $ VG.map f column
                      Nothing -> case testEquality (typeRep @b) (typeRep @Bool) of
                        Just Refl -> UnboxedColumn $ VG.map f column
                        Nothing -> case testEquality (typeRep @b) (typeRep @Float) of
                          Just Refl -> UnboxedColumn $ VG.map f column
                          Nothing -> case testEquality (typeRep @b) (typeRep @Double) of
                            Just Refl -> UnboxedColumn $ VG.map f column
                            Nothing -> case testEquality (typeRep @b) (typeRep @Word) of
                              Just Refl -> UnboxedColumn $ VG.map f column
                              Nothing -> error "Result type is unboxed" -- since we only call this after confirming 

itransformUnboxed :: forall a b . (ColumnValue a, Unbox a, ColumnValue b) => (Int -> a -> b) -> VU.Vector a -> Column
itransformUnboxed f column = case testEquality (typeRep @b) (typeRep @Int) of
  Just Refl -> UnboxedColumn $ VG.imap f column
  Nothing -> case testEquality (typeRep @b) (typeRep @Int8) of
    Just Refl -> UnboxedColumn $ VG.imap f column
    Nothing -> case testEquality (typeRep @b) (typeRep @Int16) of
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

class Transformable a where
  transform :: forall b c . (ColumnValue b, ColumnValue c) => (b -> c) -> a -> Maybe a

instance Transformable Column where
  transform :: forall b c . (ColumnValue b, ColumnValue c) => (b -> c) -> Column -> Maybe Column
  transform f (BoxedColumn (column :: V.Vector a)) = do
    Refl <- testEquality (typeRep @a) (typeRep @b)
    return (toColumn' (VG.map f column))
  transform f (UnboxedColumn (column :: VU.Vector a)) = do
    Refl <- testEquality (typeRep @a) (typeRep @b)
    return $ if testUnboxable (typeRep @c) then transformUnboxed f column else toColumn' (VG.map f (V.convert column))
  transform f (GroupedBoxedColumn (column :: Vector (Vector a))) = do
    Refl <- testEquality (typeRep @(Vector a)) (typeRep @b)
    return (toColumn' (VG.map f column))
  transform f (GroupedUnboxedColumn (column :: Vector (VU.Vector a))) = do
    Refl <- testEquality (typeRep @(VU.Vector a)) (typeRep @b)
    return (toColumn' (VG.map f column))

instance Show Column where
  show :: Column -> String
  show (BoxedColumn column) = show column
  show (UnboxedColumn column) = show column
  show (GroupedBoxedColumn column) = show column

instance Eq Column where
  (==) :: Column -> Column -> Bool
  (==) (BoxedColumn (a :: V.Vector t1)) (BoxedColumn (b :: V.Vector t2)) =
    case testEquality (typeRep @t1) (typeRep @t2) of
      Nothing -> False
      Just Refl -> a == b
  (==) (UnboxedColumn (a :: VU.Vector t1)) (UnboxedColumn (b :: VU.Vector t2)) =
    case testEquality (typeRep @t1) (typeRep @t2) of
      Nothing -> False
      Just Refl -> a == b
  (==) (GroupedBoxedColumn (a :: V.Vector t1)) (GroupedBoxedColumn (b :: V.Vector t2)) =
    case testEquality (typeRep @t1) (typeRep @t2) of
      Nothing -> False
      Just Refl -> a == b
  (==) (GroupedUnboxedColumn (a :: V.Vector t1)) (GroupedUnboxedColumn (b :: V.Vector t2)) =
    case testEquality (typeRep @t1) (typeRep @t2) of
      Nothing -> False
      Just Refl -> a == b
  (==) _ _ = False

-- Traversing columns.
itransform :: forall b c. (ColumnValue b, ColumnValue c) => (Int -> b -> c) -> Column -> Maybe Column
itransform f (BoxedColumn (column :: V.Vector a)) = do
  Refl <- testEquality (typeRep @a) (typeRep @b)
  return (toColumn' (VG.imap f column))
itransform f (UnboxedColumn (column :: VU.Vector a)) = do
  Refl <- testEquality (typeRep @a) (typeRep @b)
  return $ if testUnboxable (typeRep @c) then itransformUnboxed f column else toColumn' (VG.imap f (V.convert column))
itransform f (GroupedBoxedColumn (column :: Vector (Vector a))) = do
  Refl <- testEquality (typeRep @(Vector a)) (typeRep @b)
  return (toColumn' (VG.imap f column))
itransform f (GroupedUnboxedColumn (column :: Vector (VU.Vector a))) = do
  Refl <- testEquality (typeRep @(VU.Vector a)) (typeRep @b)
  return (toColumn' (VG.imap f column))

ifilterColumn :: forall a . (ColumnValue a) => (Int -> a -> Bool) -> Column -> Maybe Column
ifilterColumn f c@(BoxedColumn (column :: V.Vector b)) = do
  Refl <- testEquality (typeRep @a) (typeRep @b)
  return $ BoxedColumn $ VG.ifilter f column
ifilterColumn f c@(UnboxedColumn (column :: VU.Vector b)) = do
  Refl <- testEquality (typeRep @a) (typeRep @b)
  return $ UnboxedColumn $ VG.ifilter f column
ifilterColumn f c@(GroupedBoxedColumn (column :: V.Vector b)) = do
  Refl <- testEquality (typeRep @a) (typeRep @b)
  return $ GroupedBoxedColumn $ VG.ifilter f column
ifilterColumn f c@(GroupedUnboxedColumn (column :: V.Vector b)) = do
  Refl <- testEquality (typeRep @a) (typeRep @b)
  return $ GroupedUnboxedColumn $ VG.ifilter f column

ifilterColumnF :: Function -> Column -> Maybe Column
ifilterColumnF (ICond (f :: Int -> a -> Bool)) c@(BoxedColumn (column :: V.Vector b)) = do
  Refl <- testEquality (typeRep @a) (typeRep @b)
  return $ BoxedColumn $ VG.ifilter f column
ifilterColumnF (ICond (f :: Int -> a -> Bool)) c@(UnboxedColumn (column :: VU.Vector b)) = do
  Refl <- testEquality (typeRep @a) (typeRep @b)
  return $ UnboxedColumn $ VG.ifilter f column
ifilterColumnF (ICond (f :: Int -> a -> Bool)) c@(GroupedBoxedColumn (column :: V.Vector b)) = do
  Refl <- testEquality (typeRep @a) (typeRep @b)
  return $ GroupedBoxedColumn $ VG.ifilter f column
ifilterColumnF (ICond (f :: Int -> a -> Bool)) c@(GroupedUnboxedColumn (column :: V.Vector b)) = do
  Refl <- testEquality (typeRep @a) (typeRep @b)
  return $ GroupedUnboxedColumn $ VG.ifilter f column

-- We can probably generalize this to `applyVectorFunction`.
atIndices :: S.Set Int -> Column -> Column
atIndices indexes (BoxedColumn column) = BoxedColumn $ VG.ifilter (\i _ -> i `S.member` indexes) column
atIndices indexes (UnboxedColumn column) = UnboxedColumn $ VG.ifilter (\i _ -> i `S.member` indexes) column
atIndices indexes (GroupedBoxedColumn column) = GroupedBoxedColumn $ VG.ifilter (\i _ -> i `S.member` indexes) column
atIndices indexes (GroupedUnboxedColumn column) = GroupedUnboxedColumn $ VG.ifilter (\i _ -> i `S.member` indexes) column

atIndicesStable :: VU.Vector Int -> Column -> Column
atIndicesStable indexes (BoxedColumn column) = BoxedColumn $ indexes `getIndices` column
atIndicesStable indexes (UnboxedColumn column) = UnboxedColumn $ indexes `getIndicesUnboxed` column
atIndicesStable indexes (GroupedBoxedColumn column) = GroupedBoxedColumn $ indexes `getIndices` column
atIndicesStable indexes (GroupedUnboxedColumn column) = GroupedUnboxedColumn $ indexes `getIndices` column


ifoldrColumn :: forall a b. (ColumnValue a, ColumnValue b) => (Int -> a -> b -> b) -> b -> Column -> b
ifoldrColumn f acc c@(BoxedColumn (column :: V.Vector d)) = fromMaybe acc $ do
  Refl <- testEquality (typeRep @a) (typeRep @d)
  return $ VG.ifoldr f acc column
ifoldrColumn f acc c@(UnboxedColumn (column :: VU.Vector d)) = fromMaybe acc $ do
  Refl <- testEquality (typeRep @a) (typeRep @d)
  return $ VG.ifoldr f acc column
ifoldrColumn f acc c@(GroupedBoxedColumn (column :: V.Vector d)) = fromMaybe acc $ do
  Refl <- testEquality (typeRep @a) (typeRep @d)
  return $ VG.ifoldr f acc column
ifoldrColumn f acc c@(GroupedUnboxedColumn (column :: V.Vector d)) = fromMaybe acc $ do
  Refl <- testEquality (typeRep @a) (typeRep @d)
  return $ VG.ifoldr f acc column

ifoldlColumn :: forall a b . (ColumnValue a, ColumnValue b) => (b -> Int -> a -> b) -> b -> Column -> b
ifoldlColumn f acc c@(BoxedColumn (column :: V.Vector d)) = fromMaybe acc $ do
  Refl <- testEquality (typeRep @a) (typeRep @d)
  return $ VG.ifoldl' f acc column
ifoldlColumn f acc c@(UnboxedColumn (column :: VU.Vector d)) = fromMaybe acc $ do
  Refl <- testEquality (typeRep @a) (typeRep @d)
  return $ VG.ifoldl' f acc column
ifoldlColumn f acc c@(GroupedBoxedColumn (column :: V.Vector d)) = fromMaybe acc $ do
  Refl <- testEquality (typeRep @a) (typeRep @d)
  return $ VG.ifoldl' f acc column
ifoldlColumn f acc c@(GroupedUnboxedColumn (column :: V.Vector d)) = fromMaybe acc $ do
  Refl <- testEquality (typeRep @a) (typeRep @d)
  return $ VG.ifoldl' f acc column

writeColumn :: Int -> T.Text -> Column -> IO Bool
writeColumn i value (MutableBoxedColumn (col :: VM.IOVector a)) =
  case testEquality (typeRep @a) (typeRep @T.Text) of
      Just Refl -> (if isNullish value
                    then VM.write col i "" >> return False
                    else VM.write col i value >> return True)
      Nothing -> return False
writeColumn i value (MutableUnboxedColumn (col :: VUM.IOVector a)) =
  case testEquality (typeRep @a) (typeRep @Int) of
      Just Refl -> case readInt value of
        Just v -> VUM.write col i v >> return True
        Nothing -> VUM.write col i 0 >> return False
      Nothing -> case testEquality (typeRep @a) (typeRep @Double) of
        Nothing -> return False
        Just Refl -> case readDouble value of
          Just v -> VUM.write col i v >> return True
          Nothing -> VUM.write col i 0 >> return False

freezeColumn' :: S.Set Int -> Column -> IO Column
freezeColumn' nulls (MutableBoxedColumn col)
  | null nulls = BoxedColumn <$> V.unsafeFreeze col
  | otherwise  = BoxedColumn . V.imap (\i v -> if i `S.member` nulls then Nothing else Just v) <$> V.unsafeFreeze col
freezeColumn' nulls (MutableUnboxedColumn col)
  | null nulls = UnboxedColumn <$> VU.unsafeFreeze col
  | otherwise  = VU.unsafeFreeze col >>= \c -> return $ BoxedColumn $ V.generate (VU.length c) (\i -> if i `S.member` nulls then Nothing else Just (c VU.! i))

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

isGrouped :: Column -> Bool
isGrouped (GroupedBoxedColumn column) = True
isGrouped (GroupedUnboxedColumn column) = True
isGrouped _ = False

data DataFrame = DataFrame
  { -- | Our main data structure stores a dataframe as
    -- a vector of columns. This improv
    columns :: V.Vector (Maybe Column),
    -- | Keeps the column names in the order they were inserted in.
    columnIndices :: M.Map T.Text Int,
    -- | Next free index that we insert a column into.
    freeIndices :: [Int],
    dataframeDimensions :: (Int, Int)
  }

getColumn :: T.Text -> DataFrame -> Maybe Column
getColumn name df = do
  i <- columnIndices df M.!? name
  join $ columns df V.!? i

columnTypeString :: T.Text -> DataFrame -> String
columnTypeString name df = case getColumn name df of
  Just (BoxedColumn (column :: V.Vector a)) -> show (typeRep @a)
  Just (UnboxedColumn (column :: VU.Vector a)) -> show (typeRep @a)
  Just (GroupedBoxedColumn (column :: V.Vector a)) -> show (typeRep @a)
  Just (GroupedUnboxedColumn (column :: V.Vector a)) -> show (typeRep @a)

isEmpty :: DataFrame -> Bool
isEmpty df = dataframeDimensions df == (0, 0)

-- | Converts a boxed vector to a column making sure to put
-- the vector into an appropriate column type by reflection on the
-- vector's type parameter.
toColumn' :: forall a. (Typeable a, Show a, Ord a) => Vector a -> Column
toColumn' xs = case testEquality (typeRep @a) (typeRep @Int) of
  Just Refl -> UnboxedColumn (VU.convert xs)
  Nothing -> case testEquality (typeRep @a) (typeRep @Double) of
    Just Refl -> UnboxedColumn (VU.convert xs)
    Nothing -> case testEquality (typeRep @a) (typeRep @Float) of
      Just Refl -> UnboxedColumn (VU.convert xs)
      Nothing -> BoxedColumn xs

toColumn :: forall a. (Typeable a, Show a, Ord a) => [a] -> Column
toColumn = toColumn' . V.fromList

-- | O(1) Gets the number of elements in the column.
columnLength :: Column -> Int
columnLength (BoxedColumn xs) = VG.length xs
columnLength (UnboxedColumn xs) = VG.length xs
columnLength (GroupedBoxedColumn xs) = VG.length xs
columnLength (GroupedUnboxedColumn xs) = VG.length xs

takeColumn :: Int -> Column -> Column
takeColumn n (BoxedColumn xs) = BoxedColumn $ VG.take n xs
takeColumn n (UnboxedColumn xs) = UnboxedColumn $ VG.take n xs
takeColumn n (GroupedBoxedColumn xs) = GroupedBoxedColumn $ VG.take n xs
takeColumn n (GroupedUnboxedColumn xs) = GroupedUnboxedColumn $ VG.take n xs

-- | Converts a an unboxed vector to a column making sure to put
-- the vector into an appropriate column type by reflection on the
-- vector's type parameter.
toColumnUnboxed :: forall a. (Typeable a, Show a, Ord a, Unbox a) => VU.Vector a -> Column
toColumnUnboxed = UnboxedColumn

-- | O(1) Creates an empty dataframe
empty :: DataFrame
empty = DataFrame {columns = V.replicate initialColumnSize Nothing,
                   columnIndices = M.empty,
                   freeIndices = [0..(initialColumnSize - 1)],
                   dataframeDimensions = (0, 0) }

instance Show DataFrame where
  show :: DataFrame -> String
  show d = T.unpack (asText d)

instance Eq DataFrame where
  (==) :: DataFrame -> DataFrame -> Bool
  a == b = map fst (M.toList $ columnIndices a) == map fst (M.toList $ columnIndices b) &&
           foldr (\(name, index) acc -> acc && (columns a V.!? index == (columns b V.!? (columnIndices b M.! name)))) True (M.toList $ columnIndices a)

asText :: DataFrame -> T.Text
asText d =
  let header = "index" : map fst (sortBy (compare `on` snd) $ M.toList (columnIndices d))
      types = V.toList $ V.filter (/= "") $ V.map getType (columns d)
      getType Nothing = ""
      getType (Just (BoxedColumn (column :: Vector a))) = T.pack $ show (Type.Reflection.typeRep @a)
      getType (Just (UnboxedColumn (column :: VU.Vector a))) = T.pack $ show (Type.Reflection.typeRep @a)
      getType (Just (GroupedBoxedColumn (column :: V.Vector a))) = T.pack $ show (Type.Reflection.typeRep @a)
      getType (Just (GroupedUnboxedColumn (column :: V.Vector a))) = T.pack $ show (Type.Reflection.typeRep @a)
      -- Separate out cases dynamically so we don't end up making round trip string
      -- copies.
      get (Just (BoxedColumn (column :: Vector a))) =
        let repa :: Type.Reflection.TypeRep a = Type.Reflection.typeRep @a
            repString :: Type.Reflection.TypeRep String = Type.Reflection.typeRep @String
            repText :: Type.Reflection.TypeRep T.Text = Type.Reflection.typeRep @T.Text
         in case testEquality repa repText of
              Just Refl -> column
              Nothing -> case testEquality repa repString of
                Just Refl -> V.map T.pack column
                Nothing -> V.map (T.pack . show) column
      get (Just (UnboxedColumn column)) = V.map (T.pack . show) (V.convert column)
      get (Just (GroupedBoxedColumn column)) = V.map (T.pack . show) (V.convert column)
      get (Just (GroupedUnboxedColumn column)) = V.map (T.pack . show) (V.convert column)
      getTextColumnFromFrame df (i, name) = if i == 0
                                            then V.fromList (map (T.pack . show) [0..(fst (dataframeDimensions df) - 1)])
                                            else get $ (V.!) (columns d) ((M.!) (columnIndices d) name)
      rows =
        transpose $
          zipWith (curry (V.toList . getTextColumnFromFrame d)) [0..] header
   in showTable header ("Int":types) rows

metadata :: DataFrame -> String
metadata df = show (columnIndices df) ++ "\n" ++
              show (V.map isJust (columns df)) ++ "\n" ++
              show (freeIndices df) ++ "\n" ++
              show (dataframeDimensions df)
