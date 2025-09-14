{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

module DataFrame.Internal.Column where

import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Vector as VB
import qualified Data.Vector.Algorithms.Merge as VA
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Mutable as VBM
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

import Control.Exception (throw)
import Control.Monad.ST (runST)
import Data.Maybe
import Data.Type.Equality (TestEquality (..))
import DataFrame.Errors
import DataFrame.Internal.Parsing
import DataFrame.Internal.Types
import Type.Reflection

{- | Our representation of a column is a GADT that can store data based on the underlying data.

This allows us to pattern match on data kinds and limit some operations to only some
kinds of vectors. E.g. operations for missing data only happen in an OptionalColumn.
-}
data Column where
    BoxedColumn :: (Columnable a) => VB.Vector a -> Column
    UnboxedColumn :: (Columnable a, VU.Unbox a) => VU.Vector a -> Column
    OptionalColumn :: (Columnable a) => VB.Vector (Maybe a) -> Column

data MutableColumn where
    MBoxedColumn :: (Columnable a) => VBM.IOVector a -> MutableColumn
    MUnboxedColumn :: (Columnable a, VU.Unbox a) => VUM.IOVector a -> MutableColumn

{- | A TypedColumn is a wrapper around our type-erased column.
It is used to type check expressions on columns.
-}
data TypedColumn a where
    TColumn :: (Columnable a) => Column -> TypedColumn a

-- | Gets the underlying value from a TypedColumn.
unwrapTypedColumn :: TypedColumn a -> Column
unwrapTypedColumn (TColumn value) = value

-- | Checks if a column contains missing values.
hasMissing :: Column -> Bool
hasMissing (OptionalColumn column) = True
hasMissing _ = False

-- | Checks if a column contains numeric values.
isNumeric :: Column -> Bool
isNumeric (UnboxedColumn (vec :: VU.Vector a)) = case sNumeric @a of
    STrue -> True
    _ -> False
isNumeric _ = False

-- | An internal/debugging function to get the column type of a column.
columnVersionString :: Column -> String
columnVersionString column = case column of
    BoxedColumn _ -> "Boxed"
    UnboxedColumn _ -> "Unboxed"
    OptionalColumn _ -> "Optional"

{- | An internal/debugging function to get the type stored in the outermost vector
of a column.
-}
columnTypeString :: Column -> String
columnTypeString column = case column of
    BoxedColumn (column :: VB.Vector a) -> show (typeRep @a)
    UnboxedColumn (column :: VU.Vector a) -> show (typeRep @a)
    OptionalColumn (column :: VB.Vector a) -> show (typeRep @a)

instance (Show a) => Show (TypedColumn a) where
    show (TColumn col) = show col

instance Show Column where
    show :: Column -> String
    show (BoxedColumn column) = show column
    show (UnboxedColumn column) = show column
    show (OptionalColumn column) = show column

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
    (==) _ _ = False

{- | A class for converting a vector to a column of the appropriate type.
Given each Rep we tell the `toColumnRep` function which Column type to pick.
-}
class ColumnifyRep (r :: Rep) a where
    toColumnRep :: VB.Vector a -> Column

-- | Constraint synonym for what we can put into columns.
type Columnable a =
    ( Columnable' a
    , ColumnifyRep (KindOf a) a
    , UnboxIf a
    , IntegralIf a
    , FloatingIf a
    , SBoolI (Unboxable a)
    , SBoolI (Numeric a)
    , SBoolI (IntegralTypes a)
    , SBoolI (FloatingTypes a)
    )

instance
    (Columnable a, VU.Unbox a) =>
    ColumnifyRep 'RUnboxed a
    where
    toColumnRep = UnboxedColumn . VU.convert

instance
    (Columnable a) =>
    ColumnifyRep 'RBoxed a
    where
    toColumnRep = BoxedColumn

instance
    (Columnable a) =>
    ColumnifyRep 'ROptional (Maybe a)
    where
    toColumnRep = OptionalColumn

{- | O(n) Convert a vector to a column. Automatically picks the best representation of a vector to store the underlying data in.

__Examples:__

@
> import qualified Data.Vector as V
> fromVector (V.fromList [(1 :: Int), 2, 3, 4])
[1,2,3,4]
@
-}
fromVector ::
    forall a.
    (Columnable a, ColumnifyRep (KindOf a) a) =>
    VB.Vector a -> Column
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
    forall a.
    (Columnable a, ColumnifyRep (KindOf a) a) =>
    [a] -> Column
fromList = toColumnRep @(KindOf a) . VB.fromList

-- | An internal function to map a function over the values of a column.
mapColumn ::
    forall b c.
    ( Columnable b
    , Columnable c
    , UnboxIf c
    ) =>
    (b -> c) ->
    Column ->
    Maybe Column
mapColumn f = \case
    BoxedColumn (col :: VB.Vector a)
        | Just Refl <- testEquality (typeRep @a) (typeRep @b) ->
            Just (fromVector @c (VB.map f col))
        | otherwise -> Nothing
    OptionalColumn (col :: VB.Vector a)
        | Just Refl <- testEquality (typeRep @a) (typeRep @b) ->
            Just (fromVector @c (VB.map f col))
        | otherwise -> Nothing
    UnboxedColumn (col :: VU.Vector a)
        | Just Refl <- testEquality (typeRep @a) (typeRep @b) ->
            Just $ case sUnbox @c of
                STrue -> UnboxedColumn (VU.map f col)
                SFalse -> fromVector @c (VB.map f (VB.convert col))
        | otherwise -> Nothing

-- | O(1) Gets the number of elements in the column.
columnLength :: Column -> Int
columnLength (BoxedColumn xs) = VG.length xs
columnLength (UnboxedColumn xs) = VG.length xs
columnLength (OptionalColumn xs) = VG.length xs
{-# INLINE columnLength #-}

-- | O(n) Gets the number of elements in the column.
numElements :: Column -> Int
numElements (BoxedColumn xs) = VG.length xs
numElements (UnboxedColumn xs) = VG.length xs
numElements (OptionalColumn xs) = VG.foldl' (\acc x -> acc + fromEnum (isJust x)) 0 xs
{-# INLINE numElements #-}

-- | O(n) Takes the first n values of a column.
takeColumn :: Int -> Column -> Column
takeColumn n (BoxedColumn xs) = BoxedColumn $ VG.take n xs
takeColumn n (UnboxedColumn xs) = UnboxedColumn $ VG.take n xs
takeColumn n (OptionalColumn xs) = OptionalColumn $ VG.take n xs
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
{-# INLINE sliceColumn #-}

-- | O(n) Selects the elements at a given set of indices. May change the order.
atIndices :: S.Set Int -> Column -> Column
atIndices indexes (BoxedColumn column) = BoxedColumn $ VG.ifilter (\i _ -> i `S.member` indexes) column
atIndices indexes (OptionalColumn column) = OptionalColumn $ VG.ifilter (\i _ -> i `S.member` indexes) column
atIndices indexes (UnboxedColumn column) = UnboxedColumn $ VG.ifilter (\i _ -> i `S.member` indexes) column
{-# INLINE atIndices #-}

-- | O(n) Selects the elements at a given set of indices. Does not change the order.
atIndicesStable :: VU.Vector Int -> Column -> Column
atIndicesStable indexes (BoxedColumn column) = BoxedColumn $ indexes `getIndices` column
atIndicesStable indexes (UnboxedColumn column) = UnboxedColumn $ VU.unsafeBackpermute column indexes
atIndicesStable indexes (OptionalColumn column) = OptionalColumn $ indexes `getIndices` column
{-# INLINE atIndicesStable #-}

-- | Internal helper to get indices in a boxed vector.
getIndices :: VU.Vector Int -> VB.Vector a -> VB.Vector a
getIndices indices xs = VB.generate (VU.length indices) (\i -> xs VB.! (indices VU.! i))
{-# INLINE getIndices #-}

-- | Internal helper to get indices in an unboxed vector.
getIndicesUnboxed :: (VU.Unbox a) => VU.Vector Int -> VU.Vector a -> VU.Vector a
getIndicesUnboxed indices xs = VU.generate (VU.length indices) (\i -> xs VU.! (indices VU.! i))
{-# INLINE getIndicesUnboxed #-}

findIndices ::
    forall a.
    (Columnable a) =>
    (a -> Bool) ->
    Column ->
    Maybe (VU.Vector Int)
findIndices pred (BoxedColumn (column :: VB.Vector b)) = do
    Refl <- testEquality (typeRep @a) (typeRep @b)
    pure $ VG.convert (VG.findIndices pred column)
findIndices pred (UnboxedColumn (column :: VU.Vector b)) = do
    Refl <- testEquality (typeRep @a) (typeRep @b)
    pure $ VG.findIndices pred column
findIndices pred (OptionalColumn (column :: VB.Vector (Maybe b))) = do
    Refl <- testEquality (typeRep @a) (typeRep @(Maybe b))
    pure $ VG.convert (VG.findIndices pred column)

-- | An internal function that returns a vector of how indexes change after a column is sorted.
sortedIndexes :: Bool -> Column -> VU.Vector Int
sortedIndexes asc (BoxedColumn column) = runST $ do
    withIndexes <- VG.thaw $ VG.indexed column
    VA.sortBy (\(a, b) (a', b') -> (if asc then compare else flip compare) b b') withIndexes
    sorted <- VG.unsafeFreeze withIndexes
    return $ VU.generate (VG.length column) (\i -> fst (sorted VG.! i))
sortedIndexes asc (UnboxedColumn column) = runST $ do
    withIndexes <- VG.thaw $ VG.indexed column
    VA.sortBy (\(a, b) (a', b') -> (if asc then compare else flip compare) b b') withIndexes
    sorted <- VG.unsafeFreeze withIndexes
    return $ VU.generate (VG.length column) (\i -> fst (sorted VG.! i))
sortedIndexes asc (OptionalColumn column) = runST $ do
    withIndexes <- VG.thaw $ VG.indexed column
    VA.sortBy (\(a, b) (a', b') -> (if asc then compare else flip compare) b b') withIndexes
    sorted <- VG.unsafeFreeze withIndexes
    return $ VU.generate (VG.length column) (\i -> fst (sorted VG.! i))
{-# INLINE sortedIndexes #-}

-- | Applies a function that returns an unboxed result to an unboxed vector, storing the result in a column.
imapColumn ::
    forall b c.
    (Columnable b, Columnable c) =>
    (Int -> b -> c) -> Column -> Maybe Column
imapColumn f = \case
    BoxedColumn (col :: VB.Vector a)
        | Just Refl <- testEquality (typeRep @a) (typeRep @b) ->
            Just (fromVector @c (VB.imap f col))
        | otherwise -> Nothing
    UnboxedColumn (col :: VU.Vector a)
        | Just Refl <- testEquality (typeRep @a) (typeRep @b) ->
            Just $
                case sUnbox @c of
                    STrue -> UnboxedColumn (VU.imap f col)
                    SFalse -> fromVector @c (VB.imap f (VB.convert col))
        | otherwise -> Nothing
    OptionalColumn (col :: VB.Vector a)
        | Just Refl <- testEquality (typeRep @a) (typeRep @b) ->
            Just (fromVector @c (VB.imap f col))
        | otherwise -> Nothing

-- | Filter column with index.
ifilterColumn :: forall a. (Columnable a) => (Int -> a -> Bool) -> Column -> Maybe Column
ifilterColumn f c@(BoxedColumn (column :: VB.Vector b)) = do
    Refl <- testEquality (typeRep @a) (typeRep @b)
    return $ BoxedColumn $ VG.ifilter f column
ifilterColumn f c@(UnboxedColumn (column :: VU.Vector b)) = do
    Refl <- testEquality (typeRep @a) (typeRep @b)
    return $ UnboxedColumn $ VG.ifilter f column
ifilterColumn _ _ = Nothing

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

-- | Fold (left) column with index.
ifoldlColumn :: forall a b. (Columnable a, Columnable b) => (b -> Int -> a -> b) -> b -> Column -> Maybe b
ifoldlColumn f acc c@(BoxedColumn (column :: VB.Vector d)) = do
    Refl <- testEquality (typeRep @a) (typeRep @d)
    return $ VG.ifoldl' f acc column
ifoldlColumn f acc c@(OptionalColumn (column :: VB.Vector d)) = do
    Refl <- testEquality (typeRep @a) (typeRep @d)
    return $ VG.ifoldl' f acc column
ifoldlColumn f acc c@(UnboxedColumn (column :: VU.Vector d)) = do
    Refl <- testEquality (typeRep @a) (typeRep @d)
    return $ VG.ifoldl' f acc column

headColumn :: forall a. (Columnable a) => Column -> Maybe a
headColumn (BoxedColumn (col :: VB.Vector b)) = do
    Refl <- testEquality (typeRep @a) (typeRep @b)
    pure (VG.head col)
headColumn (UnboxedColumn (col :: VU.Vector b)) = do
    Refl <- testEquality (typeRep @a) (typeRep @b)
    pure (VG.head col)
headColumn (OptionalColumn (col :: VB.Vector b)) = do
    Refl <- testEquality (typeRep @a) (typeRep @b)
    pure (VG.head col)

-- | Generic reduce function for all Column types.
reduceColumn :: forall a b. (Columnable a) => (a -> b) -> Column -> Maybe b
{-# SPECIALIZE reduceColumn ::
    (VU.Vector (Double, Double) -> Double) -> Column -> Maybe Double
    , (VU.Vector Double -> Double) -> Column -> Maybe Double
    #-}
reduceColumn f (BoxedColumn (column :: c)) = do
    Refl <- testEquality (typeRep @c) (typeRep @a)
    pure $ f column
reduceColumn f (UnboxedColumn (column :: c)) = do
    Refl <- testEquality (typeRep @c) (typeRep @a)
    pure $ f column
reduceColumn f (OptionalColumn (column :: c)) = do
    Refl <- testEquality (typeRep @c) (typeRep @a)
    pure $ f column
{-# INLINE reduceColumn #-}

-- | An internal, column version of zip.
zipColumns :: Column -> Column -> Column
zipColumns (BoxedColumn column) (BoxedColumn other) = BoxedColumn (VG.zip column other)
zipColumns (BoxedColumn column) (UnboxedColumn other) = BoxedColumn (VB.generate (min (VG.length column) (VG.length other)) (\i -> (column VG.! i, other VG.! i)))
zipColumns (BoxedColumn column) (OptionalColumn optcolumn) = BoxedColumn (VG.zip (VB.convert column) optcolumn)
zipColumns (UnboxedColumn column) (BoxedColumn other) = BoxedColumn (VB.generate (min (VG.length column) (VG.length other)) (\i -> (column VG.! i, other VG.! i)))
zipColumns (UnboxedColumn column) (UnboxedColumn other) = UnboxedColumn (VG.zip column other)
zipColumns (UnboxedColumn column) (OptionalColumn optcolumn) = BoxedColumn (VG.zip (VB.convert column) optcolumn)
zipColumns (OptionalColumn optcolumn) (BoxedColumn column) = BoxedColumn (VG.zip optcolumn (VB.convert column))
zipColumns (OptionalColumn optcolumn) (UnboxedColumn column) = BoxedColumn (VG.zip optcolumn (VB.convert column))
zipColumns (OptionalColumn optcolumn) (OptionalColumn optother) = BoxedColumn (VG.zip optcolumn optother)
{-# INLINE zipColumns #-}

-- | An internal, column version of zipWith.
zipWithColumns :: forall a b c. (Columnable a, Columnable b, Columnable c) => (a -> b -> c) -> Column -> Column -> Maybe Column
zipWithColumns f (UnboxedColumn (column :: VU.Vector d)) (UnboxedColumn (other :: VU.Vector e)) = case testEquality (typeRep @a) (typeRep @d) of
    Just Refl -> case testEquality (typeRep @b) (typeRep @e) of
        Just Refl -> pure $ case sUnbox @c of
            STrue -> fromUnboxedVector (VU.zipWith f column other)
            SFalse -> fromVector $ VB.zipWith f (VG.convert column) (VG.convert other)
        Nothing -> Nothing
    Nothing -> Nothing
zipWithColumns f left right =
    let
        left' = toVector @a left
        right' = toVector @b right
     in
        pure $ fromVector $ VB.zipWith f left' right'
{-# INLINE zipWithColumns #-}

-- Functions for mutable columns (intended for IO).
writeColumn :: Int -> T.Text -> MutableColumn -> IO (Either T.Text Bool)
writeColumn i value (MBoxedColumn (col :: VBM.IOVector a)) =
    let
     in case testEquality (typeRep @a) (typeRep @T.Text) of
            Just Refl ->
                ( if isNullish value
                    then VBM.unsafeWrite col i "" >> return (Left $! value)
                    else VBM.unsafeWrite col i value >> return (Right True)
                )
            Nothing -> return (Left value)
writeColumn i value (MUnboxedColumn (col :: VUM.IOVector a)) =
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

freezeColumn' :: [(Int, T.Text)] -> MutableColumn -> IO Column
freezeColumn' nulls (MBoxedColumn col)
    | null nulls = BoxedColumn <$> VB.unsafeFreeze col
    | all (isNullish . snd) nulls = OptionalColumn . VB.imap (\i v -> if i `elem` map fst nulls then Nothing else Just v) <$> VB.unsafeFreeze col
    | otherwise = BoxedColumn . VB.imap (\i v -> if i `elem` map fst nulls then Left (fromMaybe (error "") (lookup i nulls)) else Right v) <$> VB.unsafeFreeze col
freezeColumn' nulls (MUnboxedColumn col)
    | null nulls = UnboxedColumn <$> VU.unsafeFreeze col
    | all (isNullish . snd) nulls = VU.unsafeFreeze col >>= \c -> return $ OptionalColumn $ VB.generate (VU.length c) (\i -> if i `elem` map fst nulls then Nothing else Just (c VU.! i))
    | otherwise = VU.unsafeFreeze col >>= \c -> return $ BoxedColumn $ VB.generate (VU.length c) (\i -> if i `elem` map fst nulls then Left (fromMaybe (error "") (lookup i nulls)) else Right (c VU.! i))
{-# INLINE freezeColumn' #-}

-- | Fills the end of a column, up to n, with Nothing. Does nothing if column has length greater than n.
expandColumn :: Int -> Column -> Column
expandColumn n (OptionalColumn col) = OptionalColumn $ col <> VB.replicate (n - VG.length col) Nothing
expandColumn n column@(BoxedColumn col)
    | n > VG.length col = OptionalColumn $ VB.map Just col <> VB.replicate (n - VG.length col) Nothing
    | otherwise = column
expandColumn n column@(UnboxedColumn col)
    | n > VG.length col = OptionalColumn $ VB.map Just (VU.convert col) <> VB.replicate (n - VG.length col) Nothing
    | otherwise = column

-- | Fills the beginning of a column, up to n, with Nothing. Does nothing if column has length greater than n.
leftExpandColumn :: Int -> Column -> Column
leftExpandColumn n column@(OptionalColumn col)
    | n > VG.length col = OptionalColumn $ VG.replicate (n - VG.length col) Nothing <> col
    | otherwise = column
leftExpandColumn n column@(BoxedColumn col)
    | n > VG.length col = OptionalColumn $ VG.replicate (n - VG.length col) Nothing <> VG.map Just col
    | otherwise = column
leftExpandColumn n column@(UnboxedColumn col)
    | n > VG.length col = OptionalColumn $ VG.replicate (n - VG.length col) Nothing <> VG.map Just (VU.convert col)
    | otherwise = column

-- | Concatenates two columns.
concatColumns :: Column -> Column -> Maybe Column
concatColumns (OptionalColumn left) (OptionalColumn right) = case testEquality (typeOf left) (typeOf right) of
    Nothing -> Nothing
    Just Refl -> Just (OptionalColumn $ left <> right)
concatColumns (BoxedColumn left) (BoxedColumn right) = case testEquality (typeOf left) (typeOf right) of
    Nothing -> Nothing
    Just Refl -> Just (BoxedColumn $ left <> right)
concatColumns (UnboxedColumn left) (UnboxedColumn right) = case testEquality (typeOf left) (typeOf right) of
    Nothing -> Nothing
    Just Refl -> Just (UnboxedColumn $ left <> right)
concatColumns _ _ = Nothing

{- | O(n) Converts a column to a boxed vector. Throws an exception if the wrong type is specified.

__Examples:__

@
> column = fromList [(1 :: Int), 2, 3, 4]
> toVector @Int column
[1,2,3,4]
> toVector @Double column
exception: ...
@
-}
toVector :: forall a. (Columnable a) => Column -> VB.Vector a
toVector xs = case toVectorSafe xs of
    Left err -> throw err
    Right val -> val

{- | O(n) Converts a column to a list. Throws an exception if the wrong type is specified.

__Examples:__

@
> column = fromList [(1 :: Int), 2, 3, 4]
> toList @Int column
[1,2,3,4]
> toList @Double column
exception: ...
@
-}
toList :: forall a. (Columnable a) => Column -> [a]
toList xs = case toVectorSafe @a xs of
    Left err -> throw err
    Right val -> VB.toList val

-- | A safe version of toVector that returns an Either type.
toVectorSafe :: forall a v. (VG.Vector v a, Columnable a) => Column -> Either DataFrameException (v a)
toVectorSafe column@(OptionalColumn (col :: VB.Vector b)) =
    case testEquality (typeRep @a) (typeRep @b) of
        Just Refl -> Right $ VG.convert col
        Nothing ->
            Left $
                TypeMismatchException
                    ( MkTypeErrorContext
                        { userType = Right (typeRep @a)
                        , expectedType = Right (typeRep @b)
                        , callingFunctionName = Just "toVectorSafe"
                        , errorColumnName = Nothing
                        }
                    )
toVectorSafe (BoxedColumn (col :: VB.Vector b)) =
    case testEquality (typeRep @a) (typeRep @b) of
        Just Refl -> Right $ VG.convert col
        Nothing ->
            Left $
                TypeMismatchException
                    ( MkTypeErrorContext
                        { userType = Right (typeRep @a)
                        , expectedType = Right (typeRep @b)
                        , callingFunctionName = Just "toVectorSafe"
                        , errorColumnName = Nothing
                        }
                    )
toVectorSafe (UnboxedColumn (col :: VU.Vector b)) =
    case testEquality (typeRep @a) (typeRep @b) of
        Just Refl -> Right $ VG.convert col
        Nothing ->
            Left $
                TypeMismatchException
                    ( MkTypeErrorContext
                        { userType = Right (typeRep @a)
                        , expectedType = Right (typeRep @b)
                        , callingFunctionName = Just "toVectorSafe"
                        , errorColumnName = Nothing
                        }
                    )
