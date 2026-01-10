{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Internal.Row where

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Algorithms.Merge as VA
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU

import Control.Exception (throw)
import Control.Monad.ST (runST)
import Data.Function (on)
import Data.Maybe (fromMaybe)
import Data.Type.Equality (TestEquality (..))
import Data.Typeable (type (:~:) (..))
import DataFrame.Errors (DataFrameException (..))
import DataFrame.Internal.Column
import DataFrame.Internal.DataFrame
import Text.ParserCombinators.ReadPrec (ReadPrec)
import Text.Read (
    Lexeme (Ident),
    lexP,
    parens,
    readListPrec,
    readListPrecDefault,
    readPrec,
 )
import Type.Reflection (typeOf, typeRep)

data Any where
    Value :: (Columnable a) => a -> Any

instance Eq Any where
    (==) :: Any -> Any -> Bool
    (Value a) == (Value b) = fromMaybe False $ do
        Refl <- testEquality (typeOf a) (typeOf b)
        return $ a == b

instance Ord Any where
    (<=) :: Any -> Any -> Bool
    (Value a) <= (Value b) = fromMaybe False $ do
        Refl <- testEquality (typeOf a) (typeOf b)
        return $ a <= b

instance Show Any where
    show :: Any -> String
    show (Value a) = T.unpack (showValue a)

showValue :: forall a. (Columnable a) => a -> T.Text
showValue v = case testEquality (typeRep @a) (typeRep @T.Text) of
    Just Refl -> v
    Nothing -> case testEquality (typeRep @a) (typeRep @String) of
        Just Refl -> T.pack v
        Nothing -> (T.pack . show) v

instance Read Any where
    readListPrec :: ReadPrec [Any]
    readListPrec = readListPrecDefault

    readPrec :: ReadPrec Any
    readPrec = parens $ do
        Ident "Value" <- lexP
        readPrec

-- | Wraps a value into an \Any\ type. This helps up represent rows as heterogenous lists.
toAny :: forall a. (Columnable a) => a -> Any
toAny = Value

-- | Unwraps a value from an \Any\ type.
fromAny :: forall a. (Columnable a) => Any -> Maybe a
fromAny (Value (v :: b)) = do
    Refl <- testEquality (typeRep @a) (typeRep @b)
    pure v

type Row = V.Vector Any

(!?) :: [a] -> Int -> Maybe a
(!?) [] _ = Nothing
(!?) (x : _) 0 = Just x
(!?) (x : xs) n = (!?) xs (n - 1)

mkColumnFromRow :: Int -> [[Any]] -> Column
mkColumnFromRow i rows = case rows of
    [] -> fromList ([] :: [T.Text])
    (row : _) -> case row !? i of
        Nothing -> fromList ([] :: [T.Text])
        Just (Value (v :: a)) -> fromList $ reverse $ L.foldl' addToList [v] (drop 1 rows)
          where
            addToList acc r = case r !? i of
                Nothing -> acc
                Just (Value (v' :: b)) -> case testEquality (typeRep @a) (typeRep @b) of
                    Nothing -> acc
                    Just Refl -> v' : acc

{- | Converts the entire dataframe to a list of rows.

Each row contains all columns in the dataframe, ordered by their column indices.
The rows are returned in their natural order (from index 0 to n-1).

==== __Examples__

>>> toRowList df
[[("name", "Alice"), ("age", 25), ...], [("name", "Bob"), ("age", 30), ...], ...]

==== __Performance note__

This function materializes all rows into a list, which may be memory-intensive
for large dataframes. Consider using 'toRowVector' if you need random access
or streaming operations.
-}
toRowList :: DataFrame -> [[(T.Text, Any)]]
toRowList df =
    let
        names = map fst (L.sortBy (compare `on` snd) $ M.toList (columnIndices df))
     in
        map
            (zip names . V.toList . mkRowRep df names)
            [0 .. (fst (dataframeDimensions df) - 1)]

{- | Converts the dataframe to a vector of rows with only the specified columns.

Each row will contain only the columns named in the @names@ parameter.
This is useful when you only need a subset of columns or want to control
the column order in the resulting rows.

==== __Parameters__

[@names@] List of column names to include in each row. The order of names
          determines the order of fields in the resulting rows.

[@df@] The dataframe to convert.

==== __Examples__

>>> toRowVector ["name", "age"] df
Vector of rows with only name and age fields

>>> toRowVector [] df  -- Empty column list
Vector of empty rows (one per dataframe row)
-}
toRowVector :: [T.Text] -> DataFrame -> V.Vector Row
toRowVector names df = V.generate (fst (dataframeDimensions df)) (mkRowRep df names)

mkRowFromArgs :: [T.Text] -> DataFrame -> Int -> Row
mkRowFromArgs names df i = V.map get (V.fromList names)
  where
    get name = case getColumn name df of
        Nothing ->
            throw $
                ColumnNotFoundException
                    name
                    "[INTERNAL] mkRowFromArgs"
                    (M.keys $ columnIndices df)
        Just (BoxedColumn column) -> toAny (column V.! i)
        Just (UnboxedColumn column) -> toAny (column VU.! i)
        Just (OptionalColumn column) -> toAny (column V.! i)

-- This function will return the items in the order that is specified
-- by the user. For example, if the dataframe consists of the columns
-- "Age", "Pclass", "Name", and the user asks for ["Name", "Age"],
-- this will order the values in the order ["Mr Smith", 50]
mkRowRep :: DataFrame -> [T.Text] -> Int -> Row
mkRowRep df names i = V.generate (L.length names) (\index -> get (names' V.! index))
  where
    names' = V.fromList names
    throwError name =
        error $
            "Column "
                ++ T.unpack name
                ++ " has less items than "
                ++ "the other columns at index "
                ++ show i
    get name = case getColumn name df of
        Just (BoxedColumn c) -> case c V.!? i of
            Just e -> toAny e
            Nothing -> throwError name
        Just (OptionalColumn c) -> case c V.!? i of
            Just e -> toAny e
            Nothing -> throwError name
        Just (UnboxedColumn c) -> case c VU.!? i of
            Just e -> toAny e
            Nothing -> throwError name
        Nothing ->
            throw $ ColumnNotFoundException name "mkRowRep" (M.keys $ columnIndices df)

sortedIndexes' :: [Bool] -> V.Vector Row -> VU.Vector Int
sortedIndexes' flipCompare rows = runST $ do
    withIndexes <- VG.thaw (V.indexed rows)
    VA.sortBy (produceOrderingFromRow flipCompare `on` snd) withIndexes
    sorted <- VG.unsafeFreeze withIndexes
    return $ VU.generate (VG.length rows) (\i -> fst (sorted VG.! i))

produceOrderingFromRow :: [Bool] -> Row -> Row -> Ordering
produceOrderingFromRow mustFlips v1 v2 = V.foldr (<>) mempty vZipped
  where
    vFlip = V.fromList mustFlips
    vZipped =
        V.zipWith3 (\b e1 e2 -> if b then compare e1 e2 else compare e2 e1) vFlip v1 v2
