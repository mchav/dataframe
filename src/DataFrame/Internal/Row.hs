{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Internal.Row where

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Set as S
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
import DataFrame.Internal.Types
import Text.ParserCombinators.ReadPrec (ReadPrec)
import Text.Read (Lexeme (Ident), lexP, parens, readListPrec, readListPrecDefault, readPrec)
import Type.Reflection (typeOf, typeRep)

data Any where
    Value :: (Columnable' a) => a -> Any

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

showValue :: forall a. (Columnable' a) => a -> T.Text
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

toAny :: forall a. (Columnable' a) => a -> Any
toAny = Value

type Row = V.Vector Any

toRowList :: [T.Text] -> DataFrame -> [Row]
toRowList names df =
    let
        nameSet = S.fromList names
     in
        map (mkRowRep df nameSet) [0 .. (fst (dataframeDimensions df) - 1)]

toRowVector :: [T.Text] -> DataFrame -> V.Vector Row
toRowVector names df =
    let
        nameSet = S.fromList names
     in
        V.generate (fst (dataframeDimensions df)) (mkRowRep df nameSet)

mkRowFromArgs :: [T.Text] -> DataFrame -> Int -> Row
mkRowFromArgs names df i = V.map get (V.fromList names)
  where
    get name = case getColumn name df of
        Nothing -> throw $ ColumnNotFoundException name "[INTERNAL] mkRowFromArgs" (map fst $ M.toList $ columnIndices df)
        Just (BoxedColumn column) -> toAny (column V.! i)
        Just (UnboxedColumn column) -> toAny (column VU.! i)
        Just (OptionalColumn column) -> toAny (column V.! i)

mkRowRep :: DataFrame -> S.Set T.Text -> Int -> Row
mkRowRep df names i = V.generate (S.size names) (\index -> get (names' V.! index))
  where
    inOrderIndexes = map fst $ L.sortBy (compare `on` snd) $ M.toList (columnIndices df)
    names' = V.fromList [n | n <- inOrderIndexes, S.member n names]
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

sortedIndexes' :: Bool -> V.Vector Row -> VU.Vector Int
sortedIndexes' asc rows = runST $ do
    withIndexes <- VG.thaw (V.indexed rows)
    VA.sortBy ((if asc then compare else flip compare) `on` snd) withIndexes
    sorted <- VG.unsafeFreeze withIndexes
    return $ VU.generate (VG.length rows) (\i -> fst (sorted VG.! i))
