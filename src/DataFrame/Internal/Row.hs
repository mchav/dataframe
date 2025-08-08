{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
module DataFrame.Internal.Row where

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Algorithms.Merge as VA

import Control.Exception (throw)
import Control.Monad.ST (runST)
import DataFrame.Errors (DataFrameException(..))
import DataFrame.Internal.Column
import DataFrame.Internal.DataFrame
import DataFrame.Internal.Types
import Data.Function (on)
import Data.Maybe (fromMaybe)
import Data.Typeable (Typeable, type (:~:) (..))
import Data.Word ( Word8, Word16, Word32, Word64 )
import Type.Reflection (TypeRep, typeOf, typeRep)
import Data.Type.Equality (TestEquality(..))
import Text.ParserCombinators.ReadPrec(ReadPrec)
import Text.Read (Lexeme (Ident), lexP, parens, readListPrec, readListPrecDefault, readPrec)


data RowValue where
    Value :: (Columnable' a) => a -> RowValue

instance Eq RowValue where
    (==) :: RowValue -> RowValue -> Bool
    (Value a) == (Value b) = fromMaybe False $ do
        Refl <- testEquality (typeOf a) (typeOf b)
        return $ a == b

instance Ord RowValue where
    (<=) :: RowValue -> RowValue -> Bool
    (Value a) <= (Value b) = fromMaybe False $ do
        Refl <- testEquality (typeOf a) (typeOf b)
        return $ a <= b

instance Show RowValue where
    show :: RowValue -> String
    show (Value a) = show a

instance Read RowValue where
  readListPrec :: ReadPrec [RowValue]
  readListPrec = readListPrecDefault

  readPrec :: ReadPrec RowValue
  readPrec = parens $ do
    Ident "Value" <- lexP
    readPrec


toRowValue :: forall a . (Columnable' a) => a -> RowValue
toRowValue =  Value

type Row = V.Vector RowValue

toRowList :: [T.Text] -> DataFrame -> [Row]
toRowList names df = let
    nameSet = S.fromList names
  in map (mkRowRep df nameSet) [0..(fst (dataframeDimensions df) - 1)]

toRowVector :: [T.Text] -> DataFrame -> V.Vector Row
toRowVector names df = let
    nameSet = S.fromList names
  in V.generate (fst (dataframeDimensions df)) (mkRowRep df nameSet)

mkRowFromArgs :: [T.Text] -> DataFrame -> Int -> Row
mkRowFromArgs names df i = V.map get (V.fromList names)
  where
    get name = case getColumn name df of
      Nothing -> throw $ ColumnNotFoundException name "[INTERNAL] mkRowFromArgs" (map fst $ M.toList $ columnIndices df)
      Just (BoxedColumn column) -> toRowValue (column V.! i)
      Just (UnboxedColumn column) -> toRowValue (column VU.! i)
      Just (OptionalColumn column) -> toRowValue (column V.! i)

mkRowRep :: DataFrame -> S.Set T.Text -> Int -> Row
mkRowRep df names i = V.generate (S.size names) (\index -> get (names' V.! index))
  where
    inOrderIndexes = map fst $ L.sortBy (compare `on` snd) $ M.toList (columnIndices df)
    names' = V.fromList [n | n <- inOrderIndexes, S.member n names]
    throwError name = error $ "Column "
                ++ T.unpack name
                ++ " has less items than "
                ++ "the other columns at index "
                ++ show i
    get name = case getColumn name df of
      Just (BoxedColumn c) -> case c V.!? i of
        Just e -> toRowValue e
        Nothing -> throwError name
      Just (OptionalColumn c) -> case c V.!? i of
        Just e -> toRowValue e
        Nothing -> throwError name
      Just (UnboxedColumn c) -> case c VU.!? i of
        Just e -> toRowValue e
        Nothing -> throwError name

sortedIndexes' :: Bool -> V.Vector Row -> VU.Vector Int
sortedIndexes' asc rows = runST $ do
  withIndexes <- VG.thaw (V.indexed rows)
  VA.sortBy ((if asc then compare else flip compare) `on` snd) withIndexes
  sorted <- VG.unsafeFreeze withIndexes
  return $ VU.generate (VG.length rows) (\i -> fst (sorted VG.! i))
