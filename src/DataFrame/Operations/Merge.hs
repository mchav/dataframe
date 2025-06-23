{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE Strict #-}
module DataFrame.Operations.Merge where

import qualified Data.List as L
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified DataFrame.Internal.Column as D
import qualified DataFrame.Internal.DataFrame as D
import qualified DataFrame.Operations.Core as D

instance Semigroup D.DataFrame where
    (<>) :: D.DataFrame -> D.DataFrame -> D.DataFrame
    (<>) a b = let
            columnsInBOnly = filter (\c -> c `notElem` D.columnNames b) (D.columnNames b)
            columnsInA = D.columnNames a
            addColumns a' b' df name
                | fst (D.dimensions a') == 0 && fst (D.dimensions b') == 0 = df
                | fst (D.dimensions a') == 0 = D.insertColumn' name (D.getColumn name b') df
                | fst (D.dimensions b') == 0 = D.insertColumn' name (D.getColumn name a') df
                | otherwise = let
                        numColumnsA = (fst $ D.dimensions a')
                        numColumnsB = (fst $ D.dimensions b')
                        numColumns = max numColumnsA numColumnsB
                        optA = D.getColumn name a'
                        optB = D.getColumn name b'
                    in case optB of
                        Nothing -> case optA of
                            Nothing  -> D.insertColumn' name (Just (D.toColumn ([] :: [T.Text]))) df
                            Just a'' -> D.insertColumn' name (Just (D.expandColumn numColumnsB a'')) df
                        Just b'' -> case optA of
                            Nothing  -> D.insertColumn' name (Just (D.leftExpandColumn numColumnsA b'')) df
                            Just a'' -> D.insertColumn' name (D.concatColumns a'' b'') df
        in L.foldl' (addColumns a b) D.empty (D.columnNames a `L.union` D.columnNames b)

instance Monoid D.DataFrame where
  mempty = D.empty


