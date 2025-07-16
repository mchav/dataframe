{-# LANGUAGE InstanceSigs #-}
module DataFrame.Operations.Merge where

import qualified Data.List as L
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified DataFrame.Internal.Column as D
import qualified DataFrame.Internal.DataFrame as D
import qualified DataFrame.Operations.Core as D

import Data.Maybe

instance Semigroup D.DataFrame where
    (<>) :: D.DataFrame -> D.DataFrame -> D.DataFrame
    (<>) a b = let
            columnsInBOnly = filter (\c -> c `notElem` D.columnNames b) (D.columnNames b)
            columnsInA = D.columnNames a
            addColumns a' b' df name
                | fst (D.dimensions a') == 0 && fst (D.dimensions b') == 0 = df
                | fst (D.dimensions a') == 0 = fromMaybe df $ do
                    col <- D.getColumn name b'
                    pure $ D.insertColumn name col df
                | fst (D.dimensions b') == 0 = fromMaybe df $ do
                    col <- D.getColumn name a'
                    pure $ D.insertColumn name col df
                | otherwise = let
                        numColumnsA = (fst $ D.dimensions a')
                        numColumnsB = (fst $ D.dimensions b')
                        numColumns = max numColumnsA numColumnsB
                        optA = D.getColumn name a'
                        optB = D.getColumn name b'
                    in case optB of
                        Nothing -> case optA of
                            Nothing  -> D.insertColumn name (D.fromList ([] :: [T.Text])) df
                            Just a'' -> D.insertColumn name (D.expandColumn numColumnsB a'') df
                        Just b'' -> case optA of
                            Nothing  -> D.insertColumn name (D.leftExpandColumn numColumnsA b'') df
                            Just a'' -> fromMaybe df $ do
                                concatedColumns <- D.concatColumns a'' b''
                                pure $ D.insertColumn name concatedColumns df
        in L.foldl' (addColumns a b) D.empty (D.columnNames a `L.union` D.columnNames b)

instance Monoid D.DataFrame where
  mempty = D.empty


