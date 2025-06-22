{-# LANGUAGE InstanceSigs #-}
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
            columnsInBOnly = filter (\c -> not (c `elem` (D.columnNames b))) (D.columnNames b)
            columnsInA = D.columnNames a
            addColumns a' b' df name = let
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
        in foldl' (addColumns a b) D.empty (L.union (D.columnNames a) (D.columnNames b))
