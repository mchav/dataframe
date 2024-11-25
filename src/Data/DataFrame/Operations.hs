{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
module Data.DataFrame.Operations (
    addColumn,
    addColumnWithDefault,
    dimensions,
    columnNames,
    getColumn,
    getIntColumn,
    apply,
    applyWhere,
    applyInt,
    applyDouble,
    take,
    sum,
    sumWhere,
    filterWhere
) where

import Data.DataFrame.Internal ( Column(..), DataFrame(..) )
import Data.Type.Equality
    ( type (:~:)(Refl), TestEquality(testEquality) )
import Data.Typeable (Typeable)
import Data.Vector (Vector)
import Prelude hiding (take, sum)
import qualified Prelude as P
import Type.Reflection ( Typeable, TypeRep, typeRep )

import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Set as S
import qualified Data.Vector as V
import Data.Maybe (fromMaybe)
import Data.DataFrame.Util ( applySnd, editDistance )


addColumn :: forall a. (Typeable a, Show a)
          => T.Text     -- Column Name
          -> [a]        -- Data to add to column
          -> DataFrame  -- DataFrame to add to column
          -> DataFrame
addColumn name xs d = let indexedValues = V.zip (V.fromList [0..]) (V.fromList xs)
    in d { columns = M.insert name (MkColumn indexedValues) (columns d),
           _columnNames = if name `elem` _columnNames d
                            then _columnNames d
                          else name: _columnNames d }

addColumnWithDefault :: forall a. (Typeable a, Show a)
          => a          -- Default Value
          -> T.Text     -- Column name
          -> [a]        -- Data to add to column
          -> DataFrame  -- DataFrame to add to column
          -> DataFrame
addColumnWithDefault defaultValue name xs d = let
        (rows, _) = dimensions d
        values = xs ++ repeat defaultValue
    in addColumn name (P.take rows values) d

getColumn' :: forall a. (Typeable a, Show a)
          => Maybe T.Text -- Call point
          -> T.Text     -- Column Name
          -> DataFrame  -- DataFrame to get column from
          -> Vector (Int, a)
getColumn' callPoint name d = case name `M.lookup` columns d of
    Nothing -> error $ "\n\n\ESC[31m[ERROR]\ESC[0m Column not found: " ++
                       T.unpack name ++ " for operation " ++
                       T.unpack (fromMaybe "getColumn" callPoint) ++
                       "\n\tDid you mean " ++ guessColumnName (T.unpack name) d ++ "?\n\n"
    Just (MkColumn (column :: Vector (Int, b))) ->
        let repb :: Type.Reflection.TypeRep b = Type.Reflection.typeRep @b
            repa :: Type.Reflection.TypeRep a = Type.Reflection.typeRep @a
            cp = T.unpack $ fromMaybe "getColumn" callPoint
        in case repa `testEquality` repb of
            Nothing -> error ("\n\n\ESC[31m[Error] Wrong type specified for column: " ++
                              T.unpack name ++
                              "\n\tTried to get a column of type: " ++ show repa ++
                              " but column was of type: " ++ show repb ++
                              "\n\tWhen calling function: " ++ cp ++ "\ESC[0m\n\n")
            Just Refl -> column

guessColumnName :: String -> DataFrame -> String
guessColumnName userInput df = snd
                             $ minimum
                             $ map ((\k -> (editDistance userInput k, k)) . T.unpack)
                             (M.keys $ columns df)

getColumn :: forall a. (Typeable a, Show a)
          => T.Text     -- Column Name
          -> DataFrame  -- DataFrame to get column from
          -> Vector (Int, a)
getColumn = getColumn' Nothing


getIntColumn :: T.Text -> DataFrame -> Vector (Int, Int)
getIntColumn = getColumn' Nothing

apply :: forall b c. (Typeable b, Typeable c, Show b, Show c)
      => T.Text     -- Column name
      -> (b -> c)   -- function to apply
      -> DataFrame  -- DataFrame to apply operation to
      -> DataFrame
apply columnName f d =
    d { columns = M.alter alteration columnName (columns d) }
    where
        alteration :: Maybe Column -> Maybe Column
        alteration c' = case c' of
            Nothing -> error $ "\n\n\ESC[31m[ERROR]\ESC[0m Column not found: " ++
                       T.unpack columnName ++ " for operation apply" ++
                       "\n\tDid you mean " ++ guessColumnName (T.unpack columnName) d ++ "?\n\n"
            Just (MkColumn (column' :: Vector (Int, a)))  ->
                let repb :: Type.Reflection.TypeRep b = Type.Reflection.typeRep @b
                    repa :: Type.Reflection.TypeRep a = Type.Reflection.typeRep @a
                in case repa `testEquality` repb of
                    Nothing -> error (show repb ++ " function cannot be applied to " ++ show repa ++ " in column: " ++ show columnName)
                    Just Refl -> Just (MkColumn (V.map (applySnd f) column'))

applyInt :: (Typeable b, Show b)
         => T.Text       -- Column name
         -> (Int -> b)   -- function to apply
         -> DataFrame    -- DataFrame to apply operation to
         -> DataFrame
applyInt = apply

applyDouble :: (Typeable b, Show b)
            => T.Text          -- Column name
            -> (Double -> b)   -- function to apply
            -> DataFrame       -- DataFrame to apply operation to
            -> DataFrame
applyDouble = apply

applyWhere :: forall a b c. (Typeable a, Typeable b, Show a, Show b)
           => T.Text      -- Criterion Column
           -> (a -> Bool) -- Filter condition
           -> T.Text      -- Column name
           -> (b -> b)    -- function to apply
           -> DataFrame   -- DataFrame to apply operation to
           -> DataFrame
applyWhere filterColumnName condition columnName f df = let
        filterColumn = getColumn filterColumnName df
        i = V.find (condition . snd) filterColumn
    in case i of
        Nothing -> df
        Just (i', _) -> let
                valueColumn = getColumn columnName df
                updated = V.toList $ V.map (\(index, value) -> if index == i' then f value else value) valueColumn 
            in addColumn columnName updated df

-- Take the first n rows of a DataFrame.
take :: Int -> DataFrame -> DataFrame
take n d = d { columns = M.map
                       (\(MkColumn column') -> MkColumn (V.take n column')) (columns d) }

-- Get DataFrame dimensions.
dimensions :: DataFrame -> (Int, Int)
dimensions d = (numRows, numColumns)
    where columnSize (MkColumn column') = V.length column'
          numRows = foldr (max . columnSize . snd) 0 (M.toList (columns d))
          numColumns = M.size $ columns d

-- Get column names of the DataFrame in Alphabetical order.
columnNames :: DataFrame -> [T.Text]
columnNames = _columnNames

sum :: T.Text -> DataFrame -> Int
sum name df = V.sum $ V.map snd $ getColumn' (Just "sum") name df

sumWhere :: (Typeable a, Show a) => T.Text -> (a -> Bool) -> T.Text -> DataFrame -> Int
sumWhere filterColumnName condition columnName df = let
            filterColumn = getColumn filterColumnName df
            indexes = S.fromList $ V.toList $ V.map fst $ V.filter (condition . snd) filterColumn
        in if indexes == S.empty
           then 0
           else V.sum $ V.map snd $ V.filter (\(i, v) -> i `S.member` indexes) $ getColumn' (Just "sum") columnName df

filterWhere :: (Typeable a, Show a)
            => T.Text
            -> (a -> Bool)
            -> DataFrame 
            -> DataFrame
filterWhere filterColumnName condition df = let
        filterColumn = getColumn filterColumnName df
        indexes = S.fromList $ V.toList $ V.map fst $ V.filter (condition . snd) filterColumn
        f (MkColumn (column :: Vector (Int, b))) = MkColumn $ V.filter (\(i, v) -> i `S.member` indexes) column
    in df { columns = M.map f (columns df) }
