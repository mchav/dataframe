{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Data.DataFrame.Operations (
    addColumn,
    dimensions,
    columnNames,
    getColumn,
    apply,
    take,
    sum
) where

import Data.DataFrame.Internal
import Data.DataFrame.Util
import Data.Type.Equality
    ( type (:~:)(Refl), TestEquality(testEquality) )
import Data.Typeable (Typeable)
import Data.Vector (Vector)
import Prelude hiding (take, sum)
import Type.Reflection ( Typeable, TypeRep, typeRep )

import qualified Data.Map as M
import qualified Data.Vector as V


addColumn :: forall a. (Typeable a, Show a)
          => String     -- Column Name
          -> [a]        -- Data to add to column
          -> DataFrame  -- DataFrame to add to column
          -> DataFrame
addColumn name xs d = let indexedValues = V.zip (V.fromList [0..]) (V.fromList xs) 
    in d { columns = M.insert name (MkColumn { values = indexedValues }) (columns d) }


getColumn :: forall a. (Typeable a, Show a)
          => String     -- Column Name
          -> DataFrame  -- DataFrame to get column from
          -> Vector (Int, a)
getColumn name d = case (M.!) (columns d) name of
    (MkColumn {values = column :: Vector (Int, b) }) ->
        let repb :: Type.Reflection.TypeRep b = Type.Reflection.typeRep @b
            repa :: Type.Reflection.TypeRep a = Type.Reflection.typeRep @a
        in case repa `testEquality` repb of
            Nothing -> error ((show repb) ++ " function cannot be applied to " ++ (show repa) ++ " in column: " ++ name)
            Just Refl -> column


apply :: forall b c. (Typeable b, Typeable c, Show b, Show c)
      => String     -- Column name
      -> (b -> c)   -- function to apply
      -> DataFrame  -- DataFrame to apply operation to
      -> DataFrame
apply columnName f d =
    DataFrame { columns = M.alter alteration columnName (columns d) }
    where
        alteration :: Maybe Column -> Maybe Column
        alteration c' = case c' of
            Nothing -> error $ "No such column: " ++ columnName
            Just (MkColumn { values = column' :: Vector (Int, a) })  ->
                let repb :: Type.Reflection.TypeRep b = Type.Reflection.typeRep @b
                    repa :: Type.Reflection.TypeRep a = Type.Reflection.typeRep @a
                in case repa `testEquality` repb of
                    Nothing -> error ((show repb) ++ " function cannot be applied to " ++ (show repa) ++ " in column: " ++ columnName)
                    Just Refl -> Just (MkColumn { values = V.map (applySnd f) column' })


-- Take the first n rows of a DataFrame.
take :: Int -> DataFrame -> DataFrame
take n d = DataFrame { columns = M.map
                       (\(MkColumn { values = column' }) -> (MkColumn { values = V.take n column' })) (columns d) }


-- Get DataFrame dimensions.
dimensions :: DataFrame -> (Int, Int)
dimensions d = (snd $ head $ M.toList $ M.map numRows (columns d) , M.size $ columns d) 
    where numRows (MkColumn { values = column' }) = V.length column'

-- Get column names of the DataFrame in Alphabetical order.
columnNames :: DataFrame -> [String]
columnNames d = M.keys (columns d)


-- Axis on which operations should happen.
data Axis = ColumnAxis | RowAxis

-- Sum elements along an axis.
sum :: Axis -> DataFrame -> DataFrame
sum ColumnAxis d = error  ""
sum RowAxis    d = error  ""
