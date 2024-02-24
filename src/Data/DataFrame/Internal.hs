{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE GADTs #-}

module Data.DataFrame.Internal (
    DataFrame(..),
    Column(..),
    Indexed,
    empty) where

import Data.DataFrame.Util
import Data.Function
import Data.List (groupBy, sortBy)
import Data.Map (Map)
import Data.Typeable (Typeable)
import Data.Vector (Vector)

import qualified Data.Map as M
import qualified Data.Vector as V

-- A DataFrame Column will be a int-indexed vector.
type Indexed a = Vector (Int, a)

data Column where
    MkColumn :: (Typeable a, Show a) => {values :: (Indexed a)} -> Column

instance Show Column where
    show (MkColumn {values = column}) = show column

data DataFrame = DataFrame {
    columns :: Map String Column
}

empty :: DataFrame
empty = DataFrame { columns = M.empty }

instance Show DataFrame where
    show d = let mapList = M.toList (columns d)
                 header = map fst mapList
                 rows = map (map snd) $ groupBy ((==) `on` fst) $ sortBy (compare `on` fst) $ concat $ map (\((MkColumn { values = column' })) -> V.toList $ V.map (applySnd show) column') (map snd mapList)
             in showTable header rows

