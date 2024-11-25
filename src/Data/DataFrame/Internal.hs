{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}

module Data.DataFrame.Internal (
    DataFrame(..),
    Column(..),
    Indexed,
    empty) where

import Data.DataFrame.Util ( applySnd, showTable )
import Data.Function ( on )
import Data.List (groupBy, sortBy, elemIndex)
import Data.Map (Map)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Typeable (Typeable)
import Data.Vector (Vector)

import qualified Data.Map as M
import qualified Data.Vector as V

-- A DataFrame Column will be a int-indexed vector.
type Indexed a = Vector (Int, a)

data Column where
    MkColumn :: (Typeable a, Show a) => (Indexed a) -> Column

instance Show Column where
    show :: Column -> String
    show (MkColumn column) = show column

data DataFrame = DataFrame {
    columns :: Map Text Column,
    _columnNames :: [Text]
}

empty :: DataFrame
empty = DataFrame { columns = M.empty, _columnNames = [] }

instance Show DataFrame where
    show :: DataFrame -> String
    show d = let mapList' = M.toList (columns d)
                 sortedHeader = map fst mapList'
                 header = _columnNames d
                 mapList = map (\h -> mapList' !! fromMaybe 0 (elemIndex h sortedHeader)) header
                 rows = map (map snd) $ groupBy ((==) `on` fst) $ sortBy (compare `on` fst) $ concatMap ((\((MkColumn column')) -> V.toList $ V.map (applySnd show) column') . snd) mapList
             in showTable (map T.unpack header) rows

