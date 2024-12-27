{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE GADTs #-}
module Data.DataFrame.Function where

import Data.Typeable (Typeable)

-- A GADT to wrap functions so we can have hetegeneous lists of functions.
data Function where
    F :: forall a b . (Typeable a, Show a, Ord a, Typeable b, Show b, Ord b) => (a -> b) -> Function
    Cond :: forall a . (Typeable a, Show a, Ord a) => (a -> Bool) -> Function
    ICond :: forall a . (Typeable a, Show a, Ord a) => (Int -> a -> Bool) -> Function
