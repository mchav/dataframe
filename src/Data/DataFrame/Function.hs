{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE InstanceSigs #-}

module Data.DataFrame.Function where

import qualified Data.Text as T

import Data.Typeable ( Typeable, type (:~:)(Refl) )
import Data.Type.Equality (TestEquality(testEquality))
import Type.Reflection (typeRep, typeOf)

type Val a = (Typeable a, Show a, Ord a)

-- A GADT to wrap functions so we can have hetegeneous lists of functions.
data Function where
    F1 :: forall a b . (Val a, Val b) => (a -> b) -> Function
    F2 :: forall a b c . (Val a, Val b, Val c) => (a -> b -> c) -> Function
    F3 :: forall a b c d . (Val a, Val b, Val c, Val d) => (a -> b -> c -> d) -> Function
    F4 :: forall a b c d e . (Val a, Val b, Val c, Val d, Val e) => (a -> b -> c -> d -> e) -> Function
    Cond :: forall a . (Typeable a, Show a, Ord a) => (a -> Bool) -> Function
    ICond :: forall a . (Typeable a, Show a, Ord a) => (Int -> a -> Bool) -> Function

-- Helper class to do the actual wrapping
class WrapFunction a where
    wrapFunction :: a -> Function

-- Instance for 1-argument functions
instance (Val a, Val b) => WrapFunction (a -> b) where
    wrapFunction :: (Val a, Val b) => (a -> b) -> Function
    wrapFunction = F1

-- Instance for 2-argument functions
instance {-# INCOHERENT #-} (Val a, Val b, Val c) => WrapFunction (a -> b -> c) where
    wrapFunction :: (Val a, Val b, Val c) => (a -> b -> c) -> Function
    wrapFunction = F2

-- Instance for 3-argument functions
instance {-# INCOHERENT #-} (Val a, Val b, Val c, Val d) => WrapFunction (a -> b -> c -> d) where
    wrapFunction :: (Val a, Val b, Val c, Val d) => (a -> b -> c -> d) -> Function
    wrapFunction = F3

instance {-# INCOHERENT #-} (Val a, Val b, Val c, Val d, Val e) => WrapFunction (a -> b -> c -> d -> e) where
    wrapFunction :: (Val a, Val b, Val c, Val d, Val e) => (a -> b -> c -> d -> e) -> Function
    wrapFunction = F4

-- The main function that wraps arbitrary functions
func :: forall fn . WrapFunction fn => fn -> Function
func = wrapFunction

data FuncArg where
    Arg :: (Val a) => a -> FuncArg

funcApply :: forall c . (Val c) => [FuncArg] -> Function ->  c
funcApply [] _ = error "Empty args"
funcApply [Arg (x :: a')] (F1 (f :: (a -> b))) = case testEquality (typeRep @a') (typeRep @a) of
        Just Refl -> case testEquality (typeOf (f x)) (typeRep @c) of
            Just Refl -> f x
            Nothing -> error "Result type mismatch"
        Nothing -> error "Arg type mismatch"
funcApply (Arg (x :: a') : xs) (F2 (f :: (a -> b))) = case testEquality (typeOf x) (typeRep @a) of
        Just Refl -> funcApply xs (F1 (f x))
        Nothing -> error "Arg type mismatch"
funcApply (Arg (x :: a') : xs) (F3 (f :: (a -> b))) = case testEquality (typeOf x) (typeRep @a) of
        Just Refl -> funcApply xs (F2 (f x))
        Nothing -> error "Arg type mismatch"
funcApply (Arg (x :: a') : xs) (F4 (f :: (a -> b))) = case testEquality (typeOf x) (typeRep @a) of
        Just Refl -> funcApply xs (F3 (f x))
        Nothing -> error "Arg type mismatch"
