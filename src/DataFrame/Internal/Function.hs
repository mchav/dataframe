{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE PatternSynonyms #-}

module DataFrame.Internal.Function where

import qualified Data.Text as T
import qualified Data.Vector as V

import DataFrame.Internal.Types
import Data.Typeable ( Typeable, type (:~:)(Refl) )
import Data.Type.Equality (TestEquality(testEquality))
import Type.Reflection (typeRep, typeOf)

-- A GADT to wrap functions so we can have hetegeneous lists of functions.
data Function where
    F1 :: forall a b . (Columnable' a, Columnable' b) => (a -> b) -> Function
    F2 :: forall a b c . (Columnable' a, Columnable' b, Columnable' c) => (a -> b -> c) -> Function
    F3 :: forall a b c d . (Columnable' a, Columnable' b, Columnable' c, Columnable' d) => (a -> b -> c -> d) -> Function
    F4 :: forall a b c d e . (Columnable' a, Columnable' b, Columnable' c, Columnable' d, Columnable' e) => (a -> b -> c -> d -> e) -> Function
    Cond :: forall a . (Columnable' a) => (a -> Bool) -> Function
    ICond :: forall a . (Columnable' a) => (Int -> a -> Bool) -> Function

-- Helper class to do the actual wrapping
class WrapFunction a where
    wrapFunction :: a -> Function

-- Instance for 1-argument functions
instance (Columnable' a, Columnable' b) => WrapFunction (a -> b) where
    wrapFunction :: (Columnable' a, Columnable' b) => (a -> b) -> Function
    wrapFunction = F1

-- Instance for 2-argument functions
instance {-# INCOHERENT #-} (Columnable' a, Columnable' b, Columnable' c) => WrapFunction (a -> b -> c) where
    wrapFunction :: (Columnable' a, Columnable' b, Columnable' c) => (a -> b -> c) -> Function
    wrapFunction = F2

-- Instance for 3-argument functions
instance {-# INCOHERENT #-} (Columnable' a, Columnable' b, Columnable' c, Columnable' d) => WrapFunction (a -> b -> c -> d) where
    wrapFunction :: (Columnable' a, Columnable' b, Columnable' c, Columnable' d) => (a -> b -> c -> d) -> Function
    wrapFunction = F3

instance {-# INCOHERENT #-} (Columnable' a, Columnable' b, Columnable' c, Columnable' d, Columnable' e) => WrapFunction (a -> b -> c -> d -> e) where
    wrapFunction :: (Columnable' a, Columnable' b, Columnable' c, Columnable' d, Columnable' e) => (a -> b -> c -> d -> e) -> Function
    wrapFunction = F4

-- The main function that wraps arbitrary functions
func :: forall fn . WrapFunction fn => fn -> Function
func = wrapFunction

pattern Empty :: V.Vector a
pattern Empty <- (V.null -> True) where Empty = V.empty 

uncons :: V.Vector a -> Maybe (a, V.Vector a)
uncons Empty = Nothing
uncons v     = Just (V.unsafeHead v, V.unsafeTail v)

pattern (:<|)  :: a -> V.Vector a -> V.Vector a
pattern x :<| xs <- (uncons -> Just (x, xs))

funcApply :: forall c . (Columnable' c) => V.Vector RowValue -> Function ->  c
funcApply Empty _ = error "Empty args"
funcApply (Value (x :: a') :<| Empty) (F1 (f :: (a -> b))) = case testEquality (typeRep @a') (typeRep @a) of
        Just Refl -> case testEquality (typeOf (f x)) (typeRep @c) of
            Just Refl -> f x
            Nothing -> error "Result type mismatch"
        Nothing -> error "Arg type mismatch"
funcApply (Value (x :: a') :<| xs) (F2 (f :: (a -> b))) = case testEquality (typeOf x) (typeRep @a) of
        Just Refl -> funcApply xs (F1 (f x))
        Nothing -> error "Arg type mismatch"
funcApply (Value (x :: a') :<| xs) (F3 (f :: (a -> b))) = case testEquality (typeOf x) (typeRep @a) of
        Just Refl -> funcApply xs (F2 (f x))
        Nothing -> error "Arg type mismatch"
funcApply (Value (x :: a') :<| xs) (F4 (f :: (a -> b))) = case testEquality (typeOf x) (typeRep @a) of
        Just Refl -> funcApply xs (F3 (f x))
        Nothing -> error "Arg type mismatch"
