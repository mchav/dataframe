{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE Strict #-}
module DataFrame.Internal.Types where

import Data.Int ( Int8, Int16, Int32, Int64 )
import Data.Kind (Type)
import Data.Maybe (fromMaybe)
import Data.Typeable (Typeable, type (:~:) (..))
import Data.Word ( Word8, Word16, Word32, Word64 )
import Type.Reflection (TypeRep, typeOf, typeRep)
import Data.Type.Equality (TestEquality(..))

-- We need an "Object" type as an intermediate representation
-- for rows. Useful for things like sorting and function application.
type Columnable' a = (Typeable a, Show a, Ord a, Eq a)

data RowValue where
    Value :: (Columnable' a) => a -> RowValue

instance Eq RowValue where
    (==) :: RowValue -> RowValue -> Bool
    (Value a) == (Value b) = fromMaybe False $ do
        Refl <- testEquality (typeOf a) (typeOf b)
        return $ a == b

instance Ord RowValue where
    (<=) :: RowValue -> RowValue -> Bool
    (Value a) <= (Value b) = fromMaybe False $ do
        Refl <- testEquality (typeOf a) (typeOf b)
        return $ a <= b

instance Show RowValue where
    show :: RowValue -> String
    show (Value a) = show a

toRowValue :: forall a . (Columnable' a) => a -> RowValue
toRowValue =  Value

-- Convenience functions for types.
unboxableTypes :: TypeRepList '[Int, Int8, Int16, Int32, Int64,
                                Word, Word8, Word16, Word32, Word64,
                                Char, Double, Float, Bool]
unboxableTypes = Cons typeRep (Cons typeRep (Cons typeRep (Cons typeRep (Cons typeRep (Cons typeRep (Cons typeRep (Cons typeRep (Cons typeRep (Cons typeRep (Cons typeRep (Cons typeRep (Cons typeRep (Cons typeRep Nil)))))))))))))

numericTypes :: TypeRepList '[Int, Int8, Int16, Int32, Int64, Double, Float]
numericTypes = Cons typeRep (Cons typeRep (Cons typeRep (Cons typeRep (Cons typeRep (Cons typeRep (Cons typeRep Nil))))))

data TypeRepList (xs :: [Type]) where
  Nil  :: TypeRepList '[]
  Cons :: Typeable x => TypeRep x -> TypeRepList xs -> TypeRepList (x ': xs)

matchesAnyType :: forall a xs. (Typeable a) => TypeRepList xs -> TypeRep a -> Bool
matchesAnyType Nil _ = False
matchesAnyType (Cons ty tys) rep =
  case testEquality ty rep of
    Just Refl -> True
    Nothing   -> matchesAnyType tys rep

testUnboxable :: forall a . Typeable a => TypeRep a -> Bool
testUnboxable x = matchesAnyType unboxableTypes (typeRep @a)

testNumeric :: forall a . Typeable a => TypeRep a -> Bool
testNumeric x = matchesAnyType numericTypes (typeRep @a)
