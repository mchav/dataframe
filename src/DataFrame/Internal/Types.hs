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

type Columnable' a = (Typeable a, Show a, Ord a, Eq a)
