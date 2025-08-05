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
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE FlexibleContexts #-}
module DataFrame.Internal.Types where

import Data.Int ( Int8, Int16, Int32, Int64 )
import Data.Kind (Type, Constraint)
import Data.Maybe (fromMaybe)
import Data.Typeable (Typeable, type (:~:) (..))
import Data.Word ( Word8, Word16, Word32, Word64 )
import Type.Reflection (TypeRep, typeOf, typeRep)
import Data.Type.Equality (TestEquality(..))
import qualified Data.Vector as VB
import qualified Data.Vector.Unboxed as VU

type Columnable' a = (Typeable a, Show a, Ord a, Eq a, Read a)

-- | A type with column representations used to select the
-- "right" representation when specializing the `toColumn` function.
data Rep
  = RBoxed
  | RUnboxed
  | ROptional
  | RGBoxed
  | RGUnboxed
  | RGOptional

-- | Type-level if statement.
type family If (cond :: Bool) (yes :: k) (no :: k) :: k where
  If 'True  yes _  = yes
  If 'False _   no = no

-- | All unboxable types (according to the `vector` package).
type family Unboxable (a :: Type) :: Bool where
  Unboxable Int    = 'True
  Unboxable Int8   = 'True
  Unboxable Int16  = 'True
  Unboxable Int32  = 'True
  Unboxable Int64  = 'True
  Unboxable Word   = 'True
  Unboxable Word8  = 'True
  Unboxable Word16 = 'True
  Unboxable Word32 = 'True
  Unboxable Word64 = 'True
  Unboxable Char   = 'True
  Unboxable Bool   = 'True
  Unboxable Double = 'True
  Unboxable Float  = 'True
  Unboxable _      = 'False

-- | Compute the column representation tag for any ‘a’.
type family KindOf a :: Rep where
  KindOf (Maybe a)     = 'ROptional
  KindOf (VB.Vector a) = 'RGBoxed
  KindOf (VU.Vector a) = 'RGUnboxed
  KindOf a             = If (Unboxable a) 'RUnboxed 'RBoxed

-- | Type-level boolean for constraint/type comparison.
data SBool (b :: Bool) where
  STrue  :: SBool 'True
  SFalse :: SBool 'False

-- | The runtime witness for our type-level branching.
class SBoolI (b :: Bool) where
  sbool :: SBool b

instance SBoolI 'True  where sbool = STrue
instance SBoolI 'False where sbool = SFalse

-- | Type-level function to determine whether or not a type is unboxa
sUnbox :: forall a. SBoolI (Unboxable a) => SBool (Unboxable a)
sUnbox = sbool @(Unboxable a)

type family When (flag :: Bool) (c :: Constraint) :: Constraint where
  When 'True  c = c
  When 'False c = ()          -- empty constraint

type UnboxIf a = When (Unboxable a) (VU.Unbox a)
