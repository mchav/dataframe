{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

module DataFrame.Internal.Types where

import Data.Int (Int16, Int32, Int64, Int8)
import Data.Kind (Constraint, Type)
import Data.Maybe (fromMaybe)
import Data.Type.Equality (TestEquality (..))
import Data.Typeable (Typeable, type (:~:) (..))
import qualified Data.Vector as VB
import qualified Data.Vector.Unboxed as VU
import Data.Word (Word16, Word32, Word64, Word8)
import Type.Reflection (TypeRep, typeOf, typeRep)

type Columnable' a = (Typeable a, Show a, Ord a, Eq a, Read a)

{- | A type with column representations used to select the
"right" representation when specializing the `toColumn` function.
-}
data Rep
    = RBoxed
    | RUnboxed
    | ROptional

-- | Type-level if statement.
type family If (cond :: Bool) (yes :: k) (no :: k) :: k where
    If 'True yes _ = yes
    If 'False _ no = no

-- | All unboxable types (according to the `vector` package).
type family Unboxable (a :: Type) :: Bool where
    Unboxable Int = 'True
    Unboxable Int8 = 'True
    Unboxable Int16 = 'True
    Unboxable Int32 = 'True
    Unboxable Int64 = 'True
    Unboxable Word = 'True
    Unboxable Word8 = 'True
    Unboxable Word16 = 'True
    Unboxable Word32 = 'True
    Unboxable Word64 = 'True
    Unboxable Char = 'True
    Unboxable Bool = 'True
    Unboxable Double = 'True
    Unboxable Float = 'True
    Unboxable _ = 'False

-- | All unboxable types (according to the `vector` package).
type family Numeric (a :: Type) :: Bool where
    Numeric Int = 'True
    Numeric Int8 = 'True
    Numeric Int16 = 'True
    Numeric Int32 = 'True
    Numeric Int64 = 'True
    Numeric Word = 'True
    Numeric Word8 = 'True
    Numeric Word16 = 'True
    Numeric Word32 = 'True
    Numeric Word64 = 'True
    Numeric Double = 'True
    Numeric Float = 'True
    Numeric _ = 'False

-- | Compute the column representation tag for any ‘a’.
type family KindOf a :: Rep where
    KindOf (Maybe a) = 'ROptional
    KindOf a = If (Unboxable a) 'RUnboxed 'RBoxed

-- | Type-level boolean for constraint/type comparison.
data SBool (b :: Bool) where
    STrue :: SBool 'True
    SFalse :: SBool 'False

-- | The runtime witness for our type-level branching.
class SBoolI (b :: Bool) where
    sbool :: SBool b

instance SBoolI 'True where sbool = STrue
instance SBoolI 'False where sbool = SFalse

-- | Type-level function to determine whether or not a type is unboxa
sUnbox :: forall a. (SBoolI (Unboxable a)) => SBool (Unboxable a)
sUnbox = sbool @(Unboxable a)

sNumeric :: forall a. (SBoolI (Numeric a)) => SBool (Numeric a)
sNumeric = sbool @(Numeric a)

type family When (flag :: Bool) (c :: Constraint) :: Constraint where
    When 'True c = c
    When 'False c = () -- empty constraint

type UnboxIf a = When (Unboxable a) (VU.Unbox a)
