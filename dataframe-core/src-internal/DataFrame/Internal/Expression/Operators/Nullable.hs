{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UndecidableSuperClasses #-}

-- | Nullable-aware operators working across @Maybe a@ and @a@ operands.
module DataFrame.Internal.Expression.Operators.Nullable (
    -- * Type family
    BaseType,

    -- * Arithmetic class
    NullableArithOp (..),

    -- * Comparison class
    NullableCmpOp (..),

    -- * Generalized nullable lift classes
    NullLift1Op (..),
    NullLift2Op (..),

    -- * Result-type type families (drive inference in nullLift / nullLift2)
    NullLift1Result,
    NullLift2Result,

    -- * Result-type type family for comparison operators
    NullCmpResult,

    -- * Numeric widening
    NumericWidenOp (..),
    widenArithOp,
    widenCmpOp,
    WidenResult,

    -- * Division widening (integral × integral → Double)
    DivWidenOp (..),
    divArithOp,
    WidenResultDiv,
) where

import Data.Int (Int32, Int64)
import DataFrame.Internal.Column (Columnable)
import DataFrame.Internal.Column.Types (Promote, PromoteDiv)

-- | Strip one layer of 'Maybe'.
type family BaseType a where
    BaseType (Maybe a) = a
    BaseType a = a

-- | Arithmetic binary operations over nullable and non-nullable operands.
class
    ( Columnable a
    , Columnable b
    , Columnable c
    ) =>
    NullableArithOp a b c
        | a b -> c
    where
    -- | Lift an arithmetic function over inner values; 'Nothing' wins.
    nullArithOp ::
        (BaseType a -> BaseType a -> BaseType a) ->
        a ->
        b ->
        c

-- | Compute the result type of a nullable comparison.
type family NullCmpResult a b where
    NullCmpResult (Maybe a) b = Maybe Bool
    NullCmpResult a (Maybe b) = Maybe Bool
    NullCmpResult a b = Bool

-- | Comparison binary operations over nullable and non-nullable operands.
class
    ( Columnable a
    , Columnable b
    , Columnable e
    ) =>
    NullableCmpOp a b e
    where
    -- | Lift a comparison over inner values with three-valued logic.
    nullCmpOp ::
        (BaseType a -> BaseType a -> Bool) ->
        a ->
        b ->
        e

-- | Non-nullable by non-nullable: apply directly, no wrapping.
instance
    {-# OVERLAPPABLE #-}
    (Columnable a, a ~ BaseType a) =>
    NullableArithOp a a a
    where
    nullArithOp f = f

instance
    {-# OVERLAPPABLE #-}
    (Columnable a, Columnable Bool, a ~ BaseType a) =>
    NullableCmpOp a a Bool
    where
    nullCmpOp f = f

-- | Nullable × Non-nullable: 'Nothing' short-circuits.
instance
    (Columnable a, Columnable (Maybe a)) =>
    NullableArithOp (Maybe a) a (Maybe a)
    where
    nullArithOp _f Nothing _ = Nothing
    nullArithOp f (Just x) y = Just (f x y)

instance
    (Columnable a, Columnable (Maybe a), Columnable (Maybe Bool)) =>
    NullableCmpOp (Maybe a) a (Maybe Bool)
    where
    nullCmpOp _f Nothing _ = Nothing
    nullCmpOp f (Just x) y = Just (f x y)

-- | Non-nullable × Nullable: 'Nothing' short-circuits.
instance
    ( Columnable a
    , Columnable (Maybe a)
    , a ~ BaseType a
    ) =>
    NullableArithOp a (Maybe a) (Maybe a)
    where
    nullArithOp _f _ Nothing = Nothing
    nullArithOp f x (Just y) = Just (f x y)

instance
    ( Columnable a
    , Columnable (Maybe a)
    , Columnable (Maybe Bool)
    , a ~ BaseType a
    ) =>
    NullableCmpOp a (Maybe a) (Maybe Bool)
    where
    nullCmpOp _f _ Nothing = Nothing
    nullCmpOp f x (Just y) = Just (f x y)

-- | Nullable × Nullable: either 'Nothing' short-circuits.
instance
    {-# OVERLAPPING #-}
    (Columnable a, Columnable (Maybe a)) =>
    NullableArithOp (Maybe a) (Maybe a) (Maybe a)
    where
    nullArithOp _f Nothing _ = Nothing
    nullArithOp _f _ Nothing = Nothing
    nullArithOp f (Just x) (Just y) = Just (f x y)

instance
    {-# OVERLAPPING #-}
    (Columnable a, Columnable (Maybe a), Columnable (Maybe Bool)) =>
    NullableCmpOp (Maybe a) (Maybe a) (Maybe Bool)
    where
    nullCmpOp _f Nothing _ = Nothing
    nullCmpOp _f _ Nothing = Nothing
    nullCmpOp f (Just x) (Just y) = Just (f x y)

-- ift a unary function over a column expression, propagating 'Nothing'.

-- | Compute the result type of a nullable unary lift.
type family NullLift1Result a r where
    NullLift1Result (Maybe a) r = Maybe r
    NullLift1Result a r = r

class
    ( Columnable a
    , Columnable r
    , Columnable c
    ) =>
    NullLift1Op a r c
    where
    applyNull1 :: (BaseType a -> r) -> a -> c

-- | Non-nullable: apply directly.
instance
    {-# OVERLAPPABLE #-}
    (Columnable a, Columnable r, a ~ BaseType a) =>
    NullLift1Op a r r
    where
    applyNull1 f = f

-- | Nullable: propagate 'Nothing'.
instance
    {-# OVERLAPPING #-}
    (Columnable a, Columnable r, Columnable (Maybe r)) =>
    NullLift1Op (Maybe a) r (Maybe r)
    where
    applyNull1 _ Nothing = Nothing
    applyNull1 f (Just x) = Just (f x)

-- ift a binary function over two column expressions, propagating 'Nothing'.

-- | Compute the result type of a nullable binary lift.
type family NullLift2Result a b r where
    NullLift2Result (Maybe a) b r = Maybe r
    NullLift2Result a (Maybe b) r = Maybe r
    NullLift2Result a b r = r

class
    ( Columnable a
    , Columnable b
    , Columnable r
    , Columnable c
    ) =>
    NullLift2Op a b r c
    where
    applyNull2 :: (BaseType a -> BaseType b -> r) -> a -> b -> c

-- | Both non-nullable: apply directly.
instance
    {-# OVERLAPPABLE #-}
    (Columnable a, Columnable b, Columnable r, a ~ BaseType a, b ~ BaseType b) =>
    NullLift2Op a b r r
    where
    applyNull2 f = f

-- | Left nullable: 'Nothing' short-circuits.
instance
    {-# OVERLAPPABLE #-}
    (Columnable a, Columnable b, Columnable r, Columnable (Maybe r), b ~ BaseType b) =>
    NullLift2Op (Maybe a) b r (Maybe r)
    where
    applyNull2 _ Nothing _ = Nothing
    applyNull2 f (Just x) y = Just (f x y)

-- | Right nullable: 'Nothing' short-circuits.
instance
    {-# OVERLAPPABLE #-}
    (Columnable a, Columnable b, Columnable r, Columnable (Maybe r), a ~ BaseType a) =>
    NullLift2Op a (Maybe b) r (Maybe r)
    where
    applyNull2 _ _ Nothing = Nothing
    applyNull2 f x (Just y) = Just (f x y)

-- | Both nullable: either 'Nothing' short-circuits.
instance
    {-# OVERLAPPING #-}
    (Columnable a, Columnable b, Columnable r, Columnable (Maybe r)) =>
    NullLift2Op (Maybe a) (Maybe b) r (Maybe r)
    where
    applyNull2 _ Nothing _ = Nothing
    applyNull2 _ _ Nothing = Nothing
    applyNull2 f (Just x) (Just y) = Just (f x y)

-- | Widen two numeric base types to their promoted common type.
class (Columnable (Promote a b)) => NumericWidenOp a b where
    widen1 :: a -> Promote a b
    widen2 :: b -> Promote a b

-- | Same type: identity coercions.
instance {-# OVERLAPPING #-} (Columnable a) => NumericWidenOp a a where
    widen1 = id
    widen2 = id

instance NumericWidenOp Int Double where widen1 = fromIntegral; widen2 = id
instance NumericWidenOp Double Int where
    widen1 = id
    widen2 = fromIntegral
instance NumericWidenOp Float Double where widen1 = realToFrac; widen2 = id
instance NumericWidenOp Double Float where
    widen1 = id
    widen2 = realToFrac
instance NumericWidenOp Int32 Float where widen1 = fromIntegral; widen2 = id
instance NumericWidenOp Float Int32 where
    widen1 = id
    widen2 = fromIntegral
instance NumericWidenOp Int32 Double where widen1 = fromIntegral; widen2 = id
instance NumericWidenOp Double Int32 where
    widen1 = id
    widen2 = fromIntegral
instance NumericWidenOp Int64 Float where widen1 = fromIntegral; widen2 = id
instance NumericWidenOp Float Int64 where
    widen1 = id
    widen2 = fromIntegral
instance NumericWidenOp Int64 Double where widen1 = fromIntegral; widen2 = id
instance NumericWidenOp Double Int64 where
    widen1 = id
    widen2 = fromIntegral

-- | Apply an arithmetic function after widening both operands.
widenArithOp ::
    forall a b.
    (NumericWidenOp a b) =>
    (Promote a b -> Promote a b -> Promote a b) ->
    a ->
    b ->
    Promote a b
widenArithOp f x y = f (widen1 @a @b x) (widen2 @a @b y)

-- | Apply a comparison function after widening both operands.
widenCmpOp ::
    forall a b.
    (NumericWidenOp a b) =>
    (Promote a b -> Promote a b -> Bool) ->
    a ->
    b ->
    Bool
widenCmpOp f x y = f (widen1 @a @b x) (widen2 @a @b y)

-- | Result type of a widening binary operator, accounting for nullables.
type WidenResult a b = NullLift2Result a b (Promote (BaseType a) (BaseType b))

-- | Like 'NumericWidenOp' but uses 'PromoteDiv' (integral gives Double).
class (Columnable (PromoteDiv a b)) => DivWidenOp a b where
    divWiden1 :: a -> PromoteDiv a b
    divWiden2 :: b -> PromoteDiv a b

-- Floating same-type (identity)
instance DivWidenOp Double Double where divWiden1 = id; divWiden2 = id
instance DivWidenOp Float Float where divWiden1 = id; divWiden2 = id

-- Mixed Double/Float
instance DivWidenOp Double Float where divWiden1 = id; divWiden2 = realToFrac
instance DivWidenOp Float Double where divWiden1 = realToFrac; divWiden2 = id

-- Double beats integral
instance DivWidenOp Double Int where divWiden1 = id; divWiden2 = fromIntegral
instance DivWidenOp Int Double where divWiden1 = fromIntegral; divWiden2 = id
instance DivWidenOp Double Int32 where divWiden1 = id; divWiden2 = fromIntegral
instance DivWidenOp Int32 Double where divWiden1 = fromIntegral; divWiden2 = id
instance DivWidenOp Double Int64 where divWiden1 = id; divWiden2 = fromIntegral
instance DivWidenOp Int64 Double where divWiden1 = fromIntegral; divWiden2 = id

-- Float beats integral
instance DivWidenOp Float Int where divWiden1 = id; divWiden2 = fromIntegral
instance DivWidenOp Int Float where divWiden1 = fromIntegral; divWiden2 = id
instance DivWidenOp Float Int32 where divWiden1 = id; divWiden2 = fromIntegral
instance DivWidenOp Int32 Float where divWiden1 = fromIntegral; divWiden2 = id
instance DivWidenOp Float Int64 where divWiden1 = id; divWiden2 = fromIntegral
instance DivWidenOp Int64 Float where divWiden1 = fromIntegral; divWiden2 = id

-- Integral × integral → Double
instance DivWidenOp Int Int where
    divWiden1 = fromIntegral
    divWiden2 = fromIntegral
instance DivWidenOp Int32 Int32 where
    divWiden1 = fromIntegral
    divWiden2 = fromIntegral
instance DivWidenOp Int64 Int64 where
    divWiden1 = fromIntegral
    divWiden2 = fromIntegral
instance DivWidenOp Int Int32 where
    divWiden1 = fromIntegral
    divWiden2 = fromIntegral
instance DivWidenOp Int32 Int where
    divWiden1 = fromIntegral
    divWiden2 = fromIntegral
instance DivWidenOp Int Int64 where
    divWiden1 = fromIntegral
    divWiden2 = fromIntegral
instance DivWidenOp Int64 Int where
    divWiden1 = fromIntegral
    divWiden2 = fromIntegral
instance DivWidenOp Int32 Int64 where
    divWiden1 = fromIntegral
    divWiden2 = fromIntegral
instance DivWidenOp Int64 Int32 where
    divWiden1 = fromIntegral
    divWiden2 = fromIntegral

-- | Apply an arithmetic function after widening both operands via 'PromoteDiv'.
divArithOp ::
    forall a b.
    (DivWidenOp a b) =>
    (PromoteDiv a b -> PromoteDiv a b -> PromoteDiv a b) ->
    a ->
    b ->
    PromoteDiv a b
divArithOp f x y = f (divWiden1 @a @b x) (divWiden2 @a @b y)

-- | Result type of a division-widening binary operator.
type WidenResultDiv a b =
    NullLift2Result a b (PromoteDiv (BaseType a) (BaseType b))
