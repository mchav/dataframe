{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}

-- | Generic-based opt-in for record-to-schema derivation.
module DataFrame.Typed.Generic (
    -- * Type-level schema derivation
    NameCase (..),
    SchemaOf,
    SchemaOfRaw,
    RepToSchema,
    CamelToSnake,

    -- * Value-level default methods
    genericToColumns,
    genericFromColumns,
    GHasColumns,
) where

import Data.Kind (Type)
import Data.Proxy (Proxy (..))
import qualified Data.Text as T
import qualified Data.Vector as VB
import GHC.Generics (
    C,
    D,
    Generic (..),
    K1 (..),
    M1 (..),
    Meta (..),
    S,
    type (:*:) (..),
 )
import GHC.TypeLits (
    CharToNat,
    ConsSymbol,
    KnownSymbol,
    NatToChar,
    Symbol,
    UnconsSymbol,
    symbolVal,
    type (+),
 )

import Data.Type.Bool (If, type (&&))
import Data.Type.Ord (type (<=?))

import qualified DataFrame.Internal.Column as C
import qualified DataFrame.Internal.DataFrame as D
import DataFrame.Typed.Record (requireColumn)
import DataFrame.Typed.Schema (Append)
import DataFrame.Typed.Util (camelToSnake)

-- | Field-name policy applied to record selectors for 'RepToSchema'.
data NameCase = SnakeCase | IdentityCase

-- | The schema type derived from the 'Rep' of a record type.
type family RepToSchema (nc :: NameCase) (r :: Type -> Type) :: [(Symbol, Type)] where
    RepToSchema nc (M1 D _ f) = RepToSchema nc f
    RepToSchema nc (M1 C _ f) = RepToSchema nc f
    RepToSchema nc (a :*: b) = Append (RepToSchema nc a) (RepToSchema nc b)
    RepToSchema nc (M1 S ('MetaSel ('Just name) _ _ _) (K1 _ a)) =
        '(TransformName nc name, a) ': '[]

type family TransformName (nc :: NameCase) (name :: Symbol) :: Symbol where
    TransformName 'SnakeCase s = CamelToSnake s
    TransformName 'IdentityCase s = s

-- | Type-level camelCase to snake_case, matching 'camelToSnake'.
type family CamelToSnake (s :: Symbol) :: Symbol where
    CamelToSnake s = SnakeStart (UnconsSymbol s)

type family SnakeStart (mu :: Maybe (Char, Symbol)) :: Symbol where
    SnakeStart 'Nothing = ""
    SnakeStart ('Just '(c, r)) =
        ConsSymbol (ToLowerChar c) (SnakeRest (UnconsSymbol r))

type family SnakeRest (mu :: Maybe (Char, Symbol)) :: Symbol where
    SnakeRest 'Nothing = ""
    SnakeRest ('Just '(c, r)) =
        SnakeStep (IsUpperChar c) c (SnakeRest (UnconsSymbol r))

type family SnakeStep (up :: Bool) (c :: Char) (rest :: Symbol) :: Symbol where
    SnakeStep 'True c rest = ConsSymbol '_' (ConsSymbol (ToLowerChar c) rest)
    SnakeStep 'False c rest = ConsSymbol c rest

type family IsUpperChar (c :: Char) :: Bool where
    IsUpperChar c =
        (CharToNat 'A' <=? CharToNat c) && (CharToNat c <=? CharToNat 'Z')

type family ToLowerChar (c :: Char) :: Char where
    ToLowerChar c = If (IsUpperChar c) (NatToChar (CharToNat c + 32)) c

-- | Snake_case schema derived from @a@'s 'Generic' representation.
type SchemaOf a = RepToSchema 'SnakeCase (Rep a)

-- | Identity-cased schema derived from @a@'s 'Generic' representation.
type SchemaOfRaw a = RepToSchema 'IdentityCase (Rep a)

-- | Walks the 'Rep' tree of a record, producing or consuming named columns.
class GHasColumns (r :: Type -> Type) where
    gToColumns :: [r p] -> [(T.Text, C.Column)]
    gFromColumns :: D.DataFrame -> Either T.Text [r p]

instance (GHasColumns f) => GHasColumns (M1 D meta f) where
    gToColumns rs = gToColumns (map unM1 rs)
    gFromColumns df = fmap (map M1) (gFromColumns df)

instance (GHasColumns f) => GHasColumns (M1 C meta f) where
    gToColumns rs = gToColumns (map unM1 rs)
    gFromColumns df = fmap (map M1) (gFromColumns df)

instance (GHasColumns a, GHasColumns b) => GHasColumns (a :*: b) where
    gToColumns rs =
        gToColumns (map (\(x :*: _) -> x) rs)
            ++ gToColumns (map (\(_ :*: y) -> y) rs)
    gFromColumns df = do
        as <- gFromColumns df
        bs <- gFromColumns df
        pure (zipWith (:*:) as bs)

instance
    (KnownSymbol name, C.Columnable a) =>
    GHasColumns
        ( M1
            S
            ('MetaSel ('Just name) su ss ds)
            (K1 i a)
        )
    where
    gToColumns rs =
        let colName = T.pack (camelToSnake (symbolVal (Proxy @name)))
            vals = map (unK1 . unM1) rs
         in [(colName, C.fromList vals)]
    gFromColumns df = do
        let colName = T.pack (camelToSnake (symbolVal (Proxy @name)))
        v <- requireColumn @a colName df
        pure (map (M1 . K1) (VB.toList v))

-- | Default 'DataFrame.Typed.Record.recordsToColumns' for any Generic record.
genericToColumns ::
    forall a. (Generic a, GHasColumns (Rep a)) => [a] -> [(T.Text, C.Column)]
genericToColumns = gToColumns . map from

-- | Default 'DataFrame.Typed.Record.recordsFromFrame' for any Generic record.
genericFromColumns ::
    forall a. (Generic a, GHasColumns (Rep a)) => D.DataFrame -> Either T.Text [a]
genericFromColumns df = fmap (map to) (gFromColumns df)
