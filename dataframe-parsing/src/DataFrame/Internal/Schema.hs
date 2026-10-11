{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}

-- | Runtime schema representation, free of any @template-haskell@ dependency.
module DataFrame.Internal.Schema (
    SchemaType (..),
    schemaType,
    Schema (..),
    makeSchema,
    RuntimeSchema (..),
) where

import Data.Kind (Type)
import qualified Data.Map as M
import Data.Maybe (isJust)
import qualified Data.Proxy as P
import qualified Data.Text as T
import Data.Type.Equality (TestEquality (..))
import DataFrame.Internal.Column (Columnable)
import GHC.TypeLits (KnownSymbol, Symbol, symbolVal)
import Type.Reflection (typeRep)

-- | A runtime tag for a column’s element type.
data SchemaType where
    -- | Constructor carrying a 'Proxy' of the element type.
    SType :: (Columnable a, Read a) => P.Proxy a -> SchemaType

-- | Show the underlying element type using 'typeRep'.
instance Show SchemaType where
    show :: SchemaType -> String
    show (SType (_ :: P.Proxy a)) = show (typeRep @a)

-- | Two 'SchemaType's are equal iff their element types are the same.
instance Eq SchemaType where
    (==) :: SchemaType -> SchemaType -> Bool
    (==) (SType (_ :: P.Proxy a)) (SType (_ :: P.Proxy b)) =
        isJust (testEquality (typeRep @a) (typeRep @b))

-- | Construct a 'SchemaType' for the given @a@.
schemaType :: forall a. (Columnable a, Read a) => SchemaType
schemaType = SType (P.Proxy @a)

-- | Logical schema of a 'DataFrame': column names mapped to element types.
newtype Schema = Schema
    { elements :: M.Map T.Text SchemaType
    -- ^ Mapping from column name to its 'SchemaType'; keys are unique.
    }
    deriving (Show, Eq)

-- | Construct a 'Schema' from a list of @(columnName, schemaType)@ pairs.
makeSchema :: [(T.Text, SchemaType)] -> Schema
makeSchema = Schema . M.fromList

-- | The runtime 'Schema' behind a type-level schema, names and element types.
class RuntimeSchema (cols :: [(Symbol, Type)]) where
    runtimeSchema :: Schema

instance RuntimeSchema '[] where
    runtimeSchema = makeSchema []

instance
    (KnownSymbol name, Columnable a, Read a, RuntimeSchema rest) =>
    RuntimeSchema ('(name, a) ': rest)
    where
    runtimeSchema =
        Schema $
            M.insert
                (T.pack (symbolVal (P.Proxy @name)))
                (schemaType @a)
                (elements (runtimeSchema @rest))
