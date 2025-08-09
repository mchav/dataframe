{-# LANGUAGE GADTs               #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
module DataFrame.Internal.Schema where

import qualified Data.Map as M
import qualified Data.Proxy as P
import qualified Data.Text as T

import DataFrame.Internal.Column
import Data.Maybe
import Data.Type.Equality (type (:~:)(Refl), TestEquality (..))
import Type.Reflection (typeRep)

data SchemaType where
    SType :: Columnable a => P.Proxy a -> SchemaType

instance Show SchemaType where
    show (SType (_ :: P.Proxy a)) = show (typeRep @a)

instance Eq SchemaType where
    (==) (SType (_ :: P.Proxy a)) (SType (_ :: P.Proxy b)) = isJust (testEquality (typeRep @a) (typeRep @b))

schemaType :: forall a . Columnable a => SchemaType
schemaType = SType (P.Proxy @a)

data Schema = Schema {
    elements :: M.Map T.Text SchemaType
} deriving (Show, Eq)
