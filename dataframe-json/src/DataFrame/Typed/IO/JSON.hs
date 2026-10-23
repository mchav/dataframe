{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- | Typed JSON readers: decode, then validate against the schema.
module DataFrame.Typed.IO.JSON (
    readJSON,
    readJSONEither,
) where

import qualified Data.ByteString.Lazy as LBS
import qualified Data.Text as T

import qualified DataFrame.IO.JSON as JSON
import DataFrame.Typed.Freeze (freezeOrThrow, freezeWithError)
import DataFrame.Typed.Schema (KnownSchema)
import DataFrame.Typed.Types (TypedDataFrame)

-- | Read JSON into a typed frame (numbers decode as Maybe Double); throws.
readJSON ::
    forall cols. (KnownSchema cols) => FilePath -> IO (TypedDataFrame cols)
readJSON path = JSON.readJSON path >>= freezeOrThrow @cols

-- | Decode JSON bytes into a typed DataFrame, or explain the failure.
readJSONEither ::
    forall cols.
    (KnownSchema cols) =>
    LBS.ByteString -> Either String (TypedDataFrame cols)
readJSONEither bs = do
    df <- JSON.readJSONEither bs
    either (Left . T.unpack) Right (freezeWithError @cols df)
