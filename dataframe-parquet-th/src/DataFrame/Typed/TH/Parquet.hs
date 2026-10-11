-- | Derives a typed schema from a Parquet file.
module DataFrame.Typed.TH.Parquet (
    deriveSchemaFromParquetFile,
) where

import Control.Monad.IO.Class (liftIO)
import Language.Haskell.TH

import qualified DataFrame.IO.Parquet as Parquet
import DataFrame.Typed.TH.Records (deriveSchema)

-- | Derive a typed schema synonym from a Parquet file (or directory\/glob).
deriveSchemaFromParquetFile :: String -> String -> DecsQ
deriveSchemaFromParquetFile typeName path = do
    df <- liftIO (Parquet.readParquetFiles path)
    deriveSchema typeName df
