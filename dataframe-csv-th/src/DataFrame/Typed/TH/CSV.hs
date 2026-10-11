-- | Derives a typed schema from a CSV file.
module DataFrame.Typed.TH.CSV (
    deriveSchemaFromCsvFile,
    deriveSchemaFromCsvFileWith,
) where

import Control.Monad.IO.Class (liftIO)
import Language.Haskell.TH

import qualified DataFrame.IO.CSV as D
import DataFrame.Typed.TH.Records (deriveSchema)

deriveSchemaFromCsvFile :: String -> String -> DecsQ
deriveSchemaFromCsvFile = deriveSchemaFromCsvFileWith D.defaultReadOptions

deriveSchemaFromCsvFileWith :: D.ReadOptions -> String -> String -> DecsQ
deriveSchemaFromCsvFileWith opts typeName path = do
    df <- liftIO (D.readSeparated opts path)
    deriveSchema typeName df
