{-# LANGUAGE OverloadedStrings #-}
module DataFrame
  ( module D,
    (|>),
    printSessionSchema
  )
where

import DataFrame.Internal.Types as D
import DataFrame.Internal.Expression as D
import DataFrame.Internal.Parsing as D
import DataFrame.Internal.Column as D
import DataFrame.Internal.DataFrame as D hiding (columnIndices, columns)
import DataFrame.Internal.Row as D hiding (mkRowRep)
import DataFrame.Errors as D
import DataFrame.Operations.Core as D
import DataFrame.Operations.Merge as D
import DataFrame.Operations.Join as D
import DataFrame.Operations.Subset as D
import DataFrame.Operations.Sorting as D
import DataFrame.Operations.Statistics as D
import DataFrame.Operations.Transformations as D
import DataFrame.Operations.Typing as D
import DataFrame.Operations.Aggregation as D
import DataFrame.Display.Terminal.Plot as D
import DataFrame.IO.CSV as D
-- Support for Parquet is still experimental
import DataFrame.IO.Parquet as D

import Data.Function
import Data.List
import qualified Data.Text as T
import qualified Data.Text.IO as T

(|>) = (&)

printSessionSchema :: D.DataFrame -> IO ()
printSessionSchema df = T.putStrLn $ let
    (lhs, rhs) = foldr go ([], []) (D.columnNames df)
    columnRep name = let
        colType = T.pack (D.columnTypeString (D.unsafeGetColumn name df))
      in "F.col @(" <> colType <> ") \"" <> name <> "\""
    go name (l, r) = (T.toLower name:l, columnRep name:r)
  in T.unlines [":{", "{-# LANGUAGE TypeApplications #-}",
                "import qualified DataFrame.Functions as F",
                "import Data.Text (Text)",
                "(" <> T.intercalate "," lhs <> ")" <> " = " <> "(" <> T.intercalate "," rhs <> ")",
                ":}"]
