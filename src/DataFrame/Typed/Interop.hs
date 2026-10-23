{-# LANGUAGE ScopedTypeVariables #-}

-- | Typed display, rendering and record helpers mirroring "DataFrame".
module DataFrame.Typed.Interop (
    -- * Display and rendering
    display,
    prettyPrint,
    toMarkdown,
    toMarkdown',
    toCsv,
    toCsv',
    toSeparated,

    -- * Records
    fromRecords,
    toRecords,
) where

import qualified Data.Text as T

import qualified DataFrame.Core as Core
import DataFrame.Display (DisplayOptions)
import qualified DataFrame.Display as Display
import DataFrame.Typed.Freeze (thaw)
import DataFrame.Typed.Record (HasSchema (..), fromRecordsTyped, toRecordsTyped)
import DataFrame.Typed.Types (TExpr (..), TypedDataFrame)

-- | Print the frame with the given options.
display :: DisplayOptions -> TypedDataFrame cols -> IO ()
display opts = Display.display opts . thaw

-- | Render a typed expression as source-like text.
prettyPrint :: TExpr cols a -> String
prettyPrint (TExpr e) = Core.prettyPrint e

-- | Render the frame as a Markdown table.
toMarkdown :: TypedDataFrame cols -> T.Text
toMarkdown = Core.toMarkdown . thaw

-- | 'toMarkdown' as a 'String'.
toMarkdown' :: TypedDataFrame cols -> String
toMarkdown' = Core.toMarkdown' . thaw

-- | Render the frame as CSV.
toCsv :: TypedDataFrame cols -> T.Text
toCsv = Core.toCsv . thaw

-- | 'toCsv' as a 'String'.
toCsv' :: TypedDataFrame cols -> String
toCsv' = Core.toCsv' . thaw

-- | Render the frame with the given separator.
toSeparated :: Char -> TypedDataFrame cols -> T.Text
toSeparated sep = Core.toSeparated sep . thaw

-- | Build a typed frame from records; the schema comes from the record type.
fromRecords :: forall a. (HasSchema a) => [a] -> TypedDataFrame (Schema a)
fromRecords = fromRecordsTyped

-- | Parse records back out of a typed frame.
toRecords ::
    forall a. (HasSchema a) => TypedDataFrame (Schema a) -> Either T.Text [a]
toRecords = toRecordsTyped
