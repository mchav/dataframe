-- | Direct SQLite query that returns result column names with the values.
module DataFrame.IO.Persistent.Read.Sqlite (
    runNamedQuery,
) where

import Control.Exception (bracket)
import qualified Data.ByteString as BS
import Data.Text (Text)
import qualified Data.Text.Encoding as TE
import Database.Persist (PersistValue)
import Database.Sqlite (
    StepResult (Done, Row),
    bind,
    close,
    columns,
    finalize,
    open,
    prepare,
    step,
 )
import Database.Sqlite.Internal (Statement (Statement))
import Foreign.C.String (CString)
import Foreign.C.Types (CInt (..))
import Foreign.Ptr (Ptr)

-- @Database.Sqlite@ does not re-export these, so they are bound directly.
foreign import ccall unsafe "sqlite3_column_count"
    c_sqlite3_column_count :: Ptr () -> IO CInt

foreign import ccall unsafe "sqlite3_column_name"
    c_sqlite3_column_name :: Ptr () -> CInt -> IO CString

-- | Run @sql@ with bound params and return column names and every row.
runNamedQuery :: Text -> Text -> [PersistValue] -> IO ([Text], [[PersistValue]])
runNamedQuery dbPath sql params =
    bracket (open dbPath) close $ \conn ->
        bracket (prepare conn sql) finalize $ \stmt@(Statement p) -> do
            bind stmt params
            n <- fromIntegral <$> c_sqlite3_column_count p
            names <- mapM (columnName p) [0 .. n - 1]
            rows <- stepRows stmt
            pure (names, rows)

columnName :: Ptr () -> Int -> IO Text
columnName p i = do
    cstr <- c_sqlite3_column_name p (fromIntegral i)
    TE.decodeUtf8 <$> BS.packCString cstr

stepRows :: Statement -> IO [[PersistValue]]
stepRows stmt = do
    result <- step stmt
    case result of
        Row -> (:) <$> columns stmt <*> stepRows stmt
        Done -> pure []
