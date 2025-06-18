module DataFrame.Lazy.Internal.DataFrame where

import           Data.IORef
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified DataFrame.Lazy.Internal.Column as C
import           System.FilePath

data DataFrame = DataFrame
  { columns           :: V.Vector (Maybe C.Column)
  , columnIndices     :: !(M.Map T.Text Int)
  , freeIndices       :: !(IORef [Int])
  , dataframeDims     :: !(IORef (Int, Int))   -- (rows , cols)
  , memBudgetBytes    :: !Int                  -- e.g. 512 * 1024 * 1024
  , liveMemBytes      :: !(IORef Int)          -- updated atomically
  , chunkRowTarget    :: !Int                  -- e.g. 100_000
  , spillDir          :: !FilePath
  }