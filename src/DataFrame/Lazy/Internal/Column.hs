module DataFrame.Lazy.Internal.Column where

import qualified Data.ByteString.Char8 as C
import           Data.IORef
import           Data.Sequence
import qualified Data.Vector as V
import           Data.Word
import qualified DataFrame.Internal.Column as InMemory
import           System.FilePath

data Block
  = InMem  !InMemory.Column
  | OnDisk !FilePath !Word64 !Word64  -- file, offset, length
  deriving (Eq, Show)

data Column = Column
  { blocks      :: IORef (Seq Block)
  , totalElems  :: IORef Int
  }

appendValue :: C.Columnable a => Column -> a -> IO ()
appendValue col value = do
  buf <- readIORef (blocks col)
  case viewr buf of
    EmptyR -> newBlockAndInsert
    init :> lastBlk -> case lastBlk of
      InMem inmemcol | InMemory.columnLength vec < chunkRowTarget DF -> do
         writeIORef (blocks col) (init :> InMem (vec |> bs))
      _ -> newBlockAndInsert
  bumpBytes (BS.length bs)
  where
    newBlockAndInsert = do
      writeIORef (blocks col) . (:>) =<< pure EmptyR <*> pure (InMem (V.singleton bs))


