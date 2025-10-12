{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}

{-# HLINT ignore "Use newtype instead of data" #-}
module DataFrame.Display where

import qualified Data.Text.IO as T
import qualified DataFrame.Internal.DataFrame as D
import qualified DataFrame.Operations.Subset as D

import Data.Function

data DisplayOptions = DisplayOptions
    { {-- | Maximum number of rows to render.
      --
      --   * If this value is less than or equal to 0, no rows are printed.
      --   * If it is greater than the number of rows in the frame, all rows are printed.
      --}
      displayRows :: Int
    }

defaultDisplayOptions :: DisplayOptions
defaultDisplayOptions = DisplayOptions 10

-- | Render a 'DataFrame' to stdout according to 'DisplayOptions'.
display :: DisplayOptions -> D.DataFrame -> IO ()
display opts df = df & D.take (displayRows opts) & (`D.asText` False) & T.putStrLn
