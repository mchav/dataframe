:set -XOverloadedStrings
:set -XTypeApplications
:set -XTemplateHaskell

import qualified Data.Text as T
import           DataFrame.Functions (declareColumns)
import           Data.Text (Text)
import           DataFrame ((|>))
import           Data.Int
import           Data.Time

import qualified DataFrame as D
import qualified DataFrame.Functions as F
import           DataFrame.Functions ((.=), (.==), (.<=), (.>=), (.<), (.>), (.&&), (.||), as, (./=))

import GHC.Conc

n <- getNumProcessors
setNumCapabilities $ max 1 (min 4 (n-1))

default (Int, T.Text, Double)

:def! declareColumns \s -> return ("_ = (); declareColumns " ++ s)
:set prompt "\ESC[34m\STXdataframe> \ESC[m\STX"

__brightBlue s = "\ESC[94m" ++ s ++ "\ESC[0m"
__brightGreen s = "\ESC[92m" ++ s ++ "\ESC[0m"

putStrLn "========================================"
putStrLn ("              📦" ++ (__brightBlue "Dataframe"))
putStrLn "========================================"
putStrLn ""
putStrLn "✨  Modules were automatically imported."
putStrLn ""
putStrLn "💡  Use prefix 'D' for core functionality."
putStrLn ("        ● E.g. " ++ (__brightGreen "D.readCsv \"/path/to/file\""))
putStrLn "💡  Use prefix 'F' for expression functions."
putStrLn ("        ● E.g. " ++ (__brightGreen "F.sum (F.col @Int \"value\")"))
putStrLn ""
putStrLn "✅ Ready."
