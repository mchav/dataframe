module Main where

import Control.Monad
import Data.Time (NominalDiffTime, diffUTCTime, getCurrentTime)
import System.Directory (
    XdgDirectory (..),
    createDirectoryIfMissing,
    doesFileExist,
    getModificationTime,
    getXdgDirectory,
 )
import System.FilePath ((</>))
import System.Process

main :: IO ()
main = do
    cacheDir <- getXdgDirectory XdgCache "dataframe_repl"
    createDirectoryIfMissing True cacheDir

    let filepath = cacheDir </> "dataframe.ghci"
    shouldDownload <- needsUpdate filepath

    when shouldDownload $ do
        putStrLn "\ESC[92mDownloading latest version of dataframe config...\ESC[0m"
        output <-
            readProcess
                "curl"
                [ "--output"
                , filepath
                , "https://raw.githubusercontent.com/mchav/dataframe/refs/heads/main/dataframe.ghci"
                ]
                ""
        putStrLn output

        putStrLn =<< readProcess "cabal" ["update"] ""

    let command = "cabal"
        args =
            [ "repl"
            , "-O2"
            , "--build-depends"
            , "dataframe"
            , "--repl-option=-ghci-script=" ++ filepath
            ]
    (_, _, _, processHandle) <- createProcess (proc command args)

    exitCode <- waitForProcess processHandle
    pure ()

oneWeek :: NominalDiffTime
oneWeek = 7 * 24 * 60 * 60

needsUpdate :: FilePath -> IO Bool
needsUpdate filepath = do
    exists <- doesFileExist filepath
    if not exists
        then return True
        else do
            modTime <- getModificationTime filepath
            currentTime <- getCurrentTime
            let age = diffUTCTime currentTime modTime
            return (age > oneWeek)
