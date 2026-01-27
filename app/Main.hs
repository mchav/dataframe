{-# LANGUAGE CPP #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Control.Exception (bracket)
import Control.Monad
import Data.Time (NominalDiffTime, diffUTCTime, getCurrentTime)
import System.Directory (
    XdgDirectory (..),
    createDirectoryIfMissing,
    doesFileExist,
    getModificationTime,
    getXdgDirectory,
 )
import System.Exit (ExitCode (..))
import System.FilePath ((</>))
import System.Process
#ifndef mingw32_HOST_OS
import System.Posix.Signals (Handler(..), installHandler, sigINT)
#endif

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
            , "--ignore-project"
            , "-O2"
            , "--build-depends"
            , "dataframe"
            , "--repl-option=-ghci-script=" ++ filepath
            ]
    let baseCp =
            (proc command args)
                { cwd = Just cacheDir
                , std_in = Inherit
                , std_out = Inherit
                , std_err = Inherit
                }
#ifdef mingw32_HOST_OS
        cp = baseCp {delegate_ctlc = True}
#else
        cp = baseCp
#endif
#ifndef mingw32_HOST_OS
    -- Unix: ignore Ctrl-C in the wrapper so the child handles it.
    bracket (installHandler sigINT Ignore Nothing)
            (\old -> installHandler sigINT old Nothing)
            (\_ -> runChild cp)
#else
    -- Windows: delegate Ctrl-C handling to the child.
    runChild cp
#endif

runChild :: CreateProcess -> IO ()
runChild cp = do
    (_, _, _, ph) <- createProcess cp
    ec <- waitForProcess ph
    case ec of
        ExitSuccess -> pure ()
        ExitFailure n -> fail ("cabal repl failed with exit code " <> show n)

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
