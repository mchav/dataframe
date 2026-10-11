{-# LANGUAGE CPP #-}
{-# LANGUAGE DataKinds #-}

-- Custom Setup: builds the dfusion-bridge staticlib, adds it to extraLibDirs.

import Data.Bifunctor (second)
import Distribution.Compiler (PerCompilerFlavor (..))
import Distribution.PackageDescription (
    BuildInfo (..),
    Executable (..),
    GenericPackageDescription (..),
    Library (..),
    TestSuite (..),
 )
import Distribution.Simple
import Distribution.Simple.UserHooks (UserHooks (..))
import Distribution.Types.CondTree (CondBranch (..), CondTree (..))
#if MIN_VERSION_Cabal(3,14,0)
import Distribution.Utils.Path (
    FileOrDir (Dir),
    Lib,
    Pkg,
    SymbolicPath,
    makeSymbolicPath,
 )
#endif
import Distribution.Verbosity (normal)
import System.Directory (
    canonicalizePath,
    doesFileExist,
    findExecutable,
    getHomeDirectory,
 )
import System.Exit (ExitCode (..), exitFailure)
import System.FilePath ((</>))
import System.IO (hPutStrLn, stderr)
import System.Process (proc, readCreateProcessWithExitCode)

main :: IO ()
main =
    defaultMainWithHooks
        simpleUserHooks
            { preConf = \args flags -> do
                runCargo "preConf"
                preConf simpleUserHooks args flags
            , confHook = \(gpd, hbi) flags -> do
                libDir <-
                    canonicalizePath ("rust" </> "dfusion-bridge" </> "target" </> "release")
                let gpd' = injectExtraLibDir libDir gpd
                confHook simpleUserHooks (gpd', hbi) flags
            , preBuild = \args flags -> do
                runCargo "preBuild"
                preBuild simpleUserHooks args flags
            , preRepl = \args flags -> do
                runCargo "preRepl"
                preRepl simpleUserHooks args flags
            , preTest = \args flags -> do
                runCargo "preTest"
                preTest simpleUserHooks args flags
            }

runCargo :: String -> IO ()
runCargo phase = do
    cargo <- ensureCargo phase
    let manifest = "rust/dfusion-bridge/Cargo.toml"
    hPutStrLn stderr $
        "[dataframe-fusion/Setup.hs:"
            ++ phase
            ++ "] "
            ++ cargo
            ++ " build --release --manifest-path "
            ++ manifest
    let cp = proc cargo ["build", "--release", "--manifest-path", manifest]
    (ec, out, err) <- readCreateProcessWithExitCode cp ""
    case ec of
        ExitSuccess -> return ()
        ExitFailure n -> do
            hPutStrLn stderr $
                "[dataframe-fusion/Setup.hs:"
                    ++ phase
                    ++ "] cargo failed (exit "
                    ++ show n
                    ++ ")"
            hPutStrLn stderr out
            hPutStrLn stderr err
            exitFailure

-- | Find cargo on PATH or in ~/.cargo/bin, installing Rust if absent.
ensureCargo :: String -> IO FilePath
ensureCargo phase = do
    mCargo <- findExecutable "cargo"
    case mCargo of
        Just p -> return p
        Nothing -> do
            home <- getHomeDirectory
            let cargoBin = home </> ".cargo" </> "bin" </> "cargo"
            existing <- doesFileExist cargoBin
            if existing
                then return cargoBin
                else do
                    installRust phase
                    return cargoBin

installRust :: String -> IO ()
installRust phase = do
    hPutStrLn stderr $
        "[dataframe-fusion/Setup.hs:"
            ++ phase
            ++ "] cargo not found; installing stable Rust toolchain via rustup"
    let installer =
            proc
                "sh"
                [ "-c"
                , "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs"
                    ++ " | sh -s -- -y --default-toolchain stable --profile minimal"
                ]
    (ec, out, err) <- readCreateProcessWithExitCode installer ""
    case ec of
        ExitSuccess -> return ()
        ExitFailure n -> do
            hPutStrLn stderr $
                "[dataframe-fusion/Setup.hs:"
                    ++ phase
                    ++ "] rustup installation failed (exit "
                    ++ show n
                    ++ ")"
            hPutStrLn stderr out
            hPutStrLn stderr err
            exitFailure

injectExtraLibDir ::
    FilePath -> GenericPackageDescription -> GenericPackageDescription
injectExtraLibDir libDir gpd =
    gpd
        { condLibrary = fmap (mapTree (addToLib libDir)) (condLibrary gpd)
        , condTestSuites =
            map
                (second (mapTree (addToTest libDir)))
                (condTestSuites gpd)
        , condExecutables =
            map
                (second (mapTree (addToExe libDir)))
                (condExecutables gpd)
        }

mapTree :: (a -> a) -> CondTree v c a -> CondTree v c a
mapTree f (CondNode d c bs) = CondNode (f d) c (map mapBranch bs)
  where
    mapBranch b =
        b
            { condBranchIfTrue = mapTree f (condBranchIfTrue b)
            , condBranchIfFalse = fmap (mapTree f) (condBranchIfFalse b)
            }

addToLib :: FilePath -> Library -> Library
addToLib p lib = lib{libBuildInfo = addExtra p (libBuildInfo lib)}

addToTest :: FilePath -> TestSuite -> TestSuite
addToTest p t = t{testBuildInfo = addExtra p (testBuildInfo t)}

addToExe :: FilePath -> Executable -> Executable
addToExe p e = e{buildInfo = addExtra p (buildInfo e)}

-- TODO: mchavinda - make this linking windows compatible.
addExtra :: FilePath -> BuildInfo -> BuildInfo
addExtra p bi =
    bi
        { extraLibDirs = mkLibDir p : extraLibDirs bi
        , options = addRpath (options bi)
        }
  where
    addRpath (PerCompilerFlavor ghc ghcjs) =
        PerCompilerFlavor (("-optl-Wl,-rpath," ++ p) : ghc) ghcjs

-- | In Cabal 3.14+, extraLibDirs holds 'SymbolicPath' values, not 'FilePath'.
#if MIN_VERSION_Cabal(3,14,0)
mkLibDir :: FilePath -> SymbolicPath Pkg ('Dir Lib)
mkLibDir = makeSymbolicPath
#else
mkLibDir :: FilePath -> FilePath
mkLibDir = id
#endif
