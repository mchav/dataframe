{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE OverloadedStrings #-}

-- | basic measurement and callibration
module Main where

import Chart qualified
import Control.DeepSeq
import Control.Monad
import Control.Monad.State.Lazy
import Data.List (intercalate)
import Data.Map.Strict qualified as Map
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.IO qualified as Text
import GHC.Exts
import GHC.Generics
import Optics.Core
import Options.Applicative
import Options.Applicative.Help.Pretty
import Perf
import Perf.Chart
import Prelude
import qualified Data.Vector.Unboxed as VU
import qualified DataFrame as D
import qualified DataFrame.Functions as F
import System.Mem
import System.Random.Stateful

data Run = RunExample deriving (Eq, Show)

data AppConfig = AppConfig
  { appRun :: Run,
    appReportOptions :: ReportOptions
  }
  deriving (Eq, Show, Generic)

defaultAppConfig :: AppConfig
defaultAppConfig = AppConfig RunExample (defaultReportOptions & set #reportLength 3)

parseRun :: Parser Run
parseRun =
  flag' RunExample (long "example" <> help "run on the example algorithm" <> style (annotate bold))
    <|> pure RunExample

appParser :: AppConfig -> Parser AppConfig
appParser def =
  AppConfig
    <$> parseRun
    <*> parseReportOptions (view #appReportOptions def)

appConfig :: AppConfig -> ParserInfo AppConfig
appConfig def =
  info
    (appParser def <**> helper)
    (fullDesc <> header "Examples of perf usage (defaults in bold)")

-- The short chains under investigation
--
--
-- | DataFrame instantiation from a list of vectors
makeDF :: [VU.Vector Double] -> D.DataFrame
makeDF vs = D.fromUnnamedColumns (map D.fromUnboxedVector vs)

-- | Just a sum from 1..l. used as a hopefully O(n) reference point
sumControl :: (Semigroup s) => Int -> PerfT IO s Double
sumControl l = fap "sumControl" sum' l

sum' :: Int -> Double
sum' n = sum [1..fromIntegral n]

-- | Hopefully a no-op, used as a hopefully O(1) reference point for function application
faControl :: (Semigroup s) => PerfT IO s ()
faControl = fap "faControl" (const ()) ()

-- | Generation of random variates
--
rvsPerf :: (StatefulGen g IO, Semigroup s) => g -> Int -> PerfT IO s (VU.Vector Double)
rvsPerf g l = fam "randoms" (genVU g l)

genVU :: StatefulGen g IO => g -> Int -> IO (VU.Vector Double)
genVU g n = VU.replicateM n (uniformRM (0,1) g)

genVUs :: StatefulGen g IO => g -> Int -> Int -> IO [VU.Vector Double]
genVUs g l n = replicateM n (VU.replicateM l (uniformRM (0,1) g))

-- | devRun combines performance snippets.
devRun :: (StatefulGen g IO, Semigroup s) => g -> Int -> PerfT IO s Double
devRun g l = do
  _ <- faControl
  _ <- sumControl l
  !v <- rvsPerf g l
  ffap "vusum" VU.sum v

-- | devRun combines performance snippets.
devRun_ :: (StatefulGen g IO, Semigroup s) => g -> Int -> Int -> PerfT IO s Double
devRun_ g ncols l = do
  _ <- faControl
  _ <- sumControl l
  !vs <- fam "genVUs" (genVUs g l ncols)
  !df <- fap "makeDF" makeDF vs
  !m <- fap "mean" (D.mean (F.col @Double "0")) df
  !v <- fap "variance" (D.variance (F.col @Double "1")) df
  !c <- fap "correlation" (D.correlation "1" "2") df
  pure m

{-
exampleRun g l n = do
  !vs <- replicateM l $ fam "genVU" (genVU g n)
  !df <- fam "genDF" (pure $ genDF l vs)
  _ <- fam "genDF(VU)" ((fmap (genDF l) . replicateM l . genVU g) n)

  !mean' <- fam "mean" $ pure $ D.mean (F.col @Double "0") df
  !variance' <- fam "Variance" $ pure $ D.variance (F.col @Double "1") df
  !correlation' <- fam "correlation" $ pure $ D.correlation "1" "2" df

  liftIO $ print mean'
  liftIO $ print variance'
  liftIO $ print correlation'
  pure ()
-}

-- | Execute and report on a performance run.
--
-- [Double] is the concrete type of the performance measure: representing a single-measurement. List here is mostly used as the free monoid but you can have a situation where a perfoamce measure is best represented as multiple doubles (time and space).
--
--
reportM :: ReportOptions -> Name -> Map.Map Text [Double] -> IO ()
reportM o name m = do
  let !n = reportN o
  let l = reportLength o
  let s = reportStatDType o
  let c = reportClock o
  let mt = reportMeasureType o
  let o' = replaceDefaultFilePath (intercalate "-" [name, show n, show mt, show s]) o
  report o' (statify s $ fmap (fmap pure) m)
  (\cfg -> when (view #doChart cfg) (Chart.writeChartOptions (view #chartFilepath cfg) (perfCharts cfg (Just (measureLabels mt)) (fmap (fmap pure) m)))) (reportChart o)
  (\cfg -> when (view #doDump cfg) (writeFile (view #dumpFilepath cfg) (show m))) (reportDump o)
  pure ()

-- | ghci state recipe
-- >>>  g <- newIOGenM =<< newStdGen
-- >>> :t g
-- g :: IOGenM StdGen
--
main :: IO ()
main = do
  o <- execParser (appConfig defaultAppConfig)
  let repOptions = appReportOptions o
  let run = appRun o
  let n = reportN repOptions

  let !l = reportLength repOptions
  let p = fmap (fmap fromIntegral) $ times n

  case run of
    RunExample -> do
      when (reportGC repOptions) performGC
      g <- newIOGenM =<< newStdGen
      (a, m) <- runPerfT p (devRun_ g 3 l)
      print a
      reportM repOptions (intercalate "-" [show n, show l]) m
