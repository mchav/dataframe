{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE GADTs #-}

module DataFrame.Display.Terminal.Plot where

import           Control.Monad
import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU
import           Data.Maybe (fromMaybe, catMaybes)
import           Data.Typeable (Typeable)
import           Data.Type.Equality (type (:~:)(Refl), TestEquality(testEquality))
import           Type.Reflection (typeRep)
import           GHC.Stack (HasCallStack)

import DataFrame.Internal.Column (Column(..), Columnable)
import DataFrame.Internal.DataFrame (DataFrame(..))
import DataFrame.Operations.Core
import Granite

data PlotConfig = PlotConfig
  { plotType :: PlotType
  , plotTitle :: String
  , plotSettings :: Plot
  }

data PlotType 
  = Histogram'
  | Scatter'
  | Line'
  | Bar'
  | BoxPlot'
  | Pie'
  | StackedBar'
  | Heatmap'
  deriving (Eq, Show)

defaultPlotConfig :: PlotType -> PlotConfig
defaultPlotConfig ptype = PlotConfig
  { plotType = ptype
  , plotTitle = ""
  , plotSettings = defPlot
  }

plotHistogram :: HasCallStack => T.Text -> DataFrame -> IO String
plotHistogram colName df = plotHistogramWith colName (defaultPlotConfig Histogram') df

plotHistogramWith :: HasCallStack => T.Text -> PlotConfig -> DataFrame -> IO String
plotHistogramWith colName config df = do
  values <- extractNumericColumn colName df
  let (minVal, maxVal) = if null values then (0, 1) else (minimum values, maximum values)
  histogram (plotTitle config) (bins 30 minVal maxVal) values (plotSettings config)

plotScatter :: HasCallStack => T.Text -> T.Text -> DataFrame -> IO String
plotScatter xCol yCol df = plotScatterWith xCol yCol (defaultPlotConfig Scatter') df

plotScatterWith :: HasCallStack => T.Text -> T.Text -> PlotConfig -> DataFrame -> IO String
plotScatterWith xCol yCol config df = do
  xVals <- extractNumericColumn xCol df
  yVals <- extractNumericColumn yCol df
  let points = zip xVals yVals
  scatter (plotTitle config) [(T.unpack xCol ++ " vs " ++ T.unpack yCol, points)] (plotSettings config)

plotLines :: HasCallStack => [T.Text] -> DataFrame -> IO String
plotLines colNames df = plotLinesWith colNames (defaultPlotConfig Line') df

plotLinesWith :: HasCallStack => [T.Text] -> PlotConfig -> DataFrame -> IO String
plotLinesWith colNames config df = do
  seriesData <- forM colNames $ \col -> do
    values <- extractNumericColumn col df
    let indices = map fromIntegral [0..length values - 1]
    return (T.unpack col, zip indices values)
  lineGraph (plotTitle config) seriesData (plotSettings config)

plotBoxPlots :: HasCallStack => [T.Text] -> DataFrame -> IO String
plotBoxPlots colNames df = plotBoxPlotsWith colNames (defaultPlotConfig BoxPlot') df

plotBoxPlotsWith :: HasCallStack => [T.Text] -> PlotConfig -> DataFrame -> IO String
plotBoxPlotsWith colNames config df = do
  boxData <- forM colNames $ \col -> do
    values <- extractNumericColumn col df
    return (T.unpack col, values)
  boxPlot (plotTitle config) boxData (plotSettings config)

plotStackedBars :: HasCallStack => T.Text -> [T.Text] -> DataFrame -> IO String
plotStackedBars categoryCol valueColumns df = 
  plotStackedBarsWith categoryCol valueColumns (defaultPlotConfig StackedBar') df

plotStackedBarsWith :: HasCallStack => T.Text -> [T.Text] -> PlotConfig -> DataFrame -> IO String
plotStackedBarsWith categoryCol valueColumns config df = do
  categories <- extractStringColumn categoryCol df
  let uniqueCategories = L.nub categories
  
  stackData <- forM uniqueCategories $ \cat -> do
    let indices = [i | (i, c) <- zip [0..] categories, c == cat]
    seriesData <- forM valueColumns $ \col -> do
      allValues <- extractNumericColumn col df
      let values = [allValues !! i | i <- indices, i < length allValues]
      return (T.unpack col, sum values)
    return (cat, seriesData)
  
  stackedBars (plotTitle config) stackData (plotSettings config)

plotHeatmap :: HasCallStack => DataFrame -> IO String
plotHeatmap df = plotHeatmapWith (defaultPlotConfig Heatmap') df

plotHeatmapWith :: HasCallStack => PlotConfig -> DataFrame -> IO String
plotHeatmapWith config df = do
  let numericCols = filter (isNumericColumn df) (columnNames df)
  matrix <- forM numericCols $ \col -> 
    extractNumericColumn col df
  heatmap (plotTitle config) matrix (plotSettings config)

isNumericColumn :: DataFrame -> T.Text -> Bool
isNumericColumn df colName = 
  case M.lookup colName (columnIndices df) of
    Nothing -> False
    Just idx -> 
      let col = columns df V.! idx
      in case col of
        BoxedColumn (vec :: V.Vector a) -> 
          case testEquality (typeRep @a) (typeRep @Double) of
            Just _ -> True
            Nothing -> case testEquality (typeRep @a) (typeRep @Int) of
              Just _ -> True
              Nothing -> case testEquality (typeRep @a) (typeRep @Float) of
                Just _ -> True
                Nothing -> False
        UnboxedColumn (vec :: VU.Vector a) -> 
          case testEquality (typeRep @a) (typeRep @Double) of
            Just _ -> True
            Nothing -> case testEquality (typeRep @a) (typeRep @Int) of
              Just _ -> True  
              Nothing -> case testEquality (typeRep @a) (typeRep @Float) of
                Just _ -> True
                Nothing -> False

plotAllHistograms :: HasCallStack => DataFrame -> IO ()
plotAllHistograms df = do
  let numericCols = filter (isNumericColumn df) (columnNames df)
  forM_ numericCols $ \col -> do
    result <- plotHistogram col df
    putStrLn result

plotCorrelationMatrix :: HasCallStack => DataFrame -> IO String
plotCorrelationMatrix df = do
  let numericCols = filter (isNumericColumn df) (columnNames df)
  correlations <- forM numericCols $ \col1 -> do
    forM numericCols $ \col2 -> do
      vals1 <- extractNumericColumn col1 df
      vals2 <- extractNumericColumn col2 df
      return $ correlation vals1 vals2
  heatmap "Correlation Matrix" correlations defPlot
  where
    correlation xs ys = 
      let n = fromIntegral $ length xs
          meanX = sum xs / n
          meanY = sum ys / n
          covXY = sum [(x - meanX) * (y - meanY) | (x,y) <- zip xs ys] / n
          stdX = sqrt $ sum [(x - meanX)^2 | x <- xs] / n
          stdY = sqrt $ sum [(y - meanY)^2 | y <- ys] / n
      in covXY / (stdX * stdY)

quickPlot :: HasCallStack => [T.Text] -> DataFrame -> IO String
quickPlot [] df = plotAllHistograms df >> return "Plotted all numeric columns"
quickPlot [col] df = plotHistogram col df
quickPlot [col1, col2] df = plotScatter col1 col2 df
quickPlot cols df = plotLines cols df

plotBars :: HasCallStack => T.Text -> DataFrame -> IO String
plotBars colName df = plotBarsWith colName Nothing (defaultPlotConfig Bar') df

plotBarsWith :: HasCallStack => T.Text -> Maybe T.Text -> PlotConfig -> DataFrame -> IO String
plotBarsWith colName groupByCol config df = 
  case groupByCol of
    Nothing -> plotSingleBars colName config df
    Just grpCol -> plotGroupedBarsWith grpCol colName config df

plotSingleBars :: HasCallStack => T.Text -> PlotConfig -> DataFrame -> IO String
plotSingleBars colName config df = do
  barData <- getCategoricalCounts colName df
  case barData of
    Just counts -> do
      let grouped = groupWithOther 10 counts
      bars (plotTitle config) grouped (plotSettings config)
    Nothing -> do
      values <- extractNumericColumn colName df
      if length values > 20
        then do
          let labels = map (\i -> "Item " ++ show i) [1..length values]
              paired = zip labels values
              grouped = groupWithOther 10 paired
          bars (plotTitle config) grouped (plotSettings config)
        else do
          let labels = map (\i -> "Item " ++ show i) [1..length values]
          bars (plotTitle config) (zip labels values) (plotSettings config)

plotBarsTopN :: HasCallStack => Int -> T.Text -> DataFrame -> IO String
plotBarsTopN n colName df = plotBarsTopNWith n colName (defaultPlotConfig Bar') df

plotBarsTopNWith :: HasCallStack => Int -> T.Text -> PlotConfig -> DataFrame -> IO String
plotBarsTopNWith n colName config df = do
  barData <- getCategoricalCounts colName df
  case barData of
    Just counts -> do
      let grouped = groupWithOther n counts
      bars (plotTitle config) grouped (plotSettings config)
    Nothing -> do
      values <- extractNumericColumn colName df
      let labels = map (\i -> "Item " ++ show i) [1..length values]
          paired = zip labels values
          grouped = groupWithOther n paired
      bars (plotTitle config) grouped (plotSettings config)

plotGroupedBarsWith :: HasCallStack => T.Text -> T.Text -> PlotConfig -> DataFrame -> IO String
plotGroupedBarsWith groupCol valCol config df = do
  isNumeric <- isNumericColumnCheck valCol df
  
  if isNumeric
    then do
      groups <- extractStringColumn groupCol df
      values <- extractNumericColumn valCol df
      let grouped = M.toList $ M.fromListWith (+) (zip groups values)
          finalGroups = groupWithOther 10 grouped
      bars (plotTitle config) finalGroups (plotSettings config)
    else do
      groups <- extractStringColumn groupCol df
      vals <- extractStringColumn valCol df
      let pairs = zip groups vals
          counts = M.toList $ M.fromListWith (+) 
                    [(g ++ " - " ++ v, 1) | (g, v) <- pairs]
          finalCounts = groupWithOther 15 [(k, fromIntegral v) | (k, v) <- counts]
      bars (plotTitle config) finalCounts (plotSettings config)

plotValueCounts :: HasCallStack => T.Text -> DataFrame -> IO String
plotValueCounts colName df = plotValueCountsWith colName 10 (defaultPlotConfig Bar') df

plotValueCountsWith :: HasCallStack => T.Text -> Int -> PlotConfig -> DataFrame -> IO String
plotValueCountsWith colName maxBars config df = do
  counts <- getCategoricalCounts colName df
  case counts of
    Just c -> do
      let grouped = groupWithOther maxBars c
          config' = config { plotTitle = if null (plotTitle config) 
                                         then "Value counts for " ++ T.unpack colName
                                         else plotTitle config }
      bars (T.unpack colName) grouped (plotSettings config')
    Nothing -> error $ "Could not get value counts for column " ++ T.unpack colName

plotBarsWithPercentages :: HasCallStack => T.Text -> DataFrame -> IO String
plotBarsWithPercentages colName df = do
  counts <- getCategoricalCounts colName df
  case counts of
    Just c -> do
      let total = sum (map snd c)
          percentages = [(label ++ " (" ++ show (round (100 * val / total) :: Int) ++ "%)", val) 
                        | (label, val) <- c]
          grouped = groupWithOther 10 percentages
      bars ("Distribution of " ++ T.unpack colName) grouped defPlot
    Nothing -> error $ "Could not get value counts for column " ++ T.unpack colName

smartPlotBars :: HasCallStack => T.Text -> DataFrame -> IO String
smartPlotBars colName df = do
  counts <- getCategoricalCounts colName df
  case counts of
    Just c -> do
      let numUnique = length c
          config = (defaultPlotConfig Bar') { 
            plotTitle = T.unpack colName ++ " (" ++ show numUnique ++ " unique values)"
          }
      if numUnique <= 12
        then bars (plotTitle config) c (plotSettings config)
        else if numUnique <= 20
          then do
            let grouped = groupWithOther 12 c
            bars (plotTitle config ++ " - Top 12 + Other") grouped (plotSettings config)
          else do
            let grouped = groupWithOther 10 c
                otherCount = numUnique - 10
            bars (plotTitle config ++ " - Top 10 + Other (" ++ show otherCount ++ " items)") 
                 grouped (plotSettings config)
    Nothing -> plotBars colName df

plotCategoricalSummary :: HasCallStack => DataFrame -> IO ()
plotCategoricalSummary df = do
  let cols = columnNames df
  forM_ cols $ \col -> do
    counts <- getCategoricalCounts col df
    case counts of
      Just c -> when (length c > 1) $ do
        let numUnique = length c
        putStrLn $ "\n=== " ++ T.unpack col ++ " (" ++ show numUnique ++ " unique values) ==="
        result <- if numUnique > 15
                  then plotBarsTopN 10 col df
                  else plotBars col df
        putStrLn result
      Nothing -> return ()

getCategoricalCounts :: HasCallStack => T.Text -> DataFrame -> IO (Maybe [(String, Double)])
getCategoricalCounts colName df = 
  case M.lookup colName (columnIndices df) of
    Nothing -> error $ "Column " ++ T.unpack colName ++ " not found"
    Just idx -> 
      let col = columns df V.! idx
      in case col of
        BoxedColumn vec -> do
          let counts = countValues vec
          return $ Just [(show k, fromIntegral v) | (k, v) <- counts]
        UnboxedColumn vec -> do
          let counts = countValuesUnboxed vec
          return $ Just [(show k, fromIntegral v) | (k, v) <- counts]
  where
    countValues :: (Ord a, Show a) => V.Vector a -> [(a, Int)]
    countValues vec = M.toList $ V.foldr' (\x acc -> M.insertWith (+) x 1 acc) M.empty vec
    
    countValuesUnboxed :: (Ord a, Show a, VU.Unbox a) => VU.Vector a -> [(a, Int)]
    countValuesUnboxed vec = M.toList $ VU.foldr' (\x acc -> M.insertWith (+) x 1 acc) M.empty vec

isNumericColumnCheck :: T.Text -> DataFrame -> IO Bool
isNumericColumnCheck colName df = 
  case M.lookup colName (columnIndices df) of
    Nothing -> return False
    Just idx -> 
      let col = columns df V.! idx
      in case col of
        BoxedColumn (vec :: V.Vector a) -> 
          return $ isNumericType @a
        UnboxedColumn (vec :: VU.Vector a) -> 
          return $ isNumericType @a

isNumericType :: forall a. Typeable a => Bool
isNumericType = 
  case testEquality (typeRep @a) (typeRep @Double) of
    Just _ -> True
    Nothing -> case testEquality (typeRep @a) (typeRep @Int) of
      Just _ -> True
      Nothing -> case testEquality (typeRep @a) (typeRep @Float) of
        Just _ -> True
        Nothing -> case testEquality (typeRep @a) (typeRep @Integer) of
          Just _ -> True
          Nothing -> False

extractStringColumn :: HasCallStack => T.Text -> DataFrame -> IO [String]
extractStringColumn colName df = 
  case M.lookup colName (columnIndices df) of
    Nothing -> error $ "Column " ++ T.unpack colName ++ " not found"
    Just idx -> 
      let col = columns df V.! idx
      in case col of
        BoxedColumn vec -> return $ V.toList $ V.map show vec
        UnboxedColumn vec -> return $ V.toList $ VG.map show (VG.convert vec)

extractNumericColumn :: HasCallStack => T.Text -> DataFrame -> IO [Double]
extractNumericColumn colName df = 
  case M.lookup colName (columnIndices df) of
    Nothing -> error $ "Column " ++ T.unpack colName ++ " not found"
    Just idx -> 
      let col = columns df V.! idx
      in case col of
        BoxedColumn vec -> return $ vectorToDoubles vec
        UnboxedColumn vec -> return $ unboxedVectorToDoubles vec

vectorToDoubles :: forall a. (Typeable a, Show a) => V.Vector a -> [Double]
vectorToDoubles vec = 
  case testEquality (typeRep @a) (typeRep @Double) of
    Just Refl -> V.toList vec
    Nothing -> case testEquality (typeRep @a) (typeRep @Int) of
      Just Refl -> V.toList $ V.map fromIntegral vec
      Nothing -> case testEquality (typeRep @a) (typeRep @Integer) of
        Just Refl -> V.toList $ V.map fromIntegral vec
        Nothing -> case testEquality (typeRep @a) (typeRep @Float) of
          Just Refl -> V.toList $ V.map realToFrac vec
          Nothing -> error $ "Column is not numeric (type: " ++ show (typeRep @a) ++ ")"

unboxedVectorToDoubles :: forall a. (Typeable a, VU.Unbox a, Show a) => VU.Vector a -> [Double]
unboxedVectorToDoubles vec = 
  case testEquality (typeRep @a) (typeRep @Double) of
    Just Refl -> VU.toList vec
    Nothing -> case testEquality (typeRep @a) (typeRep @Int) of
      Just Refl -> VU.toList $ VU.map fromIntegral vec
      Nothing -> case testEquality (typeRep @a) (typeRep @Float) of
        Just Refl -> VU.toList $ VU.map realToFrac vec
        Nothing -> error $ "Column is not numeric (type: " ++ show (typeRep @a) ++ ")"

groupWithOther :: Int -> [(String, Double)] -> [(String, Double)]
groupWithOther n items = 
  let sorted = L.sortOn (negate . snd) items
      (topN, rest) = splitAt n sorted
      otherSum = sum (map snd rest)
      result = if null rest || otherSum == 0
               then topN
               else topN ++ [("Other (" ++ show (length rest) ++ " items)", otherSum)]
  in result

plotPie :: HasCallStack => T.Text -> Maybe T.Text -> DataFrame -> IO String
plotPie valCol labelCol df = plotPieWith valCol labelCol (defaultPlotConfig Pie') df

plotPieWith :: HasCallStack => T.Text -> Maybe T.Text -> PlotConfig -> DataFrame -> IO String
plotPieWith valCol labelCol config df = do
  categoricalData <- getCategoricalCounts valCol df
  case categoricalData of
    Just counts -> do
      let grouped = groupWithOtherForPie 8 counts
      pie (plotTitle config) grouped (plotSettings config)
    Nothing -> do
      values <- extractNumericColumn valCol df
      labels <- case labelCol of
        Nothing -> return $ map (\i -> "Item " ++ show i) [1..length values]
        Just lCol -> extractStringColumn lCol df
      let pieData = zip labels values
          grouped = if length pieData > 10
                   then groupWithOtherForPie 8 pieData
                   else pieData
      pie (plotTitle config) grouped (plotSettings config)

groupWithOtherForPie :: Int -> [(String, Double)] -> [(String, Double)]
groupWithOtherForPie n items = 
  let total = sum (map snd items)
      sorted = L.sortOn (negate . snd) items
      (topN, rest) = splitAt n sorted
      otherSum = sum (map snd rest)
      otherPct = round (100 * otherSum / total) :: Int
      result = if null rest || otherSum == 0
               then topN
               else topN ++ [("Other (" ++ show (length rest) ++ " items, " ++ 
                             show otherPct ++ "%)", otherSum)]
  in result

plotPieWithPercentages :: HasCallStack => T.Text -> DataFrame -> IO String
plotPieWithPercentages colName df = plotPieWithPercentagesConfig colName (defaultPlotConfig Pie') df

plotPieWithPercentagesConfig :: HasCallStack => T.Text -> PlotConfig -> DataFrame -> IO String
plotPieWithPercentagesConfig colName config df = do
  counts <- getCategoricalCounts colName df
  case counts of
    Just c -> do
      let total = sum (map snd c)
          withPct = [(label ++ " (" ++ show (round (100 * val / total) :: Int) ++ "%)", val) 
                    | (label, val) <- c]
          grouped = groupWithOtherForPie 8 withPct
      pie (plotTitle config) grouped (plotSettings config)
    Nothing -> do
      values <- extractNumericColumn colName df
      let total = sum values
          labels = map (\i -> "Item " ++ show i) [1..length values]
          withPct = [(label ++ " (" ++ show (round (100 * val / total) :: Int) ++ "%)", val)
                    | (label, val) <- zip labels values]
          grouped = groupWithOtherForPie 8 withPct
      pie (plotTitle config) grouped (plotSettings config)

plotPieTopN :: HasCallStack => Int -> T.Text -> DataFrame -> IO String
plotPieTopN n colName df = plotPieTopNWith n colName (defaultPlotConfig Pie') df

plotPieTopNWith :: HasCallStack => Int -> T.Text -> PlotConfig -> DataFrame -> IO String
plotPieTopNWith n colName config df = do
  counts <- getCategoricalCounts colName df
  case counts of
    Just c -> do
      let grouped = groupWithOtherForPie n c
      pie (plotTitle config) grouped (plotSettings config)
    Nothing -> do
      values <- extractNumericColumn colName df
      let labels = map (\i -> "Item " ++ show i) [1..length values]
          paired = zip labels values
          grouped = groupWithOtherForPie n paired
      pie (plotTitle config) grouped (plotSettings config)

smartPlotPie :: HasCallStack => T.Text -> DataFrame -> IO String
smartPlotPie colName df = do
  counts <- getCategoricalCounts colName df
  case counts of
    Just c -> do
      let numUnique = length c
          total = sum (map snd c)
          significant = filter (\(_, v) -> v / total >= 0.01) c
          config = (defaultPlotConfig Pie') { 
            plotTitle = T.unpack colName ++ " Distribution"
          }
      if length significant <= 6
        then pie (plotTitle config) significant (plotSettings config)
        else if length significant <= 10
          then do
            let grouped = groupWithOtherForPie 8 c
            pie (plotTitle config) grouped (plotSettings config)
          else do
            let grouped = groupWithOtherForPie 6 c
            pie (plotTitle config) grouped (plotSettings config)
    Nothing -> plotPie colName Nothing df

plotPieGrouped :: HasCallStack => T.Text -> T.Text -> DataFrame -> IO String
plotPieGrouped groupCol valCol df = plotPieGroupedWith groupCol valCol (defaultPlotConfig Pie') df

plotPieGroupedWith :: HasCallStack => T.Text -> T.Text -> PlotConfig -> DataFrame -> IO String
plotPieGroupedWith groupCol valCol config df = do
  isNumeric <- isNumericColumnCheck valCol df
  
  if isNumeric
    then do
      groups <- extractStringColumn groupCol df
      values <- extractNumericColumn valCol df
      let grouped = M.toList $ M.fromListWith (+) (zip groups values)
          finalGroups = groupWithOtherForPie 8 grouped
      pie (plotTitle config) finalGroups (plotSettings config)
    else do
      groups <- extractStringColumn groupCol df
      vals <- extractStringColumn valCol df
      let combined = zipWith (\g v -> g ++ " - " ++ v) groups vals
          counts = M.toList $ M.fromListWith (+) [(c, 1) | c <- combined]
          finalCounts = groupWithOtherForPie 10 [(k, fromIntegral v) | (k, v) <- counts]
      pie (plotTitle config) finalCounts (plotSettings config)

plotPieComparison :: HasCallStack => [T.Text] -> DataFrame -> IO ()
plotPieComparison cols df = do
  forM_ cols $ \col -> do
    counts <- getCategoricalCounts col df
    case counts of
      Just c -> when (length c > 1 && length c <= 20) $ do
        putStrLn $ "\n=== " ++ T.unpack col ++ " Distribution ==="
        result <- smartPlotPie col df
        putStrLn result
      Nothing -> return ()

plotBinaryPie :: HasCallStack => T.Text -> DataFrame -> IO String
plotBinaryPie colName df = do
  counts <- getCategoricalCounts colName df
  case counts of
    Just c -> 
      if length c == 2
        then do
          let total = sum (map snd c)
              withPct = [(label ++ " (" ++ show (round (100 * val / total) :: Int) ++ "%)", val) 
                        | (label, val) <- c]
          pie (T.unpack colName ++ " Proportion") withPct defPlot
        else error $ "Column " ++ T.unpack colName ++ " is not binary (has " ++ 
                    show (length c) ++ " unique values)"
    Nothing -> error $ "Column " ++ T.unpack colName ++ " is not categorical"

plotMarketShare :: HasCallStack => T.Text -> DataFrame -> IO String
plotMarketShare colName df = plotMarketShareWith colName (defaultPlotConfig Pie') df

plotMarketShareWith :: HasCallStack => T.Text -> PlotConfig -> DataFrame -> IO String
plotMarketShareWith colName config df = do
  counts <- getCategoricalCounts colName df
  case counts of
    Just c -> do
      let total = sum (map snd c)
          sorted = L.sortOn (negate . snd) c
          significantShares = takeWhile (\(_, v) -> v / total >= 0.02) sorted
          otherSum = sum [v | (_, v) <- c, v `notElem` map snd significantShares]
          
          formatShare (label, val) = 
            let pct = round (100 * val / total) :: Int
            in (label ++ " (" ++ show pct ++ "%)", val)
          
          shares = map formatShare significantShares
          finalShares = if otherSum > 0 && otherSum / total >= 0.01
                       then shares ++ [("Others (<2% each)", otherSum)]
                       else shares
      
      let config' = config { plotTitle = if null (plotTitle config)
                                        then T.unpack colName ++ " Market Share"
                                        else plotTitle config }
      pie (plotTitle config') finalShares (plotSettings config')
    Nothing -> error $ "Column " ++ T.unpack colName ++ " is not categorical"
