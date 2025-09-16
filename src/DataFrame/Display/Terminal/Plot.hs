{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Display.Terminal.Plot where

import Control.Monad
import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Text.IO as T
import Data.Type.Equality (TestEquality (testEquality), type (:~:) (Refl))
import Data.Typeable (Typeable)
import qualified Data.Vector as V
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU
import GHC.Stack (HasCallStack)
import Type.Reflection (typeRep)

import DataFrame.Internal.Column (Column (..), isNumeric)
import qualified DataFrame.Internal.Column as D
import DataFrame.Internal.DataFrame (DataFrame (..), getColumn)
import DataFrame.Operations.Core
import qualified DataFrame.Operations.Subset as D
import Granite

data PlotConfig = PlotConfig
    { plotType :: PlotType
    , plotSettings :: Plot
    }

data PlotType
    = Histogram
    | Scatter
    | Line
    | Bar
    | BoxPlot
    | Pie
    | StackedBar
    | Heatmap
    deriving (Eq, Show)

defaultPlotConfig :: PlotType -> PlotConfig
defaultPlotConfig ptype =
    PlotConfig
        { plotType = ptype
        , plotSettings = defPlot
        }

plotHistogram :: (HasCallStack) => T.Text -> DataFrame -> IO ()
plotHistogram colName = plotHistogramWith colName (defaultPlotConfig Histogram)

plotHistogramWith :: (HasCallStack) => T.Text -> PlotConfig -> DataFrame -> IO ()
plotHistogramWith colName config df = do
    let values = extractNumericColumn colName df
        (minVal, maxVal) = if null values then (0, 1) else (minimum values, maximum values)
    T.putStrLn $ histogram (bins 30 minVal maxVal) values (plotSettings config)

plotScatter :: (HasCallStack) => T.Text -> T.Text -> DataFrame -> IO ()
plotScatter xCol yCol = plotScatterWith xCol yCol (defaultPlotConfig Scatter)

plotScatterWith :: (HasCallStack) => T.Text -> T.Text -> PlotConfig -> DataFrame -> IO ()
plotScatterWith xCol yCol config df = do
    let xVals = extractNumericColumn xCol df
        yVals = extractNumericColumn yCol df
        points = zip xVals yVals
    T.putStrLn $ scatter [(xCol <> " vs " <> yCol, points)] (plotSettings config)

plotScatterBy :: (HasCallStack) => T.Text -> T.Text -> T.Text -> DataFrame -> IO ()
plotScatterBy xCol yCol grouping = plotScatterByWith xCol yCol grouping (defaultPlotConfig Scatter)

plotScatterByWith :: (HasCallStack) => T.Text -> T.Text -> T.Text -> PlotConfig -> DataFrame -> IO ()
plotScatterByWith xCol yCol grouping config df = do
    let vals = extractStringColumn grouping df
    let df' = insertColumn grouping (D.fromList vals) df
    xs <- forM (L.nub vals) $ \col -> do
        let filtered = D.filter grouping (== col) df'
            xVals = extractNumericColumn xCol filtered
            yVals = extractNumericColumn yCol filtered
            points = zip xVals yVals
        pure (col, points)
    T.putStrLn $ scatter xs (plotSettings config)

plotLines :: (HasCallStack) => T.Text -> [T.Text] -> DataFrame -> IO ()
plotLines xAxis colNames = plotLinesWith xAxis colNames (defaultPlotConfig Line)

plotLinesWith :: (HasCallStack) => T.Text -> [T.Text] -> PlotConfig -> DataFrame -> IO ()
plotLinesWith xAxis colNames config df = do
    seriesData <- forM colNames $ \col -> do
        let values = extractNumericColumn col df
            indices = extractNumericColumn xAxis df
        return (col, zip indices values)
    T.putStrLn $ lineGraph seriesData (plotSettings config)

plotBoxPlots :: (HasCallStack) => [T.Text] -> DataFrame -> IO ()
plotBoxPlots colNames = plotBoxPlotsWith colNames (defaultPlotConfig BoxPlot)

plotBoxPlotsWith :: (HasCallStack) => [T.Text] -> PlotConfig -> DataFrame -> IO ()
plotBoxPlotsWith colNames config df = do
    boxData <- forM colNames $ \col -> do
        let values = extractNumericColumn col df
        return (col, values)
    T.putStrLn $ boxPlot boxData (plotSettings config)

plotStackedBars :: (HasCallStack) => T.Text -> [T.Text] -> DataFrame -> IO ()
plotStackedBars categoryCol valueColumns = plotStackedBarsWith categoryCol valueColumns (defaultPlotConfig StackedBar)

plotStackedBarsWith :: (HasCallStack) => T.Text -> [T.Text] -> PlotConfig -> DataFrame -> IO ()
plotStackedBarsWith categoryCol valueColumns config df = do
    let categories = extractStringColumn categoryCol df
        uniqueCategories = L.nub categories

    stackData <- forM uniqueCategories $ \cat -> do
        let indices = [i | (i, c) <- zip [0 ..] categories, c == cat]
        seriesData <- forM valueColumns $ \col -> do
            let allValues = extractNumericColumn col df
                values = [allValues !! i | i <- indices, i < length allValues]
            return (col, sum values)
        return (cat, seriesData)

    T.putStrLn $ stackedBars stackData (plotSettings config)

plotHeatmap :: (HasCallStack) => DataFrame -> IO ()
plotHeatmap = plotHeatmapWith (defaultPlotConfig Heatmap)

plotHeatmapWith :: (HasCallStack) => PlotConfig -> DataFrame -> IO ()
plotHeatmapWith config df = do
    let numericCols = filter (isNumericColumn df) (columnNames df)
        matrix = map (`extractNumericColumn` df) numericCols
    T.putStrLn $ heatmap matrix (plotSettings config)

isNumericColumn :: DataFrame -> T.Text -> Bool
isNumericColumn df colName = maybe False isNumeric (getColumn colName df)

plotAllHistograms :: (HasCallStack) => DataFrame -> IO ()
plotAllHistograms df = do
    let numericCols = filter (isNumericColumn df) (columnNames df)
    forM_ numericCols $ \col -> do
        T.putStrLn col
        plotHistogram col df

plotCorrelationMatrix :: (HasCallStack) => DataFrame -> IO ()
plotCorrelationMatrix df = do
    let numericCols = filter (isNumericColumn df) (columnNames df)
    let correlations =
            map
                ( \col1 ->
                    map
                        ( \col2 ->
                            let
                                vals1 = extractNumericColumn col1 df
                                vals2 = extractNumericColumn col2 df
                             in
                                correlation vals1 vals2
                        )
                        numericCols
                )
                numericCols
    print (zip [0 ..] numericCols)
    T.putStrLn $ heatmap correlations (defPlot{plotTitle = "Correlation Matrix"})
  where
    correlation xs ys =
        let n = fromIntegral $ length xs
            meanX = sum xs / n
            meanY = sum ys / n
            covXY = sum [(x - meanX) * (y - meanY) | (x, y) <- zip xs ys] / n
            stdX = sqrt $ sum [(x - meanX) ^ 2 | x <- xs] / n
            stdY = sqrt $ sum [(y - meanY) ^ 2 | y <- ys] / n
         in covXY / (stdX * stdY)

plotBars :: (HasCallStack) => T.Text -> DataFrame -> IO ()
plotBars colName = plotBarsWith colName Nothing (defaultPlotConfig Bar)

plotBarsWith :: (HasCallStack) => T.Text -> Maybe T.Text -> PlotConfig -> DataFrame -> IO ()
plotBarsWith colName groupByCol config df =
    case groupByCol of
        Nothing -> plotSingleBars colName config df
        Just grpCol -> plotGroupedBarsWith grpCol colName config df

plotSingleBars :: (HasCallStack) => T.Text -> PlotConfig -> DataFrame -> IO ()
plotSingleBars colName config df = do
    let barData = getCategoricalCounts colName df
    case barData of
        Just counts -> do
            let grouped = groupWithOther 10 counts
            T.putStrLn $ bars grouped (plotSettings config)
        Nothing -> do
            let values = extractNumericColumn colName df
            if length values > 20
                then do
                    let labels = map (\i -> "Item " <> T.pack (show i)) [1 .. length values]
                        paired = zip labels values
                        grouped = groupWithOther 10 paired
                    T.putStrLn $ bars grouped (plotSettings config)
                else do
                    let labels = map (\i -> "Item " <> T.pack (show i)) [1 .. length values]
                    T.putStrLn $ bars (zip labels values) (plotSettings config)

plotBarsTopN :: (HasCallStack) => Int -> T.Text -> DataFrame -> IO ()
plotBarsTopN n colName = plotBarsTopNWith n colName (defaultPlotConfig Bar)

plotBarsTopNWith :: (HasCallStack) => Int -> T.Text -> PlotConfig -> DataFrame -> IO ()
plotBarsTopNWith n colName config df = do
    let barData = getCategoricalCounts colName df
    case barData of
        Just counts -> do
            let grouped = groupWithOther n counts
            T.putStrLn $ bars grouped (plotSettings config)
        Nothing -> do
            let values = extractNumericColumn colName df
                labels = map (\i -> "Item " <> T.pack (show i)) [1 .. length values]
                paired = zip labels values
                grouped = groupWithOther n paired
            T.putStrLn $ bars grouped (plotSettings config)

plotGroupedBarsWith :: (HasCallStack) => T.Text -> T.Text -> PlotConfig -> DataFrame -> IO ()
plotGroupedBarsWith = plotGroupedBarsWithN 10

plotGroupedBarsWithN :: (HasCallStack) => Int -> T.Text -> T.Text -> PlotConfig -> DataFrame -> IO ()
plotGroupedBarsWithN n groupCol valCol config df = do
    let colIsNumeric = isNumericColumnCheck valCol df

    if colIsNumeric
        then do
            let groups = extractStringColumn groupCol df
                values = extractNumericColumn valCol df
                grouped = M.toList $ M.fromListWith (+) (zip groups values)
                finalGroups = groupWithOther n grouped
            T.putStrLn $ bars finalGroups (plotSettings config)
        else do
            let groups = extractStringColumn groupCol df
                vals = extractStringColumn valCol df
                pairs = zip groups vals
                counts =
                    M.toList $
                        M.fromListWith
                            (+)
                            [(g <> " - " <> v, 1) | (g, v) <- pairs]
                finalCounts = groupWithOther n [(k, fromIntegral v) | (k, v) <- counts]
            T.putStrLn $ bars finalCounts (plotSettings config)

plotValueCounts :: (HasCallStack) => T.Text -> DataFrame -> IO ()
plotValueCounts colName = plotValueCountsWith colName 10 (defaultPlotConfig Bar)

plotValueCountsWith :: (HasCallStack) => T.Text -> Int -> PlotConfig -> DataFrame -> IO ()
plotValueCountsWith colName maxBars config df = do
    let counts = getCategoricalCounts colName df
    case counts of
        Just c -> do
            let grouped = groupWithOther maxBars c
                config' =
                    config
                        { plotSettings =
                            (plotSettings config)
                                { plotTitle =
                                    if T.null (plotTitle (plotSettings config))
                                        then "Value counts for " <> colName
                                        else plotTitle (plotSettings config)
                                }
                        }
            T.putStrLn $ bars grouped (plotSettings config')
        Nothing -> error $ "Could not get value counts for column " ++ T.unpack colName

plotBarsWithPercentages :: (HasCallStack) => T.Text -> DataFrame -> IO ()
plotBarsWithPercentages colName df = do
    let counts = getCategoricalCounts colName df
    case counts of
        Just c -> do
            let total = sum (map snd c)
                percentages =
                    [ (label <> " (" <> T.pack (show (round (100 * val / total) :: Int)) <> "%)", val)
                    | (label, val) <- c
                    ]
                grouped = groupWithOther 10 percentages
            T.putStrLn $ bars grouped (defPlot{plotTitle = "Distribution of " <> colName})
        Nothing -> error $ "Could not get value counts for column " ++ T.unpack colName

smartPlotBars :: (HasCallStack) => T.Text -> DataFrame -> IO ()
smartPlotBars colName df = do
    let counts = getCategoricalCounts colName df
    case counts of
        Just c -> do
            let numUnique = length c
                config =
                    (defaultPlotConfig Bar)
                        { plotSettings = (plotSettings (defaultPlotConfig Bar)){plotTitle = colName <> " (" <> T.pack (show numUnique) <> " unique values)"}
                        }
            if numUnique <= 12
                then T.putStrLn $ bars c (plotSettings config)
                else
                    if numUnique <= 20
                        then do
                            let grouped = groupWithOther 12 c
                            T.putStrLn $ bars grouped (plotSettings config)
                        else do
                            let grouped = groupWithOther 10 c
                                otherCount = numUnique - 10
                            T.putStrLn $
                                bars
                                    grouped
                                    (plotSettings config)
        Nothing -> plotBars colName df

plotCategoricalSummary :: (HasCallStack) => DataFrame -> IO ()
plotCategoricalSummary df = do
    let cols = columnNames df
    forM_ cols $ \col -> do
        let counts = getCategoricalCounts col df
        case counts of
            Just c -> when (length c > 1) $ do
                let numUnique = length c
                putStrLn $ "\n=== " ++ T.unpack col ++ " (" ++ show numUnique ++ " unique values) ==="
                if numUnique > 15 then plotBarsTopN 10 col df else plotBars col df
            Nothing -> return ()

getCategoricalCounts :: (HasCallStack) => T.Text -> DataFrame -> Maybe [(T.Text, Double)]
getCategoricalCounts colName df =
    case M.lookup colName (columnIndices df) of
        Nothing -> error $ "Column " ++ T.unpack colName ++ " not found"
        Just idx ->
            let col = columns df V.! idx
             in case col of
                    BoxedColumn vec ->
                        let counts = countValues vec
                         in Just [(T.pack (show k), fromIntegral v) | (k, v) <- counts]
                    UnboxedColumn vec ->
                        let counts = countValuesUnboxed vec
                         in Just [(T.pack (show k), fromIntegral v) | (k, v) <- counts]
                    OptionalColumn vec ->
                        let counts = countValues vec
                         in Just [(T.pack (show k), fromIntegral v) | (k, v) <- counts]
  where
    countValues :: (Ord a, Show a) => V.Vector a -> [(a, Int)]
    countValues vec = M.toList $ V.foldr' (\x acc -> M.insertWith (+) x 1 acc) M.empty vec

    countValuesUnboxed :: (Ord a, Show a, VU.Unbox a) => VU.Vector a -> [(a, Int)]
    countValuesUnboxed vec = M.toList $ VU.foldr' (\x acc -> M.insertWith (+) x 1 acc) M.empty vec

isNumericColumnCheck :: T.Text -> DataFrame -> Bool
isNumericColumnCheck colName df = isNumericColumn df colName

extractStringColumn :: (HasCallStack) => T.Text -> DataFrame -> [T.Text]
extractStringColumn colName df =
    case M.lookup colName (columnIndices df) of
        Nothing -> error $ "Column " ++ T.unpack colName ++ " not found"
        Just idx ->
            let col = columns df V.! idx
             in case col of
                    BoxedColumn vec -> V.toList $ V.map (T.pack . show) vec
                    UnboxedColumn vec -> V.toList $ VG.map (T.pack . show) (VG.convert vec)
                    OptionalColumn vec -> V.toList $ V.map (T.pack . show) vec

extractNumericColumn :: (HasCallStack) => T.Text -> DataFrame -> [Double]
extractNumericColumn colName df =
    case M.lookup colName (columnIndices df) of
        Nothing -> error $ "Column " ++ T.unpack colName ++ " not found"
        Just idx ->
            let col = columns df V.! idx
             in case col of
                    BoxedColumn vec -> vectorToDoubles vec
                    UnboxedColumn vec -> unboxedVectorToDoubles vec
                    _ -> []

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

groupWithOther :: Int -> [(T.Text, Double)] -> [(T.Text, Double)]
groupWithOther n items =
    let sorted = L.sortOn (negate . snd) items
        (topN, rest) = splitAt n sorted
        otherSum = sum (map snd rest)
        result =
            if null rest || otherSum == 0
                then topN
                else topN ++ [("Other (" <> T.pack (show (length rest)) <> " items)", otherSum)]
     in result

plotPie :: (HasCallStack) => T.Text -> Maybe T.Text -> DataFrame -> IO ()
plotPie valCol labelCol = plotPieWith valCol labelCol (defaultPlotConfig Pie)

plotPieWith :: (HasCallStack) => T.Text -> Maybe T.Text -> PlotConfig -> DataFrame -> IO ()
plotPieWith valCol labelCol config df = do
    let categoricalData = getCategoricalCounts valCol df
    case categoricalData of
        Just counts -> do
            let grouped = groupWithOtherForPie 8 counts
            T.putStrLn $ pie grouped (plotSettings config)
        Nothing -> do
            let values = extractNumericColumn valCol df
                labels = case labelCol of
                    Nothing -> map (\i -> "Item " <> T.pack (show i)) [1 .. length values]
                    Just lCol -> extractStringColumn lCol df
            let pieData = zip labels values
                grouped =
                    if length pieData > 10
                        then groupWithOtherForPie 8 pieData
                        else pieData
            T.putStrLn $ pie grouped (plotSettings config)

groupWithOtherForPie :: Int -> [(T.Text, Double)] -> [(T.Text, Double)]
groupWithOtherForPie n items =
    let total = sum (map snd items)
        sorted = L.sortOn (negate . snd) items
        (topN, rest) = splitAt n sorted
        otherSum = sum (map snd rest)
        otherPct = round (100 * otherSum / total) :: Int
        result =
            if null rest || otherSum == 0
                then topN
                else
                    topN
                        ++ [
                               ( "Other ("
                                    <> T.pack (show (length rest))
                                    <> " items, "
                                    <> T.pack (show otherPct)
                                    <> "%)"
                               , otherSum
                               )
                           ]
     in result

plotPieWithPercentages :: (HasCallStack) => T.Text -> DataFrame -> IO ()
plotPieWithPercentages colName = plotPieWithPercentagesConfig colName (defaultPlotConfig Pie)

plotPieWithPercentagesConfig :: (HasCallStack) => T.Text -> PlotConfig -> DataFrame -> IO ()
plotPieWithPercentagesConfig colName config df = do
    let counts = getCategoricalCounts colName df
    case counts of
        Just c -> do
            let total = sum (map snd c)
                withPct =
                    [ (label <> " (" <> T.pack (show (round (100 * val / total) :: Int)) <> "%)", val)
                    | (label, val) <- c
                    ]
                grouped = groupWithOtherForPie 8 withPct
            T.putStrLn $ pie grouped (plotSettings config)
        Nothing -> do
            let values = extractNumericColumn colName df
                total = sum values
                labels = map (\i -> "Item " <> T.pack (show i)) [1 .. length values]
                withPct =
                    [ (label <> " (" <> T.pack (show (round (100 * val / total) :: Int)) <> "%)", val)
                    | (label, val) <- zip labels values
                    ]
                grouped = groupWithOtherForPie 8 withPct
            T.putStrLn $ pie grouped (plotSettings config)

plotPieTopN :: (HasCallStack) => Int -> T.Text -> DataFrame -> IO ()
plotPieTopN n colName = plotPieTopNWith n colName (defaultPlotConfig Pie)

plotPieTopNWith :: (HasCallStack) => Int -> T.Text -> PlotConfig -> DataFrame -> IO ()
plotPieTopNWith n colName config df = do
    let counts = getCategoricalCounts colName df
    case counts of
        Just c -> do
            let grouped = groupWithOtherForPie n c
            T.putStrLn $ pie grouped (plotSettings config)
        Nothing -> do
            let values = extractNumericColumn colName df
                labels = map (\i -> "Item " <> T.pack (show i)) [1 .. length values]
                paired = zip labels values
                grouped = groupWithOtherForPie n paired
            T.putStrLn $ pie grouped (plotSettings config)

smartPlotPie :: (HasCallStack) => T.Text -> DataFrame -> IO ()
smartPlotPie colName df = do
    let counts = getCategoricalCounts colName df
    case counts of
        Just c -> do
            let numUnique = length c
                total = sum (map snd c)
                significant = filter (\(_, v) -> v / total >= 0.01) c
                config =
                    (defaultPlotConfig Pie)
                        { plotSettings = (plotSettings (defaultPlotConfig Pie)){plotTitle = colName <> " Distribution"}
                        }
            if length significant <= 6
                then T.putStrLn $ pie significant (plotSettings config)
                else
                    if length significant <= 10
                        then do
                            let grouped = groupWithOtherForPie 8 c
                            T.putStrLn $ pie grouped (plotSettings config)
                        else do
                            let grouped = groupWithOtherForPie 6 c
                            T.putStrLn $ pie grouped (plotSettings config)
        Nothing -> plotPie colName Nothing df

plotPieGrouped :: (HasCallStack) => T.Text -> T.Text -> DataFrame -> IO ()
plotPieGrouped groupCol valCol = plotPieGroupedWith groupCol valCol (defaultPlotConfig Pie)

plotPieGroupedWith :: (HasCallStack) => T.Text -> T.Text -> PlotConfig -> DataFrame -> IO ()
plotPieGroupedWith groupCol valCol config df = do
    let colIsNumeric = isNumericColumnCheck valCol df

    if colIsNumeric
        then do
            let groups = extractStringColumn groupCol df
                values = extractNumericColumn valCol df
                grouped = M.toList $ M.fromListWith (+) (zip groups values)
                finalGroups = groupWithOtherForPie 8 grouped
            T.putStrLn $ pie finalGroups (plotSettings config)
        else do
            let groups = extractStringColumn groupCol df
                vals = extractStringColumn valCol df
                combined = zipWith (\g v -> g <> " - " <> v) groups vals
                counts = M.toList $ M.fromListWith (+) [(c, 1) | c <- combined]
                finalCounts = groupWithOtherForPie 10 [(k, fromIntegral v) | (k, v) <- counts]
            T.putStrLn $ pie finalCounts (plotSettings config)

plotPieComparison :: (HasCallStack) => [T.Text] -> DataFrame -> IO ()
plotPieComparison cols df = forM_ cols $ \col -> do
    let counts = getCategoricalCounts col df
    case counts of
        Just c -> when (length c > 1 && length c <= 20) $ do
            putStrLn $ "\n=== " ++ T.unpack col ++ " Distribution ==="
            smartPlotPie col df
        Nothing -> return ()

plotBinaryPie :: (HasCallStack) => T.Text -> DataFrame -> IO ()
plotBinaryPie colName df = do
    let counts = getCategoricalCounts colName df
    case counts of
        Just c ->
            if length c == 2
                then do
                    let total = sum (map snd c)
                        withPct =
                            [ (label <> " (" <> T.pack (show (round (100 * val / total) :: Int)) <> "%)", val)
                            | (label, val) <- c
                            ]
                    T.putStrLn $ pie withPct defPlot
                else
                    error $
                        "Column "
                            ++ T.unpack colName
                            ++ " is not binary (has "
                            ++ show (length c)
                            ++ " unique values)"
        Nothing -> error $ "Column " ++ T.unpack colName ++ " is not categorical"

plotMarketShare :: (HasCallStack) => T.Text -> DataFrame -> IO ()
plotMarketShare colName = plotMarketShareWith colName (defaultPlotConfig Pie)

plotMarketShareWith :: (HasCallStack) => T.Text -> PlotConfig -> DataFrame -> IO ()
plotMarketShareWith colName config df = do
    let counts = getCategoricalCounts colName df
    case counts of
        Just c -> do
            let total = sum (map snd c)
                sorted = L.sortOn (negate . snd) c
                significantShares = takeWhile (\(_, v) -> v / total >= 0.02) sorted
                otherSum = sum [v | (_, v) <- c, v `notElem` map snd significantShares]

                formatShare (label, val) =
                    let pct = round (100 * val / total) :: Int
                     in (label <> " (" <> T.pack (show pct) <> "%)", val)

                shares = map formatShare significantShares
                finalShares =
                    if otherSum > 0 && otherSum / total >= 0.01
                        then shares <> [("Others (<2% each)", otherSum)]
                        else shares

            let config' =
                    config
            -- { plotSettings = (plotSettings config) {
            --         plotTitle = colName <> ": market share"
            --     }
            -- }
            T.putStrLn $ pie finalShares (plotSettings config')
        Nothing -> error $ "Column " ++ T.unpack colName ++ " is not categorical"
