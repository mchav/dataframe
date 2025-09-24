{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Display.Web.Plot where

import Control.Monad
import Data.Char
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
import System.Random (newStdGen, randomRs)
import Type.Reflection (typeRep)

import DataFrame.Display.Web.ChartJs
import DataFrame.Internal.Column (Column (..), isNumeric)
import qualified DataFrame.Internal.Column as D
import DataFrame.Internal.DataFrame (DataFrame (..), getColumn)
import DataFrame.Operations.Core
import qualified DataFrame.Operations.Subset as D
import System.Directory
import System.Info
import System.Process (
    StdStream (NoStream),
    createProcess,
    proc,
    std_err,
    std_in,
    std_out,
    waitForProcess,
 )

newtype HtmlPlot = HtmlPlot T.Text deriving (Show)

data PlotConfig = PlotConfig
    { plotType :: PlotType
    , plotTitle :: T.Text
    , plotWidth :: Int
    , plotHeight :: Int
    , plotFile :: Maybe FilePath
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
        , plotTitle = ""
        , plotWidth = 600
        , plotHeight = 400
        , plotFile = Nothing
        }

generateChartId :: IO T.Text
generateChartId = do
    gen <- newStdGen
    let randomWords =
            filter
                (\c -> c `elem` ([49 .. 57] ++ [65 .. 90] ++ [97 .. 122]))
                (take 64 (randomRs (49, 126) gen :: [Int]))
    return $ "chart_" <> T.pack (map chr randomWords)

wrapInHTML :: T.Text -> T.Text -> Int -> Int -> T.Text
wrapInHTML chartId content width height =
    T.concat
        [ "<canvas id=\""
        , chartId
        , "\" style=\"width:100%;max-width:"
        , T.pack (show width)
        , "px;height:"
        , T.pack (show height)
        , "px\"></canvas>\n"
        , "<script>"
        , minifiedChartJs
        , "</script>\n"
        , "<script>\n"
        , content
        , "\n</script>\n"
        ]

plotHistogram :: (HasCallStack) => T.Text -> DataFrame -> IO HtmlPlot
plotHistogram colName = plotHistogramWith colName (defaultPlotConfig Histogram)

plotHistogramWith ::
    (HasCallStack) => T.Text -> PlotConfig -> DataFrame -> IO HtmlPlot
plotHistogramWith colName config df = do
    chartId <- generateChartId
    let values = extractNumericColumn colName df
        (minVal, maxVal) = if null values then (0, 1) else (minimum values, maximum values)
        numBins = 30
        binWidth = (maxVal - minVal) / fromIntegral numBins
        bins = [minVal + fromIntegral i * binWidth | i <- [0 .. numBins - 1]]
        counts = calculateHistogram values bins binWidth

        labels =
            T.intercalate "," ["\"" <> T.pack (show (round b :: Int)) <> "\"" | b <- bins]
        dataPoints = T.intercalate "," [T.pack (show c) | c <- counts]

        chartTitle =
            if T.null (plotTitle config)
                then "Histogram of " <> colName
                else plotTitle config

        jsCode =
            T.concat
                [ "new Chart(\""
                , chartId
                , "\", {\n"
                , "  type: \"bar\",\n"
                , "  data: {\n"
                , "    labels: ["
                , labels
                , "],\n"
                , "    datasets: [{\n"
                , "      label: \""
                , colName
                , "\",\n"
                , "      data: ["
                , dataPoints
                , "],\n"
                , "      backgroundColor: \"rgba(75, 192, 192, 0.6)\",\n"
                , "      borderColor: \"rgba(75, 192, 192, 1)\",\n"
                , "      borderWidth: 1\n"
                , "    }]\n"
                , "  },\n"
                , "  options: {\n"
                , "    title: { display: true, text: \""
                , chartTitle
                , "\" },\n"
                , "    scales: {\n"
                , "      yAxes: [{ ticks: { beginAtZero: true } }]\n"
                , "    }\n"
                , "  }\n"
                , "});"
                ]

    return $
        HtmlPlot $
            wrapInHTML chartId jsCode (plotWidth config) (plotHeight config)

calculateHistogram :: [Double] -> [Double] -> Double -> [Int]
calculateHistogram values bins binWidth =
    let countBin b = length [v | v <- values, v >= b && v < b + binWidth]
     in map countBin bins

plotScatter :: (HasCallStack) => T.Text -> T.Text -> DataFrame -> IO HtmlPlot
plotScatter xCol yCol = plotScatterWith xCol yCol (defaultPlotConfig Scatter)

plotScatterWith ::
    (HasCallStack) => T.Text -> T.Text -> PlotConfig -> DataFrame -> IO HtmlPlot
plotScatterWith xCol yCol config df = do
    chartId <- generateChartId
    let xVals = extractNumericColumn xCol df
        yVals = extractNumericColumn yCol df
        points = zip xVals yVals

        dataPoints =
            T.intercalate
                ","
                [ "{x:" <> T.pack (show x) <> ", y:" <> T.pack (show y) <> "}" | (x, y) <- points
                ]
        chartTitle =
            if T.null (plotTitle config) then xCol <> " vs " <> yCol else plotTitle config

        jsCode =
            T.concat
                [ "new Chart(\""
                , chartId
                , "\", {\n"
                , "  type: \"scatter\",\n"
                , "  data: {\n"
                , "    datasets: [{\n"
                , "      label: \""
                , chartTitle
                , "\",\n"
                , "      data: ["
                , dataPoints
                , "],\n"
                , "      pointRadius: 4,\n"
                , "      pointBackgroundColor: \"rgb(75, 192, 192)\"\n"
                , "    }]\n"
                , "  },\n"
                , "  options: {\n"
                , "    title: { display: true, text: \""
                , chartTitle
                , "\" },\n"
                , "    scales: {\n"
                , "      xAxes: [{ scaleLabel: { display: true, labelString: \""
                , xCol
                , "\" } }],\n"
                , "      yAxes: [{ scaleLabel: { display: true, labelString: \""
                , yCol
                , "\" } }]\n"
                , "    }\n"
                , "  }\n"
                , "});"
                ]

    return $
        HtmlPlot $
            wrapInHTML chartId jsCode (plotWidth config) (plotHeight config)

plotScatterBy ::
    (HasCallStack) => T.Text -> T.Text -> T.Text -> DataFrame -> IO HtmlPlot
plotScatterBy xCol yCol grouping = plotScatterByWith xCol yCol grouping (defaultPlotConfig Scatter)

plotScatterByWith ::
    (HasCallStack) =>
    T.Text -> T.Text -> T.Text -> PlotConfig -> DataFrame -> IO HtmlPlot
plotScatterByWith xCol yCol grouping config df = do
    chartId <- generateChartId
    let vals = extractStringColumn grouping df
        df' = insertColumn grouping (D.fromList vals) df
        uniqueVals = L.nub vals

        colors =
            cycle
                [ "rgb(255, 99, 132)"
                , "rgb(54, 162, 235)"
                , "rgb(255, 206, 86)"
                , "rgb(75, 192, 192)"
                , "rgb(153, 102, 255)"
                , "rgb(255, 159, 64)"
                ]

    datasets <- forM (zip uniqueVals colors) $ \(val, color) -> do
        let filtered = D.filter grouping (== val) df'
            xVals = extractNumericColumn xCol filtered
            yVals = extractNumericColumn yCol filtered
            points = zip xVals yVals
            dataPoints =
                T.intercalate
                    ","
                    [ "{x:" <> T.pack (show x) <> ", y:" <> T.pack (show y) <> "}" | (x, y) <- points
                    ]
        return $
            T.concat
                [ "    {\n"
                , "      label: \""
                , val
                , "\",\n"
                , "      data: ["
                , dataPoints
                , "],\n"
                , "      pointRadius: 4,\n"
                , "      pointBackgroundColor: \""
                , color
                , "\"\n"
                , "    }"
                ]

    let datasetsStr = T.intercalate ",\n" datasets
        chartTitle =
            if T.null (plotTitle config)
                then xCol <> " vs " <> yCol <> " by " <> grouping
                else plotTitle config

        jsCode =
            T.concat
                [ "new Chart(\""
                , chartId
                , "\", {\n"
                , "  type: \"scatter\",\n"
                , "  data: {\n"
                , "    datasets: [\n"
                , datasetsStr
                , "\n    ]\n"
                , "  },\n"
                , "  options: {\n"
                , "    title: { display: true, text: \""
                , chartTitle
                , "\" },\n"
                , "    scales: {\n"
                , "      xAxes: [{ scaleLabel: { display: true, labelString: \""
                , xCol
                , "\" } }],\n"
                , "      yAxes: [{ scaleLabel: { display: true, labelString: \""
                , yCol
                , "\" } }]\n"
                , "    }\n"
                , "  }\n"
                , "});"
                ]

    return $
        HtmlPlot $
            wrapInHTML chartId jsCode (plotWidth config) (plotHeight config)

plotLines :: (HasCallStack) => T.Text -> [T.Text] -> DataFrame -> IO HtmlPlot
plotLines xAxis colNames = plotLinesWith xAxis colNames (defaultPlotConfig Line)

plotLinesWith ::
    (HasCallStack) => T.Text -> [T.Text] -> PlotConfig -> DataFrame -> IO HtmlPlot
plotLinesWith xAxis colNames config df = do
    chartId <- generateChartId
    let xValues = extractNumericColumn xAxis df
        labels = T.intercalate "," [T.pack (show x) | x <- xValues]

        colors =
            cycle
                [ "rgb(255, 99, 132)"
                , "rgb(54, 162, 235)"
                , "rgb(255, 206, 86)"
                , "rgb(75, 192, 192)"
                , "rgb(153, 102, 255)"
                , "rgb(255, 159, 64)"
                ]

    datasets <- forM (zip colNames colors) $ \(col, color) -> do
        let values = extractNumericColumn col df
            dataPoints = T.intercalate "," [T.pack (show v) | v <- values]
        return $
            T.concat
                [ "    {\n"
                , "      label: \""
                , col
                , "\",\n"
                , "      data: ["
                , dataPoints
                , "],\n"
                , "      fill: false,\n"
                , "      borderColor: \""
                , color
                , "\",\n"
                , "      tension: 0.1\n"
                , "    }"
                ]

    let datasetsStr = T.intercalate ",\n" datasets
        chartTitle = if T.null (plotTitle config) then "Line Chart" else plotTitle config

        jsCode =
            T.concat
                [ "new Chart(\""
                , chartId
                , "\", {\n"
                , "  type: \"line\",\n"
                , "  data: {\n"
                , "    labels: ["
                , labels
                , "],\n"
                , "    datasets: [\n"
                , datasetsStr
                , "\n    ]\n"
                , "  },\n"
                , "  options: {\n"
                , "    title: { display: true, text: \""
                , chartTitle
                , "\" },\n"
                , "    scales: {\n"
                , "      xAxes: [{ scaleLabel: { display: true, labelString: \""
                , xAxis
                , "\" } }]\n"
                , "    }\n"
                , "  }\n"
                , "});"
                ]

    return $
        HtmlPlot $
            wrapInHTML chartId jsCode (plotWidth config) (plotHeight config)

plotBars :: (HasCallStack) => T.Text -> DataFrame -> IO HtmlPlot
plotBars colName = plotBarsWith colName Nothing (defaultPlotConfig Bar)

plotBarsWith ::
    (HasCallStack) =>
    T.Text -> Maybe T.Text -> PlotConfig -> DataFrame -> IO HtmlPlot
plotBarsWith colName groupByCol config df =
    case groupByCol of
        Nothing -> plotSingleBars colName config df
        Just grpCol -> plotGroupedBarsWith grpCol colName config df

plotSingleBars ::
    (HasCallStack) => T.Text -> PlotConfig -> DataFrame -> IO HtmlPlot
plotSingleBars colName config df = do
    chartId <- generateChartId
    let barData = getCategoricalCounts colName df
    case barData of
        Just counts -> do
            let grouped = groupWithOther 10 counts
                labels = T.intercalate "," ["\"" <> label <> "\"" | (label, _) <- grouped]
                dataPoints = T.intercalate "," [T.pack (show val) | (_, val) <- grouped]
                chartTitle = if T.null (plotTitle config) then colName else plotTitle config

                jsCode =
                    T.concat
                        [ "new Chart(\""
                        , chartId
                        , "\", {\n"
                        , "  type: \"bar\",\n"
                        , "  data: {\n"
                        , "    labels: ["
                        , labels
                        , "],\n"
                        , "    datasets: [{\n"
                        , "      label: \"Count\",\n"
                        , "      data: ["
                        , dataPoints
                        , "],\n"
                        , "      backgroundColor: \"rgba(54, 162, 235, 0.6)\",\n"
                        , "      borderColor: \"rgba(54, 162, 235, 1)\",\n"
                        , "      borderWidth: 1\n"
                        , "    }]\n"
                        , "  },\n"
                        , "  options: {\n"
                        , "    title: { display: true, text: \""
                        , chartTitle
                        , "\" },\n"
                        , "    scales: {\n"
                        , "      yAxes: [{ ticks: { beginAtZero: true } }]\n"
                        , "    }\n"
                        , "  }\n"
                        , "});"
                        ]
            return $
                HtmlPlot $
                    wrapInHTML chartId jsCode (plotWidth config) (plotHeight config)
        Nothing -> do
            let values = extractNumericColumn colName df
                labels' =
                    if length values > 20
                        then take 20 ["Item " <> T.pack (show i) | i <- [1 ..]]
                        else ["Item " <> T.pack (show i) | i <- [1 .. length values]]
                vals = if length values > 20 then take 20 values else values
                labels = T.intercalate "," ["\"" <> label <> "\"" | label <- labels']
                dataPoints = T.intercalate "," [T.pack (show val) | val <- vals]
                chartTitle = if T.null (plotTitle config) then colName else plotTitle config

                jsCode =
                    T.concat
                        [ "new Chart(\""
                        , chartId
                        , "\", {\n"
                        , "  type: \"bar\",\n"
                        , "  data: {\n"
                        , "    labels: ["
                        , labels
                        , "],\n"
                        , "    datasets: [{\n"
                        , "      label: \"Value\",\n"
                        , "      data: ["
                        , dataPoints
                        , "],\n"
                        , "      backgroundColor: \"rgba(54, 162, 235, 0.6)\",\n"
                        , "      borderColor: \"rgba(54, 162, 235, 1)\",\n"
                        , "      borderWidth: 1\n"
                        , "    }]\n"
                        , "  },\n"
                        , "  options: {\n"
                        , "    title: { display: true, text: \""
                        , chartTitle
                        , "\" },\n"
                        , "    scales: {\n"
                        , "      yAxes: [{ ticks: { beginAtZero: true } }]\n"
                        , "    }\n"
                        , "  }\n"
                        , "});"
                        ]
            return $
                HtmlPlot $
                    wrapInHTML chartId jsCode (plotWidth config) (plotHeight config)

plotPie :: (HasCallStack) => T.Text -> Maybe T.Text -> DataFrame -> IO HtmlPlot
plotPie valCol labelCol = plotPieWith valCol labelCol (defaultPlotConfig Pie)

plotPieWith ::
    (HasCallStack) =>
    T.Text -> Maybe T.Text -> PlotConfig -> DataFrame -> IO HtmlPlot
plotPieWith valCol labelCol config df = do
    chartId <- generateChartId
    let categoricalData = getCategoricalCounts valCol df
    case categoricalData of
        Just counts -> do
            let grouped = groupWithOtherForPie 8 counts
                labels = T.intercalate "," ["\"" <> label <> "\"" | (label, _) <- grouped]
                dataPoints = T.intercalate "," [T.pack (show val) | (_, val) <- grouped]
                colors = T.intercalate "," ["\"" <> c <> "\"" | c <- take (length grouped) pieColors]
                chartTitle = if T.null (plotTitle config) then valCol else plotTitle config

                jsCode =
                    T.concat
                        [ "new Chart(\""
                        , chartId
                        , "\", {\n"
                        , "  type: \"pie\",\n"
                        , "  data: {\n"
                        , "    labels: ["
                        , labels
                        , "],\n"
                        , "    datasets: [{\n"
                        , "      data: ["
                        , dataPoints
                        , "],\n"
                        , "      backgroundColor: ["
                        , colors
                        , "]\n"
                        , "    }]\n"
                        , "  },\n"
                        , "  options: {\n"
                        , "    title: { display: true, text: \""
                        , chartTitle
                        , "\" }\n"
                        , "  }\n"
                        , "});"
                        ]
            return $
                HtmlPlot $
                    wrapInHTML chartId jsCode (plotWidth config) (plotHeight config)
        Nothing -> do
            let values = extractNumericColumn valCol df
                labels' = case labelCol of
                    Nothing -> map (\i -> "Item " <> T.pack (show i)) [1 .. length values]
                    Just lCol -> extractStringColumn lCol df
                pieData = zip labels' values
                grouped =
                    if length pieData > 10
                        then groupWithOtherForPie 8 pieData
                        else pieData
                labels = T.intercalate "," ["\"" <> label <> "\"" | (label, _) <- grouped]
                dataPoints = T.intercalate "," [T.pack (show val) | (_, val) <- grouped]
                colors = T.intercalate "," ["\"" <> c <> "\"" | c <- take (length grouped) pieColors]
                chartTitle = if T.null (plotTitle config) then valCol else plotTitle config

                jsCode =
                    T.concat
                        [ "new Chart(\""
                        , chartId
                        , "\", {\n"
                        , "  type: \"pie\",\n"
                        , "  data: {\n"
                        , "    labels: ["
                        , labels
                        , "],\n"
                        , "    datasets: [{\n"
                        , "      data: ["
                        , dataPoints
                        , "],\n"
                        , "      backgroundColor: ["
                        , colors
                        , "]\n"
                        , "    }]\n"
                        , "  },\n"
                        , "  options: {\n"
                        , "    title: { display: true, text: \""
                        , chartTitle
                        , "\" }\n"
                        , "  }\n"
                        , "});"
                        ]
            return $
                HtmlPlot $
                    wrapInHTML chartId jsCode (plotWidth config) (plotHeight config)

pieColors :: [T.Text]
pieColors =
    [ "rgb(255, 99, 132)"
    , "rgb(54, 162, 235)"
    , "rgb(255, 206, 86)"
    , "rgb(75, 192, 192)"
    , "rgb(153, 102, 255)"
    , "rgb(255, 159, 64)"
    , "rgb(201, 203, 207)"
    , "rgb(255, 99, 71)"
    , "rgb(60, 179, 113)"
    , "rgb(238, 130, 238)"
    ]

plotStackedBars ::
    (HasCallStack) => T.Text -> [T.Text] -> DataFrame -> IO HtmlPlot
plotStackedBars categoryCol valueColumns = plotStackedBarsWith categoryCol valueColumns (defaultPlotConfig StackedBar)

plotStackedBarsWith ::
    (HasCallStack) => T.Text -> [T.Text] -> PlotConfig -> DataFrame -> IO HtmlPlot
plotStackedBarsWith categoryCol valueColumns config df = do
    chartId <- generateChartId
    let categories = extractStringColumn categoryCol df
        uniqueCategories = L.nub categories

        colors =
            cycle
                [ "rgb(255, 99, 132)"
                , "rgb(54, 162, 235)"
                , "rgb(255, 206, 86)"
                , "rgb(75, 192, 192)"
                , "rgb(153, 102, 255)"
                , "rgb(255, 159, 64)"
                ]

    datasets <- forM (zip valueColumns colors) $ \(col, color) -> do
        dataVals <- forM uniqueCategories $ \cat -> do
            let indices = [i | (i, c) <- zip [0 ..] categories, c == cat]
                allValues = extractNumericColumn col df
                values = [allValues !! i | i <- indices, i < length allValues]
            return $ sum values
        let dataPoints = T.intercalate "," [T.pack (show v) | v <- dataVals]
        return $
            T.concat
                [ "    {\n"
                , "      label: \""
                , col
                , "\",\n"
                , "      data: ["
                , dataPoints
                , "],\n"
                , "      backgroundColor: \""
                , color
                , "\"\n"
                , "    }"
                ]

    let datasetsStr = T.intercalate ",\n" datasets
        labels = T.intercalate "," ["\"" <> cat <> "\"" | cat <- uniqueCategories]
        chartTitle = if T.null (plotTitle config) then "Stacked Bar Chart" else plotTitle config

        jsCode =
            T.concat
                [ "new Chart(\""
                , chartId
                , "\", {\n"
                , "  type: \"bar\",\n"
                , "  data: {\n"
                , "    labels: ["
                , labels
                , "],\n"
                , "    datasets: [\n"
                , datasetsStr
                , "\n    ]\n"
                , "  },\n"
                , "  options: {\n"
                , "    title: { display: true, text: \""
                , chartTitle
                , "\" },\n"
                , "    scales: {\n"
                , "      xAxes: [{ stacked: true }],\n"
                , "      yAxes: [{ stacked: true, ticks: { beginAtZero: true } }]\n"
                , "    }\n"
                , "  }\n"
                , "});"
                ]

    return $
        HtmlPlot $
            wrapInHTML chartId jsCode (plotWidth config) (plotHeight config)

plotBoxPlots :: (HasCallStack) => [T.Text] -> DataFrame -> IO HtmlPlot
plotBoxPlots colNames = plotBoxPlotsWith colNames (defaultPlotConfig BoxPlot)

plotBoxPlotsWith ::
    (HasCallStack) => [T.Text] -> PlotConfig -> DataFrame -> IO HtmlPlot
plotBoxPlotsWith colNames config df = do
    chartId <- generateChartId
    boxData <- forM colNames $ \col -> do
        let values = extractNumericColumn col df
            sorted = L.sort values
            n = length values
            q1 = sorted !! (n `div` 4)
            median = sorted !! (n `div` 2)
            q3 = sorted !! (3 * n `div` 4)
            minVal = minimum values
            maxVal = maximum values
        return (col, minVal, q1, median, q3, maxVal)

    let labels = T.intercalate "," ["\"" <> col <> "\"" | (col, _, _, _, _, _) <- boxData]
        medians = T.intercalate "," [T.pack (show med) | (_, _, _, med, _, _) <- boxData]
        mins = T.intercalate "," [T.pack (show minV) | (_, minV, _, _, _, _) <- boxData]
        maxs = T.intercalate "," [T.pack (show maxV) | (_, _, _, _, _, maxV) <- boxData]
        chartTitle = if T.null (plotTitle config) then "Box Plot" else plotTitle config

        jsCode =
            T.concat
                [ "new Chart(\""
                , chartId
                , "\", {\n"
                , "  type: \"bar\",\n"
                , "  data: {\n"
                , "    labels: ["
                , labels
                , "],\n"
                , "    datasets: [{\n"
                , "      label: \"Median\",\n"
                , "      data: ["
                , medians
                , "],\n"
                , "      backgroundColor: \"rgba(75, 192, 192, 0.6)\",\n"
                , "      borderColor: \"rgba(75, 192, 192, 1)\",\n"
                , "      borderWidth: 1\n"
                , "    }]\n"
                , "  },\n"
                , "  options: {\n"
                , "    title: { display: true, text: \""
                , chartTitle
                , " (showing medians)\" },\n"
                , "    scales: {\n"
                , "      yAxes: [{ ticks: { beginAtZero: true } }]\n"
                , "    }\n"
                , "  }\n"
                , "});"
                ]

    return $
        HtmlPlot $
            wrapInHTML chartId jsCode (plotWidth config) (plotHeight config)

plotGroupedBarsWith ::
    (HasCallStack) => T.Text -> T.Text -> PlotConfig -> DataFrame -> IO HtmlPlot
plotGroupedBarsWith = plotGroupedBarsWithN 10

plotGroupedBarsWithN ::
    (HasCallStack) =>
    Int -> T.Text -> T.Text -> PlotConfig -> DataFrame -> IO HtmlPlot
plotGroupedBarsWithN n groupCol valCol config df = do
    chartId <- generateChartId
    let colIsNumeric = isNumericColumnCheck valCol df

    if colIsNumeric
        then do
            let groups = extractStringColumn groupCol df
                values = extractNumericColumn valCol df
                grouped = M.toList $ M.fromListWith (+) (zip groups values)
                finalGroups = groupWithOther n grouped
                labels = T.intercalate "," ["\"" <> label <> "\"" | (label, _) <- finalGroups]
                dataPoints = T.intercalate "," [T.pack (show val) | (_, val) <- finalGroups]
                chartTitle =
                    if T.null (plotTitle config)
                        then groupCol <> " by " <> valCol
                        else plotTitle config

                jsCode =
                    T.concat
                        [ "new Chart(\""
                        , chartId
                        , "\", {\n"
                        , "  type: \"bar\",\n"
                        , "  data: {\n"
                        , "    labels: ["
                        , labels
                        , "],\n"
                        , "    datasets: [{\n"
                        , "      label: \""
                        , valCol
                        , "\",\n"
                        , "      data: ["
                        , dataPoints
                        , "],\n"
                        , "      backgroundColor: \"rgba(54, 162, 235, 0.6)\",\n"
                        , "      borderColor: \"rgba(54, 162, 235, 1)\",\n"
                        , "      borderWidth: 1\n"
                        , "    }]\n"
                        , "  },\n"
                        , "  options: {\n"
                        , "    title: { display: true, text: \""
                        , chartTitle
                        , "\" },\n"
                        , "    scales: {\n"
                        , "      yAxes: [{ ticks: { beginAtZero: true } }]\n"
                        , "    }\n"
                        , "  }\n"
                        , "});"
                        ]
            return $
                HtmlPlot $
                    wrapInHTML chartId jsCode (plotWidth config) (plotHeight config)
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
                labels = T.intercalate "," ["\"" <> label <> "\"" | (label, _) <- finalCounts]
                dataPoints = T.intercalate "," [T.pack (show val) | (_, val) <- finalCounts]
                chartTitle =
                    if T.null (plotTitle config)
                        then groupCol <> " by " <> valCol
                        else plotTitle config

                jsCode =
                    T.concat
                        [ "new Chart(\""
                        , chartId
                        , "\", {\n"
                        , "  type: \"bar\",\n"
                        , "  data: {\n"
                        , "    labels: ["
                        , labels
                        , "],\n"
                        , "    datasets: [{\n"
                        , "      label: \"Count\",\n"
                        , "      data: ["
                        , dataPoints
                        , "],\n"
                        , "      backgroundColor: \"rgba(54, 162, 235, 0.6)\",\n"
                        , "      borderColor: \"rgba(54, 162, 235, 1)\",\n"
                        , "      borderWidth: 1\n"
                        , "    }]\n"
                        , "  },\n"
                        , "  options: {\n"
                        , "    title: { display: true, text: \""
                        , chartTitle
                        , "\" },\n"
                        , "    scales: {\n"
                        , "      yAxes: [{ ticks: { beginAtZero: true } }]\n"
                        , "    }\n"
                        , "  }\n"
                        , "});"
                        ]
            return $
                HtmlPlot $
                    wrapInHTML chartId jsCode (plotWidth config) (plotHeight config)

-- TODO: Move these helpers to a common module.

isNumericColumn :: DataFrame -> T.Text -> Bool
isNumericColumn df colName = maybe False isNumeric (getColumn colName df)

isNumericColumnCheck :: T.Text -> DataFrame -> Bool
isNumericColumnCheck colName df = isNumericColumn df colName

extractStringColumn :: (HasCallStack) => T.Text -> DataFrame -> [T.Text]
extractStringColumn colName df =
    case M.lookup colName (columnIndices df) of
        Nothing -> error $ "Column " ++ T.unpack colName ++ " not found"
        Just idx ->
            let col = columns df V.! idx
             in case col of
                    BoxedColumn (vec :: V.Vector a) -> case testEquality (typeRep @a) (typeRep @T.Text) of
                        Just Refl -> V.toList vec
                        Nothing -> V.toList $ V.map (T.pack . show) vec
                    UnboxedColumn vec -> V.toList $ VG.map (T.pack . show) (VG.convert vec)
                    OptionalColumn (vec :: V.Vector (Maybe a)) -> case testEquality (typeRep @a) (typeRep @T.Text) of
                        Nothing -> V.toList $ V.map (T.pack . show) vec
                        Just Refl -> V.toList $ V.map (maybe "Nothing" ("Just " <>)) vec

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

unboxedVectorToDoubles ::
    forall a. (Typeable a, VU.Unbox a, Show a) => VU.Vector a -> [Double]
unboxedVectorToDoubles vec =
    case testEquality (typeRep @a) (typeRep @Double) of
        Just Refl -> VU.toList vec
        Nothing -> case testEquality (typeRep @a) (typeRep @Int) of
            Just Refl -> VU.toList $ VU.map fromIntegral vec
            Nothing -> case testEquality (typeRep @a) (typeRep @Float) of
                Just Refl -> VU.toList $ VU.map realToFrac vec
                Nothing -> error $ "Column is not numeric (type: " ++ show (typeRep @a) ++ ")"

getCategoricalCounts ::
    (HasCallStack) => T.Text -> DataFrame -> Maybe [(T.Text, Double)]
getCategoricalCounts colName df =
    case M.lookup colName (columnIndices df) of
        Nothing -> error $ "Column " ++ T.unpack colName ++ " not found"
        Just idx ->
            let col = columns df V.! idx
             in case col of
                    BoxedColumn (vec :: V.Vector a) ->
                        let counts = countValues vec
                         in case testEquality (typeRep @a) (typeRep @T.Text) of
                                Nothing -> Just [(T.pack (show k), fromIntegral v) | (k, v) <- counts]
                                Just Refl -> Just [(k, fromIntegral v) | (k, v) <- counts]
                    UnboxedColumn vec ->
                        let counts = countValuesUnboxed vec
                         in Just [(T.pack (show k), fromIntegral v) | (k, v) <- counts]
                    OptionalColumn (vec :: V.Vector (Maybe a)) ->
                        let counts = countValues vec
                         in case testEquality (typeRep @a) (typeRep @T.Text) of
                                Nothing -> Just [((T.pack . show) k, fromIntegral v) | (k, v) <- counts]
                                Just Refl ->
                                    Just
                                        [(maybe "Nothing" ("Just " <>) k, fromIntegral v) | (k, v) <- counts]
  where
    countValues :: (Ord a, Show a) => V.Vector a -> [(a, Int)]
    countValues vec = M.toList $ V.foldr' (\x acc -> M.insertWith (+) x 1 acc) M.empty vec

    countValuesUnboxed :: (Ord a, Show a, VU.Unbox a) => VU.Vector a -> [(a, Int)]
    countValuesUnboxed vec = M.toList $ VU.foldr' (\x acc -> M.insertWith (+) x 1 acc) M.empty vec

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

plotBarsTopN :: (HasCallStack) => Int -> T.Text -> DataFrame -> IO HtmlPlot
plotBarsTopN n colName = plotBarsTopNWith n colName (defaultPlotConfig Bar)

plotBarsTopNWith ::
    (HasCallStack) => Int -> T.Text -> PlotConfig -> DataFrame -> IO HtmlPlot
plotBarsTopNWith n colName config df = do
    let config' = config{plotTitle = plotTitle config <> " (Top " <> T.pack (show n) <> ")"}
    plotBarsWith colName Nothing config' df

plotValueCounts :: (HasCallStack) => T.Text -> DataFrame -> IO HtmlPlot
plotValueCounts colName = plotValueCountsWith colName 10 (defaultPlotConfig Bar)

plotValueCountsWith ::
    (HasCallStack) => T.Text -> Int -> PlotConfig -> DataFrame -> IO HtmlPlot
plotValueCountsWith colName maxBars config df = do
    let config' = config{plotTitle = "Value counts for " <> colName}
    plotBarsTopNWith maxBars colName config' df

plotAllHistograms :: (HasCallStack) => DataFrame -> IO HtmlPlot
plotAllHistograms df = do
    let numericCols = filter (isNumericColumn df) (columnNames df)
    xs <- forM numericCols $ \col -> do
        T.putStrLn $ "<!-- Histogram for " <> col <> " -->"
        plotHistogram col df
    let allPlots =
            L.foldl'
                (\acc (HtmlPlot contents) -> acc <> "\n" <> T.replace minifiedChartJs "" contents)
                ""
                xs
    return
        (HtmlPlot (T.concat ["\n<script>\n", minifiedChartJs, "\n</script>\n", allPlots]))

plotCategoricalSummary :: (HasCallStack) => DataFrame -> IO HtmlPlot
plotCategoricalSummary df = do
    let cols = columnNames df
    xs <- forM cols $ \col -> do
        let counts = getCategoricalCounts col df
        case counts of
            Just c -> do
                if length c > 1
                    then
                        ( do
                            let numUnique = length c
                            putStrLn $
                                "\n<!-- " ++ T.unpack col ++ " (" ++ show numUnique ++ " unique values) -->"
                            if numUnique > 15 then plotBarsTopN 10 col df else plotBars col df
                        )
                    else return (HtmlPlot "")
            Nothing -> return (HtmlPlot "")
    let allPlots = L.foldl' (\acc (HtmlPlot contents) -> acc <> "\n" <> contents) "" xs
    return (HtmlPlot allPlots)

plotBarsWithPercentages :: (HasCallStack) => T.Text -> DataFrame -> IO HtmlPlot
plotBarsWithPercentages colName df = do
    let config = (defaultPlotConfig Bar){plotTitle = "Distribution of " <> colName}
    plotBarsWith colName Nothing config df

smartPlotBars :: (HasCallStack) => T.Text -> DataFrame -> IO HtmlPlot
smartPlotBars colName df = do
    let counts = getCategoricalCounts colName df
    case counts of
        Just c -> do
            let numUnique = length c
                config =
                    (defaultPlotConfig Bar)
                        { plotTitle = colName <> " (" <> T.pack (show numUnique) <> " unique values)"
                        }
            if numUnique <= 12
                then plotBarsWith colName Nothing config df
                else plotBarsTopNWith 10 colName config df
        Nothing -> plotBars colName df

showInDefaultBrowser :: HtmlPlot -> IO ()
showInDefaultBrowser (HtmlPlot p) = do
    plotId <- generateChartId
    home <- getHomeDirectory
    let operatingSystem = os
    let path = "plot-" <> T.unpack plotId <> ".html"

    let fullPath =
            if operatingSystem == "mingw32"
                then home <> "\\" <> path
                else home <> "/" <> path
    putStr "Saving plot to: "
    putStrLn fullPath
    T.writeFile fullPath p
    if operatingSystem == "mingw32"
        then openFileSilently "start" fullPath
        else openFileSilently "xdg-open" fullPath
    pure ()

openFileSilently :: FilePath -> FilePath -> IO ()
openFileSilently program path = do
    (_, _, _, ph) <-
        createProcess
            (proc program [path])
                { std_in = NoStream
                , std_out = NoStream
                , std_err = NoStream
                }
    void (waitForProcess ph)
