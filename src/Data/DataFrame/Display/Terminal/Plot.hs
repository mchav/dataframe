{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE TupleSections #-}
module Data.DataFrame.Display.Terminal.Plot where

import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU
import qualified Type.Reflection as Ref

import Control.Monad ( forM_, forM )
import Data.Bifunctor ( first )
import Data.Char ( ord, chr )
import Data.DataFrame.Display.Terminal.Colours
import Data.DataFrame.Internal.Column (Column(..))
import Data.DataFrame.Internal.DataFrame (DataFrame(..))
import Data.DataFrame.Internal.Types (Columnable)
import Data.DataFrame.Operations.Core
import Data.Maybe (fromMaybe)
import Data.Typeable (Typeable)
import Data.Type.Equality
    ( type (:~:)(Refl), TestEquality(testEquality) )
import GHC.Stack (HasCallStack)
import Text.Printf ( printf )
import Type.Reflection (typeRep)

data HistogramOrientation = VerticalHistogram | HorizontalHistogram

data PlotColumns = PlotAll | PlotSubset [T.Text]

plotHistograms :: HasCallStack => PlotColumns -> HistogramOrientation -> DataFrame -> IO ()
plotHistograms plotSet orientation df = do
    let cs = case plotSet of
            PlotAll       -> columnNames df
            PlotSubset xs -> columnNames df `L.intersect` xs
    forM_ cs $ \cname -> do
        plotForColumn cname ((V.!) (columns df) (columnIndices df M.! cname)) orientation df


plotHistogramsBy :: HasCallStack => T.Text -> PlotColumns -> HistogramOrientation -> DataFrame -> IO ()
plotHistogramsBy col plotSet orientation df = do
    let cs = case plotSet of
            PlotAll       -> columnNames df
            PlotSubset xs -> columnNames df `L.intersect` xs
    forM_ cs $ \cname -> do
        let plotColumn = (V.!) (columns df) (columnIndices df M.! cname)
        let byColumn = (V.!) (columns df) (columnIndices df M.! col)
        plotForColumnBy col cname byColumn plotColumn orientation df

-- Plot code adapted from: https://alexwlchan.net/2018/ascii-bar-charts/
plotForColumnBy :: HasCallStack => T.Text -> T.Text -> Maybe Column -> Maybe Column -> HistogramOrientation -> DataFrame -> IO ()
plotForColumnBy _ _ Nothing _ _ _ = return ()
plotForColumnBy byCol cname (Just (BoxedColumn (byColumn :: V.Vector a))) (Just (BoxedColumn (plotColumn :: V.Vector b))) orientation df = do
    let zipped = VG.zipWith (\left right -> (show left, show right)) plotColumn byColumn
    let counts = countOccurrences zipped
    if null counts || length counts > 20
    then pure ()
    else case orientation of
        VerticalHistogram -> error "Vertical histograms aren't yet supported"
        HorizontalHistogram -> plotGivenCounts' cname counts
plotForColumnBy byCol cname (Just (UnboxedColumn byColumn)) (Just (BoxedColumn plotColumn)) orientation df = do
    let zipped = VG.zipWith (\left right -> (show left, show right)) plotColumn (V.convert byColumn)
    let counts = countOccurrences zipped
    if null counts || length counts > 20
    then pure ()
    else case orientation of
        VerticalHistogram -> error "Vertical histograms aren't yet supported"
        HorizontalHistogram -> plotGivenCounts' cname counts
plotForColumnBy byCol cname (Just (BoxedColumn byColumn)) (Just (UnboxedColumn plotColumn)) orientation df = do
    let zipped = VG.zipWith (\left right -> (show left, show right)) (V.convert plotColumn) (V.convert byColumn)
    let counts = countOccurrences zipped
    if null counts || length counts > 20
    then pure ()
    else case orientation of
        -- VerticalHistogram -> plotVerticalGivenCounts cname counts
        HorizontalHistogram -> plotGivenCounts' cname counts
plotForColumnBy byCol cname (Just (UnboxedColumn byColumn)) (Just (UnboxedColumn plotColumn)) orientation df = do
    let zipped = VG.zipWith (\left right -> (show left, show right)) (V.convert plotColumn) (V.convert byColumn)
    let counts = countOccurrences zipped
    if null counts || length counts > 20
    then pure ()
    else case orientation of
        VerticalHistogram -> error "Vertical histograms aren't yet supported"
        HorizontalHistogram -> plotGivenCounts' cname counts

-- Plot code adapted from: https://alexwlchan.net/2018/ascii-bar-charts/
plotForColumn :: HasCallStack => T.Text -> Maybe Column -> HistogramOrientation -> DataFrame -> IO ()
plotForColumn _ Nothing _ _ = return ()
plotForColumn cname (Just (BoxedColumn (column :: V.Vector a))) orientation df = do
    let repa :: Ref.TypeRep a = Ref.typeRep @a
        repText :: Ref.TypeRep T.Text = Ref.typeRep @T.Text
        repString :: Ref.TypeRep String = Ref.typeRep @String
    let counts = case repa `testEquality` repText of
            Just Refl -> map (first T.unpack) $ valueCounts @T.Text cname df
            Nothing -> case repa `testEquality` repString of
                Just Refl -> valueCounts @String cname df
                -- Support other scalar types.
                Nothing -> [] -- numericHistogram column
    if null counts || length counts > 20
    then putStrLn $ numericHistogram cname (V.convert column)
    else case orientation of
        VerticalHistogram -> plotVerticalGivenCounts cname counts
        HorizontalHistogram -> plotGivenCounts cname counts
plotForColumn cname (Just (UnboxedColumn (column :: VU.Vector a))) orientation df = do
    let repa :: Ref.TypeRep a = Ref.typeRep @a
        repText :: Ref.TypeRep T.Text = Ref.typeRep @T.Text
        repString :: Ref.TypeRep String = Ref.typeRep @String
    let counts = case repa `testEquality` repText of
            Just Refl -> map (first show) $ valueCounts @T.Text cname df
            Nothing -> case repa `testEquality` repString of
                Just Refl -> valueCounts @String cname df
                -- Support other scalar types.
                Nothing -> []
    if null counts || length counts > 20
    then putStrLn $ numericHistogram cname (V.convert column)
    else case orientation of
        VerticalHistogram -> plotVerticalGivenCounts cname counts
        HorizontalHistogram -> plotGivenCounts cname counts

plotGivenCounts :: HasCallStack => T.Text -> [(String, Integer)] -> IO ()
plotGivenCounts cname counts = do
    putStrLn $ "\nHistogram for " ++ show cname ++ "\n"
    let n = 8 :: Int
    let maxValue = maximum $ map snd counts
    let increment = max 1 (maxValue `div` 50)
    let longestLabelLength = maximum $ map (length . fst) counts
    let longestBar = fromIntegral $ (maxValue * fromIntegral n `div` increment) `div` fromIntegral n + 1
    let border = "|" ++ replicate (longestLabelLength + length (show maxValue) + longestBar + 6) '-' ++ "|"
    body <- forM counts $ \(label, count) -> do
        let barChunks = fromIntegral $ (count * fromIntegral n `div` increment) `div` fromIntegral n
        let remainder = fromIntegral $ (count * fromIntegral n `div` increment) `rem` fromIntegral n
        
#       ifdef mingw32_HOST_OS
        -- Windows doesn't deal well with the fractional unicode types.
        -- They may use a different encoding.
        let fractional = []
#       else
        let fractional = ([chr (ord '█' + n - remainder - 1) | remainder > 0])
#       endif

        let bar = replicate barChunks '█' ++ fractional
        let disp = if null bar then "| " else bar
        let hist=  "|" ++ brightGreen (leftJustify label longestLabelLength) ++ " | " ++
                    leftJustify (show count) (length (show maxValue)) ++ " |" ++
                    " " ++ brightBlue bar
        return $ hist ++ "\n" ++ border
    mapM_ putStrLn (border : body)
    putChar '\n'

plotVerticalGivenCounts :: HasCallStack => T.Text -> [(String, Integer)] -> IO ()
plotVerticalGivenCounts cname counts' = do
    putStrLn $ "\nHistogram for " ++ show cname ++ "\n"
    let n = 8 :: Int
    let clip s = if length s > n then take n s ++ ".." else s
    let counts = map (first clip) counts'
    let maxValue = maximum $ map snd counts
    let increment = max 1 (maxValue `div` 10)
    let longestLabelLength = 2 + maximum (map (length . fst) counts)
    let longestBar = fromIntegral $ (maxValue * fromIntegral n `div` increment) `div` fromIntegral n + 1
    let border = "‾" ++ replicate (longestBar + 1) '|' ++ "+"
    let maximumLineLength = length border
    body <- forM counts $ \(label, count) -> do
        let barChunks = fromIntegral $ (count * fromIntegral n `div` increment) `div` fromIntegral n
        let remainder = fromIntegral $ (count * fromIntegral n `div` increment) `rem` fromIntegral n

#       ifdef mingw32_HOST_OS
        -- Windows doesn't deal well with the fractional unicode types.
        -- They may use a different encoding.
        let fractional = []
#       else
        let fractional = ([chr (ord '█' - (n - remainder - 1)) | remainder > 0])
#       endif

        let bar = replicate barChunks '█' ++ fractional
        let disp = if null bar then "| " else bar
        let hist = "‾" ++ bar
        return $ replicate longestLabelLength (leftJustify hist maximumLineLength) ++ [border]
    let increments = reverse [0,(smallestPartition increment intPlotRanges)..maxValue]
    let incString = map ((++) " " . flip leftJustify longestLabelLength . (++) " " . show) increments
    mapM_ putStrLn (zipWith (++) incString (map brightBlue $ rotate $ border : concat body))
    putStrLn $ " " ++ replicate longestLabelLength ' ' ++ unwords (map (brightGreen . flip leftJustify longestLabelLength . fst) counts)
    putChar '\n'

leftJustify :: String -> Int -> String
leftJustify s n = s ++ replicate (max 0 (n - length s)) ' '


plotGivenCounts' :: HasCallStack => T.Text -> [((String, String), Integer)] -> IO ()
plotGivenCounts' cname counts = do
    putStrLn $ "\nHistogram for " ++ show cname ++ "\n"
    let n = 8 :: Int
    let maxValue = maximum $ map snd counts
    let increment = max 1 (maxValue `div` 50)
    let longestLabelLength = maximum $ map (length. (\(a, b) -> a ++ " " ++ b) . fst) counts
    let longestBar = fromIntegral $ (maxValue * fromIntegral n `div` increment) `div` fromIntegral n + 1
    let border = "|" ++ replicate (longestLabelLength + length (show maxValue) + longestBar + 6) '-' ++ "|"
    body <- forM counts $ \((plotCol, byCol), count) -> do
        let barChunks = fromIntegral $ (count * fromIntegral n `div` increment) `div` fromIntegral n
        let remainder = fromIntegral $ (count * fromIntegral n `div` increment) `rem` fromIntegral n
        
#       ifdef mingw32_HOST_OS
        -- Windows doesn't deal well with the fractional unicode types.
        -- They may use a different encoding.
        let fractional = []
#       else
        let fractional = ([chr (ord '█' + n - remainder - 1) | remainder > 0])
#       endif

        let bar = replicate barChunks '█' ++ fractional
        let disp = if null bar then "| " else bar
        let label = plotCol ++ " " ++ byCol
        let hist=  "|" ++ brightGreen (leftJustify label longestLabelLength) ++ " | " ++
                    leftJustify (show count) (length (show maxValue)) ++ " |" ++
                    " " ++ brightBlue bar
        return $ hist ++ "\n" ++ border
    mapM_ putStrLn (border : body)
    putChar '\n'

numericHistogram :: forall a . (HasCallStack, Columnable a)
                         => T.Text
                         -> V.Vector a
                         -> String
numericHistogram name xs =
    case testEquality (typeRep @a) (typeRep @Double) of
        Just Refl -> let config = defaultConfig {
                    title = Just (T.unpack name),
                    width = 30,
                    height = 10
                }
            in createHistogram config (V.toList xs)
        Nothing -> case testEquality (typeRep @a) (typeRep @Int) of
            Just Refl -> let config = defaultConfig {
                        title = Just (T.unpack name),
                        width = 30,
                        height = 10
                    }
                in createHistogram config (map fromIntegral $ V.toList xs)
            Nothing -> case testEquality (typeRep @a) (typeRep @Integer) of
                Just Refl -> let config = defaultConfig {
                            title = Just (T.unpack name),
                            width = 30,
                            height = 10
                        }
                    in createHistogram config (map fromIntegral $ V.toList xs)
                Nothing -> []

smallestPartition :: (Ord a) => a -> [a] -> a
-- TODO: Find a more graceful way to handle this.
smallestPartition p [] = error "Data range too large to plot"
smallestPartition p (x:y:rest)
    | p < y = x
    | otherwise = smallestPartition p (y:rest)
smallestPartition p (x:rest)
    | p < x = x
    | otherwise = error ""

largestPartition :: (Ord a) => a -> [a] -> a
-- TODO: Find a more graceful way to handle this.
largestPartition p [] = error "Data range too large to plot"
largestPartition p (x:rest)
    | p < x = x
    | otherwise = largestPartition p rest

plotRanges :: [Double]
plotRanges = reverse [-0.1, -0.5,
              -1, -5,
              -10, -50,
              -100, -500,
              -1_000, -5_000,
              -10_000, -50_000,
              -100_000, -500_000,
              -1_000_000, -5_000_000] ++
            [0, 0.1, 0.5,
              1, 5,
              10, 50,
              100, 500,
              1_000, 5_000,
              10_000, 50_000,
              100_000, 500_000,
              1_000_000, 5_000_000]

intPlotRanges :: [Integer]
intPlotRanges = [1, 5,
                10, 50,
                100, 500,
                1_000, 5_000,
                10_000, 50_000,
                100_000, 500_000,
                1_000_000, 5_000_000]

rotate :: [String] -> [String]
rotate [] = []
rotate xs
    | head xs == "" = []
    | otherwise = map last xs : rotate (map init xs)


countOccurrences :: Ord a => V.Vector a -> [(a, Integer)]
countOccurrences xs = M.toList $ VG.foldr count initMap xs
    where initMap = M.fromList (map (, 0) (V.toList xs))
          count k = M.insertWith (+) k 1

data HistogramConfig = HistogramConfig {
    width :: Int,          -- Width of the histogram in characters
    height :: Int,         -- Height of the histogram in rows
    barChar :: Char,       -- Character to use for bars
    title :: Maybe String  -- Optional title for the histogram
}

defaultConfig :: HistogramConfig
defaultConfig = HistogramConfig {
    width = 40,
    height = 15,
    barChar = '█',
    title = Nothing
}

-- Calculate the histogram bins and counts
calculateBins :: [Double] -> Int -> [(Double, Int)]
calculateBins values numBins =
    let minVal = minimum values
        maxVal = maximum values
        binWidth = (maxVal - minVal) / fromIntegral numBins
        toBin x = floor ((x - minVal) / binWidth)
        bins = map toBin values
        counts = map length . L.group . L.sort $ bins
        binValues = [minVal + (fromIntegral i * binWidth) | i <- [0..numBins-1]]
    in zip binValues (counts ++ repeat 0)

-- Format a number with appropriate scaling (k, M, B, etc.)
formatNumber :: Double -> String
formatNumber n
    | n >= 1e9  = printf "%.1fB" (n / 1e9)
    | n >= 1e6  = printf "%.1fM" (n / 1e6)
    | n >= 1e3  = printf "%.1fk" (n / 1e3)
    | otherwise = printf "%.1f" n

-- Create the ASCII histogram
createHistogram :: HistogramConfig -> [Double] -> String
createHistogram config values =
    let bins = calculateBins values (width config)
        maxCount = maximum $ map snd bins
        scaleY = fromIntegral maxCount / fromIntegral (height config)

        -- Create Y-axis labels
        yLabels = [formatNumber (fromIntegral i * scaleY) | i <- [height config, height config-1..0]]
        maxYLabelWidth = maximum $ map length yLabels

        -- Create X-axis labels
        xValues = map fst bins
        xLabels = map formatNumber [head xValues, last xValues]

        -- Create histogram rows
        makeRow :: Int -> String
        makeRow row =
            let threshold = fromIntegral (height config - row) * scaleY
                barLine = map (\(_, count) ->
                    if fromIntegral count >= threshold
                    then barChar config
                    else ' ') bins
            in printf "%*s |%s" maxYLabelWidth (yLabels !! row) (brightBlue $ L.foldl' (\acc c -> c:'|':acc) "" barLine)

        -- Build the complete histogram
        histogramRows = map makeRow [0..height config - 1]
        xAxis = replicate maxYLabelWidth ' ' ++ " " ++
                L.intercalate (replicate (2 * (width config - length xLabels)) ' ') xLabels

        -- Add title if provided
        titleLine = case title config of
            Just t  -> t ++ "\n\n"
            Nothing -> ""

    in titleLine ++ unlines (histogramRows ++ [xAxis])
