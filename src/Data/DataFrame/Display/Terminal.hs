{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE TupleSections #-}
module Data.DataFrame.Display.Terminal where

import qualified Data.DataFrame.Internal as DI
import qualified Data.DataFrame.Operations as Ops
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
import Data.List (intercalate)
import Data.DataFrame.Colours
import Data.Typeable (Typeable)
import Data.Type.Equality
    ( type (:~:)(Refl), TestEquality(testEquality) )
import GHC.Stack (HasCallStack)
import Text.Printf
import Type.Reflection (typeRep)

data HistogramOrientation = VerticalHistogram | HorizontalHistogram

data PlotColumns = PlotAll | PlotSubset [T.Text]

plotHistograms :: HasCallStack => PlotColumns -> HistogramOrientation -> DI.DataFrame -> IO ()
plotHistograms columns orientation df = do
    let cs = case columns of
            PlotAll       -> Ops.columnNames df
            PlotSubset xs -> Ops.columnNames df `L.intersect` xs
    forM_ cs $ \cname -> do
        plotForColumn cname ((V.!) (DI.columns df) (DI.columnIndices df M.! cname)) orientation df


plotHistogramsBy :: HasCallStack => T.Text -> PlotColumns -> HistogramOrientation -> DI.DataFrame -> IO ()
plotHistogramsBy col columns orientation df = do
    let cs = case columns of
            PlotAll       -> Ops.columnNames df
            PlotSubset xs -> Ops.columnNames df `L.intersect` xs
    forM_ cs $ \cname -> do
        let plotColumn = (V.!) (DI.columns df) (DI.columnIndices df M.! cname)
        let byColumn = (V.!) (DI.columns df) (DI.columnIndices df M.! col)
        plotForColumnBy col cname byColumn plotColumn orientation df

-- Plot code adapted from: https://alexwlchan.net/2018/ascii-bar-charts/
plotForColumnBy :: HasCallStack => T.Text -> T.Text -> Maybe DI.Column -> Maybe DI.Column -> HistogramOrientation -> DI.DataFrame -> IO ()
plotForColumnBy _ _ Nothing _ _ _ = return ()
plotForColumnBy byCol cname (Just (DI.BoxedColumn (byColumn :: V.Vector a))) (Just (DI.BoxedColumn (plotColumn :: V.Vector b))) orientation df = do
    let zipped = VG.zipWith (\left right -> (show left, show right)) plotColumn byColumn
    let counts = countOccurrences zipped
    if null counts
    then pure ()
    else case orientation of
        VerticalHistogram -> error "Vertical histograms aren't yet supported"
        HorizontalHistogram -> plotGivenCounts' cname counts
plotForColumnBy byCol cname (Just (DI.UnboxedColumn byColumn)) (Just (DI.BoxedColumn plotColumn)) orientation df = do
    let zipped = VG.zipWith (\left right -> (show left, show right)) plotColumn (V.convert byColumn)
    let counts = countOccurrences zipped
    if null counts
    then pure ()
    else case orientation of
        VerticalHistogram -> error "Vertical histograms aren't yet supported"
        HorizontalHistogram -> plotGivenCounts' cname counts
plotForColumnBy byCol cname (Just (DI.BoxedColumn byColumn)) (Just (DI.UnboxedColumn plotColumn)) orientation df = do
    let zipped = VG.zipWith (\left right -> (show left, show right)) (V.convert plotColumn) (V.convert byColumn)
    let counts = countOccurrences zipped
    if null counts
    then pure ()
    else case orientation of
        -- VerticalHistogram -> plotVerticalGivenCounts cname counts
        HorizontalHistogram -> plotGivenCounts' cname counts
plotForColumnBy byCol cname (Just (DI.UnboxedColumn byColumn)) (Just (DI.UnboxedColumn plotColumn)) orientation df = do
    let zipped = VG.zipWith (\left right -> (show left, show right)) (V.convert plotColumn) (V.convert byColumn)
    let counts = countOccurrences zipped
    if null counts
    then pure ()
    else case orientation of
        VerticalHistogram -> error "Vertical histograms aren't yet supported"
        HorizontalHistogram -> plotGivenCounts' cname counts

-- Plot code adapted from: https://alexwlchan.net/2018/ascii-bar-charts/
plotForColumn :: HasCallStack => T.Text -> Maybe DI.Column -> HistogramOrientation -> DI.DataFrame -> IO ()
plotForColumn _ Nothing _ _ = return ()
plotForColumn cname (Just (DI.BoxedColumn (column :: V.Vector a))) orientation df = do
    let repa :: Ref.TypeRep a = Ref.typeRep @a
        repText :: Ref.TypeRep T.Text = Ref.typeRep @T.Text
        repString :: Ref.TypeRep String = Ref.typeRep @String
    let counts = case repa `testEquality` repText of
            Just Refl -> map (first show) $ Ops.valueCounts @T.Text cname df
            Nothing -> case repa `testEquality` repString of
                Just Refl -> Ops.valueCounts @String cname df
                -- Support other scalar types.
                Nothing -> rangedNumericValueCounts column
    if null counts
    then pure ()
    else case orientation of
        VerticalHistogram -> plotVerticalGivenCounts cname counts
        HorizontalHistogram -> plotGivenCounts cname counts
plotForColumn cname (Just (DI.UnboxedColumn (column :: VU.Vector a))) orientation df = do
    let repa :: Ref.TypeRep a = Ref.typeRep @a
        repText :: Ref.TypeRep T.Text = Ref.typeRep @T.Text
        repString :: Ref.TypeRep String = Ref.typeRep @String
    let counts = case repa `testEquality` repText of
            Just Refl -> map (first show) $ Ops.valueCounts @T.Text cname df
            Nothing -> case repa `testEquality` repString of
                Just Refl -> Ops.valueCounts @String cname df
                -- Support other scalar types.
                Nothing -> rangedNumericValueCounts (V.convert column)
    if null counts
    then pure ()
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
plotVerticalGivenCounts cname counts = do
    putStrLn $ "\nHistogram for " ++ show cname ++ "\n"
    let n = 8 :: Int
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

rangedNumericValueCounts :: forall a . (HasCallStack, Typeable a, Show a)
                         => V.Vector a
                         -> [(String, Integer)]
rangedNumericValueCounts xs =
    case testEquality (typeRep @a) (typeRep @Double) of
        Just Refl -> doubleToValueCounts xs
        Nothing -> []

doubleToValueCounts :: HasCallStack => V.Vector Double -> [(String, Integer)]
doubleToValueCounts xs = let
        minValue = V.minimum xs
        maxValue = V.maximum xs
        partitions = smallestPartition ((maxValue - minValue) / 20.0) plotRanges
        f n = (fromIntegral (round (n * 20) `div` round (partitions * 20)) * (partitions * 20)) / 20.0
        frequencies = V.foldl' (\m n -> M.insertWith (+) (f n) 1 m) M.empty xs
    in map (first (\n -> if n >= 1_000 then printf "%e" n else printf "%.2f" n)) $ M.toList frequencies

smallestPartition :: (Ord a) => a -> [a] -> a
-- TODO: Find a more graceful way to handle this.
smallestPartition p [] = error "Data range too large to plot"
smallestPartition p (x:y:rest)
    | p < y = x
    | otherwise = smallestPartition p (y:rest)

plotRanges :: [Double]
plotRanges = [0.1, 0.5,
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
