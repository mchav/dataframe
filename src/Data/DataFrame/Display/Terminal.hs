{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE NumericUnderscores #-}
module Data.DataFrame.Display.Terminal where

import qualified Data.DataFrame.Internal as DI
import qualified Data.DataFrame.Operations as Ops
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import Control.Monad ( forM_, forM )
import Data.Bifunctor ( first )
import Data.Char ( ord, chr )
import Data.List (intercalate)
import Data.DataFrame.Util
import Data.Typeable (Typeable)
import Data.Type.Equality
    ( type (:~:)(Refl), TestEquality(testEquality) )
import GHC.Stack (HasCallStack)
import Text.Printf
import qualified Type.Reflection
import Type.Reflection (typeRep)

data HistogramOrientation = VerticalHistogram | HorizontalHistogram

plotHistograms :: HasCallStack => HistogramOrientation -> DI.DataFrame -> IO ()
plotHistograms orientation df = do
    forM_ (Ops.columnNames df) $ \cname -> do
        plotForColumn cname ((M.!) (DI.columns df) cname) orientation df


-- Plot code adapted from: https://alexwlchan.net/2018/ascii-bar-charts/
plotForColumn :: HasCallStack => Str.ByteString -> DI.Column -> HistogramOrientation -> DI.DataFrame -> IO ()
plotForColumn cname (DI.BoxedColumn (column :: V.Vector a)) orientation df = do
    let repa :: Type.Reflection.TypeRep a = Type.Reflection.typeRep @a
        repText :: Type.Reflection.TypeRep T.Text = Type.Reflection.typeRep @T.Text
        repString :: Type.Reflection.TypeRep String = Type.Reflection.typeRep @String
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
plotForColumn cname (DI.UnboxedColumn (column :: VU.Vector a)) orientation df = do
    let repa :: Type.Reflection.TypeRep a = Type.Reflection.typeRep @a
        repByteString :: Type.Reflection.TypeRep Str.ByteString = Type.Reflection.typeRep @Str.ByteString
        repString :: Type.Reflection.TypeRep String = Type.Reflection.typeRep @String
    let counts = case repa `testEquality` repByteString of
            Just Refl -> map (first show) $ Ops.valueCounts @Str.ByteString cname df
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
