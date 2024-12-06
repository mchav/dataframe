{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE CPP #-}
module Data.DataFrame.Display.Terminal where

import qualified Data.ByteString.Char8 as Str
import qualified Data.DataFrame.Internal as DI
import qualified Data.DataFrame.Operations as Ops
import qualified Data.Map as M
import qualified Data.Vector as V

import Control.Monad ( forM_ )
import Data.Bifunctor ( first )
import Data.Char ( ord, chr )
import Data.DataFrame.Util
import Data.Typeable (Typeable)
import Data.Type.Equality
    ( type (:~:)(Refl), TestEquality(testEquality) )
import qualified Type.Reflection
import GHC.Stack (HasCallStack)

plotHistograms :: HasCallStack => DI.DataFrame -> IO ()
plotHistograms df = do
    forM_ (Ops.columnNames df) $ \cname -> do
        plotForColumn cname ((M.!) (DI.columns df) cname) df


-- Plot code adapted from: https://alexwlchan.net/2018/ascii-bar-charts/
plotForColumn :: HasCallStack => Str.ByteString -> DI.Column -> DI.DataFrame -> IO ()
plotForColumn cname (DI.MkColumn (column :: V.Vector a)) df = do
    let repa :: Type.Reflection.TypeRep a = Type.Reflection.typeRep @a
        repByteString :: Type.Reflection.TypeRep Str.ByteString = Type.Reflection.typeRep @Str.ByteString
        repString :: Type.Reflection.TypeRep String = Type.Reflection.typeRep @String
    let counts = case repa `testEquality` repByteString of
            Just Refl -> map (first show) $ Ops.valueCounts @Str.ByteString cname df
            Nothing -> case repa `testEquality` repString of
                Nothing -> []
                Just Refl -> map (first show) $ Ops.valueCounts @String cname df
    if null counts then pure () else plotGivenCounts cname counts

plotGivenCounts :: Str.ByteString -> [(String, Integer)] -> IO ()
plotGivenCounts cname counts = do
    putStrLn $ "\nHistogram for " ++ show cname ++ "\n"
    let n = 8 :: Int
    let maxValue = maximum $ map snd counts
    let increment = max 1 (maxValue `div` 50)
    let longestLabelLength = maximum $ map (length . fst) counts
    let longestBar = fromIntegral $ (maxValue * fromIntegral n `div` increment) `div` fromIntegral n + 1
    putStrLn $ "|" ++ replicate (longestLabelLength + length (show maxValue) + longestBar + 6) '-' ++ "|"
    forM_ counts $ \(label, count) -> do
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
        putStrLn $ "|" ++ brightGreen (rightJustify label longestLabelLength) ++ " | " ++
                    rightJustify (show count) (length (show maxValue)) ++ " |" ++
                    " " ++ brightBlue bar
        putStrLn $ "|" ++ replicate (longestLabelLength + length (show maxValue) + longestBar + 6) '-' ++ "|"
    putChar '\n'

rightJustify :: String -> Int -> String
rightJustify s n = s ++ replicate (max 0 (n - length s)) ' '
