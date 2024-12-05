{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE GADTs #-}
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

plotHistograms :: DI.DataFrame -> IO ()
plotHistograms df = do
    forM_ (Ops.columnNames df) $ \cname -> do
        plotForColumn cname ((M.!) (DI.columns df) cname) df


-- Plot code adapted from: https://alexwlchan.net/2018/ascii-bar-charts/
plotForColumn :: Str.ByteString -> DI.Column -> DI.DataFrame -> IO ()
plotForColumn cname (DI.MkColumn (column :: V.Vector a)) df = do
    let repa :: Type.Reflection.TypeRep a = Type.Reflection.typeRep @a
        repByteString :: Type.Reflection.TypeRep Str.ByteString = Type.Reflection.typeRep @Str.ByteString
        repString :: Type.Reflection.TypeRep String = Type.Reflection.typeRep @String
    case repa `testEquality` repByteString of
        Nothing -> pure ()
        Just Refl -> do
            putStrLn $ "\nHistogram for " ++ show cname ++ "\n"
            let n = 8 :: Int
            let counts = map (first show) $ Ops.valueCounts @a cname df
            let maxValue = maximum $ map snd counts
            let increment = maxValue `div` 50
            let longestLabelLength = maximum $ map (length . fst) counts
            forM_ counts $ \(label, count) -> do
                let barChunks = fromIntegral $ (count * fromIntegral n `div` increment) `div` fromIntegral n
                let remainder = fromIntegral $ (count * fromIntegral n `div` increment) `rem` fromIntegral n

                let fractional = ([chr (ord '█' + n - remainder) | remainder > 0])

                let bar = replicate barChunks '█' ++ fractional

                let disp = if null bar then "| " else bar
                putStrLn $ brightGreen (rightJustify label longestLabelLength) ++ " | " ++
                            rightJustify (show count) (length (show maxValue)) ++
                            " " ++ brightBlue bar
            putStrLn $ replicate 50 '-'

rightJustify :: String -> Int -> String
rightJustify s n = replicate (max 0 (n - length s)) ' ' ++ s