{-# LANGUAGE BangPatterns #-}

module DataFrame.Hasktorch (
    toTensor,
) where

import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import qualified DataFrame as D

import Control.Exception (throw)
import DataFrame (DataFrame)
import Torch

toTensor :: DataFrame -> Tensor
toTensor df = case D.toFloatMatrix df of
    Left e -> throw e
    Right m ->
        let
            (r, c) = D.dimensions df
            dims' = if c == 1 then [r] else [r, c]
         in
            reshape dims' (asTensor (flattenFeatures m))

toIntTensor :: DataFrame -> Tensor
toIntTensor df = case D.toIntMatrix df of
    Left e -> throw e
    Right m ->
        let
            (r, c) = D.dimensions df
            dims' = if c == 1 then [r] else [r, c]
         in
            reshape dims' (asTensor (flattenFeatures m))

flattenFeatures :: V.Vector (VU.Vector a) -> VU.Vector a
flattenFeatures rows =
    let
        total = V.foldl' (\s v -> s + VU.length v) 0 rows
     in
        VU.create $ do
            ret <- VUM.unsafeNew total
            let go !i !off
                    | i == V.length rows = pure ()
                    | otherwise = do
                        let v = rows V.! i
                            len = VU.length v
                        VU.unsafeCopy (VUM.unsafeSlice off len ret) v
                        go (i + 1) (off + len)
            go 0 0
            pure ret
