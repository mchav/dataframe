-- | Persist and reload a fitted Transform.
module DataFrame.Transform.Serialize (
    encodeTransform,
    decodeTransform,
    saveTransformToFile,
    loadTransformFromFile,
) where

import qualified Data.Aeson as Aeson

import DataFrame.Expr.Serialize (
    decodeNamedExprs,
    encodeNamedExprs,
    loadPipelineFromFile,
    savePipelineToFile,
 )
import DataFrame.Transform (Transform (..))

-- | Encode a transform's output expressions to JSON.
encodeTransform :: Transform -> Either String Aeson.Value
encodeTransform = encodeNamedExprs . transformOutputs

-- | Decode a transform produced by 'encodeTransform'.
decodeTransform :: Aeson.Value -> Either String Transform
decodeTransform = fmap Transform . decodeNamedExprs

-- | Encode a transform and write it to a file. No file is written on failure.
saveTransformToFile :: FilePath -> Transform -> IO (Either String ())
saveTransformToFile fp = savePipelineToFile fp . transformOutputs

-- | Load a transform produced by 'saveTransformToFile'.
loadTransformFromFile :: FilePath -> IO (Either String Transform)
loadTransformFromFile fp = fmap (fmap Transform) (loadPipelineFromFile fp)
