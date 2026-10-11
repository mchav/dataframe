{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Proximal-gradient (FISTA) solver for L1/L2-regularized linear models.
module DataFrame.LinearSolver (
    -- * Model
    LinearModel (..),

    -- * Configuration
    SolverConfig (..),
    defaultSolverConfig,

    -- * Solvers
    fitL1Logistic,
    fitProx,

    -- * Expr conversion
    modelToExpr,

    -- * Internals (exposed for testing)
    standardize,
    columnStats,
    softThreshold,
    sigmoid,
    dotProduct,
) where

import DataFrame.Expression.Operators ((.*.), (.+.), (.>.))
import qualified DataFrame.Functions as F
import DataFrame.Internal.Expression (Expr (..))
import DataFrame.LinearAlgebra (Matrix, gram, scaleV)
import DataFrame.LinearAlgebra.Eigen (powerIterTop)
import DataFrame.LinearSolver.Loss (
    SmoothLoss (..),
    logisticLoss,
    sigmoid,
 )

import Control.Monad.ST (ST, runST)
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM

-- | A fitted linear classifier; zero weights mark features dropped by L1.
data LinearModel = LinearModel
    { lmWeights :: !(VU.Vector Double)
    , lmIntercept :: !Double
    , lmFeatureNames :: !(V.Vector T.Text)
    }
    deriving (Eq, Show)

-- | Hyper-parameters for the FISTA solver.
data SolverConfig = SolverConfig
    { scL1Lambda :: !Double
    -- ^ Strength of the L1 penalty on weights (intercept is not regularized).
    , scL2Lambda :: !Double
    -- ^ Strength of the L2 penalty (elastic net); 0 reduces to pure L1.
    , scMaxIter :: !Int
    -- ^ Maximum number of FISTA iterations.
    , scTol :: !Double
    -- ^ Convergence tolerance on the weight delta (L-inf norm).
    , scSampleWeights :: !(Maybe (VU.Vector Double))
    -- ^ Optional per-row sample weights with mean 1 (Nothing is uniform).
    }
    deriving (Eq, Show)

defaultSolverConfig :: SolverConfig
defaultSolverConfig =
    SolverConfig
        { scL1Lambda = 0.005
        , scL2Lambda = 0.005
        , scMaxIter = 200
        , scTol = 1.0e-4
        , scSampleWeights = Nothing
        }

-- | Fit L1-regularized binary logistic regression by FISTA.
fitL1Logistic ::
    SolverConfig ->
    V.Vector (VU.Vector Double) ->
    VU.Vector Double ->
    V.Vector T.Text ->
    LinearModel
{-# INLINEABLE fitL1Logistic #-}
fitL1Logistic = runFista logisticLoss (specNormLipschitz logisticLoss)

-- | Fit any SmoothLoss with the elastic-net proximal-gradient engine.
fitProx ::
    SmoothLoss ->
    SolverConfig ->
    V.Vector (VU.Vector Double) ->
    VU.Vector Double ->
    V.Vector T.Text ->
    LinearModel
fitProx loss = runFista loss (specNormLipschitz loss)

-- | FISTA smooth-part Lipschitz bound from the standardized Gram spectral norm.
specNormLipschitz :: SmoothLoss -> Matrix -> Int -> Double
specNormLipschitz loss xKept _ =
    let n = V.length xKept
        gramN = V.map (scaleV (1 / fromIntegral n)) (gram xKept)
        (specNorm, _) = powerIterTop 50 gramN
     in slCurvBound loss * (specNorm + 1)

-- | Shared FISTA scaffolding: standardize, drop constants, loop, unscale.
runFista ::
    SmoothLoss ->
    (Matrix -> Int -> Double) ->
    SolverConfig ->
    V.Vector (VU.Vector Double) ->
    VU.Vector Double ->
    V.Vector T.Text ->
    LinearModel
runFista loss lipschitzOf cfg rows labels featureNames
    | n == 0 || d == 0 = zeroModel
    | otherwise =
        let (!means, !stds, !variances) = columnStats rows
            !keep = keptIndices variances
         in if VU.null keep
                then zeroModel
                else
                    let !meansKept = gatherBy keep means
                        !stdsKept = gatherBy keep stds
                        !xKept = V.map (standardizeRowKept keep means stds) rows
                        !lipschitz =
                            lipschitzOf xKept (VU.length keep) + scL2Lambda cfg
                        (!wStdKept, !bStd) =
                            fistaLoop
                                loss
                                (scL1Lambda cfg)
                                (scL2Lambda cfg)
                                lipschitz
                                (scMaxIter cfg)
                                (scTol cfg)
                                (scSampleWeights cfg)
                                xKept
                                labels
                                (VU.replicate (VU.length keep) 0)
                                0
                        !wRawKept = VU.zipWith (/) wStdKept stdsKept
                        !bRaw = bStd - VU.sum (VU.zipWith (*) wRawKept meansKept)
                     in LinearModel (expandWeights d keep wRawKept) bRaw featureNames
  where
    !n = V.length rows
    !d = V.length featureNames
    zeroModel = LinearModel (VU.replicate d 0) 0 featureNames

-- | Indices of columns whose variance clears the near-constant threshold.
keptIndices :: VU.Vector Double -> VU.Vector Int
keptIndices variances =
    VU.fromList
        [ j
        | j <- [0 .. VU.length variances - 1]
        , VU.unsafeIndex variances j >= 1.0e-12
        ]

-- | Gather the entries of a vector at the given indices, preserving order.
gatherBy :: VU.Vector Int -> VU.Vector Double -> VU.Vector Double
gatherBy idxs v = VU.map (VU.unsafeIndex v) idxs

-- | Standardize one row to the kept columns only.
standardizeRowKept ::
    VU.Vector Int ->
    VU.Vector Double ->
    VU.Vector Double ->
    VU.Vector Double ->
    VU.Vector Double
standardizeRowKept keep means stds row = VU.map standardizeAt keep
  where
    standardizeAt j =
        (VU.unsafeIndex row j - VU.unsafeIndex means j) / VU.unsafeIndex stds j

-- | Scatter kept-column weights into a full-width vector, zero elsewhere.
expandWeights :: Int -> VU.Vector Int -> VU.Vector Double -> VU.Vector Double
expandWeights d keep wKept = VU.create $ do
    mv <- VUM.replicate d 0
    VU.iforM_ keep $ \k j -> VUM.unsafeWrite mv j (VU.unsafeIndex wKept k)
    pure mv

-- | Convert a fitted model to a Bool expression, dropping zero-weight features.
modelToExpr :: LinearModel -> Expr Bool
modelToExpr m =
    case nonZero of
        [] -> F.lit (b > 0)
        (w0, n0) : rest -> score rest (term w0 n0) .>. F.lit (0 :: Double)
  where
    b = lmIntercept m
    nonZero =
        [ (w, n)
        | (w, n) <- zip (VU.toList (lmWeights m)) (V.toList (lmFeatureNames m))
        , w /= 0
        ]
    term w n = F.lit w .*. (Col n :: Expr Double)
    score rest first = foldl (\acc (w, n) -> acc .+. term w n) first rest .+. F.lit b

-- | Per-column means, stds and variances of a feature matrix.
columnStats ::
    V.Vector (VU.Vector Double) ->
    (VU.Vector Double, VU.Vector Double, VU.Vector Double)
columnStats x
    | V.null x = (VU.empty, VU.empty, VU.empty)
    | otherwise =
        let !d = VU.length (V.unsafeHead x)
            !invN = 1 / fromIntegral (V.length x)
            !means = columnMeans d invN x
            !variances = columnVariances d invN means x
            !stds = VU.map (\v -> if v < 1e-12 then 1 else sqrt v) variances
         in (means, stds, variances)

-- | Mean of each of the @d@ columns; @invN@ is @1 / nRows@.
columnMeans :: Int -> Double -> V.Vector (VU.Vector Double) -> VU.Vector Double
columnMeans d invN x = runST $ do
    acc <- VUM.replicate d 0
    V.forM_ x $ \row ->
        VU.iforM_ row $ \j v -> VUM.unsafeModify acc (+ v) j
    scaleInPlace invN acc
    VU.unsafeFreeze acc

-- | Variance of each of the @d@ columns about the supplied @means@.
columnVariances ::
    Int ->
    Double ->
    VU.Vector Double ->
    V.Vector (VU.Vector Double) ->
    VU.Vector Double
columnVariances d invN means x = runST $ do
    acc <- VUM.replicate d 0
    V.forM_ x $ \row ->
        VU.iforM_ row $ \j v ->
            let !c = v - VU.unsafeIndex means j in VUM.unsafeModify acc (+ c * c) j
    scaleInPlace invN acc
    VU.unsafeFreeze acc

-- | Multiply every element of a mutable vector by @factor@ in place.
scaleInPlace :: Double -> VUM.MVector s Double -> ST s ()
scaleInPlace factor mv = go 0
  where
    go !j
        | j >= VUM.length mv = pure ()
        | otherwise = VUM.unsafeModify mv (* factor) j >> go (j + 1)

-- | Standardize columns to zero mean and unit variance, with their statistics.
standardize ::
    V.Vector (VU.Vector Double) ->
    ( V.Vector (VU.Vector Double)
    , VU.Vector Double
    , VU.Vector Double
    , VU.Vector Double
    )
standardize x
    | V.null x = (x, VU.empty, VU.empty, VU.empty)
    | otherwise =
        let (!means, !stds, !variances) = columnStats x
            !d = VU.length (V.unsafeHead x)
            standardizeRow row =
                VU.generate d $ \j ->
                    (VU.unsafeIndex row j - VU.unsafeIndex means j) / VU.unsafeIndex stds j
         in (V.map standardizeRow x, means, stds, variances)

-- | Proximal operator for the L1 norm: shrink toward zero, clamping at zero.
softThreshold :: Double -> Double -> Double
softThreshold lambda v
    | v > lambda = v - lambda
    | v < -lambda = v + lambda
    | otherwise = 0

-- | Dot product of two unboxed vectors; lengths are not checked.
dotProduct :: VU.Vector Double -> VU.Vector Double -> Double
dotProduct u v = VU.sum (VU.zipWith (*) u v)

-- | Gradient of the average loss, optionally scaled by per-row sample weights.
lossGradient ::
    SmoothLoss ->
    Maybe (VU.Vector Double) ->
    V.Vector (VU.Vector Double) ->
    VU.Vector Double ->
    VU.Vector Double ->
    Double ->
    (VU.Vector Double, Double)
lossGradient loss sampleWeights features labels w b = (gradW, gradB)
  where
    !invN = 1 / fromIntegral (V.length features)
    !coeffs = rowCoeffs loss sampleWeights features labels w b invN
    !gradW = accumulateGradW (VU.length w) features coeffs
    !gradB = VU.sum coeffs

-- | Per-row loss coefficient at the current margin, optionally weighted.
rowCoeffs ::
    SmoothLoss ->
    Maybe (VU.Vector Double) ->
    V.Vector (VU.Vector Double) ->
    VU.Vector Double ->
    VU.Vector Double ->
    Double ->
    Double ->
    VU.Vector Double
rowCoeffs loss sampleWeights features labels w b invN =
    VU.generate (V.length features) $ \i ->
        let !yi = VU.unsafeIndex labels i
            !row = V.unsafeIndex features i
            !z = dotProduct w row + b
            !base = slGradZ loss yi z * invN
         in case sampleWeights of
                Nothing -> base
                Just ws -> base * VU.unsafeIndex ws i

-- | Accumulate the weight gradient in one pass over every row and feature.
accumulateGradW ::
    Int -> V.Vector (VU.Vector Double) -> VU.Vector Double -> VU.Vector Double
accumulateGradW d features coeffs = runST $ do
    mv <- VUM.replicate d 0
    V.iforM_ features $ \i row ->
        let !c = VU.unsafeIndex coeffs i
         in VU.iforM_ row $ \j v -> VUM.unsafeModify mv (+ c * v) j
    VU.unsafeFreeze mv

-- | Inner FISTA loop over standardized features, returning the final weights.
fistaLoop ::
    SmoothLoss ->
    Double ->
    Double ->
    Double ->
    Int ->
    Double ->
    Maybe (VU.Vector Double) ->
    V.Vector (VU.Vector Double) ->
    VU.Vector Double ->
    VU.Vector Double ->
    Double ->
    (VU.Vector Double, Double)
fistaLoop loss lambda1 lambda2 lp maxIter tol sampleWeights features labels w0 b0 =
    go 0 w0 b0 w0 b0 1.0
  where
    !shrink = lambda1 / lp
    !ridgeDenom = 1 + lambda2 / lp
    !stepInv = 1 / lp
    proxStep = fistaProxStep loss sampleWeights features labels shrink ridgeDenom stepInv
    go !iter !xWPrev !xBPrev !yW !yB !t
        | iter >= maxIter = (xWPrev, xBPrev)
        | iter > 0 && delta < tol = (xW, xB)
        | otherwise = go (iter + 1) xW xB yWNew yBNew tNew
      where
        (!xW, !xB) = proxStep yW yB
        !delta = if VU.null xW then 0 else deltaInf xWPrev xW
        (!yWNew, !yBNew, !tNew) = fistaMomentum t xWPrev xBPrev xW xB

-- | One fused FISTA prox step: gradient step plus the elastic-net prox.
fistaProxStep ::
    SmoothLoss ->
    Maybe (VU.Vector Double) ->
    V.Vector (VU.Vector Double) ->
    VU.Vector Double ->
    Double ->
    Double ->
    Double ->
    VU.Vector Double ->
    Double ->
    (VU.Vector Double, Double)
fistaProxStep loss sampleWeights features labels shrink ridgeDenom stepInv yW yB =
    let (gW, gB) = lossGradient loss sampleWeights features labels yW yB
        !wNew =
            VU.zipWith
                (\yi gi -> softThreshold shrink (yi - gi * stepInv) / ridgeDenom)
                yW
                gW
        !bNew = yB - gB * stepInv
     in (wNew, bNew)

-- | Nesterov momentum extrapolation: new look-ahead point and step size.
fistaMomentum ::
    Double ->
    VU.Vector Double ->
    Double ->
    VU.Vector Double ->
    Double ->
    (VU.Vector Double, Double, Double)
fistaMomentum t xWPrev xBPrev xW xB =
    let !tNew = (1 + sqrt (1 + 4 * t * t)) / 2
        !mom = (t - 1) / tNew
        !yW = VU.zipWith (\new old -> new + mom * (new - old)) xW xWPrev
        !yB = xB + mom * (xB - xBPrev)
     in (yW, yB, tNew)

-- | L-inf norm of the weight delta.
{-# INLINE deltaInf #-}
deltaInf :: VU.Vector Double -> VU.Vector Double -> Double
deltaInf xWPrev = VU.ifoldl' (\acc i x -> max acc (abs (x - VU.unsafeIndex xWPrev i))) 0
