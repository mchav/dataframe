{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

-- | Logistic regression: binary and one-vs-rest multiclass via FISTA.
module DataFrame.LinearModel.Logistic (
    module DataFrame.Model,
    LogisticConfig (..),
    defaultLogisticConfig,
    LogisticModel (..),
    logisticMarginExprs,
    logisticProbExprs,
) where

import Control.Parallel.Strategies (Strategy, parList, rseq, using)
import Data.List (sort)
import qualified Data.Map.Strict as M
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import DataFrame.Featurize.Internal (
    affineExpr,
    argMaxExpr,
    featureNames,
    numericMatrix,
    targetValues,
 )
import qualified DataFrame.Functions as F
import DataFrame.Internal.Column (Columnable)
import DataFrame.Internal.DataFrame (DataFrame)
import DataFrame.Internal.Expression (Expr)
import DataFrame.LinearSolver (
    LinearModel (..),
    SolverConfig,
    defaultSolverConfig,
    fitL1Logistic,
 )
import DataFrame.Model

-- | Hyperparameters for logistic regression (the FISTA solver config).
newtype LogisticConfig = LogisticConfig {lgSolver :: SolverConfig}
    deriving (Eq, Show)

defaultLogisticConfig :: LogisticConfig
defaultLogisticConfig = LogisticConfig defaultSolverConfig

-- | A fitted one-vs-rest logistic model of class labels and sub-models.
data LogisticModel a = LogisticModel
    { lgClasses :: !(V.Vector a)
    , lgModels :: !(V.Vector LinearModel)
    }
    deriving (Eq, Show)

instance (Columnable a, Ord a) => Fit LogisticConfig (Expr a) where
    type ModelOf LogisticConfig (Expr a) = (LogisticModel a)
    fit = fitLogistic

instance (Columnable a, Ord a) => Predict (LogisticModel a) where
    type Prediction (LogisticModel a) = Expr a
    predict m = argMaxExpr (labelledMargins m)

-- | Fit one-vs-rest logistic regression; the target supplies the classes.
fitLogistic ::
    (Columnable a, Ord a) =>
    LogisticConfig -> Expr a -> DataFrame -> LogisticModel a
fitLogistic (LogisticConfig cfg) target df =
    LogisticModel (V.fromList classes) (V.fromList models)
  where
    names = featureNames target df
    (nameVec, mat) = numericMatrix names df
    ys = targetValues target df
    classes = sort (foldr dedup [] (V.toList ys))
    dedup x acc = if x `elem` acc then acc else x : acc
    fitOne c =
        let labels =
                VU.generate (V.length ys) (\i -> if ys V.! i == c then 1 else -1)
         in fitL1Logistic cfg mat labels nameVec
    -- Binary fits two sign-mirrored models; solve once and negate.
    models = case classes of
        [c0, _c1] -> let m0 = fitOne c0 in [m0, negateModel m0]
        _ -> map fitOne classes `using` parList forceModel

-- | Sign-flip a fitted binary model.
negateModel :: LinearModel -> LinearModel
negateModel m =
    m{lmWeights = VU.map negate (lmWeights m), lmIntercept = negate (lmIntercept m)}

-- | Force a fitted model's weight vector so per-class fits run in parallel.
forceModel :: Strategy LinearModel
forceModel m = lmWeights m `seq` rseq m

-- | The raw margin @Expr@ for each class.
logisticMarginExprs ::
    (Columnable a, Ord a) => LogisticModel a -> M.Map a (Expr Double)
logisticMarginExprs m = M.fromList (labelledMargins m)

-- | Per-class probability expressions: @1 / (1 + exp(-margin))@.
logisticProbExprs ::
    (Columnable a, Ord a) => LogisticModel a -> M.Map a (Expr Double)
logisticProbExprs = M.map sigmoidExpr . logisticMarginExprs

labelledMargins :: LogisticModel a -> [(a, Expr Double)]
labelledMargins m =
    [ (lgClasses m V.! i, marginOf (lgModels m V.! i))
    | i <- [0 .. V.length (lgClasses m) - 1]
    ]

marginOf :: LinearModel -> Expr Double
marginOf m =
    affineExpr
        (lmIntercept m)
        (zip (VU.toList (lmWeights m)) (V.toList (lmFeatureNames m)))

sigmoidExpr :: Expr Double -> Expr Double
sigmoidExpr z = F.lit 1 / (F.lit 1 + exp (negate z))
