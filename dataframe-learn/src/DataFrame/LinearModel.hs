-- | Linear regression (OLS, ridge, lasso, elastic net) and logistic models.
module DataFrame.LinearModel (
    module DataFrame.LinearModel.Regression,
    module DataFrame.LinearModel.Logistic,
    -- Re-exported so the configs' @SolverConfig@\/@LinearModel@ fields work.
    SolverConfig (..),
    defaultSolverConfig,
    LinearModel (..),
) where

import DataFrame.LinearModel.Logistic
import DataFrame.LinearModel.Regression
import DataFrame.LinearSolver (
    LinearModel (..),
    SolverConfig (..),
    defaultSolverConfig,
 )
