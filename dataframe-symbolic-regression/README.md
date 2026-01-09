# DataFrame.SymbolicRegression

A Haskell library (based on [eggp](https://github.com/folivetti/eggp) which is in turn based on [srtree](https://github.com/folivetti/srtree)) for symbolic regression on DataFrames. Automatically discover mathematical expressions that best fit your data using genetic programming with e-graph optimization.

## Overview

DataFrame.SymbolicRegression integrates symbolic regression capabilities into a DataFrame workflow. Given a target column and a dataset, it evolves mathematical expressions that predict the target variable, returning a Pareto front of expressions trading off complexity and accuracy.

## Quick Start

```haskell
import qualified DataFrame as D
import DataFrame.Functions ((.=))
import DataFrame.SymbolicRegression

-- Load your data
df <- D.readParquet "data/mtcars.parquet"

-- Run symbolic regression to predict 'mpg'
-- NOTE: ALL COLUMNS MUST BE CONVERTED TO DOUBLE FIRST
-- e.g df' = D.derive "some_column" (F.toDouble (F.col @Int "some_column")) df
exprs <- fitSymbolicRegression defaultRegressionConfig mpg df

-- View discovered expressions (Pareto front from simplest to most complex)
exprs
-- [(col @Double "qsec"),
--  (divide (lit 57.33) (col @Double "wt")),
--  (add (lit 10.75) (divide (lit 1557.67) (col @Double "disp")))]

-- Create named expressions that we'll use in a dataframe.
levels = zipWith (.=) ["level_1", "level_2", "level_3"] exprs

-- Show the various predictions in our dataframe.
D.deriveMany levels df

-- Or pick the best one
D.derive "prediction" (last exprs) df
```

## Configuration

Customize the search with `RegressionConfig`:

```haskell
data RegressionConfig = RegressionConfig
    { generations              :: Int      -- Number of evolutionary generations (default: 100)
    , maxExpressionSize        :: Int      -- Maximum tree depth/complexity (default: 5)
    , numFolds                 :: Int      -- Cross-validation folds (default: 3)
    , showTrace                :: Bool     -- Print progress during evolution (default: True)
    , lossFunction             :: Distribution  -- MSE, Gaussian, Poisson, etc. (default: MSE)
    , numOptimisationIterations :: Int     -- Parameter optimization iterations (default: 30)
    , numParameterRetries      :: Int      -- Retries for parameter fitting (default: 2)
    , populationSize           :: Int      -- Population size (default: 100)
    , tournamentSize           :: Int      -- Tournament selection size (default: 3)
    , crossoverProbability     :: Double   -- Crossover rate (default: 0.95)
    , mutationProbability      :: Double   -- Mutation rate (default: 0.3)
    , unaryFunctions           :: [...]    -- Unary operations to include
    , binaryFunctions          :: [...]    -- Binary operations to include
    , numParams                :: Int      -- Number of parameters (-1 for auto)
    , generational             :: Bool     -- Use generational replacement (default: False)
    , simplifyExpressions      :: Bool     -- Simplify output expressions (default: True)
    , maxTime                  :: Int      -- Time limit in seconds (-1 for none)
    , dumpTo                   :: String   -- Save e-graph state to file
    , loadFrom                 :: String   -- Load e-graph state from file
    }
```

### Example: Custom Configuration

```haskell
myConfig :: RegressionConfig
myConfig = defaultRegressionConfig
    { generations = 200
    , maxExpressionSize = 7
    , populationSize = 200
    }

exprs <- fitSymbolicRegression myConfig targetColumn df
```

## Output

`fitSymbolicRegression` returns a list of expressions representing the Pareto front, ordered by complexity (simplest first). Each expression:

- Is a valid `Expr Double` that can be used with DataFrame operations
- Represents a different trade-off between simplicity and accuracy
- Has optimized numerical constants

## How It Works

1. **Genetic Programming**: Evolves a population of expression trees through selection, crossover, and mutation
2. **E-graph Optimization**: Uses equality saturation to discover equivalent expressions and simplify
3. **Parameter Optimization**: Fits numerical constants using nonlinear optimization
4. **Pareto Selection**: Returns expressions across the complexity-accuracy frontier

## Dependencies

### System dependencies
To install DataFrame.SymbolicRegression you'll need:
* libz: `sudo apt install libz-dev`
* libnlopt: `sudo apt install libnlopt-dev`
* libgmp: `sudo apt install libgmp-dev`
