{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

-- | The two verbs every model speaks: fit and predict.
module DataFrame.Model (
    Fit (..),
    ToDataFrame (..),
    Fitted (..),
    FitResult,
    FrameFor,
    FrameKind (..),
    CheckFrame,
    AllDouble,
    Predict (..),
    AsTExpr,
    ToTExpr (..),
) where

import Data.Kind (Constraint, Type)

import DataFrame.Internal.DataFrame (DataFrame)
import DataFrame.Internal.Expression (Expr (..))
import DataFrame.Typed.Freeze (ToDataFrame (..), thaw)
import DataFrame.Typed.Schema (AllDouble)
import DataFrame.Typed.Types (AsTExpr, TExpr (..), ToTExpr (..), TypedDataFrame)

import GHC.TypeLits (Symbol)

-- | A model trained on a typed frame, carrying the schema as a phantom.
newtype Fitted (cols :: [(Symbol, Type)]) model = Fitted {fittedModel :: model}

-- | The type fit returns: the bare model, or a schema-tagged Fitted.
type family FitResult (f :: Type) (model :: Type) :: Type where
    FitResult DataFrame model = model
    FitResult (TypedDataFrame cols) model = Fitted cols model

-- | The frame a training input is fit against, sharing its schema if typed.
type family FrameFor (input :: Type) :: Type where
    FrameFor (Expr a) = DataFrame
    FrameFor (TExpr cols a) = TypedDataFrame cols
    FrameFor [Expr Double] = DataFrame
    FrameFor [TExpr cols Double] = TypedDataFrame cols

-- | The schema requirement a model places on its training frame.
data FrameKind = AnyFrame | AllDoubleFrame

-- | Turn a model's FrameKind requirement into a constraint on the frame.
type family CheckFrame (req :: FrameKind) (f :: Type) :: Constraint where
    CheckFrame _ DataFrame = ()
    CheckFrame 'AnyFrame _ = ()
    CheckFrame 'AllDoubleFrame (TypedDataFrame cols) = AllDouble cols

-- | Train a model from a config, an input and a frame.
class Fit cfg input where
    -- | The model this config and input train.
    type ModelOf cfg input :: Type

    type FrameReq cfg input :: FrameKind
    type FrameReq cfg input = 'AnyFrame
    fit ::
        (CheckFrame (FrameReq cfg input) (FrameFor input)) =>
        cfg ->
        input ->
        FrameFor input ->
        FitResult (FrameFor input) (ModelOf cfg input)

-- | Lift a model fittable on an untyped target to a typed target.
instance (Fit cfg (Expr a)) => Fit cfg (TExpr cols a) where
    type ModelOf cfg (TExpr cols a) = ModelOf cfg (Expr a)
    type FrameReq cfg (TExpr cols a) = FrameReq cfg (Expr a)
    fit cfg (TExpr e) tdf = Fitted (fit cfg e (thaw tdf))

-- | The same lift for the unsupervised feature-list inputs.
instance (Fit cfg [Expr Double]) => Fit cfg [TExpr cols Double] where
    type ModelOf cfg [TExpr cols Double] = ModelOf cfg [Expr Double]
    type FrameReq cfg [TExpr cols Double] = FrameReq cfg [Expr Double]
    fit cfg feats tdf = Fitted (fit cfg (map unTExpr feats) (thaw tdf))

-- | Compile a fitted model's prediction to an expression over raw columns.
class Predict model where
    type Prediction model :: Type
    predict :: model -> Prediction model

instance (Predict model, ToTExpr cols (Prediction model)) => Predict (Fitted cols model) where
    type Prediction (Fitted cols model) = AsTExpr cols (Prediction model)
    predict (Fitted m) = toTExpr @cols (predict m)
