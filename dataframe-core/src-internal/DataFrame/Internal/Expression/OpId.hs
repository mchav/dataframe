{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE OverloadedStrings #-}

-- | What an operator computes; kernels key on these tags, not display names.
module DataFrame.Internal.Expression.OpId (
    OpRef (..),
    UnOpId (..),
    BinOpId (..),
    AggId (..),
    CastId (..),
    sameOp,
    builtinOf,
    opRefName,
    unOpName,
    binOpName,
    aggName,
    castName,
    castExprName,
) where

import qualified Data.Text as T

-- | Builtin: library op; Named: equal names, equal functions; Anonymous: label.
data OpRef k
    = Builtin !k
    | Named !T.Text
    | Anonymous !T.Text
    deriving (Show, Functor)

-- | Provable equality: 'Anonymous' is never equal to anything, even itself.
sameOp :: (Eq k) => OpRef k -> OpRef k -> Bool
sameOp (Builtin a) (Builtin b) = a == b
sameOp (Named a) (Named b) = a == b
sameOp _ _ = False
{-# INLINE sameOp #-}

-- | The builtin tag, if any; the only thing kernels may dispatch on.
builtinOf :: OpRef k -> Maybe k
builtinOf (Builtin k) = Just k
builtinOf _ = Nothing
{-# INLINE builtinOf #-}

-- | The display name: the builtin's name, the given name, or the label.
opRefName :: (k -> T.Text) -> OpRef k -> T.Text
opRefName f (Builtin k) = f k
opRefName _ (Named n) = n
opRefName _ (Anonymous l) = l

-- | Library unary operators. Parameters are part of the identity.
data UnOpId
    = UNegate
    | UAbs
    | USignum
    | UExp
    | USqrt
    | ULog
    | USin
    | UCos
    | UTan
    | UAsin
    | UAcos
    | UAtan
    | USinh
    | UCosh
    | UAsinh
    | UAcosh
    | UAtanh
    | UNot
    | UToDouble
    | URelu
    | UToMaybe
    | -- | The default, rendered with 'show'.
      UFromMaybe !T.Text
    | UIsJust
    | UIsNothing
    | UFromJust
    | UFirstOrNothing
    | ULastOrNothing
    | UTextLength
    | USplitOn !T.Text
    | UMatch !T.Text
    | UMatchAll !T.Text
    | UParseDate !T.Text
    | -- | The mapping, rendered with 'show'.
      URecode !T.Text
    | -- | The default and the mapping, rendered with 'show'.
      URecodeWithDefault !T.Text !T.Text
    deriving (Eq, Ord, Show)

-- | Library binary operators.
data BinOpId
    = BAdd
    | BSub
    | BMult
    | BDivide
    | BExponentiate
    | BLogBase
    | BDiv
    | BMod
    | -- | @(^)@: integral exponent.
      BPow
    | -- | @(^^)@: fractional base, integral exponent.
      BPowFrac
    | BEq
    | BNeq
    | BLt
    | BLeq
    | BGt
    | BGeq
    | BAnd
    | BOr
    | BNullAnd
    | BNullOr
    | BNullAdd
    | BNullSub
    | BNullMul
    | BNullDiv
    | BMin
    | BMax
    | BContains
    | BDaysBetween
    deriving (Eq, Ord, Show, Enum, Bounded)

-- | Library aggregations.
data AggId
    = ASum
    | AMinimum
    | AMaximum
    | AMean
    | ACount
    | ACountDistinct
    | AMode
    | ACollect
    | ASumMaybe
    | AMeanMaybe
    | AMedianMaybe
    | AVariance
    | AMedian
    | AStddev
    | AStddevMaybe
    | APercentile !Int
    | ATopK !Int
    | ABottomK !Int
    deriving (Eq, Ord, Show)

-- | Display and wire name of a unary builtin.
unOpName :: UnOpId -> T.Text
unOpName op = case op of
    UNegate -> "negate"
    UAbs -> "abs"
    USignum -> "signum"
    UExp -> "exp"
    USqrt -> "sqrt"
    ULog -> "log"
    USin -> "sin"
    UCos -> "cos"
    UTan -> "tan"
    UAsin -> "asin"
    UAcos -> "acos"
    UAtan -> "atan"
    USinh -> "sinh"
    UCosh -> "cosh"
    UAsinh -> "asinh"
    UAcosh -> "acosh"
    UAtanh -> "atanh"
    UNot -> "not"
    UToDouble -> "toDouble"
    URelu -> "relu"
    UToMaybe -> "toMaybe"
    UFromMaybe _ -> "fromMaybe"
    UIsJust -> "isJust"
    UIsNothing -> "isNothing"
    UFromJust -> "fromJust"
    UFirstOrNothing -> "firstOrNothing"
    ULastOrNothing -> "lastOrNothing"
    UTextLength -> "textLength"
    USplitOn _ -> "splitOn"
    UMatch r -> "match " <> T.pack (show r)
    UMatchAll r -> "matchAll " <> T.pack (show r)
    UParseDate f -> "parseDate " <> f
    URecode m -> "recode " <> m
    URecodeWithDefault d m -> "recodeWithDefault " <> d <> " " <> m

-- | Display and wire name of a binary builtin.
binOpName :: BinOpId -> T.Text
binOpName op = case op of
    BAdd -> "add"
    BSub -> "sub"
    BMult -> "mult"
    BDivide -> "divide"
    BExponentiate -> "exponentiate"
    BLogBase -> "logBase"
    BDiv -> "div"
    BMod -> "mod"
    BPow -> "pow"
    BPowFrac -> "pow"
    BEq -> "eq"
    BNeq -> "neq"
    BLt -> "lt"
    BLeq -> "leq"
    BGt -> "gt"
    BGeq -> "geq"
    BAnd -> "and"
    BOr -> "or"
    BNullAnd -> "nulland"
    BNullOr -> "nullor"
    BNullAdd -> "nulladd"
    BNullSub -> "nullsub"
    BNullMul -> "nullmul"
    BNullDiv -> "nulldiv"
    BMin -> "min"
    BMax -> "max"
    BContains -> "contains"
    BDaysBetween -> "daysBetween"

-- | Display and wire name of an aggregation builtin.
aggName :: AggId -> T.Text
aggName op = case op of
    ASum -> "sum"
    AMinimum -> "minimum"
    AMaximum -> "maximum"
    AMean -> "mean"
    ACount -> "count"
    ACountDistinct -> "countDistinct"
    AMode -> "mode"
    ACollect -> "collect"
    ASumMaybe -> "sumMaybe"
    AMeanMaybe -> "meanMaybe"
    AMedianMaybe -> "medianMaybe"
    AVariance -> "variance"
    AMedian -> "median"
    AStddev -> "stddev"
    AStddevMaybe -> "stddevMaybe"
    APercentile n -> "percentile " <> T.pack (show n)
    ATopK k -> "topK_" <> T.pack (show k)
    ABottomK k -> "bottomK_" <> T.pack (show k)

-- | Library casts; a default value is shown so different defaults differ.
data CastId
    = CCast
    | CCastWithDefault !T.Text
    | CCastEither
    | CUnsafeCast
    deriving (Eq, Ord, Show)

-- | Display name of a cast reading a column by name.
castName :: CastId -> T.Text
castName c = case c of
    CCast -> "cast"
    CCastWithDefault d -> "castWithDefault:" <> d
    CCastEither -> "castEither"
    CUnsafeCast -> "unsafeCast"

-- | Display name of a cast applied to an expression.
castExprName :: CastId -> T.Text
castExprName c = case c of
    CCast -> "castExpr"
    CCastWithDefault d -> "castExprWithDefault:" <> d
    CCastEither -> "castExprEither"
    CUnsafeCast -> "unsafeCastExpr"
