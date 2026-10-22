{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE MonoLocalBinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

-- | Typed counterparts of the remaining "DataFrame.Functions" combinators.
module DataFrame.Typed.Expr.Extra (
    cast,
    castWithDefault,
    castEither,
    unsafeCast,
    eq,
    lt,
    gt,
    leq,
    geq,
    and,
    or,
    countDistinct,
    topK,
    bottomK,
    isNull,
    isNotNull,
    contains,
    length,
    div,
    mod,
    mode,
    sumMaybe,
    meanMaybe,
    variance,
    medianMaybe,
    percentile,
    stddev,
    stddevMaybe,
    zScore,
    pow,
    relu,
    min,
    max,
    reduce,
    toMaybe,
    fromMaybe,
    isJust,
    isNothing,
    fromJust,
    whenPresent,
    whenBothPresent,
    recode,
    recodeWithCondition,
    recodeWithDefault,
    firstOrNothing,
    lastOrNothing,
    splitOn,
    match,
    matchAll,
    parseDate,
    daysBetween,
    bind,
) where

import Data.Proxy (Proxy (..))
import qualified Data.Text as T
import Data.Time (Day, ParseTime)
import qualified Data.Vector.Unboxed as VU
import GHC.TypeLits (KnownSymbol, Symbol, symbolVal)
import Prelude hiding (and, div, length, max, min, mod, or)

import qualified DataFrame.Functions as F
import DataFrame.Internal.Column (Columnable)
import DataFrame.Internal.Expression.Operators.Nullable (BaseType)
import DataFrame.Typed.Schema (AssertPresent)
import DataFrame.Typed.Types (TExpr (..))

colName :: forall (name :: Symbol). (KnownSymbol name) => T.Text
colName = T.pack (symbolVal (Proxy @name))

-- | Parse a schema-checked column as @a@; failures become 'Nothing'.
cast ::
    forall (name :: Symbol) a cols.
    (KnownSymbol name, AssertPresent name cols, Columnable a, Read a) =>
    TExpr cols (Maybe a)
cast = TExpr (F.cast @a (colName @name))

-- | Parse a schema-checked column as @a@, with a default for failures.
castWithDefault ::
    forall (name :: Symbol) a cols.
    (KnownSymbol name, AssertPresent name cols, Columnable a, Read a) =>
    a -> TExpr cols a
castWithDefault d = TExpr (F.castWithDefault d (colName @name))

-- | Parse a schema-checked column as @a@, keeping failures as 'Left'.
castEither ::
    forall (name :: Symbol) a cols.
    (KnownSymbol name, AssertPresent name cols, Columnable a, Read a) =>
    TExpr cols (Either T.Text a)
castEither = TExpr (F.castEither @a (colName @name))

-- | Parse a schema-checked column as @a@, erroring on failures.
unsafeCast ::
    forall (name :: Symbol) a cols.
    (KnownSymbol name, AssertPresent name cols, Columnable a, Read a) =>
    TExpr cols a
unsafeCast = TExpr (F.unsafeCast @a (colName @name))

-- | Equality.
eq ::
    (Columnable a, Eq a, a ~ BaseType a) =>
    TExpr cols a -> TExpr cols a -> TExpr cols Bool
eq (TExpr a) (TExpr b) = TExpr (F.eq a b)

-- | Less than.
lt ::
    (Columnable a, Ord a, a ~ BaseType a) =>
    TExpr cols a -> TExpr cols a -> TExpr cols Bool
lt (TExpr a) (TExpr b) = TExpr (F.lt a b)

-- | Greater than.
gt ::
    (Columnable a, Ord a, a ~ BaseType a) =>
    TExpr cols a -> TExpr cols a -> TExpr cols Bool
gt (TExpr a) (TExpr b) = TExpr (F.gt a b)

-- | Less than or equal.
leq ::
    (Columnable a, Ord a, a ~ BaseType a) =>
    TExpr cols a -> TExpr cols a -> TExpr cols Bool
leq (TExpr a) (TExpr b) = TExpr (F.leq a b)

-- | Greater than or equal.
geq ::
    (Columnable a, Ord a, a ~ BaseType a) =>
    TExpr cols a -> TExpr cols a -> TExpr cols Bool
geq (TExpr a) (TExpr b) = TExpr (F.geq a b)

-- | Logical and.
and :: TExpr cols Bool -> TExpr cols Bool -> TExpr cols Bool
and (TExpr a) (TExpr b) = TExpr (F.and a b)

-- | Logical or.
or :: TExpr cols Bool -> TExpr cols Bool -> TExpr cols Bool
or (TExpr a) (TExpr b) = TExpr (F.or a b)

-- | Number of distinct values (aggregation).
countDistinct :: (Columnable a, Ord a) => TExpr cols a -> TExpr cols Int
countDistinct (TExpr e) = TExpr (F.countDistinct e)

-- | The k largest values per group (aggregation).
topK :: (Columnable a, Ord a) => Int -> TExpr cols a -> TExpr cols [a]
topK k (TExpr e) = TExpr (F.topK k e)

-- | The k smallest values per group (aggregation).
bottomK :: (Columnable a, Ord a) => Int -> TExpr cols a -> TExpr cols [a]
bottomK k (TExpr e) = TExpr (F.bottomK k e)

-- | SQL spelling of 'isNothing'.
isNull :: (Columnable a) => TExpr cols (Maybe a) -> TExpr cols Bool
isNull (TExpr e) = TExpr (F.isNull e)

-- | SQL spelling of 'isJust'.
isNotNull :: (Columnable a) => TExpr cols (Maybe a) -> TExpr cols Bool
isNotNull (TExpr e) = TExpr (F.isNotNull e)

-- | Whether the text contains the literal substring.
contains :: T.Text -> TExpr cols T.Text -> TExpr cols Bool
contains needle (TExpr e) = TExpr (F.contains needle e)

-- | Character count.
length :: TExpr cols T.Text -> TExpr cols Int
length (TExpr e) = TExpr (F.length e)

-- | Integer division.
div ::
    (Integral a, Columnable a) => TExpr cols a -> TExpr cols a -> TExpr cols a
div (TExpr a) (TExpr b) = TExpr (F.div a b)

-- | Integer modulus.
mod ::
    (Integral a, Columnable a) => TExpr cols a -> TExpr cols a -> TExpr cols a
mod (TExpr a) (TExpr b) = TExpr (F.mod a b)

-- | Most frequent value (aggregation).
mode :: (Ord a, Columnable a, Eq a) => TExpr cols a -> TExpr cols a
mode (TExpr e) = TExpr (F.mode e)

-- | Sum of a nullable column, ignoring 'Nothing' (aggregation).
sumMaybe :: (Columnable a, Num a) => TExpr cols (Maybe a) -> TExpr cols a
sumMaybe (TExpr e) = TExpr (F.sumMaybe e)

-- | Mean of a nullable column, ignoring 'Nothing' (aggregation).
meanMaybe :: (Columnable a, Real a) => TExpr cols (Maybe a) -> TExpr cols Double
meanMaybe (TExpr e) = TExpr (F.meanMaybe e)

-- | Variance (aggregation).
variance ::
    (Columnable a, Real a, VU.Unbox a) => TExpr cols a -> TExpr cols Double
variance (TExpr e) = TExpr (F.variance e)

-- | Median of a nullable column, ignoring 'Nothing' (aggregation).
medianMaybe ::
    (Columnable a, Real a) => TExpr cols (Maybe a) -> TExpr cols Double
medianMaybe (TExpr e) = TExpr (F.medianMaybe e)

-- | The @n@-th percentile (aggregation).
percentile :: Int -> TExpr cols Double -> TExpr cols Double
percentile n (TExpr e) = TExpr (F.percentile n e)

-- | Standard deviation (aggregation).
stddev ::
    (Columnable a, Real a, VU.Unbox a) => TExpr cols a -> TExpr cols Double
stddev (TExpr e) = TExpr (F.stddev e)

-- | Standard deviation of a nullable column, ignoring 'Nothing' (aggregation).
stddevMaybe ::
    (Columnable a, Real a) => TExpr cols (Maybe a) -> TExpr cols Double
stddevMaybe (TExpr e) = TExpr (F.stddevMaybe e)

-- | Z-score (value minus group mean, over standard deviation).
zScore :: TExpr cols Double -> TExpr cols Double
zScore (TExpr e) = TExpr (F.zScore e)

-- | Raise an expression to an integer power.
pow :: (Columnable a, Num a) => TExpr cols a -> Int -> TExpr cols a
pow (TExpr e) i = TExpr (F.pow e i)

-- | Rectified linear unit: @max 0@.
relu :: (Columnable a, Num a, Ord a) => TExpr cols a -> TExpr cols a
relu (TExpr e) = TExpr (F.relu e)

-- | Element-wise minimum of two expressions.
min :: (Columnable a, Ord a) => TExpr cols a -> TExpr cols a -> TExpr cols a
min (TExpr a) (TExpr b) = TExpr (F.min a b)

-- | Element-wise maximum of two expressions.
max :: (Columnable a, Ord a) => TExpr cols a -> TExpr cols a -> TExpr cols a
max (TExpr a) (TExpr b) = TExpr (F.max a b)

-- | Fold a column into a single value with a seed and step function.
reduce ::
    (Columnable a, Columnable b) =>
    TExpr cols b -> a -> (a -> b -> a) -> TExpr cols a
reduce (TExpr e) start f = TExpr (F.reduce e start f)

-- | Wrap each value in 'Just'.
toMaybe :: (Columnable a) => TExpr cols a -> TExpr cols (Maybe a)
toMaybe (TExpr e) = TExpr (F.toMaybe e)

-- | Replace 'Nothing' with a default.
fromMaybe :: (Columnable a) => a -> TExpr cols (Maybe a) -> TExpr cols a
fromMaybe d (TExpr e) = TExpr (F.fromMaybe d e)

-- | True where the value is 'Just'.
isJust :: (Columnable a) => TExpr cols (Maybe a) -> TExpr cols Bool
isJust (TExpr e) = TExpr (F.isJust e)

-- | True where the value is 'Nothing'.
isNothing :: (Columnable a) => TExpr cols (Maybe a) -> TExpr cols Bool
isNothing (TExpr e) = TExpr (F.isNothing e)

-- | Unwrap a 'Just', erroring on 'Nothing'.
fromJust :: (Columnable a) => TExpr cols (Maybe a) -> TExpr cols a
fromJust (TExpr e) = TExpr (F.fromJust e)

-- | Apply a function only where the value is present.
whenPresent ::
    (Columnable a, Columnable b) =>
    (a -> b) -> TExpr cols (Maybe a) -> TExpr cols (Maybe b)
whenPresent f (TExpr e) = TExpr (F.whenPresent f e)

-- | Apply a binary function only where both values are present.
whenBothPresent ::
    (Columnable a, Columnable b, Columnable c) =>
    (a -> b -> c) ->
    TExpr cols (Maybe a) ->
    TExpr cols (Maybe b) ->
    TExpr cols (Maybe c)
whenBothPresent f (TExpr a) (TExpr b) = TExpr (F.whenBothPresent f a b)

-- | Map values through a lookup table, yielding 'Nothing' for misses.
recode ::
    (Columnable a, Columnable b, Show a, Show b, Show (a, b)) =>
    [(a, b)] -> TExpr cols a -> TExpr cols (Maybe b)
recode mapping (TExpr e) = TExpr (F.recode mapping e)

-- | Pick the first value whose condition holds, else a fallback.
recodeWithCondition ::
    (Columnable a, Columnable b) =>
    TExpr cols b ->
    [(TExpr cols a -> TExpr cols Bool, b)] ->
    TExpr cols a ->
    TExpr cols b
recodeWithCondition (TExpr fallback) conds (TExpr e) =
    TExpr (F.recodeWithCondition fallback (map untype conds) e)
  where
    untype (p, v) = (unTExpr . p . TExpr, v)

-- | Map values through a lookup table, with a default for misses.
recodeWithDefault ::
    (Columnable a, Columnable b, Show (a, b)) =>
    b -> [(a, b)] -> TExpr cols a -> TExpr cols b
recodeWithDefault d mapping (TExpr e) = TExpr (F.recodeWithDefault d mapping e)

-- | First element of a list column, or 'Nothing'.
firstOrNothing :: (Columnable a) => TExpr cols [a] -> TExpr cols (Maybe a)
firstOrNothing (TExpr e) = TExpr (F.firstOrNothing e)

-- | Last element of a list column, or 'Nothing'.
lastOrNothing :: (Columnable a) => TExpr cols [a] -> TExpr cols (Maybe a)
lastOrNothing (TExpr e) = TExpr (F.lastOrNothing e)

-- | Split text on a delimiter.
splitOn :: T.Text -> TExpr cols T.Text -> TExpr cols [T.Text]
splitOn delim (TExpr e) = TExpr (F.splitOn delim e)

-- | First regex match, or 'Nothing'.
match :: T.Text -> TExpr cols T.Text -> TExpr cols (Maybe T.Text)
match regex (TExpr e) = TExpr (F.match regex e)

-- | All regex matches.
matchAll :: T.Text -> TExpr cols T.Text -> TExpr cols [T.Text]
matchAll regex (TExpr e) = TExpr (F.matchAll regex e)

-- | Parse text into a time value with the given format.
parseDate ::
    (ParseTime t, Columnable t) =>
    T.Text -> TExpr cols T.Text -> TExpr cols (Maybe t)
parseDate format (TExpr e) = TExpr (F.parseDate format e)

-- | Number of days between two dates.
daysBetween :: TExpr cols Day -> TExpr cols Day -> TExpr cols Int
daysBetween (TExpr a) (TExpr b) = TExpr (F.daysBetween a b)

-- | Monadic bind over a column of monadic values.
bind ::
    (Columnable a, Columnable (m a), Monad m, Columnable b, Columnable (m b)) =>
    (a -> m b) -> TExpr cols (m a) -> TExpr cols (m b)
bind f (TExpr e) = TExpr (F.bind f e)
