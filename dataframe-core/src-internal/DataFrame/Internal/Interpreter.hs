{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module DataFrame.Internal.Interpreter (
    -- * New core API
    Value (..),
    Ctx (..),
    eval,
    materialize,

    -- * Backward-compatible API
    interpret,
    interpretAggregation,
    AggregationResult (..),
) where

import Data.Bifunctor (first)
import qualified Data.Map as M
import qualified Data.Set as S
import qualified Data.Text as T
import Data.Type.Equality (TestEquality (testEquality), type (:~:) (Refl))
import qualified Data.Vector as V
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import DataFrame.Errors
import DataFrame.Internal.Aggregation.Kernel.Distinct (
    countDistinctColumn,
    countDistinctGroups,
 )
import DataFrame.Internal.Column
import DataFrame.Internal.DataFrame
import DataFrame.Internal.Expression
import qualified DataFrame.Internal.Grouping as G
import qualified DataFrame.Internal.Interpreter.Kernels as K
import Type.Reflection (
    SomeTypeRep (..),
    Typeable,
    typeRep,
 )

import Data.Int (Int16, Int32, Int64, Int8)
import Data.Word (Word64)
import GHC.Float (castDoubleToWord64)

-- Specializations for common aggregation types to avoid dictionary overhead.
{-# SPECIALIZE foldLinearGroups ::
    ((Double, Int) -> Double -> (Double, Int)) ->
    (Double, Int) ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE foldLinearGroups ::
    ((Double, Int) -> Float -> (Double, Int)) ->
    (Double, Int) ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE foldLinearGroups ::
    ((Double, Int) -> Int -> (Double, Int)) ->
    (Double, Int) ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE foldLinearGroups ::
    ((Double, Int) -> Int8 -> (Double, Int)) ->
    (Double, Int) ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE foldLinearGroups ::
    ((Double, Int) -> Int16 -> (Double, Int)) ->
    (Double, Int) ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE foldLinearGroups ::
    ((Double, Int) -> Int32 -> (Double, Int)) ->
    (Double, Int) ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE foldLinearGroups ::
    ((Double, Int) -> Int64 -> (Double, Int)) ->
    (Double, Int) ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
-- foldLinearGroups: count accumulator
{-# SPECIALIZE foldLinearGroups ::
    (Int -> Double -> Int) ->
    Int ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE foldLinearGroups ::
    (Int -> Float -> Int) ->
    Int ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE foldLinearGroups ::
    (Int -> Int -> Int) ->
    Int ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE foldLinearGroups ::
    (Int -> Int8 -> Int) ->
    Int ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE foldLinearGroups ::
    (Int -> Int16 -> Int) ->
    Int ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE foldLinearGroups ::
    (Int -> Int32 -> Int) ->
    Int ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE foldLinearGroups ::
    (Int -> Int64 -> Int) ->
    Int ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
-- foldLinearGroups: sum/min/max (acc == elem)
{-# SPECIALIZE foldLinearGroups ::
    (Double -> Double -> Double) ->
    Double ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE foldLinearGroups ::
    (Float -> Float -> Float) ->
    Float ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE foldLinearGroups ::
    (Int8 -> Int8 -> Int8) ->
    Int8 ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE foldLinearGroups ::
    (Int16 -> Int16 -> Int16) ->
    Int16 ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE foldLinearGroups ::
    (Int32 -> Int32 -> Int32) ->
    Int32 ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}
{-# SPECIALIZE foldLinearGroups ::
    (Int64 -> Int64 -> Int64) ->
    Int64 ->
    Column ->
    VU.Vector Int ->
    Int ->
    Either DataFrameException Column
    #-}

-- mapColumn: finalize
{-# SPECIALIZE mapColumn ::
    ((Double, Int) -> Double) -> Column -> Either DataFrameException Column
    #-}
{-# SPECIALIZE mapColumn ::
    (Double -> Double) -> Column -> Either DataFrameException Column
    #-}
{-# SPECIALIZE mapColumn ::
    (Float -> Float) -> Column -> Either DataFrameException Column
    #-}
{-# SPECIALIZE mapColumn ::
    (Int -> Int) -> Column -> Either DataFrameException Column
    #-}
-- toDouble on an Int column (hot path for derived arithmetic)
{-# SPECIALIZE mapColumn ::
    (Int -> Double) -> Column -> Either DataFrameException Column
    #-}
-- Bool-mapping unary ops (e.g. 'not')
{-# SPECIALIZE mapColumn ::
    (Bool -> Bool) -> Column -> Either DataFrameException Column
    #-}

-- | The result of interpreting an expression; literals stay scalar.
data Value a where
    -- | A single value, not yet broadcast to any length.
    Scalar ::
        (Columnable a) =>
        !a ->
        -- | A flat column, one element per row (or per group once aggregated).
        Value a
    Flat ::
        (Columnable a) =>
        !Column ->
        -- | A grouped column: one 'Column' slice per group.
        Value a
    Group :: (Columnable a) => !(V.Vector Column) -> Value a

instance (Show a) => Show (Value a) where
    show (Scalar v) = show v
    show (Flat v) = show v
    show (Group v) = show v

-- | The interpretation context.
data Ctx
    = FlatCtx DataFrame
    | GroupCtx GroupedDataFrame

-- | Force a 'Value' into a flat 'Column' of the given length.
materialize :: forall a. (Columnable a) => Int -> Value a -> Column
materialize n (Scalar v) = broadcastScalar @a n v
materialize _ (Flat c) = c
materialize _ (Group _) =
    error "materialize: cannot flatten a grouped value to a single column"

-- | Replicate a scalar to a column of length @n@ in the cheapest form.
broadcastScalar :: forall a. (Columnable a) => Int -> a -> Column
broadcastScalar n v = case sUnbox @a of
    STrue -> fromUnboxedVector (VU.replicate n v)
    SFalse -> fromVector (V.replicate n v)

-- | Apply a pure function to a 'Value'.
liftValue ::
    (Columnable b, Columnable a) =>
    (b -> a) -> Value b -> Either DataFrameException (Value a)
liftValue f (Scalar v) = Right (Scalar (f v))
liftValue f (Flat col) = Flat <$> mapColumn f col
liftValue f (Group gs) = Group <$> V.mapM (mapColumn f) gs
{-# INLINEABLE liftValue #-}

-- | Apply a binary function to two 'Value's, degenerating when one is scalar.
liftValue2 ::
    (Columnable c, Columnable b, Columnable a) =>
    (c -> b -> a) ->
    Value c ->
    Value b ->
    Either DataFrameException (Value a)
liftValue2 f (Scalar l) (Scalar r) = Right (Scalar (f l r))
liftValue2 f (Scalar l) v = liftValue (f l) v
liftValue2 f v (Scalar r) = liftValue (`f` r) v
liftValue2 f (Flat l) (Flat r) = Flat <$> zipWithColumns f l r
liftValue2 f (Group ls) (Group rs)
    | V.length ls == V.length rs =
        Group <$> V.zipWithM (zipWithColumns f) ls rs
liftValue2 _ (Flat _) (Group _) =
    Left $ AggregatedAndNonAggregatedException "aggregated" "non-aggregated"
liftValue2 _ (Group _) (Flat _) =
    Left $ AggregatedAndNonAggregatedException "non-aggregated" "aggregated"
liftValue2 _ (Group _) (Group _) =
    Left $ InternalException "Group count mismatch in binary operation"
{-# INLINEABLE liftValue2 #-}

-- | If-then-else over values already computed for every row.
branchValue ::
    forall a.
    (Columnable a) =>
    Value Bool ->
    Value a ->
    Value a ->
    Either DataFrameException (Value a)
branchValue (Scalar True) l _ = Right l
branchValue (Scalar False) _ r = Right r
branchValue (Flat cc) l r = do
    lc <- alongside cc l
    rc <- alongside cc r
    Flat <$> chooseRows cc lc rc
branchValue (Group cgs) l r =
    Group
        <$> V.imapM
            ( \i cc -> do
                lc <- alongside cc =<< groupAt i l
                rc <- alongside cc =<< groupAt i r
                chooseRows cc lc rc
            )
            cgs

shapeMismatch :: DataFrameException
shapeMismatch =
    AggregatedAndNonAggregatedException "if-then-else branches" "mismatched shapes"

-- | A branch as a column with one value per row of the condition column.
alongside ::
    forall a.
    (Columnable a) => Column -> Value a -> Either DataFrameException Column
alongside cc (Scalar x) = Right (broadcastScalar (columnLength cc) x)
alongside _ (Flat c) = Right c
alongside _ (Group _) = Left shapeMismatch

-- | The part of a branch that belongs to group @i@.
groupAt :: Int -> Value a -> Either DataFrameException (Value a)
groupAt _ v@(Scalar _) = Right v
groupAt i (Group gs) | i < V.length gs = Right (Flat (gs V.! i))
groupAt _ _ = Left shapeMismatch

chooseRows :: Column -> Column -> Column -> Either DataFrameException Column
chooseRows cc lc rc = do
    cs <- conditionVector cc
    let n = VU.length cs
    both <- mappendColumns lc rc
    pure (atIndicesStable (VU.imap (\i c -> if c then i else n + i) cs) both)

-- | A condition with nulls is an error.
conditionVector :: Column -> Either DataFrameException (VU.Vector Bool)
conditionVector (UnboxedColumn bm (v :: VU.Vector x))
    | Just Refl <- testEquality (typeRep @x) (typeRep @Bool)
    , maybe True (not . bitmapHasNulls (VU.length v)) bm =
        Right v
conditionVector c = toVector @Bool @VU.Vector c

-- | A value as a kernel operand: a literal or a null-free unboxed column.
operand :: forall a. (Columnable a) => Value a -> Maybe (K.Operand a)
operand (Scalar s) = Just (K.Literal s)
operand (Flat (UnboxedColumn Nothing (v :: VU.Vector x)))
    | Just Refl <- testEquality (typeRep @x) (typeRep @a) = Just (K.Column v)
operand _ = Nothing

fromOperand :: (Columnable a, VU.Unbox a) => K.Operand a -> Value a
fromOperand (K.Literal x) = Scalar x
fromOperand (K.Column v) = Flat (UnboxedColumn Nothing v)

-- | A column loop for a built-in binary operator, if it can run.
builtinBinary ::
    forall c b a.
    (Columnable c, Columnable b, Columnable a) =>
    T.Text -> Maybe (Value c -> Value b -> Maybe (Value a))
builtinBinary name = do
    Refl <- testEquality (typeRep @c) (typeRep @b)
    case (comparison, arithmetic, integral, textEquality, textContains) of
        (Just run, _, _, _, _) -> Just run
        (_, Just run, _, _, _) -> Just run
        (_, _, Just run, _, _) -> Just run
        (_, _, _, Just run, _) -> Just run
        (_, _, _, _, found) -> found
  where
    -- Int-only integer division and remainder.
    integral :: (c ~ b) => Maybe (Value c -> Value c -> Maybe (Value a))
    integral = do
        Refl <- testEquality (typeRep @a) (typeRep @Int)
        Refl <- testEquality (typeRep @c) (typeRep @Int)
        op <- case name of
            "div" -> Just div
            "mod" -> Just mod
            _ -> Nothing
        -- Only non-zero literal divisors: lazy evaluation might skip a zero.
        Just $ \l r -> case r of
            Scalar k | k /= 0 -> fromOperand <$> (K.zipOperands op <$> operand l <*> operand r)
            _ -> Nothing
    textContains :: (c ~ b) => Maybe (Value c -> Value c -> Maybe (Value a))
    textContains = do
        Refl <- testEquality (typeRep @c) (typeRep @T.Text)
        Refl <- testEquality (typeRep @a) (typeRep @Bool)
        case name of
            "contains" -> Just packedContains
            _ -> Nothing
    comparison :: (c ~ b) => Maybe (Value c -> Value c -> Maybe (Value a))
    comparison = do
        Refl <- testEquality (typeRep @a) (typeRep @Bool)
        op <- K.cmpOpByName name
        run <- K.compareOperands @c
        Just (\l r -> fromOperand <$> (run op <$> operand l <*> operand r))
    arithmetic :: (c ~ b) => Maybe (Value c -> Value c -> Maybe (Value a))
    arithmetic = do
        Refl <- testEquality (typeRep @a) (typeRep @c)
        op <- K.arithOpByName name
        run <- K.arithOperands @c
        case sUnbox @c of
            STrue -> Just (\l r -> fromOperand <$> (run op <$> operand l <*> operand r))
            SFalse -> Nothing
    textEquality :: (c ~ b) => Maybe (Value c -> Value c -> Maybe (Value a))
    textEquality = do
        Refl <- testEquality (typeRep @c) (typeRep @T.Text)
        Refl <- testEquality (typeRep @a) (typeRep @Bool)
        wantEq <- case name of
            "eq" -> Just True
            "neq" -> Just False
            _ -> Nothing
        Just (packedEquals wantEq)

-- | Column on the left, needle on the right; not used if needle has U+FFFD.
packedContains :: Value T.Text -> Value T.Text -> Maybe (Value Bool)
packedContains (Flat (PackedText Nothing p)) (Scalar t)
    | not (T.any (== '\xFFFD') t) =
        Just (Flat (UnboxedColumn Nothing (K.packedContainsLit p t)))
packedContains _ _ = Nothing

-- | Not used when the literal contains U+FFFD (see 'K.packedEqLit').
packedEquals :: Bool -> Value T.Text -> Value T.Text -> Maybe (Value Bool)
packedEquals wantEq l r = do
    (p, t) <- case (l, r) of
        (Flat (PackedText Nothing p), Scalar t) -> Just (p, t)
        (Scalar t, Flat (PackedText Nothing p)) -> Just (p, t)
        _ -> Nothing
    if T.any (== '\xFFFD') t
        then Nothing
        else Just (Flat (UnboxedColumn Nothing (K.packedEqLit wantEq p t)))

-- | As 'builtinBinary', for unary operators.
builtinUnary ::
    forall b a.
    (Columnable b, Columnable a) => T.Text -> Maybe (Value b -> Maybe (Value a))
builtinUnary "toDouble"
    | Just Refl <- testEquality (typeRep @b) (typeRep @Int)
    , Just Refl <- testEquality (typeRep @a) (typeRep @Double) =
        Just $ \v -> case operand v of
            Just (K.Column x) -> Just (Flat (UnboxedColumn Nothing (K.intToDouble x)))
            _ -> Nothing
builtinUnary "textLength"
    | Just Refl <- testEquality (typeRep @b) (typeRep @T.Text)
    , Just Refl <- testEquality (typeRep @a) (typeRep @Int) =
        Just $ \case
            Flat (PackedText Nothing p) -> Just (Flat (UnboxedColumn Nothing (K.packedCharCount p)))
            _ -> Nothing
builtinUnary _ = Nothing

applyUnary ::
    forall op b a.
    (UnaryOp op, Columnable b, Columnable a) =>
    op b a -> Value b -> Either DataFrameException (Value a)
applyUnary op v = case builtinUnary @b @a (unaryName op) >>= ($ v) of
    Just out -> Right out
    Nothing -> liftValue (fastUnaryFn @b @a (unaryName op) (unaryFn op)) v

applyBinary ::
    forall op c b a.
    (BinaryOp op, Columnable c, Columnable b, Columnable a) =>
    op c b a -> Value c -> Value b -> Either DataFrameException (Value a)
applyBinary op l r = case builtinBinary @c @b @a (binaryName op) >>= (\run -> run l r) of
    Just out -> Right out
    Nothing -> liftValue2 (binaryFn op) l r

-- | The rows of the frame an evaluation covers.
data Rows = AllRows | Rows !(VU.Vector Int)

data Env = Env {envFrame :: DataFrame, envRows :: Rows}

frameRows :: Env -> Int
frameRows env = fst (dataframeDimensions (envFrame env))

rowCount :: Env -> Int
rowCount env = case envRows env of
    AllRows -> frameRows env
    Rows sel -> VU.length sel

-- | Keep only the covered rows of a value computed for the whole frame.
restrict :: Rows -> Value a -> Value a
restrict (Rows sel) (Flat col) = Flat (atIndicesStable sel col)
restrict _ v = v

-- | The rows at these positions of the rows already covered.
narrow :: Env -> VU.Vector Int -> Env
narrow env positions = env{envRows = Rows rows}
  where
    rows = case envRows env of
        AllRows -> positions
        Rows sel -> VU.unsafeBackpermute sel positions

-- | Identifies an expression built only from columns, literals and built-ins.
data Key
    = KeyColumn T.Text SomeTypeRep
    | KeyLiteral Literal
    | KeyUnary T.Text SomeTypeRep Key
    | KeyBinary T.Text SomeTypeRep Key Key
    deriving (Eq, Ord)

data Literal
    = LitDouble Word64
    | LitInt Int
    | LitBool Bool
    | LitText T.Text
    deriving (Eq, Ord)

-- | Keys are only built for expressions up to this many nodes.
maxKeyNodes :: Int
maxKeyNodes = 32

keyOf :: (Columnable a) => Expr a -> Maybe Key
keyOf expr = fst <$> go maxKeyNodes expr
  where
    -- Returns the key and the node budget left.
    go :: forall x. (Columnable x) => Int -> Expr x -> Maybe (Key, Int)
    go budget _ | budget <= 0 = Nothing
    go budget e = case e of
        Col name -> Just (KeyColumn name (SomeTypeRep (typeRep @x)), budget - 1)
        Lit v -> (\l -> (KeyLiteral l, budget - 1)) <$> literalKey v
        Unary op (inner :: Expr b) -> do
            _ <- builtinUnary @b @x (unaryName op)
            (k, left) <- go (budget - 1) inner
            Just (KeyUnary (unaryName op) (SomeTypeRep (typeRep @b)) k, left)
        Binary op (l :: Expr c) (r :: Expr b) -> do
            _ <- builtinBinary @c @b @x (binaryName op)
            -- A division is shareable only with a non-zero literal divisor.
            if binaryName op `elem` ["div", "mod"]
                then case r of
                    Lit (k :: b)
                        | Just Refl <- testEquality (typeRep @b) (typeRep @Int)
                        , k /= 0 ->
                            Just ()
                    _ -> Nothing
                else Just ()
            (kl, afterLeft) <- go (budget - 1) l
            (kr, left) <- go afterLeft r
            Just (KeyBinary (binaryName op) (SomeTypeRep (typeRep @c)) kl kr, left)
        _ -> Nothing

literalKey :: forall a. (Columnable a) => a -> Maybe Literal
literalKey v
    | Just Refl <- testEquality (typeRep @a) (typeRep @Double) =
        Just (LitDouble (castDoubleToWord64 v))
    | Just Refl <- testEquality (typeRep @a) (typeRep @Int) = Just (LitInt v)
    | Just Refl <- testEquality (typeRep @a) (typeRep @Bool) = Just (LitBool v)
    | Just Refl <- testEquality (typeRep @a) (typeRep @T.Text) = Just (LitText v)
    | otherwise = Nothing

-- | The keys of operator nodes that occur more than once in an expression.
repeatedKeys :: (Columnable a) => Expr a -> S.Set Key
repeatedKeys expr = M.keysSet (M.filter (> 1) (count expr M.empty))
  where
    count :: forall x. (Columnable x) => Expr x -> M.Map Key Int -> M.Map Key Int
    count e seen = case e of
        Unary _ inner -> count inner (note e seen)
        Binary _ l r -> count r (count l (note e seen))
        If c l r -> count r (count l (count c seen))
        _ -> seen
    note :: forall x. (Columnable x) => Expr x -> M.Map Key Int -> M.Map Key Int
    note e seen = maybe seen (\k -> M.insertWith (+) k (1 :: Int) seen) (keyOf e)

data Sharing = Sharing
    { repeated :: !(S.Set Key)
    , rowsAsked :: !(M.Map Key Int)
    -- ^ How many rows each repeated key has been computed for so far.
    , computed :: !(M.Map Key Column)
    -- ^ Results kept for the whole frame.
    , bytesKept :: !Int
    }

-- | Results kept for reuse during one evaluation may total this many bytes.
maxBytesKept :: Int
maxBytesKept = 256 * 1024 * 1024

newtype Eval a = Eval
    {runEval :: Sharing -> Either DataFrameException (a, Sharing)}

instance Functor Eval where
    fmap f (Eval m) = Eval (fmap (first f) . m)

instance Applicative Eval where
    pure x = Eval (\s -> Right (x, s))
    Eval mf <*> Eval mx = Eval $ \s -> do
        (f, s') <- mf s
        (x, s'') <- mx s'
        Right (f x, s'')

instance Monad Eval where
    Eval m >>= k = Eval $ \s -> do
        (x, s') <- m s
        runEval (k x) s'

failing :: Either DataFrameException a -> Eval a
failing r = Eval (\s -> fmap (,s) r)

inContext :: (Show e) => Expr e -> Eval a -> Eval a
inContext expr (Eval m) = Eval (addContext expr . m)

evalFlat ::
    (Columnable a) => DataFrame -> Expr a -> Either DataFrameException (Value a)
evalFlat df expr =
    fst
        <$> runEval
            (evalRows (Env df AllRows) expr)
            (Sharing (repeatedKeys expr) M.empty M.empty 0)

evalRows :: forall a. (Columnable a) => Env -> Expr a -> Eval (Value a)
evalRows env expr = case expr of
    Lit v -> pure (Scalar v)
    Unary{} -> evalShared env expr
    Binary{} -> evalShared env expr
    If cond l r -> inContext expr $ do
        c <- evalRows env cond
        case c of
            Scalar holds -> do
                -- The other branch runs on no rows; it can only report columns.
                let (chosen, other) = if holds then (l, r) else (r, l)
                _ <- evalRows (narrow env VU.empty) other
                evalRows env chosen
            Flat cc -> do
                cs <- failing (conditionVector cc)
                let (yes, no) = K.partitionRows cs
                lv <- evalArm env yes l
                rv <- evalArm env no r
                failing (mergeArms cs yes no lv rv)
            Group _ -> failing (Left (InternalException "grouped condition in a flat frame"))
    -- Aggregations and windows need every row; then keep the covered ones.
    _ -> failing (restrict (envRows env) <$> eval (FlatCtx (envFrame env)) expr)

-- | Evaluate an operator node, reusing a result kept for the whole frame.
evalShared :: forall a. (Columnable a) => Env -> Expr a -> Eval (Value a)
evalShared env expr = Eval $ \s -> case keyOf expr of
    Just key
        | S.member key (repeated s) -> case M.lookup key (computed s) of
            Just col -> Right (restrict (envRows env) (Flat col), s)
            Nothing -> runEval (askFor key) s
    _ -> runEval (evalNode env expr) s
  where
    total = frameRows env
    askFor key = Eval $ \s ->
        let asked = M.findWithDefault 0 key (rowsAsked s) + rowCount env
         in if asked >= total && bytesKept s + 8 * total <= maxBytesKept
                then do
                    (full, s') <- runEval (evalNode env{envRows = AllRows} expr) s
                    Right (restrict (envRows env) full, keep key full s')
                else runEval (evalNode env expr) s{rowsAsked = M.insert key asked (rowsAsked s)}
    keep :: Key -> Value a -> Sharing -> Sharing
    keep key (Flat col) s =
        s
            { computed = M.insert key col (computed s)
            , bytesKept = bytesKept s + columnBytes col
            }
    keep _ _ s = s

evalNode :: forall a. (Columnable a) => Env -> Expr a -> Eval (Value a)
evalNode env expr = inContext expr $ case expr of
    Unary op inner -> evalRows env inner >>= failing . applyUnary op
    Binary op l r -> do
        lv <- evalRows env l
        rv <- evalRows env r
        failing (applyBinary op lv rv)
    _ -> evalRows env expr

columnBytes :: Column -> Int
columnBytes (UnboxedColumn _ (v :: VU.Vector x))
    | Just Refl <- testEquality (typeRep @x) (typeRep @Bool) = VU.length v
    | otherwise = 8 * VU.length v
columnBytes c = 16 * columnLength c

-- | A branch computed for every covered row, or only for the rows choosing it.
data Arm a = ForEveryRow (Value a) | ForChosenRows (Value a)

-- | A column or literal is already there for every row; else compute lazily.
evalArm :: (Columnable a) => Env -> VU.Vector Int -> Expr a -> Eval (Arm a)
evalArm env chosen expr = case expr of
    Col _ -> ForEveryRow <$> evalRows env expr
    Lit _ -> ForEveryRow <$> evalRows env expr
    _ -> ForChosenRows <$> evalRows (narrow env chosen) expr

-- | Each row's value from the branch its condition chooses.
mergeArms ::
    forall a.
    (Columnable a) =>
    VU.Vector Bool ->
    VU.Vector Int ->
    VU.Vector Int ->
    Arm a ->
    Arm a ->
    Either DataFrameException (Value a)
mergeArms cs yes no l r = case (K.mergeBranches @a, branch l, branch r, sUnbox @a) of
    (Just run, Just lb, Just rb, STrue) -> Right (Flat (UnboxedColumn Nothing (run cs lb rb)))
    _
        | VU.null no -> Right (Flat lc)
        | VU.null yes -> Right (Flat rc)
        | otherwise -> do
            both <- mappendColumns lc rc
            -- Row i reads the next unused value of its branch.
            let place (y, f) c = if c then (y + 1, f) else (y, f + 1)
                taken = VU.prescanl' place (0, VU.length yes) cs
                source = VU.zipWith (\c (y, f) -> if c then y else f) cs taken
            Right (Flat (atIndicesStable source both))
  where
    branch :: Arm a -> Maybe (K.Branch a)
    branch arm = case arm of
        ForEveryRow v ->
            (\case K.Literal x -> K.Constant x; K.Column xs -> K.EveryRow xs)
                <$> operand v
        ForChosenRows v ->
            (\case K.Literal x -> K.Constant x; K.Column xs -> K.ChosenRows xs)
                <$> operand v
    -- The generic path wants each branch as one value per row choosing it.
    chosenColumn :: VU.Vector Int -> Arm a -> Column
    chosenColumn rows arm = case arm of
        ForEveryRow v -> materialize @a (VU.length rows) (restrict (Rows rows) v)
        ForChosenRows v -> materialize @a (VU.length rows) v
    lc = chosenColumn yes l
    rc = chosenColumn no r

-- | Annotate any 'TypeMismatchException' with the expression being evaluated.
addContext ::
    (Show a) => Expr a -> Either DataFrameException b -> Either DataFrameException b
addContext expr = first (enrichError (show expr))

enrichError :: String -> DataFrameException -> DataFrameException
enrichError loc (TypeMismatchException ctx) =
    TypeMismatchException
        ctx
            { callingFunctionName =
                callingFunctionName ctx <|+> Just "eval"
            , errorColumnName =
                errorColumnName ctx <|+> Just loc
            }
  where
    Nothing <|+> b = b
    a <|+> _ = a
enrichError _ e = e

-- | Given a flat column and grouping metadata, produce one 'Column' per group.
sliceGroups :: Column -> VU.Vector Int -> VU.Vector Int -> V.Vector Column
sliceGroups col os indices = case col of
    PackedText _ _ -> sliceGroups (materializePacked col) os indices
    MergedColumn _ _ -> sliceGroups (materializeMerged col) os indices
    BoxedColumn bm vec ->
        let !sorted =
                V.generate
                    (VU.length indices)
                    ((vec `V.unsafeIndex`) . (indices `VU.unsafeIndex`))
            !sortedBm = permuteBitmap bm
         in V.generate nGroups $ \i ->
                BoxedColumn
                    (fmap (bitmapSlice (start i) (len i)) sortedBm)
                    (V.unsafeSlice (start i) (len i) sorted)
    UnboxedColumn bm vec ->
        let !sorted = VU.unsafeBackpermute vec indices
            !sortedBm = permuteBitmap bm
         in V.generate nGroups $ \i ->
                UnboxedColumn
                    (fmap (bitmapSlice (start i) (len i)) sortedBm)
                    (VU.unsafeSlice (start i) (len i) sorted)
  where
    !nGroups = VU.length os - 1
    start i = os `VU.unsafeIndex` i
    len i = os `VU.unsafeIndex` (i + 1) - start i
    permuteBitmap = fmap $ \bm ->
        buildBitmapFromValid $
            VU.map
                (\r -> if bitmapTestBit bm r then 1 else 0)
                indices
{-# INLINE sliceGroups #-}

numGroups :: GroupedDataFrame -> Int
numGroups gdf = VU.length (offsets gdf) - 1

-- | Build the inverse of a permutation vector.
invertPermutation :: VU.Vector Int -> VU.Vector Int
invertPermutation perm = VU.create $ do
    let !n = VU.length perm
    inv <- VUM.new n
    VU.imapM_ (flip (VUM.unsafeWrite inv)) perm
    return inv
{-# INLINE invertPermutation #-}

-- | Coerce a column to type @a@, then apply @onResult@ to each element.
promoteColumnWith ::
    forall a b.
    (Columnable a, Columnable b, Read a) =>
    (Either String a -> b) -> Column -> Either DataFrameException Column
promoteColumnWith onResult col
    | hasElemType @b col = Right col
    | hasElemType @a col = mapColumn @a (onResult . Right) col
    | Just result <- tryMaybeWrap @a @b onResult col = result
    | otherwise =
        case testEquality (typeRep @a) (typeRep @Double) of
            Just Refl -> promoteToDoubleWith onResult col
            Nothing ->
                case testEquality (typeRep @a) (typeRep @Float) of
                    Just Refl -> promoteToFloatWith onResult col
                    Nothing ->
                        case testEquality (typeRep @a) (typeRep @Int) of
                            Just Refl -> promoteToIntWith onResult col
                            Nothing -> tryParseWith @a onResult col

promoteToDoubleWith ::
    forall b.
    (Columnable b) =>
    (Either String Double -> b) -> Column -> Either DataFrameException Column
promoteToDoubleWith onResult col = case col of
    UnboxedColumn Nothing (v :: VU.Vector c) ->
        case sFloating @c of
            STrue ->
                Right $
                    fromVector @b
                        (V.map (onResult . Right . (realToFrac :: c -> Double)) (VG.convert v))
            SFalse -> case sIntegral @c of
                STrue ->
                    Right $
                        fromVector @b
                            (V.map (onResult . Right . (fromIntegral :: c -> Double)) (VG.convert v))
                SFalse -> castMismatch @c @b
    UnboxedColumn (Just bm) (v :: VU.Vector c) ->
        case sFloating @c of
            STrue ->
                Right $
                    fromVector @b
                        ( V.generate (VU.length v) $ \i ->
                            if bitmapTestBit bm i
                                then onResult (Right (realToFrac (VU.unsafeIndex v i) :: Double))
                                else onResult (Left "null")
                        )
            SFalse -> case sIntegral @c of
                STrue ->
                    Right $
                        fromVector @b
                            ( V.generate (VU.length v) $ \i ->
                                if bitmapTestBit bm i
                                    then onResult (Right (fromIntegral (VU.unsafeIndex v i) :: Double))
                                    else onResult (Left "null")
                            )
                SFalse -> castMismatch @c @b
    BoxedColumn _ _ -> tryParseWith @Double onResult col
    PackedText _ _ -> promoteToDoubleWith onResult (materializePacked col)
    MergedColumn _ _ -> promoteToDoubleWith onResult (materializeMerged col)

promoteToFloatWith ::
    forall b.
    (Columnable b) =>
    (Either String Float -> b) -> Column -> Either DataFrameException Column
promoteToFloatWith onResult col = case col of
    UnboxedColumn Nothing (v :: VU.Vector c) ->
        case sFloating @c of
            STrue ->
                Right $
                    fromVector @b
                        (V.map (onResult . Right . (realToFrac :: c -> Float)) (VG.convert v))
            SFalse -> case sIntegral @c of
                STrue ->
                    Right $
                        fromVector @b
                            (V.map (onResult . Right . (fromIntegral :: c -> Float)) (VG.convert v))
                SFalse -> castMismatch @c @b
    UnboxedColumn (Just bm) (v :: VU.Vector c) ->
        case sFloating @c of
            STrue ->
                Right $
                    fromVector @b
                        ( V.generate (VU.length v) $ \i ->
                            if bitmapTestBit bm i
                                then onResult (Right (realToFrac (VU.unsafeIndex v i) :: Float))
                                else onResult (Left "null")
                        )
            SFalse -> case sIntegral @c of
                STrue ->
                    Right $
                        fromVector @b
                            ( V.generate (VU.length v) $ \i ->
                                if bitmapTestBit bm i
                                    then onResult (Right (fromIntegral (VU.unsafeIndex v i) :: Float))
                                    else onResult (Left "null")
                            )
                SFalse -> castMismatch @c @b
    BoxedColumn _ _ -> tryParseWith @Float onResult col
    PackedText _ _ -> promoteToFloatWith onResult (materializePacked col)
    MergedColumn _ _ -> promoteToFloatWith onResult (materializeMerged col)

promoteToIntWith ::
    forall b.
    (Columnable b) =>
    (Either String Int -> b) -> Column -> Either DataFrameException Column
promoteToIntWith onResult col = case col of
    UnboxedColumn Nothing (v :: VU.Vector c) ->
        case sFloating @c of
            STrue ->
                Right $
                    fromVector @b
                        (V.map (onResult . Right . (round . (realToFrac :: c -> Double))) (VG.convert v))
            SFalse -> case sIntegral @c of
                STrue ->
                    Right $
                        fromVector @b
                            (V.map (onResult . Right . (fromIntegral :: c -> Int)) (VG.convert v))
                SFalse -> castMismatch @c @b
    UnboxedColumn (Just bm) (v :: VU.Vector c) ->
        case sFloating @c of
            STrue ->
                Right $
                    fromVector @b
                        ( V.generate (VU.length v) $ \i ->
                            if bitmapTestBit bm i
                                then onResult (Right (round (realToFrac (VU.unsafeIndex v i) :: Double)))
                                else onResult (Left "null")
                        )
            SFalse -> case sIntegral @c of
                STrue ->
                    Right $
                        fromVector @b
                            ( V.generate (VU.length v) $ \i ->
                                if bitmapTestBit bm i
                                    then onResult (Right (fromIntegral (VU.unsafeIndex v i) :: Int))
                                    else onResult (Left "null")
                            )
                SFalse -> castMismatch @c @b
    BoxedColumn _ _ -> tryParseWith @Int onResult col
    PackedText _ _ -> promoteToIntWith onResult (materializePacked col)
    MergedColumn _ _ -> promoteToIntWith onResult (materializeMerged col)

-- | Single parse primitive: apply @onResult@ to the result of 'reads'.
parseWith :: (Read a) => (Either String a -> b) -> String -> b
parseWith f s = case reads s of
    [(x, "")] -> f (Right x)
    _ -> case reads (show s) of
        [(x, "")] -> f (Right x)
        _ -> f (Left s)

tryParseWith ::
    forall a b.
    (Columnable a, Columnable b, Read a) =>
    (Either String a -> b) -> Column -> Either DataFrameException Column
tryParseWith onResult col = case col of
    PackedText _ _ -> tryParseWith onResult (materializePacked col)
    MergedColumn _ _ -> tryParseWith onResult (materializeMerged col)
    BoxedColumn bm (v :: V.Vector c) ->
        case testEquality (typeRep @c) (typeRep @String) of
            Just Refl -> case bm of
                Nothing -> Right $ fromVector @b $ V.map (parseWith onResult) v
                Just bitmap ->
                    Right $
                        fromVector @b $
                            V.imap
                                ( \i x ->
                                    if bitmapTestBit bitmap i then parseWith onResult x else onResult (Left "null")
                                )
                                v
            Nothing ->
                case testEquality (typeRep @c) (typeRep @T.Text) of
                    Just Refl -> case bm of
                        Nothing -> Right $ fromVector @b $ V.map (parseWith onResult . T.unpack) v
                        Just bitmap ->
                            Right $
                                fromVector @b $
                                    V.imap
                                        ( \i x ->
                                            if bitmapTestBit bitmap i
                                                then parseWith onResult (T.unpack x)
                                                else onResult (Left "null")
                                        )
                                        v
                    Nothing -> castMismatch @c @b
    UnboxedColumn bm (v :: VU.Vector c) -> case bm of
        Nothing -> Right $ fromVector @b $ V.map (parseWith onResult . show) (V.convert v)
        Just bitmap ->
            Right $
                fromVector @b $
                    V.imap
                        ( \i x ->
                            if bitmapTestBit bitmap i
                                then parseWith onResult (show x)
                                else onResult (Left "null")
                        )
                        (V.convert v)

-- | Wrap each element in 'Just' when the output type is @Maybe c@.
tryMaybeWrap ::
    forall a b.
    (Columnable a, Columnable b) =>
    (Either String a -> b) -> Column -> Maybe (Either DataFrameException Column)
tryMaybeWrap _onResult col = case col of
    UnboxedColumn Nothing (v :: VU.Vector c) ->
        let wrapped = V.map Just (VG.convert v) :: V.Vector (Maybe c)
         in case testEquality (typeRep @b) (typeRep @(Maybe c)) of
                Just Refl -> Just $ Right $ fromVector @b wrapped
                Nothing ->
                    case testEquality (typeRep @b) (typeRep @(Maybe (Maybe c))) of
                        Just _ -> Just $ Right $ fromVector @(Maybe c) wrapped
                        Nothing -> Nothing
    BoxedColumn Nothing (v :: V.Vector c) ->
        let wrapped = V.map Just v :: V.Vector (Maybe c)
         in case testEquality (typeRep @b) (typeRep @(Maybe c)) of
                Just Refl -> Just $ Right $ fromVector @b wrapped
                Nothing ->
                    case testEquality (typeRep @b) (typeRep @(Maybe (Maybe c))) of
                        Just _ -> Just $ Right $ fromVector @(Maybe c) wrapped
                        Nothing -> Nothing
    _ -> Nothing

castMismatch ::
    forall src tgt.
    (Typeable src, Typeable tgt) =>
    Either DataFrameException Column
castMismatch =
    Left $
        TypeMismatchException
            MkTypeErrorContext
                { userType = Right (typeRep @tgt)
                , expectedType = Right (typeRep @src)
                , callingFunctionName = Just "cast"
                , errorColumnName = Nothing
                }

-- | Evaluate an expression in a given context, producing a 'Value'.
eval ::
    forall a.
    (Columnable a) =>
    Ctx -> Expr a -> Either DataFrameException (Value a)
eval _ (Lit v) = Right (Scalar v)
eval (FlatCtx df) (Col name) =
    case getColumn name df of
        Nothing ->
            Left $ ColumnsNotFoundException [name] "" (M.keys $ columnIndices df)
        Just c
            | hasElemType @a c -> Right (Flat c)
            | otherwise ->
                Left $
                    TypeMismatchException
                        ( MkTypeErrorContext
                            { userType = Right (typeRep @a)
                            , expectedType = Left (columnTypeString c)
                            , errorColumnName = Just (T.unpack name)
                            , callingFunctionName = Just "col"
                            } ::
                            TypeErrorContext a ()
                        )
eval (GroupCtx gdf) (Col name) =
    case getColumn name (fullDataframe gdf) of
        Nothing ->
            Left $
                ColumnsNotFoundException
                    [name]
                    ""
                    (M.keys $ columnIndices $ fullDataframe gdf)
        Just c
            | hasElemType @a c ->
                Right (Group (sliceGroups c (offsets gdf) (valueIndices gdf)))
            | otherwise ->
                Left $
                    TypeMismatchException
                        ( MkTypeErrorContext
                            { userType = Right (typeRep @a)
                            , expectedType = Left (columnTypeString c)
                            , errorColumnName = Just (T.unpack name)
                            , callingFunctionName = Just "col"
                            } ::
                            TypeErrorContext a ()
                        )
eval (FlatCtx df) (CastWith name _tag onResult) =
    case getColumn name df of
        Nothing ->
            Left $
                ColumnsNotFoundException [name] "" (M.keys $ columnIndices df)
        Just c -> Flat <$> promoteColumnWith onResult c
eval (GroupCtx gdf) (CastWith name _tag onResult) =
    case getColumn name (fullDataframe gdf) of
        Nothing ->
            Left $
                ColumnsNotFoundException
                    [name]
                    ""
                    (M.keys $ columnIndices $ fullDataframe gdf)
        Just c -> do
            promoted <- promoteColumnWith onResult c
            Right $ Group (sliceGroups promoted (offsets gdf) (valueIndices gdf))
eval ctx (CastExprWith _tag onResult (inner :: Expr src)) = do
    v <- eval @src ctx inner
    case v of
        Scalar s ->
            Flat <$> promoteColumnWith onResult (fromList @src [s])
        Flat col ->
            Flat <$> promoteColumnWith onResult col
        Group gs ->
            Group <$> V.mapM (promoteColumnWith onResult) gs
eval (FlatCtx df) expr@(Unary{}) = evalFlat df expr
eval (FlatCtx df) expr@(Binary{}) = evalFlat df expr
eval (FlatCtx df) expr@(If{}) = evalFlat df expr
eval ctx expr@(Unary op (inner :: Expr b)) = addContext expr $ do
    v <- eval @b ctx inner
    applyUnary op v
eval ctx expr@(Binary op (left :: Expr c) (right :: Expr b)) =
    addContext expr $ do
        l <- eval @c ctx left
        r <- eval @b ctx right
        applyBinary op l r
eval ctx expr@(If cond l r) = addContext expr $ do
    c <- eval @Bool ctx cond
    lv <- eval @a ctx l
    rv <- eval @a ctx r
    branchValue c lv rv
eval (FlatCtx df) expr@(Over keys inner) = addContext expr $ do
    let gdf = G.groupBy keys df
    v <- eval (GroupCtx gdf) inner
    case v of
        Scalar s ->
            Right (Scalar s)
        Flat groupCol ->
            Right (Flat (atIndicesStable (rowToGroup gdf) groupCol))
        Group groupCols -> do
            sorted <- V.fold1M' mappendColumns groupCols
            let inv = invertPermutation (valueIndices gdf)
            Right (Flat (atIndicesStable inv sorted))
eval (GroupCtx _) expr@(Over _ _) =
    addContext expr $
        Left
            ( InternalException
                "Over (window function) is not supported inside a grouped context"
            )
-- Fast path: seeded FoldAgg on a bare Col folds over permuted indices.

eval (GroupCtx gdf) expr@(Agg (FoldAgg _ (Just seed) (f :: a -> b -> a)) (Col name :: Expr b)) =
    addContext expr $
        case getColumn name (fullDataframe gdf) of
            Nothing ->
                Left $
                    ColumnsNotFoundException
                        [name]
                        ""
                        (M.keys $ columnIndices $ fullDataframe gdf)
            Just col ->
                Flat <$> foldLinearGroups @b @a f seed col (rowToGroup gdf) (numGroups gdf)
-- Fast path: FoldAgg (seedless) on a bare Col in GroupCtx.

eval (GroupCtx gdf) expr@(Agg (FoldAgg _ Nothing (f :: a -> b -> a)) (Col name :: Expr b)) =
    addContext expr $
        case testEquality (typeRep @a) (typeRep @b) of
            Nothing ->
                Left $
                    InternalException
                        "Type mismatch in seedless fold: \
                        \accumulator and element types must match"
            Just Refl ->
                case getColumn name (fullDataframe gdf) of
                    Nothing ->
                        Left $
                            ColumnsNotFoundException
                                [name]
                                ""
                                (M.keys $ columnIndices $ fullDataframe gdf)
                    Just col ->
                        Flat <$> foldl1DirectGroups @b f col (valueIndices gdf) (offsets gdf)
-- Fast path: countDistinct on a bare Col goes to the partitioned hash kernel.
eval ctx (Agg (MergeAgg "countDistinct" _ _ _ _) (Col name))
    | Just Refl <- testEquality (typeRep @a) (typeRep @Int)
    , Just col <- getColumn name (ctxFrame ctx)
    , Just v <- distinctValue col =
        Right v
  where
    ctxFrame (FlatCtx df) = df
    ctxFrame (GroupCtx gdf) = fullDataframe gdf
    distinctValue col = case ctx of
        FlatCtx _ -> Scalar <$> countDistinctColumn col
        GroupCtx gdf ->
            Flat . fromUnboxedVector
                <$> countDistinctGroups (rowToGroup gdf) (numGroups gdf) col
-- Fast path: MergeAgg on a bare Col in GroupCtx.

eval
    (GroupCtx gdf)
    expr@( Agg
                (MergeAgg _ seed (step :: acc -> b -> acc) _ (finalize :: acc -> a))
                (Col name :: Expr b)
            ) =
        addContext expr $
            case getColumn name (fullDataframe gdf) of
                Nothing ->
                    Left $
                        ColumnsNotFoundException
                            [name]
                            ""
                            (M.keys $ columnIndices $ fullDataframe gdf)
                Just col ->
                    Flat
                        <$> ( foldLinearGroups @b step seed col (rowToGroup gdf) (numGroups gdf)
                                >>= mapColumn finalize
                            )
eval ctx expr@(Agg (CollectAgg _ (f :: v b -> a)) inner) =
    addContext expr $ do
        v <- eval @b ctx inner
        case v of
            Scalar _ ->
                Left $
                    InternalException
                        "Cannot apply a collection aggregation to a scalar"
            Flat col ->
                Scalar <$> applyCollect @v @b @a f col
            Group gs ->
                Flat . fromVector
                    <$> V.mapM (applyCollect @v @b @a f) gs
eval ctx expr@(Agg (FoldAgg _ (Just seed) (f :: a -> b -> a)) inner) =
    addContext expr $ do
        v <- eval @b ctx inner
        case v of
            Scalar x -> Right (broadcastFold ctx seed f x)
            Flat col ->
                Scalar <$> foldlColumn @b @a f seed col
            Group gs ->
                Flat . fromVector
                    <$> V.mapM (foldlColumn @b @a f seed) gs
eval (FlatCtx df) (Agg (MergeAgg "mean" _ _ _ (_ :: acc -> a)) (Col name))
    | Just Refl <- testEquality (typeRep @a) (typeRep @Double)
    , Just v <- fusedMean df name =
        Right (Scalar v)
eval
    ctx
    expr@( Agg
                (MergeAgg _ seed (step :: acc -> b -> acc) _ (finalize :: acc -> a))
                (inner :: Expr b)
            ) =
        addContext expr $ do
            v <- eval @b ctx inner
            case v of
                Scalar x -> case broadcastFold ctx seed step x of
                    Scalar acc -> Right (Scalar (finalize acc))
                    Flat col -> Flat <$> mapColumn @acc @a finalize col
                    Group _ ->
                        Left
                            ( InternalException
                                "broadcastFold unexpectedly produced a Group value"
                            )
                Flat col ->
                    Scalar . finalize <$> foldlColumn @b step seed col
                Group gs ->
                    Flat . fromVector
                        <$> V.mapM (fmap finalize . foldlColumn @b step seed) gs
-- Fast path: whole-frame sum of a clean Int/Double column, one parallel loop.
eval (FlatCtx df) (Agg (FoldAgg "sum" Nothing (_ :: a -> b -> a)) inner)
    | Just Refl <- testEquality (typeRep @a) (typeRep @b)
    , Just v <- fusedSum df inner =
        Right (Scalar v)
eval ctx expr@(Agg (FoldAgg _ Nothing (f :: a -> b -> a)) inner) =
    addContext expr $
        case testEquality (typeRep @a) (typeRep @b) of
            Nothing ->
                Left $
                    InternalException
                        "Type mismatch in seedless fold: \
                        \accumulator and element types must match"
            Just Refl -> do
                v <- eval @b ctx inner
                case v of
                    Scalar _ ->
                        Left $
                            InternalException
                                "fold1 requires at least one element"
                    Flat col ->
                        Scalar <$> foldl1Column @a f col
                    Group gs ->
                        Flat . fromVector
                            <$> V.mapM (foldl1Column @a f) gs

-- | The op's element function, with a fast path for @toDouble@.
fastUnaryFn ::
    forall b a.
    (Columnable b, Columnable a) =>
    T.Text -> (b -> a) -> (b -> a)
fastUnaryFn name f
    | name == "toDouble"
    , Just Refl <- testEquality (typeRep @a) (typeRep @Double) =
        integralToDouble @b f
    | otherwise = f

integralToDouble :: forall b. (Columnable b) => (b -> Double) -> b -> Double
integralToDouble f
    | Just Refl <- testEquality rb (typeRep @Int) = fromIntegral
    | Just Refl <- testEquality rb (typeRep @Int8) = fromIntegral
    | Just Refl <- testEquality rb (typeRep @Int16) = fromIntegral
    | Just Refl <- testEquality rb (typeRep @Int32) = fromIntegral
    | Just Refl <- testEquality rb (typeRep @Int64) = fromIntegral
    | Just Refl <- testEquality rb (typeRep @Word) = fromIntegral
    | otherwise = f
  where
    rb = typeRep @b

broadcastFold ::
    forall acc b.
    (Columnable acc) =>
    Ctx -> acc -> (acc -> b -> acc) -> b -> Value acc
broadcastFold (FlatCtx df) seed step x =
    let n = fst (dataframeDimensions df)
     in Scalar (iterateStep n step seed x)
broadcastFold (GroupCtx gdf) seed step x =
    let offs = offsets gdf
        ng = VU.length offs - 1
        results =
            V.generate ng $ \i ->
                let sz = offs VU.! (i + 1) - offs VU.! i
                 in iterateStep sz step seed x
     in Flat (fromVector results)

iterateStep :: Int -> (acc -> b -> acc) -> acc -> b -> acc
iterateStep n step = go n
  where
    go 0 !acc _ = acc
    go k !acc x = go (k - 1) (step acc x) x

-- | Apply a 'CollectAgg' function to a single column.
applyCollect ::
    forall v b a.
    (VG.Vector v b, Typeable v, Columnable b, Columnable a) =>
    (v b -> a) -> Column -> Either DataFrameException a
applyCollect f col = f <$> toVector @b @v col

-- | Result of interpreting an expression in a grouped context.
data AggregationResult a
    = UnAggregated Column
    | Aggregated (TypedColumn a)

-- | Interpret an expression against a flat 'DataFrame', producing a column.
interpret ::
    forall a.
    (Columnable a) =>
    DataFrame -> Expr a -> Either DataFrameException (TypedColumn a)
interpret df expr = do
    v <- eval (FlatCtx df) expr
    pure $ TColumn $ materialize @a (fst (dataframeDimensions df)) v

-- | Interpret an expression against a 'GroupedDataFrame'.
interpretAggregation ::
    forall a.
    (Columnable a) =>
    GroupedDataFrame ->
    Expr a ->
    Either DataFrameException (AggregationResult a)
interpretAggregation gdf expr = do
    v <- eval (GroupCtx gdf) expr
    case v of
        Scalar a ->
            Right $
                Aggregated $
                    TColumn $
                        broadcastScalar @a (numGroups gdf) a
        Flat col ->
            Right $ Aggregated $ TColumn col
        Group _ ->
            Right $ UnAggregated $ BoxedColumn @T.Text Nothing V.empty

-- | The mean of a clean Int or Double column. Empty gives NaN, like 'mean'.
fusedMean :: DataFrame -> T.Text -> Maybe Double
fusedMean df name = do
    UnboxedColumn Nothing (v :: VU.Vector t) <- getColumn name df
    total <- case testEquality (typeRep @t) (typeRep @Int) of
        Just Refl -> Just (K.parSumBy fromIntegral v)
        Nothing -> case testEquality (typeRep @t) (typeRep @Double) of
            Just Refl -> Just (K.parSumBy id v)
            Nothing -> Nothing
    pure $! if VU.null v then 0 / 0 else total / fromIntegral (VU.length v)

-- | The sum of a column, or of an arithmetic combination with a literal.
fusedSum :: forall a. (Columnable a) => DataFrame -> Expr a -> Maybe a
fusedSum df expr = case expr of
    Col name -> withClean name Nothing
    Binary op (Col name :: Expr c) (Lit (k :: b))
        | Just Refl <- testEquality (typeRep @c) (typeRep @a)
        , Just Refl <- testEquality (typeRep @b) (typeRep @a) ->
            withClean name (Just (binaryName op, k, False))
    Binary op (Lit (k :: c)) (Col name :: Expr b)
        | Just Refl <- testEquality (typeRep @c) (typeRep @a)
        , Just Refl <- testEquality (typeRep @b) (typeRep @a) ->
            withClean name (Just (binaryName op, k, True))
    _ -> Nothing
  where
    -- The operator is picked by name at a concrete element type.
    withClean :: T.Text -> Maybe (T.Text, a, Bool) -> Maybe a
    withClean name combo = do
        UnboxedColumn Nothing (v :: VU.Vector t) <- getColumn name df
        Refl <- testEquality (typeRep @t) (typeRep @a)
        case testEquality (typeRep @a) (typeRep @Int) of
            Just Refl -> sumInt combo v
            Nothing -> case testEquality (typeRep @a) (typeRep @Double) of
                Just Refl -> sumDouble combo v
                Nothing -> Nothing

sumInt :: Maybe (T.Text, Int, Bool) -> VU.Vector Int -> Maybe Int
sumInt combo v = case combo of
    Nothing -> Just (K.parSumBy id v)
    Just ("add", k, _) -> Just (K.parSumBy (+ k) v)
    Just ("mult", k, _) -> Just (K.parSumBy (* k) v)
    Just ("sub", k, False) -> Just (K.parSumBy (subtract k) v)
    Just ("sub", k, True) -> Just (K.parSumBy (k -) v)
    _ -> Nothing

sumDouble :: Maybe (T.Text, Double, Bool) -> VU.Vector Double -> Maybe Double
sumDouble combo v = case combo of
    Nothing -> Just (K.parSumBy id v)
    Just ("add", k, _) -> Just (K.parSumBy (+ k) v)
    Just ("mult", k, _) -> Just (K.parSumBy (* k) v)
    Just ("sub", k, False) -> Just (K.parSumBy (subtract k) v)
    Just ("sub", k, True) -> Just (K.parSumBy (k -) v)
    _ -> Nothing
