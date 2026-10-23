{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DisambiguateRecordFields #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE NoFieldSelectors #-}

module DataFrame.Internal.Expression (
    module DataFrame.Internal.Expression,
    module DataFrame.Internal.Expression.OpId,
) where

import qualified Data.Map.Strict as M
import Data.Maybe (fromMaybe)
import Data.String
import qualified Data.Text as T
import Data.Type.Equality (TestEquality (testEquality), type (:~:) (Refl))
import qualified Data.Vector.Generic as VG
import DataFrame.Internal.Column
import qualified DataFrame.Internal.Display.Pretty as P
import DataFrame.Internal.Expression.OpId
import Type.Reflection (Typeable, typeOf, typeRep)

-- | Operators are an open typeclass so the simplifier can match them by 'cast'.
class (Typeable op) => UnaryOp op where
    unaryFn :: op a b -> a -> b
    unaryId :: op a b -> OpRef UnOpId
    unarySymbol :: op a b -> Maybe T.Text
    unarySymbol _ = Nothing

class (Typeable op) => BinaryOp op where
    binaryFn :: op a b c -> a -> b -> c
    binaryId :: op a b c -> OpRef BinOpId
    binarySymbol :: op a b c -> Maybe T.Text
    binarySymbol _ = Nothing
    binaryCommutative :: op a b c -> Bool
    binaryCommutative _ = False
    binaryPrecedence :: op a b c -> Int
    binaryPrecedence _ = 9

-- | Display name of a unary operator.
unaryName :: (UnaryOp op) => op a b -> T.Text
unaryName = opRefName unOpName . unaryId

-- | Display name of a binary operator.
binaryName :: (BinaryOp op) => op a b c -> T.Text
binaryName = opRefName binOpName . binaryId

data UnUDF a b = MkUnaryOp
    { unaryFn :: a -> b
    , unaryId :: OpRef UnOpId
    , unarySymbol :: Maybe T.Text
    }

data BinUDF a b c = MkBinaryOp
    { binaryFn :: a -> b -> c
    , binaryId :: OpRef BinOpId
    , binarySymbol :: Maybe T.Text
    , binaryCommutative :: Bool
    , binaryPrecedence :: Int
    }

instance UnaryOp UnUDF where
    unaryFn (MkUnaryOp{unaryFn = f}) = f
    unaryId (MkUnaryOp{unaryId = n}) = n
    unarySymbol (MkUnaryOp{unarySymbol = s}) = s

instance BinaryOp BinUDF where
    binaryFn (MkBinaryOp{binaryFn = f}) = f
    binaryId (MkBinaryOp{binaryId = n}) = n
    binarySymbol (MkBinaryOp{binarySymbol = s}) = s
    binaryCommutative (MkBinaryOp{binaryCommutative = c}) = c
    binaryPrecedence (MkBinaryOp{binaryPrecedence = p}) = p

data AggStrategy a b where
    CollectAgg ::
        (VG.Vector v b, Typeable v) =>
        OpRef AggId -> (v b -> a) -> AggStrategy a b
    FoldAgg :: OpRef AggId -> Maybe a -> (a -> b -> a) -> AggStrategy a b
    MergeAgg ::
        (Columnable acc) =>
        OpRef AggId ->
        acc ->
        (acc -> b -> acc) ->
        (acc -> acc -> acc) ->
        (acc -> a) ->
        AggStrategy a b

aggStrategyId :: AggStrategy a b -> OpRef AggId
aggStrategyId (CollectAgg i _) = i
aggStrategyId (FoldAgg i _ _) = i
aggStrategyId (MergeAgg i _ _ _ _) = i

-- | Display name of an aggregation.
aggStrategyName :: AggStrategy a b -> T.Text
aggStrategyName = opRefName aggName . aggStrategyId

data Expr a where
    Col :: (Columnable a) => T.Text -> Expr a
    CastWith ::
        (Columnable a, Columnable b, Read a) =>
        T.Text ->
        OpRef CastId ->
        (Either String a -> b) ->
        Expr b
    CastExprWith ::
        (Columnable a, Columnable b, Columnable src, Read a) =>
        OpRef CastId ->
        (Either String a -> b) ->
        Expr src ->
        Expr b
    Lit :: (Columnable a) => a -> Expr a
    Unary ::
        (UnaryOp op, Columnable a, Columnable b) => op b a -> Expr b -> Expr a
    Binary ::
        (BinaryOp op, Columnable c, Columnable b, Columnable a) =>
        op c b a -> Expr c -> Expr b -> Expr a
    If :: (Columnable a) => Expr Bool -> Expr a -> Expr a -> Expr a
    Agg :: (Columnable a, Columnable b) => AggStrategy a b -> Expr b -> Expr a
    Over :: (Columnable a) => [T.Text] -> Expr a -> Expr a

data SomeExpr where
    SomeExpr :: (Columnable a) => Expr a -> SomeExpr

instance Show SomeExpr where
    show :: SomeExpr -> String
    show (SomeExpr expr) = show expr

toSomeExpr :: (Columnable a) => Expr a -> SomeExpr
toSomeExpr = SomeExpr

fromSomeExpr :: forall a. (Columnable a) => SomeExpr -> Maybe (Expr a)
fromSomeExpr (SomeExpr (expr :: Expr b)) = do
    Refl <- testEquality (typeRep @a) (typeRep @b)
    pure expr

type NamedExpr = (T.Text, SomeExpr)

toNamedExpr :: (Columnable a) => T.Text -> Expr a -> NamedExpr
toNamedExpr exprName expr = (exprName, SomeExpr expr)

instance (Num a, Columnable a) => Num (Expr a) where
    (+) :: Expr a -> Expr a -> Expr a
    (+) =
        Binary
            ( MkBinaryOp
                { binaryFn = (+)
                , binaryId = Builtin BAdd
                , binarySymbol = Just "+"
                , binaryCommutative = True
                , binaryPrecedence = 6
                }
            )

    (-) :: Expr a -> Expr a -> Expr a
    (-) =
        Binary
            ( MkBinaryOp
                { binaryFn = (-)
                , binaryId = Builtin BSub
                , binarySymbol = Just "-"
                , binaryCommutative = False
                , binaryPrecedence = 6
                }
            )

    (*) :: Expr a -> Expr a -> Expr a
    (*) =
        Binary
            ( MkBinaryOp
                { binaryFn = (*)
                , binaryId = Builtin BMult
                , binarySymbol = Just "*"
                , binaryCommutative = True
                , binaryPrecedence = 7
                }
            )

    fromInteger :: Integer -> Expr a
    fromInteger = Lit . fromInteger

    negate :: Expr a -> Expr a
    negate =
        Unary
            (MkUnaryOp{unaryFn = negate, unaryId = Builtin UNegate, unarySymbol = Nothing})

    abs :: (Num a) => Expr a -> Expr a
    abs =
        Unary (MkUnaryOp{unaryFn = abs, unaryId = Builtin UAbs, unarySymbol = Nothing})

    signum :: (Num a) => Expr a -> Expr a
    signum =
        Unary
            (MkUnaryOp{unaryFn = signum, unaryId = Builtin USignum, unarySymbol = Nothing})

add :: (Num a, Columnable a) => Expr a -> Expr a -> Expr a
add = (+)

sub :: (Num a, Columnable a) => Expr a -> Expr a -> Expr a
sub = (-)

mult :: (Num a, Columnable a) => Expr a -> Expr a -> Expr a
mult = (*)

instance (Fractional a, Columnable a) => Fractional (Expr a) where
    fromRational :: (Fractional a, Columnable a) => Rational -> Expr a
    fromRational = Lit . fromRational

    (/) :: (Fractional a, Columnable a) => Expr a -> Expr a -> Expr a
    (/) =
        Binary
            ( MkBinaryOp
                { binaryFn = (/)
                , binaryId = Builtin BDivide
                , binarySymbol = Just "/"
                , binaryCommutative = False
                , binaryPrecedence = 7
                }
            )

divide :: (Fractional a, Columnable a) => Expr a -> Expr a -> Expr a
divide = (/)

instance (IsString a, Columnable a) => IsString (Expr a) where
    fromString :: String -> Expr a
    fromString s = Lit (fromString s)

instance (Floating a, Columnable a) => Floating (Expr a) where
    pi :: (Floating a, Columnable a) => Expr a
    pi = Lit pi
    exp :: (Floating a, Columnable a) => Expr a -> Expr a
    exp =
        Unary (MkUnaryOp{unaryFn = exp, unaryId = Builtin UExp, unarySymbol = Nothing})
    sqrt :: (Floating a, Columnable a) => Expr a -> Expr a
    sqrt =
        Unary
            (MkUnaryOp{unaryFn = sqrt, unaryId = Builtin USqrt, unarySymbol = Nothing})
    (**) :: (Floating a, Columnable a) => Expr a -> Expr a -> Expr a
    (**) =
        Binary
            ( MkBinaryOp
                { binaryFn = (**)
                , binaryId = Builtin BExponentiate
                , binarySymbol = Just "**"
                , binaryCommutative = False
                , binaryPrecedence = 8
                }
            )
    log :: (Floating a, Columnable a) => Expr a -> Expr a
    log =
        Unary (MkUnaryOp{unaryFn = log, unaryId = Builtin ULog, unarySymbol = Nothing})
    logBase :: (Floating a, Columnable a) => Expr a -> Expr a -> Expr a
    logBase =
        Binary
            ( MkBinaryOp
                { binaryFn = logBase
                , binaryId = Builtin BLogBase
                , binarySymbol = Nothing
                , binaryCommutative = False
                , binaryPrecedence = 1
                }
            )
    sin :: (Floating a, Columnable a) => Expr a -> Expr a
    sin =
        Unary (MkUnaryOp{unaryFn = sin, unaryId = Builtin USin, unarySymbol = Nothing})
    cos :: (Floating a, Columnable a) => Expr a -> Expr a
    cos =
        Unary (MkUnaryOp{unaryFn = cos, unaryId = Builtin UCos, unarySymbol = Nothing})
    tan :: (Floating a, Columnable a) => Expr a -> Expr a
    tan =
        Unary (MkUnaryOp{unaryFn = tan, unaryId = Builtin UTan, unarySymbol = Nothing})
    asin :: (Floating a, Columnable a) => Expr a -> Expr a
    asin =
        Unary
            (MkUnaryOp{unaryFn = asin, unaryId = Builtin UAsin, unarySymbol = Nothing})
    acos :: (Floating a, Columnable a) => Expr a -> Expr a
    acos =
        Unary
            (MkUnaryOp{unaryFn = acos, unaryId = Builtin UAcos, unarySymbol = Nothing})
    atan :: (Floating a, Columnable a) => Expr a -> Expr a
    atan =
        Unary
            (MkUnaryOp{unaryFn = atan, unaryId = Builtin UAtan, unarySymbol = Nothing})
    sinh :: (Floating a, Columnable a) => Expr a -> Expr a
    sinh =
        Unary
            (MkUnaryOp{unaryFn = sinh, unaryId = Builtin USinh, unarySymbol = Nothing})
    cosh :: (Floating a, Columnable a) => Expr a -> Expr a
    cosh =
        Unary
            (MkUnaryOp{unaryFn = cosh, unaryId = Builtin UCosh, unarySymbol = Nothing})
    asinh :: (Floating a, Columnable a) => Expr a -> Expr a
    asinh =
        Unary
            (MkUnaryOp{unaryFn = asinh, unaryId = Builtin UAsinh, unarySymbol = Nothing})
    acosh :: (Floating a, Columnable a) => Expr a -> Expr a
    acosh =
        Unary
            (MkUnaryOp{unaryFn = acosh, unaryId = Builtin UAcosh, unarySymbol = Nothing})
    atanh :: (Floating a, Columnable a) => Expr a -> Expr a
    atanh =
        Unary
            (MkUnaryOp{unaryFn = atanh, unaryId = Builtin UAtanh, unarySymbol = Nothing})

instance (Show a) => Show (Expr a) where
    show :: Expr a -> String
    show (Col name) = "(col @" ++ show (typeRep @a) ++ " " ++ show name ++ ")"
    show (CastWith name tag _) =
        "(castWith " ++ show (opRefName castName tag) ++ " " ++ show name ++ ")"
    show (CastExprWith tag _ inner) =
        "(castExprWith "
            ++ show (opRefName castExprName tag)
            ++ " "
            ++ show inner
            ++ ")"
    show (Lit value) = "(lit (" ++ show value ++ "))"
    show (If cond l r) = "(ifThenElse " ++ show cond ++ " " ++ show l ++ " " ++ show r ++ ")"
    show (Unary op value) = "(" ++ T.unpack (unaryName op) ++ " " ++ show value ++ ")"
    show (Binary op a b) = "(" ++ T.unpack (binaryName op) ++ " " ++ show a ++ " " ++ show b ++ ")"
    show (Agg strat expr) =
        "(" ++ T.unpack (aggStrategyName strat) ++ " " ++ show expr ++ ")"
    show (Over keys inner) = "(over " ++ show keys ++ " " ++ show inner ++ ")"

normalize :: (Show a, Typeable a) => Expr a -> Expr a
normalize expr = case expr of
    Col name -> Col name
    CastWith n t f -> CastWith n t f
    CastExprWith t f e -> CastExprWith t f (normalize e)
    Lit val -> Lit val
    If cond th el -> If (normalize cond) (normalize th) (normalize el)
    Unary op e -> Unary op (normalize e)
    Binary op e1 e2
        | binaryCommutative op ->
            let n1 = normalize e1
                n2 = normalize e2
             in case testEquality (typeOf n1) (typeOf n2) of
                    Nothing -> expr
                    Just Refl ->
                        if compareExpr n1 n2 == GT
                            then Binary op n2 n1
                            else Binary op n1 n2
        | otherwise -> Binary op (normalize e1) (normalize e2)
    Agg strat e -> Agg strat (normalize e)
    Over keys inner -> Over keys (normalize inner)

-- Compare expressions for ordering (used in normalization)
compareExpr :: Expr a -> Expr a -> Ordering
compareExpr e1 e2 = compare (exprKey e1) (exprKey e2)
  where
    exprKey :: Expr a -> String
    exprKey (Col name) = "0:" ++ T.unpack name
    exprKey (CastWith name tag _) = "0CW:" ++ T.unpack name ++ ":" ++ idKey tag
    exprKey (CastExprWith tag _ _) = "0CE:" ++ idKey tag
    exprKey (Lit val) = "1:" ++ show val
    exprKey (If c t e) = "2:" ++ exprKey c ++ exprKey t ++ exprKey e
    exprKey (Unary op e) = "3:" ++ idKey (unaryId op) ++ exprKey e
    exprKey (Binary op e1' e2') =
        "4:" ++ idKey (binaryId op) ++ exprKey e1' ++ exprKey e2'
    exprKey (Agg strat e) = "5:" ++ idKey (aggStrategyId strat) ++ exprKey e
    exprKey (Over keys e) = "6:over:" ++ show keys ++ exprKey e
    idKey :: (Show k) => OpRef k -> String
    idKey (Builtin k) = "b:" ++ show k
    idKey (Named n) = "n:" ++ T.unpack n
    idKey (Anonymous l) = "a:" ++ T.unpack l

-- | A key equal only for provably equal expressions; Nothing if anonymous.
structuralKey :: (Columnable a) => Expr a -> Maybe String
structuralKey = go . normalize
  where
    go :: forall x. Expr x -> Maybe String
    go e = case e of
        Col name -> Just ("c" ++ show name ++ "@" ++ show (typeRep @x))
        CastWith name tag _ ->
            idKey tag >>= \k -> Just ("w" ++ show name ++ k ++ "@" ++ show (typeRep @x))
        CastExprWith tag _ inner -> idKey tag >>= \k -> wrap ("x" ++ k) [go inner]
        Lit v -> Just ("l" ++ show v ++ "@" ++ show (typeRep @x))
        If c t f -> wrap "if" [go c, go t, go f]
        Unary op inner -> idKey (unaryId op) >>= \k -> wrap k [go inner]
        Binary op l r -> idKey (binaryId op) >>= \k -> wrap k [go l, go r]
        Agg strat inner ->
            idKey (aggStrategyId strat) >>= \k -> wrap ("agg" ++ k) [go inner]
        Over keys inner -> wrap ("over" ++ show keys) [go inner]
    wrap tag parts = (\ps -> tag ++ "(" ++ concatMap (++ ",") ps ++ ")") <$> sequence parts
    idKey :: (Show k) => OpRef k -> Maybe String
    idKey (Builtin k) = Just ("b" ++ show k)
    idKey (Named n) = Just ("n" ++ show n)
    idKey (Anonymous _) = Nothing

-- | Provable equality; anonymous operators are never equal.
eqExpr :: forall a. (Columnable a) => Expr a -> Expr a -> Bool
eqExpr l r = eqNormalized (normalize l) (normalize r)

eqNormalized :: forall a. Expr a -> Expr a -> Bool
eqNormalized = go
  where
    sub :: forall b c. (Columnable b, Columnable c) => Expr b -> Expr c -> Bool
    sub e1 e2 = case testEquality (typeRep @b) (typeRep @c) of
        Just Refl -> go e1 e2
        Nothing -> False
    go :: forall x. Expr x -> Expr x -> Bool
    go (Col n1) (Col n2) = n1 == n2
    go (CastWith n1 t1 _) (CastWith n2 t2 _) = n1 == n2 && sameOp t1 t2
    go (CastExprWith t1 _ e1) (CastExprWith t2 _ e2) = sameOp t1 t2 && e1 `sub` e2
    go (Lit v1) (Lit v2) = v1 == v2
    go (If c1 t1 e1) (If c2 t2 e2) = go c1 c2 && go t1 t2 && go e1 e2
    go (Unary op1 e1) (Unary op2 e2) =
        sameOp (unaryId op1) (unaryId op2) && e1 `sub` e2
    go (Binary op1 e1a e1b) (Binary op2 e2a e2b) =
        sameOp (binaryId op1) (binaryId op2)
            && e1a `sub` e2a
            && e1b `sub` e2b
    go (Agg s1 e1) (Agg s2 e2) =
        sameStrategy s1 s2
            && sameOp (aggStrategyId s1) (aggStrategyId s2)
            && e1 `sub` e2
    go (Over k1 e1) (Over k2 e2) = k1 == k2 && go e1 e2
    go _ _ = False
    sameStrategy :: AggStrategy p q -> AggStrategy p' q' -> Bool
    sameStrategy CollectAgg{} CollectAgg{} = True
    sameStrategy FoldAgg{} FoldAgg{} = True
    sameStrategy MergeAgg{} MergeAgg{} = True
    sameStrategy _ _ = False

replaceExpr ::
    forall a b.
    (Columnable a, Columnable b) =>
    Expr a -> Expr a -> Expr b -> Expr b
replaceExpr old new expr = case testEquality (typeRep @a) (typeRep @b) of
    Just Refl -> if eqExpr old expr then new else replace'
    Nothing -> replace'
  where
    replace' = case expr of
        (Col _) -> expr
        (CastWith{}) -> expr
        (CastExprWith t f e) -> CastExprWith t f (replaceExpr old new e)
        (Lit _) -> expr
        (If cond l r) ->
            If (replaceExpr old new cond) (replaceExpr old new l) (replaceExpr old new r)
        (Unary op value) -> Unary op (replaceExpr old new value)
        (Binary op l r) -> Binary op (replaceExpr old new l) (replaceExpr old new r)
        (Agg op inner) -> Agg op (replaceExpr old new inner)
        (Over keys inner) -> Over keys (replaceExpr old new inner)

-- | Substitute 'Col' references from a name-to-expression map in one pass.
substituteColumns ::
    forall a. (Columnable a) => M.Map T.Text SomeExpr -> Expr a -> Expr a
substituteColumns subs = go
  where
    go :: forall b. (Columnable b) => Expr b -> Expr b
    go e@(Col name) = case M.lookup name subs of
        Nothing -> e
        Just (SomeExpr (repl :: Expr c)) -> case testEquality (typeRep @b) (typeRep @c) of
            Just Refl -> repl
            Nothing ->
                error $
                    "substituteColumns: type mismatch for column "
                        ++ show name
                        ++ "; column has type "
                        ++ show (typeRep @b)
                        ++ " but replacement has type "
                        ++ show (typeRep @c)
    go e@(CastWith{}) = e
    go (CastExprWith t f e) = CastExprWith t f (go e)
    go e@(Lit _) = e
    go (If cond l r) = If (go cond) (go l) (go r)
    go (Unary op value) = Unary op (go value)
    go (Binary op l r) = Binary op (go l) (go r)
    go (Agg op inner) = Agg op (go inner)
    go (Over keys inner) = Over keys (go inner)

eSize :: Expr a -> Int
eSize (Col _) = 1
eSize (CastWith{}) = 1
eSize (CastExprWith _ _ e) = 1 + eSize e
eSize (Lit _) = 1
eSize (If c l r) = 1 + eSize c + eSize l + eSize r
eSize (Unary _ e) = 1 + eSize e
eSize (Binary _ l r) = 1 + eSize l + eSize r
eSize (Agg _strategy expr) = eSize expr + 1
eSize (Over _ inner) = 1 + eSize inner

getColumns :: Expr a -> [T.Text]
getColumns (Col cName) = [cName]
getColumns (CastWith name _ _) = [name]
getColumns (CastExprWith _ _ e) = getColumns e
getColumns _expr@(Lit _) = []
getColumns (If cond l r) = getColumns cond <> getColumns l <> getColumns r
getColumns (Unary _op value) = getColumns value
getColumns (Binary _op l r) = getColumns l <> getColumns r
getColumns (Agg _strategy expr) = getColumns expr
getColumns (Over keys inner) = keys <> getColumns inner

-- | Render an expression as readable pseudo-code at the default width.
prettyPrint :: Expr a -> String
prettyPrint = prettyPrintWidth P.defaultWidth

-- | Render an expression as readable, width-aware pseudo-code.
prettyPrintWidth :: Int -> Expr a -> String
prettyPrintWidth width = P.render width . toDoc 0
  where
    toDoc :: Int -> Expr x -> P.Doc
    toDoc prec expr = case expr of
        Col name -> P.text (T.unpack name)
        CastWith name _ _ -> P.text (T.unpack name)
        CastExprWith tag _ inner ->
            P.text (T.unpack (opRefName castExprName tag)) <> P.parens (toDoc 0 inner)
        Lit value -> P.text (show value)
        If{} -> renderIf prec expr
        Unary op arg ->
            let fn = fromMaybe (unaryName op) (unarySymbol op)
             in P.text (T.unpack fn) <> P.parens (toDoc 0 arg)
        Binary op l r -> case binarySymbol op of
            Just sym -> renderBinary prec op (T.unpack sym) l r
            Nothing ->
                P.text (T.unpack (binaryName op))
                    <> P.parens (toDoc 0 l <> P.text ", " <> toDoc 0 r)
        Agg strat arg ->
            P.text (T.unpack (aggStrategyName strat)) <> P.parens (toDoc 0 arg)
        Over keys inner ->
            toDoc 0 inner <> P.text (".over(" ++ show (map T.unpack keys) ++ ")")

    renderBinary ::
        (BinaryOp op) => Int -> op c b a -> String -> Expr c -> Expr b -> P.Doc
    renderBinary prec op sym l r =
        let p = binaryPrecedence op
            body
                | binaryCommutative op =
                    let operands =
                            flattenChain (binaryName op) p l
                                ++ flattenChain (binaryName op) p r
                     in case operands of
                            (o0 : os@(_ : _)) ->
                                P.group
                                    ( o0
                                        <> P.nest
                                            2
                                            (P.hcat [P.line <> P.text sym P.<+> o | o <- os])
                                    )
                            _ -> toDoc p l P.<+> P.text sym P.<+> toDoc p r
                | otherwise =
                    P.group (toDoc p l <> P.nest 2 (P.line <> P.text sym P.<+> toDoc p r))
         in if prec > p
                then P.parens body
                else if prec >= 1 && prec < p then P.parensWhenBroken body else body

    flattenChain :: T.Text -> Int -> Expr x -> [P.Doc]
    flattenChain name p e = case e of
        Binary op' l' r'
            | binaryName op' == name
            , binaryPrecedence op' == p
            , binaryCommutative op' ->
                flattenChain name p l' ++ flattenChain name p r'
        _ -> [toDoc p e]

    renderIf :: Int -> Expr x -> P.Doc
    renderIf prec (If c t e) =
        let blk =
                P.text "if" P.<+> P.nest 3 (P.group (toDoc 0 c))
                    <> P.nest indentWidth (P.hardline <> toDoc 0 t)
                    <> P.hardline
                    <> renderElse e
         in if prec > 0 then P.parens (P.nest 2 blk) else blk
    renderIf _ _ = mempty

    renderElse :: Expr x -> P.Doc
    renderElse (If c t e) =
        P.text "else if" P.<+> P.nest 5 (P.group (toDoc 0 c))
            <> P.nest indentWidth (P.hardline <> toDoc 0 t)
            <> P.hardline
            <> renderElse e
    renderElse other =
        P.text "else" <> P.nest indentWidth (P.hardline <> toDoc 0 other)
    indentWidth :: Int
    indentWidth = 4
