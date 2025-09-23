{-# LANGUAGE BangPatterns #-}
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

module DataFrame.Internal.Expression where

import Control.Exception (throw)
import qualified Data.Map as M
import Data.Maybe (fromMaybe)
import qualified Data.Text as T
import Data.Type.Equality (TestEquality (testEquality), type (:~:) (Refl))
import qualified Data.Vector as V
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU
import DataFrame.Errors (DataFrameException (ColumnNotFoundException))
import DataFrame.Internal.Column
import DataFrame.Internal.DataFrame
import DataFrame.Internal.Types
import Type.Reflection (Typeable, typeRep)

data Expr a where
    Col :: (Columnable a) => T.Text -> Expr a
    Lit :: (Columnable a) => a -> Expr a
    If ::
        (Columnable a) =>
        Expr Bool ->
        Expr a ->
        Expr a ->
        Expr a
    UnaryOp ::
        ( Columnable a
        , Columnable b
        ) =>
        T.Text -> -- Operation name
        (b -> a) ->
        Expr b ->
        Expr a
    BinaryOp ::
        ( Columnable c
        , Columnable b
        , Columnable a
        ) =>
        T.Text -> -- operation name
        (c -> b -> a) ->
        Expr c ->
        Expr b ->
        Expr a
    AggVector ::
        ( VG.Vector v b
        , Typeable v
        , Columnable a
        , Columnable b
        ) =>
        Expr b ->
        T.Text -> -- Operation name
        (v b -> a) ->
        Expr a
    AggReduce ::
        (Columnable a) =>
        Expr a ->
        T.Text -> -- Operation name
        (forall a. (Columnable a) => a -> a -> a) ->
        Expr a
    AggNumericVector ::
        ( Columnable a
        , Columnable b
        , VU.Unbox a
        , VU.Unbox b
        , Num a
        , Num b
        ) =>
        Expr b ->
        T.Text -> -- Operation name
        (VU.Vector b -> a) ->
        Expr a
    AggFold ::
        forall a b.
        (Columnable a, Columnable b) =>
        Expr b ->
        T.Text -> -- Operation name
        a ->
        (a -> b -> a) ->
        Expr a

data UExpr where
    Wrap :: (Columnable a) => Expr a -> UExpr

interpret :: forall a. (Columnable a) => DataFrame -> Expr a -> TypedColumn a
interpret df (Lit value) = TColumn $ fromVector $ V.replicate (fst $ dataframeDimensions df) value
interpret df (Col name) = case getColumn name df of
    Nothing -> throw $ ColumnNotFoundException name "" (M.keys $ columnIndices df)
    Just col -> TColumn col
interpret df (If cond l r) =
    let
        (TColumn conditions) = interpret @Bool df cond
        (TColumn left) = interpret @a df l
        (TColumn right) = interpret @a df r
     in
        TColumn $
            fromMaybe (error "zipWithColumns returned nothing") $
                zipWithColumns
                    (\(c :: Bool) (l' :: a, r' :: a) -> if c then l' else r')
                    conditions
                    (zipColumns left right)
interpret df (UnaryOp _ (f :: c -> d) value) =
    let
        (TColumn value') = interpret @c df value
     in
        -- TODO: Handle this gracefully.
        TColumn $ fromMaybe (error "mapColumn returned nothing") (mapColumn f value')
interpret df (BinaryOp _ (f :: c -> d -> e) (Lit left) (Lit right)) =
    TColumn $ fromVector $ V.replicate (fst $ dataframeDimensions df) (f left right)
interpret df (BinaryOp _ (f :: c -> d -> e) (Lit left) right) =
    let
        (TColumn right') = interpret @d df right
     in
        TColumn $
            fromMaybe (error "mapColumn returned nothing") (mapColumn (f left) right')
interpret df (BinaryOp _ (f :: c -> d -> e) left (Lit right)) =
    let
        (TColumn left') = interpret @c df left
     in
        TColumn $
            fromMaybe (error "mapColumn returned nothing") (mapColumn (`f` right) left')
interpret df (BinaryOp _ (f :: c -> d -> e) left right) =
    let
        (TColumn left') = interpret @c df left
        (TColumn right') = interpret @d df right
     in
        TColumn $
            fromMaybe (error "mapColumn returned nothing") (zipWithColumns f left' right')
interpret df (AggReduce expr op (f :: forall a. (Columnable a) => a -> a -> a)) =
    let
        (TColumn column) = interpret @a df expr
     in
        case headColumn @a column of
            Nothing -> error "Invalid operation"
            Just h -> case ifoldlColumn (\acc _ v -> f acc v) h column of
                Nothing -> error "Invalid operation"
                Just value -> TColumn $ fromVector $ V.replicate (fst $ dataframeDimensions df) value
interpret df (AggNumericVector expr op (f :: VU.Vector b -> c)) =
    let
        (TColumn column) = interpret @b df expr
     in
        case column of
            (UnboxedColumn (v :: VU.Vector d)) -> case testEquality (typeRep @d) (typeRep @b) of
                Just Refl -> TColumn $ fromVector $ V.replicate (fst $ dataframeDimensions df) (f v)
                Nothing -> error "Invalid operation"
            _ -> error "Invalid operation"
interpret df (AggFold expr op start (f :: (a -> b -> a))) =
    let
        (TColumn column) = interpret @b df expr
     in
        case headColumn @a column of
            Nothing -> error "Invalid operation"
            Just h -> case ifoldlColumn (\acc _ v -> f acc v) h column of
                Nothing -> error "Invalid operation"
                Just value -> TColumn $ fromVector $ V.replicate (fst $ dataframeDimensions df) value
interpret _ expr = error ("Invalid operation for dataframe: " ++ show expr)

interpretAggregation ::
    forall a. (Columnable a) => GroupedDataFrame -> Expr a -> TypedColumn a
interpretAggregation gdf (Lit value) = TColumn $ fromVector $ V.replicate (VG.length (offsets gdf) - 1) value
interpretAggregation gdf@(Grouped df names indices os) (Col name) = case getColumn name df of
    Nothing -> throw $ ColumnNotFoundException name "" (M.keys $ columnIndices df)
    Just col ->
        TColumn $ atIndicesStable (VG.map (indices `VG.unsafeIndex`) (VG.init os)) col
interpretAggregation gdf (UnaryOp _ (f :: c -> d) expr) =
    let
        (TColumn value) = interpretAggregation @c gdf expr
     in
        case mapColumn f value of
            Nothing -> error "Type error in interpretation"
            Just col -> TColumn col
interpretAggregation gdf (If cond l r) =
    let
        (TColumn conditions) = interpretAggregation @Bool gdf cond
        (TColumn left) = interpretAggregation @a gdf l
        (TColumn right) = interpretAggregation @a gdf r
     in
        TColumn $
            fromMaybe (error "zipWithColumns returned nothing") $
                zipWithColumns
                    (\(c :: Bool) (l' :: a, r' :: a) -> if c then l' else r')
                    conditions
                    (zipColumns left right)
interpretAggregation gdf (BinaryOp _ (f :: c -> d -> e) left right) =
    let
        (TColumn left') = interpretAggregation @c gdf left
        (TColumn right') = interpretAggregation @d gdf right
     in
        case zipWithColumns f left' right' of
            Nothing -> error "Type error in binary operation"
            Just col -> TColumn col
interpretAggregation gdf@(Grouped df names indices os) (AggVector expr op (f :: v b -> c)) =
    case unwrapTypedColumn (interpretAggregation @b gdf expr) of
        BoxedColumn (col :: V.Vector d) -> case testEquality (typeRep @b) (typeRep @d) of
            Nothing -> error "Type mismatch"
            Just Refl -> case testEquality (typeRep @v) (typeRep @V.Vector) of
                Nothing -> error "Container mismatch"
                Just Refl ->
                    TColumn $
                        fromVector $
                            V.generate
                                (VG.length os - 1)
                                ( \i ->
                                    f
                                        ( V.generate
                                            (os `VG.unsafeIndex` (i + 1) - (os `VG.unsafeIndex` i))
                                            ( \j ->
                                                col `VG.unsafeIndex` (indices `VG.unsafeIndex` (j + (os `VG.unsafeIndex` i)))
                                            )
                                        )
                                )
        UnboxedColumn (col :: VU.Vector d) -> case testEquality (typeRep @b) (typeRep @d) of
            Nothing -> error "Type mismatch"
            Just Refl -> case testEquality (typeRep @v) (typeRep @VU.Vector) of
                Nothing -> error "Container mismatch"
                Just Refl ->
                    case sUnbox @c of
                        SFalse ->
                            TColumn $
                                fromVector $
                                    V.generate
                                        (VG.length os - 1)
                                        ( \i ->
                                            f
                                                ( VU.generate
                                                    (os `VG.unsafeIndex` (i + 1) - (os `VG.unsafeIndex` i))
                                                    ( \j ->
                                                        col `VG.unsafeIndex` (indices `VG.unsafeIndex` (j + (os `VG.unsafeIndex` i)))
                                                    )
                                                )
                                        )
                        STrue ->
                            TColumn $
                                fromUnboxedVector $
                                    VU.generate
                                        (VG.length os - 1)
                                        ( \i ->
                                            f
                                                ( VU.generate
                                                    (os `VG.unsafeIndex` (i + 1) - (os `VG.unsafeIndex` i))
                                                    ( \j ->
                                                        col `VG.unsafeIndex` (indices `VG.unsafeIndex` (j + (os `VG.unsafeIndex` i)))
                                                    )
                                                )
                                        )
        OptionalColumn (col :: V.Vector d) -> case testEquality (typeRep @b) (typeRep @d) of
            Nothing -> error "Type mismatch"
            Just Refl -> case testEquality (typeRep @v) (typeRep @V.Vector) of
                Nothing -> error "Container mismatch"
                Just Refl ->
                    TColumn $
                        fromVector $
                            V.generate
                                (VG.length os - 1)
                                ( \i ->
                                    f
                                        ( V.generate
                                            (os `VG.unsafeIndex` (i + 1) - (os `VG.unsafeIndex` i))
                                            ( \j ->
                                                col `VG.unsafeIndex` (indices `VG.unsafeIndex` (j + (os `VG.unsafeIndex` i)))
                                            )
                                        )
                                )
interpretAggregation gdf@(Grouped df names indices os) (AggReduce expr op (f :: forall a. (Columnable a) => a -> a -> a)) = case unwrapTypedColumn (interpretAggregation @a gdf expr) of
    BoxedColumn col -> TColumn $
        fromVector $
            VG.generate (VG.length os - 1) $ \g ->
                let !start = os `VG.unsafeIndex` g
                    !end = os `VG.unsafeIndex` (g + 1)
                 in go (col `VG.unsafeIndex` (indices `VG.unsafeIndex` start)) (start + 1) end
      where
        {-# INLINE go #-}
        go !acc j e
            | j == e = acc
            | otherwise =
                let !x = col `VG.unsafeIndex` (indices `VG.unsafeIndex` j)
                 in go (f acc x) (j + 1) e
    UnboxedColumn col -> case sUnbox @a of
        SFalse -> TColumn $
            fromVector $
                VG.generate (VG.length os - 1) $ \g ->
                    let !start = os `VG.unsafeIndex` g
                        !end = os `VG.unsafeIndex` (g + 1)
                     in go (col `VG.unsafeIndex` (indices `VG.unsafeIndex` start)) (start + 1) end
          where
            {-# INLINE go #-}
            go !acc j e
                | j == e = acc
                | otherwise =
                    let !x = col `VG.unsafeIndex` (indices `VG.unsafeIndex` j)
                     in go (f acc x) (j + 1) e
        STrue -> TColumn $
            fromUnboxedVector $
                VG.generate (VG.length os - 1) $ \g ->
                    let !start = os `VG.unsafeIndex` g
                        !end = os `VG.unsafeIndex` (g + 1)
                     in go (col `VG.unsafeIndex` (indices `VG.unsafeIndex` start)) (start + 1) end
          where
            {-# INLINE go #-}
            go !acc j e
                | j == e = acc
                | otherwise =
                    let !x = col `VG.unsafeIndex` (indices `VG.unsafeIndex` j)
                     in go (f acc x) (j + 1) e
    OptionalColumn col -> TColumn $
        fromVector $
            VG.generate (VG.length os - 1) $ \g ->
                let !start = os `VG.unsafeIndex` g
                    !end = os `VG.unsafeIndex` (g + 1)
                 in go (col `VG.unsafeIndex` (indices `VG.unsafeIndex` start)) (start + 1) end
      where
        {-# INLINE go #-}
        go !acc j e
            | j == e = acc
            | otherwise =
                let !x = col `VG.unsafeIndex` (indices `VG.unsafeIndex` j)
                 in go (f acc x) (j + 1) e
interpretAggregation gdf@(Grouped df names indices os) (AggFold expr op s (f :: (a -> b -> a))) = case unwrapTypedColumn (interpretAggregation @b gdf expr) of
    BoxedColumn (col :: V.Vector c) -> case testEquality (typeRep @b) (typeRep @c) of
        Nothing -> error "Type mismatch"
        Just Refl -> TColumn $
            fromVector $
                VG.generate (VG.length os - 1) $ \g ->
                    let !start = os `VG.unsafeIndex` g
                        !end = os `VG.unsafeIndex` (g + 1)
                     in go s start end
          where
            {-# INLINE go #-}
            go !acc j e
                | j == e = acc
                | otherwise =
                    let !x = col `VG.unsafeIndex` (indices `VG.unsafeIndex` j)
                     in go (f acc x) (j + 1) e
    UnboxedColumn (col :: VU.Vector c) -> case testEquality (typeRep @b) (typeRep @c) of
        Nothing -> error "Type mismatch"
        Just Refl -> case sUnbox @a of
            SFalse -> TColumn $
                fromVector $
                    VG.generate (VG.length os - 1) $ \g ->
                        let !start = os `VG.unsafeIndex` g
                            !end = os `VG.unsafeIndex` (g + 1)
                         in go s start end
              where
                {-# INLINE go #-}
                go !acc j e
                    | j == e = acc
                    | otherwise =
                        let !x = col `VG.unsafeIndex` (indices `VG.unsafeIndex` j)
                         in go (f acc x) (j + 1) e
            STrue -> TColumn $
                fromUnboxedVector $
                    VG.generate (VG.length os - 1) $ \g ->
                        let !start = os `VG.unsafeIndex` g
                            !end = os `VG.unsafeIndex` (g + 1)
                         in go s start end
          where
            {-# INLINE go #-}
            go !acc j e
                | j == e = acc
                | otherwise =
                    let !x = col `VG.unsafeIndex` (indices `VG.unsafeIndex` j)
                     in go (f acc x) (j + 1) e
    OptionalColumn (col :: V.Vector c) -> case testEquality (typeRep @b) (typeRep @c) of
        Nothing -> error "Type mismatch"
        Just Refl -> TColumn $
            fromVector $
                VG.generate (VG.length os - 1) $ \g ->
                    let !start = os `VG.unsafeIndex` g
                        !end = os `VG.unsafeIndex` (g + 1)
                     in go s start end
          where
            {-# INLINE go #-}
            go !acc j e
                | j == e = acc
                | otherwise =
                    let !x = col `VG.unsafeIndex` (indices `VG.unsafeIndex` j)
                     in go (f acc x) (j + 1) e
interpretAggregation gdf@(Grouped df names indices os) (AggNumericVector expr op (f :: VU.Vector b -> c)) = case unwrapTypedColumn (interpretAggregation @b gdf expr) of
    UnboxedColumn (col :: VU.Vector d) -> case testEquality (typeRep @b) (typeRep @d) of
        Nothing -> case testEquality (typeRep @d) (typeRep @Int) of
            Nothing -> error "Type mismatch"
            Just Refl -> case sUnbox @c of
                SFalse ->
                    TColumn $
                        fromVector $
                            V.generate
                                (VG.length os - 1)
                                ( \i ->
                                    f
                                        ( VU.generate
                                            (os `VG.unsafeIndex` (i + 1) - (os `VG.unsafeIndex` i))
                                            ( \j ->
                                                fromIntegral
                                                    (col `VG.unsafeIndex` (indices `VG.unsafeIndex` (j + (os `VG.unsafeIndex` i))))
                                            )
                                        )
                                )
                STrue ->
                    TColumn $
                        fromUnboxedVector $
                            VU.generate
                                (VG.length os - 1)
                                ( \i ->
                                    f
                                        ( VU.generate
                                            (os `VG.unsafeIndex` (i + 1) - (os `VG.unsafeIndex` i))
                                            ( \j ->
                                                fromIntegral
                                                    (col `VG.unsafeIndex` (indices `VG.unsafeIndex` (j + (os `VG.unsafeIndex` i))))
                                            )
                                        )
                                )
        Just Refl -> case sNumeric @d of
            SFalse ->
                error $ "Cannot apply numeric aggregation to non-numeric column: " ++ show expr
            STrue -> case sUnbox @c of
                SFalse ->
                    TColumn $
                        fromVector $
                            V.generate
                                (VG.length os - 1)
                                ( \i ->
                                    f
                                        ( VU.generate
                                            (os `VG.unsafeIndex` (i + 1) - (os `VG.unsafeIndex` i))
                                            ( \j ->
                                                col `VG.unsafeIndex` (indices `VG.unsafeIndex` (j + (os `VG.unsafeIndex` i)))
                                            )
                                        )
                                )
                STrue ->
                    TColumn $
                        fromUnboxedVector $
                            VU.generate
                                (VG.length os - 1)
                                ( \i ->
                                    f
                                        ( VU.generate
                                            (os `VG.unsafeIndex` (i + 1) - (os `VG.unsafeIndex` i))
                                            ( \j ->
                                                col `VG.unsafeIndex` (indices `VG.unsafeIndex` (j + (os `VG.unsafeIndex` i)))
                                            )
                                        )
                                )
    _ -> error $ "Cannot apply numeric aggregation to non-numeric: " ++ show expr

instance (Num a, Columnable a) => Num (Expr a) where
    (+) :: Expr a -> Expr a -> Expr a
    (+) = BinaryOp "add" (+)

    (*) :: Expr a -> Expr a -> Expr a
    (*) = BinaryOp "mult" (*)

    fromInteger :: Integer -> Expr a
    fromInteger = Lit . fromInteger

    negate :: Expr a -> Expr a
    negate = UnaryOp "negate" negate

    abs :: (Num a) => Expr a -> Expr a
    abs = UnaryOp "abs" abs

    signum :: (Num a) => Expr a -> Expr a
    signum = UnaryOp "signum" signum

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
    (/) = BinaryOp "divide" (/)

divide :: (Fractional a, Columnable a) => Expr a -> Expr a -> Expr a
divide = (/)

instance (Floating a, Columnable a) => Floating (Expr a) where
    pi :: (Floating a, Columnable a) => Expr a
    pi = Lit pi
    exp :: (Floating a, Columnable a) => Expr a -> Expr a
    exp = UnaryOp "exp" exp
    log :: (Floating a, Columnable a) => Expr a -> Expr a
    log = UnaryOp "log" log
    sin :: (Floating a, Columnable a) => Expr a -> Expr a
    sin = UnaryOp "sin" sin
    cos :: (Floating a, Columnable a) => Expr a -> Expr a
    cos = UnaryOp "cos" cos
    asin :: (Floating a, Columnable a) => Expr a -> Expr a
    asin = UnaryOp "asin" asin
    acos :: (Floating a, Columnable a) => Expr a -> Expr a
    acos = UnaryOp "acos" acos
    atan :: (Floating a, Columnable a) => Expr a -> Expr a
    atan = UnaryOp "atan" atan
    sinh :: (Floating a, Columnable a) => Expr a -> Expr a
    sinh = UnaryOp "sinh" sinh
    cosh :: (Floating a, Columnable a) => Expr a -> Expr a
    cosh = UnaryOp "cosh" cosh
    asinh :: (Floating a, Columnable a) => Expr a -> Expr a
    asinh = UnaryOp "asinh" sinh
    acosh :: (Floating a, Columnable a) => Expr a -> Expr a
    acosh = UnaryOp "acosh" acosh
    atanh :: (Floating a, Columnable a) => Expr a -> Expr a
    atanh = UnaryOp "atanh" atanh

instance (Show a) => Show (Expr a) where
    show :: forall a. (Show a) => Expr a -> String
    show (Col name) = "(col @" ++ show (typeRep @a) ++ " " ++ show name ++ ")"
    show (Lit value) = "(lit (" ++ show value ++ "))"
    show (If cond l r) = "(ifThenElse " ++ show cond ++ " " ++ show l ++ " " ++ show r ++ ")"
    show (UnaryOp name f value) = "(" ++ T.unpack name ++ " " ++ show value ++ ")"
    show (BinaryOp name f a b) = "(" ++ T.unpack name ++ " " ++ show a ++ " " ++ show b ++ ")"
    show (AggNumericVector expr op _) = "(" ++ T.unpack op ++ " " ++ show expr ++ ")"
    show (AggVector expr op _) = "(" ++ T.unpack op ++ " " ++ show expr ++ ")"
    show (AggReduce expr op _) = "(" ++ T.unpack op ++ " " ++ show expr ++ ")"
    show (AggFold expr op _ _) = "(" ++ T.unpack op ++ " " ++ show expr ++ ")"

instance (Eq a, Show a) => Eq (Expr a) where
    (==) :: (Eq a, Show a) => Expr a -> Expr a -> Bool
    (==) l r = show l == show r

replaceExpr ::
    forall a b c.
    (Columnable a, Columnable b, Columnable c) =>
    Expr a -> Expr b -> Expr c -> Expr c
replaceExpr new old expr = case testEquality (typeRep @b) (typeRep @c) of
    Just Refl -> case testEquality (typeRep @a) (typeRep @c) of
        Just Refl -> if old == expr then new else replace'
        Nothing -> expr
    Nothing -> replace'
  where
    replace' = case expr of
        (Col _) -> expr
        (Lit _) -> expr
        (If cond l r) ->
            If (replaceExpr new old cond) (replaceExpr new old l) (replaceExpr new old r)
        (UnaryOp name f value) -> UnaryOp name f (replaceExpr new old value)
        (BinaryOp name f l r) -> BinaryOp name f (replaceExpr new old l) (replaceExpr new old r)
        (AggNumericVector expr op f) -> AggNumericVector (replaceExpr new old expr) op f
        (AggVector expr op f) -> AggVector (replaceExpr new old expr) op f
        (AggReduce expr op f) -> AggReduce (replaceExpr new old expr) op f
        (AggFold expr op acc f) -> AggFold (replaceExpr new old expr) op acc f

eSize :: Expr a -> Int
eSize (Col _) = 1
eSize (Lit _) = 1
eSize (If c l r) = 1 + eSize c + eSize l + eSize r
eSize (UnaryOp _ _ e) = 1 + eSize e
eSize (BinaryOp _ _ l r) = 1 + eSize l + eSize r
eSize (AggNumericVector expr op _) = eSize expr + 1
eSize (AggVector expr op _) = eSize expr + 1
eSize (AggReduce expr op _) = eSize expr + 1
eSize (AggFold expr op _ _) = eSize expr + 1
