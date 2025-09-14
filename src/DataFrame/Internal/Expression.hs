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
import Type.Reflection (typeRep)

data Expr a where
    Col :: (Columnable a) => T.Text -> Expr a
    Lit :: (Columnable a) => a -> Expr a
    Apply ::
        ( Columnable a
        , Columnable b
        ) =>
        T.Text -> -- Operation name
        (b -> a) ->
        Expr b ->
        Expr a
    BinOp ::
        ( Columnable c
        , Columnable b
        , Columnable a
        ) =>
        T.Text -> -- operation name
        (c -> b -> a) ->
        Expr c ->
        Expr b ->
        Expr a
    GeneralAggregate ::
        (Columnable a) =>
        T.Text -> -- Column name
        T.Text -> -- Operation name
        (forall v b. (VG.Vector v b, Columnable b) => v b -> a) ->
        Expr a
    ReductionAggregate ::
        (Columnable a) =>
        T.Text -> -- Column name
        T.Text -> -- Operation name
        (forall a. (Columnable a) => a -> a -> a) ->
        Expr a
    NumericAggregate ::
        ( Columnable a
        , Columnable b
        , VU.Unbox a
        , VU.Unbox b
        , Num a
        , Num b
        ) =>
        T.Text -> -- Column name
        T.Text -> -- Operation name
        (VU.Vector b -> a) ->
        Expr a
    FoldAggregate ::
        forall a b.
        (Columnable a, Columnable b) =>
        T.Text -> -- Column name
        T.Text -> -- Operation name
        a ->
        (a -> b -> a) ->
        Expr a

data UExpr where
    Wrap :: (Columnable a) => Expr a -> UExpr

interpret :: forall a. (Columnable a) => DataFrame -> Expr a -> TypedColumn a
interpret df (Lit value) = TColumn $ fromVector $ V.replicate (fst $ dataframeDimensions df) value
interpret df (Col name) = case getColumn name df of
    Nothing -> throw $ ColumnNotFoundException name "" (map fst $ M.toList $ columnIndices df)
    Just col -> TColumn col
interpret df (Apply _ (f :: c -> d) value) =
    let
        (TColumn value') = interpret @c df value
     in
        -- TODO: Handle this gracefully.
        TColumn $ fromMaybe (error "mapColumn returned nothing") (mapColumn f value')
interpret df (BinOp _ (f :: c -> d -> e) (Lit left) right) =
    let
        (TColumn right') = interpret @d df right
     in
        TColumn $ fromMaybe (error "mapColumn returned nothing") (mapColumn (f left) right')
interpret df (BinOp _ (f :: c -> d -> e) left (Lit right)) =
    let
        (TColumn left') = interpret @c df left
     in
        TColumn $ fromMaybe (error "mapColumn returned nothing") (mapColumn (`f` right) left')
interpret df (BinOp _ (f :: c -> d -> e) left right) =
    let
        (TColumn left') = interpret @c df left
        (TColumn right') = interpret @d df right
     in
        TColumn $ fromMaybe (error "mapColumn returned nothing") (zipWithColumns f left' right')
interpret df (ReductionAggregate name op (f :: forall a. (Columnable a) => a -> a -> a)) =
    let
        (TColumn column) = interpret @a df (Col name)
     in
        case headColumn @a column of
            Nothing -> error "Invalid operation"
            Just h -> case ifoldlColumn (\acc _ v -> f acc v) h column of
                Nothing -> error "Invalid operation"
                Just value -> TColumn $ fromVector $ V.replicate (fst $ dataframeDimensions df) value
interpret df (NumericAggregate name op (f :: VU.Vector b -> c)) =
    let
        (TColumn column) = interpret @b df (Col name)
     in
        case column of
            (UnboxedColumn (v :: VU.Vector d)) -> case testEquality (typeRep @d) (typeRep @b) of
                Just Refl -> TColumn $ fromVector $ V.replicate (fst $ dataframeDimensions df) (f v)
                Nothing -> error "Invalid operation"
            _ -> error "Invalid operation"
interpret df (FoldAggregate name op start (f :: (a -> b -> a))) =
    let
        (TColumn column) = interpret @b df (Col name)
     in
        case headColumn @a column of
            Nothing -> error "Invalid operation"
            Just h -> case ifoldlColumn (\acc _ v -> f acc v) h column of
                Nothing -> error "Invalid operation"
                Just value -> TColumn $ fromVector $ V.replicate (fst $ dataframeDimensions df) value
interpret _ expr = error ("Invalid operation for dataframe: " ++ show expr)

interpretAggregation :: forall a. (Columnable a) => GroupedDataFrame -> Expr a -> TypedColumn a
interpretAggregation gdf (Lit value) = TColumn $ fromVector $ V.replicate (VG.length (offsets gdf) - 1) value
interpretAggregation gdf@(Grouped df names indices os) (Col name) = case getColumn name df of
    Nothing -> throw $ ColumnNotFoundException name "" (map fst $ M.toList $ columnIndices df)
    Just col -> TColumn $ atIndicesStable (VG.map (indices `VG.unsafeIndex`) (VG.init os)) col
interpretAggregation gdf (Apply _ (f :: c -> d) expr) =
    let
        (TColumn value) = interpretAggregation @c gdf expr
     in
        case mapColumn f value of
            Nothing -> error "Type error in interpretation"
            Just col -> TColumn col
interpretAggregation gdf (BinOp _ (f :: c -> d -> e) left right) =
    let
        (TColumn left') = interpretAggregation @c gdf left
        (TColumn right') = interpretAggregation @d gdf right
     in
        case zipWithColumns f left' right' of
            Nothing -> error "Type error in binary operation"
            Just col -> TColumn col
interpretAggregation gdf@(Grouped df names indices os) (GeneralAggregate name op (f :: forall v b. (VG.Vector v b, Columnable b) => v b -> c)) = case getColumn name df of
    Nothing -> throw $ ColumnNotFoundException name "" (map fst $ M.toList $ columnIndices df)
    Just (BoxedColumn col) ->
        TColumn $
            fromVector $
                V.generate
                    (VG.length os - 1)
                    ( \i ->
                        f
                            ( V.generate
                                (os `VG.unsafeIndex` (i + 1) - (os `VG.unsafeIndex` i))
                                (\j -> col `VG.unsafeIndex` (indices `VG.unsafeIndex` (j + (os `VG.unsafeIndex` i))))
                            )
                    )
    Just (UnboxedColumn col) -> case sUnbox @c of
        SFalse ->
            TColumn $
                fromVector $
                    V.generate
                        (VG.length os - 1)
                        ( \i ->
                            f
                                ( VU.generate
                                    (os `VG.unsafeIndex` (i + 1) - (os `VG.unsafeIndex` i))
                                    (\j -> col `VG.unsafeIndex` (indices `VG.unsafeIndex` (j + (os `VG.unsafeIndex` i))))
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
                                    (\j -> col `VG.unsafeIndex` (indices `VG.unsafeIndex` (j + (os `VG.unsafeIndex` i))))
                                )
                        )
    Just (OptionalColumn col) ->
        TColumn $
            fromVector $
                V.generate
                    (VG.length os - 1)
                    ( \i ->
                        f
                            ( V.generate
                                (os `VG.unsafeIndex` (i + 1) - (os `VG.unsafeIndex` i))
                                (\j -> col `VG.unsafeIndex` (indices `VG.unsafeIndex` (j + (os `VG.unsafeIndex` i))))
                            )
                    )
interpretAggregation gdf@(Grouped df names indices os) (ReductionAggregate name op (f :: forall a. (Columnable a) => a -> a -> a)) = case getColumn name df of
    Nothing -> throw $ ColumnNotFoundException name "" (map fst $ M.toList $ columnIndices df)
    Just (BoxedColumn col) -> TColumn $
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
    Just (UnboxedColumn col) -> case sUnbox @a of
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
    Just (OptionalColumn col) -> TColumn $
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
interpretAggregation gdf@(Grouped df names indices os) (FoldAggregate name op s (f :: (a -> b -> a))) = case getColumn name df of
    Nothing -> throw $ ColumnNotFoundException name "" (map fst $ M.toList $ columnIndices df)
    Just (BoxedColumn (col :: V.Vector c)) -> case testEquality (typeRep @b) (typeRep @c) of
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
    Just (UnboxedColumn (col :: VU.Vector c)) -> case testEquality (typeRep @b) (typeRep @c) of
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
    Just (OptionalColumn (col :: V.Vector c)) -> case testEquality (typeRep @b) (typeRep @c) of
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
interpretAggregation gdf@(Grouped df names indices os) (NumericAggregate name op (f :: VU.Vector b -> c)) = case getColumn name df of
    Nothing -> throw $ ColumnNotFoundException name "" (map fst $ M.toList $ columnIndices df)
    Just (UnboxedColumn (col :: VU.Vector d)) -> case testEquality (typeRep @b) (typeRep @d) of
        Nothing -> case testEquality (typeRep @d) (typeRep @Int) of
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
                                            (\j -> fromIntegral (col `VG.unsafeIndex` (indices `VG.unsafeIndex` (j + (os `VG.unsafeIndex` i)))))
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
                                            (\j -> fromIntegral (col `VG.unsafeIndex` (indices `VG.unsafeIndex` (j + (os `VG.unsafeIndex` i)))))
                                        )
                                )
        Just Refl -> case sNumeric @d of
            SFalse -> error $ "Cannot apply numeric aggregation to non-numeric column: " ++ T.unpack name
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
                                            (\j -> col `VG.unsafeIndex` (indices `VG.unsafeIndex` (j + (os `VG.unsafeIndex` i))))
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
                                            (\j -> col `VG.unsafeIndex` (indices `VG.unsafeIndex` (j + (os `VG.unsafeIndex` i))))
                                        )
                                )
    _ -> error $ "Cannot apply numeric aggregation to non-numeric column: " ++ T.unpack name

instance (Num a, Columnable a) => Num (Expr a) where
    (+) :: Expr a -> Expr a -> Expr a
    (+) = BinOp "add" (+)

    (*) :: Expr a -> Expr a -> Expr a
    (*) = BinOp "mult" (*)

    fromInteger :: Integer -> Expr a
    fromInteger = Lit . fromInteger

    negate :: Expr a -> Expr a
    negate = Apply "negate" negate

    abs :: (Num a) => Expr a -> Expr a
    abs = Apply "abs" abs

    signum :: (Num a) => Expr a -> Expr a
    signum = Apply "signum" signum

instance (Fractional a, Columnable a) => Fractional (Expr a) where
    fromRational :: (Fractional a, Columnable a) => Rational -> Expr a
    fromRational = Lit . fromRational

    (/) :: (Fractional a, Columnable a) => Expr a -> Expr a -> Expr a
    (/) = BinOp "divide" (/)

instance (Floating a, Columnable a) => Floating (Expr a) where
    pi :: (Floating a, Columnable a) => Expr a
    pi = Lit pi
    exp :: (Floating a, Columnable a) => Expr a -> Expr a
    exp = Apply "exp" exp
    log :: (Floating a, Columnable a) => Expr a -> Expr a
    log = Apply "log" log
    sin :: (Floating a, Columnable a) => Expr a -> Expr a
    sin = Apply "sin" sin
    cos :: (Floating a, Columnable a) => Expr a -> Expr a
    cos = Apply "cos" cos
    asin :: (Floating a, Columnable a) => Expr a -> Expr a
    asin = Apply "asin" asin
    acos :: (Floating a, Columnable a) => Expr a -> Expr a
    acos = Apply "acos" acos
    atan :: (Floating a, Columnable a) => Expr a -> Expr a
    atan = Apply "atan" atan
    sinh :: (Floating a, Columnable a) => Expr a -> Expr a
    sinh = Apply "sinh" sinh
    cosh :: (Floating a, Columnable a) => Expr a -> Expr a
    cosh = Apply "cosh" cosh
    asinh :: (Floating a, Columnable a) => Expr a -> Expr a
    asinh = Apply "asinh" sinh
    acosh :: (Floating a, Columnable a) => Expr a -> Expr a
    acosh = Apply "acosh" acosh
    atanh :: (Floating a, Columnable a) => Expr a -> Expr a
    atanh = Apply "atanh" atanh

instance (Show a) => Show (Expr a) where
    show :: forall a. (Show a) => Expr a -> String
    show (Col name) = "col@" ++ show (typeRep @a) ++ "(" ++ T.unpack name ++ ")"
    show (Lit value) = show value
    show (Apply name f value) = T.unpack name ++ "(" ++ show value ++ ")"
    show (BinOp name f a b) = T.unpack name ++ "(" ++ show a ++ ", " ++ show b ++ ")"
    show (NumericAggregate columnName op f) = T.unpack op ++ "(" ++ T.unpack columnName ++ ")"
