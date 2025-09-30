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
import Type.Reflection (Typeable, typeOf, typeRep)

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
interpret df (AggVector expr op (f :: v b -> c)) =
    let
        (TColumn column) = interpret @b df expr
     in
        case column of
            (BoxedColumn col) -> case testEquality (typeRep @(v b)) (typeOf col) of
                Just Refl -> interpret @c df (Lit (f col))
                Nothing -> error "Type mismatch"
            (OptionalColumn col) -> case testEquality (typeRep @(v b)) (typeOf col) of
                Just Refl -> interpret @c df (Lit (f col))
                Nothing -> error "Type mismatch"
            (UnboxedColumn col) -> case testEquality (typeRep @(v b)) (typeOf col) of
                Just Refl -> interpret @c df (Lit (f col))
                Nothing -> error "Type mismatch"
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
            (BoxedColumn (v :: V.Vector d)) -> case testEquality (typeRep @d) (typeRep @Integer) of
                Just Refl ->
                    TColumn $
                        fromVector $
                            V.replicate
                                (fst $ dataframeDimensions df)
                                (f (VU.convert $ V.map fromInteger v))
                Nothing -> error "Invalid operation"
            _ -> error "Invalid operation"
interpret df (AggFold expr op start (f :: (a -> b -> a))) =
    let
        (TColumn column) = interpret @b df expr
     in
        case ifoldlColumn (\acc _ v -> f acc v) start column of
            Nothing -> error "Invalid operation"
            Just value -> TColumn $ fromVector $ V.replicate (fst $ dataframeDimensions df) value

interpretAggregation ::
    forall a.
    (Columnable a) => GroupedDataFrame -> Expr a -> Either Column (TypedColumn a)
interpretAggregation gdf (Lit value) =
    Right $ TColumn $ fromVector $ V.replicate (VG.length (offsets gdf) - 1) value
interpretAggregation gdf@(Grouped df names indices os) (Col name) = case getColumn name df of
    Nothing -> throw $ ColumnNotFoundException name "" (M.keys $ columnIndices df)
    Just (BoxedColumn col) ->
        Left $
            fromVector $
                V.generate
                    (VU.length os - 1)
                    ( \i ->
                        ( V.generate
                            (os `VG.unsafeIndex` (i + 1) - (os `VG.unsafeIndex` i))
                            ( \j ->
                                col `VG.unsafeIndex` (indices `VG.unsafeIndex` (j + (os `VG.unsafeIndex` i)))
                            )
                        )
                    )
    Just (OptionalColumn col) ->
        Left $
            fromVector $
                V.generate
                    (VU.length os - 1)
                    ( \i ->
                        ( V.generate
                            (os `VG.unsafeIndex` (i + 1) - (os `VG.unsafeIndex` i))
                            ( \j ->
                                col `VG.unsafeIndex` (indices `VG.unsafeIndex` (j + (os `VG.unsafeIndex` i)))
                            )
                        )
                    )
    Just (UnboxedColumn col) ->
        Left $
            fromVector $
                V.generate
                    (VU.length os - 1)
                    ( \i ->
                        ( VU.generate
                            (os `VG.unsafeIndex` (i + 1) - (os `VG.unsafeIndex` i))
                            ( \j ->
                                col `VG.unsafeIndex` (indices `VG.unsafeIndex` (j + (os `VG.unsafeIndex` i)))
                            )
                        )
                    )
interpretAggregation gdf (UnaryOp _ (f :: c -> d) expr) =
    case interpretAggregation @c gdf expr of
        Left unaggregated -> case unaggregated of
            BoxedColumn (col :: V.Vector b) -> case testEquality (typeRep @b) (typeRep @(V.Vector c)) of
                Just Refl -> Left $ fromVector $ V.map (V.map f) col
                Nothing -> case testEquality (typeRep @b) (typeRep @(VU.Vector c)) of
                    Nothing -> error "Type mismatch"
                    Just Refl -> case (sUnbox @c, sUnbox @a) of
                        (STrue, STrue) -> Left $ fromVector $ V.map (VU.map f) col
                        (_, _) -> error ""
            _ -> error "Very unlikely: unaggregated column is boxed"
        Right (TColumn aggregated) -> case mapColumn f aggregated of
            Nothing -> error "Type error in interpretation"
            Just col -> Right $ TColumn col
interpretAggregation gdf (BinaryOp _ (f :: c -> d -> e) left right) =
    case (interpretAggregation @c gdf left, interpretAggregation @d gdf right) of
        (Right (TColumn left'), Right (TColumn right')) -> case zipWithColumns f left' right' of
            Nothing -> error "Type error in binary operation"
            Just col -> Right $ TColumn col
        (Left left', Left right') -> case (left', right') of
            (BoxedColumn (l :: V.Vector m), BoxedColumn (r :: V.Vector n)) -> case testEquality (typeRep @m) (typeRep @(VU.Vector c)) of
                Just Refl -> case testEquality (typeRep @n) (typeRep @(VU.Vector d)) of
                    Just Refl -> case (sUnbox @c, sUnbox @d, sUnbox @e) of
                        (STrue, STrue, STrue) -> Left $ fromVector $ V.zipWith (\l' r' -> VU.zipWith f l' r') l r
                        (_, _, _) -> error "Weird error"
                    Nothing -> case testEquality (typeRep @n) (typeRep @(V.Vector d)) of
                        Just Refl -> case sUnbox @c of
                            STrue -> Left $ fromVector $ V.zipWith (\l' r' -> V.zipWith f (V.convert l') r') l r
                            SFalse -> error "Very unlikely: boxed value in unboxed column."
                        Nothing -> error "Not a vector of any type???"
                Nothing -> error "Not a vector of any type???"
            _ -> error "Very unlikely: aggregation shouldn't be in a boxed column"
        (_, _) -> error "Cannot apply binary operation to unaggregated and aggregated column"
interpretAggregation gdf (If cond l r) =
    case ( interpretAggregation @Bool gdf cond
         , interpretAggregation @a gdf l
         , interpretAggregation @a gdf r
         ) of
        (Right (TColumn conditions), Right (TColumn left), Right (TColumn right)) ->
            Right $
                TColumn $
                    fromMaybe (error "zipWithColumns returned nothing") $
                        zipWithColumns
                            (\(c :: Bool) (l' :: a, r' :: a) -> if c then l' else r')
                            conditions
                            (zipColumns left right)
        ( Left conditions
            , Left left@(BoxedColumn left')
            , Left right@(BoxedColumn right')
            ) ->
                Left $
                    fromMaybe (error "zipWithColumns returned nothing") $
                        zipWithColumns
                            ( \(c :: VU.Vector Bool) (l' :: V.Vector a, r' :: V.Vector a) ->
                                V.zipWith
                                    (\c' (l'', r'') -> if c' then l'' else r'')
                                    (V.convert c)
                                    (V.zip l' r')
                            )
                            conditions
                            (zipColumns left right)
        ( Left conditions
            , Left left@(UnboxedColumn left')
            , Left right@(UnboxedColumn right')
            ) -> case sUnbox @a of
                STrue ->
                    Left $
                        fromMaybe (error "zipWithColumns returned nothing") $
                            zipWithColumns
                                ( \(c :: VU.Vector Bool) (l' :: VU.Vector a, r' :: VU.Vector a) -> VU.zipWith (\c' (l'', r'') -> if c' then l'' else r'') c (VU.zip l' r')
                                )
                                conditions
                                (zipColumns left right)
                SFalse -> error "Very unlikely: Unboxed type in boxed column"
        (_, _, _) -> error "Some columns in the conditional are unaggregated"
interpretAggregation gdf@(Grouped df names indices os) (AggVector expr op (f :: v b -> c)) =
    case interpretAggregation @b gdf expr of
        Left (BoxedColumn (col :: V.Vector d)) -> case testEquality (typeRep @(v b)) (typeRep @d) of
            Nothing -> error "Type mismatch"
            Just Refl -> case testEquality (typeRep @v) (typeRep @V.Vector) of
                Nothing -> error "Container mismatch"
                Just Refl -> Right $ TColumn $ fromVector $ V.map f col
        Left _ -> error "Very unlikely: aggregated into unboxed column???"
        Right (TColumn (BoxedColumn col)) -> case testEquality (typeRep @(v b)) (typeOf col) of
            Just Refl -> interpretAggregation @c gdf (Lit (f col))
            Nothing -> error "Type mismatch"
        Right (TColumn (OptionalColumn col)) -> case testEquality (typeRep @(v b)) (typeOf col) of
            Just Refl -> interpretAggregation @c gdf (Lit (f col))
            Nothing -> error "Type mismatch"
        Right (TColumn (UnboxedColumn col)) -> case testEquality (typeRep @(v b)) (typeOf col) of
            Just Refl -> interpretAggregation @c gdf (Lit (f col))
            Nothing -> error "Type mismatch"
interpretAggregation gdf@(Grouped df names indices os) (AggReduce expr op (f :: forall a. (Columnable a) => a -> a -> a)) =
    case interpretAggregation @a gdf expr of
        Left (BoxedColumn (col :: V.Vector d)) -> case testEquality (typeRep @(V.Vector a)) (typeRep @d) of
            Nothing -> case testEquality (typeRep @(VU.Vector a)) (typeRep @d) of
                Nothing -> error "Type mismatch"
                Just Refl -> case sUnbox @a of
                    STrue ->
                        Right $
                            TColumn $
                                fromVector $
                                    V.map (\v -> VU.foldl' f (VG.head v) (VG.drop 1 v)) col
                    SFalse -> error "Very unlikely case: unboxed vector contains a boxed type"
            Just Refl ->
                Right $
                    TColumn $
                        fromVector $
                            V.map (\v -> VG.foldl' f (VG.head v) (VG.drop 1 v)) col
        Left _ -> error "Impossible to aggregate into unaggregated column"
        Right (TColumn column) -> case headColumn @a column of
            Nothing -> error "Invalid operation"
            Just h -> case ifoldlColumn (\acc _ v -> f acc v) h column of
                Nothing -> error "Invalid operation"
                Just value -> interpretAggregation @a gdf (Lit value)
interpretAggregation gdf@(Grouped df names indices os) (AggFold expr op s (f :: (a -> b -> a))) =
    case interpretAggregation @b gdf expr of
        Left (BoxedColumn (col :: V.Vector d)) -> case testEquality (typeRep @(V.Vector b)) (typeRep @d) of
            Just Refl -> Right $ TColumn $ fromVector $ V.map (\v -> V.foldl' f s v) col
            Nothing -> case testEquality (typeRep @(VU.Vector b)) (typeRep @d) of
                Just Refl -> case sUnbox @b of
                    STrue -> Right $ TColumn $ fromVector $ V.map (\v -> VU.foldl' f s v) col
                    SFalse -> error "Very unlikely: boxed type in unboxed column"
                Nothing -> undefined
        Left _ -> error "Impossible to aggregate into unaggregated column"
        Right (TColumn column) -> case ifoldlColumn (\acc _ v -> f acc v) s column of
            Nothing -> error "Invalid operation"
            Just value -> interpretAggregation @a gdf (Lit value)
interpretAggregation gdf@(Grouped df names indices os) (AggNumericVector expr op (f :: VU.Vector b -> c)) =
    case interpretAggregation @b gdf expr of
        Left (BoxedColumn (col :: V.Vector d)) -> case testEquality (typeRep @(VU.Vector b)) (typeRep @d) of
            Nothing -> case testEquality (typeRep @(VU.Vector Int)) (typeRep @d) of
                Nothing -> error $ "Type mismatch - non numeric " ++ (show (typeRep @d))
                Just Refl -> Right $ TColumn $ fromVector $ V.map f (VG.map (VG.map fromIntegral) col)
            Just Refl -> Right $ TColumn $ fromVector $ V.map f col
        Left _ -> error "Impossible to aggregate into unaggregated column"
        Right (TColumn (UnboxedColumn col)) -> case testEquality (typeRep @(VU.Vector b)) (typeOf col) of
            Just Refl -> interpretAggregation @c gdf (Lit (f col))
            Nothing -> error "Type mismatch"
        Right (TColumn (BoxedColumn col)) -> case testEquality (typeRep @(V.Vector Integer)) (typeOf col) of
            Just Refl -> interpretAggregation @c gdf (Lit (f (VU.convert $ V.map fromInteger col)))
            Nothing -> error "Type mismatch"
        Right _ -> error "Cannot apply numeric aggregation to non-numeric column."

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
