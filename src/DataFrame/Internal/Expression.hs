{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE UndecidableInstances #-}

module DataFrame.Internal.Expression where

import qualified Data.Map as M
import Data.Maybe (fromMaybe, isJust)
import Data.String
import qualified Data.Text as T
import Data.Type.Equality (TestEquality (testEquality), type (:~:) (Refl))
import qualified Data.Vector as V
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU
import DataFrame.Errors
import DataFrame.Internal.Column
import DataFrame.Internal.DataFrame
import DataFrame.Internal.Types
import Type.Reflection (TypeRep, Typeable, typeOf, typeRep, pattern App)

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

interpret ::
    forall a.
    (Columnable a) =>
    DataFrame -> Expr a -> Either DataFrameException (TypedColumn a)
interpret df (Lit value) =
    pure $ TColumn $ fromVector $ V.replicate (fst $ dataframeDimensions df) value
interpret df (Col name) = case getColumn name df of
    Nothing -> Left $ ColumnNotFoundException name "" (M.keys $ columnIndices df)
    Just col -> pure $ TColumn col
interpret df expr@(If cond l r) = case interpret @Bool df cond of
    Left (TypeMismatchException context) ->
        Left $
            TypeMismatchException
                ( context
                    { callingFunctionName = Just "interpret"
                    , errorColumnName = Just (show cond)
                    }
                )
    Left e -> Left e
    Right (TColumn conditions) -> case interpret @a df l of
        Left (TypeMismatchException context) ->
            Left $
                TypeMismatchException
                    (context{callingFunctionName = Just "interpret", errorColumnName = Just (show l)})
        Left e -> Left e
        Right (TColumn left) -> case interpret @a df r of
            Left (TypeMismatchException context) ->
                Left $
                    TypeMismatchException
                        (context{callingFunctionName = Just "interpret", errorColumnName = Just (show r)})
            Left e -> Left e
            Right (TColumn right) -> case zipWithColumns
                (\(c :: Bool) (l' :: a, r' :: a) -> if c then l' else r')
                conditions
                (zipColumns left right) of
                Left (TypeMismatchException context) ->
                    Left $
                        TypeMismatchException
                            ( context
                                { callingFunctionName = Just "interpret"
                                , errorColumnName = Just (show expr)
                                }
                            )
                Left e -> Left e
                Right res -> pure $ TColumn res
interpret df expr@(UnaryOp _ (f :: c -> d) value) = case interpret @c df value of
    Left (TypeMismatchException context) ->
        Left $
            TypeMismatchException
                ( context
                    { callingFunctionName = Just "interpret"
                    , errorColumnName = Just (show value)
                    }
                )
    Left e -> Left e
    Right (TColumn value') -> case mapColumn f value' of
        Left (TypeMismatchException context) ->
            Left $
                TypeMismatchException
                    ( context
                        { callingFunctionName = Just "interpret"
                        , errorColumnName = Just (show expr)
                        }
                    )
        Left e -> Left e
        Right res -> pure $ TColumn res
interpret df expr@(BinaryOp _ (f :: c -> d -> e) (Lit left) (Lit right)) =
    pure $
        TColumn $
            fromVector $
                V.replicate (fst $ dataframeDimensions df) (f left right)
interpret df expr@(BinaryOp _ (f :: c -> d -> e) (Lit left) right) = case interpret @d df right of
    Left (TypeMismatchException context) ->
        Left $
            TypeMismatchException
                ( context
                    { callingFunctionName = Just "interpret"
                    , errorColumnName = Just (show right)
                    }
                )
    Left e -> Left e
    Right (TColumn right') -> case mapColumn (f left) right' of
        Left (TypeMismatchException context) ->
            Left $
                TypeMismatchException
                    ( context
                        { callingFunctionName = Just "interpret"
                        , errorColumnName = Just (show expr)
                        }
                    )
        Left e -> Left e
        Right res -> pure $ TColumn res
interpret df expr@(BinaryOp _ (f :: c -> d -> e) left (Lit right)) = case interpret @c df left of
    Left (TypeMismatchException context) ->
        Left $
            TypeMismatchException
                ( context
                    { callingFunctionName = Just "interpret"
                    , errorColumnName = Just (show left)
                    }
                )
    Left e -> Left e
    Right (TColumn left') -> case mapColumn (`f` right) left' of
        Left (TypeMismatchException context) ->
            Left $
                TypeMismatchException
                    ( context
                        { callingFunctionName = Just "interpret"
                        , errorColumnName = Just (show expr)
                        }
                    )
        Left e -> Left e
        Right res -> pure $ TColumn res
interpret df expr@(BinaryOp _ (f :: c -> d -> e) left right) = case interpret @c df left of
    Left (TypeMismatchException context) ->
        Left $
            TypeMismatchException
                ( context
                    { callingFunctionName = Just "interpret"
                    , errorColumnName = Just (show left)
                    }
                )
    Left e -> Left e
    Right (TColumn left') -> case interpret @d df right of
        Left (TypeMismatchException context) ->
            Left $
                TypeMismatchException
                    ( context
                        { callingFunctionName = Just "interpret"
                        , errorColumnName = Just (show right)
                        }
                    )
        Left e -> Left e
        Right (TColumn right') -> case zipWithColumns f left' right' of
            Left (TypeMismatchException context) ->
                Left $
                    TypeMismatchException
                        ( context
                            { callingFunctionName = Just "interpret"
                            , errorColumnName = Just (show expr)
                            }
                        )
            Left e -> Left e
            Right res -> pure $ TColumn res
interpret df expression@(AggVector expr op (f :: v b -> c)) = case interpret @b df expr of
    Left (TypeMismatchException context) ->
        Left $
            TypeMismatchException
                ( context
                    { callingFunctionName = Just "interpret"
                    , errorColumnName = Just (show expr)
                    }
                )
    Left e -> Left e
    Right (TColumn column) -> case column of
        (BoxedColumn col) -> case testEquality (typeRep @(v b)) (typeOf col) of
            Just Refl -> interpret @c df (Lit (f col))
            Nothing ->
                Left $
                    TypeMismatchException
                        ( MkTypeErrorContext
                            { userType = Right (typeRep @(v b))
                            , expectedType = Right (typeOf col)
                            , callingFunctionName = Just "interpret"
                            , errorColumnName = Nothing
                            }
                        )
        (OptionalColumn col) -> case testEquality (typeRep @(v b)) (typeOf col) of
            Just Refl -> interpret @c df (Lit (f col))
            Nothing ->
                Left $
                    TypeMismatchException
                        ( MkTypeErrorContext
                            { userType = Right (typeRep @(v b))
                            , expectedType = Right (typeOf col)
                            , callingFunctionName = Just "interpret"
                            , errorColumnName = Nothing
                            }
                        )
        (UnboxedColumn col) -> case testEquality (typeRep @(v b)) (typeOf col) of
            Just Refl -> interpret @c df (Lit (f col))
            Nothing ->
                Left $
                    TypeMismatchException
                        ( MkTypeErrorContext
                            { userType = Right (typeRep @(v b))
                            , expectedType = Right (typeOf col)
                            , callingFunctionName = Just "interpret"
                            , errorColumnName = Nothing
                            }
                        )
interpret df expression@(AggReduce expr op (f :: forall a. (Columnable a) => a -> a -> a)) = case interpret @a df expr of
    Left (TypeMismatchException context) ->
        Left $
            TypeMismatchException
                ( context
                    { callingFunctionName = Just "interpret"
                    , errorColumnName = Just (show expr)
                    }
                )
    Left e -> Left e
    Right (TColumn column) -> case headColumn @a column of
        Left (TypeMismatchException context) ->
            Left $
                TypeMismatchException
                    ( context
                        { callingFunctionName = Just "interpret"
                        , errorColumnName = Just (show expr)
                        }
                    )
        Left (EmptyDataSetException loc) -> Left (EmptyDataSetException (T.pack $ show expr))
        Left e -> Left e
        Right h -> case ifoldlColumn (\acc _ v -> f acc v) h column of
            Left (TypeMismatchException context) ->
                Left $
                    TypeMismatchException
                        ( context
                            { callingFunctionName = Just "interpret"
                            , errorColumnName = Just (show expression)
                            }
                        )
            Left e -> Left e
            Right value ->
                pure $ TColumn $ fromVector $ V.replicate (fst $ dataframeDimensions df) value
interpret df expression@(AggNumericVector expr op (f :: VU.Vector b -> c)) = case interpret @b df expr of
    Left (TypeMismatchException context) ->
        Left $
            TypeMismatchException
                ( context
                    { callingFunctionName = Just "interpret"
                    , errorColumnName = Just (show expr)
                    }
                )
    Left e -> Left e
    Right (TColumn column) -> case column of
        (UnboxedColumn (v :: VU.Vector d)) -> case testEquality (typeRep @d) (typeRep @b) of
            Just Refl ->
                pure $ TColumn $ fromVector $ V.replicate (fst $ dataframeDimensions df) (f v)
            Nothing ->
                Left $
                    TypeMismatchException
                        ( MkTypeErrorContext
                            { userType = Right (typeRep @b)
                            , expectedType = Right (typeRep @d)
                            , callingFunctionName = Just "interpret"
                            , errorColumnName = Just (show expression)
                            }
                        )
        (BoxedColumn (v :: V.Vector d)) -> case testEquality (typeRep @d) (typeRep @Integer) of
            Just Refl ->
                Right $
                    TColumn $
                        fromVector $
                            V.replicate
                                (fst $ dataframeDimensions df)
                                (f (VU.convert $ V.map fromInteger v))
            Nothing ->
                Left $
                    TypeMismatchException
                        ( MkTypeErrorContext
                            { userType = Right (typeRep @Integer)
                            , expectedType = Right (typeRep @d)
                            , callingFunctionName = Just "interpret"
                            , errorColumnName = Just (show expression)
                            }
                        )
        (OptionalColumn (v :: V.Vector (Maybe d))) -> case sNumeric @d of
            STrue -> case testEquality (typeRep @d) (typeRep @b) of
                Nothing ->
                    Left $
                        TypeMismatchException
                            ( MkTypeErrorContext
                                { userType = Right (typeRep @b)
                                , expectedType = Right (typeRep @d)
                                , callingFunctionName = Just "interpret"
                                , errorColumnName = Just (show expression)
                                }
                            )
                Just Refl ->
                    pure $
                        TColumn $
                            fromVector $
                                V.replicate
                                    (fst $ dataframeDimensions df)
                                    (f (VU.convert $ V.map (fromMaybe 0) $ V.filter isJust v))
            SFalse ->
                Left $
                    TypeMismatchException
                        ( MkTypeErrorContext
                            { userType = Right (typeRep @d)
                            , expectedType = Right (typeRep @b)
                            , callingFunctionName = Just "interpret"
                            , errorColumnName = Just (show expression)
                            }
                        )
interpret df expression@(AggFold expr op start (f :: (a -> b -> a))) = case interpret @b df expr of
    Left (TypeMismatchException context) ->
        Left $
            TypeMismatchException
                ( context
                    { callingFunctionName = Just "interpret"
                    , errorColumnName = Just (show expr)
                    }
                )
    Left e -> Left e
    Right (TColumn column) -> case ifoldlColumn (\acc _ v -> f acc v) start column of
        Left (TypeMismatchException context) ->
            Left $
                TypeMismatchException
                    ( context
                        { callingFunctionName = Just "interpret"
                        , errorColumnName = Just (show expression)
                        }
                    )
        Left e -> Left e
        Right value ->
            pure $ TColumn $ fromVector $ V.replicate (fst $ dataframeDimensions df) value

data AggregationResult a
    = UnAggregated Column
    | Aggregated (TypedColumn a)

mkUnaggregatedColumn ::
    forall v a.
    (VG.Vector v a, Columnable a) =>
    v a -> VU.Vector Int -> VU.Vector Int -> V.Vector (v a)
mkUnaggregatedColumn col os indices =
    V.generate
        (VU.length os - 1)
        ( \i ->
            VG.generate
                (os `VG.unsafeIndex` (i + 1) - (os `VG.unsafeIndex` i))
                ( \j ->
                    col `VG.unsafeIndex` (indices `VG.unsafeIndex` (j + (os `VG.unsafeIndex` i)))
                )
        )

nestedTypeException ::
    forall a b. (Typeable a, Typeable b) => String -> DataFrameException
nestedTypeException expression = case typeRep @a of
    App t1 t2 ->
        TypeMismatchException
            ( MkTypeErrorContext
                { userType = Left (show (typeRep @b)) :: Either String (TypeRep ())
                , expectedType = Left (show (typeRep @a)) :: Either String (TypeRep ())
                , callingFunctionName = Just "interpretAggregation"
                , errorColumnName = Just expression
                }
            )
    t ->
        TypeMismatchException
            ( MkTypeErrorContext
                { userType = Right (typeRep @(VU.Vector b))
                , expectedType = Right (typeRep @b)
                , callingFunctionName = Just "interpretAggregation"
                , errorColumnName = Just expression
                }
            )

interpretAggregation ::
    forall a.
    (Columnable a) =>
    GroupedDataFrame -> Expr a -> Either DataFrameException (AggregationResult a)
interpretAggregation gdf (Lit value) =
    Right $
        Aggregated $
            TColumn $
                fromVector $
                    V.replicate (VG.length (offsets gdf) - 1) value
interpretAggregation gdf@(Grouped df names indices os) (Col name) = case getColumn name df of
    Nothing -> Left $ ColumnNotFoundException name "" (M.keys $ columnIndices df)
    Just (BoxedColumn col) -> Right $ UnAggregated $ fromVector $ mkUnaggregatedColumn col os indices
    Just (OptionalColumn col) -> Right $ UnAggregated $ fromVector $ mkUnaggregatedColumn col os indices
    Just (UnboxedColumn col) -> Right $ UnAggregated $ fromVector $ mkUnaggregatedColumn col os indices
interpretAggregation gdf expression@(UnaryOp _ (f :: c -> d) expr) =
    case interpretAggregation @c gdf expr of
        Left (TypeMismatchException context) ->
            Left $
                TypeMismatchException
                    ( context
                        { callingFunctionName = Just "interpretAggregation"
                        , errorColumnName = Just (show expr)
                        }
                    )
        Left e -> Left e
        Right (UnAggregated unaggregated) -> case unaggregated of
            BoxedColumn (col :: V.Vector b) -> case testEquality (typeRep @b) (typeRep @(V.Vector c)) of
                Just Refl -> Right $ UnAggregated $ fromVector $ V.map (V.map f) col
                Nothing -> case testEquality (typeRep @b) (typeRep @(VU.Vector c)) of
                    Nothing -> Left $ nestedTypeException @b @c (show expression)
                    Just Refl -> case (sUnbox @c, sUnbox @a) of
                        (SFalse, _) -> Left $ InternalException "Boxed type inside an unboxed column"
                        (STrue, STrue) -> Right $ UnAggregated $ fromVector $ V.map (VU.map f) col
                        (STrue, _) -> Right $ UnAggregated $ fromVector $ V.map (V.map f . VU.convert) col
            _ -> Left $ InternalException "Aggregated into a non-boxed column"
        Right (Aggregated (TColumn aggregated)) -> case mapColumn f aggregated of
            Left e -> Left e
            Right col -> Right $ Aggregated $ TColumn col
interpretAggregation gdf expression@(BinaryOp _ (f :: c -> d -> e) left right) =
    case (interpretAggregation @c gdf left, interpretAggregation @d gdf right) of
        (Right (Aggregated (TColumn left')), Right (Aggregated (TColumn right'))) -> case zipWithColumns f left' right' of
            Left e -> Left e
            Right col -> Right $ Aggregated $ TColumn col
        (Right (UnAggregated left'), Right (UnAggregated right')) -> case (left', right') of
            (BoxedColumn (l :: V.Vector m), BoxedColumn (r :: V.Vector n)) -> case testEquality (typeRep @m) (typeRep @(VU.Vector c)) of
                Just Refl -> case testEquality (typeRep @n) (typeRep @(VU.Vector d)) of
                    Just Refl -> case (sUnbox @c, sUnbox @d, sUnbox @e) of
                        (STrue, STrue, STrue) ->
                            Right $ UnAggregated $ fromVector $ V.zipWith (VU.zipWith f) l r
                        (STrue, STrue, SFalse) ->
                            Right $
                                UnAggregated $
                                    fromVector $
                                        V.zipWith (\l' r' -> V.zipWith f (V.convert l') (V.convert r')) l r
                        (_, _, _) -> Left $ InternalException "Boxed vectors contain unboxed types"
                    Nothing -> case testEquality (typeRep @n) (typeRep @(V.Vector d)) of
                        Just Refl -> case sUnbox @c of
                            STrue ->
                                Right $
                                    UnAggregated $
                                        fromVector $
                                            V.zipWith (V.zipWith f . V.convert) l r
                            SFalse -> Left $ InternalException "Unboxed vectors contain boxed types"
                        Nothing -> Left $ nestedTypeException @n @d (show right)
                Nothing -> case testEquality (typeRep @m) (typeRep @(V.Vector c)) of
                    Nothing -> Left $ nestedTypeException @m @c (show left)
                    Just Refl -> case testEquality (typeRep @n) (typeRep @(VU.Vector d)) of
                        Just Refl -> case (sUnbox @d, sUnbox @e) of
                            (STrue, STrue) ->
                                Right $
                                    UnAggregated $
                                        fromVector $
                                            V.zipWith
                                                (\l' r' -> V.convert @V.Vector @e @VU.Vector $ V.zipWith f l' (V.convert r'))
                                                l
                                                r
                            (STrue, SFalse) ->
                                Right $
                                    UnAggregated $
                                        fromVector $
                                            V.zipWith (\l' r' -> V.zipWith f l' (V.convert r')) l r
                            (_, _) -> Left $ InternalException "Unboxed vectors contain boxed types"
                        Nothing -> case testEquality (typeRep @n) (typeRep @(V.Vector d)) of
                            Just Refl -> case sUnbox @c of
                                STrue -> case sUnbox @e of
                                    SFalse ->
                                        Right $
                                            UnAggregated $
                                                fromVector $
                                                    V.zipWith (V.zipWith f . V.convert) l r
                                    STrue ->
                                        Right $
                                            UnAggregated $
                                                fromVector $
                                                    V.zipWith (\l' r' -> V.convert @V.Vector @e @VU.Vector $ V.zipWith f l' r') l r
                                SFalse -> Left $ InternalException "Unboxed vectors contain boxed types"
                            Nothing -> Left $ nestedTypeException @n @d (show right)
            _ -> Left $ InternalException "Aggregated into a non-boxed column"
        (Right _, Right _) ->
            Left $
                AggregatedAndNonAggregatedException (T.pack $ show left) (T.pack $ show right)
        (Left (TypeMismatchException context), _) ->
            Left $
                TypeMismatchException
                    ( context
                        { callingFunctionName = Just "interpretAggregation"
                        , errorColumnName = Just (show left)
                        }
                    )
        (Left e, _) -> Left e
        (_, Left (TypeMismatchException context)) ->
            Left $
                TypeMismatchException
                    ( context
                        { callingFunctionName = Just "interpretAggregation"
                        , errorColumnName = Just (show right)
                        }
                    )
        (_, Left e) -> Left e
interpretAggregation gdf expression@(If cond l r) =
    case ( interpretAggregation @Bool gdf cond
         , interpretAggregation @a gdf l
         , interpretAggregation @a gdf r
         ) of
        ( Right (Aggregated (TColumn conditions))
            , Right (Aggregated (TColumn left))
            , Right (Aggregated (TColumn right))
            ) -> case zipWithColumns
                (\(c :: Bool) (l' :: a, r' :: a) -> if c then l' else r')
                conditions
                (zipColumns left right) of
                Left e -> Left e
                Right v -> Right $ Aggregated (TColumn v)
        ( Right (UnAggregated conditions)
            , Right (UnAggregated left@(BoxedColumn (left' :: V.Vector b)))
            , Right (UnAggregated right@(BoxedColumn (right' :: V.Vector c)))
            ) -> case testEquality (typeRep @b) (typeRep @c) of
                Nothing ->
                    Left $
                        TypeMismatchException
                            ( MkTypeErrorContext
                                { userType = Right (typeRep @b)
                                , expectedType = Right (typeRep @c)
                                , callingFunctionName = Just "interpretAggregation"
                                , errorColumnName = Just (show expression)
                                }
                            )
                Just Refl -> case testEquality (typeRep @(V.Vector a)) (typeRep @b) of
                    Just Refl -> case zipWithColumns
                        ( \(c :: VU.Vector Bool) (l' :: V.Vector a, r' :: V.Vector a) ->
                            V.zipWith
                                (\c' (l'', r'') -> if c' then l'' else r'')
                                (V.convert c)
                                (V.zip l' r')
                        )
                        conditions
                        (zipColumns left right) of
                        Left (TypeMismatchException context) ->
                            Left $
                                TypeMismatchException
                                    ( context
                                        { callingFunctionName = Just "interpretAggregation"
                                        , errorColumnName = Just (show expression)
                                        }
                                    )
                        Left e -> Left e
                        Right v -> Right $ UnAggregated v
                    Nothing -> case testEquality (typeRep @(VU.Vector a)) (typeRep @b) of
                        Nothing -> Left $ nestedTypeException @b @a (show expression)
                        Just Refl -> case sUnbox @a of
                            SFalse -> Left $ InternalException "Boxed type in unboxed column"
                            STrue -> case zipWithColumns
                                ( \(c :: VU.Vector Bool) (l' :: VU.Vector a, r' :: VU.Vector a) ->
                                    VU.zipWith
                                        (\c' (l'', r'') -> if c' then l'' else r'')
                                        c
                                        (VU.zip l' r')
                                )
                                conditions
                                (zipColumns left right) of
                                Left (TypeMismatchException context) ->
                                    Left $
                                        TypeMismatchException
                                            ( context
                                                { callingFunctionName = Just "interpretAggregation"
                                                , errorColumnName = Just (show expression)
                                                }
                                            )
                                Left e -> Left e
                                Right v -> Right $ UnAggregated v
        (Right _, Right _, Right _) ->
            Left $
                AggregatedAndNonAggregatedException (T.pack $ show l) (T.pack $ show r)
        (Left (TypeMismatchException context), _, _) ->
            Left $
                TypeMismatchException
                    ( context
                        { callingFunctionName = Just "interpretAggregation"
                        , errorColumnName = Just (show cond)
                        }
                    )
        (Left e, _, _) -> Left e
        (_, Left (TypeMismatchException context), _) ->
            Left $
                TypeMismatchException
                    ( context
                        { callingFunctionName = Just "interpretAggregation"
                        , errorColumnName = Just (show l)
                        }
                    )
        (_, Left e, _) -> Left e
        (_, _, Left (TypeMismatchException context)) ->
            Left $
                TypeMismatchException
                    ( context
                        { callingFunctionName = Just "interpretAggregation"
                        , errorColumnName = Just (show r)
                        }
                    )
        (_, _, Left e) -> Left e
interpretAggregation gdf@(Grouped df names indices os) expression@(AggVector expr op (f :: v b -> c)) =
    case interpretAggregation @b gdf expr of
        Right (UnAggregated (BoxedColumn (col :: V.Vector d))) -> case testEquality (typeRep @(v b)) (typeRep @d) of
            Nothing -> Left $ nestedTypeException @d @b (show expr)
            Just Refl -> case testEquality (typeRep @v) (typeRep @V.Vector) of
                Nothing -> Right $ Aggregated $ TColumn $ fromVector $ V.map (f . V.convert) col
                Just Refl -> Right $ Aggregated $ TColumn $ fromVector $ V.map f col
        Right (UnAggregated _) -> Left $ InternalException "Aggregated into non-boxed column"
        Right (Aggregated (TColumn (BoxedColumn (col :: V.Vector d)))) -> case testEquality (typeRep @b) (typeRep @d) of
            Just Refl -> case testEquality (typeRep @v) (typeRep @V.Vector) of
                Just Refl -> interpretAggregation @c gdf (Lit (f col))
                Nothing -> interpretAggregation @c gdf (Lit ((f . V.convert) col))
            Nothing ->
                Left $
                    TypeMismatchException
                        ( MkTypeErrorContext
                            { userType = Right (typeRep @b)
                            , expectedType = Right (typeRep @d)
                            , callingFunctionName = Just "interpretAggregation"
                            , errorColumnName = Just (show expr)
                            }
                        )
        Right (Aggregated (TColumn (UnboxedColumn (col :: VU.Vector d)))) -> case testEquality (typeRep @b) (typeRep @d) of
            Just Refl -> case testEquality (typeRep @v) (typeRep @VU.Vector) of
                Just Refl -> interpretAggregation @c gdf (Lit (f col))
                Nothing -> interpretAggregation @c gdf (Lit ((f . VU.convert) col))
            Nothing ->
                Left $
                    TypeMismatchException
                        ( MkTypeErrorContext
                            { userType = Right (typeRep @b)
                            , expectedType = Right (typeRep @d)
                            , callingFunctionName = Just "interpretAggregation"
                            , errorColumnName = Just (show expr)
                            }
                        )
        Right (Aggregated (TColumn (OptionalColumn (col :: V.Vector d)))) -> case testEquality (typeRep @b) (typeRep @d) of
            Just Refl -> case testEquality (typeRep @v) (typeRep @V.Vector) of
                Just Refl -> interpretAggregation @c gdf (Lit (f col))
                Nothing -> interpretAggregation @c gdf (Lit ((f . V.convert) col))
            Nothing ->
                Left $
                    TypeMismatchException
                        ( MkTypeErrorContext
                            { userType = Right (typeRep @b)
                            , expectedType = Right (typeRep @d)
                            , callingFunctionName = Just "interpretAggregation"
                            , errorColumnName = Just (show expr)
                            }
                        )
        (Left (TypeMismatchException context)) ->
            Left $
                TypeMismatchException
                    ( context
                        { callingFunctionName = Just "interpretAggregation"
                        , errorColumnName = Just (show expression)
                        }
                    )
        (Left e) -> Left e
interpretAggregation gdf@(Grouped df names indices os) expression@(AggNumericVector expr op (f :: VU.Vector b -> c)) =
    case interpretAggregation @b gdf expr of
        (Left (TypeMismatchException context)) ->
            Left $
                TypeMismatchException
                    ( context
                        { callingFunctionName = Just "interpretAggregation"
                        , errorColumnName = Just (show expression)
                        }
                    )
        (Left e) -> Left e
        Right (UnAggregated (BoxedColumn (col :: V.Vector d))) -> case testEquality (typeRep @(VU.Vector b)) (typeRep @d) of
            Nothing -> case testEquality (typeRep @(VU.Vector Int)) (typeRep @d) of
                Nothing -> case testEquality (typeRep @(V.Vector Integer)) (typeRep @d) of
                    Nothing -> Left $ nestedTypeException @d @b (show expr)
                    Just Refl ->
                        Right $
                            Aggregated $
                                TColumn $
                                    fromVector $
                                        V.map (f . VU.convert . V.map fromIntegral) col
                Just Refl ->
                    Right $
                        Aggregated $
                            TColumn $
                                fromVector $
                                    V.map f (VG.map (VG.map fromIntegral) col)
            Just Refl -> Right $ Aggregated $ TColumn $ fromVector $ V.map f col
        Right (UnAggregated _) -> Left $ InternalException "Aggregated into non-boxed column"
        Right (Aggregated (TColumn (BoxedColumn (col :: V.Vector d)))) -> case testEquality (typeRep @Integer) (typeRep @d) of
            Just Refl -> interpretAggregation @c gdf (Lit ((f . V.convert . V.map fromIntegral) col))
            Nothing ->
                Left $
                    TypeMismatchException
                        ( MkTypeErrorContext
                            { userType = Right (typeRep @b)
                            , expectedType = Right (typeRep @d)
                            , callingFunctionName = Just "interpretAggregation"
                            , errorColumnName = Just (show expr)
                            }
                        )
        Right (Aggregated (TColumn (UnboxedColumn (col :: VU.Vector d)))) -> case testEquality (typeRep @b) (typeRep @d) of
            Just Refl -> interpretAggregation @c gdf (Lit (f col))
            Nothing ->
                Left $
                    TypeMismatchException
                        ( MkTypeErrorContext
                            { userType = Right (typeRep @b)
                            , expectedType = Right (typeRep @d)
                            , callingFunctionName = Just "interpretAggregation"
                            , errorColumnName = Just (show expr)
                            }
                        )
        Right (Aggregated (TColumn (OptionalColumn (col :: V.Vector (Maybe d))))) -> case testEquality (typeRep @b) (typeRep @d) of
            Just Refl ->
                interpretAggregation @c
                    gdf
                    (Lit ((f . V.convert . V.map (fromMaybe 0) . V.filter isJust) col))
            Nothing ->
                Left $
                    TypeMismatchException
                        ( MkTypeErrorContext
                            { userType = Right (typeRep @b)
                            , expectedType = Right (typeRep @d)
                            , callingFunctionName = Just "interpretAggregation"
                            , errorColumnName = Just (show expr)
                            }
                        )
interpretAggregation gdf@(Grouped df names indices os) expression@(AggReduce expr op (f :: forall a. (Columnable a) => a -> a -> a)) =
    case interpretAggregation @a gdf expr of
        (Left (TypeMismatchException context)) ->
            Left $
                TypeMismatchException
                    ( context
                        { callingFunctionName = Just "interpretAggregation"
                        , errorColumnName = Just (show expression)
                        }
                    )
        (Left e) -> Left e
        Right (UnAggregated (BoxedColumn (col :: V.Vector d))) -> case testEquality (typeRep @(V.Vector a)) (typeRep @d) of
            Nothing -> case testEquality (typeRep @(VU.Vector a)) (typeRep @d) of
                Nothing -> Left $ nestedTypeException @d @a (show expr)
                Just Refl -> case sUnbox @a of
                    STrue ->
                        Right $
                            Aggregated $
                                TColumn $
                                    fromVector $
                                        V.map (\v -> VU.foldl' f (VG.head v) (VG.drop 1 v)) col
                    SFalse -> Left $ InternalException "Boxed type inside an unboxed column"
            Just Refl ->
                Right $
                    Aggregated $
                        TColumn $
                            fromVector $
                                V.map (\v -> VG.foldl' f (VG.head v) (VG.drop 1 v)) col
        Right (UnAggregated _) -> Left $ InternalException "Aggregated into non-boxed column"
        Right (Aggregated (TColumn column)) -> case headColumn @a column of
            Left e -> Left e
            Right h -> case ifoldlColumn (\acc _ v -> f acc v) h column of
                Left e -> Left e
                Right value -> interpretAggregation @a gdf (Lit value)
interpretAggregation gdf@(Grouped df names indices os) expression@(AggFold expr op s (f :: (a -> b -> a))) =
    case interpretAggregation @b gdf expr of
        (Left (TypeMismatchException context)) ->
            Left $
                TypeMismatchException
                    ( context
                        { callingFunctionName = Just "interpretAggregation"
                        , errorColumnName = Just (show expression)
                        }
                    )
        (Left e) -> Left e
        Right (UnAggregated (BoxedColumn (col :: V.Vector d))) -> case testEquality (typeRep @(V.Vector b)) (typeRep @d) of
            Just Refl -> Right $ Aggregated $ TColumn $ fromVector $ V.map (V.foldl' f s) col
            Nothing -> case testEquality (typeRep @(VU.Vector b)) (typeRep @d) of
                Just Refl -> case sUnbox @b of
                    STrue ->
                        Right $ Aggregated $ TColumn $ fromVector $ V.map (VU.foldl' f s) col
                    SFalse -> Left $ InternalException "Boxed type inside an unboxed column"
                Nothing -> Left $ nestedTypeException @d @b (show expr)
        Right (UnAggregated _) -> Left $ InternalException "Aggregated into non-boxed column"
        Right (Aggregated (TColumn column)) -> case ifoldlColumn (\acc _ v -> f acc v) s column of
            Left e -> Left e
            Right value -> interpretAggregation @a gdf (Lit value)

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

instance (IsString a, Columnable a) => IsString (Expr a) where
    fromString :: String -> Expr a
    fromString s = Lit (fromString s)

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
