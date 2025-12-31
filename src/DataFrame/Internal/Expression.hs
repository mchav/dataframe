{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
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

import Control.Monad.ST (runST)
import Data.Bifunctor
import qualified Data.Map as M
import Data.Maybe (fromMaybe, isJust)
import Data.String
import qualified Data.Text as T
import Data.Type.Equality (TestEquality (testEquality), type (:~:) (Refl))
import qualified Data.Vector as V
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Mutable as VM
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import DataFrame.Errors
import DataFrame.Internal.Column
import DataFrame.Internal.DataFrame
import DataFrame.Internal.Types
import Type.Reflection (TypeRep, Typeable, typeOf, typeRep, pattern App)

data Expr a where
    Col :: (Columnable a) => T.Text -> Expr a
    Lit :: (Columnable a) => a -> Expr a
    UnaryOp ::
        (Columnable a, Columnable b) => T.Text -> (b -> a) -> Expr b -> Expr a
    BinaryOp ::
        (Columnable c, Columnable b, Columnable a) =>
        T.Text -> (c -> b -> a) -> Expr c -> Expr b -> Expr a
    If :: (Columnable a) => Expr Bool -> Expr a -> Expr a -> Expr a
    AggVector ::
        (VG.Vector v b, Typeable v, Columnable a, Columnable b) =>
        Expr b -> T.Text -> (v b -> a) -> Expr a
    AggReduce :: (Columnable a) => Expr a -> T.Text -> (a -> a -> a) -> Expr a
    -- TODO(mchav): Numeric reduce might be superfluous since expressions are already type checked.
    AggNumericVector ::
        (Columnable a, Columnable b, VU.Unbox a, VU.Unbox b, Num a, Num b) =>
        Expr b -> T.Text -> (VU.Vector b -> a) -> Expr a
    AggFold ::
        forall a b.
        (Columnable a, Columnable b) => Expr b -> T.Text -> a -> (a -> b -> a) -> Expr a

data UExpr where
    Wrap :: (Columnable a) => Expr a -> UExpr

type NamedExpr = (T.Text, UExpr)

interpret ::
    forall a.
    (Columnable a) =>
    DataFrame -> Expr a -> Either DataFrameException (TypedColumn a)
interpret df (Lit value) = case sUnbox @a of
    -- Specialize the creation of unboxed columns to avoid an extra allocation.
    STrue -> pure $ TColumn $ fromUnboxedVector $ VU.replicate (numRows df) value
    SFalse -> pure $ TColumn $ fromVector $ V.replicate (numRows df) value
interpret df (Col name) = maybe columnNotFound (pure . TColumn) (getColumn name df)
  where
    columnNotFound = Left $ ColumnNotFoundException name "" (M.keys $ columnIndices df)
-- Unary operations.
interpret df expr@(UnaryOp _ (f :: c -> d) value) = first (handleInterpretException (show expr)) $ do
    (TColumn value') <- interpret @c df value
    fmap TColumn (mapColumn f value')
-- Variations of binary operations.
interpret df expr@(BinaryOp _ (f :: c -> d -> e) left right) = first (handleInterpretException (show expr)) $ case (left, right) of
    (Lit left, Lit right) -> interpret df (Lit (f left right))
    (Lit left, right) -> do
        -- If we have a literal then we don't have to materialise
        -- the column.
        (TColumn value') <- interpret @d df right
        fmap TColumn (mapColumn (f left) value')
    (left, Lit right) -> do
        -- Same as the above except the right side is the
        -- literl.
        (TColumn value') <- interpret @c df left
        fmap TColumn (mapColumn (`f` right) value')
    (_, _) -> do
        -- In the general case we interpret and zip.
        (TColumn left') <- interpret @c df left
        (TColumn right') <- interpret @d df right
        fmap TColumn (zipWithColumns f left' right')
-- Conditionals
interpret df expr@(If cond l r) = first (handleInterpretException (show expr)) $ do
    (TColumn conditions) <- interpret @Bool df cond
    (TColumn left) <- interpret @a df l
    (TColumn right) <- interpret @a df r
    let branch (c :: Bool) (l' :: a, r' :: a) = if c then l' else r'
    fmap TColumn (zipWithColumns branch conditions (zipColumns left right))
interpret df expression@(AggVector expr op (f :: v b -> c)) = do
    (TColumn column) <- interpret @b df expr
    -- Helper for errors. Should probably find a way of throwing this
    -- without leaking the fact that we use `Vector` to users.
    let aggTypeError expected =
            TypeMismatchException
                ( MkTypeErrorContext
                    { userType = Right (typeRep @(v b))
                    , expectedType = Left expected :: Either String (TypeRep ())
                    , callingFunctionName = Just "interpret"
                    , errorColumnName = Nothing
                    }
                )
    let processColumn ::
            (Columnable d) => d -> Either DataFrameException (TypedColumn a)
        processColumn col = case testEquality (typeRep @(v b)) (typeOf col) of
            Just Refl -> interpret @c df (Lit (f col))
            Nothing -> Left $ aggTypeError (show (typeOf col))
    case column of
        (BoxedColumn col) -> processColumn col
        (OptionalColumn col) -> processColumn col
        (UnboxedColumn col) -> processColumn col
interpret df expression@(AggReduce expr op (f :: a -> a -> a)) = first (handleInterpretException (show expr)) $ do
    (TColumn column) <- interpret @a df expr
    value <- foldl1Column f column
    pure $ TColumn $ fromVector $ V.replicate (fst $ dataframeDimensions df) value
interpret df expression@(AggNumericVector expr op (f :: VU.Vector b -> c)) = first (handleInterpretException (show expression)) $ do
    (TColumn column) <- interpret @b df expr
    case column of
        (UnboxedColumn (v :: VU.Vector d)) -> case testEquality (typeRep @d) (typeRep @b) of
            Just Refl ->
                pure $ TColumn $ fromVector $ V.replicate (fst $ dataframeDimensions df) (f v)
            Nothing ->
                Left $
                    TypeMismatchException
                        ( MkTypeErrorContext
                            (Right (typeRep @b))
                            (Right (typeRep @d))
                            (Just "interpret")
                            (Just (show expression))
                        )
        _ -> error "Trying to apply numeric computation to non-numeric column"
interpret df expression@(AggFold expr op start (f :: (a -> b -> a))) = first (handleInterpretException (show expression)) $ do
    (TColumn column) <- interpret @b df expr
    value <- foldlColumn f start column
    pure $ TColumn $ fromVector $ V.replicate (fst $ dataframeDimensions df) value

data AggregationResult a
    = UnAggregated Column
    | Aggregated (TypedColumn a)

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
    Just (BoxedColumn col) -> Right $ UnAggregated $ fromVector $ mkUnaggregatedColumnBoxed col os indices
    Just (OptionalColumn col) -> Right $ UnAggregated $ fromVector $ mkUnaggregatedColumnBoxed col os indices
    Just (UnboxedColumn col) ->
        Right $ UnAggregated $ fromVector $ mkUnaggregatedColumnUnboxed col os indices
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
                Just Refl -> case sUnbox @d of
                    SFalse -> Right $ UnAggregated $ fromVector $ V.map (V.map f) col
                    STrue ->
                        Right $
                            UnAggregated $
                                fromVector $
                                    V.map (V.convert @V.Vector @d @VU.Vector . V.map f) col
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
interpretAggregation gdf expression@(BinaryOp name (f :: c -> d -> e) left (Lit (right :: g))) = case testEquality (typeRep @g) (typeRep @d) of
    Just Refl -> interpretAggregation gdf (UnaryOp name (`f` right) left)
    Nothing ->
        Left $
            TypeMismatchException
                ( MkTypeErrorContext
                    { userType = Right (typeRep @g)
                    , expectedType = Right (typeRep @d)
                    , callingFunctionName = Just "interpretAggregation"
                    , errorColumnName = Just (show expression)
                    }
                )
interpretAggregation gdf expression@(BinaryOp name (f :: c -> d -> e) (Lit (left :: g)) right) = case testEquality (typeRep @g) (typeRep @c) of
    Just Refl -> interpretAggregation gdf (UnaryOp name (f left) right)
    Nothing ->
        Left $
            TypeMismatchException
                ( MkTypeErrorContext
                    { userType = Right (typeRep @g)
                    , expectedType = Right (typeRep @c)
                    , callingFunctionName = Just "interpretAggregation"
                    , errorColumnName = Just (show expression)
                    }
                )
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
                            Just Refl -> case sUnbox @e of
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
interpretAggregation gdf expression@(If cond (Lit l) (Lit r)) =
    case interpretAggregation @Bool gdf cond of
        Right (Aggregated (TColumn conditions)) -> case mapColumn
            (\(c :: Bool) -> if c then l else r)
            conditions of
            Left e -> Left e
            Right v -> Right $ Aggregated (TColumn v)
        Right (UnAggregated conditions) -> case sUnbox @a of
            STrue -> case mapColumn
                (\(c :: VU.Vector Bool) -> VU.map (\c' -> if c' then l else r) c)
                conditions of
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
            SFalse -> case mapColumn
                (\(c :: VU.Vector Bool) -> V.map (\c' -> if c' then l else r) (VU.convert c))
                conditions of
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
        Left (TypeMismatchException context) ->
            Left $
                TypeMismatchException
                    ( context
                        { callingFunctionName = Just "interpretAggregation"
                        , errorColumnName = Just (show cond)
                        }
                    )
        Left e -> Left e
interpretAggregation gdf expression@(If cond (Lit l) r) =
    case ( interpretAggregation @Bool gdf cond
         , interpretAggregation @a gdf r
         ) of
        ( Right (Aggregated (TColumn conditions))
            , Right (Aggregated (TColumn right))
            ) -> case zipWithColumns
                (\(c :: Bool) (r' :: a) -> if c then l else r')
                conditions
                right of
                Left e -> Left e
                Right v -> Right $ Aggregated (TColumn v)
        ( Right (UnAggregated conditions)
            , Right (UnAggregated right@(BoxedColumn (right' :: V.Vector c)))
            ) -> case testEquality (typeRep @(V.Vector a)) (typeRep @c) of
                Just Refl -> case zipWithColumns
                    ( \(c :: VU.Vector Bool) (r' :: V.Vector a) ->
                        V.zipWith
                            (\c' r'' -> if c' then l else r'')
                            (V.convert c)
                            r'
                    )
                    conditions
                    right of
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
                Nothing -> case testEquality (typeRep @(VU.Vector a)) (typeRep @c) of
                    Nothing -> Left $ nestedTypeException @c @a (show expression)
                    Just Refl -> case sUnbox @a of
                        SFalse -> Left $ InternalException "Boxed type in unboxed column"
                        STrue -> case zipWithColumns
                            ( \(c :: VU.Vector Bool) (r' :: VU.Vector a) ->
                                VU.zipWith
                                    (\c' r'' -> if c' then l else r'')
                                    c
                                    r'
                            )
                            conditions
                            right of
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
        (Right _, Right _) ->
            Left $
                AggregatedAndNonAggregatedException (T.pack $ show l) (T.pack $ show r)
        (Left (TypeMismatchException context), _) ->
            Left $
                TypeMismatchException
                    ( context
                        { callingFunctionName = Just "interpretAggregation"
                        , errorColumnName = Just (show cond)
                        }
                    )
        (Left e, _) -> Left e
        (_, Left (TypeMismatchException context)) ->
            Left $
                TypeMismatchException
                    ( context
                        { callingFunctionName = Just "interpretAggregation"
                        , errorColumnName = Just (show r)
                        }
                    )
        (_, Left e) -> Left e
interpretAggregation gdf expression@(If cond l (Lit r)) =
    case ( interpretAggregation @Bool gdf cond
         , interpretAggregation @a gdf l
         ) of
        ( Right (Aggregated (TColumn conditions))
            , Right (Aggregated (TColumn left))
            ) -> case zipWithColumns
                (\(c :: Bool) (l' :: a) -> if c then l' else r)
                conditions
                left of
                Left e -> Left e
                Right v -> Right $ Aggregated (TColumn v)
        ( Right (UnAggregated conditions)
            , Right (UnAggregated left@(BoxedColumn (left' :: V.Vector c)))
            ) -> case testEquality (typeRep @(V.Vector a)) (typeRep @c) of
                Just Refl -> case zipWithColumns
                    ( \(c :: VU.Vector Bool) (l' :: V.Vector a) ->
                        V.zipWith
                            (\c' l'' -> if c' then l'' else r)
                            (V.convert c)
                            l'
                    )
                    conditions
                    left of
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
                Nothing -> case testEquality (typeRep @(VU.Vector a)) (typeRep @c) of
                    Nothing -> Left $ nestedTypeException @c @a (show expression)
                    Just Refl -> case sUnbox @a of
                        SFalse -> Left $ InternalException "Boxed type in unboxed column"
                        STrue -> case zipWithColumns
                            ( \(c :: VU.Vector Bool) (l' :: VU.Vector a) ->
                                VU.zipWith
                                    (\c' l'' -> if c' then l'' else r)
                                    c
                                    l'
                            )
                            conditions
                            left of
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
        (Right _, Right _) ->
            Left $
                AggregatedAndNonAggregatedException (T.pack $ show l) (T.pack $ show r)
        (Left (TypeMismatchException context), _) ->
            Left $
                TypeMismatchException
                    ( context
                        { callingFunctionName = Just "interpretAggregation"
                        , errorColumnName = Just (show cond)
                        }
                    )
        (Left e, _) -> Left e
        (_, Left (TypeMismatchException context)) ->
            Left $
                TypeMismatchException
                    ( context
                        { callingFunctionName = Just "interpretAggregation"
                        , errorColumnName = Just (show r)
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
interpretAggregation gdf@(Grouped df names indices os) expression@(AggNumericVector (Col name) op (f :: VU.Vector b -> c)) =
    case getColumn name df of
        -- TODO(mchavinda): Fix the compedium of type errors here
        -- This is mostly done help with the benchmarking.
        Nothing -> Left $ ColumnNotFoundException name "" (M.keys $ columnIndices df)
        Just (BoxedColumn col) -> error "Type mismatch."
        Just (OptionalColumn col) -> error "Type mismatch."
        Just (UnboxedColumn (col :: VU.Vector d)) -> case testEquality (typeRep @b) (typeRep @d) of
            Just Refl -> case testEquality (typeRep @c) (typeRep @a) of
                Just Refl ->
                    Right $
                        Aggregated $
                            TColumn $
                                fromUnboxedVector $
                                    mkAggregatedColumnUnboxed col os indices f
                Nothing -> error "Type mismatch"
            Nothing -> error "Type mismatch"
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
interpretAggregation gdf@(Grouped df names indices os) expression@(AggReduce (Col name) op (f :: a -> a -> a)) =
    case getColumn name df of
        Nothing -> Left $ ColumnNotFoundException name "" (M.keys $ columnIndices df)
        Just (BoxedColumn (col :: V.Vector d)) -> case testEquality (typeRep @a) (typeRep @d) of
            Nothing -> error "Type mismatch"
            Just Refl ->
                Right $
                    Aggregated $
                        TColumn $
                            fromVector $
                                mkReducedColumnBoxed col os indices f
        Just (OptionalColumn (col :: V.Vector d)) -> case testEquality (typeRep @a) (typeRep @d) of
            Nothing -> error "Type mismatch"
            Just Refl ->
                Right $
                    Aggregated $
                        TColumn $
                            fromVector $
                                mkReducedColumnBoxed col os indices f
        Just (UnboxedColumn (col :: VU.Vector d)) -> case testEquality (typeRep @a) (typeRep @d) of
            Just Refl ->
                Right $
                    Aggregated $
                        TColumn $
                            fromUnboxedVector $
                                mkReducedColumnUnboxed col os indices f
            Nothing -> error "Type mismatch"
interpretAggregation gdf@(Grouped df names indices os) expression@(AggReduce expr op (f :: a -> a -> a)) =
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
                                        V.map (VU.foldl1' f) col
                    SFalse -> Left $ InternalException "Boxed type inside an unboxed column"
            Just Refl ->
                Right $
                    Aggregated $
                        TColumn $
                            fromVector $
                                V.map (VG.foldl1' f) col
        Right (UnAggregated _) -> Left $ InternalException "Aggregated into non-boxed column"
        Right (Aggregated (TColumn column)) -> case foldl1Column f column of
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
        Right (Aggregated (TColumn column)) -> case foldlColumn f s column of
            Left e -> Left e
            Right value -> interpretAggregation @a gdf (Lit value)

instance (Num a, Columnable a) => Num (Expr a) where
    (+) :: Expr a -> Expr a -> Expr a
    (+) (Lit x) (Lit y) = Lit (x + y)
    (+) e1 e2
        | e1 == e2 = BinaryOp "mult" (*) e1 (Lit 2)
        | otherwise = BinaryOp "add" (+) e1 e2

    (-) :: Expr a -> Expr a -> Expr a
    (-) (Lit x) (Lit y) = Lit (x - y)
    (-) e1 e2 = BinaryOp "sub" (-) e1 e2

    (*) :: Expr a -> Expr a -> Expr a
    (*) (Lit 0) _ = Lit 0
    (*) _ (Lit 0) = Lit 0
    (*) (Lit 1) e = e
    (*) e (Lit 1) = e
    (*) (Lit x) (Lit y) = Lit (x * y)
    (*) e1 e2
        | e1 == e2 = UnaryOp "pow 2" (^ 2) e1
        | otherwise = BinaryOp "mult" (*) e1 e2

    fromInteger :: Integer -> Expr a
    fromInteger = Lit . fromInteger

    negate :: Expr a -> Expr a
    negate (Lit n) = Lit (negate n)
    negate expr = UnaryOp "negate" negate expr

    abs :: (Num a) => Expr a -> Expr a
    abs (Lit n) = Lit (abs n)
    abs expr = UnaryOp "abs" abs expr

    signum :: (Num a) => Expr a -> Expr a
    signum (Lit n) = Lit (signum n)
    signum expr = UnaryOp "signum" signum expr

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
    (/) (Lit l1) (Lit l2) = Lit (l1 / l2)
    (/) e1 e2 = BinaryOp "divide" (/) e1 e2

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

normalize :: (Eq a, Ord a, Show a, Typeable a) => Expr a -> Expr a
normalize expr = case expr of
    Col name -> Col name
    Lit val -> Lit val
    If cond th el -> If (normalize cond) (normalize th) (normalize el)
    UnaryOp name f e -> UnaryOp name f (normalize e)
    BinaryOp name f e1 e2
        | isCommutative name ->
            let n1 = normalize e1
                n2 = normalize e2
             in case testEquality (typeOf n1) (typeOf n2) of
                    Nothing -> expr
                    Just Refl ->
                        if compareExpr n1 n2 == GT
                            then BinaryOp name f n2 n1 -- Swap to canonical order
                            else BinaryOp name f n1 n2
        | otherwise -> BinaryOp name f (normalize e1) (normalize e2)
    AggVector e name f -> AggVector (normalize e) name f
    AggReduce e name f -> AggReduce (normalize e) name f
    AggNumericVector e name f -> AggNumericVector (normalize e) name f
    AggFold e name init f -> AggFold (normalize e) name init f

isCommutative :: T.Text -> Bool
isCommutative name =
    name
        `elem` [ "add"
               , "mult"
               , "min"
               , "max"
               , "eq"
               , "and"
               , "or"
               ]

-- Compare expressions for ordering (used in normalization)
compareExpr :: Expr a -> Expr a -> Ordering
compareExpr e1 e2 = compare (exprKey e1) (exprKey e2)
  where
    exprKey :: Expr a -> String
    exprKey (Col name) = "0:" ++ T.unpack name
    exprKey (Lit val) = "1:" ++ show val
    exprKey (If c t e) = "2:" ++ exprKey c ++ exprKey t ++ exprKey e
    exprKey (UnaryOp name _ e) = "3:" ++ T.unpack name ++ exprKey e
    exprKey (BinaryOp name _ e1 e2) = "4:" ++ T.unpack name ++ exprKey e1 ++ exprKey e2
    exprKey (AggVector e name _) = "5:" ++ T.unpack name ++ exprKey e
    exprKey (AggReduce e name _) = "6:" ++ T.unpack name ++ exprKey e
    exprKey (AggNumericVector e name _) = "7:" ++ T.unpack name ++ exprKey e
    exprKey (AggFold e name _ _) = "8:" ++ T.unpack name ++ exprKey e

instance (Eq a, Columnable a) => Eq (Expr a) where
    (==) l r = eqNormalized (normalize l) (normalize r)
      where
        exprEq :: (Columnable b, Columnable c) => Expr b -> Expr c -> Bool
        exprEq e1 e2 = case testEquality (typeOf e1) (typeOf e2) of
            Just Refl -> e1 == e2
            Nothing -> False
        eqNormalized :: Expr a -> Expr a -> Bool
        eqNormalized (Col n1) (Col n2) = n1 == n2
        eqNormalized (Lit v1) (Lit v2) = v1 == v2
        eqNormalized (If c1 t1 e1) (If c2 t2 e2) =
            c1 == c2 && t1 `exprEq` t2 && e1 `exprEq` e2
        eqNormalized (UnaryOp n1 _ e1) (UnaryOp n2 _ e2) =
            n1 == n2 && e1 `exprEq` e2
        eqNormalized (BinaryOp n1 _ e1a e1b) (BinaryOp n2 _ e2a e2b) =
            n1 == n2 && e1a `exprEq` e2a && e1b `exprEq` e2b
        eqNormalized (AggVector e1 n1 _) (AggVector e2 n2 _) =
            n1 == n2 && e1 `exprEq` e2
        eqNormalized (AggReduce e1 n1 _) (AggReduce e2 n2 _) =
            n1 == n2 && e1 `exprEq` e2
        eqNormalized (AggNumericVector e1 n1 _) (AggNumericVector e2 n2 _) =
            n1 == n2 && e1 `exprEq` e2
        eqNormalized (AggFold e1 n1 i1 _) (AggFold e2 n2 i2 _) =
            n1 == n2 && e1 `exprEq` e2 && i1 == i2
        eqNormalized _ _ = False

instance (Ord a, Columnable a) => Ord (Expr a) where
    compare :: Expr a -> Expr a -> Ordering
    compare e1 e2 = case (e1, e2) of
        (Col n1, Col n2) -> compare n1 n2
        (Lit v1, Lit v2) -> compare v1 v2
        (If c1 t1 e1', If c2 t2 e2') ->
            compare c1 c2 <> exprComp t1 t2 <> exprComp e1' e2'
        (UnaryOp n1 _ e1', UnaryOp n2 _ e2') ->
            compare n1 n2 <> exprComp e1' e2'
        (BinaryOp n1 _ a1 b1, BinaryOp n2 _ a2 b2) ->
            compare n1 n2 <> exprComp a1 a2 <> exprComp b1 b2
        (AggVector e1' n1 _, AggVector e2' n2 _) ->
            compare n1 n2 <> exprComp e1' e2'
        (AggReduce e1' n1 _, AggReduce e2' n2 _) ->
            compare n1 n2 <> exprComp e1' e2'
        (AggNumericVector e1' n1 _, AggNumericVector e2' n2 _) ->
            compare n1 n2 <> exprComp e1' e2'
        (AggFold e1' n1 i1 _, AggFold e2' n2 i2 _) ->
            compare n1 n2 <> exprComp e1' e2' <> compare i1 i2
        -- Different constructors - compare by priority
        (Col _, _) -> LT
        (_, Col _) -> GT
        (Lit _, _) -> LT
        (_, Lit _) -> GT
        (UnaryOp{}, _) -> LT
        (_, UnaryOp{}) -> GT
        (BinaryOp{}, _) -> LT
        (_, BinaryOp{}) -> GT
        (If{}, _) -> LT
        (_, If{}) -> GT
        (AggVector{}, _) -> LT
        (_, AggVector{}) -> GT
        (AggReduce{}, _) -> LT
        (_, AggReduce{}) -> GT
        (AggNumericVector{}, _) -> LT
        (_, AggNumericVector{}) -> GT

exprComp :: (Columnable b, Columnable c) => Expr b -> Expr c -> Ordering
exprComp e1 e2 = case testEquality (typeOf e1) (typeOf e2) of
    Just Refl -> e1 `compare` e2
    Nothing -> LT

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

-- Helpers
mkTypeMismatchException ::
    (Typeable a, Typeable b) =>
    Maybe String -> Maybe String -> TypeErrorContext a b -> DataFrameException
mkTypeMismatchException callPoint errorLocation context =
    TypeMismatchException
        ( context
            { callingFunctionName = callPoint
            , errorColumnName = errorLocation
            }
        )

handleInterpretException :: String -> DataFrameException -> DataFrameException
handleInterpretException errorLocation (TypeMismatchException context) = mkTypeMismatchException (Just "interpret") (Just errorLocation) context
handleInterpretException _ e = e

numRows :: DataFrame -> Int
numRows df = fst (dataframeDimensions df)

mkUnaggregatedColumnBoxed ::
    forall a.
    (Columnable a) =>
    V.Vector a -> VU.Vector Int -> VU.Vector Int -> V.Vector (V.Vector a)
mkUnaggregatedColumnBoxed col os indices =
    let
        sorted = V.unsafeBackpermute col (V.convert indices)
        n i = os `VG.unsafeIndex` (i + 1) - (os `VG.unsafeIndex` i)
        start i = os `VG.unsafeIndex` i
     in
        V.generate
            (VU.length os - 1)
            ( \i ->
                V.unsafeSlice (start i) (n i) sorted
            )

mkUnaggregatedColumnUnboxed ::
    forall a.
    (Columnable a, VU.Unbox a) =>
    VU.Vector a -> VU.Vector Int -> VU.Vector Int -> V.Vector (VU.Vector a)
mkUnaggregatedColumnUnboxed col os indices =
    let
        sorted = VU.unsafeBackpermute col indices
        n i = os `VU.unsafeIndex` (i + 1) - (os `VU.unsafeIndex` i)
        start i = os `VG.unsafeIndex` i
     in
        V.generate
            (VU.length os - 1)
            ( \i ->
                VU.unsafeSlice (start i) (n i) sorted
            )

mkAggregatedColumnUnboxed ::
    forall a b.
    (Columnable a, VU.Unbox a, Columnable b, VU.Unbox b) =>
    VU.Vector a ->
    VU.Vector Int ->
    VU.Vector Int ->
    (VU.Vector a -> b) ->
    VU.Vector b
mkAggregatedColumnUnboxed col os indices f =
    let
        sorted = VU.unsafeBackpermute col indices
        n i = os `VU.unsafeIndex` (i + 1) - (os `VU.unsafeIndex` i)
        start i = os `VG.unsafeIndex` i
     in
        VU.generate
            (VU.length os - 1)
            ( \i ->
                f (VU.unsafeSlice (start i) (n i) sorted)
            )

mkReducedColumnUnboxed ::
    forall a.
    (VU.Unbox a) =>
    VU.Vector a ->
    VU.Vector Int ->
    VU.Vector Int ->
    (a -> a -> a) ->
    VU.Vector a
mkReducedColumnUnboxed col os indices f = runST $ do
    let len = VU.length os - 1
    mvec <- VUM.unsafeNew len

    let loopOut i
            | i == len = return ()
            | otherwise = do
                let start = os `VU.unsafeIndex` i
                let end = os `VU.unsafeIndex` (i + 1)
                let initVal = col `VU.unsafeIndex` (indices `VU.unsafeIndex` start)

                let loopIn !acc idx
                        | idx == end = acc
                        | otherwise =
                            let val = col `VU.unsafeIndex` (indices `VU.unsafeIndex` idx)
                             in loopIn (f acc val) (idx + 1)
                let !finalVal = loopIn initVal (start + 1)
                VUM.unsafeWrite mvec i finalVal
                loopOut (i + 1)

    loopOut 0
    VU.unsafeFreeze mvec
{-# INLINE mkReducedColumnUnboxed #-}

mkReducedColumnBoxed ::
    V.Vector a ->
    VU.Vector Int ->
    VU.Vector Int ->
    (a -> a -> a) ->
    V.Vector a
mkReducedColumnBoxed col os indices f = runST $ do
    let len = VU.length os - 1
    mvec <- VM.unsafeNew len

    let loopOut i
            | i == len = return ()
            | otherwise = do
                let start = os `VU.unsafeIndex` i
                let end = os `VU.unsafeIndex` (i + 1)
                let initVal = col `V.unsafeIndex` (indices `VU.unsafeIndex` start)

                let loopIn !acc idx
                        | idx == end = acc
                        | otherwise =
                            let val = col `V.unsafeIndex` (indices `VU.unsafeIndex` idx)
                             in loopIn (f acc val) (idx + 1)
                let !finalVal = loopIn initVal (start + 1)
                VM.unsafeWrite mvec i finalVal
                loopOut (i + 1)

    loopOut 0
    V.unsafeFreeze mvec
{-# INLINE mkReducedColumnBoxed #-}

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
