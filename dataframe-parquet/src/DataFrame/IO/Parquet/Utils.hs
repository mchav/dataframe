{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

module DataFrame.IO.Parquet.Utils (
    ColumnDescription (..),
    generateColumnDescriptions,
    getColumnNames,
    foldNonNullable,
    foldNonNullableUnboxed,
    foldNullable,
    foldNullableUnboxed,
    foldRepeated,
    foldRepeatedUnboxed,
) where

import Control.Monad.IO.Class (MonadIO (..))
import Data.Int (Int32)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Vector as VB
import qualified Data.Vector.Mutable as VBM
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import Data.Word (Word8)
import DataFrame.IO.Parquet.Levels (
    stitchList,
    stitchList2,
    stitchList3,
 )
import DataFrame.IO.Parquet.Thrift (
    ConvertedType (..),
    FieldRepetitionType (..),
    LogicalType (..),
    SchemaElement (..),
    ThriftType,
    unField,
 )
import DataFrame.Internal.Column (
    Column (..),
    Columnable,
    fromList,
 )
import DataFrame.Internal.Column.Bitmap (buildBitmapFromValid)

-- | A left-fold driver over a column's per-page triples.
type PageFold m v a =
    forall acc.
    (acc -> (v a, VU.Vector Int, VU.Vector Int) -> m acc) -> acc -> m acc

data ColumnDescription = ColumnDescription
    { colElementType :: !(Maybe ThriftType)
    , maxDefinitionLevel :: !Int32
    , maxRepetitionLevel :: !Int32
    , colLogicalType :: !(Maybe LogicalType)
    , colConvertedType :: !(Maybe ConvertedType)
    , typeLength :: !(Maybe Int32)
    }
    deriving (Show, Eq)

levelContribution :: Maybe FieldRepetitionType -> (Int, Int)
levelContribution = \case
    Just (REPEATED _) -> (1, 1)
    Just (OPTIONAL _) -> (1, 0)
    _ -> (0, 0) -- REQUIRED or absent

data SchemaTree = SchemaTree SchemaElement [SchemaTree]

buildTree :: [SchemaElement] -> (SchemaTree, [SchemaElement])
buildTree [] = error "buildTree: schema ended unexpectedly"
buildTree (se : rest) =
    let n = fromIntegral $ fromMaybe 0 (unField (num_children se)) :: Int
        (children, rest') = buildChildren n rest
     in (SchemaTree se children, rest')

-- | Build a forest of sibling trees from a flat depth-first element list.
buildForest :: [SchemaElement] -> ([SchemaTree], [SchemaElement])
buildForest [] = ([], [])
buildForest xs =
    let (tree, rest') = buildTree xs
        (siblings, rest'') = buildForest rest'
     in (tree : siblings, rest'')

-- | Build exactly @n@ child trees, each consuming only its own subtree.
buildChildren :: Int -> [SchemaElement] -> ([SchemaTree], [SchemaElement])
buildChildren 0 xs = ([], xs)
buildChildren n xs =
    let (child, rest') = buildTree xs
        (siblings, rest'') = buildChildren (n - 1) rest'
     in (child : siblings, rest'')

collectLeaves :: Int -> Int -> SchemaTree -> [ColumnDescription]
collectLeaves defAcc repAcc (SchemaTree se children) =
    let (dInc, rInc) = levelContribution (unField (repetition_type se))
        defLevel = defAcc + dInc
        repLevel = repAcc + rInc
     in case children of
            [] ->
                -- leaf: emit a description
                let pType = unField (schematype se)
                 in [ ColumnDescription
                        pType
                        (fromIntegral defLevel)
                        (fromIntegral repLevel)
                        (unField (logicalType se))
                        (unField (converted_type se))
                        (unField (type_length se))
                    ]
            _ ->
                -- internal node: recurse into children
                concatMap (collectLeaves defLevel repLevel) children

generateColumnDescriptions :: [SchemaElement] -> [ColumnDescription]
generateColumnDescriptions [] = []
generateColumnDescriptions (_ : rest) =
    -- drop schema root
    let (forest, _) = buildForest rest
     in concatMap (collectLeaves 0 0) forest

getColumnNames :: [SchemaElement] -> [Text]
getColumnNames [] = []
getColumnNames schemaElements =
    let (forest, _) = buildForest schemaElements
     in go forest [] False
  where
    isRepeated se = case unField (repetition_type se) of
        Just (REPEATED _) -> True
        _ -> False

    go [] _ _ = []
    go (SchemaTree se children : rest) path skipThis =
        case children of
            -- Leaf node
            [] ->
                let newPath = if skipThis then path else path ++ [unField (name se)]
                    fullName = T.intercalate "." newPath
                 in fullName : go rest path skipThis
            -- REPEATED intermediate: skip this name; skip single child too
            _
                | isRepeated se ->
                    let skipChildren = length children == 1
                        childLeaves = go children path skipChildren
                     in childLeaves ++ go rest path skipThis
            -- Name-skipped intermediate: recurse with skip cleared
            _
                | skipThis ->
                    let childLeaves = go children path False
                     in childLeaves ++ go rest path skipThis
            -- Normal intermediate: add name to path, recurse
            _ ->
                let subPath = path ++ [unField (name se)]
                    childLeaves = go children subPath False
                 in childLeaves ++ go rest path skipThis

-- | Fold a column's value pages into a non-nullable 'Column'.
{-# INLINEABLE foldNonNullable #-}
foldNonNullable ::
    forall m a.
    (MonadIO m, Columnable a) =>
    Int ->
    PageFold m VB.Vector a ->
    m Column
foldNonNullable totalRows runPages = do
    mv <- liftIO $ VBM.unsafeNew totalRows
    _ <-
        runPages
            ( \off (chunk, _, _) -> liftIO $ do
                let n = VB.length chunk
                VB.copy (VBM.unsafeSlice off n mv) chunk
                return (off + n)
            )
            (0 :: Int)
    v <- liftIO $ VB.unsafeFreeze mv
    return (BoxedColumn Nothing v)

{-# INLINEABLE foldNonNullableUnboxed #-}
foldNonNullableUnboxed ::
    forall m a.
    (MonadIO m, Columnable a, VU.Unbox a) =>
    Int ->
    PageFold m VU.Vector a ->
    m Column
foldNonNullableUnboxed totalRows runPages = do
    mv <- liftIO $ VUM.unsafeNew totalRows
    _ <-
        runPages
            ( \off (chunk, _, _) -> liftIO $ do
                let n = VU.length chunk
                VU.unsafeCopy (VUM.unsafeSlice off n mv) chunk
                return (off + n)
            )
            (0 :: Int)
    dat <- liftIO $ VU.unsafeFreeze mv
    return (UnboxedColumn Nothing dat)

-- | Fold a column's value and def-level pages into a nullable 'Column'.
{-# INLINEABLE foldNullable #-}
foldNullable ::
    forall m a.
    (MonadIO m, Columnable a) =>
    Int ->
    Int ->
    PageFold m VB.Vector a ->
    m Column
foldNullable maxDef totalRows runPages = do
    -- The '!v' bang forces each value so decoder errors surface immediately.
    mvDat <-
        liftIO $ VBM.replicate totalRows (error "parquet: null slot accessed")
    mvValid <- liftIO (VUM.new totalRows :: IO (VUM.IOVector Word8))
    (_, hasNull) <-
        runPages
            ( \(rowOff, anyNull) (vals, defs, _) -> liftIO $ do
                let nDefs = VU.length defs
                    go i j acc
                        | i >= nDefs = return acc
                        | VU.unsafeIndex defs i == maxDef = do
                            let !v = VB.unsafeIndex vals j
                            VBM.unsafeWrite mvDat (rowOff + i) v
                            VUM.unsafeWrite mvValid (rowOff + i) 1
                            go (i + 1) (j + 1) acc
                        | otherwise = go (i + 1) j True
                newNull <- go 0 0 False
                return (rowOff + nDefs, anyNull || newNull)
            )
            (0 :: Int, False)
    dat <- liftIO $ VB.unsafeFreeze mvDat
    maybeBm <-
        if hasNull
            then do
                validV <- liftIO $ VU.unsafeFreeze mvValid
                return (Just (buildBitmapFromValid validV))
            else return Nothing
    return (BoxedColumn maybeBm dat)

{-# INLINEABLE foldNullableUnboxed #-}
foldNullableUnboxed ::
    forall m a.
    (MonadIO m, Columnable a, VU.Unbox a) =>
    Int ->
    Int ->
    PageFold m VU.Vector a ->
    m Column
foldNullableUnboxed maxDef totalRows runPages = do
    -- zero-init means null slots silently hold 0, guarded by bitmap.
    mvDat <- liftIO $ VUM.new totalRows
    mvValid <- liftIO (VUM.new totalRows :: IO (VUM.IOVector Word8))
    (_, hasNull) <-
        runPages
            ( \(rowOff, anyNull) (vals, defs, _) -> liftIO $ do
                let !nDefs = VU.length defs
                    go !i !j !acc
                        | i >= nDefs = pure acc
                        | VU.unsafeIndex defs i == maxDef = do
                            VUM.unsafeWrite mvDat (rowOff + i) (VU.unsafeIndex vals j)
                            VUM.unsafeWrite mvValid (rowOff + i) 1
                            go (i + 1) (j + 1) acc
                        | otherwise = go (i + 1) j True
                !newNull <- go 0 0 False
                return (rowOff + nDefs, anyNull || newNull)
            )
            (0 :: Int, False)
    dat <- liftIO $ VU.unsafeFreeze mvDat
    maybeBm <-
        if hasNull
            then do
                validV <- liftIO $ VU.unsafeFreeze mvValid
                return (Just (buildBitmapFromValid validV))
            else return Nothing
    return (UnboxedColumn maybeBm dat)

-- | Fold page triples into a repeated (list) 'Column' by level stitching.
{-# INLINEABLE foldRepeated #-}
foldRepeated ::
    forall m a.
    ( MonadIO m
    , Columnable a
    , Columnable (Maybe [Maybe a])
    , Columnable (Maybe [Maybe [Maybe a]])
    , Columnable (Maybe [Maybe [Maybe [Maybe a]]])
    ) =>
    Int ->
    Int ->
    PageFold m VB.Vector a ->
    m Column
foldRepeated maxRep maxDef runPages = do
    chunks <- collectPages runPages
    let allVals = VB.concat [vs | (vs, _, _) <- chunks]
        allDefs = VU.concat [ds | (_, ds, _) <- chunks]
        allReps = VU.concat [rs | (_, _, rs) <- chunks]
    return $ case maxRep of
        2 -> fromList (stitchList2 (maxDef - 2) maxDef allReps allDefs allVals)
        3 ->
            fromList (stitchList3 (maxDef - 4) (maxDef - 2) maxDef allReps allDefs allVals)
        _ -> fromList (stitchList maxDef allReps allDefs allVals)

{-# INLINEABLE foldRepeatedUnboxed #-}
foldRepeatedUnboxed ::
    forall m a.
    ( MonadIO m
    , Columnable a
    , VU.Unbox a
    , Columnable (Maybe [Maybe a])
    , Columnable (Maybe [Maybe [Maybe a]])
    , Columnable (Maybe [Maybe [Maybe [Maybe a]]])
    ) =>
    Int ->
    Int ->
    PageFold m VU.Vector a ->
    m Column
foldRepeatedUnboxed maxRep maxDef runPages = do
    chunks <- collectPages runPages
    let allVals = VB.convert $ VU.concat [vs | (vs, _, _) <- chunks]
        allDefs = VU.concat [ds | (_, ds, _) <- chunks]
        allReps = VU.concat [rs | (_, _, rs) <- chunks]
    return $ case maxRep of
        2 -> fromList (stitchList2 (maxDef - 2) maxDef allReps allDefs allVals)
        3 ->
            fromList (stitchList3 (maxDef - 4) (maxDef - 2) maxDef allReps allDefs allVals)
        _ -> fromList (stitchList maxDef allReps allDefs allVals)

-- | Collect all of a column's page triples into a list, in page order.
collectPages ::
    (Monad m) =>
    PageFold m v a ->
    m [(v a, VU.Vector Int, VU.Vector Int)]
collectPages runPages = reverse <$> runPages (\acc triple -> return (triple : acc)) []
