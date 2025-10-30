{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module DataFrame.Internal.DataFrame where

import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU

import Control.Exception (throw)
import Data.Function (on)
import Data.List (sortBy, transpose, (\\))
import Data.Type.Equality (TestEquality (testEquality), type (:~:) (Refl))
import DataFrame.Display.Terminal.PrettyPrint
import DataFrame.Errors
import DataFrame.Internal.Column
import Text.Printf
import Type.Reflection (typeRep)

data DataFrame = DataFrame
    { columns :: V.Vector Column
    {- ^ Our main data structure stores a dataframe as
    a vector of columns. This improv
    -}
    , columnIndices :: M.Map T.Text Int
    -- ^ Keeps the column names in the order they were inserted in.
    , dataframeDimensions :: (Int, Int)
    -- ^ (rows, columns)
    }

{- | A record that contains information about how and what
rows are grouped in the dataframe. This can only be used with
`aggregate`.
-}
data GroupedDataFrame = Grouped
    { fullDataframe :: DataFrame
    , groupedColumns :: [T.Text]
    , valueIndices :: VU.Vector Int
    , offsets :: VU.Vector Int
    }

instance Show GroupedDataFrame where
    show (Grouped df cols indices os) =
        printf
            "{ keyColumns: %s groupedColumns: %s }"
            (show cols)
            (show (M.keys (columnIndices df) \\ cols))

instance Eq GroupedDataFrame where
    (==) (Grouped df cols indices os) (Grouped df' cols' indices' os') = (df == df') && (cols == cols')

instance Eq DataFrame where
    (==) :: DataFrame -> DataFrame -> Bool
    a == b =
        M.keys (columnIndices a) == M.keys (columnIndices b)
            && foldr
                ( \(name, index) acc -> acc && (columns a V.!? index == (columns b V.!? (columnIndices b M.! name)))
                )
                True
                (M.toList $ columnIndices a)

instance Show DataFrame where
    show :: DataFrame -> String
    show d =
        let
            d' =
                d
                    { columns = V.map (takeColumn 10) (columns d)
                    , dataframeDimensions = (10, snd (dataframeDimensions d))
                    }
         in
            T.unpack (asText d' False)
                ++ "\n"
                ++ "Showing "
                ++ show (min 10 (fst (dataframeDimensions d)))
                ++ " rows out of "
                ++ show (fst (dataframeDimensions d))

-- | For showing the dataframe as markdown in notebooks.
toMarkdownTable :: DataFrame -> T.Text
toMarkdownTable df = asText df True

asText :: DataFrame -> Bool -> T.Text
asText d properMarkdown =
    let header = map fst (sortBy (compare `on` snd) $ M.toList (columnIndices d))
        types = V.toList $ V.filter (/= "") $ V.map getType (columns d)
        getType :: Column -> T.Text
        getType (BoxedColumn (column :: V.Vector a)) = T.pack $ show (typeRep @a)
        getType (UnboxedColumn (column :: VU.Vector a)) = T.pack $ show (typeRep @a)
        getType (OptionalColumn (column :: V.Vector a)) = T.pack $ show (typeRep @a)
        -- Separate out cases dynamically so we don't end up making round trip string
        -- copies.
        get :: Maybe Column -> V.Vector T.Text
        get (Just (BoxedColumn (column :: V.Vector a))) = case testEquality (typeRep @a) (typeRep @T.Text) of
            Just Refl -> column
            Nothing -> case testEquality (typeRep @a) (typeRep @String) of
                Just Refl -> V.map T.pack column
                Nothing -> V.map (T.pack . show) column
        get (Just (UnboxedColumn column)) = V.map (T.pack . show) (V.convert column)
        get (Just (OptionalColumn column)) = V.map (T.pack . show) column
        get Nothing = V.empty
        getTextColumnFromFrame df (i, name) = get $ (V.!?) (columns d) ((M.!) (columnIndices d) name)
        rows =
            transpose $
                zipWith (curry (V.toList . getTextColumnFromFrame d)) [0 ..] header
     in showTable properMarkdown header types rows

-- | O(1) Creates an empty dataframe
empty :: DataFrame
empty =
    DataFrame
        { columns = V.empty
        , columnIndices = M.empty
        , dataframeDimensions = (0, 0)
        }

{- | Safely retrieves a column by name from the dataframe.

Returns 'Nothing' if the column does not exist.

==== __Examples__

>>> getColumn "age" df
Just (UnboxedColumn ...)

>>> getColumn "nonexistent" df
Nothing
-}
getColumn :: T.Text -> DataFrame -> Maybe Column
getColumn name df = do
    i <- columnIndices df M.!? name
    columns df V.!? i

{- | Retrieves a column by name from the dataframe, throwing an exception if not found.

This is an unsafe version of 'getColumn' that throws 'ColumnNotFoundException'
if the column does not exist. Use this when you are certain the column exists.

==== __Throws__

* 'ColumnNotFoundException' - if the column with the given name does not exist
-}
unsafeGetColumn :: T.Text -> DataFrame -> Column
unsafeGetColumn name df = case getColumn name df of
    Nothing -> throw $ ColumnNotFoundException name "" (M.keys $ columnIndices df)
    Just col -> col

{- | Checks if the dataframe is empty (has no columns).

Returns 'True' if the dataframe has no columns, 'False' otherwise.
Note that a dataframe with columns but no rows is not considered null.
-}
null :: DataFrame -> Bool
null df = V.null (columns df)

{- | Returns a dataframe as a two dimensional vector of floats.

Converts all columns in the dataframe to float vectors and transposes them
into a row-major matrix representation.

This is useful for handing data over into ML systems.

Returns 'Left' with an error if any column cannot be converted to floats.
-}
toFloatMatrix ::
    DataFrame -> Either DataFrameException (V.Vector (VU.Vector Float))
toFloatMatrix df = case V.foldl'
    (\acc c -> V.snoc <$> acc <*> toFloatVector c)
    (Right V.empty :: Either DataFrameException (V.Vector (VU.Vector Float)))
    (columns df) of
    Left e -> Left e
    Right m ->
        pure $
            V.generate
                (fst (dataframeDimensions df))
                ( \i ->
                    foldl
                        (\acc j -> acc `VU.snoc` ((m VG.! j) VG.! i))
                        VU.empty
                        [0 .. (V.length m - 1)]
                )

{- | Returns a dataframe as a two dimensional vector of doubles.

Converts all columns in the dataframe to double vectors and transposes them
into a row-major matrix representation.

This is useful for handing data over into ML systems.

Returns 'Left' with an error if any column cannot be converted to doubles.
-}
toDoubleMatrix ::
    DataFrame -> Either DataFrameException (V.Vector (VU.Vector Double))
toDoubleMatrix df = case V.foldl'
    (\acc c -> V.snoc <$> acc <*> toDoubleVector c)
    (Right V.empty :: Either DataFrameException (V.Vector (VU.Vector Double)))
    (columns df) of
    Left e -> Left e
    Right m ->
        pure $
            V.generate
                (fst (dataframeDimensions df))
                ( \i ->
                    foldl
                        (\acc j -> acc `VU.snoc` ((m VG.! j) VG.! i))
                        VU.empty
                        [0 .. (V.length m - 1)]
                )

{- | Returns a dataframe as a two dimensional vector of ints.

Converts all columns in the dataframe to int vectors and transposes them
into a row-major matrix representation.

This is useful for handing data over into ML systems.

Returns 'Left' with an error if any column cannot be converted to ints.
-}
toIntMatrix :: DataFrame -> Either DataFrameException (V.Vector (VU.Vector Int))
toIntMatrix df = case V.foldl'
    (\acc c -> V.snoc <$> acc <*> toIntVector c)
    (Right V.empty :: Either DataFrameException (V.Vector (VU.Vector Int)))
    (columns df) of
    Left e -> Left e
    Right m ->
        pure $
            V.generate
                (fst (dataframeDimensions df))
                ( \i ->
                    foldl
                        (\acc j -> acc `VU.snoc` ((m VG.! j) VG.! i))
                        VU.empty
                        [0 .. (V.length m - 1)]
                )

{- | Get a specific column as a vector.

You must specify the type via type applications.

==== __Examples__

>>> columnAsVector @Int "age" df
[25, 30, 35, ...]

>>> columnAsVector @Text "name" df
["Alice", "Bob", "Charlie", ...]

==== __Throws__

* 'error' - if the column type doesn't match the requested type
-}
columnAsVector :: forall a. (Columnable a) => T.Text -> DataFrame -> V.Vector a
columnAsVector name df = case unsafeGetColumn name df of
    (BoxedColumn (col :: V.Vector b)) -> case testEquality (typeRep @a) (typeRep @b) of
        Nothing -> error "Type error"
        Just Refl -> col
    (OptionalColumn (col :: V.Vector b)) -> case testEquality (typeRep @a) (typeRep @b) of
        Nothing -> error "Type error"
        Just Refl -> col
    (UnboxedColumn (col :: VU.Vector b)) -> case testEquality (typeRep @a) (typeRep @b) of
        Nothing -> error "Type error"
        Just Refl -> VG.convert col

{- | Retrieves a column as an unboxed vector of 'Int' values.

Returns 'Left' with a 'DataFrameException' if the column cannot be converted to ints.
This may occur if the column contains non-numeric data or values outside the 'Int' range.
-}
columnAsIntVector ::
    T.Text -> DataFrame -> Either DataFrameException (VU.Vector Int)
columnAsIntVector name df = toIntVector (unsafeGetColumn name df)

{- | Retrieves a column as an unboxed vector of 'Double' values.

Returns 'Left' with a 'DataFrameException' if the column cannot be converted to doubles.
This may occur if the column contains non-numeric data.
-}
columnAsDoubleVector ::
    T.Text -> DataFrame -> Either DataFrameException (VU.Vector Double)
columnAsDoubleVector name df = toDoubleVector (unsafeGetColumn name df)

{- | Retrieves a column as an unboxed vector of 'Float' values.

Returns 'Left' with a 'DataFrameException' if the column cannot be converted to floats.
This may occur if the column contains non-numeric data.
-}
columnAsFloatVector ::
    T.Text -> DataFrame -> Either DataFrameException (VU.Vector Float)
columnAsFloatVector name df = toFloatVector (unsafeGetColumn name df)
