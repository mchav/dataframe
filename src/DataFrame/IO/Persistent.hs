{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}

module DataFrame.IO.Persistent (
    -- * Main functions
    fromPersistent,
    fromPersistentWith,
    toPersistent,

    -- * Configuration
    PersistentConfig (..),
    defaultPersistentConfig,

    -- * Type classes
    EntityToDataFrame (..),
    DataFrameToEntity (..),

    -- * Utilities
    entityToColumns,
    columnsToEntity,
) where

import Control.Monad (forM, forM_)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Reader (ReaderT)
import Data.Int (Int64)
import Data.List (transpose)
import Data.Maybe (catMaybes, fromMaybe)
import Data.Proxy (Proxy (..))
import qualified Data.Map.Strict as M
import Data.Text (Text)
import qualified Data.Text as T
import Data.Time (Day, UTCTime)
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import Database.Persist
import Database.Persist.Sql hiding (Column)
import qualified DataFrame.Internal.Column as DFCol
import DataFrame.Internal.DataFrame (DataFrame (..))
import qualified DataFrame.Internal.DataFrame as DF
import Type.Reflection (Typeable, typeRep)

-- | Get number of rows in a DataFrame
nRows :: DataFrame -> Int
nRows df = fst (DF.dataframeDimensions df)

-- | Configuration for Persistent DataFrame operations
data PersistentConfig = PersistentConfig
    { pcBatchSize :: Int
    -- ^ Number of records to fetch at once (default: 10000)
    , pcIncludeId :: Bool
    -- ^ Whether to include the entity ID as a column (default: True)
    , pcIdColumnName :: Text
    -- ^ Name for the ID column (default: "id")
    }

-- | Default configuration
defaultPersistentConfig :: PersistentConfig
defaultPersistentConfig =
    PersistentConfig
        { pcBatchSize = 10000
        , pcIncludeId = True
        , pcIdColumnName = "id"
        }

-- | Type class for converting Persistent entities to DataFrame columns
class (PersistEntity record) => EntityToDataFrame record where
    -- | Get column names for the entity
    entityColumnNames :: Proxy record -> [Text]

    -- | Convert entity fields to column data
    entityToColumnData :: Entity record -> [(Text, SomeColumn)]

-- | Type class for converting DataFrame rows back to Persistent entities
class (PersistEntity record) => DataFrameToEntity record where
    -- | Convert a row from DataFrame to entity
    rowToEntity :: Int -> DataFrame -> Either String (Entity record)

-- | Helper type for heterogeneous column data  
data SomeColumn = forall a. (DFCol.Columnable a) => SomeColumn (V.Vector a)

-- | Read entities from database into a DataFrame
fromPersistent ::
    ( MonadIO m
    , PersistEntity record
    , PersistEntityBackend record ~ SqlBackend
    , EntityToDataFrame record
    , Show record
    ) =>
    [Filter record] ->
    ReaderT SqlBackend m DataFrame
fromPersistent = fromPersistentWith defaultPersistentConfig

-- | Read entities from database into a DataFrame with custom config
fromPersistentWith ::
    ( MonadIO m
    , PersistEntity record
    , PersistEntityBackend record ~ SqlBackend
    , EntityToDataFrame record
    , Show record
    ) =>
    PersistentConfig ->
    [Filter record] ->
    ReaderT SqlBackend m DataFrame
fromPersistentWith config filters = do
    entities <- selectList filters []
    liftIO $ entitiesToDataFrame config entities

-- | Convert entities to DataFrame
entitiesToDataFrame ::
    forall record. (PersistEntity record, EntityToDataFrame record, Show record) =>
    PersistentConfig ->
    [Entity record] ->
    IO DataFrame
entitiesToDataFrame config entities = do
    let proxy = Proxy @record
        baseColNames = entityColumnNames proxy
        colNames =
            if pcIncludeId config
                then pcIdColumnName config : baseColNames
                else baseColNames

    if null entities
        then return DF.empty
        else do
            -- Create columns from entity data
            columns <- createColumnsFromEntities config entities
            let indices = M.fromList $ zip colNames [0..]
                numRows = if null entities then 0 else length entities
                numCols = length colNames
                dimensions = (numRows, numCols)
            return $ DataFrame (V.fromList columns) indices dimensions

-- | Create columns from entities
createColumnsFromEntities ::
    forall record. (PersistEntity record, EntityToDataFrame record, Show record) =>
    PersistentConfig ->
    [Entity record] ->
    IO [DFCol.Column]
createColumnsFromEntities config entities = do
    let proxy = Proxy @record
        baseColNames = entityColumnNames proxy
        colNames =
            if pcIncludeId config
                then pcIdColumnName config : baseColNames
                else baseColNames
    
    -- Extract data for each column
    sequence $ map (\colName -> createColumnForName colName entities) colNames

-- | Create a column for a specific column name from entities
createColumnForName ::
    forall record. (PersistEntity record, EntityToDataFrame record, Show record) =>
    Text ->
    [Entity record] ->
    IO DFCol.Column
createColumnForName colName entities = do
    -- This is a simplified implementation
    -- In practice, you would need to extract the specific field data
    -- For now, we'll create text columns with entity representation
    let textData = map (T.pack . show) entities
    return $ DFCol.fromList textData


-- | Write DataFrame to database as entities
toPersistent ::
    ( MonadIO m
    , PersistEntity record
    , PersistEntityBackend record ~ SqlBackend
    , DataFrameToEntity record
    , SafeToInsert record
    ) =>
    DataFrame ->
    ReaderT SqlBackend m [Key record]
toPersistent df = do
    let rowCount = nRows df
    keys <- forM [0 .. rowCount - 1] $ \i -> do
        case rowToEntity i df of
            Left err -> error $ "Failed to convert row " <> show i <> ": " <> err
            Right entity -> insert (entityVal entity)
    return keys

-- | Convert entity fields to columns (helper for implementations)
entityToColumns ::
    (PersistEntity record, ToBackendKey SqlBackend record) =>
    Entity record ->
    [(Text, SomeColumn)]
entityToColumns (Entity key val) = 
    [("id", SomeColumn (V.singleton $ fromSqlKey key))]

-- | Convert persistent fields to columns
persistFieldsToColumns :: (PersistEntity record) => record -> [(Text, SomeColumn)]
persistFieldsToColumns record = 
    -- This would need to be implemented per entity type
    -- as Persistent doesn't provide generic field extraction
    []

-- | Convert columns back to entity (helper for implementations)
columnsToEntity ::
    (PersistEntity record) =>
    Int ->
    DataFrame ->
    Either String (Entity record)
columnsToEntity rowIdx df = 
    -- This would need to be implemented per entity type
    Left "Not implemented: entity-specific conversion required"