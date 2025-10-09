# DataFrame Persistent Integration

This document describes the integration between the DataFrame library and Haskell's Persistent database library.

## Overview

The DataFrame library now supports loading data from and saving data to databases using the Persistent library. This allows you to:

- Load database query results directly into DataFrames
- Perform DataFrame operations on database data
- Save DataFrame results back to the database
- Work with type-safe database entities

## Installation

Add the following dependencies to your project:

```haskell
build-depends:
  dataframe >= 0.3.2.0,
  persistent >= 2.14,
  persistent-sqlite >= 2.13,  -- or your preferred backend
  persistent-template >= 2.12
```

## Basic Usage

### 1. Define Your Entities

```haskell
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}

import Database.Persist.TH
import DataFrame.IO.Persistent.TH

share [mkPersist sqlSettings, mkMigrate "migrateAll"] [persistLowerCase|
Person
    name Text
    age Int
    email Text Maybe
    registered Day
    deriving Show
|]

-- Derive DataFrame instances
$(derivePersistentDataFrame ''Person)
```

### 2. Load Data from Database

```haskell
import Database.Persist.Sqlite
import DataFrame as DF
import DataFrame.Functions as F

loadPersons :: IO ()
loadPersons = runSqlite "mydb.sqlite" $ do
    -- Load all persons
    allPersonsDF <- fromPersistent @Person []
    
    -- Load with filters
    youngPersonsDF <- fromPersistent [PersonAge <. 30]
    
    -- Custom configuration
    let config = defaultPersistentConfig 
            { pcIdColumnName = "person_id"
            , pcIncludeId = True
            }
    customDF <- fromPersistentWith config []
    
    liftIO $ print allPersonsDF
```

### 3. Perform DataFrame Operations

Once loaded, you can use all standard DataFrame operations:

```haskell
analyzePersons :: IO ()
analyzePersons = runSqlite "mydb.sqlite" $ do
    df <- fromPersistent @Person []
    
    liftIO $ do
        -- Filter
        let adults = DF.filter @Int "age" (>= 18) df
        
        -- Sort
        let sorted = DF.sortBy DF.Ascending ["age"] df
        
        -- Derive columns
        let withAgeGroup = DF.derive "age_group"
                (F.ifThenElse (F.col @Int "age" F.lt F.lit 30)
                    (F.lit @Text "young")
                    (F.lit @Text "adult"))
                df
        
        -- Group and aggregate
        let grouped = DF.groupBy ["age_group"] withAgeGroup
        let stats = DF.aggregate
                [ F.count @Text (F.col @Text "name") `F.as` "count"
                , F.mean (F.col @Int "age") `F.as` "avg_age"
                ]
                grouped
        
        print stats
```

### 4. Save DataFrame to Database

```haskell
saveToDatabase :: DataFrame -> IO ()
saveToDatabase df = runSqlite "mydb.sqlite" $ do
    keys <- toPersistent @Person df
    liftIO $ putStrLn $ "Inserted " ++ show (length keys) ++ " records"
```

## Configuration Options

The `PersistentConfig` type allows you to customize the behavior:

```haskell
data PersistentConfig = PersistentConfig
    { pcBatchSize :: Int          -- Number of records to fetch at once (default: 10000)
    , pcIncludeId :: Bool         -- Include entity ID as column (default: True)
    , pcIdColumnName :: Text      -- Name for ID column (default: "id")
    }
```

## Advanced Features

### Custom Entity Mapping

For more control over the conversion process, you can implement the type classes manually:

```haskell
instance EntityToDataFrame MyEntity where
    entityColumnNames _ = ["id", "field1", "field2"]
    entityToColumnData (Entity key val) = 
        [ ("id", SomeColumn $ V.singleton $ fromSqlKey key)
        , ("field1", SomeColumn $ V.singleton $ myEntityField1 val)
        , ("field2", SomeColumn $ V.singleton $ myEntityField2 val)
        ]

instance DataFrameToEntity MyEntity where
    rowToEntity idx df = -- Implementation
```

### Working with Relationships

When working with related entities, load them separately and use DataFrame join operations:

```haskell
joinExample :: IO ()
joinExample = runSqlite "mydb.sqlite" $ do
    usersDF <- fromPersistent @User []
    ordersDF <- fromPersistent @Order []
    
    liftIO $ do
        -- Join users with their orders
        let joined = DF.innerJoin "id" "user_id" usersDF ordersDF
        print joined
```

## Performance Considerations

1. **Batch Size**: Adjust `pcBatchSize` based on your memory constraints and query size
2. **Filtering**: Apply database filters when possible rather than loading all data
3. **Lazy Loading**: For very large datasets, consider implementing streaming support

## Limitations

1. The current implementation loads all data into memory
2. Complex persistent fields may require custom conversion logic
3. Streaming/lazy evaluation is not yet supported

## Examples

See the `examples/PersistentExample.hs` file for a complete working example.