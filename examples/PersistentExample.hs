{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE EmptyDataDecls #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

module PersistentExample where

import Control.Monad.IO.Class (liftIO)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Time (Day)
import Database.Persist
import Database.Persist.Sqlite
import Database.Persist.TH
import qualified DataFrame as DF
import DataFrame.IO.Persistent
import DataFrame.IO.Persistent.TH
import qualified DataFrame.Functions as F

-- Define Persistent entities using Template Haskell
share [mkPersist sqlSettings, mkMigrate "migrateAll"] [persistLowerCase|
Person
    name Text
    age Int
    email Text Maybe
    registered Day
    deriving Show

Product  
    name Text
    price Double
    category Text
    inStock Bool
    deriving Show
|]

-- Derive DataFrame instances
$(derivePersistentDataFrame ''Person)
$(derivePersistentDataFrame ''Product)

-- Example: Loading Person entities into a DataFrame
exampleLoadPersons :: IO ()
exampleLoadPersons = runSqlite "example.db" $ do
    -- Run migrations
    runMigration migrateAll
    
    -- Insert some test data
    johnId <- insert $ Person "John Doe" 30 (Just "john@example.com") (read "2024-01-15")
    janeId <- insert $ Person "Jane Smith" 25 Nothing (read "2024-02-20")
    bobId <- insert $ Person "Bob Wilson" 45 (Just "bob@example.com") (read "2023-12-01")
    
    -- Load all persons into a DataFrame
    personsDF <- fromPersistent @Person []
    liftIO $ do
        putStrLn "All persons:"
        print personsDF
        putStrLn ""
    
    -- Load persons with filters
    youngPersonsDF <- fromPersistent [PersonAge <. 35]
    liftIO $ do
        putStrLn "Young persons (age < 35):"
        print youngPersonsDF
        putStrLn ""
    
    -- Use custom configuration
    let config = defaultPersistentConfig { pcIdColumnName = "person_id" }
    customDF <- fromPersistentWith config [PersonEmail !=. Nothing]
    liftIO $ do
        putStrLn "Persons with email (custom ID column name):"
        print customDF

-- Example: DataFrame operations on database data
exampleAnalyzeProducts :: IO ()
exampleAnalyzeProducts = runSqlite "example.db" $ do
    -- Insert product data
    forM_ sampleProducts $ \(name, price, cat, stock) ->
        insert $ Product name price cat stock
    
    -- Load products into DataFrame
    productsDF <- fromPersistent @Product []
    
    liftIO $ do
        -- Basic stats
        putStrLn "Product summary:"
        let summary = DF.summarize productsDF
        print summary
        putStrLn ""
        
        -- Group by category and aggregate
        let grouped = DF.groupBy ["category"] productsDF
        let categoryStats = DF.aggregate
                [ F.count @Text (F.col @Text "name") `F.as` "count"
                , F.mean (F.col @Double "price") `F.as` "avg_price"
                , F.sum (F.col @Bool "in_stock") `F.as` "items_in_stock"
                ]
                grouped
        
        putStrLn "Stats by category:"
        print categoryStats
        putStrLn ""
        
        -- Filter and derive
        let expensiveDF = DF.filter @Double "price" (> 50.0) productsDF
            |> DF.derive "price_category" 
                (F.ifThenElse (F.col @Double "price" F.gt F.lit 100.0)
                    (F.lit @Text "premium")
                    (F.lit @Text "standard"))
        
        putStrLn "Expensive products with price category:"
        print $ DF.select ["name", "price", "price_category"] expensiveDF
  where
    sampleProducts =
        [ ("Laptop", 999.99, "Electronics", True)
        , ("Mouse", 29.99, "Electronics", True)
        , ("Desk", 199.99, "Furniture", True)
        , ("Chair", 149.99, "Furniture", False)
        , ("Monitor", 299.99, "Electronics", True)
        , ("Keyboard", 79.99, "Electronics", False)
        , ("Bookshelf", 89.99, "Furniture", True)
        ]

-- Example: Converting DataFrame back to entities
exampleDataFrameToEntities :: IO ()
exampleDataFrameToEntities = do
    -- Create a DataFrame programmatically
    let df = DF.fromList
            [ ("name", ["Alice Johnson", "Charlie Brown"])
            , ("age", [28 :: Int, 35])
            , ("email", [Just "alice@example.com", Nothing])
            , ("registered", [read "2024-03-01" :: Day, read "2024-03-15"])
            ]
    
    putStrLn "DataFrame to insert:"
    print df
    putStrLn ""
    
    -- Insert into database
    runSqlite "example.db" $ do
        runMigration migrateAll
        
        -- Convert and insert
        keys <- toPersistent @Person df
        liftIO $ putStrLn $ "Inserted " ++ show (length keys) ++ " persons"
        
        -- Verify by reading back
        allPersons <- selectList @Person [] []
        liftIO $ do
            putStrLn "All persons in database:"
            mapM_ print allPersons

-- Main entry point
main :: IO ()
main = do
    putStrLn "=== Persistent DataFrame Example ==="
    putStrLn ""
    
    putStrLn "1. Loading persons from database:"
    exampleLoadPersons
    putStrLn ""
    
    putStrLn "2. Analyzing products:"
    exampleAnalyzeProducts
    putStrLn ""
    
    putStrLn "3. Converting DataFrame to entities:"
    exampleDataFrameToEntities