{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE LambdaCase #-}

module DataFrame.IO.Persistent.TH (
    -- * Template Haskell derivation
    deriveEntityToDataFrame,
    deriveDataFrameToEntity,
    derivePersistentDataFrame,
) where

import Control.Monad (forM, when)
import Data.List (foldl')
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Vector as V
import Database.Persist
import Database.Persist.TH
import Language.Haskell.TH
import Language.Haskell.TH.Syntax (Lift, lift)
import DataFrame.IO.Persistent

-- | Derive EntityToDataFrame instance using Template Haskell
deriveEntityToDataFrame :: Name -> Q [Dec]
deriveEntityToDataFrame entityName = do
    info <- reify entityName
    case info of
        TyConI (DataD _ _ _ _ [RecC conName fields] _) -> do
            let fieldNames = [nameBase fname | (fname, _, _) <- fields]
                textFieldNames = map (T.pack . camelToSnake) fieldNames
            
            -- Generate entityColumnNames implementation
            colNamesImpl <- [| $(lift textFieldNames) |]
            let colNamesMethod = FunD 'entityColumnNames
                    [Clause [WildP] (NormalB colNamesImpl) []]
            
            -- Generate entityToColumnData implementation
            entityDataImpl <- generateEntityToColumnData conName fields
            let entityDataMethod = FunD 'entityToColumnData
                    [Clause [VarP (mkName "entity")] (NormalB entityDataImpl) []]
            
            -- Create instance
            let instanceDec = InstanceD Nothing [] 
                    (AppT (ConT ''EntityToDataFrame) (ConT entityName))
                    [colNamesMethod, entityDataMethod]
            
            return [instanceDec]
        _ -> fail $ "deriveEntityToDataFrame: " ++ show entityName ++ " must be a record type"

-- | Generate the entityToColumnData expression
generateEntityToColumnData :: Name -> [(Name, Bang, Type)] -> Q Exp
generateEntityToColumnData conName fields = do
    entityVar <- newName "entity"
    keyVar <- newName "key"
    valVar <- newName "val"
    
    -- Pattern match on Entity (using newer TH syntax)
    let entityPat = ConP 'Entity [] [VarP keyVar, VarP valVar]
    
    -- Generate field extraction expressions
    fieldExprs <- forM fields $ \(fname, _, ftype) -> do
        let fieldName = nameBase fname
            textName = T.pack (camelToSnake fieldName)
        -- Create expression to extract and wrap field value
        [| ($(lift textName), 
            SomeColumn (V.singleton ($(varE fname) $(varE valVar)))) |]
    
    -- Add ID column
    idExpr <- [| ("id", SomeColumn (V.singleton (fromSqlKey $(varE keyVar)))) |]
    
    -- Combine all expressions
    let allExprs = ListE (idExpr : fieldExprs)
    
    -- Create case expression
    lamE [varP entityVar] 
        (caseE (varE entityVar) 
            [match (return entityPat) (normalB (return allExprs)) []])

-- | Derive DataFrameToEntity instance
deriveDataFrameToEntity :: Name -> Q [Dec]
deriveDataFrameToEntity entityName = do
    info <- reify entityName
    case info of
        TyConI (DataD _ _ _ _ [RecC conName fields] _) -> do
            -- Generate rowToEntity implementation
            rowToEntityImpl <- generateRowToEntity conName fields
            let rowToEntityMethod = FunD 'rowToEntity
                    [Clause [VarP (mkName "idx"), VarP (mkName "df")] 
                        (NormalB rowToEntityImpl) []]
            
            -- Create instance
            let instanceDec = InstanceD Nothing []
                    (AppT (ConT ''DataFrameToEntity) (ConT entityName))
                    [rowToEntityMethod]
            
            return [instanceDec]
        _ -> fail $ "deriveDataFrameToEntity: " ++ show entityName ++ " must be a record type"

-- | Generate the rowToEntity expression
generateRowToEntity :: Name -> [(Name, Bang, Type)] -> Q Exp
generateRowToEntity conName fields = do
    -- This is a simplified implementation
    -- In practice, you'd need to extract values from the DataFrame
    [| Left "Auto-generated rowToEntity not fully implemented" |]

-- | Derive both instances at once
derivePersistentDataFrame :: Name -> Q [Dec]
derivePersistentDataFrame name = do
    entityDecs <- deriveEntityToDataFrame name
    dfDecs <- deriveDataFrameToEntity name
    return (entityDecs ++ dfDecs)

-- | Convert camelCase to snake_case
camelToSnake :: String -> String
camelToSnake [] = []
camelToSnake (c:cs) = toLower c : go cs
  where
    go [] = []
    go (c:cs)
        | isUpper c = '_' : toLower c : go cs
        | otherwise = c : go cs
    isUpper c = c >= 'A' && c <= 'Z'
    toLower c = if isUpper c then toEnum (fromEnum c + 32) else c