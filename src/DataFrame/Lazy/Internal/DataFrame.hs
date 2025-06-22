{-# LANGUAGE GADTs #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE NumericUnderscores #-}
module DataFrame.Lazy.Internal.DataFrame where

import           Control.Monad (forM_)
import           Data.IORef
import           Data.Kind
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified DataFrame.Internal.DataFrame as D
import qualified DataFrame.Internal.Column as C
import qualified DataFrame.Internal.Expression as E
import qualified DataFrame.Operations.Core as D
import qualified DataFrame.Operations.Subset as D
import qualified DataFrame.Operations.Transformations as D
import qualified DataFrame.IO.CSV as D
import           System.FilePath

data LazyOperation where
  Derive :: C.Columnable a => T.Text -> E.Expr a -> LazyOperation
  Select :: [T.Text] -> LazyOperation
  Filter :: E.Expr Bool -> LazyOperation

instance Show LazyOperation where
  show :: LazyOperation -> String
  show (Derive name expr) = T.unpack name ++ " := " ++ show expr
  show (Select columns) =  "select(" ++ show columns ++ ")"
  show (Filter expr) = "filter(" ++ show expr ++ ")"

data InputType = ICSV deriving Show

data LazyDataFrame = LazyDataFrame
  { inputPath        :: FilePath
  , inputType        :: InputType
  , operations          :: [LazyOperation]
  , batchSize        :: Int
  } deriving Show

eval :: LazyOperation -> D.DataFrame -> D.DataFrame
eval (Derive name expr) = D.derive name expr
eval (Select columns) = D.select columns
eval (Filter expr) = D.filterWhere expr

runDataFrame :: forall a . (C.Columnable a) => LazyDataFrame -> IO D.DataFrame
runDataFrame df = do
  let path = inputPath df
  -- totalRows <- D.countRows ',' path
  let batches = batchRanges 1000000 (batchSize df)
  _ <- forM_ batches $ \ (start, end) -> do
    -- TODO: implement specific read operations for batching that returns a seek instead of re-reading everything.
    sdf <- D.readSeparated ',' (D.defaultOptions { D.rowRange = Just (start, (batchSize df)) }) path
    let rdf = foldl' (\d op -> eval op d) sdf (operations df)
    if fst (D.dimensions rdf) == 0 then return () else print rdf 
  return (D.empty)

batchRanges :: Int -> Int -> [(Int, Int)]
batchRanges n inc = go n [0,inc..n]
  where 
    go _ []         = []
    go n [x]        = [(x, n)]
    go n (f:s:rest) =(f, s) : go n (s:rest)

scanCsv :: T.Text -> LazyDataFrame
scanCsv path = LazyDataFrame (T.unpack path) ICSV [] 1024

addOperation :: LazyOperation -> LazyDataFrame -> LazyDataFrame
addOperation op df = df { operations = (operations df) ++ [op] } 

derive :: C.Columnable a => T.Text -> E.Expr a -> LazyDataFrame -> LazyDataFrame
derive name expr = addOperation (Derive name expr)

select :: C.Columnable a => [T.Text] -> LazyDataFrame -> LazyDataFrame
select columns = addOperation (Select columns)

filter :: C.Columnable a => E.Expr Bool -> LazyDataFrame -> LazyDataFrame
filter cond = addOperation (Filter cond)
