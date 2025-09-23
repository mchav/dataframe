{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}

module DataFrame.Lazy.Internal.DataFrame where

import Control.Monad (foldM)
import qualified Data.List as L
import qualified Data.Text as T
import qualified DataFrame.Internal.Column as C
import qualified DataFrame.Internal.DataFrame as D
import qualified DataFrame.Internal.Expression as E
import qualified DataFrame.Lazy.IO.CSV as D
import DataFrame.Operations.Merge ()
import qualified DataFrame.Operations.Subset as D
import qualified DataFrame.Operations.Transformations as D

data LazyOperation where
    Derive :: (C.Columnable a) => T.Text -> E.Expr a -> LazyOperation
    Select :: [T.Text] -> LazyOperation
    Filter :: E.Expr Bool -> LazyOperation

instance Show LazyOperation where
    show :: LazyOperation -> String
    show (Derive name expr) = T.unpack name ++ " := " ++ show expr
    show (Select columns) = "select(" ++ show columns ++ ")"
    show (Filter expr) = "filter(" ++ show expr ++ ")"

data InputType = ICSV deriving (Show)

data LazyDataFrame = LazyDataFrame
    { inputPath :: FilePath
    , inputType :: InputType
    , operations :: [LazyOperation]
    , batchSize :: Int
    }
    deriving (Show)

eval :: LazyOperation -> D.DataFrame -> D.DataFrame
eval (Derive name expr) = D.derive name expr
eval (Select columns) = D.select columns
eval (Filter expr) = D.filterWhere expr

runDataFrame :: forall a. (C.Columnable a) => LazyDataFrame -> IO D.DataFrame
runDataFrame df = do
    let path = inputPath df
    totalRows <- D.countRows ',' path
    let batches = batchRanges totalRows (batchSize df)
    (df', _) <-
        foldM
            ( \(accDf, (pos, unused, r)) (start, end) -> do
                mapM_
                    putStr
                    [ "Scanning: "
                    , show start
                    , " to "
                    , show end
                    , " rows out of "
                    , show totalRows
                    , "\n"
                    ]

                (sdf, (pos', unconsumed, rowsRead)) <-
                    D.readSeparated
                        ','
                        ( D.defaultOptions
                            { D.rowRange = Just (start, batchSize df)
                            , D.totalRows = Just totalRows
                            , D.seekPos = pos
                            , D.rowsRead = r
                            , D.leftOver = unused
                            }
                        )
                        path
                let rdf = L.foldl' (flip eval) sdf (operations df)
                return (accDf <> rdf, (Just pos', unconsumed, rowsRead + r))
            )
            (D.empty, (Nothing, "", 0))
            batches
    return df'

batchRanges :: Int -> Int -> [(Int, Int)]
batchRanges n inc = go n [0, inc .. n]
  where
    go _ [] = []
    go n [x] = [(x, n)]
    go n (f : s : rest) = (f, s) : go n (s : rest)

scanCsv :: T.Text -> LazyDataFrame
scanCsv path = LazyDataFrame (T.unpack path) ICSV [] 512_000

addOperation :: LazyOperation -> LazyDataFrame -> LazyDataFrame
addOperation op df = df{operations = operations df ++ [op]}

derive ::
    (C.Columnable a) => T.Text -> E.Expr a -> LazyDataFrame -> LazyDataFrame
derive name expr = addOperation (Derive name expr)

select :: (C.Columnable a) => [T.Text] -> LazyDataFrame -> LazyDataFrame
select columns = addOperation (Select columns)

filter :: (C.Columnable a) => E.Expr Bool -> LazyDataFrame -> LazyDataFrame
filter cond = addOperation (Filter cond)
