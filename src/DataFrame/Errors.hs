{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

module DataFrame.Errors where

import qualified Data.Text as T

import Control.Exception
import Data.Array
import Data.Either
import Data.Typeable (Typeable)
import DataFrame.Display.Terminal.Colours
import Type.Reflection (TypeRep)

data TypeErrorContext a b = MkTypeErrorContext
    { userType :: Either String (TypeRep a)
    , expectedType :: Either String (TypeRep b)
    , errorColumnName :: Maybe String
    , callingFunctionName :: Maybe String
    }

data DataFrameException where
    TypeMismatchException ::
        forall a b.
        (Typeable a, Typeable b) =>
        TypeErrorContext a b ->
        DataFrameException
    ColumnNotFoundException :: T.Text -> T.Text -> [T.Text] -> DataFrameException
    deriving (Exception)

instance Show DataFrameException where
    show :: DataFrameException -> String
    show (TypeMismatchException context) =
        let
            errorString = typeMismatchError (either id show (userType context)) (either id show (expectedType context))
         in
            addCallPointInfo (errorColumnName context) (callingFunctionName context) errorString
    show (ColumnNotFoundException columnName callPoint availableColumns) = columnNotFound columnName callPoint availableColumns

columnNotFound :: T.Text -> T.Text -> [T.Text] -> String
columnNotFound name callPoint columns =
    red "\n\n[ERROR] "
        ++ "Column not found: "
        ++ T.unpack name
        ++ " for operation "
        ++ T.unpack callPoint
        ++ "\n\tDid you mean "
        ++ T.unpack (guessColumnName name columns)
        ++ "?\n\n"

typeMismatchError :: String -> String -> String
typeMismatchError givenType expectedType =
    red $
        red "\n\n[Error]: Type Mismatch"
            ++ "\n\tWhile running your code I tried to "
            ++ "get a column of type: "
            ++ red (show givenType)
            ++ " but the column in the dataframe was actually of type: "
            ++ green (show expectedType)

addCallPointInfo :: Maybe String -> Maybe String -> String -> String
addCallPointInfo (Just name) (Just cp) err =
    err
        ++ ( "\n\tThis happened when calling function "
                ++ brightGreen cp
                ++ " on the column "
                ++ brightGreen name
                ++ "\n\n"
                ++ typeAnnotationSuggestion cp
           )
addCallPointInfo Nothing (Just cp) err = err ++ "\n" ++ typeAnnotationSuggestion cp
addCallPointInfo (Just name) Nothing err =
    err
        ++ ( "\n\tOn the column "
                ++ name
                ++ "\n\n"
           )
addCallPointInfo Nothing Nothing err = err

typeAnnotationSuggestion :: String -> String
typeAnnotationSuggestion cp =
    "\n\n\tTry adding a type at the end of the function e.g "
        ++ "change\n\t\t"
        ++ red (cp ++ " ...")
        ++ " to \n\t\t"
        ++ green ("(" ++ cp ++ " ... :: <Type>)")
        ++ "\n\tor add "
        ++ "{-# LANGUAGE TypeApplications #-} to the top of your "
        ++ "file then change the call to \n\t\t"
        ++ brightGreen (cp ++ " @<Type> ....")

guessColumnName :: T.Text -> [T.Text] -> T.Text
guessColumnName userInput columns = case map (\k -> (editDistance userInput k, k)) columns of
    [] -> ""
    res -> (snd . minimum) res

editDistance :: T.Text -> T.Text -> Int
editDistance xs ys = table ! (m, n)
  where
    (m, n) = (T.length xs, T.length ys)
    x = array (1, m) (zip [1 ..] (T.unpack xs))
    y = array (1, n) (zip [1 ..] (T.unpack ys))

    table :: Array (Int, Int) Int
    table = array bnds [(ij, dist ij) | ij <- range bnds]
    bnds = ((0, 0), (m, n))

    dist (0, j) = j
    dist (i, 0) = i
    dist (i, j) =
        minimum
            [ table ! (i - 1, j) + 1
            , table ! (i, j - 1) + 1
            , if x ! i == y ! j then table ! (i - 1, j - 1) else 1 + table ! (i - 1, j - 1)
            ]
