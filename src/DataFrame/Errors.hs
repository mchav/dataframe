{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE InstanceSigs #-}

module DataFrame.Errors where

import qualified Data.Text as T

import Control.Exception
import Data.Array
import DataFrame.Display.Terminal.Colours
import Data.Typeable (Typeable)
import Type.Reflection (TypeRep)

data DataFrameException where
    TypeMismatchException :: forall a b. (Typeable a, Typeable b)
                          => TypeRep a -- ^ given type
                          -> TypeRep b -- ^ expected type
                          -> T.Text    -- ^ column name
                          -> T.Text    -- ^ call point
                          -> DataFrameException
    TypeMismatchException' :: forall a . (Typeable a)
                           => TypeRep a -- ^ expected type
                           -> String    -- ^ given type
                           -> T.Text    -- ^ column name
                           -> T.Text    -- ^ call point
                           -> DataFrameException
    ColumnNotFoundException :: T.Text -> T.Text -> [T.Text] -> DataFrameException
    deriving (Exception)

instance Show DataFrameException where
    show :: DataFrameException -> String
    show (TypeMismatchException a b columnName callPoint) = addCallPointInfo columnName (Just callPoint) (typeMismatchError a b)
    show (TypeMismatchException' a b columnName callPoint) = addCallPointInfo columnName (Just callPoint) (typeMismatchError' (show a) b)
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

typeMismatchError ::
  Type.Reflection.TypeRep a ->
  Type.Reflection.TypeRep b ->
  String
typeMismatchError a b = typeMismatchError' (show a) (show b)

typeMismatchError' :: String -> String -> String
typeMismatchError' givenType expectedType =
  red $
    red "\n\n[Error]: Type Mismatch"
      ++ "\n\tWhile running your code I tried to "
      ++ "get a column of type: "
      ++ red (show givenType)
      ++ " but column was of type: "
      ++ green (show expectedType)

addCallPointInfo :: T.Text -> Maybe T.Text -> String -> String
addCallPointInfo name (Just cp) err =
  err
    ++ ( "\n\tThis happened when calling function "
           ++ brightGreen (T.unpack cp)
           ++ " on the column "
           ++ brightGreen (T.unpack name)
           ++ "\n\n"
           ++ typeAnnotationSuggestion (T.unpack cp)
       )
addCallPointInfo name Nothing err =
  err
    ++ ( "\n\tOn the column "
           ++ T.unpack name
           ++ "\n\n"
           ++ typeAnnotationSuggestion "<function>"
       )

typeAnnotationSuggestion :: String -> String
typeAnnotationSuggestion cp =
  "\n\n\tTry adding a type at the end of the function e.g "
    ++ "change\n\t\t"
    ++ red (cp ++ " arg1 arg2")
    ++ " to \n\t\t"
    ++ green ("(" ++ cp ++ " arg1 arg2 :: <Type>)")
    ++ "\n\tor add "
    ++ "{-# LANGUAGE TypeApplications #-} to the top of your "
    ++ "file then change the call to \n\t\t"
    ++ brightGreen (cp ++ " @<Type> arg1 arg2")

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
        [ table ! (i - 1, j) + 1,
          table ! (i, j - 1) + 1,
          if x ! i == y ! j then table ! (i - 1, j - 1) else 1 + table ! (i - 1, j - 1)
        ]
