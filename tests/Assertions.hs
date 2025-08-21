{-# LANGUAGE ScopedTypeVariables #-}

module Assertions where

import qualified Data.List as L

import Control.Exception
import Test.HUnit

-- Adapted from: https://github.com/BartMassey/chunk/blob/1ee4bd6545e0db6b8b5f4935d97e7606708eacc9/hunit.hs#L29
assertExpectException ::
    String ->
    String ->
    IO a ->
    Assertion
assertExpectException preface expected action = do
    r <-
        catch
            (action >> (return . Just) "no exception thrown")
            ( \(e :: SomeException) ->
                return (checkForExpectedException e)
            )
    case r of
        Nothing -> return ()
        Just msg -> assertFailure $ preface ++ ": " ++ msg
  where
    checkForExpectedException :: SomeException -> Maybe String
    checkForExpectedException e
        | expected `L.isInfixOf` show e = Nothing
        | otherwise =
            Just $
                "wrong exception detail, expected "
                    ++ expected
                    ++ ", got: "
                    ++ show e
