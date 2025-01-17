{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Operations.Derive where

import qualified Data.DataFrame as D
import qualified Data.DataFrame as DI
import qualified Data.DataFrame as DE
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import Assertions
import Test.HUnit
import Type.Reflection (typeRep)

values :: [(T.Text, DI.Column)]
values = [ ("test1", DI.toColumn ([1..26] :: [Int]))
         , ("test2", DI.toColumn (map show ['a'..'z']))
         , ("test3", DI.toColumn ['a'..'z'])
         ]

testData :: D.DataFrame
testData = D.fromList values

deriveFromWAI :: Test
deriveFromWAI = TestCase (assertEqual "deriveFrom works when function args align"
                                (Just $ DI.BoxedColumn (V.fromList (zipWith (\n c -> show n ++ [c]) [1..26] ['a'..'z'])))
                                (DI.getColumn "test4" $ D.deriveFrom (
                                    ["test1", "test3"],
                                    D.func (\(n :: Int) (c :: Char) -> show n ++ [c])) "test4" testData))

tests :: [Test]
tests = [ TestLabel "deriveFromWAI" deriveFromWAI
        ]