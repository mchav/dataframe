{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Operations.Derive where

import qualified DataFrame as D
import qualified DataFrame.Functions as F
import qualified DataFrame as DI
import qualified DataFrame as DE
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import Assertions
import Test.HUnit
import Type.Reflection (typeRep)

values :: [(T.Text, DI.Column)]
values = [ ("test1", DI.fromList ([1..26] :: [Int]))
         , ("test2", DI.fromList (map show ['a'..'z']))
         , ("test3", DI.fromList ['a'..'z'])
         ]

testData :: D.DataFrame
testData = D.fromNamedColumns values

deriveWAI :: Test
deriveWAI = TestCase (assertEqual "derive works with column expression"
                                (Just $ DI.BoxedColumn (V.fromList (zipWith (\n c -> show n ++ [c]) [1..26] ['a'..'z'])))
                                (DI.getColumn "test4" $ D.derive "test4" (
                                    F.lift2 (++) (F.lift show (F.col @Int "test1")) (F.lift (: ([] :: [Char])) (F.col @Char "test3"))
                                    ) testData))

tests :: [Test]
tests = [ TestLabel "deriveWAI" deriveWAI
        ]