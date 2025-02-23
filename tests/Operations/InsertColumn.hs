{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE OverloadedStrings #-}
module Operations.InsertColumn where

import qualified Data.DataFrame as D
import qualified Data.DataFrame as DI
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import Assertions
import Test.HUnit

testData :: D.DataFrame
testData = D.fromList [ ("test1", DI.toColumn ([1..26] :: [Int]))
                      , ("test2", DI.toColumn ['a'..'z'])
                      , ("test3", DI.toColumn ([1..26] :: [Int]))
                      , ("test4", DI.toColumn ['a'..'z'])
                      , ("test5", DI.toColumn ([1..26] :: [Int]))
                      , ("test6", DI.toColumn ['a'..'z'])
                      , ("test7", DI.toColumn ([1..26] :: [Int]))
                      , ("test8", DI.toColumn ['a'..'z'])
                      ]

-- Adding a boxed vector to an empty dataframe creates a new column boxed containing the vector elements.
addBoxedColumn :: Test
addBoxedColumn = TestCase (assertEqual "Two columns should be equal"
                            (Just $ DI.BoxedColumn (V.fromList ["Thuba" :: T.Text, "Zodwa", "Themba"]))
                            (DI.getColumn "new" $ D.insertColumn "new" (V.fromList ["Thuba" :: T.Text, "Zodwa", "Themba"]) D.empty))

addBoxedColumn' :: Test
addBoxedColumn' = TestCase (assertEqual "Two columns should be equal"
                            (Just $ DI.toColumn ["Thuba" :: T.Text, "Zodwa", "Themba"])
                            (DI.getColumn "new" $ D.insertColumn' "new" (Just $ DI.toColumn ["Thuba" :: T.Text, "Zodwa", "Themba"]) D.empty))

-- Adding an boxed vector with an unboxable type (Int/Double) to an empty dataframe creates a new column boxed containing the vector elements.
addUnboxedColumn :: Test
addUnboxedColumn = TestCase (assertEqual "Value should be boxed"
                            (Just $ DI.UnboxedColumn (VU.fromList [1 :: Int, 2, 3]))
                            (DI.getColumn "new" $ D.insertColumn "new" (V.fromList [1 :: Int, 2, 3]) D.empty))

addUnboxedColumn' :: Test
addUnboxedColumn' = TestCase (assertEqual "Value should be boxed"
                            (Just $ DI.toColumn [1 :: Int, 2, 3])
                            (DI.getColumn "new" $ D.insertColumn' "new" (Just $ DI.toColumn [1 :: Int, 2, 3]) D.empty))

-- Adding a column with less values than the current DF dimensions adds column with optionals.
addSmallerColumnBoxed :: Test
addSmallerColumnBoxed = TestCase (
    assertEqual "Missing values should be replaced with Nothing"
    (Just $ DI.OptionalColumn (V.fromList [Just "a" :: Maybe T.Text, Just "b",  Just "c", Nothing, Nothing]))
    (DI.getColumn "newer" $ D.insertColumn "newer" (V.fromList ["a" :: T.Text, "b", "c"]) $ D.insertColumn "new" (V.fromList ["a" :: T.Text, "b", "c", "d", "e"]) D.empty)
  )

addSmallerColumnUnboxed :: Test
addSmallerColumnUnboxed = TestCase (
    assertEqual "Missing values should be replaced with Nothing"
    (Just $ DI.OptionalColumn (V.fromList [Just 1 :: Maybe Int, Just 2,  Just 3, Nothing, Nothing]))
    (DI.getColumn "newer" $ D.insertColumn "newer" (V.fromList [1 :: Int, 2, 3]) $ D.insertColumn "new" (V.fromList [1 :: Int, 2, 3, 4, 5]) D.empty)
  )

insertColumnWithDefaultFillsWithDefault :: Test
insertColumnWithDefaultFillsWithDefault = TestCase (
    assertEqual "Missing values should be replaced with Nothing"
    (Just $ DI.UnboxedColumn (VU.fromList [1 :: Int, 2,  3, 0, 0]))
    (DI.getColumn "newer" $ D.insertColumnWithDefault 0 "newer" (V.fromList [1 :: Int, 2, 3]) $ D.insertColumn "new" (V.fromList [1 :: Int, 2, 3, 4, 5]) D.empty)
  )

insertColumnWithDefaultFillsLargerNoop :: Test
insertColumnWithDefaultFillsLargerNoop = TestCase (
    assertEqual "Lists should be the same size"
    (Just $ DI.UnboxedColumn (VU.fromList [(6 :: Int)..10]))
    (DI.getColumn "newer" $ D.insertColumnWithDefault 0 "newer" (V.fromList [(6 :: Int)..10]) $ D.insertColumn "new" (V.fromList [1 :: Int, 2, 3, 4, 5]) D.empty)
  )

addLargerColumnBoxed :: Test
addLargerColumnBoxed =
  TestCase (assertEqual "Smaller lists should grow and contain optionals"
                    (D.fromList [("new", D.toColumn [Just "a" :: Maybe T.Text, Just "b", Just "c", Nothing, Nothing]),
                                 ("newer", D.toColumn ["a" :: T.Text, "b", "c", "d", "e"])])
                    (D.insertColumn "newer" (V.fromList ["a" :: T.Text, "b", "c", "d", "e"])
                            $ D.insertColumn "new" (V.fromList ["a" :: T.Text, "b", "c"]) D.empty))
addLargerColumnUnboxed :: Test
addLargerColumnUnboxed =
    TestCase (assertEqual "Smaller lists should grow and contain optionals"
                    (D.fromList [("old", D.toColumn [Just 1 :: Maybe Int, Just 2, Nothing, Nothing, Nothing]),
                                 ("new", D.toColumn [Just 1 :: Maybe Int, Just 2, Just 3, Nothing, Nothing]),
                                 ("newer", D.toColumn [1 :: Int, 2, 3, 4, 5])])
                    (D.insertColumn "newer" (V.fromList [1 :: Int, 2, 3, 4, 5])
                     $ D.insertColumn "new" (V.fromList [1 :: Int, 2, 3]) $ 
                     D.insertColumn "old" (V.fromList [1 :: Int, 2]) D.empty))

dimensionsChangeAfterAdd :: Test
dimensionsChangeAfterAdd = TestCase (assertEqual "should be (26, 3)"
                                     (26, 9)
                                     (D.dimensions $ D.insertColumn @Int "new" (V.fromList [1..26]) testData))

dimensionsNotChangedAfterDuplicate :: Test
dimensionsNotChangedAfterDuplicate = TestCase (assertEqual "should be (26, 3)"
                                     (26, 9)
                                     (D.dimensions $ D.insertColumn @Int "new" (V.fromList [1..26])
                                                   $ D.insertColumn @Int "new" (V.fromList [1..26]) testData))


tests :: [Test]
tests = [
             TestLabel "dimensionsChangeAfterAdd" dimensionsChangeAfterAdd
           , TestLabel "dimensionsNotChangedAfterDuplicate" dimensionsNotChangedAfterDuplicate
           , TestLabel "addBoxedColunmToEmpty" addBoxedColumn
           , TestLabel "addBoxedColumnAutoUnboxes" addBoxedColumn
           , TestLabel "addSmallerColumnBoxed" addSmallerColumnBoxed
           , TestLabel "addSmallerColumnUnboxed" addSmallerColumnUnboxed
           , TestLabel "addLargerColumnBoxed" addLargerColumnBoxed
           , TestLabel "addLargerColumnUnboxed" addLargerColumnUnboxed
           , TestLabel "insertColumnWithDefaultFillsWithDefault" insertColumnWithDefaultFillsWithDefault
           , TestLabel "insertColumnWithDefaultFillsLargerNoop" insertColumnWithDefaultFillsLargerNoop
           ]
