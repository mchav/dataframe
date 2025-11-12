{-# LANGUAGE OverloadedStrings #-}

module Operations.Join where

import qualified DataFrame as D
import DataFrame.Operations.Join
import Test.HUnit
import Data.Text (Text)

df1 :: D.DataFrame
df1 = D.fromNamedColumns [("key", D.fromList ["K0" :: Text, "K1", "K2", "K3", "K4", "K5"]), ("A", D.fromList ["A0" :: Text, "A1", "A2", "A3", "A4", "A5"])]

df2 :: D.DataFrame
df2 = D.fromNamedColumns [("key", D.fromList ["K0" :: Text, "K1", "K2"]), ("B", D.fromList ["B0" :: Text, "B1", "B2"])]

testInnerJoin :: Test
testInnerJoin = TestCase
            ( assertEqual
                "Test inner join with single key"
                ( D.fromNamedColumns
                    [ ("key", D.fromList ["K0" :: Text, "K1", "K2"])
                    , ("A", D.fromList ["A0" :: Text, "A1", "A2"])
                    , ("B", D.fromList ["B0" :: Text, "B1", "B2"])
                    ]
                )
                (D.sortBy D.Ascending ["key"] (innerJoin ["key"] df1 df2))
            )

testLeftJoin :: Test
testLeftJoin = TestCase
            ( assertEqual
                "Test left join with single key"
                ( D.fromNamedColumns
                    [ ("key", D.fromList ["K0" :: Text, "K1", "K2", "K3", "K4", "K5"]), ("A", D.fromList ["A0" :: Text, "A1", "A2", "A3", "A4", "A5"])
                    , ("B", D.fromList [Just "B0", Just "B1" :: Maybe Text, Just "B2"])
                    ]
                )
                (D.sortBy D.Ascending ["key"] (leftJoin ["key"] df2 df1))
            )

testRightJoin :: Test
testRightJoin = TestCase
            ( assertEqual
                "Test right join with single key"
                ( D.fromNamedColumns
                    [ ("key", D.fromList ["K0" :: Text, "K1", "K2"])
                    , ("A", D.fromList ["A0" :: Text, "A1", "A2"])
                    , ("B", D.fromList ["B0" :: Text, "B1", "B2"])
                    ]
                )
                (D.sortBy D.Ascending ["key"] (rightJoin ["key"] df2 df1))
            )

testFullOuterJoin :: Test
testFullOuterJoin = TestCase
            ( assertEqual
                "Test full outer join with single key"
                ( D.fromNamedColumns
                    [ ("key", D.fromList ["K0" :: Text, "K1", "K2", "K3", "K4", "K5"]), ("A", D.fromList ["A0" :: Text, "A1", "A2", "A3", "A4", "A5"])
                    , ("B", D.fromList [Just "B0", Just "B1" :: Maybe Text, Just "B2"])
                    ]
                )
                (D.sortBy D.Ascending ["key"] (fullOuterJoin ["key"] df2 df1))
            )

tests :: [Test]
tests =
    [ TestLabel "innerJoin" testInnerJoin
    , TestLabel "leftJoin" testLeftJoin
    , TestLabel "rightJoin" testRightJoin
    ]
