{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- | Every operation, run through eager, typed, lazy and typed lazy APIs.
module Parity.FourWay (tests) where

import qualified Data.Text as T

import qualified DataFrame as D
import qualified DataFrame.Functions as F
import qualified DataFrame.Internal.Column as DI
import qualified DataFrame.Lazy as L
import qualified DataFrame.Typed as DT
import qualified DataFrame.Typed.Functions as DTF
import qualified DataFrame.Typed.Lazy as TL

import Test.HUnit

type Orders =
    '[ '("cid", Int)
     , '("region", T.Text)
     , '("amount", Double)
     ]

type Customers =
    '[ '("cid", Int)
     , '("region", T.Text)
     , '("tier", T.Text)
     ]

type Users = '[ '("uid", Int), '("name", T.Text)]

ordersDF :: D.DataFrame
ordersDF =
    D.fromColumns
        [ ("cid", DI.fromList [1, 2, 2, 3, 5, 1 :: Int])
        , ("region", DI.fromList ["eu", "us", "us", "eu", "us", "us" :: T.Text])
        , ("amount", DI.fromList [10, 20, 5, 7.5, 3, 1 :: Double])
        ]

customersDF :: D.DataFrame
customersDF =
    D.fromColumns
        [ ("cid", DI.fromList [1, 2, 3, 4 :: Int])
        , ("region", DI.fromList ["eu", "us", "eu", "eu" :: T.Text])
        , ("tier", DI.fromList ["gold", "free", "free", "gold" :: T.Text])
        ]

usersDF :: D.DataFrame
usersDF =
    D.fromColumns
        [ ("uid", DI.fromList [1, 2, 4 :: Int])
        , ("name", DI.fromList ["ann", "bo", "cy" :: T.Text])
        ]

ordersT :: DT.TypedDataFrame Orders
ordersT = either (error . show) id (DT.freezeWithError ordersDF)

customersT :: DT.TypedDataFrame Customers
customersT = either (error . show) id (DT.freezeWithError customersDF)

usersT :: DT.TypedDataFrame Users
usersT = either (error . show) id (DT.freezeWithError usersDF)

-- | Compare all four results against the eager one.
fourWay ::
    String ->
    D.DataFrame ->
    DT.TypedDataFrame cols ->
    L.LazyDataFrame ->
    TL.TypedLazyDataFrame cols ->
    Test
fourWay lbl eager typed lazy typedLazy = TestCase $ do
    assertEqual (lbl ++ ": typed == eager") eager (DT.thaw typed)
    lazyDf <- L.run lazy
    assertEqual (lbl ++ ": lazy == eager") eager lazyDf
    typedLazyDf <- TL.run typedLazy
    assertEqual (lbl ++ ": typed lazy == eager") eager (DT.thaw typedLazyDf)

lo, lc, lu :: L.LazyDataFrame
lo = L.fromDataFrame ordersDF
lc = L.fromDataFrame customersDF
lu = L.fromDataFrame usersDF

tlo :: TL.TypedLazyDataFrame Orders
tlo = TL.fromDataFrame ordersT

tlc :: TL.TypedLazyDataFrame Customers
tlc = TL.fromDataFrame customersT

tlu :: TL.TypedLazyDataFrame Users
tlu = TL.fromDataFrame usersT

joins :: Test
joins =
    TestList
        [ fourWay
            "innerJoin multi-key"
            (D.innerJoin ["cid", "region"] ordersDF customersDF)
            (DT.innerJoin @'["cid", "region"] ordersT customersT)
            (L.innerJoin ["cid", "region"] lo lc)
            (TL.innerJoin @'["cid", "region"] tlo tlc)
        , fourWay
            "join INNER"
            (D.join D.INNER ["cid"] ordersDF customersDF)
            (DT.join @'DT.INNER @'["cid"] ordersT customersT)
            (L.join L.INNER ["cid"] lo lc)
            (TL.join @'TL.INNER @'["cid"] tlo tlc)
        , fourWay
            "leftJoin"
            (D.leftJoin ["cid"] ordersDF customersDF)
            (DT.leftJoin @'["cid"] ordersT customersT)
            (L.leftJoin ["cid"] lo lc)
            (TL.leftJoin @'["cid"] tlo tlc)
        , fourWay
            "rightJoin"
            (D.rightJoin ["cid"] ordersDF customersDF)
            (DT.rightJoin @'["cid"] ordersT customersT)
            (L.rightJoin ["cid"] lo lc)
            (TL.rightJoin @'["cid"] tlo tlc)
        , fourWay
            "fullOuterJoin"
            (D.fullOuterJoin ["cid"] ordersDF customersDF)
            (DT.fullOuterJoin @'["cid"] ordersT customersT)
            (L.fullOuterJoin ["cid"] lo lc)
            (TL.fullOuterJoin @'["cid"] tlo tlc)
        , fourWay
            "joinOn differing key names"
            (D.joinOn D.LEFT [("cid", "uid")] ordersDF usersDF)
            (DT.joinOn @'DT.LEFT @'[ '("cid", "uid")] ordersT usersT)
            (L.joinOn L.LEFT [("cid", "uid")] lo lu)
            (TL.joinOn @'TL.LEFT @'[ '("cid", "uid")] tlo tlu)
        ]

groupAndSort :: Test
groupAndSort =
    TestList
        [ fourWay
            "groupBy + aggregate + sortBy"
            ( ordersDF
                D.|> D.groupBy ["region"]
                D.|> D.aggregate
                    [ F.sum (F.col @Double "amount") `D.as` "total"
                    , F.count (F.col @Double "amount") `D.as` "n"
                    ]
                D.|> D.sortBy [D.Asc (F.col @T.Text "region")]
            )
            ( ordersT
                DT.|> DT.groupBy @'["region"]
                DT.|> DT.aggregate
                    ( DT.as @"n" (DTF.count (DT.col @"amount"))
                        . DT.as @"total" (DTF.sum (DT.col @"amount"))
                    )
                DT.|> DT.sortBy [DT.Asc (DT.col @"region")]
            )
            ( lo
                L.|> L.groupBy ["region"]
                L.|> L.aggregate
                    [ F.sum (F.col @Double "amount") `L.as` "total"
                    , F.count (F.col @Double "amount") `L.as` "n"
                    ]
                L.|> L.sortBy [L.Asc (L.col @T.Text "region")]
            )
            ( tlo
                TL.|> TL.groupBy @'["region"]
                TL.|> TL.aggregate
                    ( TL.as @"n" (DTF.count (TL.col @"amount"))
                        . TL.as @"total" (DTF.sum (TL.col @"amount"))
                    )
                TL.|> TL.sortBy [TL.Asc (TL.col @"region")]
            )
        , fourWay
            "sortBy expression, filter, derive, select, take"
            ( ordersDF
                D.|> D.filter (F.col @Double "amount" D..>. 2)
                D.|> D.derive "twice" (F.col @Double "amount" * 2)
                D.|> D.sortBy [D.Desc (F.col @Double "amount" * (-1))]
                D.|> D.select ["cid", "twice"]
                D.|> D.take 3
            )
            ( ordersT
                DT.|> DT.filter (DT.col @"amount" DT..>. 2)
                DT.|> DT.derive @"twice" (DT.col @"amount" * 2)
                DT.|> DT.sortBy [DT.Desc (DT.col @"amount" * (-1))]
                DT.|> DT.select @'["cid", "twice"]
                DT.|> DT.take 3
            )
            ( lo
                L.|> L.filter (F.col @Double "amount" L..>. 2)
                L.|> L.derive "twice" (F.col @Double "amount" * 2)
                L.|> L.sortBy [L.Desc (F.col @Double "amount" * (-1))]
                L.|> L.select ["cid", "twice"]
                L.|> L.take 3
            )
            ( tlo
                TL.|> TL.filter (TL.col @"amount" TL..>. 2)
                TL.|> TL.derive @"twice" (TL.col @"amount" * 2)
                TL.|> TL.sortBy [TL.Desc (TL.col @"amount" * (-1))]
                TL.|> TL.select @'["cid", "twice"]
                TL.|> TL.take 3
            )
        ]

tests :: [Test]
tests = [joins, groupAndSort]
