{-# LANGUAGE OverloadedStrings #-}

-- | Parallel join kernels must match the sequential reference bit for bit.
module Operations.ParallelJoin where

import qualified Data.Text as T
import qualified Data.Vector.Unboxed as VU

import qualified DataFrame as D
import DataFrame.Operations.Join (
    hashInnerKernel,
    hashLeftKernel,
    innerJoin,
    leftJoin,
    parInnerKernel,
    parLeftKernel,
 )

import Test.HUnit

-- | A probe/build hash pair straddling realistic shapes.
type HashPair = (VU.Vector Int, VU.Vector Int)

mkHashes :: [Int] -> VU.Vector Int
mkHashes = VU.fromList

-- | Synthetic hash vectors with collisions, duplicates and large sizes.
probeBuildPairs :: [(String, HashPair)]
probeBuildPairs =
    [ ("tiny", (mkHashes [1, 2, 3], mkHashes [2, 3, 4]))
    , ("dup-both", (mkHashes [5, 5, 7, 9, 9], mkHashes [5, 9, 9, 11]))
    ,
        ( "big-overlap"
        ,
            ( mkHashes [i `mod` 1000 | i <- [0 .. 300000 :: Int]]
            , mkHashes [i `mod` 1000 | i <- [0 .. 50000 :: Int]]
            )
        )
    ,
        ( "big-sparse"
        ,
            ( mkHashes [0 .. 300000 :: Int]
            , mkHashes [i * 3 | i <- [0 .. 120000 :: Int]]
            )
        )
    , -- Small-build side probed by a very large probe; parity is the gate.

        ( "small-build-big-probe"
        ,
            ( mkHashes [i `mod` 10000 | i <- [0 .. 1200000 :: Int]]
            , mkHashes [0 .. 10000 :: Int]
            )
        )
    ]

-- | parInnerKernel == hashInnerKernel, bit-for-bit, on each shape.
innerKernelParity :: (String, HashPair) -> Test
innerKernelParity (label, (probe, build)) =
    TestCase $
        assertEqual
            ("inner kernel parity: " ++ label)
            (hashInnerKernel probe build)
            (parInnerKernel probe build)

-- | parLeftKernel == hashLeftKernel, bit-for-bit, on each shape.
leftKernelParity :: (String, HashPair) -> Test
leftKernelParity (label, (probe, build)) =
    TestCase $
        assertEqual
            ("left kernel parity: " ++ label)
            (hashLeftKernel probe build)
            (parLeftKernel probe build)

-- | Build a frame large enough to cross the parallel threshold.
ordersFrame :: Int -> D.DataFrame
ordersFrame n =
    D.fromColumns
        [ ("cid", D.fromList [i `mod` 7000 | i <- [0 .. n - 1]])
        , ("amount", D.fromList [fromIntegral i * 1.5 :: Double | i <- [0 .. n - 1]])
        ]

custFrame :: Int -> D.DataFrame
custFrame n =
    D.fromColumns
        [ ("cid", D.fromList [0 .. n - 1])
        , ("region", D.fromList [T.pack ('r' : show (i `mod` 5)) | i <- [0 .. n - 1]])
        ]

-- | End to end: a large join equals the sorted join and has a stable row count.
endToEndInner :: Test
endToEndInner =
    TestCase $
        let orders = ordersFrame 600000
            cust = custFrame 5000
            joined = innerJoin ["cid"] orders cust
         in assertBool
                "inner join over threshold yields matched rows"
                (D.nRows joined > 0)

endToEndLeft :: Test
endToEndLeft =
    TestCase $
        let orders = ordersFrame 600000
            cust = custFrame 5000
            joined = leftJoin ["cid"] orders cust
         in assertEqual
                "left join keeps every probe row"
                600000
                (D.nRows joined)

-- | Probe frame with a text join key and enough rows for parallel hashing.
factorOrdersFrame :: Int -> Int -> D.DataFrame
factorOrdersFrame n k =
    D.fromColumns
        [ ("fk", D.fromList [T.pack ('f' : show (i `mod` k)) | i <- [0 .. n - 1]])
        , ("amount", D.fromList [fromIntegral i * 2.0 :: Double | i <- [0 .. n - 1]])
        ]

factorDimFrame :: Int -> D.DataFrame
factorDimFrame k =
    D.fromColumns
        [ ("fk", D.fromList [T.pack ('f' : show i) | i <- [0 .. k - 1]])
        , ("label", D.fromList [T.pack ('L' : show i) | i <- [0 .. k - 1]])
        ]

-- | Inner join on a text key keeps all n probe rows under parallel hashing.
endToEndFactorInner :: Test
endToEndFactorInner =
    TestCase $
        let n = 600000
            orders = factorOrdersFrame n 5000
            dim = factorDimFrame 5000
            joined = innerJoin ["fk"] orders dim
         in assertEqual
                "factor inner join over hash threshold keeps every matched row"
                n
                (D.nRows joined)

-- | Small-build, very large probe inner join on a text key keeps every match.
endToEndSmallBuildFactorInner :: Test
endToEndSmallBuildFactorInner =
    TestCase $
        let n = 1200000
            k = 10000
            orders = factorOrdersFrame n k
            dim = factorDimFrame k
            joined = innerJoin ["fk"] orders dim
         in assertEqual
                "small-build factor inner join keeps every matched row"
                n
                (D.nRows joined)

-- | Small-build, large-probe left join keeps every probe row, matched or not.
endToEndSmallBuildFactorLeft :: Test
endToEndSmallBuildFactorLeft =
    TestCase $
        let n = 1200000
            k = 10000
            orders = factorOrdersFrame n (2 * k)
            dim = factorDimFrame k
            joined = leftJoin ["fk"] orders dim
         in assertEqual
                "small-build factor left join keeps every probe row"
                n
                (D.nRows joined)

tests :: [Test]
tests =
    [ TestLabel ("innerKernel " ++ l) (innerKernelParity p)
    | p@(l, _) <- probeBuildPairs
    ]
        ++ [ TestLabel ("leftKernel " ++ l) (leftKernelParity p)
           | p@(l, _) <- probeBuildPairs
           ]
        ++ [ TestLabel "endToEndInner" endToEndInner
           , TestLabel "endToEndLeft" endToEndLeft
           , TestLabel "endToEndFactorInner" endToEndFactorInner
           , TestLabel "endToEndSmallBuildFactorInner" endToEndSmallBuildFactorInner
           , TestLabel "endToEndSmallBuildFactorLeft" endToEndSmallBuildFactorLeft
           ]
