{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}

module DataFrame.Internal.Interpreter.Kernels (
    Operand (..),
    Branch (..),
    CmpOp (..),
    cmpOpByName,
    ArithOp (..),
    arithOpByName,
    compareOperands,
    arithOperands,
    zipOperands,
    mergeBranches,
    partitionRows,
    intToDouble,
    packedEqLit,
    packedContainsLit,
    packedCharCount,
    parSumBy,
) where

import Control.Monad.ST (runST)
import Data.Bits ((.&.))
import qualified Data.Text as T
import qualified Data.Text.Array as A
import Data.Text.Internal (Text (Text))
import Data.Type.Equality (testEquality, (:~:) (Refl))
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import Type.Reflection (Typeable, typeRep)

import DataFrame.Internal.Column.Operations (parGenerateUnboxedInline)
import DataFrame.Internal.Control.Concurrent (parThreshold, parallelChunks)
import DataFrame.Internal.Data.PackedText (
    PackedOffsets (..),
    PackedTextData (..),
    packedLength,
    selAt,
    sliceEqBytes,
 )
import System.IO.Unsafe (unsafePerformIO)

-- | One side of an operation: the same value in every row, or one per row.
data Operand a = Literal !a | Column !(VU.Vector a)

data CmpOp = CmpLt | CmpLeq | CmpGt | CmpGeq | CmpEq | CmpNeq
    deriving (Eq, Show)

cmpOpByName :: T.Text -> Maybe CmpOp
cmpOpByName name = case name of
    "lt" -> Just CmpLt
    "leq" -> Just CmpLeq
    "gt" -> Just CmpGt
    "geq" -> Just CmpGeq
    "eq" -> Just CmpEq
    "neq" -> Just CmpNeq
    _ -> Nothing

data ArithOp = ArithAdd | ArithSub | ArithMult
    deriving (Eq, Show)

arithOpByName :: T.Text -> Maybe ArithOp
arithOpByName name = case name of
    "add" -> Just ArithAdd
    "sub" -> Just ArithSub
    "mult" -> Just ArithMult
    _ -> Nothing

-- | Runs @k@ with the element type known, only for 'Double' or 'Int'.
withNumeric ::
    forall a r.
    (Typeable a) =>
    (forall x. (VU.Unbox x, Ord x, Num x) => a :~: x -> r) -> Maybe r
withNumeric k
    | Just Refl <- testEquality (typeRep @a) (typeRep @Double) =
        Just (k @Double Refl)
    | Just Refl <- testEquality (typeRep @a) (typeRep @Int) = Just (k @Int Refl)
    | otherwise = Nothing
{-# INLINE withNumeric #-}

compareOperands ::
    forall a.
    (Typeable a) => Maybe (CmpOp -> Operand a -> Operand a -> Operand Bool)
compareOperands = withNumeric @a (\Refl -> compareLoop)

arithOperands ::
    forall a. (Typeable a) => Maybe (ArithOp -> Operand a -> Operand a -> Operand a)
arithOperands = withNumeric @a (\Refl -> arithLoop)

-- | One branch of an if-then-else: constant, every-row, or chosen-rows values.
data Branch a = Constant !a | EveryRow !(VU.Vector a) | ChosenRows !(VU.Vector a)

-- | Each row's value from the branch its condition chooses.
mergeBranches ::
    forall a.
    (Typeable a) => Maybe (VU.Vector Bool -> Branch a -> Branch a -> VU.Vector a)
mergeBranches = withNumeric @a (\Refl -> mergeLoop)

-- The operator must be fully applied or it is not inlined into the loop.
compareLoop ::
    (VU.Unbox a, Ord a) => CmpOp -> Operand a -> Operand a -> Operand Bool
compareLoop op l r = case op of
    CmpLt -> zipOperands (<) l r
    CmpLeq -> zipOperands (<=) l r
    CmpGt -> zipOperands (>) l r
    CmpGeq -> zipOperands (>=) l r
    CmpEq -> zipOperands (==) l r
    CmpNeq -> zipOperands (/=) l r
{-# INLINE compareLoop #-}

arithLoop ::
    (VU.Unbox a, Num a) => ArithOp -> Operand a -> Operand a -> Operand a
arithLoop op l r = case op of
    ArithAdd -> zipOperands (+) l r
    ArithSub -> zipOperands (-) l r
    ArithMult -> zipOperands (*) l r
{-# INLINE arithLoop #-}

zipOperands ::
    (VU.Unbox a, VU.Unbox b, VU.Unbox c) =>
    (a -> b -> c) -> Operand a -> Operand b -> Operand c
zipOperands f (Literal x) (Literal y) = Literal (f x y)
zipOperands f (Column xs) (Literal y) =
    Column
        (parGenerateUnboxedInline (VU.length xs) (\i -> f (VU.unsafeIndex xs i) y))
zipOperands f (Literal x) (Column ys) =
    Column (parGenerateUnboxedInline (VU.length ys) (f x . VU.unsafeIndex ys))
zipOperands f (Column xs) (Column ys) =
    Column
        ( parGenerateUnboxedInline
            (min (VU.length xs) (VU.length ys))
            (\i -> f (VU.unsafeIndex xs i) (VU.unsafeIndex ys i))
        )
{-# INLINE zipOperands #-}

mergeLoop ::
    (VU.Unbox a) => VU.Vector Bool -> Branch a -> Branch a -> VU.Vector a
mergeLoop cs l r = case (l, r) of
    -- With a value per chosen row, the output is filled in row order.
    (ChosenRows _, _) -> inOrder
    (_, ChosenRows _) -> inOrder
    _ ->
        parGenerateUnboxedInline
            n
            (\i -> if VU.unsafeIndex cs i then at l i 0 else at r i 0)
  where
    n = VU.length cs
    -- Row i's value, given how many values this branch has given out.
    at (Constant x) _ _ = x
    at (EveryRow xs) i _ = VU.unsafeIndex xs i
    at (ChosenRows xs) _ used = VU.unsafeIndex xs used
    {-# INLINE at #-}
    inOrder = runST $ do
        out <- VUM.unsafeNew n
        let go !i !lUsed !rUsed
                | i >= n = pure ()
                | VU.unsafeIndex cs i =
                    VUM.unsafeWrite out i (at l i lUsed) >> go (i + 1) (lUsed + 1) rUsed
                | otherwise =
                    VUM.unsafeWrite out i (at r i rUsed) >> go (i + 1) lUsed (rUsed + 1)
        go 0 0 0
        VU.unsafeFreeze out
{-# INLINE mergeLoop #-}

-- | The positions where the condition holds and where it does not.
partitionRows :: VU.Vector Bool -> (VU.Vector Int, VU.Vector Int)
partitionRows cs = runST $ do
    let n = VU.length cs
    yes <- VUM.unsafeNew n
    no <- VUM.unsafeNew n
    -- Write to both and advance one side, so there is no data-dependent branch.
    let go !i !ny !nn
            | i >= n = pure (ny, nn)
            | otherwise = do
                let c = fromEnum (VU.unsafeIndex cs i)
                VUM.unsafeWrite yes ny i
                VUM.unsafeWrite no nn i
                go (i + 1) (ny + c) (nn + 1 - c)
    (ny, nn) <- go 0 0 0
    (,) <$> VU.unsafeFreeze (VUM.take ny yes) <*> VU.unsafeFreeze (VUM.take nn no)

intToDouble :: VU.Vector Int -> VU.Vector Double
intToDouble v = parGenerateUnboxedInline (VU.length v) (fromIntegral . VU.unsafeIndex v)
{-# NOINLINE intToDouble #-}

-- | Whether each row of a packed Text column equals a literal.
packedEqLit :: Bool -> PackedTextData -> T.Text -> VU.Vector Bool
packedEqLit wantEq p@(PackedTextData arr offs sel _) (Text la lo ll) =
    -- Match the layout once so each row costs two offset reads and a compare.
    case (sel, offs) of
        (Nothing, Offs32 o) -> gen id (off32 o)
        (Nothing, Offs64 o) -> gen id (VU.unsafeIndex o)
        (Just s, Offs32 o) -> gen (selAt s) (off32 o)
        (Just s, Offs64 o) -> gen (selAt s) (VU.unsafeIndex o)
  where
    off32 o i = fromIntegral (VU.unsafeIndex o i)
    -- A negative base row is the empty string.
    gen base off =
        parGenerateUnboxedInline (packedLength p) $ \i ->
            let !r = base i
                !o = if r < 0 then 0 else off r
                !l = if r < 0 then 0 else off (r + 1) - o
             in (l == ll && sliceEqBytes arr o l la lo ll) == wantEq
    {-# INLINE gen #-}
{-# NOINLINE packedEqLit #-}

-- | Whether each row of a packed Text column contains a literal substring.
packedContainsLit :: PackedTextData -> T.Text -> VU.Vector Bool
packedContainsLit p@(PackedTextData arr offs sel _) (Text la lo ll)
    | ll == 0 = VU.replicate (packedLength p) True
    | otherwise = case (sel, offs) of
        (Nothing, Offs32 o) -> gen id (off32 o)
        (Nothing, Offs64 o) -> gen id (VU.unsafeIndex o)
        (Just s, Offs32 o) -> gen (selAt s) (off32 o)
        (Just s, Offs64 o) -> gen (selAt s) (VU.unsafeIndex o)
  where
    off32 o i = fromIntegral (VU.unsafeIndex o i)
    !first = A.unsafeIndex la lo
    gen base off =
        parGenerateUnboxedInline (packedLength p) $ \i ->
            let !r = base i
             in r >= 0 && search (off r) (off (r + 1))
    {-# INLINE gen #-}
    -- Scan for the needle's first byte, then compare the rest.
    search !o !end = go o
      where
        !lastStart = end - ll
        go !k
            | k > lastStart = False
            | A.unsafeIndex arr k == first && sliceEqBytes arr k ll la lo ll = True
            | otherwise = go (k + 1)
{-# NOINLINE packedContainsLit #-}

-- | Character count of each row of a packed Text column (UTF-8 lead bytes).
packedCharCount :: PackedTextData -> VU.Vector Int
packedCharCount p@(PackedTextData arr offs sel _) = case (sel, offs) of
    (Nothing, Offs32 o) -> gen id (off32 o)
    (Nothing, Offs64 o) -> gen id (VU.unsafeIndex o)
    (Just s, Offs32 o) -> gen (selAt s) (off32 o)
    (Just s, Offs64 o) -> gen (selAt s) (VU.unsafeIndex o)
  where
    off32 o i = fromIntegral (VU.unsafeIndex o i)
    gen base off =
        parGenerateUnboxedInline (packedLength p) $ \i ->
            let !r = base i
             in if r < 0 then 0 else count (off r) (off (r + 1)) 0
    {-# INLINE gen #-}
    count !k !end !n
        | k >= end = n
        | A.unsafeIndex arr k .&. 0xC0 == 0x80 = count (k + 1) end n
        | otherwise = count (k + 1) end (n + 1)
{-# NOINLINE packedCharCount #-}

-- | @sum (map f v)@ as one parallel loop with no intermediate column.
parSumBy :: (VU.Unbox a, Num b) => (a -> b) -> VU.Vector a -> b
parSumBy f v = unsafePerformIO $ do
    parts <-
        parallelChunks parThreshold (VU.length v) (\lo hi -> pure $! go lo hi 0)
    pure $! sum parts
  where
    go !i !hi !acc
        | i >= hi = acc
        | otherwise = go (i + 1) hi (acc + f (VU.unsafeIndex v i))
{-# INLINE parSumBy #-}
