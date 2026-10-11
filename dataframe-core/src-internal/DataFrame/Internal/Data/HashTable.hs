{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | A flat open-addressing hash table mapping row hash to a dense group id.
module DataFrame.Internal.Data.HashTable (
    HashTable (..),
    newHashTable,
    htInsert,
    nextPow2Above,
) where

import Control.Monad.Primitive (PrimMonad, PrimState)
import Data.Bits (popCount, unsafeShiftR, (.&.))
import qualified Data.Vector.Unboxed.Mutable as VUM
import Data.Word (Word64)

-- | An open-addressing linear-probe table with a power-of-two capacity.
data HashTable s = HashTable
    { htHash :: !(VUM.MVector s Int)
    , htGroup :: !(VUM.MVector s Int)
    , htRep :: !(VUM.MVector s Int)
    , htMask :: !Int
    }

-- | Smallest power of two strictly greater than @n@, at least 2.
nextPow2Above :: Int -> Int
nextPow2Above n = go 2
  where
    go !p
        | p > n = p
        | otherwise = go (p * 2)
{-# INLINE nextPow2Above #-}

-- | Allocate an empty table for up to @n@ groups at load factor under 0.5.
newHashTable :: (PrimMonad m) => Int -> m (HashTable (PrimState m))
newHashTable n = do
    let !cap = nextPow2Above (2 * max 1 n)
    h <- VUM.unsafeNew cap
    g <- VUM.replicate cap (-1)
    r <- VUM.unsafeNew cap
    pure (HashTable h g r (cap - 1))
{-# INLINE newHashTable #-}

-- | Look up @row@ by hash and return its group id, adding one if new.
htInsert ::
    (PrimMonad m) =>
    HashTable (PrimState m) ->
    -- | @eqRow a b@: do rows @a@ and @b@ have equal key columns?
    (Int -> Int -> Bool) ->
    -- | Next dense group id to assign if this row starts a new group.
    Int ->
    -- | Row index being inserted.
    Int ->
    -- | Precomputed hash of the row's key.
    Int ->
    m (Int, Bool)
htInsert ht eqRow nextGroup row hash = go (homeSlot mask hash)
  where
    !mask = htMask ht
    !hs = htHash ht
    !gs = htGroup ht
    !rs = htRep ht
    go !slot = do
        g <- VUM.unsafeRead gs slot
        if g < 0
            then do
                VUM.unsafeWrite hs slot hash
                VUM.unsafeWrite gs slot nextGroup
                VUM.unsafeWrite rs slot row
                pure (nextGroup, True)
            else do
                h <- VUM.unsafeRead hs slot
                if h == hash
                    then do
                        rep <- VUM.unsafeRead rs slot
                        if eqRow rep row
                            then pure (g, False)
                            else go ((slot + 1) .&. mask)
                    else go ((slot + 1) .&. mask)
{-# INLINE htInsert #-}

-- | Home slot of a hash: the top bits of a Fibonacci multiply.
homeSlot :: Int -> Int -> Int
homeSlot !mask !hash =
    fromIntegral
        ( (fromIntegral hash * (0x9E3779B97F4A7C15 :: Word64))
            `unsafeShiftR` (64 - popCount mask)
        )
{-# INLINE homeSlot #-}
