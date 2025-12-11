{-# LANGUAGE ExplicitNamespaces #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
module DataFrame.Monad where

import qualified DataFrame as D
import DataFrame (DataFrame)
import DataFrame.Internal.Expression (Expr)
import DataFrame.Internal.Column (Columnable)

import qualified Data.Text as T

-- A re-implementation of the state monad.
-- `mtl` might be too heavy a dependency just to get
-- a single monad instance.
newtype FrameM a = FrameM { runFrameM_ :: DataFrame -> (DataFrame, a) }

instance Functor FrameM where
  fmap f (FrameM g) = FrameM $ \df ->
    let (df', x) = g df
    in  (df', f x)

instance Applicative FrameM where
  pure x = FrameM (, x)
  FrameM ff <*> FrameM fx = FrameM $ \df ->
    let (df1, f) = ff df
        (df2, x) = fx df1
    in  (df2, f x)

instance Monad FrameM where
  FrameM g >>= f = FrameM $ \df ->
    let (df1, x) = g df
        FrameM h     = f x
    in  h df1

deriveM :: Columnable a => T.Text -> Expr a -> FrameM (Expr a)
deriveM name expr = FrameM $ \df ->
  let df' = D.derive name expr df
  in  (df', expr)    -- or (df', F.col @a name)

filterWhereM :: Expr Bool -> FrameM ()
filterWhereM p = FrameM $ \df ->
  (D.filterWhere p df, ())

runFrameM :: DataFrame -> FrameM a -> DataFrame
runFrameM df action = fst (runFrameM_ action df)
