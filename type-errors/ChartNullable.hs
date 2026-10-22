{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeApplications #-}

-- | Must NOT compile: a typed chart cannot encode a nullable column.
module ChartNullable where

import qualified DataFrame.Display.Web.Chart.Typed as C
import qualified DataFrame.Typed as DT

type S = '[ '("age", Maybe Double)]

badChart :: DT.TypedDataFrame S -> C.Chart S
badChart df = C.enc C.X (DT.col @"age") (C.chart df)
