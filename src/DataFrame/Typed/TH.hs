{-# LANGUAGE CPP #-}

-- | Backwards-compatibility re-export hub for @DataFrame.Typed.TH.*@ modules.
module DataFrame.Typed.TH (
    module DataFrame.Typed.TH.Records,
#ifdef WITH_CSV_TH
    module DataFrame.Typed.TH.CSV,
#endif
#ifdef WITH_PARQUET_TH
    module DataFrame.Typed.TH.Parquet,
#endif
) where

import DataFrame.Typed.TH.Records
#ifdef WITH_CSV_TH
import DataFrame.Typed.TH.CSV
#endif
#ifdef WITH_PARQUET_TH
import DataFrame.Typed.TH.Parquet
#endif
