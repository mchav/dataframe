{-# LANGUAGE CPP #-}

-- | Backwards-compatibility re-export hub for the split TH modules.
module DataFrame.TH (
    module DataFrame.TH.Records,
#ifdef WITH_CSV_TH
    module DataFrame.TH.CSV,
#endif
#ifdef WITH_PARQUET_TH
    module DataFrame.TH.Parquet,
#endif
) where

import DataFrame.TH.Records
#ifdef WITH_CSV_TH
import DataFrame.TH.CSV
#endif
#ifdef WITH_PARQUET_TH
import DataFrame.TH.Parquet
#endif
