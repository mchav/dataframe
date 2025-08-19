module DataFrame.IO.Parquet.Page where

import Data.Int

data PageType
  = DATA_PAGE
  | INDEX_PAGE
  | DICTIONARY_PAGE
  | DATA_PAGE_V2
  | PAGE_TYPE_UNKNOWN
  deriving (Show, Eq)

pageTypeFromInt :: Int32 -> PageType
pageTypeFromInt 0 = DATA_PAGE
pageTypeFromInt 1 = INDEX_PAGE
pageTypeFromInt 2 = DICTIONARY_PAGE
pageTypeFromInt 3 = DATA_PAGE_V2
pageTypeFromInt _ = PAGE_TYPE_UNKNOWN

