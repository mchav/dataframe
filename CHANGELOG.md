# Revision history for dataframe

## 0.1.0.0

* Initial release

## 0.1.0.1

* Fixed parse failure on nested, escaped quotation.
* Fixed column info when field name isn't found. 

## 0.1.0.2

* Change namespace from `Data.DataFrame` to `DataFrame`
* Add `toVector` function for converting columns to vectors.
* Add `impute` function for replacing `Nothing` values in optional columns.
* Add `filterAllJust` to filter out all rows with missing data.
* Add `distinct` function that returns a dataframe with distict rows.

## 0.1.0.3
* Use older version of correlation for ihaskell itegration

# 0.2.0.0
* Remove `Function` adt with a column expression syntax.
* Adds a coverage report to the repository (thanks to @oforero)
* Convenience functions for comparisons
