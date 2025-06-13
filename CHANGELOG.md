# Revision history for dataframe

## 0.2.0.0
### Remove `Function` adt with a column expression syntax.

Previously, we tried to stay as close to Haskell as possible. We used the explicit
ordering of the column names in the first part of the tuple to determine the function
arguments and the a regular Haskell function that we evaluated piece-wise on each row.

```haskell
let multiply (a :: Int) (b :: Double) = fromIntegral a * b
let withTotalPrice = D.deriveFrom (["quantity", "item_price"], D.func multiply) "total_price" df
```

Now, we have a column expression syntax that mirrors Pyspark and Polars.

```haskell
let withTotalPrice = D.derive "total_price" (D.lift fromIntegral (D.col @Int "quantity") * (D.col @Double"item_price")) df
```

### Adds a coverage report to the repository (thanks to @oforero)
We don't have good test coverage right now. This will help us determine where to invest.
@oforero provided a script to make an HPC HTML report for coverage.

### Convenience functions for comparisons 
Instead of lifting all bool operations we provide `eq`, `leq` etc.

## 0.1.0.3
* Use older version of correlation for ihaskell itegration

## 0.1.0.2
* Change namespace from `Data.DataFrame` to `DataFrame`
* Add `toVector` function for converting columns to vectors.
* Add `impute` function for replacing `Nothing` values in optional columns.
* Add `filterAllJust` to filter out all rows with missing data.
* Add `distinct` function that returns a dataframe with distict rows.

## 0.1.0.1
* Fixed parse failure on nested, escaped quotation.
* Fixed column info when field name isn't found.

## 0.1.0.0
* Initial release
