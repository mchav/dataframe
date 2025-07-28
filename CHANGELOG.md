# Revision history for dataframe

## 0.3.0.0
* Now supports inner joins
```haskell
ghci> df |> D.innerJoin ["key_1", "key_2"] other
```
* Aggregations are now expressions allowing for more expressive aggregation logic. Previously: `D.aggregate [("quantity", D.Mean), ("price", D.Sum)] df` now ``D.aggregate [(F.sum (F.col @Double "label") / (F.count (F.col @Double "label")) `F.as` "positive_rate")]``
* In GHCI, you can now create type-safe bindings for each column and use those in expressions.

```haskell
ghci> :exposeColumns df
ghci> D.aggregate  [(F.sum label / F.count label) `F.as` "positive_rate"]
```
* Added pandas and polars benchmarks.
* Performance improvements to `groupBy`.
* Various bug fixes.

## 0.2.0.2
* Experimental Apache Parquet support.
* Rename conversion columns (changed from toColumn and toColumn' to fromVector and fromList).
* Rename constructor for dataframe to fromNamedColumns
* Create an error context for error messages so we can change the exceptions as they are thrown.
* Provide safe versions of building block functions that allow us to build good traces.
* Add readthedocs support.

## 0.2.0.1
* Fix bug with new comparison expressions. gt and geq were actually implemented as lt and leq.
* Changes to make library work with ghc 9.10.1 and 9.12.2

## 0.2.0.0
### Replace `Function` adt with a column expression syntax.

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
