# Revision history for dataframe

## 0.3.1.0
* Add new `selectBy` function which subsumes all the other select functions. Specifically we can:
    * `selectBy [byName "x"] df`: normal select.
    * `selectBy [byProperty isNumeric] df`: all columns with a given property.
    * `selectBy [byNameProperty (T.isPrefixOf "weight")] df`: select by column name predicate.
    * `selectBy [byIndexRange (0, 5)] df`: picks the first size columns.
    * `selectBy [byNameRange ("a", "c")] df`: select names within a range.
* Cut down dependencies to reduce binary/installation size.
* Add module for web plots that uses chartjs.
* Web plots can open in the browser.

## 0.3.0.4
* Fix bug with parquet reader.

## 0.3.0.3
* Improved parquet reader. The reader now supports most parquet files downloaded from internet sources
  * Supports all primitive parquet types plain and uncompressed.
  * Can decode both v1 and v2 data pages.
  * Supports Snappy and ZSTD compression.
  * Supports RLE/bitpacking encoding for primitive types
  * Backward compatible with INT96 type.
  * From the parquet-testing repo we can successfully read the following:
    * alltypes_dictionary.parquet
    * alltypes_plain.parquet
    * alltypes_plain.snappy.parquet
    * alltypes_tiny_pages_plain.parquet
    * binary_truncated_min_max.parquet
    * datapage_v1-corrupt-checksum.parquet
    * datapage_v1-snappy-compressed-checksum.parquet
    * datapage_v1-uncompressed-checksum.parquet
* Improve CSV parsing: Parse bytestring and convert to text only at the end. Remove some redundancies in parsing with suggestions from @Jhingon.
* Faster correlation computation.
* Update version of granite that ships with dataframe and add new scatterBy plot.

## 0.3.0.2
* Re-enable Parquet.
* Change columnInfo to describeColumns
* We can now convert columns to lists.
* Fast reductions and groupings. GroupBys are now a dataframe construct not a column construct (thanks to @stites).
* Filter is now faster because we do mutation on the index vector.
* Frequencies table nnow correctly display percentages (thanks @kayvank)
* Show table implementations have been unified (thanks @metapho-re)
* We now compute statistics on null columns
* Drastic improvement in plotting since we now use granite.

## 0.3.0.1
* Temporarily remove Parquet support. I think it'll be worth creating a spin off of snappy that doesn't rely on C bindings. Also I'll probably spin Parquet off into a separate library.

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
