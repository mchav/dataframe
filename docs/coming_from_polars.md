# Coming from Polars

This tutorial will walk through the examples in Polars' [getting started guide](https://docs.pola.rs/user-guide/getting-started/) showing how concepts in Polars map to dataframe.

## Reading and writing CSV

### Round trip test

To test our CSV IO we'll create a dataframe programtically, write it to a CSV file, then read the CSV file back again.

In polars this looks like:

```python
import polars as pl
import datetime as dt

df = pl.DataFrame(
    {
        "name": ["Alice Archer", "Ben Brown", "Chloe Cooper", "Daniel Donovan"],
        "birthdate": [
            dt.date(1997, 1, 10),
            dt.date(1985, 2, 15),
            dt.date(1983, 3, 22),
            dt.date(1981, 4, 30),
        ],
        "weight": [57.9, 72.5, 53.6, 83.1],  # (kg)
        "height": [1.56, 1.77, 1.65, 1.75],  # (m)
    }
)
df.write_csv("docs/assets/data/output.csv")
df_csv = pl.read_csv("docs/assets/data/output.csv", try_parse_dates=True)
print(df_csv)
```

As a standalone dataframe script this would look like.


```haskell
import qualified Data.DataFrame as D
import Data.Time.Calendar

main :: IO
main = do
    let df = D.fromList [
        ("name", D.toColumn [ "Alice Archer"
                            , "Ben Brown"
                            , "Chloe Cooper"
                            , "Daniel Donovan"])
        , ("birthdate", D.toColumn [ fromGregorian 1997 01 10
                                   , fromGregorian 1985 02 15
                                   , fromGregorian 1983 03 22
                                   , fromGregorian 1981 04 30])
        , ("weight", D.toColumn [57.9, 72.5, 53.6, 83.1])
        , ("height", D.toColumn [1.56, 1.77, 1.65, 1.75])]
    print df
    D.writeCsv "./data/output.csv" df
    let df_csv = D.readCsv "./data/output.csv"
    print df_csv
```

This round trip prints the following tables:

```
-----------------------------------------------------
index |      name      | birthdate  | weight | height
------|----------------|------------|--------|-------
 Int  |     [Char]     |    Day     | Double | Double
------|----------------|------------|--------|-------
0     | Alice Archer   | 1997-01-10 | 57.9   | 1.56  
1     | Ben Brown      | 1985-02-15 | 72.5   | 1.77  
2     | Chloe Cooper   | 1983-03-22 | 53.6   | 1.65  
3     | Daniel Donovan | 1981-04-30 | 83.1   | 1.75

-----------------------------------------------------
index |      name      | birthdate  | weight | height
------|----------------|------------|--------|-------
 Int  |      Text      |    Day     | Double | Double
------|----------------|------------|--------|-------
0     | Alice Archer   | 1997-01-10 | 57.9   | 1.56  
1     | Ben Brown      | 1985-02-15 | 72.5   | 1.77  
2     | Chloe Cooper   | 1983-03-22 | 53.6   | 1.65  
3     | Daniel Donovan | 1981-04-30 | 83.1   | 1.75  

```

Notice that the type of the string column changes from `[Char]` (Haskell's default) to `Text` (dataframe's default).


## Expressions

Rather than use expressions on columns like in Polars, dataframe uses a SQL-like API to transform the whole dataframe.

For example:

```python
result = df.select(
    pl.col("name"),
    pl.col("birthdate").dt.year().alias("birth_year"),
    (pl.col("weight") / (pl.col("height") ** 2)).alias("bmi"),
)
print(result)
```

Would be written as:

```haskell
import qualified Data.DataFrame as D
import qualified Data.Text as T

import Data.DataFrame.Operations ( (|>) )
import Data.Time.Calendar

main :: IO ()
main = do
    ...
    let year = (\(YearMonthDay y _ _) -> y)
    print $ df_csv
          |> D.derive "birth_year" year "birthdate"
          |> D.combine "bmi"
                      (\w h -> w / h ** 2)
                      "weight"
                      "height"
          |> D.select ["name", "birth_year", "bmi"]
```

Resulting in:

```
--------------------------------------------------------
index |      name      | birth_year |        bmi        
------|----------------|------------|-------------------
 Int  |      Text      |  Integer   |       Double      
------|----------------|------------|-------------------
0     | Alice Archer   | 1997       | 23.791913214990135
1     | Ben Brown      | 1985       | 23.14149829231702 
2     | Chloe Cooper   | 1983       | 19.687786960514234
3     | Daniel Donovan | 1981       | 27.13469387755102 
```

The dataframe implementation can be read top down. `apply` a function that gets the year to the `birthdate`; store the result in the `birth_year` column; combine `weight` and `height` into the bmi column using the formula `w / h ** 2`; then select the `name`, `birth_year` and `bmi` fields.

Dataframe focuses on splitting transformations into transformations on the whole dataframe so it's easily usable in a repl-like environment.

This means we also do not do expression expansion. We prefer each expression to roughly mimick functional programming concepts. In the previous example `apply` is a map over `birthdate` and `combine` zips `weight` and `height` with the bmi function.

So in the example Polars expression expansion example:

```python
result = df.select(
    pl.col("name"),
    (pl.col("weight", "height") * 0.95).round(2).name.suffix("-5%"),
)
print(result)
```

We instead write this two `applyWithAlias` calls:

```haskell
df_csv
    |> D.derive "weight-5%" (*0.95) "weight"
    -- Alternatively we can use the `as` function.
    |> D.as "height-5%" D.apply (*0.95) "height"
    |> D.select ["name", "weight-5%", "height-5%"]
```

```
----------------------------------------------------------------
index |      name      |     height-5%      |     weight-5%     
------|----------------|--------------------|-------------------
 Int  |     [Char]     |       Double       |       Double      
------|----------------|--------------------|-------------------
0     | Alice Archer   | 1.482              | 55.004999999999995
1     | Ben Brown      | 1.6815             | 68.875            
2     | Chloe Cooper   | 1.5675             | 50.92             
3     | Daniel Donovan | 1.6624999999999999 | 78.945
```

However we can make our program shorter by using regular Haskell and folding over the dataframe.

```haskell
let reduce name = D.derive (name <> "-5%") (*0.95) name
df_csv
    |> D.fold reduce ["weight", "height"]
    |> D.select ["name", "weight-5%", "height-5%"]
```

Or alternatively,

```haskell
addSuffix suffix name = D.rename name (name <> suffix)
df_csv
  |> D.applyMany ["weight", "height"] (*0.95)
  |> D.fold (addSuffix "-5%")
  |> D.select ["name", "weight-5%", "height-5%"]
```

Filtering looks much the same:

```python
result = df.filter(pl.col("birthdate").dt.year() < 1990)
print(result)
```
Versus

```haskell
bornAfter1990 = ( (< 1990)
                . (\(YearMonthDay y _ _) -> y))
df_csv &
    D.filter "birthdate" bornAfter1990
```

```
-----------------------------------------------------
index |      name      | birthdate  | weight | height
------|----------------|------------|--------|-------
 Int  |      Text      |    Day     | Double | Double
------|----------------|------------|--------|-------
0     | Ben Brown      | 1985-02-15 | 72.5   | 1.77  
1     | Chloe Cooper   | 1983-03-22 | 53.6   | 1.65  
2     | Daniel Donovan | 1981-04-30 | 83.1   | 1.75
```

For multiple filter conditions we again make all the filter statements separate. Filtering by m

```python
result = df.filter(
    pl.col("birthdate").is_between(dt.date(1982, 12, 31), dt.date(1996, 1, 1)),
    pl.col("height") > 1.7,
)
print(result)
```

```haskell
year (YearMonthDay y _ _) = y
between a b y = y >= a && y <= b 
df_csv
  |> D.filter "birthdate"
             (between 1982 1996 . year)
  |> D.filter "height" (1.7 <)
```

```
------------------------------------------------
index |   name    | birthdate  | weight | height
 Int  |   Text    |    Day     | Double | Double
------|-----------|------------|--------|-------
0     | Ben Brown | 1985-02-15 | 72.5   | 1.77
```

```python
result = df.group_by(
    (pl.col("birthdate").dt.year() // 10 * 10).alias("decade"),
    maintain_order=True,
).len()
print(result)
```

Polars's `groupBy` does an implicit select. In dataframe the select is written explcitly.

We implicitly create a `Count` variable as the result of grouping by an aggregate. In general when for a `groupByAgg` we create a variable with the same name as the aggregation to store the aggregation in. 

```haskell
let decade = (*10) . flip div 10 . year
df_csv
    |> D.derive "decade" decade "birthdate"
    |> D.select ["decade"]
    |> D.groupByAgg D.Count ["decade"]
```

```
----------------------
index | decade | Count
------|--------|------
 Int  |  Int   | Int
------|--------|------
0     | 1990   | 1  
1     | 1980   | 3 
```

TODO: Add notes

```python
result = df.group_by(
    (pl.col("birthdate").dt.year() // 10 * 10).alias("decade"),
    maintain_order=True,
).agg(
    pl.len().alias("sample_size"),
    pl.col("weight").mean().round(2).alias("avg_weight"),
    pl.col("height").max().alias("tallest"),
)
print(result)
```

```haskell
decade = (*10) . flip div 10 . year
df_csv
    |> D.derive "decade" decade "birthdate"
    |> D.groupByAgg D.Count ["decade"]
    |> D.aggregate [("height", D.Maximum), ("weight", D.Mean)]
    |> D.select ["decade", "sampleSize", "Mean_weight", "Maximum_height"]
```

```
----------------------------------------------------
index | decade  |    Mean_weight    | Maximum_height
------|---------|-------------------|---------------
 Int  | Integer |      Double       |     Double    
------|---------|-------------------|---------------
0     | 1990    | 57.9              | 1.56          
1     | 1980    | 69.73333333333333 | 1.77
```


```python
result = (
    df.with_columns(
        (pl.col("birthdate").dt.year() // 10 * 10).alias("decade"),
        pl.col("name").str.split(by=" ").list.first(),
    )
    .select(
        pl.all().exclude("birthdate"),
    )
    .group_by(
        pl.col("decade"),
        maintain_order=True,
    )
    .agg(
        pl.col("name"),
        pl.col("weight", "height").mean().round(2).name.prefix("avg_"),
    )
)
print(result)
```

```haskell
let firstWord = head . T.split (' ' ==)
df_csv
    |> D.apply firstWord "name"
    |> D.derive "decade" decade "birthdate"
    |> D.exclude ["birthdate"]
    |> D.groupByAgg D.Count ["decade"]
    |> D.aggregate [("weight",  D.Mean), ("height", D.Mean)]
```

```
-------------------------------------------------------------------------------------------
index | decade  |           name           | Count |    Mean_height     |    Mean_weight   
------|---------|--------------------------|-------|--------------------|------------------
 Int  | Integer |       Vector Text        |  Int  |       Double       |      Double      
------|---------|--------------------------|-------|--------------------|------------------
0     | 1990    | ["Alice"]                | 1     | 1.56               | 57.9             
1     | 1980    | ["Ben","Daniel","Chloe"] | 3     | 1.7233333333333334 | 69.73333333333333
```