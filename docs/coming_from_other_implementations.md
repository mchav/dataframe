# Coming from other implementations

## Coming from pandas

We'll be porting over concepts from [10 minutes to Pandas](https://pandas.pydata.org/docs/user_guide/10min.html).

### Basic Data Structures

A pandas `Series` maps to a `Column`. `Series` are indexable (labelled) arrays. We currently don't support indexing so `Column`s aren't meant to be manipulated directly so we don't focus on them too much.

A `DataFrame` maps to a `DataFrame` as expected. Our dataframes are essentially a list of `Vector`s with some metadata for managing state.

### Creating our structures

Creaing a series.

```python
python> s = pd.Series([1, 3, 5, np.nan, 6, 8])
python> s
0    1.0
1    3.0
2    5.0
3    NaN
4    6.0
5    8.0
dtype: float64
```

```haskell
ghci> import qualified DataFrame as D
ghci> D.fromList [1, 3, 5, read @Float "NaN", 6, 8]
[1.0,3.0,5.0,NaN,6.0,8.0]
```

```python
python> dates = pd.date_range("20130101", periods=6)
python> dates
DatetimeIndex(['2013-01-01', '2013-01-02', '2013-01-03', '2013-01-04',
               '2013-01-05', '2013-01-06'],
              dtype='datetime64[ns]', freq='D')
```

```haskell
ghci> import Data.Time.Calendar
ghci> dates = D.fromList $ Prelude.take 6 $ [fromGregorian 2013 01 01..]
ghci> dates
[2013-01-01,2013-01-02,2013-01-03,2013-01-04,2013-01-05,2013-01-06]
```

Use the series to create a dataframe.

```python
python> df = pd.DataFrame(np.random.randn(6, 4), index=dates, columns=list("ABCD"))
python> df
                   A         B         C         D
2013-01-01  0.469112 -0.282863 -1.509059 -1.135632
2013-01-02  1.212112 -0.173215  0.119209 -1.044236
2013-01-03 -0.861849 -2.104569 -0.494929  1.071804
2013-01-04  0.721555 -0.706771 -1.039575  0.271860
2013-01-05 -0.424972  0.567020  0.276232 -1.087401
2013-01-06 -0.673690  0.113648 -1.478427  0.524988
```

```haskell
ghci> import qualified Data.Vector as V
ghci> import System.Random (randomRIO)
ghci> import Control.Monad (replicateM)
ghci> import Data.List (foldl')
ghci> :set -XOverloadedStrings
ghci> initDf = D.fromNamedColumns [("date", dates)]
ghci> ns <- replicateM 4 (replicateM 6 (randomRIO (-2.0, 2.0)))
ghci> df = foldl' (\d (name, col) -> D.insertColumn name (V.fromList col) d) initDf (zip ["A","B","C","D"] ns)
ghci> df
------------------------------------------------------------------------------------------------------------
index |    date    |          A          |          B           |          C           |          D         
------|------------|---------------------|----------------------|----------------------|--------------------
 Int  |    Day     |       Double        |        Double        |        Double        |       Double       
------|------------|---------------------|----------------------|----------------------|--------------------
0     | 2013-01-01 | 0.49287792598710745 | 1.2126312556288785   | -1.3553292904555625  | 1.8491213627748553 
1     | 2013-01-02 | 0.7936547276080512  | -1.5209756494542028  | -0.5208055385837551  | 0.8895325450813525 
2     | 2013-01-03 | 1.8883976214395153  | 1.3453541205495676   | -1.1801018894304223  | 0.20583994035730901
3     | 2013-01-04 | -1.3262867911904324 | -0.37375298679005686 | -0.8580515357149543  | 1.4681616115128593 
4     | 2013-01-05 | 1.9068894062167745  | 0.792553168600036    | -0.13526265076664545 | -1.6239378251651466
5     | 2013-01-06 | -0.5541246187320041 | -1.5791034339829042  | -1.5650415391333796  | -1.7802523632196152
```

As hinted in the previous example we can create a dataframe with `fromNamedColumns`. This function takes in a list of tuples. We don't broadast values like python does i.e if you put in a single value into a column all other values will be null/nothing. But we'll detail how to get the same functionality.

```python
df2 = pd.DataFrame(
    {
        "A": 1.0,
        "B": pd.Timestamp("20130102"),
        "C": pd.Series(1, index=list(range(4)), dtype="float32"),
        "D": np.array([3] * 4, dtype="int32"),
        "E": pd.Categorical(["test", "train", "test", "train"]),
        "F": "foo",
    }
)

## Result
## df2
##      A          B    C  D      E    F
## 0  1.0 2013-01-02  1.0  3   test  foo
## 1  1.0 2013-01-02  1.0  3  train  foo
## 2  1.0 2013-01-02  1.0  3   test  foo
## 3  1.0 2013-01-02  1.0  3  train  foo

```

```haskell
-- All our data types must be printable and orderable.
data Transport = Test | Train deriving (Show, Ord, Eq)
ghci> :{
ghci| df = D.fromNamedColumns [
ghci|        ("A", D.fromList (replicate 4 1.0)),
ghci|        ("B", D.fromList (replicate 4 (fromGregorian 2013 01 02))),
ghci|        ("C", D.fromList (replicate 4 (1.0 :: Float))),
ghci|        ("D", D.fromList (replicate 4 (3 :: Int))),
ghci|        ("E", D.fromList (take 4 $ cycle [Test, Train])),
ghci|        ("F", D.fromList (replicate 4 "foo"))]
ghci|:}
ghci> df
--------------------------------------------------------------
index |   A    |     B      |   C   |  D  |     E     |   F   
------|--------|------------|-------|-----|-----------|-------
 Int  | Double |    Day     | Float | Int | Transport | [Char]
------|--------|------------|-------|-----|-----------|-------
0     | 1.0    | 2013-01-02 | 1.0   | 3   | Test      | foo   
1     | 1.0    | 2013-01-02 | 1.0   | 3   | Train     | foo   
2     | 1.0    | 2013-01-02 | 1.0   | 3   | Test      | foo   
3     | 1.0    | 2013-01-02 | 1.0   | 3   | Train     | foo
```

Rather than label a string value as categorial we create a type that encapsulates the value.

### Viewing data

By default we print the whole dataframe. To see the first `n` rows we instead provide a `take` function that takes in as arguments `n` and the dataframe.

```haskell
ghci> D.take 2 df
--------------------------------------------------------------
index |   A    |     B      |   C   |  D  |     E     |   F   
------|--------|------------|-------|-----|-----------|-------
 Int  | Double |    Day     | Float | Int | Transport | [Char]
------|--------|------------|-------|-----|-----------|-------
0     | 1.0    | 2013-01-02 | 1.0   | 3   | Test      | foo   
1     | 1.0    | 2013-01-02 | 1.0   | 3   | Train     | foo 
```

Our equivalent of describe is `summarize`:

```haskell
ghci> D.summarize df
-----------------------------------------------------
index | Statistic |     D     |     C     |     A    
------|-----------|-----------|-----------|----------
 Int  |   Text    |  Double   |  Double   |  Double  
------|-----------|-----------|-----------|----------
0     | Mean      | 3.0       | 1.0       | 1.0      
1     | Minimum   | 3.0       | 1.0       | 1.0      
2     | 25%       | 3.0       | 1.0       | 1.0      
3     | Median    | 3.0       | 1.0       | 1.0      
4     | 75%       | 3.0       | 1.0       | 1.0      
5     | Max       | 3.0       | 1.0       | 1.0      
6     | StdDev    | 0.0       | 0.0       | 0.0      
7     | IQR       | 0.0       | 0.0       | 0.0      
8     | Skewness  | -Infinity | -Infinity | -Infinity
```

##### Sorting

Since we don't have indexes we only have one sort function that sorts by a column.

```haskell
ghci> D.sortBy D.Ascending ["E"] df
--------------------------------------------------------------
index |   A    |     B      |   C   |  D  |     E     |   F   
------|--------|------------|-------|-----|-----------|-------
 Int  | Double |    Day     | Float | Int | Transport | [Char]
------|--------|------------|-------|-----|-----------|-------
0     | 1.0    | 2013-01-02 | 1.0   | 3   | Test      | foo   
1     | 1.0    | 2013-01-02 | 1.0   | 3   | Test      | foo   
2     | 1.0    | 2013-01-02 | 1.0   | 3   | Train     | foo   
3     | 1.0    | 2013-01-02 | 1.0   | 3   | Train     | foo
```

### Selection
Panda's `[]` operator is a jack-knife that does a number of kinds of aggregation.
As such it doesn't map to one construct and doesn't always have an equivalent in Haskell.

#### Selecting columns

```python
python> df.loc[:, ["A", "B"]]
                   A         B
2013-01-01  0.469112 -0.282863
2013-01-02  1.212112 -0.173215
2013-01-03 -0.861849 -2.104569
2013-01-04  0.721555 -0.706771
2013-01-05 -0.424972  0.567020
2013-01-06 -0.673690  0.113648
```

Pandas indexes the dataframe like a 2D array. We get all rows with `:` and then specify which columns after the comma.

In DataFrame we mimick SQL's select.

```haskell
ghci> D.select ["A"] df
--------------
index |   A   
------|-------
 Int  | Double
------|-------
0     | 1.0   
1     | 1.0   
2     | 1.0   
3     | 1.0
```

To filter by rows we have to filter by the values we are interested in rather than indexes.

```python
python> df.loc["20130102":"20130104", ["A", "B"]]
                   A         B
2013-01-02  1.212112 -0.173215
2013-01-03 -0.861849 -2.104569
2013-01-04  0.721555 -0.706771
```

```haskell
ghci> :{
ghci| df' |> D.filter "date" (\d -> d >= (fromGregorian 2013 01 02) && d <= (fromGregorian 2013 01 04))
ghci| |> D.select ["A", "B"]
ghci| :}
ghci> df
---------------------------
index |   A    |     B     
------|--------|-----------
 Int  | Double |    Day    
------|--------|-----------
0     | 1.0    | 2013-01-02
1     | 1.0    | 2013-01-02
2     | 1.0    | 2013-01-02
```

### Missing values

Rows with missing values are represented by a `Maybe a` type. Dealing with missing values means applying the usual `Maybe` functions to the data.

#### Filling

```haskell
ghci> df' = D.addColumn "G" (V.fromList [Just 1, Just 2, Nothing, Just 4]) df
ghci> df'
------------------------------------------------------------------------------
index |   A    |     B      |   C   |  D  |     E     |   F    |       G      
------|--------|------------|-------|-----|-----------|--------|--------------
 Int  | Double |    Day     | Float | Int | Transport | [Char] | Maybe Integer
------|--------|------------|-------|-----|-----------|--------|--------------
0     | 1.0    | 2013-01-02 | 1.0   | 3   | Test      | foo    | Just 1       
1     | 1.0    | 2013-01-02 | 1.0   | 3   | Train     | foo    | Just 2       
2     | 1.0    | 2013-01-02 | 1.0   | 3   | Test      | foo    | Nothing      
3     | 1.0    | 2013-01-02 | 1.0   | 3   | Train     | foo    | Just 4 
ghci> D.apply (fromMaybe 5) "G" df'
------------------------------------------------------------------------
index |   A    |     B      |   C   |  D  |     E     |   F    |    G   
------|--------|------------|-------|-----|-----------|--------|--------
 Int  | Double |    Day     | Float | Int | Transport | [Char] | Integer
------|--------|------------|-------|-----|-----------|--------|--------
0     | 1.0    | 2013-01-02 | 1.0   | 3   | Test      | foo    | 1      
1     | 1.0    | 2013-01-02 | 1.0   | 3   | Train     | foo    | 2      
2     | 1.0    | 2013-01-02 | 1.0   | 3   | Test      | foo    | 5      
3     | 1.0    | 2013-01-02 | 1.0   | 3   | Train     | foo    | 4
ghci> df' |> D.filter "G" (isJust @Integer)
------------------------------------------------------------------------------
index |   A    |     B      |   C   |  D  |     E     |   F    |       G      
------|--------|------------|-------|-----|-----------|--------|--------------
 Int  | Double |    Day     | Float | Int | Transport | [Char] | Maybe Integer
------|--------|------------|-------|-----|-----------|--------|--------------
0     | 1.0    | 2013-01-02 | 1.0   | 3   | Test      | foo    | Just 1       
1     | 1.0    | 2013-01-02 | 1.0   | 3   | Train     | foo    | Just 2       
2     | 1.0    | 2013-01-02 | 1.0   | 3   | Train     | foo    | Just 4
```

## Coming from Polars

This tutorial will walk through the examples in Polars' [getting started guide](https://docs.pola.rs/user-guide/getting-started/) showing how concepts in Polars map to dataframe.

### Reading and writing CSV

#### Round trip test

To test our CSV IO we'll create a dataframe programmatically, write it to a CSV file, then read the CSV file back again.

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
        "weight": [57.9, 72.5, 53.6, 83.1],  ## (kg)
        "height": [1.56, 1.77, 1.65, 1.75],  ## (m)
    }
)
df.write_csv("docs/assets/data/output.csv")
df_csv = pl.read_csv("docs/assets/data/output.csv", try_parse_dates=True)
print(df_csv)
```

As a standalone dataframe script this would look like.


```haskell
import qualified DataFrame as D
import Data.Time.Calendar

main :: IO
main = do
    let df = D.fromList [
        ("name", D.fromList [ "Alice Archer"
                            , "Ben Brown"
                            , "Chloe Cooper"
                            , "Daniel Donovan"])
        , ("birthdate", D.fromList [ fromGregorian 1997 01 10
                                   , fromGregorian 1985 02 15
                                   , fromGregorian 1983 03 22
                                   , fromGregorian 1981 04 30])
        , ("weight", D.fromList [57.9, 72.5, 53.6, 83.1])
        , ("height", D.fromList [1.56, 1.77, 1.65, 1.75])]
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


### Expressions

We support expressions similar to Polars and PySpark. These expressions help us write row-level computations.

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
{-## LANGUAGE ScopedTypeVariables #-}
{-## LANGUAGE TypeApplications #-}
import qualified DataFrame as D
import qualified Data.Text as T

import DataFrame.Operations ( (|>) )
import Data.Time.Calendar

main :: IO ()
main = do
    ...
    let year = (\(YearMonthDay y _ _) -> y)
    print $ df_csv
          |> D.derive "birth_year" (F.lift year (F.col @Day "birthdate"))
          |> D.derive "bmi" ((F.col @Double "weight") / (F.col @Double "height" ** F.lit 2))
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


The Haskell implementation can be read top down:
* Create a column called `birth_year` by getting the year from the `birthdate` column.
* Create a column called `bmi`which is computed as `weight / height ** 2`, 
* then select the `name`, `birth_year` and `bmi` fields.

`lift` takes a regular, unary (one argument) Haskell function and applied it to a column. To apply a binary function to two columns we use `lift2`.

The Polars column type can be a single column or a list of columns. This means that applying a single transformation to many columns can be written as follows:

In the example Polars expression expansion example:

```python
result = df.select(
    pl.col("name"),
    (pl.col("weight", "height") * 0.95).round(2).name.suffix("-5%"),
)
print(result)
```

In Haskell, we don't provide a way of doing this out of the box. So you'd have to write something more explicit:

```haskell
df_csv
    |> D.derive "weight-5%" ((col @Double "weight") * (lit 0.95))
    |> D.derive "height-5%" ((col @Double "height") * (lit 0.95))
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

We can use standard Haskell machinery to make the program short without sactificing readability.

```haskell
let reduce name = D.derive (name <> "-5%") ((col @Double name) * (lit 0.95))
df_csv
    |> D.fold reduce ["weight", "height"]
    |> D.select ["name", "weight-5%", "height-5%"]
```

Or alternatively, if our transformation only involves the variable we are modifying we can write the same code as follows:

```haskell
addSuffix suffix name = D.rename name (name <> suffix)
df_csv
  |> D.applyMany ["weight", "height"] (*0.95)
  -- We have to rename the fields so they match what we had before.
  |> D.fold (addSuffix "-5%")
  |> D.select ["name", "weight-5%", "height-5%"]
```

This means that we can still rely on the expressive power of Haskell itself without relying entirely on the column expressions. This keeps our implementation more flexible.

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

Polars's `groupBy` does an implicit select. In dataframe the select is written explicitly.

We implicitly create a `Count` variable as the result of grouping by an aggregate. In general when for a `groupByAgg` we create a variable with the same name as the aggregation to store the aggregation in.

```haskell
let decade d = (year d) `div` 10 * 10
df_csv
    |> D.derive "decade" (lift decade (col @Day "birthdate"))
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
    |> D.derive "decade" (lift decade (col @Day "birthdate"))
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

## Coming from dplyr

This tutorial will walk through the examples in dplyr's [mini tutorial](https://dplyr.tidyverse.org/) showing how concepts in dplyr map to dataframe.

### Filtering
Filtering looks similar in both libraries.

```r
starwars %>% 
  filter(species == "Droid")
#> ## A tibble: 6 × 14
#>   name   height  mass hair_color skin_color  eye_color birth_year sex   gender  
#>   <chr>   <int> <dbl> <chr>      <chr>       <chr>          <dbl> <chr> <chr>   
#> 1 C-3PO     167    75 <NA>       gold        yellow           112 none  masculi…
#> 2 R2-D2      96    32 <NA>       white, blue red               33 none  masculi…
#> 3 R5-D4      97    32 <NA>       white, red  red               NA none  masculi…
#> 4 IG-88     200   140 none       metal       red               15 none  masculi…
#> 5 R4-P17     96    NA none       silver, red red, blue         NA none  feminine
#> ## ℹ 1 more row
#> ## ℹ 5 more variables: homeworld <chr>, species <chr>, films <list>,
#> ##   vehicles <list>, starships <list>
```

```haskell
starwars |> D.filter "species" (("Droid" :: Str.Text) ==)
         |> D.take 10
```

```
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
index |  name  |  height   |   mass    | hair_color | skin_color  | eye_color | birth_year | sex  |  gender   | homeworld | species |                                                                   films                                                                   |  vehicles  | starships 
------|--------|-----------|-----------|------------|-------------|-----------|------------|------|-----------|-----------|---------|-------------------------------------------------------------------------------------------------------------------------------------------|------------|-----------
 Int  |  Text  | Maybe Int | Maybe Int |    Text    |    Text     |   Text    | Maybe Int  | Text |   Text    |   Text    |  Text   |                                                                   Text                                                                    | Maybe Text | Maybe Text
------|--------|-----------|-----------|------------|-------------|-----------|------------|------|-----------|-----------|---------|-------------------------------------------------------------------------------------------------------------------------------------------|------------|-----------
0     | C-3PO  | Just 167  | Just 75   | NA         | gold        | yellow    | Just 112   | none | masculine | Tatooine  | Droid   | A New Hope, The Empire Strikes Back, Return of the Jedi, The Phantom Menace, Attack of the Clones, Revenge of the Sith                    | Nothing    | Nothing   
1     | R2-D2  | Just 96   | Just 32   | NA         | white, blue | red       | Just 33    | none | masculine | Naboo     | Droid   | A New Hope, The Empire Strikes Back, Return of the Jedi, The Phantom Menace, Attack of the Clones, Revenge of the Sith, The Force Awakens | Nothing    | Nothing   
2     | R5-D4  | Just 97   | Just 32   | NA         | white, red  | red       | Nothing    | none | masculine | Tatooine  | Droid   | A New Hope                                                                                                                                | Nothing    | Nothing   
3     | IG-88  | Just 200  | Just 140  | none       | metal       | red       | Just 15    | none | masculine | NA        | Droid   | The Empire Strikes Back                                                                                                                   | Nothing    | Nothing   
4     | R4-P17 | Just 96   | Nothing   | none       | silver, red | red, blue | Nothing    | none | feminine  | NA        | Droid   | Attack of the Clones, Revenge of the Sith                                                                                                 | Nothing    | Nothing   
5     | BB8    | Nothing   | Nothing   | none       | none        | black     | Nothing    | none | masculine | NA        | Droid   | The Force Awakens                                                                                                                         | Nothing    | Nothing
```

### Selecting columns
Select looks similar except in Haskell we take as argument a list of strings instead of a mix of predicates and strings.

```r
starwars %>% 
  select(name, ends_with("color"))
#> ## A tibble: 87 × 4
#>   name           hair_color skin_color  eye_color
#>   <chr>          <chr>      <chr>       <chr>    
#> 1 Luke Skywalker blond      fair        blue     
#> 2 C-3PO          <NA>       gold        yellow   
#> 3 R2-D2          <NA>       white, blue red      
#> 4 Darth Vader    none       white       yellow   
#> 5 Leia Organa    brown      light       brown    
#> ## ℹ 82 more rows
```

To get the same predicate-like functionality we use `selectBy`.

```haskell
starwars |> D.selectBy (\cname -> cname == "name" || T.isSuffixOf "color" cname)
         |> D.take 10
```


```
--------------------------------------------------------------------
index |        name        |  hair_color   | skin_color  | eye_color
------|--------------------|---------------|-------------|----------
 Int  |        Text        |     Text      |    Text     |   Text   
------|--------------------|---------------|-------------|----------
0     | Luke Skywalker     | blond         | fair        | blue     
1     | C-3PO              | NA            | gold        | yellow   
2     | R2-D2              | NA            | white, blue | red      
3     | Darth Vader        | none          | white       | yellow   
4     | Leia Organa        | brown         | light       | brown    
5     | Owen Lars          | brown, grey   | light       | blue     
6     | Beru Whitesun Lars | brown         | light       | blue     
7     | R5-D4              | NA            | white, red  | red      
8     | Biggs Darklighter  | black         | light       | brown    
9     | Obi-Wan Kenobi     | auburn, white | fair        | blue-gray
```

### Transforming columns

R has a general mutate function that takes in a mix of expressions and column names.

```r
starwars %>% 
  mutate(name, bmi = mass / ((height / 100)  ^ 2)) %>%
  select(name:mass, bmi)
#> ## A tibble: 87 × 4
#>   name           height  mass   bmi
#>   <chr>           <int> <dbl> <dbl>
#> 1 Luke Skywalker    172    77  26.0
#> 2 C-3PO             167    75  26.9
#> 3 R2-D2              96    32  34.7
#> 4 Darth Vader       202   136  33.3
#> 5 Leia Organa       150    49  21.8
#> ## ℹ 82 more rows
```

Our logic is more explicit about what's going on. Because both our fields are nullable/optional we have to specify the type.

```haskell
convertEitherToDouble name d = D.apply (either (\unparsed -> if unparsed == "NA" then Nothing else D.readDouble unparsed) (Just . (fromIntegral @Int))) name d

starwars
  |> D.fold convertEitherToDouble ["mass", "height"]
  |> D.selectRange ("name", "mass")
  -- Remove Nothing/empty rows.
  |> D.filterJust "mass"
  |> D.filterJust "height"
  |> D.derive "bmi" ((F.col @Double "mass") / (F.lift2 (**) (F.col @Double "height") (F.lit 2)))
  |> D.take 10
```

```
-------------------------------------------------------------------------------
index |         name          |  height   |   mass    |           bmi          
------|-----------------------|-----------|-----------|------------------------
 Int  |         Text          | Maybe Int | Maybe Int |      Maybe Double      
------|-----------------------|-----------|-----------|------------------------
0     | Luke Skywalker        | Just 172  | Just 77   | Just 26.027582477014604
1     | C-3PO                 | Just 167  | Just 75   | Just 26.89232313815483 
2     | R2-D2                 | Just 96   | Just 32   | Just 34.72222222222222 
3     | Darth Vader           | Just 202  | Just 136  | Just 33.33006567983531 
4     | Leia Organa           | Just 150  | Just 49   | Just 21.77777777777778 
5     | Owen Lars             | Just 178  | Just 120  | Just 37.87400580734756 
6     | Beru Whitesun Lars    | Just 165  | Just 75   | Just 27.548209366391188
7     | R5-D4                 | Just 97   | Just 32   | Just 34.009990434690195
8     | Biggs Darklighter     | Just 183  | Just 84   | Just 25.082863029651524
9     | Obi-Wan Kenobi        | Just 182  | Just 77   | Just 23.24598478444632 
```

Haskell's applicative syntax does take some getting used to.

`f <$> a` means apply f to the thing inside the "container". In this
case the container (or more infamously the monad) is of type `Maybe`.
So this can also be written as `fmap f a`.

But this only works if our `f` takes a single argument. If it takes
two arguments then the we use `<*>` to specify the second argument.

So, applying bmi to two optionals can be written as:

```haskell
ghci> fmap (+) (Just 2) <*> Just 2
Just 4
ghci> (+) <$> Just 2 <*> Just 2
Just 4
```

You'll find a wealth of functions for dealing with optionals in the package
`Data.Maybe`.

### Sorting

```r
starwars %>% 
  arrange(desc(mass))
#> ## A tibble: 87 × 14
#>   name      height  mass hair_color skin_color eye_color birth_year sex   gender
#>   <chr>      <int> <dbl> <chr>      <chr>      <chr>          <dbl> <chr> <chr> 
#> 1 Jabba De…    175  1358 <NA>       green-tan… orange         600   herm… mascu…
#> 2 Grievous     216   159 none       brown, wh… green, y…       NA   male  mascu…
#> 3 IG-88        200   140 none       metal      red             15   none  mascu…
#> 4 Darth Va…    202   136 none       white      yellow          41.9 male  mascu…
#> 5 Tarfful      234   136 brown      brown      blue            NA   male  mascu…
#> ## ℹ 82 more rows
#> ## ℹ 5 more variables: homeworld <chr>, species <chr>, films <list>,
#> ##   vehicles <list>, starships <list>
```

```haskell
starwars |> D.sortBy D.Descending ["mass"] |> D.take 5
```

```
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
index |         name          |  height   |   mass    | hair_color |    skin_color    |   eye_color   | birth_year |      sex       |  gender   | homeworld | species |                                    films                                     |              vehicles              |            starships           
------|-----------------------|-----------|-----------|------------|------------------|---------------|------------|----------------|-----------|-----------|---------|------------------------------------------------------------------------------|------------------------------------|--------------------------------
 Int  |         Text          | Maybe Int | Maybe Int |    Text    |       Text       |     Text      | Maybe Int  |      Text      |   Text    |   Text    |  Text   |                                     Text                                     |             Maybe Text             |           Maybe Text           
------|-----------------------|-----------|-----------|------------|------------------|---------------|------------|----------------|-----------|-----------|---------|------------------------------------------------------------------------------|------------------------------------|--------------------------------
0     | Jabba Desilijic Tiure | Just 175  | Just 1358 | NA         | green-tan, brown | orange        | Just 600   | hermaphroditic | masculine | Nal Hutta | Hutt    | A New Hope, Return of the Jedi, The Phantom Menace                           | Nothing                            | Nothing                        
1     | Grievous              | Just 216  | Just 159  | none       | brown, white     | green, yellow | Nothing    | male           | masculine | Kalee     | Kaleesh | Revenge of the Sith                                                          | Just "Tsmeu-6 personal wheel bike" | Just "Belbullab-22 starfighter"
2     | IG-88                 | Just 200  | Just 140  | none       | metal            | red           | Just 15    | none           | masculine | NA        | Droid   | The Empire Strikes Back                                                      | Nothing                            | Nothing                        
3     | Tarfful               | Just 234  | Just 136  | brown      | brown            | blue          | Nothing    | male           | masculine | Kashyyyk  | Wookiee | Revenge of the Sith                                                          | Nothing                            | Nothing                        
4     | Darth Vader           | Just 202  | Just 136  | none       | white            | yellow        | Nothing    | male           | masculine | Tatooine  | Human   | A New Hope, The Empire Strikes Back, Return of the Jedi, Revenge of the Sith | Nothing                            | Just "TIE Advanced x1"
```

### Grouping and aggregating

```r
starwars %>%
  group_by(species) %>%
  summarise(
    n = n(),
    mass = mean(mass, na.rm = TRUE)
  ) %>%
  filter(
    n > 1,
    mass > 50
  )
#> ## A tibble: 9 × 3
#>   species      n  mass
#>   <chr>    <int> <dbl>
#> 1 Droid        6  69.8
#> 2 Gungan       3  74  
#> 3 Human       35  81.3
#> 4 Kaminoan     2  88  
#> 5 Mirialan     2  53.1
#> ## ℹ 4 more rows
```

```haskell
starwars |> D.select ["species", "mass"]
         |> D.groupByAgg D.Count ["species"]
         -- This will be saved in a variable called  "Mean_mass"
         |> D.reduceByAgg D.Mean "mass"
         -- Always better to be explcit about types for
         -- numbers but you can also turn on defaults
         -- to save keystrokes.
         |> D.filterWhere (F.lift2 (&&) (F.lift (>1) (F.col @Int "Count")) (F.lift (>50) (F.col @Int "Mean_mass")))
```

```
--------------------------------------------
index | species  |     Mean_mass     | Count
------|----------|-------------------|------
 Int  |   Text   |      Double       |  Int 
------|----------|-------------------|------
0     | Human    | 81.47368421052632 | 35   
1     | Droid    | 69.75             | 6    
2     | Wookiee  | 124.0             | 2    
3     | NA       | 81.0              | 4    
4     | Gungan   | 74.0              | 3    
5     | Zabrak   | 80.0              | 2    
6     | Twi'lek  | 55.0              | 2    
7     | Kaminoan | 88.0              | 2
```

