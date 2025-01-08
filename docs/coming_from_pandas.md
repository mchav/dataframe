# Coming from pandas

We'll be porting over concepts from [10 minutes to Pandas](https://pandas.pydata.org/docs/user_guide/10min.html).

## Basic Data Structures

A pandas `Series` maps to a `Column`. `Series` are indexable (labelled) arrays. We currently don't support indexing so `Column`s aren't meant to be manipulated directly so we don't focus on them too much.

A `DataFrame` maps to a `DataFrame` as expected. Our dataframes are essentially a list of `Vector`s with some metadata for managing state.

## Creating our structures

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
ghci> D.toColumn [1, 3, 5, read @Float "NaN", 6, 8]
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
ghci> dates = D.toColumn $ Prelude.take 6 $ [fromGregorian 2013 01 01..]
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
ghci> import qualified Data.DataFrame as D
ghci> import qualified Data.Vector as V
ghci> import System.Random (randomRIO)
ghci> import Control.Monad (replicateM)
ghci> import Data.List (foldl')
ghci> :set -XOverloadedStrings
ghci> initDf = D.fromList [("date", dates)]
ghci> ns <- replicateM 4 (replicateM 6 (randomRIO (-2.0, 2.0)))
ghci> df = foldl' (\d (name, col) -> addColumn name (V.fromList col) d) initDf (zip ["A","B","C","D"] ns)
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

As hinted in the previous example we can create a dataframe with `fromList`. This function takes in a list of tuples. We don't broadast values like python does i.e if you put in a single value into a column all other values will be null/nothing. But we'll detail how to get the same functionality.

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

# Result
# df2
#      A          B    C  D      E    F
# 0  1.0 2013-01-02  1.0  3   test  foo
# 1  1.0 2013-01-02  1.0  3  train  foo
# 2  1.0 2013-01-02  1.0  3   test  foo
# 3  1.0 2013-01-02  1.0  3  train  foo

```

```haskell
-- All our data types must be printable and orderable.
data Transport = Test | Train deriving (Show, Ord, Eq)
ghci> :{
ghci| df = D.fromList [
ghci|        ("A", D.toColumn (replicate 4 1.0)),
ghci|        ("B", D.toColumn (replicate 4 (fromGregorian 2013 01 02))),
ghci|        ("C", D.toColumn (replicate 4 (1.0 :: Float))),
ghci|        ("D", D.toColumn (replicate 4 (3 :: Int))),
ghci|        ("E", D.toColumn (take 4 $ cycle [Test, Train])),
ghci|        ("F", D.toColumn (replicate 4 "foo"))]
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

## Viewing data

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

#### Sorting

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

## Selection
Panda's `[]` operator is a jack-knife that does a number of kinds of aggregation.
As such it doesn't map to one construct and doesn't always have an equivalent in Haskell.

### Selecting columns

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

## Missing values

Rows with missing values are represented by a `Maybe a` type. Dealing with missing values means applying the usual `Maybe` functions to the data.

### Filling

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
