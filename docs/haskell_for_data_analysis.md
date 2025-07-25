# Haskell for Data Analysis

This section ports/mirrors Wes McKinney's book [Python for Data Analysis](https://wesmckinney.com/book/). Examples and organizations are drawn from there. This tutorial does not assume an understanding of Haskell.

## What is a dataframe?

A DataFrame is like a spreadsheet or a table — it organizes data into rows and columns.

* Each column has a name (like "Name", "Age", or "Price") and usually contains the same type of information (like numbers or text).

* Each row is one entry or record — like a person, a product, or a day’s worth of sales.

Imagine an Excel sheet or Google Sheets file:

| Name	| Age	| City      |
|-------|-------|-----------|
| Alice	| 30	| New York  |
| Bob	| 25	| San Diego |
| Cara	| 35	| Austin    |

That’s essentially a DataFrame!

DataFrames make it easy to:
* Look at your data
* Filter or sort it (like showing only people over 30)
* Do math on it (like averaging ages)
* Clean it (like removing bad or incomplete data)

They're a key tool for data scientists, analysts, and programmers when working with data. This guide is about how to use dataframes in a language called Haskell.

## Why use Haskell?

* Having types around eliminates many kinds of bugs before you even run the code.
* It's easy to write pipelines.
* The Haskell compiler has a lot of optimization that makes code very fast.
* The syntax is more approachable than other compiled languages' dataframes.

## What to install
For most of this guide we will be using a tool called GHCi. This is a program that allows you to write and evaluate Haskell code interactively. In fact, the 'i' in GHCi means interactive. GHCi comes bundled in an installation of the Haskell programming language. At the time of writing, [ghcup](www.haskell.org/ghcup/) is the best way to install Haskell tooling. To get through this guide you're going to need a tool called Cabal (which also is installed visa ghcup). Cabal is a package manager for Haskell and allows you to get the code you can use to code along.

After you've installed cabal you'll need to install `dataframe`. To do so run `cabal update && cabal install dataframe` on your command line. To start running GHCi type `cabal repl --build-depends dataframe` in your terminal.

You're now ready to start using and exploring dataframes!

## Getting the data
Data enters a computer program in one of two ways:
* manual entry of the data by a human, or,
* through a file whose data was the output of another computer program or the result of manual entry.

We will show how to do both in dataframes.

### Entering the data manually

I live in Seattle where the weather is a legitimate, non-small-talk topic of conversation for most of the year. At any given point in time I care about what the weather is and what it will be. I'd like to do some simple computation on a week's worth of high and low temperatures. A week of data is small enough that I can enter it myself so I'll do just that.

```haskell
ghci> import qualified DataFrame as D
ghci> let df = D.fromNamedColumns [("Day", D.fromList ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]), ("High Temperature (Celcius)", D.fromList [24, 20, 22, 23, 25, 26, 26]), ("Low Temperature (Celcius)", D.fromList [14, 13, 13, 13, 14, 15, 15])]
ghci> df
--------------------------------------------------------------------------
index |    Day    | High Temperature (Celcius) | Low Temperature (Celcius)
------|-----------|----------------------------|--------------------------
 Int  |  [Char]   |          Integer           |          Integer         
------|-----------|----------------------------|--------------------------
0     | Monday    | 24                         | 14                       
1     | Tuesday   | 20                         | 13                       
2     | Wednesday | 22                         | 13                       
3     | Thursday  | 23                         | 13                       
4     | Friday    | 25                         | 14                       
5     | Saturday  | 26                         | 15                       
6     | Sunday    | 26                         | 15
```

We use the function `fromNamedColumns` to create a dataframe from manually entered data. The format of the function is `fromNamedColumns [(<name>, <column>), (<name>, <column>),...]`. It has an equivalent for data without column names called `fromUnnamedColumns`.

```haskell
ghci> let df = D.fromUnnamedColumns [D.fromList ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"], D.fromList [24, 20, 22, 23, 25, 26, 26], D.fromList [14, 13, 13, 13, 14, 15, 15]]
ghci> df
-------------------------------------
index |     0     |    1    |    2   
------|-----------|---------|--------
 Int  |  [Char]   | Integer | Integer
------|-----------|---------|--------
0     | Monday    | 24      | 14     
1     | Tuesday   | 20      | 13     
2     | Wednesday | 22      | 13     
3     | Thursday  | 23      | 13     
4     | Friday    | 25      | 14     
5     | Saturday  | 26      | 15     
6     | Sunday    | 26      | 15
```

This function automatcally names columns with numbers 0 to n. This is generally bad practice (everything must havea descriptive name) but is useful for an initial entry where the columns are unknown/have no name.

### Getting the data from a file

We can also get data froma CSV file using the `readCsv` function.

```haskell
ghci> df <- D.readCsv "./data/housing.csv" 
ghci> D.take 10 df
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
index | longitude | latitude | housing_median_age | total_rooms | total_bedrooms | population | households |   median_income    | median_house_value | ocean_proximity
------|-----------|----------|--------------------|-------------|----------------|------------|------------|--------------------|--------------------|----------------
 Int  |  Double   |  Double  |       Double       |   Double    |  Maybe Double  |   Double   |   Double   |       Double       |       Double       |      Text      
------|-----------|----------|--------------------|-------------|----------------|------------|------------|--------------------|--------------------|----------------
0     | -122.23   | 37.88    | 41.0               | 880.0       | Just 129.0     | 322.0      | 126.0      | 8.3252             | 452600.0           | NEAR BAY       
1     | -122.22   | 37.86    | 21.0               | 7099.0      | Just 1106.0    | 2401.0     | 1138.0     | 8.3014             | 358500.0           | NEAR BAY       
2     | -122.24   | 37.85    | 52.0               | 1467.0      | Just 190.0     | 496.0      | 177.0      | 7.2574             | 352100.0           | NEAR BAY       
3     | -122.25   | 37.85    | 52.0               | 1274.0      | Just 235.0     | 558.0      | 219.0      | 5.6431000000000004 | 341300.0           | NEAR BAY       
4     | -122.25   | 37.85    | 52.0               | 1627.0      | Just 280.0     | 565.0      | 259.0      | 3.8462             | 342200.0           | NEAR BAY       
5     | -122.25   | 37.85    | 52.0               | 919.0       | Just 213.0     | 413.0      | 193.0      | 4.0368             | 269700.0           | NEAR BAY       
6     | -122.25   | 37.84    | 52.0               | 2535.0      | Just 489.0     | 1094.0     | 514.0      | 3.6591             | 299200.0           | NEAR BAY       
7     | -122.25   | 37.84    | 52.0               | 3104.0      | Just 687.0     | 1157.0     | 647.0      | 3.12               | 241400.0           | NEAR BAY       
8     | -122.26   | 37.84    | 42.0               | 2555.0      | Just 665.0     | 1206.0     | 595.0      | 2.0804             | 226700.0           | NEAR BAY       
9     | -122.25   | 37.84    | 52.0               | 3549.0      | Just 707.0     | 1551.0     | 714.0      | 3.6912000000000003 | 261100.0           | NEAR BAY
```

We've introduced a new function in the example above. The `take` function, given a number `n` and a dataframe, cuts everything but the first `n` rows of a dataframe. We use the function so we can check a few rows of a dataframe.

## Data preparation
Data in the wild doesn't always come in a form that's easy to work with. A data analysis tool should make preparing and cleaning data easy. There are a number of common issues that data analysis too must handle. We'll go through a few common ones and show how to deal with them in Haskell. 

### Handling missing data
Data is oftentimes incomplete. Sometimes because of legitimate reasons, often times because of errors. Handling missing data is a foundational tasks in data analysis. In Haskell, potentially missing values are represented by a "wrapper" type called [`Maybe`](https://en.wikibooks.org/wiki/Haskell/Understanding_monads/Maybe).

```haskell
ghci> import qualified DataFrame as D
ghci> let df = D.fromUnnamedColumns [D.fromList [Just 1, Just 1, Nothing, Nothing], D.fromList [Just 6.5, Nothing, Nothing, Just 6.5], D.fromList [Just 3.0, Nothing, Nothing, Just 3.0]]
ghci> df
---------------------------------------------------
index |       0       |      1       |      2      
------|---------------|--------------|-------------
 Int  | Maybe Integer | Maybe Double | Maybe Double
------|---------------|--------------|-------------
0     | Just 1        | Just 6.5     | Just 3.0    
1     | Just 1        | Nothing      | Nothing     
2     | Nothing       | Nothing      | Nothing     
3     | Nothing       | Just 6.5     | Just 3.0    

```

If we'd like to drop all rows with missing values we can use the `filterJust` function.

```haskell
ghci> D.filterJust "0" df
---------------------------------------------
index |    0    |      1       |      2      
------|---------|--------------|-------------
 Int  | Integer | Maybe Double | Maybe Double
------|---------|--------------|-------------
0     | 1       | Just 6.5     | Just 3.0    
1     | 1       | Nothing      | Nothing     
```

The function filters out the non-`Nothing` values and "unwrap" the `Maybe` type. To filter all `Nothing` values we use the `filterAllJust` function.

```haskell
ghci> D.filterAllJust df
---------------------------------
index |    0    |   1    |   2   
------|---------|--------|-------
 Int  | Integer | Double | Double
------|---------|--------|-------
0     | 1       | 6.5    | 3.0   
```

To fill in the missing values we the impute function which replaces all instances of `Nothing` with a given value.

```haskell
ghci> D.impute "0" (0 :: Integer) df
---------------------------------------------
index |    0    |      1       |      2      
------|---------|--------------|-------------
 Int  | Integer | Maybe Double | Maybe Double
------|---------|--------------|-------------
0     | 1       | Just 6.5     | Just 3.0    
1     | 1       | Nothing      | Nothing     
2     | 0       | Nothing      | Nothing     
3     | 0       | Just 6.5     | Just 3.0    
```

There is no general way to replace ALL nothing values with a default since the default depends on the type. In fact, trying to apply the wrong type to a function throws an error:

```haskell
ghci> D.impute @Double "0" 0 df
*** Exception: 

[Error]: Type Mismatch
        While running your code I tried to get a column of type: "Maybe Double" but column was of type: "Maybe Integer"
        This happened when calling function apply on the column 0



        Try adding a type at the end of the function e.g change
                apply arg1 arg2 to 
                (apply arg1 arg2 :: <Type>)
        or add {-# LANGUAGE TypeApplications #-} to the top of your file then change the call to 
                apply @<Type> arg1 arg2
```

In general, Haskell would usually have a compile-time. But because dataframes are usually run in REPL-like environments which offer immediate feedback to users, `dataframe` is fine turning these into runtime exceptions.

