<h1 align="center">
  <a href="https://dataframe.readthedocs.io/en/latest/">
    <img width="100" height="100" src="https://raw.githubusercontent.com/mchav/dataframe/master/docs/_static/haskell-logo.svg" alt="dataframe logo">
  </a>
</h1>

<div align="center">
  <a href="https://hackage.haskell.org/package/dataframe-0.2.0.2">
    <img src="https://img.shields.io/hackage/v/dataframe" alt="hackage Latest Release"/>
  </a>
  <a href="https://github.com/mchav/dataframe/actions/workflows/haskel-ci.yml">
    <img src="https://github.com/mchav/dataframe/actions/workflows/haskell-ci.yml/badge.svg" alt="C/I"/>
  </a>
</div>

<p align="center">
  <a href="https://dataframe.readthedocs.io/en/latest/">User guide</a>
  |
  <a href="https://discord.gg/XJE5wKT2kb">Discord</a>
</p>

# DataFrame

A fast, safe, and intuitive DataFrame library.

## Why use this DataFrame library?

* Encourages concise, declarative, and composable data pipelines.
* Static typing makes code easier to reason about and catches many bugs at compile time—before your code ever runs.
* Delivers high performance thanks to Haskell’s optimizing compiler and efficient memory model.
* Designed for interactivity: expressive syntax, helpful error messages, and sensible defaults.

## Example usage

### Interactive environment
```haskell
ghci> import qualified DataFrame as D
ghci> import DataFrame ((|>))
ghci> df <- D.readCsv "./data/housing.csv"
ghci> D.columnInfo df
--------------------------------------------------------------------------------------------------------------------
index |    Column Name     | # Non-null Values | # Null Values | # Partially parsed | # Unique Values |     Type    
------|--------------------|-------------------|---------------|--------------------|-----------------|-------------
 Int  |        Text        |        Int        |      Int      |        Int         |       Int       |     Text    
------|--------------------|-------------------|---------------|--------------------|-----------------|-------------
0     | total_bedrooms     | 20433             | 207           | 0                  | 1924            | Maybe Double
1     | ocean_proximity    | 20640             | 0             | 0                  | 5               | Text        
2     | median_house_value | 20640             | 0             | 0                  | 3842            | Double      
3     | median_income      | 20640             | 0             | 0                  | 12928           | Double      
4     | households         | 20640             | 0             | 0                  | 1815            | Double      
5     | population         | 20640             | 0             | 0                  | 3888            | Double      
6     | total_rooms        | 20640             | 0             | 0                  | 5926            | Double      
7     | housing_median_age | 20640             | 0             | 0                  | 52              | Double      
8     | latitude           | 20640             | 0             | 0                  | 862             | Double      
9     | longitude          | 20640             | 0             | 0                  | 844             | Double
ghci> :exposeColumns df
ghci> import qualified DataFrame.Functions as F
ghci> df |> D.groupBy ["ocean_proximity"] |> D.aggregate [(F.mean median_house_value) `F.as` "avg_house_value" ]
--------------------------------------------
index | ocean_proximity |  avg_house_value  
------|-----------------|-------------------
 Int  |      Text       |       Double      
------|-----------------|-------------------
0     | <1H OCEAN       | 240084.28546409807
1     | INLAND          | 124805.39200122119
2     | ISLAND          | 380440.0          
3     | NEAR BAY        | 259212.31179039303
4     | NEAR OCEAN      | 249433.97742663656
ghci> df |> D.derive "rooms_per_household" (total_rooms / households) |> D.take 10
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
index | longitude | latitude | housing_median_age | total_rooms | total_bedrooms | population | households |   median_income    | median_house_value | ocean_proximity | rooms_per_household
------|-----------|----------|--------------------|-------------|----------------|------------|------------|--------------------|--------------------|-----------------|--------------------
 Int  |  Double   |  Double  |       Double       |   Double    |  Maybe Double  |   Double   |   Double   |       Double       |       Double       |      Text       |       Double       
------|-----------|----------|--------------------|-------------|----------------|------------|------------|--------------------|--------------------|-----------------|--------------------
0     | -122.23   | 37.88    | 41.0               | 880.0       | Just 129.0     | 322.0      | 126.0      | 8.3252             | 452600.0           | NEAR BAY        | 6.984126984126984  
1     | -122.22   | 37.86    | 21.0               | 7099.0      | Just 1106.0    | 2401.0     | 1138.0     | 8.3014             | 358500.0           | NEAR BAY        | 6.238137082601054  
2     | -122.24   | 37.85    | 52.0               | 1467.0      | Just 190.0     | 496.0      | 177.0      | 7.2574             | 352100.0           | NEAR BAY        | 8.288135593220339  
3     | -122.25   | 37.85    | 52.0               | 1274.0      | Just 235.0     | 558.0      | 219.0      | 5.6431000000000004 | 341300.0           | NEAR BAY        | 5.8173515981735155 
4     | -122.25   | 37.85    | 52.0               | 1627.0      | Just 280.0     | 565.0      | 259.0      | 3.8462             | 342200.0           | NEAR BAY        | 6.281853281853282  
5     | -122.25   | 37.85    | 52.0               | 919.0       | Just 213.0     | 413.0      | 193.0      | 4.0368             | 269700.0           | NEAR BAY        | 4.761658031088083  
6     | -122.25   | 37.84    | 52.0               | 2535.0      | Just 489.0     | 1094.0     | 514.0      | 3.6591             | 299200.0           | NEAR BAY        | 4.9319066147859925 
7     | -122.25   | 37.84    | 52.0               | 3104.0      | Just 687.0     | 1157.0     | 647.0      | 3.12               | 241400.0           | NEAR BAY        | 4.797527047913447  
8     | -122.26   | 37.84    | 42.0               | 2555.0      | Just 665.0     | 1206.0     | 595.0      | 2.0804             | 226700.0           | NEAR BAY        | 4.294117647058823  
9     | -122.25   | 37.84    | 52.0               | 3549.0      | Just 707.0     | 1551.0     | 714.0      | 3.6912000000000003 | 261100.0           | NEAR BAY        | 4.970588235294118
ghci> df |> D.derive "nonsense_feature" (latitude + ocean_proximity) |> D.take 10

<interactive>:14:47: error: [GHC-83865]
    • Couldn't match type ‘Text’ with ‘Double’
      Expected: Expr Double
        Actual: Expr Text
    • In the second argument of ‘(+)’, namely ‘ocean_proximity’
      In the second argument of ‘derive’, namely
        ‘(latitude + ocean_proximity)’
      In the second argument of ‘(|>)’, namely
        ‘derive "nonsense_feature" (latitude + ocean_proximity)’
```

Key features in example:
* Intuitive, SQL-like API to get from data to insights.
* Create type-safe references to columns in a dataframe using `:exponseColumns`
* Type-safe column transformations for faster and safer exploration.
* Fluid, chaining API that makes code easy to reason about.

### Standalone script example
```haskell
-- Useful Haskell extensions.
{-# LANGUAGE OverloadedStrings #-} -- Allow string literal to be interpreted as any other string type.
{-# LANGUAGE TypeApplications #-} -- Convenience syntax for specifiying the type `sum a b :: Int` vs `sum @Int a b'. 

import qualified DataFrame as D -- import for general functionality.
import qualified DataFrame.Functions as F -- import for column expressions.

import DataFrame ((|>)) -- import chaining operator with unqualified.

main :: IO ()
main = do
    df <- D.readTsv "./data/chipotle.tsv"
    let quantity = F.col "quantity" :: D.Expr Int -- A typed reference to a column.
    print (df
      |> D.select ["item_name", "quantity"]
      |> D.groupBy ["item_name"]
      |> D.aggregate [ (F.sum quantity)     `F.as` "sum_quantity"
                     , (F.mean quantity)    `F.as` "mean_quantity"
                     , (F.maximum quantity) `F.as` "maximum_quantity"
                     ]
      |> D.sortBy D.Descending ["sum_quantity"]
      |> D.take 10)

```

Output:

```
------------------------------------------------------------------------------------------
index |          item_name           | sum_quantity |    mean_quanity    | maximum_quanity
------|------------------------------|--------------|--------------------|----------------
 Int  |             Text             |     Int      |       Double       |       Int      
------|------------------------------|--------------|--------------------|----------------
0     | Chicken Bowl                 | 761          | 1.0482093663911847 | 3              
1     | Chicken Burrito              | 591          | 1.0687160940325497 | 4              
2     | Chips and Guacamole          | 506          | 1.0563674321503131 | 4              
3     | Steak Burrito                | 386          | 1.048913043478261  | 3              
4     | Canned Soft Drink            | 351          | 1.1661129568106312 | 4              
5     | Chips                        | 230          | 1.0900473933649288 | 3              
6     | Steak Bowl                   | 221          | 1.04739336492891   | 3              
7     | Bottled Water                | 211          | 1.3024691358024691 | 10             
8     | Chips and Fresh Tomato Salsa | 130          | 1.1818181818181819 | 15             
9     | Canned Soda                  | 126          | 1.2115384615384615 | 4 
```

Full example in `./examples` folder using many of the constructs in the API.

### Visual example
![Screencast of usage in GHCI](./static/example.gif)

## Installing

### Jupyter notebook
* We have a [hosted version of the Jupyter notebook](https://ihaskell-dataframe-crf7g5fvcpahdegz.westus2-01.azurewebsites.net/lab/) on azure sites.
* Use the Dockerfile in the [ihaskell-dataframe](https://github.com/mchav/ihaskell-dataframe) to build and run an image with dataframe integration.
* For a preview check out the [California Housing](https://ihaskell-dataframe-crf7g5fvcpahdegz.westus2-01.azurewebsites.net/lab/tree/California%20Housing.ipynb) notebook.

### CLI
* Install Haskell (ghc + cabal) via [ghcup](https://www.haskell.org/ghcup/install/) selecting all the default options.
* Install snappy (needed for Parquet support) by running: `sudo apt install libsnappy-dev`.
* To install dataframe run `cabal update && cabal install dataframe`
* Open a Haskell repl with dataframe loaded by running `cabal repl --build-depends dataframe`.
* Follow along any one of the tutorials below.


## What is exploratory data analysis?
We provide a primer [here](https://github.com/mchav/dataframe/blob/main/docs/exploratory_data_analysis_primer.md) and show how to do some common analyses.

## Coming from other dataframe libraries
Familiar with another dataframe library? Get started:
* [Coming from Pandas](https://github.com/mchav/dataframe/blob/main/docs/coming_from_pandas.md)
* [Coming from Polars](https://github.com/mchav/dataframe/blob/main/docs/coming_from_polars.md)
* [Coming from dplyr](https://github.com/mchav/dataframe/blob/main/docs/coming_from_dplyr.md)

## Supported input formats
* CSV
* Apache Parquet (still buggy and experimental)

## Future work
* Apache arrow compatability
* Integration with common data formats (currently only supports CSV)
* Support windowed plotting (currently only supports ASCII plots)
* Host the whole library + Jupyter lab on Azure with auth and isolation.
