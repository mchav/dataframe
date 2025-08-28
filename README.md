<h1 align="center">
  <a href="https://dataframe.readthedocs.io/en/latest/">
    <img width="100" height="100" src="https://raw.githubusercontent.com/mchav/dataframe/master/docs/_static/haskell-logo.svg" alt="dataframe logo">
  </a>
</h1>

<div align="center">
  <a href="https://hackage.haskell.org/package/dataframe">
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
* Works seamlessly in both command-line and notebook environments—great for exploration and scripting alike.

## Example usage

### Interactive environment
![Screencast of usage in GHCI](./static/example.gif)

Key features in example:
* Intuitive, SQL-like API to get from data to insights.
* Create typed, completion-ready references to columns in a dataframe using `:exposeColumns`
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

## Installing

### Jupyter notebook
* We have a [hosted version of the Jupyter notebook](https://ulwazi-exh9dbh2exbzgbc9.westus-01.azurewebsites.net/lab) on azure sites. This is hosted on Azure's free tier so it can only support 3 or 4 kernels at a time.
* To get started quickly, use the Dockerfile in the [ihaskell-dataframe](https://github.com/mchav/ihaskell-dataframe) to build and run an image with dataframe integration.
* For a preview check out the [California Housing](https://github.com/mchav/dataframe/blob/main/docs/California%20Housing.ipynb) notebook.

### CLI
* Run the installation script `curl '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/mchav/dataframe/refs/heads/main/scripts/install.sh | sh`
* Download the run script with: `curl --output dataframe "https://raw.githubusercontent.com/mchav/dataframe/refs/heads/main/scripts/dataframe.sh"`
* Make the script executable: `chmod +x dataframe`
* Add the script your path: `export PATH=$PATH:./dataframe`
* Run the script with: `dataframe`


## What is exploratory data analysis?
We provide a primer [here](https://github.com/mchav/dataframe/blob/main/docs/exploratory_data_analysis_primer.md) and show how to do some common analyses.

## Coming from other dataframe libraries
Familiar with another dataframe library? Get started:
* [Coming from Pandas](https://github.com/mchav/dataframe/blob/main/docs/coming_from_pandas.md)
* [Coming from Polars](https://github.com/mchav/dataframe/blob/main/docs/coming_from_polars.md)
* [Coming from dplyr](https://github.com/mchav/dataframe/blob/main/docs/coming_from_dplyr.md)

## Supported input formats
* CSV
* Apache Parquet

## Supported output formats
* CSV

## Future work
* Apache arrow compatability
* Integration with common data formats (currently only supports CSV)
* Support windowed plotting (currently only supports ASCII plots)
* Host the whole library + Jupyter lab on Azure with auth and isolation.
