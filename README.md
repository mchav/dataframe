# DataFrame

An intuitive, dynamically-typed DataFrame library.

A tool for exploratory data analysis.

## What is exploratory data analysis?
We provide a primer [here](./docs/exploratory_data_analysis_primer.md) and show how to do some common analyses.

## Coming from other dataframe libraries
Familiar with another dataframe library? Get started:
* [Coming from Pandas](./docs/coming_from_pandas.md)
* [Coming from Polars](./docs/coming_from_polars.md)
* [Coming from dplyr](./docs/coming_from_dplyr.md)

## Example usage

### Code example
```haskell
import qualified Data.DataFrame as D

import Data.DataFrame ((|>))

main :: IO ()
    df <- D.readTsv "./data/chipotle.tsv"
    print $ df
      |> D.select ["item_name", "quantity"]
      |> D.groupBy ["item_name"]
      |> D.aggregate (zip (repeat "quantity") [D.Maximum, D.Mean, D.Sum])
      |> D.sortBy D.Descending ["Sum_quantity"]
```

Output:

```
----------------------------------------------------------------------------------------------------
index |               item_name               | Sum_quantity |   Mean_quantity    | Maximum_quantity
------|---------------------------------------|--------------|--------------------|-----------------
 Int  |                 Text                  |     Int      |       Double       |       Int       
------|---------------------------------------|--------------|--------------------|-----------------
0     | Chips and Fresh Tomato Salsa          | 130          | 1.1818181818181819 | 15              
1     | Izze                                  | 22           | 1.1                | 3               
2     | Nantucket Nectar                      | 31           | 1.1481481481481481 | 3               
3     | Chips and Tomatillo-Green Chili Salsa | 35           | 1.1290322580645162 | 3               
4     | Chicken Bowl                          | 761          | 1.0482093663911847 | 3               
5     | Side of Chips                         | 110          | 1.0891089108910892 | 8               
6     | Steak Burrito                         | 386          | 1.048913043478261  | 3               
7     | Steak Soft Tacos                      | 56           | 1.018181818181818  | 2               
8     | Chips and Guacamole                   | 506          | 1.0563674321503131 | 4               
9     | Chicken Crispy Tacos                  | 50           | 1.0638297872340425 | 2
```

Full example in `./app` folder using many of the constructs in the API.

### Visual example
![Screencast of usage in GHCI](./static/example.gif)

## Future work
* Apache arrow and Parquet compatability
* Integration with common data formats (currently only supports CSV)
* Support windowed plotting (currently only supports ASCII plots)
* Create a lazy API that builds an execution graph instead of running eagerly (will be used to compute on files larger than RAM)

## Contributing
* Please first submit an issue and we can discuss there.
