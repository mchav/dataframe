# DataFrame

An intuitive, dynamically-typed DataFrame library.

A tool for exploratory data analysis.

## What is exploratory data analysis?
We provide a primer [here](./docs/exploratory_data_analysis_primer.md) and show how to do some common analyses.

## Coming from other dataframe libraries
Familiar with another dataframe library? Get started:
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
      |> D.filter "item_name" (searchTerm ==)
      |> D.groupBy ["item_name"]
      |> D.aggregate (zip (repeat "quantity") [D.Maximum, D.Mean, D.Sum])
      |> D.sortBy D.Descending "Sum_quantity"
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
