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

![Screencast of usage in GHCI](./static/example.gif)

Longer form examples in `./app` folder using many of the constructs in the API.

## Future work
* Apache arrow and Parquet compatability
* Integration with common data formats (currently only supports CSV)
* Support windowed plotting (currently only supports ASCII plots)
* Create a lazy API that builds an execution graph instead of running eagerly (will be used to compute on files larger than RAM)
