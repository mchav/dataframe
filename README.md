# DataFrame

An intuitive, dynamically-typed DataFrame library.

Goals:
* Exploratory data analysis
* Shallow learning curve

Non-goals:
* Static types/strong type-safety

Example usage

![Screencast of usage in GHCI](./static/example.gif)

Longer form examples in `./app` folder using many of the constructs in the API.

Future work:
* Apache arrow and Parquet compatability
* Integration with common data formats (currently only supports CSV)
* Support windowed plotting (currently only supports ASCII plots)
* Create a lazy API that builds an execution graph instead of running eagerly (will be used to compute on files larger than RAM)
