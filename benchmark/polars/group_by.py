import datetime
import numpy as np
import polars as pl

# ------------------------------------------------------------------------------

df = pl.read_csv("./data/housing.csv")

result = (
    df
    .group_by("ocean_proximity")
    .agg([
        pl.col("median_house_value").min().alias("Minimum_median_house_value"),
        pl.col("median_house_value").max().alias("Maximum_median_house_value")
    ])
)

print(result)
