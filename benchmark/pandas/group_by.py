import pandas as pd

df = pd.read_csv("./data/housing.csv")

# Group, aggregate, and rename in one shot
agg_df = (
    df
    .groupby("ocean_proximity")["median_house_value"]
    .agg(
        Minimum_median_house_value="min",
        Maximum_median_house_value="max"
    )
)

print(agg_df)
