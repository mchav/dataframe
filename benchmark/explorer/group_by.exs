alias Explorer.DataFrame

require DataFrame

df = DataFrame.from_csv!("./data/housing.csv")

agg_df =
  df
  |> DataFrame.group_by("ocean_proximity")
  |> DataFrame.summarise(
    min_median_house_value: min(median_house_value),
    max_median_house_value: max(median_house_value)
  )

IO.inspect(agg_df)
