import datetime
import numpy as np
import polars as pl

# ------------------------------------------------------------------------------

SIZE: int = 100_000_000

first = datetime.datetime.now()
df = pl.DataFrame({"normal": np.random.normal(size=SIZE),
                   "log_normal": np.random.lognormal(size=SIZE),
                   "exponential": np.random.exponential(size=SIZE),
                   })
print(df)
second = datetime.datetime.now()
print(f"Data generation/load time: "
      f"{(second - first).seconds}.{(second - first).microseconds} secs")

df2 = df.select(
   mean = pl.col("normal").mean(),
   var = pl.col("log_normal").var(),
   corr = pl.corr("exponential", "log_normal")
)

mean: float = df2["mean"]
var: float = df2["var"]
corr: float = df2["corr"]

print(f"{mean[0]}, {var[0]}, {corr[0]}")
third = datetime.datetime.now()

df3 = df.filter(pl.col("log_normal") > 8)
print(df3.limit(10))
print(f"Number of rows after select: {df3.select(pl.len()).item()}")
fourth = datetime.datetime.now()

fifth = datetime.datetime.now()

print(f"Calculation time: {(third - second).seconds}.{(third - second).microseconds} secs")
print(f"Selection time: {(fourth - third).seconds}.{(fourth - third).microseconds} secs")
print(f"Overall time: {(fifth - first).seconds}.{(fifth - first).microseconds} secs")

