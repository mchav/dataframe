alias Explorer.DataFrame
alias Explorer.Series

require DataFrame

Nx.global_default_backend(EXLA.Backend)

format_time = fn start_time, end_time ->
  diff = end_time - start_time
  seconds = div(diff, 1_000_000)
  microseconds = rem(diff, 1_000_000)
  "#{seconds}.#{microseconds}"
end

size = 100_000_000

first = System.monotonic_time(:microsecond)

key = Nx.Random.key(System.system_time(:nanosecond) |> rem(:math.pow(2, 32) |> round))

df =
  Explorer.DataFrame.new(%{
    "normal" => Nx.Random.normal(key, 0, 1, shape: {size, 1}) |> elem(0),
    "log_normal" => Nx.Random.normal(key, 0, 1, shape: {size, 1}) |> elem(0) |> Nx.exp(),
    "exponential" =>
      Nx.subtract(1.0, Nx.Random.uniform(key, 0, 1, shape: {size, 1}) |> elem(0))
      |> Nx.log()
      |> Nx.negate()
  })

second = System.monotonic_time(:microsecond)

IO.puts("Data generation/load time: #{format_time.(first, second)}")

mean = Series.mean(df["normal"])
var = Series.variance(df["log_normal"])
corr = Series.correlation(df["exponential"], df["log_normal"])

IO.puts("#{mean}, #{var}, #{corr}")

third = System.monotonic_time(:microsecond)

df2 = DataFrame.filter(df, log_normal > 8.0)

IO.puts("Number of rows after select: #{DataFrame.n_rows(df2)}")

fourth = System.monotonic_time(:microsecond)

IO.inspect(DataFrame.head(df, 10))

IO.puts("Calculation time: #{format_time.(second, third)}")
IO.puts("Selection time: #{format_time.(third, fourth)}")
IO.puts("Overall time: #{format_time.(first, fourth)}")
