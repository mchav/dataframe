defmodule Benchmark.MixProject do
  use Mix.Project

  def project do
    [
      app: :benchmark,
      version: "0.1.0",
      deps: deps(),
      lockfile: Path.expand("mix.lock", __DIR__),
      deps_path: Path.expand("deps", __DIR__),
      build_path: Path.expand("_build", __DIR__)
    ]
  end

  defp deps do
    [
      #{:exla, "~> 0.10.0"},
      {:explorer, "~> 0.11.1"},
      #{:nx, "~> 0.10.0"}
    ]
  end
end
