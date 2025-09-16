defmodule Benchmark.MixProject do
  use Mix.Project

  def project do
    [
      app: :benchmark,
      version: "0.1.0",
      deps: deps()
    ]
  end

  defp deps do
    [
      {:exla, "~> 0.10.0"},
      {:explorer, "~> 0.11.1"},
      {:nx, "~> 0.10.0"}
    ]
  end
end
