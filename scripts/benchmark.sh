#!/bin/bash

export MIX_EXS="$PWD/benchmark/explorer/mix.exs"

VENV_DIR="./benchmark/dataframe_benchmark"
EXPLORER_DIR="./benchmark/explorer"

if ! command -v python3 &> /dev/null; then
    echo -e "\nPython3 is not installed.\n"
    exit 1
fi

if ! command -v elixir &> /dev/null; then
    echo -e "\nElixir is not installed.\n"
    exit 1
fi

echo -e "\nSetting up Python environment...\n"

python3 -m venv "$VENV_DIR"

source "$VENV_DIR/bin/activate"

pip install --upgrade pip
pip install numpy pandas polars

echo -e "\nPython environment ready.\n"

echo -e "Setting up Elixir environment...\n"

mix deps.get
mix deps.loadpaths

echo -e "\nElixir environment ready.\n"

echo -e "Running benchmark...\n"

cabal bench -O2

echo -e "\nCleaning up...\n"

rm -rf "$VENV_DIR"
find "$EXPLORER_DIR" -mindepth 1 | grep -v '\.exs$' | xargs -I{} rm -rf "{}"

echo "Done!"