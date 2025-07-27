#!/bin/bash

curl --output dataframe.ghci "https://raw.githubusercontent.com/mchav/dataframe/refs/heads/main/dataframe.ghci"

cabal repl -O2 --build-depends dataframe --repl-options="-ghci-script ./dataframe.ghci"
