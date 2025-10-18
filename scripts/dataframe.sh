#!/bin/bash

if [ ! -f ./dataframe.ghci ]; then
    curl --output dataframe.ghci "https://raw.githubusercontent.com/mchav/dataframe/refs/heads/main/dataframe.ghci"
fi

cabal update

cabal repl --ghc-options="-O2 -fobject-code" --repl-option=-ghci-script=dataframe.ghci

