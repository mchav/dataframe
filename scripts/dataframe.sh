#!/bin/bash

if [ ! -f ./dataframe.ghci ]; then
    curl --output dataframe.ghci "https://raw.githubusercontent.com/mchav/dataframe/refs/heads/main/dataframe.ghci"
fi

cabal update

cabal repl --repl-options="-O2 -fobject-code" --build-depends dataframe --repl-option=-ghci-script=dataframe.ghci

