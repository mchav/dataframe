#!/bin/bash

if ! command -v fourmolu &> /dev/null; then
    echo -e "\nFourmolu not found. Installing...\n"
    cabal v2-install fourmolu-0.17.0.0 --overwrite-policy=always --force-reinstalls
    echo -e "\nFourmolu installed successfully!"
fi

fourmolu --mode inplace $(git ls-files '*.hs')
