#!/bin/bash

sudo apt install libsnappy-dev curl

curl --proto '=https' --tlsv1.2 -sSf https://get-ghcup.haskell.org | sh

cabal update
