#!/bin/bash

set -e

./scripts/format.sh
./scripts/lint.sh

cabal build
cabal sdist
