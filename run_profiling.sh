#!/bin/bash

cabal v2-run --enable-profiling dataframe -- +RTS -hy -l-agu
