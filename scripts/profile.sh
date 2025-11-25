#!/bin/bash

PROF_TYPE=""

if [ -z "$1" ]; then
  # Default to eventlog profile if no argument given
  PROF_TYPE="eventlog"
else
  case "$1" in
    "eventlog")
      PROF_TYPE="eventlog"
      ;;
    "pprof")
      PROF_TYPE="pprof"
      ;;
    *)
      echo "invalid profile type $1, should be one of 'eventlog' or 'pprof'"
      exit 1
      ;;
  esac
fi

case "$PROF_TYPE" in
  "eventlog")
    cabal v2-run -O2 --enable-profiling dataframe-benchmark-example -- +RTS -hc -l-agu
    ;;
  "pprof")
    cabal v2-run -O2 --enable-profiling dataframe-benchmark-example -- +RTS -pj -RTS
    ;;
esac
