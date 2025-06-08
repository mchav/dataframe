#!/usr/bin/env bash
set -euo pipefail

open_browser() {
  if command -v open >/dev/null; then
    open "$1"
  elif command -v xdg-open >/dev/null; then
    xdg-open "$1"
  elif command -v start >/dev/null; then
    start "$1"
  else
    echo "Open the report manually: $1"
  fi
}

cabal clean
cabal test all --enable-coverage

HPCDIRS=$(find . -type f -name '*.mix' -exec dirname {} \; | xargs dirname | sort -u | sed 's/^/--hpcdir=/')
TIX=$(find . -name '*.tix' | head -n1)

hpc markup \
  $HPCDIRS \
    --exclude=dataframe:Dataframe \
  $TIX \
  --destdir=coverage-html \
  --reset

open_browser ./coverage-html/hpc_index.html
