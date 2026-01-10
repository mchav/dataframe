#!/usr/bin/env bash
set -euo pipefail

# Run HLint on the project
# Usage: ./scripts/lint.sh [--fix]

FIX_MODE=false

if [[ "${1:-}" == "--fix" ]]; then
    FIX_MODE=true
fi

DIRS=("src" "app" "tests" "benchmark" "examples" "dataframe-hasktorch" "dataframe-persistent" "dataframe-symbolic-regression")

echo "Running HLint on dataframe project..."

if [ "$FIX_MODE" = true ]; then
    echo "Running in fix mode (applying suggestions where possible)..."
    hlint "${DIRS[@]}" --refactor --refactor-options="-i"
else
    hlint "${DIRS[@]}"
fi

if [ $? -eq 0 ]; then
    echo "✓ HLint passed! No issues found."
else
    echo "✗ HLint found issues. Run './scripts/lint.sh --fix' to auto-fix some issues."
    exit 1
fi
