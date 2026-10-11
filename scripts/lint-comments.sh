#!/usr/bin/env bash
set -euo pipefail

# Fail if a `--` comment runs longer than MAX_LINES, any comment is wider than
# WIDTH, or a {- -} block comment (Haddock included) is used.
# Usage: ./scripts/lint-comments.sh [--max-lines N] [--width W] [paths...]
# Haddock counts like any comment. Only pragmas and `-- *` headings are
# exempt. Section banners (`-- -----` rulers, `-- Title -----`) are errors.

MAX_LINES=1
WIDTH=80
PATHS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --max-lines) MAX_LINES="$2"; shift 2 ;;
    --width) WIDTH="$2"; shift 2 ;;
    *) PATHS+=("$1"); shift ;;
  esac
done

cd "$(dirname "$0")/.."

if [[ ${#PATHS[@]} -eq 0 ]]; then
  PATHS=(src app tests benchmark examples dataframe-*)
fi

mapfile -t files < <(find "${PATHS[@]}" -name dist-newstyle -prune -o -name '*.hs' -print | sort)

check() {
  awk -v max="$MAX_LINES" -v width="$WIDTH" '
    function report(kind, start, n) {
      printf "%s:%d: %s comment is %d lines (max %d)\n", FILENAME, start, kind, n, max
      bad = 1
    }
    function flush() {
      if (lines > 0 && !(haddock == 2) && lines > max) report("line", start, lines)
      lines = 0; haddock = 0
    }
    FNR == 1 { flush(); depth = 0 }
    {
      code = $0; gsub(/"([^"\\]|\\.)*"/, "\"\"", code)
      chars = $0; gsub(/[\200-\277]/, "", chars)
      if (length(chars) > width && code ~ /--|\{-/)
        { printf "%s:%d: line is %d chars (max %d)\n", FILENAME, FNR, length(chars), width; bad = 1 }
    }
    # Section banners: rulers like `-- -----` or titles like `-- Name -----`.
    /^[ \t]*--[ \t]*[-=][-=][-=][-=]+[ \t]*$/ || /^[ \t]*--[ \t]+[^-= \t].*[ \t][-=][-=][-=][-=]+[ \t]*$/ {
      printf "%s:%d: section banner comment\n", FILENAME, FNR; bad = 1
    }
    # Inside a {- -} block: track nesting, report when it closes.
    depth > 0 {
      n = gsub(/\{-/, "&"); m = gsub(/-\}/, "&"); depth += n - m
      if (depth <= 0) {
        depth = 0
        if (!pragma) {
          printf "%s:%d: {- -} block comment (use a one-line -- instead)\n", FILENAME, bstart
          bad = 1
        }
      }
      next
    }
    # Block comment opens on this line (ignore ones that also close on it).
    /\{-/ {
      flush()
      line = $0
      n = gsub(/\{-/, "&", line); m = gsub(/-\}/, "&", line)
      if (n > m) {
        depth = n - m; bstart = FNR
        bhaddock = ($0 ~ /\{-[ \t]*[|^$*]/); pragma = ($0 ~ /\{-#/)
      } else if ($0 !~ /\{-#/) {
        printf "%s:%d: {- -} block comment (use a one-line -- instead)\n", FILENAME, FNR
        bad = 1
      }
      next
    }
    # `--` line comments: consecutive comment-only lines form one comment.
    /^[ \t]*--/ {
      if (lines == 0) { start = FNR; haddock = ($0 ~ /^[ \t]*--[ \t]*\*/) ? 2 : 0 }
      if ($0 !~ /^[ \t]*--[-= \t]*$/) lines++
      next
    }
    { flush() }
    END { flush(); exit bad }
  ' "$@"
}

if check "${files[@]}"; then
  echo "✓ Comments OK (max ${MAX_LINES} line(s), ${WIDTH} cols) in ${#files[@]} files."
else
  echo "✗ Comment lint failed." >&2
  exit 1
fi
