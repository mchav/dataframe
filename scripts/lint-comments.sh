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
  # C locale: count bytes so the UTF-8 continuation-byte regex below is valid
  # (gawk in a UTF-8 locale rejects it as an invalid collation character).
  LC_ALL=C awk -v max="$MAX_LINES" -v width="$WIDTH" -v gha="${GITHUB_ACTIONS:-}" '
    # One finding: location, what is wrong, the source line, how to fix it.
    function report(line, last, title, msg, src, fix,   loc) {
      loc = (last > line) ? line "-" last : line
      printf "%s:%s: %s\n", FILENAME, loc, msg
      if (src != "") printf "    | %s\n", src
      printf "    fix: %s\n\n", fix
      if (gha == "true")
        printf "::error file=%s,line=%d,endLine=%d,title=%s::%s. Fix: %s\n", \
          FILENAME, line, (last > line ? last : line), title, msg, fix
      problems++
      if (!(FILENAME in seen)) { seen[FILENAME] = 1; nfiles++ }
    }
    function flush() {
      if (lines > 0 && !(haddock == 2) && lines > max)
        report(start, start + span - 1, "Multi-line comment", \
          sprintf("%d-line -- comment; comments must fit on %d line(s)", lines, max), \
          first, "condense it to one line, or delete it if the code says it already")
      lines = 0; haddock = 0
    }
    FNR == 1 { flush(); depth = 0 }
    {
      code = $0; gsub(/"([^"\\]|\\.)*"/, "\"\"", code)
      chars = $0; gsub(/[\200-\277]/, "", chars)
      if (length(chars) > width && code ~ /--|\{-/)
        report(FNR, FNR, "Comment too wide", \
          sprintf("comment line is %d columns; the limit is %d", length(chars), width), \
          $0, sprintf("shorten the text so the line ends by column %d", width))
    }
    # Section banners: rulers like `-- -----` or titles like `-- Name -----`.
    /^[ \t]*--[ \t]*[-=][-=][-=][-=]+[ \t]*$/ || /^[ \t]*--[ \t]+[^-= \t].*[ \t][-=][-=][-=][-=]+[ \t]*$/ {
      report(FNR, FNR, "Section banner", "section banner comments are not allowed", \
        $0, "delete it, or use a Haddock heading: -- * Title")
    }
    # Inside a {- -} block: track nesting, report when it closes.
    depth > 0 {
      n = gsub(/\{-/, "&"); m = gsub(/-\}/, "&"); depth += n - m
      if (depth <= 0) {
        depth = 0
        if (!pragma)
          report(bstart, FNR, "Block comment", \
            sprintf("%d-line {- -} block comment; only one-line -- comments are allowed", FNR - bstart + 1), \
            bfirst, (bhaddock ? "replace it with a single -- | line" : "replace it with a single -- line"))
      }
      next
    }
    # Block comment opens on this line (ignore ones that also close on it).
    /\{-/ {
      flush()
      line = $0
      n = gsub(/\{-/, "&", line); m = gsub(/-\}/, "&", line)
      if (n > m) {
        depth = n - m; bstart = FNR; bfirst = $0
        bhaddock = ($0 ~ /\{-[ \t]*[|^$*]/); pragma = ($0 ~ /\{-#/)
      } else if ($0 !~ /\{-#/) {
        report(FNR, FNR, "Block comment", \
          "{- -} block comment; only one-line -- comments are allowed", \
          $0, "rewrite it as a -- comment (on its own line, or at the end of the line)")
      }
      next
    }
    # `--` line comments: consecutive comment-only lines form one comment.
    /^[ \t]*--/ {
      if (lines == 0) {
        start = FNR; span = 0; first = $0
        haddock = ($0 ~ /^[ \t]*--[ \t]*\*/) ? 2 : 0
      }
      span = FNR - start + 1
      if ($0 !~ /^[ \t]*--[-= \t]*$/) lines++
      next
    }
    { flush() }
    END {
      flush()
      if (problems) printf "%d problem(s) in %d file(s).\n", problems, nfiles
      exit (problems > 0)
    }
  ' "$@"
}

rc=0
check "${files[@]}" || rc=$?
if [[ $rc -eq 0 ]]; then
  echo "✓ Comments OK (max ${MAX_LINES} line(s), ${WIDTH} cols) in ${#files[@]} files."
elif [[ $rc -eq 1 ]]; then
  {
    echo "✗ Comment lint failed."
    echo "  Rules: comments are one line, at most ${WIDTH} columns, and use --"
    echo "  (not {- -}). Pragmas and '-- * Heading' lines are exempt."
    echo "  Re-run locally: ./scripts/lint-comments.sh [paths...]"
  } >&2
  exit 1
else
  echo "✗ lint-comments.sh crashed (awk exit $rc); this is a script bug, not a" >&2
  echo "  comment problem. See the awk error above." >&2
  exit "$rc"
fi
