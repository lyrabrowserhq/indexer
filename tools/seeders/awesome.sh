#!/usr/bin/env bash
# awesome-* readme link extraction - curated link lists per topic
set -euo pipefail
. "$(dirname "$0")/lib.sh"
REPOS="sindresorhus/awesome vinta/awesome-python awesome-selfhosted/awesome-selfhosted donnemartin/system-design-primer eugeneyan/applied-ml papers-we-love/papers-we-love"
for r in $REPOS; do
  for b in main master; do
    body=$(curl -fsSL -m 20 "https://raw.githubusercontent.com/${r}/${b}/readme.md" 2>/dev/null \
      || curl -fsSL -m 20 "https://raw.githubusercontent.com/${r}/${b}/README.md" 2>/dev/null || true)
    [ -n "$body" ] && break
  done
  echo "$body" | grep -oE '\(https?://[^)]+\)' | tr -d '()' | grep -vE "github\.com/[^/]+/[^/]+/(issues|pull|blob)|shields\.io|github\.com/sponsors|travis|badges" || true
done | push_urls
