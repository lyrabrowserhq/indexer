#!/usr/bin/env bash
# run every seeder in order; each takes an optional count arg
set -uo pipefail
DIR="$(dirname "$0")"
N="${1:-2000}"
for s in majestic umbrella hn lobsters wikipedia awesome se feeds arxiv devto; do
  echo "=== $s ==="
  bash "$DIR/$s.sh" "$N" 2>&1 | tail -3
done
