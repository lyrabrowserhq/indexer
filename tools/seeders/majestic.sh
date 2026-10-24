#!/usr/bin/env bash
# majestic million - free top-domain csv, homepage seed per domain
set -euo pipefail
. "$(dirname "$0")/lib.sh"
N="${1:-2000}"
curl -fsSL --max-time 90 "https://downloads.majestic.com/majestic_million.csv" \
  | tail -n +2 | head -n "$N" | cut -d, -f3 | sed 's|^|https://|;s|$|/|' \
  | push_urls
