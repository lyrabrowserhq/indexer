#!/usr/bin/env bash
# lobste.rs hottest + newest - external story urls
set -euo pipefail
. "$(dirname "$0")/lib.sh"
for feed in hottest newest; do
  curl -fsS -m 20 "https://lobste.rs/${feed}.json" \
    | python3 -c 'import json,sys;[print(s["url"]) for s in json.load(sys.stdin) if s.get("url") and not s["url"].startswith("https://lobste.rs")]'
done | push_urls
