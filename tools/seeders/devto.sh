#!/usr/bin/env bash
# dev.to top articles - canonical urls only so we seed the source not the mirror
set -euo pipefail
. "$(dirname "$0")/lib.sh"
for period in day week month; do
  curl -fsS -m 20 -H "User-Agent: LyraSeed/1.0" "https://dev.to/api/articles?top=1&per_page=100" \
    | python3 -c 'import json,sys;[print(a.get("canonical_url") or a.get("url")) for a in json.load(sys.stdin) if a.get("canonical_url") or a.get("url")]'
done | push_urls
