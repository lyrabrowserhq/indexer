#!/usr/bin/env bash
# hacker news algolia api - front page + newest + best story external urls
set -euo pipefail
. "$(dirname "$0")/lib.sh"
for tag in front_page story show_hn ask_hn; do
  curl -fsS -m 20 "https://hn.algolia.com/api/v1/search?tags=${tag}&hitsPerPage=100" \
    | python3 -c 'import json,sys;[print(h["url"]) for h in json.load(sys.stdin)["hits"] if h.get("url")]'
done | push_urls
