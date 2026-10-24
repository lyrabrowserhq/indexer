#!/usr/bin/env bash
# stack exchange top questions - the question pages themselves are the seed
set -euo pipefail
. "$(dirname "$0")/lib.sh"
for site in stackoverflow serverfault superuser askubuntu unix; do
  curl -fsS -m 20 "https://api.stackexchange.com/2.3/questions?pagesize=100&order=desc&sort=votes&site=${site}&filter=withbody" \
    | python3 -c 'import json,sys
try:d=json.load(sys.stdin)
except Exception:d={}
for i in d.get("items",[]):print(i["link"])'
done | push_urls
