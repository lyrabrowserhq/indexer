#!/usr/bin/env bash
# pull known-good urls out of the common crawl index and seed them into lyra.
# queries per-domain so we never download the full crawl; each domain pulls
# html 200s collapsed by urlkey then posts to /v1/seed in batches.
#
# usage: cc_seed.sh [domains-file] [lyra-origin]
#        defaults: tools/cc_domains.txt and http://127.0.0.1:8091

set -euo pipefail
DOMAINS_FILE="${1:-$(dirname "$0")/cc_domains.txt}"
LYRA="${2:-http://127.0.0.1:8091}"
TOKEN="${LYRA_API_TOKEN:-}"

LATEST=$(curl -fsS "https://index.commoncrawl.org/collinfo.json" | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["cdx-api"])')

batch=()
push_batch() {
    [ ${#batch[@]} -eq 0 ] && return 0
    printf '%s\n' "${batch[@]}" | python3 -c '
import json,sys,urllib.request,os
urls=[l.strip() for l in sys.stdin if l.strip()]
tok=os.environ.get("TOKEN","")
req=urllib.request.Request(sys.argv[1]+"/v1/seed", data=json.dumps({"urls":urls}).encode(), headers={"Content-Type":"application/json","Authorization":"Bearer "+tok})
print(urllib.request.urlopen(req,timeout=30).read().decode())
' "$LYRA"
    batch=()
}

while IFS= read -r domain; do
    [ -z "$domain" ] && continue
    case "$domain" in \#*) continue;; esac
    echo "fetching $domain from $LATEST"
    while IFS= read -r line; do
        url=$(echo "$line" | python3 -c 'import json,sys;print(json.loads(sys.stdin.read()).get("url",""))' 2>/dev/null)
        [ -z "$url" ] && continue
        batch+=("$url")
        if [ ${#batch[@]} -ge 500 ]; then push_batch; fi
    done < <(curl -fsS --max-time 60 "${LATEST}?url=${domain}/*&output=json&filter=status:200&filter=mime:text/html&collapse=urlkey&fl=url&limit=2000" 2>/dev/null || true)
done < "$DOMAINS_FILE"
push_batch
echo "done"
