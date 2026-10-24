#!/usr/bin/env bash
# seed the tranco top-list into lyra - reliable bulk source while the common
# crawl index api is flaky. pulls the top N domains and seeds each homepage.
#
# usage: tranco_seed.sh [count] [lyra-origin]
#        defaults: 2000 and http://127.0.0.1:8091

set -euo pipefail
N="${1:-2000}"
LYRA="${2:-http://127.0.0.1:8091}"
TOKEN="${LYRA_API_TOKEN:-}"

LIST=$(curl -fsSL --max-time 60 "https://tranco-list.eu/top-1m.csv.zip" -o /tmp/tranco.zip 2>/dev/null && python3 -c 'import zipfile; z=zipfile.ZipFile("/tmp/tranco.zip"); data=z.read(z.namelist()[0]).decode().splitlines(); print("\n".join(data[:'"$N"']))')

LIST="$LIST" LYRA_API_TOKEN="${TOKEN:-}" python3 - "$LYRA" <<'PY'
import json,sys,urllib.request,os
lyra=sys.argv[1]
tok=os.environ.get("LYRA_API_TOKEN","")
urls=[]
for line in os.environ.get("LIST","").splitlines():
    line=line.strip()
    if not line or "," not in line:
        continue
    domain=line.split(",",1)[1].strip()
    if domain:
        urls.append("https://"+domain+"/")
    if len(urls)>=500:
        req=urllib.request.Request(lyra+"/v1/seed",data=json.dumps({"urls":urls}).encode(),headers={"Content-Type":"application/json","Authorization":"Bearer "+tok})
        print(urllib.request.urlopen(req,timeout=30).read().decode())
        urls=[]
if urls:
    req=urllib.request.Request(lyra+"/v1/seed",data=json.dumps({"urls":urls}).encode(),headers={"Content-Type":"application/json","Authorization":"Bearer "+tok})
    print(urllib.request.urlopen(req,timeout=30).read().decode())
PY
