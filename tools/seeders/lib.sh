# shared seeder helpers - push_urls reads urls from stdin, batches to /v1/seed
LYRA="${LYRA_ORIGIN:-http://127.0.0.1:8091}"
TOKEN="${LYRA_API_TOKEN:-}"

push_urls() {
    local f; f=$(mktemp)
    cat > "$f"
    python3 - "$LYRA" "$f" <<'PY'
import json,sys,urllib.request,os
lyra,f=sys.argv[1],sys.argv[2]
tok=os.environ.get("LYRA_API_TOKEN","")
seen=set(); urls=[]
def flush():
    if not urls: return
    req=urllib.request.Request(lyra+"/v1/seed",data=json.dumps({"urls":urls}).encode(),headers={"Content-Type":"application/json","Authorization":"Bearer "+tok})
    print(urllib.request.urlopen(req,timeout=30).read().decode()); urls.clear()
for line in open(f,errors="replace"):
    u=line.strip()
    if u and u.startswith("http") and u not in seen:
        seen.add(u); urls.append(u)
    if len(urls)>=400:
        flush()
flush()
PY
    rm -f "$f"
}
