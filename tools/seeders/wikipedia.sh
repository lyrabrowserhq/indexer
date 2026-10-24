#!/usr/bin/env bash
# wikipedia vital + featured articles - curated high-quality pages.
# walks level-1..5 vital categories and featured articles via categorymembers
set -euo pipefail
. "$(dirname "$0")/lib.sh"
python3 - <<'PY' | push_urls
import json,urllib.request,urllib.parse,os
LYRA=os.environ.get("LYRA_ORIGIN","http://127.0.0.1:8091")
cats=["Wikipedia_level-%d_vital_articles"%i for i in range(1,6)]
cats+=["Featured_articles","Good_articles"]
urls=[]
for cat in cats:
    cont=""; n=0
    while n<3000:
        q={"action":"query","list":"categorymembers","cmtitle":"Category:"+cat,"cmlimit":"500","cmtype":"page","format":"json"}
        if cont: q["cmcontinue"]=cont
        u="https://en.wikipedia.org/w/api.php?"+urllib.parse.urlencode(q)
        req=urllib.request.Request(u,headers={"User-Agent":"LyraSeed/1.0 (contact@example.com)"})
        try: d=json.load(urllib.request.urlopen(req,timeout=30))
        except Exception: break
        for m in d["query"]["categorymembers"]:
            urls.append("https://en.wikipedia.org/wiki/"+urllib.parse.quote(m["title"].replace(" ","_")))
            n+=1
        cont=d.get("continue",{}).get("cmcontinue")
        if not cont: break
print("\n".join(urls))
PY
