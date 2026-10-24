#!/usr/bin/env bash
# cisco umbrella top-1m - hosted on s3, plain csv of domains
set -euo pipefail
. "$(dirname "$0")/lib.sh"
N="${1:-2000}"
curl -fsSL --max-time 90 "https://s3-us-west-1.amazonaws.com/umbrella-static/top-1m.csv.zip" -o /tmp/umbrella.zip
python3 -c 'import zipfile; z=zipfile.ZipFile("/tmp/umbrella.zip"); print("\n".join("https://"+l.split(",")[1]+"/" for l in z.read(z.namelist()[0]).decode().splitlines()[:'"$N"']))' | push_urls
