#!/usr/bin/env bash
# arxiv recent submissions per major category - feeds the papers lens
set -euo pipefail
. "$(dirname "$0")/lib.sh"
for cat in cs.AI cs.LG cs.CL cs.CR cs.SE physics.cond-mat math.CO q-bio stat.ML cs.DC eess.AS; do
  curl -fsS -m 20 "https://export.arxiv.org/api/query?search_query=cat:${cat}&sortBy=submittedDate&sortOrder=descending&max_results=50" \
    | grep -oE 'https?://arxiv.org/abs/[0-9]+\.[0-9]+' || true
  sleep 3
done | push_urls
