#!/usr/bin/env bash
# feed urls themselves - lyra's crawler mines feeds for article links.
# pulls a curated set of high-quality rss/atom endpoints
set -euo pipefail
. "$(dirname "$0")/lib.sh"
cat <<'FEEDS' | push_urls
https://news.ycombinator.com/rss
https://lobste.rs/rss
https://www.reddit.com/.rss
https://hnrss.org/frontpage
https://feeds.bbci.co.uk/news/rss.xml
https://feeds.arstechnica.com/arstechnica/index
https://www.theguardian.com/world/rss
https://feeds.npr.org/1001/rss.xml
https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml
https://feeds.feedburner.com/TechCrunch/
https://www.wired.com/feed/rss
https://www.theverge.com/rss/index.xml
https://feeds.arstechnica.com/arstechnica/features
https://blog.rust-lang.org/feed.xml
https://lwn.net/headlines/rss
https://news.ycombinator.com/best
FEEDS
