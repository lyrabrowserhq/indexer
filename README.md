# Lyra index

Crawler and Tantivy index for Seek. 

## Run

Serve the search API from an existing index:

```
INDEX_DIR=./data BIND=127.0.0.1:8091 cargo run --release -- serve
```

Crawl and serve in one process:

```
INDEX_DIR=./data BIND=127.0.0.1:8091 cargo run --release -- run --seeds-file seeds.txt --max-docs 50000
```

Crawl only:

```
INDEX_DIR=./data cargo run --release -- crawl --seeds-file seeds.txt --max-docs 50000 --max-bytes 10737418240
```

Optional config file (default `crawler.toml`):

```
lyra-index -c crawler.toml serve
```

`crawler.example.toml` lists every field.

## Size limits

`--max-docs` and `--max-bytes` stop the crawl when the index hits those
caps. `0` (the default) means no cap. Env aliases:
`LYRA_INDEX_MAX_DOCS`, `LYRA_INDEX_MAX_BYTES`, `LYRA_INDEX_TEXT_BYTES`,
`LYRA_INDEX_MIN_TEXT_CHARS`.

`LYRA_INDEX_TEXT_BYTES` (default 16384) is the stored body per page.
`LYRA_INDEX_MIN_TEXT_CHARS` (default 200) skips thin pages.

## Compact and prune

Merge Tantivy segments and compact the redb file:

```
INDEX_DIR=./data cargo run --release -- compact
```

Drop oldest documents until a keep, size, or age cap holds, then compact.
`--max-bytes` estimates how many pages to drop from current size. It keeps
at least one document when the cap is below index overhead.

```
INDEX_DIR=./data cargo run --release -- prune --keep-docs 40000
INDEX_DIR=./data cargo run --release -- prune --max-bytes 8589934592
INDEX_DIR=./data cargo run --release -- prune --max-age-days 90
```

`stats` prints document counts and on-disk bytes.

## API

- `GET /health` returns `OK`
- `GET /healthz` JSON status and session doc count
- `GET /stats` stored docs, index docs, and data bytes
- `GET /search?q=&limit=&offset=&kind=` crawler JSON
- `GET /v1/search?q=&limit=&kind=` JSON hits for Seek
- `GET /v1/search.rss?q=` RSS 2.0 for the Seek `lyra` engine
- `POST /v1/index` JSON `{url,title,body,kind}` kind is `page` or `rss`
- `GET /v1/feeds` listed RSS origins
- `POST /v1/feeds` JSON `{url}` fetch and index
- `POST /v1/feeds/refresh` re-fetch every listed feed

Private, loopback, and link-local URLs are rejected. Challenge walls,
thin pages, cart/login URLs, binary files, and Mediawiki File, Talk, and
Special pages are not indexed. Search requires every query term and
returns a body snippet for Seek.

One request is in flight per host. The next wait is the larger of
`min_delay_ms`, robots `Crawl-delay`, and last-fetch-time times
`delay_factor`, capped by `max_delay_ms`, plus jitter. HTTP 429 honors
`Retry-After`. Unchanged pages are skipped via ETag / Last-Modified.

## Env

`BIND` default `0.0.0.0:8091`. `INDEX_DIR` default `data` (redb state plus
the Tantivy dir at `$INDEX_DIR/index`).

`LYRA_INDEX_LISTEN`, `LYRA_INDEX_DIR`, `LYRA_INDEX_WORKERS`,
`LYRA_INDEX_PROXY`, `LYRA_INDEX_FLARESOLVERR`, `LYRA_INDEX_HISTER_URL`,
`LYRA_INDEX_HISTER_TOKEN`, `LYRA_INDEX_USER_AGENT`,
`LYRA_INDEX_MAX_PAGES_PER_SITE`, `LYRA_INDEX_COMMIT_EVERY` override the matching config keys.

License is AGPL-3.0.
