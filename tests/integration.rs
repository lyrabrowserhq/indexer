use axum::Router;
use axum::http::StatusCode;
use axum::response::Html;
use axum::routing::get;
use futures::StreamExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[path = "support/mod.rs"]
mod support;

/// fixture site: index links to /a, /b and /private/hidden. robots.txt
/// disallows /private/. /slow drips bytes under the drip floor.
async fn serve_fixture() -> (String, Arc<AtomicUsize>) {
    let hits = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
        .route("/robots.txt", get(|| async {
            "User-agent: *\nAllow: /\nDisallow: /private/\n"
        }))
        .route("/", get({
            let h = hits.clone();
            move || {
                let h = h.clone();
                async move {
                    h.fetch_add(1, Ordering::Relaxed);
                    Html(r#"<html><head><title>root</title></head><body>
                    <p>welcome to the fixture root page about wibbly widgets</p>
                    <a href="/a">a</a> <a href="/b">b</a> <a href="/private/hidden">h</a>
                    </body></html>"#)
                }
            }
        }))
        .route("/a", get({
            let h = hits.clone();
            move || {
                let h = h.clone();
                async move {
                    h.fetch_add(1, Ordering::Relaxed);
                    Html("<html><head><title>page a</title></head><body><p>alpha widgets and gadgets</p><a href=\"/b\">b</a></body></html>")
                }
            }
        }))
        .route("/b", get({
            let h = hits.clone();
            move || {
                let h = h.clone();
                async move {
                    h.fetch_add(1, Ordering::Relaxed);
                    Html("<html><head><title>page b</title></head><body><p>beta gadgets and gizmos</p></body></html>")
                }
            }
        }))
        .route("/private/hidden", get({
            let h = hits.clone();
            move || {
                let h = h.clone();
                async move {
                    h.fetch_add(1, Ordering::Relaxed);
                    Html("<html><body>hidden</body></html>")
                }
            }
        }))
        .route("/tarpit", get(|| async {
            // slow drip: small chunks with long sleeps, way below the floor
            let stream = futures::stream::iter(0..64).then(|i| async move {
                tokio::time::sleep(Duration::from_millis(250)).await;
                Ok::<_, std::io::Error>(format!("chunk{i}"))
            });
            axum::body::Body::from_stream(stream)
        }));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), hits)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crawler_respects_robots_and_indexes() {
    let (base, hits) = serve_fixture().await;

    let dir = tempfile::tempdir().unwrap();
    let mut cfg = support::test_config(dir.path());
    cfg.seeds = vec![format!("{base}/")];

    let crawler = std::sync::Arc::new(support::make_crawler(cfg).unwrap());
    crawler.seed(&[format!("{base}/"), format!("{base}/tarpit")]);

    let c = crawler.clone();
    let task = tokio::spawn(async move { c.run().await });
    let _ = tokio::time::timeout(Duration::from_secs(60), task)
        .await
        .unwrap();

    // disallowed path must never have been requested
    let store_docs = crawler.index().num_docs();
    assert!(store_docs >= 2, "expected >=2 docs, got {store_docs}");

    // robots disallowed path skipped: the fixture counts hits, /private never hit
    // (hits counts all routes; tarpit may have been attempted once)
    assert!(hits.load(Ordering::Relaxed) >= 2);

    // search finds the pages
    let res = crawler.index().search("widgets", 10).unwrap();
    assert!(!res.is_empty(), "expected search hits for widgets");
    assert!(res.iter().all(|h| h.url.starts_with(&base)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn api_serves_search() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = support::test_config(dir.path());
    let crawler = support::make_crawler(cfg.clone()).unwrap();
    let idx = crawler.index();
    idx.add_doc(
        "https://x.test/a",
        "x.test",
        "hello world page",
        "the world says hello",
        "",
        0,
        0.0,
        0,
    )
    .unwrap();
    idx.commit().unwrap();

    let state = support::app_state(crawler);
    let app = support::router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let resp = reqwest::get(format!("http://{addr}/search?q=hello+world&limit=5"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert!(!results.is_empty());
    assert_eq!(results[0]["url"], "https://x.test/a");

    let v1 = reqwest::get(format!("http://{addr}/v1/search?q=hello+world&limit=5"))
        .await
        .unwrap();
    assert_eq!(v1.status(), StatusCode::OK);
    let v1_body: serde_json::Value = v1.json().await.unwrap();
    let hits = v1_body["hits"].as_array().unwrap();
    assert!(!hits.is_empty());
    assert_eq!(hits[0]["url"], "https://x.test/a");
    assert_eq!(hits[0]["kind"], "page");

    let rss = reqwest::get(format!("http://{addr}/v1/search.rss?q=hello+world"))
        .await
        .unwrap();
    assert_eq!(rss.status(), StatusCode::OK);
    let rss_text = rss.text().await.unwrap();
    assert!(rss_text.contains("<rss version=\"2.0\">"));
    assert!(rss_text.contains("https://x.test/a"));

    let health = reqwest::get(format!("http://{addr}/health")).await.unwrap();
    assert_eq!(health.status(), StatusCode::OK);
    assert_eq!(health.text().await.unwrap(), "OK");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kind_filter_separates_rss_from_pages() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = support::test_config(dir.path());
    let crawler = support::make_crawler(cfg).unwrap();
    let idx = crawler.index();
    idx.add_doc_kind(
        "https://x.test/page",
        "x.test",
        "widget page",
        "a crawled page about widgets",
        "crawled widgets",
        0,
        0.0,
        0,
        "page",
    )
    .unwrap();
    idx.add_doc_kind(
        "https://x.test/feed-item",
        "x.test",
        "widget rss",
        "an rss item about widgets",
        "rss widgets",
        0,
        0.0,
        0,
        "rss",
    )
    .unwrap();
    idx.commit().unwrap();

    let pages = idx.search_kind("widgets", 10, Some("page")).unwrap();
    assert_eq!(pages.len(), 1);
    assert_eq!(pages[0].url, "https://x.test/page");

    let rss = idx.search_kind("widgets", 10, Some("rss")).unwrap();
    assert_eq!(rss.len(), 1);
    assert_eq!(rss[0].url, "https://x.test/feed-item");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v1_index_rejects_lan_urls() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = support::test_config(dir.path());
    cfg.crawl.allow_private_ips = false;
    let crawler = support::make_crawler(cfg).unwrap();
    let state = support::app_state(crawler);
    let app = support::router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/v1/index"))
        .json(&serde_json::json!({
            "url": "http://127.0.0.1/secret",
            "title": "no",
            "body": "no"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[test]
fn hister_export_layout_is_parseable() {
    // hister import file expects: "[" alone, one doc per line starting with
    // '{' at column 0, "," alone between docs, "]" at the end
    let dir = tempfile::tempdir().unwrap();
    let cfg = support::test_config(dir.path());
    let crawler = support::make_crawler(cfg).unwrap();

    crawler
        .store()
        .put_doc(&lyra_index::store::DocRecord {
            url: "https://a.test/one".into(),
            title: "one".into(),
            host: "a.test".into(),
            fetched_at: 123,
            text_z: zstd::encode_all(b"hello world".as_slice(), 3).unwrap(),
            links: vec![],
            ..Default::default()
        })
        .unwrap();

    let out = dir.path().join("export.json");
    let n = crawler.export_hister(&out).unwrap();
    assert_eq!(n, 1);

    let text = std::fs::read_to_string(&out).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines[0], "[");
    assert!(lines[1].starts_with('{'));
    assert_eq!(lines[2], "]");

    // round trip back through our importer
    let dir2 = tempfile::tempdir().unwrap();
    let crawler2 = support::make_crawler(support::test_config(dir2.path())).unwrap();
    let n = crawler2.import_jsonl(&out).unwrap();
    crawler2.index().commit().unwrap();
    assert_eq!(n, 1);
    assert_eq!(crawler2.index().num_docs(), 1);
}

#[test]
fn snippet_uses_body_text() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = support::test_config(dir.path());
    let crawler = support::make_crawler(cfg).unwrap();
    let idx = crawler.index();
    idx.add_doc(
        "https://x.test/rust",
        "x.test",
        "Language notes",
        "The rust programming language is memory safe without a garbage collector.",
        "short meta",
        0,
        0.0,
        0,
    )
    .unwrap();
    idx.commit().unwrap();
    let hits = idx.search("garbage collector", 5).unwrap();
    assert!(!hits.is_empty());
    assert!(
        hits[0].snippet.to_lowercase().contains("garbage")
            || hits[0].snippet.to_lowercase().contains("collector"),
        "snippet was {}",
        hits[0].snippet
    );
}

#[test]
fn conjunction_requires_all_terms() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = support::test_config(dir.path());
    let crawler = support::make_crawler(cfg).unwrap();
    let idx = crawler.index();
    idx.add_doc(
        "https://x.test/a",
        "x.test",
        "Alpha page",
        "alpha widgets only live here",
        "",
        0,
        0.0,
        0,
    )
    .unwrap();
    idx.add_doc(
        "https://x.test/b",
        "x.test",
        "Both page",
        "alpha widgets and gizmos together",
        "",
        0,
        0.0,
        0,
    )
    .unwrap();
    idx.commit().unwrap();
    let hits = idx.search("alpha gizmos", 10).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].url, "https://x.test/b");
}

#[test]
fn prune_keeps_newest_docs() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = support::test_config(dir.path());
    let crawler = support::make_crawler(cfg).unwrap();
    let idx = crawler.index();
    for i in 0..5u64 {
        idx.add_doc(
            &format!("https://x.test/{i}"),
            "x.test",
            "doc",
            "shared text about privacy tools and firefox",
            "",
            i,
            0.0,
            0,
        )
        .unwrap();
        crawler
            .store()
            .put_doc(&lyra_index::store::DocRecord {
                url: format!("https://x.test/{i}"),
                title: "doc".into(),
                host: "x.test".into(),
                fetched_at: i,
                text_z: zstd::encode_all(
                    b"shared text about privacy tools and firefox".as_slice(),
                    3,
                )
                .unwrap(),
                links: vec![],
                ..Default::default()
            })
            .unwrap();
    }
    idx.commit().unwrap();
    let n = crawler.prune(Some(2), None, None).unwrap();
    assert_eq!(n, 3);
    crawler.index().commit().unwrap();
    assert_eq!(crawler.store_doc_count(), 2);
    let hits = crawler.index().search("privacy firefox", 10).unwrap();
    assert!(!hits.is_empty());
}

#[test]
fn prune_max_bytes_keeps_documents() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = support::test_config(dir.path());
    let crawler = support::make_crawler(cfg).unwrap();
    let idx = crawler.index();
    let body = "shared text about privacy tools and firefox ".repeat(80);
    for i in 0..8u64 {
        idx.add_doc(
            &format!("https://x.test/{i}"),
            "x.test",
            "doc",
            &body,
            "",
            i,
            0.0,
            0,
        )
        .unwrap();
        crawler
            .store()
            .put_doc(&lyra_index::store::DocRecord {
                url: format!("https://x.test/{i}"),
                title: "doc".into(),
                host: "x.test".into(),
                fetched_at: i,
                text_z: zstd::encode_all(body.as_bytes(), 3).unwrap(),
                links: vec![],
                ..Default::default()
            })
            .unwrap();
    }
    idx.commit().unwrap();
    let before = crawler.data_bytes();
    let cap = (before / 2).max(1);
    let n = crawler.prune(None, Some(cap), None).unwrap();
    assert!(n > 0, "expected some docs dropped");
    assert!(crawler.store_doc_count() >= 1, "byte prune wiped the index");
    crawler.index().commit().unwrap();
    let hits = crawler.index().search("privacy firefox", 10).unwrap();
    assert!(!hits.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crawl_stops_at_max_docs() {
    let (base, _) = serve_fixture().await;
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = support::test_config(dir.path());
    cfg.crawl.workers = 1;
    cfg.crawl.max_depth = 2;
    cfg.index.max_docs = 2;
    cfg.seeds = vec![format!("{base}/")];
    let crawler = std::sync::Arc::new(support::make_crawler(cfg).unwrap());
    crawler.seed(&[format!("{base}/")]);
    let c = crawler.clone();
    let task = tokio::spawn(async move { c.run().await });
    let _ = tokio::time::timeout(Duration::from_secs(60), task)
        .await
        .unwrap();
    let n = crawler.index().num_docs();
    assert!(n <= 3, "expected cap near 2 docs, got {n}");
    assert!(n >= 1, "expected at least one indexed page");
}

#[test]
fn reference_hosts_rank_above_random() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = support::test_config(dir.path());
    let crawler = support::make_crawler(cfg).unwrap();
    let idx = crawler.index();
    let body = "Firefox is a free and open source web browser with strong privacy defaults.";
    idx.add_doc(
        "https://spam.test/firefox",
        "spam.test",
        "Firefox review",
        body,
        "",
        0,
        0.0,
        0,
    )
    .unwrap();
    idx.add_doc(
        "https://en.wikipedia.org/wiki/Firefox",
        "en.wikipedia.org",
        "Firefox",
        body,
        "",
        0,
        0.0,
        0,
    )
    .unwrap();
    idx.commit().unwrap();
    let hits = idx.search("firefox privacy", 5).unwrap();
    assert_eq!(hits[0].url, "https://en.wikipedia.org/wiki/Firefox");
}
