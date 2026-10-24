use crate::blocklist::url_is_public;
use crate::crawl::Crawler;
use crate::index::SearchIndex;
use crate::store::Store;
use axum::extract::{Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::info;
use url::Url;

#[derive(Clone)]
pub struct AppState {
    pub index: Arc<SearchIndex>,
    pub store: Arc<Store>,
    pub doc_count: Arc<std::sync::atomic::AtomicU64>,
    pub io_bad: Arc<std::sync::atomic::AtomicU64>,
    pub crawler: Option<Arc<Crawler>>,
    pub queries_total: Arc<std::sync::atomic::AtomicU64>,
    pub queries_ms: Arc<std::sync::atomic::AtomicU64>,
    pub http: reqwest::Client,
    pub allow_private: bool,
    pub data_dir: std::path::PathBuf,
    /// bearer token for mutating endpoints, None = open
    pub api_token: Option<String>,
    /// json body cap for mutating endpoints
    pub body_limit: usize,
}

impl AppState {
    pub fn from_crawler(c: &Arc<Crawler>) -> anyhow::Result<Self> {
        Ok(Self {
            index: c.index(),
            store: c.store(),
            doc_count: c.docs_done(),
            io_bad: c.io_bad.clone(),
            crawler: Some(c.clone()),
            queries_total: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            queries_ms: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            http: crate::rss::http_client()?,
            allow_private: c.allow_private_ips(),
            data_dir: c.data_dir().to_path_buf(),
            api_token: c.api_token().map(str::to_string),
            body_limit: c.api_body_limit(),
        })
    }
}

#[derive(Deserialize)]
pub struct SearchParams {
    q: String,
    #[serde(default = "default_limit")]
    limit: usize,
    #[serde(default)]
    offset: usize,
    #[serde(default)]
    kind: Option<String>,
}

fn default_limit() -> usize {
    20
}

#[derive(Serialize)]
pub struct SearchResponse {
    pub query: String,
    pub results: Vec<crate::index::SearchHit>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub did_you_mean: Option<String>,
    pub took_ms: u64,
}

#[derive(Serialize)]
struct SearchOut {
    hits: Vec<HitOut>,
    #[serde(skip_serializing_if = "Option::is_none")]
    did_you_mean: Option<String>,
    took_ms: u64,
}

#[derive(Serialize)]
struct HitOut {
    url: String,
    host: String,
    title: String,
    snippet: String,
    kind: String,
    score: f32,
    /// unix timestamp of the last successful fetch; clients show this as
    /// the result date
    ts: u64,
}

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub docs: u64,
}

#[derive(Serialize)]
pub struct StatsResponse {
    pub docs: u64,
    pub index_docs: u64,
    pub bytes: u64,
    pub uptime_secs: u64,
    pub docs_per_min: f64,
    pub queue_pending: usize,
    pub queue_disk: u64,
    pub in_flight: usize,
    pub io_errors: u64,
    pub io_degraded: bool,
    pub queries_total: u64,
    pub queries_avg_ms: f64,
}

#[derive(Deserialize)]
struct IndexIn {
    url: String,
    #[serde(default)]
    title: String,
    #[serde(default)]
    body: String,
    #[serde(default)]
    kind: String,
}

#[derive(Deserialize)]
struct FeedIn {
    url: String,
}

#[derive(Serialize)]
struct FeedsOut {
    feeds: Vec<String>,
}

#[derive(Serialize)]
struct CountOut {
    indexed: usize,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/search", get(search))
        .route("/healthz", get(healthz))
        .route("/stats", get(stats))
        .route("/health", get(health))
        .route("/v1/search", get(search_json))
        .route("/v1/search.rss", get(search_rss))
        .route("/v1/index", post(index_doc))
        .route("/v1/seed", post(seed_urls))
        .route("/v1/feeds", get(list_feeds).post(add_feed))
        .route("/v1/feeds/refresh", post(refresh_feeds))
        .route("/metrics", get(metrics))
        .layer(tower_http::limit::RequestBodyLimitLayer::new(4 * 1024 * 1024))
        .with_state(state)
}

fn clamp_limit(limit: usize) -> usize {
    limit.clamp(1, 100)
}

// tantivy reads are synchronous and can stall on merges, so run them on the
// blocking pool instead of a tokio worker
async fn run_search(
    index: Arc<SearchIndex>,
    q: String,
    limit: usize,
    offset: usize,
    kind: Option<String>,
) -> Result<Vec<crate::index::SearchHit>, StatusCode> {
    tokio::task::spawn_blocking(move || {
        index
            .search_kind(&q, limit + offset, kind.as_deref())
            .map(|hits| hits.into_iter().skip(offset).collect())
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn search(
    State(s): State<AppState>,
    Query(p): Query<SearchParams>,
) -> Result<Json<SearchResponse>, StatusCode> {
    if p.q.trim().is_empty() || p.q.len() > 512 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let started = std::time::Instant::now();
    let results = run_search(
        s.index.clone(),
        p.q.clone(),
        clamp_limit(p.limit),
        p.offset.min(10_000),
        p.kind.clone(),
    )
    .await?;
    s.queries_total
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    s.queries_ms.fetch_add(
        started.elapsed().as_millis() as u64,
        std::sync::atomic::Ordering::Relaxed,
    );
    let thin = results.len() < 3;
    let did_you_mean = if thin {
        let idx = s.index.clone();
        let qq = p.q.clone();
        tokio::task::spawn_blocking(move || idx.suggest(&qq))
            .await
            .ok()
            .flatten()
    } else {
        None
    };
    Ok(Json(SearchResponse {
        query: p.q,
        results,
        did_you_mean,
        took_ms: started.elapsed().as_millis() as u64,
    }))
}

async fn healthz(State(s): State<AppState>) -> Json<HealthResponse> {
    let io_ok = s.io_bad.load(std::sync::atomic::Ordering::Relaxed) < 3
        && !s.store.unhealthy();
    Json(HealthResponse {
        status: if io_ok { "ok" } else { "degraded" },
        docs: s.doc_count.load(std::sync::atomic::Ordering::Relaxed),
    })
}

async fn stats(State(s): State<AppState>) -> Json<StatsResponse> {
    let (queue_pending, queue_disk, in_flight, uptime_secs) = match &s.crawler {
        Some(c) => (
            c.frontier().pending(),
            c.frontier().disk_backlog(),
            c.frontier().in_flight(),
            c.uptime_secs(),
        ),
        None => (0, 0, 0, 0),
    };
    let docs = s.store.doc_count();
    let queries_total = s.queries_total.load(std::sync::atomic::Ordering::Relaxed);
    let queries_ms = s.queries_ms.load(std::sync::atomic::Ordering::Relaxed);
    Json(StatsResponse {
        docs,
        index_docs: s.index.num_docs(),
        bytes: crate::index::dir_bytes(&s.data_dir),
        uptime_secs,
        docs_per_min: if uptime_secs > 0 {
            (s.doc_count.load(std::sync::atomic::Ordering::Relaxed) as f64
                / uptime_secs as f64)
                * 60.0
        } else {
            0.0
        },
        queue_pending,
        queue_disk,
        in_flight,
        io_errors: s.io_bad.load(std::sync::atomic::Ordering::Relaxed),
        io_degraded: s.io_bad.load(std::sync::atomic::Ordering::Relaxed) >= 3
            || s.store.unhealthy(),
        queries_total,
        queries_avg_ms: if queries_total > 0 {
            queries_ms as f64 / queries_total as f64
        } else {
            0.0
        },
    })
}

async fn health() -> &'static str {
    "OK"
}

async fn search_json(
    State(s): State<AppState>,
    Query(p): Query<SearchParams>,
) -> Result<Json<SearchOut>, ApiError> {
    if p.q.trim().is_empty() {
        return Err(ApiError::bad("missing q"));
    }
    let started = std::time::Instant::now();
    let hits = run_search(
        s.index.clone(),
        p.q.clone(),
        clamp_limit(p.limit),
        p.offset.min(10_000),
        p.kind.clone(),
    )
    .await?;
    let n = hits.len();
    let hit_vec: Vec<HitOut> = hits
        .into_iter()
        .map(|h| HitOut {
            snippet: h.clipped(240),
            url: h.url,
            host: h.host,
            title: h.title,
            kind: h.kind,
            score: h.score,
            ts: h.fetched_at,
        })
        .collect();
    // thin result set? try a spelling correction from the term dictionary
    let did_you_mean = if n < 3 {
        let idx = s.index.clone();
        let qq = p.q.clone();
        tokio::task::spawn_blocking(move || idx.suggest(&qq))
            .await
            .ok()
            .flatten()
    } else {
        None
    };
    Ok(Json(SearchOut {
        hits: hit_vec,
        did_you_mean,
        took_ms: started.elapsed().as_millis() as u64,
    }))
}

async fn search_rss(
    State(s): State<AppState>,
    Query(p): Query<SearchParams>,
) -> Result<Response, ApiError> {
    if p.q.trim().is_empty() {
        return Err(ApiError::bad("missing q"));
    }
    let hits = run_search(
        s.index.clone(),
        p.q.clone(),
        clamp_limit(p.limit),
        p.offset.min(10_000),
        p.kind.clone(),
    )
    .await?;
    let mut xml = String::from(
        r#"<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel><title>Lyra Index</title>"#,
    );
    if hits.len() < 3 {
        let idx = s.index.clone();
        let qq = p.q.clone();
        if let Ok(Some(dym)) =
            tokio::task::spawn_blocking(move || idx.suggest(&qq)).await
        {
            xml.push_str("<didyoumean>");
            xml.push_str(&escape_xml(&dym));
            xml.push_str("</didyoumean>");
        }
    }
    for h in hits {
        xml.push_str("<item><title>");
        xml.push_str(&escape_xml(&h.title));
        xml.push_str("</title><link>");
        xml.push_str(&escape_xml(&h.url));
        xml.push_str("</link><pubDate>");
        xml.push_str(&rss_date(h.fetched_at));
        xml.push_str("</pubDate><description>");
        xml.push_str(&escape_xml(&h.clipped(240)));
        xml.push_str("</description></item>");
    }
    xml.push_str("</channel></rss>");
    Ok((
        [(header::CONTENT_TYPE, "application/rss+xml; charset=utf-8")],
        xml,
    )
        .into_response())
}

async fn index_doc(
    State(s): State<AppState>,
    headers: axum::http::HeaderMap,
    body: Result<Json<IndexIn>, axum::extract::rejection::JsonRejection>,
) -> Result<StatusCode, ApiError> {
    authed(&headers, &s)?;
    let Json(body) = body.map_err(ApiError::from)?;
    if !url_is_public(&body.url, s.allow_private) {
        return Err(ApiError::bad("url is not a public http(s) address"));
    }
    let kind = if body.kind.is_empty() {
        "page"
    } else {
        body.kind.as_str()
    };
    let title = if body.title.is_empty() {
        body.url.clone()
    } else {
        body.title.clone()
    };
    let host = Url::parse(&body.url)
        .ok()
        .and_then(|u| u.host_str().map(str::to_string))
        .unwrap_or_default();
    let index = s.index.clone();
    let kind = kind.to_string();
    let url = body.url.clone();
    let doc_body = body.body.clone();
    tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        index.add_doc_kind(
            &url,
            &host,
            &title,
            &doc_body,
            &doc_body,
            crate::index::now_epoch(),
            0.0,
            0,
            &kind,
        )?;
        index.commit()
    })
    .await
    .map_err(|e| ApiError::from(anyhow::anyhow!("join: {e}")))?
    .map_err(ApiError::from)?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct SeedIn {
    urls: Vec<String>,
    /// when true the urls jump the queue instead of joining the disk backlog
    #[serde(default)]
    priority: bool,
}

/// queue urls for crawling without a restart - they lead the frontier at
/// seed score and join the recrawl set
/// bearer check - only enforced when a token is configured
fn authed(headers: &axum::http::HeaderMap, s: &AppState) -> Result<(), ApiError> {
    let Some(tok) = &s.api_token else { return Ok(()) };
    let want = format!("Bearer {tok}");
    let ok = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(|v| v == want || v == tok.as_str())
        .unwrap_or(false);
    if ok {
        Ok(())
    } else {
        Err(ApiError::unauthorized())
    }
}

async fn seed_urls(
    State(s): State<AppState>,
    headers: axum::http::HeaderMap,
    body: Result<Json<SeedIn>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<CountOut>, ApiError> {
    authed(&headers, &s)?;
    let Json(body) = body.map_err(ApiError::from)?;
    if body.urls.is_empty() || body.urls.len() > 5000 {
        return Err(ApiError::bad("urls must be a list of 1..5000"));
    }
    let Some(c) = s.crawler else {
        return Err(ApiError::bad("seeding only in crawl mode"));
    };
    let n = if body.priority {
        c.seed_priority(&body.urls)
    } else {
        c.seed(&body.urls)
    };
    Ok(Json(CountOut { indexed: n }))
}

async fn add_feed(
    State(s): State<AppState>,
    headers: axum::http::HeaderMap,
    body: Result<Json<FeedIn>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<CountOut>, ApiError> {
    authed(&headers, &s)?;
    let Json(body) = body.map_err(ApiError::from)?;
    let n = crate::rss::ingest_feed(&s.index, &s.store, &s.http, &body.url, s.allow_private)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(CountOut { indexed: n }))
}

async fn list_feeds(State(s): State<AppState>) -> Json<FeedsOut> {
    Json(FeedsOut {
        feeds: s.store.list_feeds(),
    })
}

async fn refresh_feeds(
    State(s): State<AppState>,
    headers: axum::http::HeaderMap,
) -> Result<Json<CountOut>, ApiError> {
    authed(&headers, &s)?;
    let n = crate::rss::refresh_all(&s.index, &s.store, &s.http, s.allow_private)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(CountOut { indexed: n }))
}

/// prometheus text exposition - one counter per stat the api already tracks
async fn metrics(State(s): State<AppState>) -> Response {
    let docs = s.store.doc_count();
    let index_docs = s.index.num_docs();
    let io_errors = s.io_bad.load(std::sync::atomic::Ordering::Relaxed);
    let degraded = (io_errors >= 3 || s.store.unhealthy()) as u64;
    let qt = s.queries_total.load(std::sync::atomic::Ordering::Relaxed);
    let qms = s.queries_ms.load(std::sync::atomic::Ordering::Relaxed);
    let (queue_pending, queue_disk, in_flight, uptime) = match &s.crawler {
        Some(c) => (
            c.frontier().pending(),
            c.frontier().disk_backlog(),
            c.frontier().in_flight(),
            c.uptime_secs(),
        ),
        None => (0, 0, 0, 0),
    };
    let bytes = crate::index::dir_bytes(&s.data_dir);
    let body = format!(
        "lyra_docs {docs}\n         lyra_index_docs {index_docs}\n         lyra_index_bytes {bytes}\n         lyra_queue_pending {queue_pending}\n         lyra_queue_disk {queue_disk}\n         lyra_workers_in_flight {in_flight}\n         lyra_io_errors {io_errors}\n         lyra_io_degraded {degraded}\n         lyra_queries_total {qt}\n         lyra_queries_ms_total {qms}\n         lyra_uptime_secs {uptime}\n"
    );
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        body,
    )
        .into_response()
}

fn rss_date(ts: u64) -> String {
    let Ok(dt) = time::OffsetDateTime::from_unix_timestamp(ts as i64) else {
        return String::new();
    };
    dt.format(&time::format_description::well_known::Rfc2822)
        .unwrap_or_default()
}

fn escape_xml(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(c),
        }
    }
    out
}

struct ApiError {
    status: StatusCode,
    msg: String,
}

impl ApiError {
    fn bad(msg: &str) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            msg: msg.to_string(),
        }
    }
    fn unauthorized() -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            msg: "missing or invalid api token".into(),
        }
    }
}

impl From<axum::extract::rejection::JsonRejection> for ApiError {
    fn from(e: axum::extract::rejection::JsonRejection) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            msg: format!("invalid json: {e}"),
        }
    }
}

impl From<StatusCode> for ApiError {
    fn from(status: StatusCode) -> Self {
        Self {
            status,
            msg: status.to_string(),
        }
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            msg: e.to_string(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, self.msg).into_response()
    }
}

pub async fn serve(state: AppState, listen: &str) -> anyhow::Result<()> {
    let app = router(state);
    let listener = tokio::net::TcpListener::bind(listen).await?;
    info!(listen, "search api up");
    axum::serve(listener, app).await?;
    Ok(())
}
