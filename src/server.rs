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
    pub http: reqwest::Client,
    pub allow_private: bool,
    pub data_dir: std::path::PathBuf,
}

impl AppState {
    pub fn from_crawler(c: &Crawler) -> anyhow::Result<Self> {
        Ok(Self {
            index: c.index(),
            store: c.store(),
            doc_count: c.docs_done(),
            http: crate::rss::http_client()?,
            allow_private: c.allow_private_ips(),
            data_dir: c.data_dir().to_path_buf(),
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
    pub took_ms: u64,
}

#[derive(Serialize)]
struct SearchOut {
    hits: Vec<HitOut>,
}

#[derive(Serialize)]
struct HitOut {
    url: String,
    title: String,
    snippet: String,
    kind: String,
    score: f32,
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
        .route("/v1/feeds", get(list_feeds).post(add_feed))
        .route("/v1/feeds/refresh", post(refresh_feeds))
        .with_state(state)
}

fn clamp_limit(limit: usize) -> usize {
    limit.clamp(1, 100)
}

fn run_search(
    index: &SearchIndex,
    q: &str,
    limit: usize,
    offset: usize,
    kind: Option<&str>,
) -> Result<Vec<crate::index::SearchHit>, StatusCode> {
    index
        .search_kind(q, limit + offset, kind)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
        .map(|hits| hits.into_iter().skip(offset).collect())
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
        &s.index,
        &p.q,
        clamp_limit(p.limit),
        p.offset.min(10_000),
        p.kind.as_deref(),
    )?;
    Ok(Json(SearchResponse {
        query: p.q,
        results,
        took_ms: started.elapsed().as_millis() as u64,
    }))
}

async fn healthz(State(s): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        docs: s.doc_count.load(std::sync::atomic::Ordering::Relaxed),
    })
}

async fn stats(State(s): State<AppState>) -> Json<StatsResponse> {
    Json(StatsResponse {
        docs: s.store.doc_count(),
        index_docs: s.index.num_docs(),
        bytes: crate::index::dir_bytes(&s.data_dir),
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
    let hits = s
        .index
        .search_kind(&p.q, clamp_limit(p.limit), p.kind.as_deref())
        .map_err(ApiError::from)?;
    Ok(Json(SearchOut {
        hits: hits
            .into_iter()
            .map(|h| HitOut {
                snippet: h.clipped(240),
                url: h.url,
                title: h.title,
                kind: h.kind,
                score: h.score,
            })
            .collect(),
    }))
}

async fn search_rss(
    State(s): State<AppState>,
    Query(p): Query<SearchParams>,
) -> Result<Response, ApiError> {
    if p.q.trim().is_empty() {
        return Err(ApiError::bad("missing q"));
    }
    let hits = s
        .index
        .search_kind(&p.q, clamp_limit(p.limit), p.kind.as_deref())
        .map_err(ApiError::from)?;
    let mut xml = String::from(
        r#"<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel><title>Lyra Index</title>"#,
    );
    for h in hits {
        xml.push_str("<item><title>");
        xml.push_str(&escape_xml(&h.title));
        xml.push_str("</title><link>");
        xml.push_str(&escape_xml(&h.url));
        xml.push_str("</link><description>");
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
    Json(body): Json<IndexIn>,
) -> Result<StatusCode, ApiError> {
    if !url_is_public(&body.url, s.allow_private) {
        return Err(ApiError::bad("url is not a public http(s) address"));
    }
    let kind = if body.kind.is_empty() {
        "page"
    } else {
        body.kind.as_str()
    };
    let title = if body.title.is_empty() {
        body.url.as_str()
    } else {
        body.title.as_str()
    };
    let host = Url::parse(&body.url)
        .ok()
        .and_then(|u| u.host_str().map(str::to_string))
        .unwrap_or_default();
    s.index
        .add_doc_kind(
            &body.url,
            &host,
            title,
            &body.body,
            &body.body,
            crate::index::now_epoch(),
            0.0,
            0,
            kind,
        )
        .map_err(ApiError::from)?;
    s.index.commit().map_err(ApiError::from)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn add_feed(
    State(s): State<AppState>,
    Json(body): Json<FeedIn>,
) -> Result<Json<CountOut>, ApiError> {
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

async fn refresh_feeds(State(s): State<AppState>) -> Result<Json<CountOut>, ApiError> {
    let n = crate::rss::refresh_all(&s.index, &s.store, &s.http, s.allow_private)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(CountOut { indexed: n }))
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
