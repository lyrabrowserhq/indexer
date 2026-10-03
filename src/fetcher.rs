use crate::config::Crawl;
use futures::StreamExt;
use reqwest::header::{ACCEPT, ACCEPT_LANGUAGE, HeaderMap, HeaderValue, USER_AGENT};
use reqwest::{Client, StatusCode};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::time::timeout;
use tracing::debug;

#[derive(Error, Debug)]
pub enum FetchError {
    #[error("http {code}")]
    Http {
        code: StatusCode,
        retry_after_ms: Option<u64>,
    },
    #[error("not modified")]
    NotModified,
    #[error("too large: {0} bytes")]
    TooLarge(usize),
    #[error("slow drip detected")]
    SlowDrip,
    #[error("timeout")]
    Timeout,
    #[error("not html")]
    NotHtml,
    #[error(transparent)]
    Net(#[from] reqwest::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[allow(dead_code)]
pub struct FetchResponse {
    pub status: StatusCode,
    pub final_url: reqwest::Url,
    pub headers: HeaderMap,
    pub body: String,
    pub elapsed_ms: u64,
    pub bytes: usize,
    pub etag: String,
    pub last_modified: String,
}

#[derive(Clone)]
pub struct Fetcher {
    client: Client,
    cfg: Crawl,
    inflight_bytes: std::sync::Arc<AtomicUsize>,
}

impl FetchError {
    pub fn http_code(&self) -> Option<u16> {
        match self {
            FetchError::Http { code, .. } => Some(code.as_u16()),
            _ => None,
        }
    }

    pub fn retry_after_ms(&self) -> Option<u64> {
        match self {
            FetchError::Http { retry_after_ms, .. } => *retry_after_ms,
            _ => None,
        }
    }
}

fn retry_after_ms(headers: &HeaderMap) -> Option<u64> {
    let raw = headers.get(reqwest::header::RETRY_AFTER)?.to_str().ok()?;
    if let Ok(secs) = raw.trim().parse::<u64>() {
        return Some(secs.saturating_mul(1000));
    }
    None
}

fn header_text(headers: &HeaderMap, name: reqwest::header::HeaderName) -> String {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .trim()
        .to_string()
}

impl Fetcher {
    pub fn new(cfg: &Crawl, ua: &str) -> anyhow::Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_str(ua)?);
        headers.insert(
            ACCEPT,
            HeaderValue::from_static(
                "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            ),
        );
        headers.insert(ACCEPT_LANGUAGE, HeaderValue::from_static("en-US,en;q=0.9"));

        let max_redirects = cfg.max_redirects;
        let mut builder = Client::builder();
        if !cfg.proxy.is_empty() {
            builder = builder.proxy(reqwest::Proxy::all(&cfg.proxy)?);
        }
        let client = builder
            .default_headers(headers)
            .connect_timeout(Duration::from_secs(cfg.connect_timeout_secs))
            .redirect(reqwest::redirect::Policy::custom(move |attempt| {
                if attempt.previous().len() >= max_redirects {
                    attempt.stop()
                } else {
                    attempt.follow()
                }
            }))
            .pool_max_idle_per_host(2)
            .tcp_nodelay(true)
            .tcp_keepalive(Duration::from_secs(30))
            .cookie_store(true)
            .build()?;

        Ok(Self {
            client,
            cfg: cfg.clone(),
            inflight_bytes: std::sync::Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    /// fetch with hard timeout, size cap and slow-drip tarpit detection.
    /// a tarpit serves bytes steadily but at very low throughput to keep a
    /// crawler busy; we abort once enough bytes have arrived slowly.
    pub async fn get(&self, url: &str) -> Result<FetchResponse, FetchError> {
        self.get_conditional(url, "", "").await
    }

    pub async fn get_conditional(
        &self,
        url: &str,
        etag: &str,
        last_modified: &str,
    ) -> Result<FetchResponse, FetchError> {
        let started = Instant::now();
        let mut req = self.client.get(url);
        if !etag.is_empty() {
            req = req.header(reqwest::header::IF_NONE_MATCH, etag);
        }
        if !last_modified.is_empty() {
            req = req.header(reqwest::header::IF_MODIFIED_SINCE, last_modified);
        }
        let resp = timeout(Duration::from_secs(self.cfg.fetch_timeout_secs), req.send())
            .await
            .map_err(|_| FetchError::Timeout)??;

        let status = resp.status();
        let final_url = resp.url().clone();
        let headers = resp.headers().clone();
        let etag = header_text(&headers, reqwest::header::ETAG);
        let last_modified = header_text(&headers, reqwest::header::LAST_MODIFIED);

        if status == StatusCode::NOT_MODIFIED {
            return Err(FetchError::NotModified);
        }

        if !status.is_success() {
            return Err(FetchError::Http {
                code: status,
                retry_after_ms: retry_after_ms(&headers),
            });
        }

        let ctype = headers
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_lowercase();
        if !ctype.is_empty()
            && !ctype.contains("html")
            && !ctype.contains("text/plain")
            && !ctype.contains("xml")
        {
            return Err(FetchError::NotHtml);
        }

        if let Some(len) = resp.content_length()
            && len as usize > self.cfg.max_body_bytes
        {
            return Err(FetchError::TooLarge(len as usize));
        }

        // stream body with size + drip guard
        let mut body: Vec<u8> = Vec::new();
        let mut stream = resp.bytes_stream();
        let deadline = Duration::from_secs(self.cfg.fetch_timeout_secs);
        while let Some(chunk) = stream.next().await {
            if started.elapsed() > deadline {
                return Err(FetchError::Timeout);
            }
            let chunk = chunk.map_err(FetchError::Net)?;
            body.extend_from_slice(&chunk);
            self.inflight_bytes
                .fetch_add(chunk.len(), Ordering::Relaxed);
            if body.len() > self.cfg.max_body_bytes {
                return Err(FetchError::TooLarge(body.len()));
            }
            // slow-drip check: after slow_drip_min_bytes, if average throughput
            // is below the floor, this looks like a tarpit
            if body.len() >= self.cfg.slow_drip_min_bytes {
                let secs = started.elapsed().as_secs_f64().max(0.1);
                let kbps = (body.len() as f64 / 1024.0) / secs;
                if kbps < self.cfg.slow_drip_floor_kbps as f64 {
                    debug!(url, kbps, "aborting slow-drip response");
                    return Err(FetchError::SlowDrip);
                }
            }
        }

        let elapsed_ms = started.elapsed().as_millis() as u64;
        let bytes = body.len();
        let text = String::from_utf8_lossy(&body).into_owned();
        Ok(FetchResponse {
            status,
            final_url,
            headers,
            body: text,
            elapsed_ms,
            bytes,
            etag,
            last_modified,
        })
    }
}
