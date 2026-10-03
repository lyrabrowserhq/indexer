use parking_lot::Mutex;
use serde::Serialize;
use std::sync::Arc;
use tracing::{debug, warn};

/// live document push into a running hister server through POST /api/batch.
/// batches of at most 100 ops, x-access-token auth header.
#[derive(Clone)]
pub struct HisterClient {
    inner: Arc<Inner>,
}

struct Inner {
    base: String,
    token: String,
    client: reqwest::Client,
    pending: Mutex<Vec<BatchOp>>,
    batch_size: usize,
}

#[derive(Serialize, Clone)]
struct BatchOp {
    op: &'static str,
    url: String,
    domain: String,
    title: String,
    text: String,
    added: u64,
    updated: u64,
    #[serde(rename = "type")]
    doc_type: u8,
    label: String,
}

#[derive(Serialize)]
struct BatchReq {
    ops: Vec<BatchOp>,
}

impl HisterClient {
    /// base empty means disabled - callers check enabled() first
    pub fn new(base: &str, token: &str, client: reqwest::Client, batch_size: usize) -> Self {
        Self {
            inner: Arc::new(Inner {
                base: base.trim_end_matches('/').to_string(),
                token: token.to_string(),
                client,
                pending: Mutex::new(Vec::new()),
                batch_size: batch_size.clamp(1, 100),
            }),
        }
    }

    pub fn enabled(&self) -> bool {
        !self.inner.base.is_empty()
    }

    /// buffer a doc for the next batch. text is the decoded page body -
    /// hister indexes it, so empty text means a dead doc there
    pub fn queue(&self, url: &str, host: &str, title: &str, text: &str, fetched_at: u64) {
        let ready = {
            let mut p = self.inner.pending.lock();
            p.push(BatchOp {
                op: "add",
                url: url.into(),
                domain: host.into(),
                title: title.into(),
                text: text.into(),
                added: fetched_at,
                updated: fetched_at,
                doc_type: 0,
                label: "lyra-index".into(),
            });
            p.len() >= self.inner.batch_size
        };
        if ready {
            // best-effort async flush; caller can also flush explicitly
            let this = self.clone();
            tokio::spawn(async move {
                if let Err(e) = this.flush().await {
                    warn!(error = %e, "hister background flush failed");
                }
            });
        }
    }

    pub async fn flush(&self) -> anyhow::Result<usize> {
        let ops = {
            let mut p = self.inner.pending.lock();
            std::mem::take(&mut *p)
        };
        if ops.is_empty() {
            return Ok(0);
        }
        let mut sent = 0usize;
        for chunk in ops.chunks(100) {
            let req = BatchReq {
                ops: chunk.to_vec(),
            };
            let mut rb = self
                .inner
                .client
                .post(format!("{}/api/batch", self.inner.base));
            if !self.inner.token.is_empty() {
                rb = rb.header("X-Access-Token", &self.inner.token);
            }
            match rb.json(&req).send().await {
                Ok(resp) if resp.status().is_success() => sent += chunk.len(),
                Ok(resp) => warn!(status = %resp.status(), "hister batch rejected"),
                Err(e) => warn!(error = %e, "hister push failed"),
            }
        }
        debug!(sent, "pushed docs to hister");
        Ok(sent)
    }
}
