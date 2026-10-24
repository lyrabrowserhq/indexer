use anyhow::Context;
use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use tracing::info;

/// frontier entry persisted in redb
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrontierItem {
    pub url: String,
    pub depth: u32,
    /// authority hint from the linking page, used for crawl prioritization
    pub score: f32,
    /// failed fetch attempts so far - lets the crawler retry transient
    /// failures instead of dropping the url on first error
    #[serde(default)]
    pub attempts: u8,
}

/// stored document record - html body text compressed with zstd
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DocRecord {
    pub url: String,
    pub title: String,
    pub host: String,
    pub fetched_at: u64,
    pub text_z: Vec<u8>,
    pub links: Vec<String>,
    #[serde(default)]
    pub etag: String,
    #[serde(default)]
    pub last_modified: String,
    /// 64-bit simhash of normalized text - persisted so near-dup detection
    /// survives restarts
    #[serde(default)]
    pub simhash: u64,
}

const FRONTIER: TableDefinition<u64, &[u8]> = TableDefinition::new("frontier");
const SEEN: TableDefinition<u64, u8> = TableDefinition::new("seen");
/// enqueue epoch per url key so stale entries can be recrawled
const SEEN_TS: TableDefinition<u64, u64> = TableDefinition::new("seen_ts");
const DOCS: TableDefinition<&str, &[u8]> = TableDefinition::new("docs");
const HOST_STATE: TableDefinition<&str, u64> = TableDefinition::new("host_state");
const FEEDS: TableDefinition<&str, u8> = TableDefinition::new("feeds");

pub struct Store {
    db: Arc<Database>,
    /// consecutive io errors - the crawler pauses while this stays high
    io_errors: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl Store {
    pub fn open(data_dir: &Path) -> anyhow::Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        let path = data_dir.join("state.redb");
        let db = Database::create(&path).with_context(|| format!("open {}", path.display()))?;
        let w = db.begin_write()?;
        {
            let _ = w.open_table(FRONTIER)?;
            let _ = w.open_table(SEEN)?;
            let _ = w.open_table(DOCS)?;
            let _ = w.open_table(HOST_STATE)?;
            let _ = w.open_table(FEEDS)?;
        }
        w.commit()?;
        Ok(Self {
            db: Arc::new(db),
            io_errors: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        })
    }

    pub fn seen(&self, key: u64) -> bool {
        let Ok(r) = self.db.begin_read() else {
            return false;
        };
        match r.open_table(SEEN) {
            Ok(t) => t.get(key).ok().flatten().is_some(),
            Err(_) => false,
        }
    }

    pub fn mark_seen(&self, key: u64) -> anyhow::Result<()> {
        let w = self.wtxn_volatile()?;
        {
            let mut t = w.open_table(SEEN)?;
            t.insert(key, 1u8)?;
            let mut tt = w.open_table(SEEN_TS)?;
            tt.insert(key, crate::index::now_epoch())?;
        }
        self.cmt(w)?;
        Ok(())
    }

    /// when this url key was last enqueued, or none if never
    pub fn seen_at(&self, key: u64) -> Option<u64> {
        let Ok(r) = self.db.begin_read() else {
            return None;
        };
        r.open_table(SEEN_TS)
            .ok()
            .and_then(|t| t.get(key).ok().flatten().map(|v| v.value()))
    }

    pub fn push_frontier(&self, item: &FrontierItem, key: u64) -> anyhow::Result<()> {
        let buf = serde_json::to_vec(item)?;
        let w = self.wtxn_volatile()?;
        {
            let mut t = w.open_table(FRONTIER)?;
            t.insert(key, buf.as_slice())?;
            let mut s = w.open_table(SEEN)?;
            s.insert(key, 1u8)?;
        }
        self.cmt(w)?;
        Ok(())
    }

    /// drop a persisted frontier row once the in-memory queue leases it,
    /// so refill never redelivers it
    pub fn remove_frontier(&self, key: u64) -> anyhow::Result<()> {
        let w = self.wtxn_volatile()?;
        {
            let mut t = w.open_table(FRONTIER)?;
            t.remove(key)?;
        }
        self.cmt(w)?;
        Ok(())
    }

    /// persist many frontier rows in one transaction
    pub fn push_frontier_many(&self, items: &[(u64, FrontierItem)]) -> anyhow::Result<()> {
        let w = self.wtxn_volatile()?;
        {
            let mut t = w.open_table(FRONTIER)?;
            let mut sn = w.open_table(SEEN)?;
            for (key, item) in items {
                let buf = serde_json::to_vec(item)?;
                t.insert(*key, buf.as_slice())?;
                sn.insert(*key, 1u8)?;
            }
        }
        self.cmt(w)?;
        Ok(())
    }

    /// pop up to n pending urls, lowest keys first (insertion order)
    pub fn pop_frontier(&self, n: usize) -> anyhow::Result<Vec<(u64, FrontierItem)>> {
        let w = self.wtxn_volatile()?;
        let mut out = Vec::with_capacity(n);
        {
            let mut t = w.open_table(FRONTIER)?;
            let keys: Vec<u64> = t
                .iter()?
                .take(n)
                .filter_map(|kv| kv.ok().map(|(k, _)| k.value()))
                .collect();
            for k in keys {
                if let Some(v) = t.remove(k)?
                    && let Ok(item) = serde_json::from_slice::<FrontierItem>(v.value())
                {
                    out.push((k, item));
                }
            }
        }
        self.cmt(w)?;
        Ok(out)
    }

    pub fn frontier_len(&self) -> u64 {
        let Ok(r) = self.db.begin_read() else {
            return 0;
        };
        r.open_table(FRONTIER)
            .map(|t| t.len().unwrap_or(0))
            .unwrap_or(0)
    }

    pub fn put_doc(&self, doc: &DocRecord) -> anyhow::Result<()> {
        let buf = serde_json::to_vec(doc)?;
        let w = self.wtxn()?;
        {
            let mut t = w.open_table(DOCS)?;
            t.insert(doc.url.as_str(), buf.as_slice())?;
        }
        self.cmt(w)?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn get_doc(&self, url: &str) -> anyhow::Result<Option<DocRecord>> {
        let r = self.db.begin_read()?;
        let t = match r.open_table(DOCS) {
            Ok(t) => t,
            Err(_) => return Ok(None),
        };
        let Some(v) = t.get(url)? else {
            return Ok(None);
        };
        Ok(Some(serde_json::from_slice(v.value())?))
    }

    /// per-host persistent state: epoch secs until which the host is banned
    pub fn host_banned_until(&self, site: &str) -> Option<u64> {
        let r = self.db.begin_read().ok()?;
        let t = r.open_table(HOST_STATE).ok()?;
        t.get(site).ok().flatten().map(|v| v.value())
    }

    pub fn ban_host(&self, site: &str, until_epoch: u64) -> anyhow::Result<()> {
        let w = self.wtxn_volatile()?;
        {
            let mut t = w.open_table(HOST_STATE)?;
            t.insert(site, until_epoch)?;
        }
        self.cmt(w)?;
        info!(site, until_epoch, "host banned");
        Ok(())
    }

    #[allow(dead_code)]
    /// just the simhashes, used to warm the in-memory dedupe set at startup
    pub fn all_simhashes(&self) -> anyhow::Result<Vec<u64>> {
        let rt = self.db.begin_read()?;
        let t = rt.open_table(DOCS)?;
        let mut out = Vec::new();
        for e in t.iter()? {
            let (_, v) = e?;
            if let Ok(d) = serde_json::from_slice::<DocRecord>(v.value())
                && d.simhash != 0
            {
                out.push(d.simhash);
            }
        }
        Ok(out)
    }

    /// stream docs one at a time without materializing the whole table -
    /// doc text is compressed but 40k+ records still add up to gigs if held
    /// at once
    pub fn for_each_doc(&self, mut f: impl FnMut(&DocRecord)) -> anyhow::Result<()> {
        let r = self.db.begin_read()?;
        let t = r.open_table(DOCS)?;
        for e in t.iter()? {
            let (_, v) = e?;
            if let Ok(d) = serde_json::from_slice::<DocRecord>(v.value()) {
                f(&d);
            }
        }
        Ok(())
    }

    /// url + fetched_at only - light enough to collect for sorting
    pub fn iter_doc_meta(&self) -> anyhow::Result<Vec<(String, u64)>> {
        let mut out = Vec::new();
        self.for_each_doc(|d| out.push((d.url.clone(), d.fetched_at)))?;
        Ok(out)
    }

    pub fn iter_docs(&self) -> anyhow::Result<Vec<DocRecord>> {
        let r = self.db.begin_read()?;
        let t = r.open_table(DOCS)?;
        let mut out = Vec::with_capacity(t.len()? as usize);
        for kv in t.iter()? {
            let (_, v) = kv?;
            out.push(serde_json::from_slice(v.value())?);
        }
        Ok(out)
    }

    #[allow(dead_code)]
    /// true when recent writes keep failing - the disk may be gone or full
    pub fn unhealthy(&self) -> bool {
        self.io_errors.load(std::sync::atomic::Ordering::Relaxed) >= 3
    }

    fn note<T>(&self, r: &anyhow::Result<T>) {
        use std::sync::atomic::Ordering::Relaxed;
        match r {
            Ok(_) => self.io_errors.store(0, Relaxed),
            Err(_) => {
                let n = self.io_errors.fetch_add(1, Relaxed) + 1;
                if n == 3 {
                    tracing::warn!("store io errors piling up - crawler will pause");
                }
            }
        }
    }

    fn wtxn(&self) -> anyhow::Result<redb::WriteTransaction> {
        let r = self.db.begin_write().map_err(anyhow::Error::from);
        self.note(&r);
        r
    }

    /// write txn without fsync - for rebuildable state like the frontier
    /// queue and seen set where a crash just means recrawling a page
    fn wtxn_volatile(&self) -> anyhow::Result<redb::WriteTransaction> {
        let r = self.db.begin_write().map_err(anyhow::Error::from);
        self.note(&r);
        let mut w = r?;
        w.set_durability(redb::Durability::None)?;
        Ok(w)
    }

    fn cmt(&self, w: redb::WriteTransaction) -> anyhow::Result<()> {
        let r = w.commit().map_err(anyhow::Error::from);
        self.note(&r);
        r
    }

    pub fn doc_count(&self) -> u64 {
        let Ok(r) = self.db.begin_read() else {
            return 0;
        };
        r.open_table(DOCS)
            .map(|t| t.len().unwrap_or(0))
            .unwrap_or(0)
    }

    pub fn delete_doc(&self, url: &str) -> anyhow::Result<()> {
        let w = self.wtxn()?;
        {
            let mut t = w.open_table(DOCS)?;
            t.remove(url)?;
        }
        self.cmt(w)?;
        Ok(())
    }

    pub fn add_feed(&self, feed_url: &str) -> anyhow::Result<()> {
        let w = self.wtxn()?;
        {
            let mut t = w.open_table(FEEDS)?;
            t.insert(feed_url, 1u8)?;
        }
        self.cmt(w)?;
        Ok(())
    }

    pub fn list_feeds(&self) -> Vec<String> {
        let Ok(r) = self.db.begin_read() else {
            return Vec::new();
        };
        let Ok(t) = r.open_table(FEEDS) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        if let Ok(iter) = t.iter() {
            for kv in iter.flatten() {
                out.push(kv.0.value().to_string());
            }
        }
        out.sort();
        out
    }
}

pub fn compact_file(data_dir: &Path) -> anyhow::Result<bool> {
    let path = data_dir.join("state.redb");
    if !path.exists() {
        return Ok(false);
    }
    let mut db = Database::open(&path)?;
    Ok(db.compact()?)
}
