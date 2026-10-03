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
}

const FRONTIER: TableDefinition<u64, &[u8]> = TableDefinition::new("frontier");
const SEEN: TableDefinition<u64, u8> = TableDefinition::new("seen");
const DOCS: TableDefinition<&str, &[u8]> = TableDefinition::new("docs");
const HOST_STATE: TableDefinition<&str, u64> = TableDefinition::new("host_state");
const FEEDS: TableDefinition<&str, u8> = TableDefinition::new("feeds");

pub struct Store {
    db: Arc<Database>,
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
        Ok(Self { db: Arc::new(db) })
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
        let w = self.db.begin_write()?;
        {
            let mut t = w.open_table(SEEN)?;
            t.insert(key, 1u8)?;
        }
        w.commit()?;
        Ok(())
    }

    pub fn push_frontier(&self, item: &FrontierItem, key: u64) -> anyhow::Result<()> {
        let buf = serde_json::to_vec(item)?;
        let w = self.db.begin_write()?;
        {
            let mut t = w.open_table(FRONTIER)?;
            t.insert(key, buf.as_slice())?;
            let mut s = w.open_table(SEEN)?;
            s.insert(key, 1u8)?;
        }
        w.commit()?;
        Ok(())
    }

    /// pop up to n pending urls, lowest keys first (insertion order)
    pub fn pop_frontier(&self, n: usize) -> anyhow::Result<Vec<(u64, FrontierItem)>> {
        let w = self.db.begin_write()?;
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
        w.commit()?;
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
        let w = self.db.begin_write()?;
        {
            let mut t = w.open_table(DOCS)?;
            t.insert(doc.url.as_str(), buf.as_slice())?;
        }
        w.commit()?;
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
        let w = self.db.begin_write()?;
        {
            let mut t = w.open_table(HOST_STATE)?;
            t.insert(site, until_epoch)?;
        }
        w.commit()?;
        info!(site, until_epoch, "host banned");
        Ok(())
    }

    #[allow(dead_code)]
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
    pub fn doc_count(&self) -> u64 {
        let Ok(r) = self.db.begin_read() else {
            return 0;
        };
        r.open_table(DOCS)
            .map(|t| t.len().unwrap_or(0))
            .unwrap_or(0)
    }

    pub fn delete_doc(&self, url: &str) -> anyhow::Result<()> {
        let w = self.db.begin_write()?;
        {
            let mut t = w.open_table(DOCS)?;
            t.remove(url)?;
        }
        w.commit()?;
        Ok(())
    }

    pub fn add_feed(&self, feed_url: &str) -> anyhow::Result<()> {
        let w = self.db.begin_write()?;
        {
            let mut t = w.open_table(FEEDS)?;
            t.insert(feed_url, 1u8)?;
        }
        w.commit()?;
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
