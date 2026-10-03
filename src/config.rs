use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct Config {
    pub identity: Identity,
    pub crawl: Crawl,
    pub frontier: FrontierCfg,
    pub challenge: Challenge,
    pub store: Store,
    pub index: IndexCfg,
    pub api: Api,
    pub blocklists: Blocklists,
    /// live document push into a hister server while crawling
    pub hister: Hister,
    pub seeds: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Hister {
    /// base url of the hister server, empty disables live push
    pub url: String,
    /// access token - matches hister's app.access_token
    pub token: String,
    /// docs per /api/batch call, server caps at 100
    pub batch_size: usize,
}

impl Default for Hister {
    fn default() -> Self {
        Self {
            url: String::new(),
            token: String::new(),
            batch_size: 50,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Identity {
    pub user_agent: String,
    pub contact: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Crawl {
    pub workers: usize,
    pub min_delay_ms: u64,
    pub max_retries: u32,
    pub fetch_timeout_secs: u64,
    pub connect_timeout_secs: u64,
    pub max_body_bytes: usize,
    pub max_redirects: usize,
    pub max_depth: u32,
    pub slow_drip_min_bytes: usize,
    pub slow_drip_floor_kbps: u64,
    /// allow private/loopback ips - off by default, tests turn it on
    pub allow_private_ips: bool,
    /// outbound proxy for fetches, e.g. socks5h://10.64.0.1:1080 for the
    /// mullvad in-tunnel socks5 proxy. empty means direct egress.
    pub proxy: String,
    /// crawl budget per site - keeps a giant site from eating the frontier
    pub max_pages_per_site: u32,
    /// wait this many times the last fetch latency before the next url on
    /// the same host (Heritrix-style). 1.0 means wait the fetch time
    pub delay_factor: f64,
    /// hard cap on per-host wait, including robots Crawl-delay
    pub max_delay_ms: u64,
    /// skip the host when robots Crawl-delay exceeds this (0 disables)
    pub max_crawl_delay_ms: u64,
    /// random extra wait added to each per-host delay
    pub jitter_ms: u64,
    /// outlinks kept per page after junk filters
    pub max_outlinks: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct FrontierCfg {
    pub max_pending: usize,
    pub refill_batch: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Challenge {
    pub flaresolverr_url: String,
    pub flaresolverr_timeout_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Store {
    pub path: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct IndexCfg {
    pub writer_heap_mb: usize,
    pub commit_every_docs: usize,
    /// 0 means no cap on indexed documents
    pub max_docs: u64,
    /// 0 means no cap on INDEX_DIR bytes (tantivy plus redb)
    pub max_bytes: u64,
    /// stored body bytes per document used for ranking and snippets
    pub index_text_bytes: usize,
    /// skip pages with fewer alphanumeric characters than this
    pub min_text_chars: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Api {
    pub listen: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct Blocklists {
    pub extra_files: Vec<PathBuf>,
}

impl Default for Identity {
    fn default() -> Self {
        Self {
            user_agent: format!(
                "LyraIndex/{} (+https://lyrabrowser.com/bot)",
                env!("CARGO_PKG_VERSION")
            ),
            contact: "https://lyrabrowser.com/about".into(),
        }
    }
}

impl Default for Crawl {
    fn default() -> Self {
        Self {
            workers: 8,
            min_delay_ms: 2000,
            max_retries: 2,
            fetch_timeout_secs: 20,
            connect_timeout_secs: 8,
            max_body_bytes: 2 * 1024 * 1024,
            max_redirects: 5,
            max_depth: 3,
            slow_drip_min_bytes: 64 * 1024,
            slow_drip_floor_kbps: 8,
            allow_private_ips: false,
            proxy: String::new(),
            max_pages_per_site: 500,
            delay_factor: 2.0,
            max_delay_ms: 30_000,
            max_crawl_delay_ms: 30_000,
            jitter_ms: 250,
            max_outlinks: 80,
        }
    }
}

impl Default for FrontierCfg {
    fn default() -> Self {
        Self {
            max_pending: 100_000,
            refill_batch: 256,
        }
    }
}

impl Default for Challenge {
    fn default() -> Self {
        Self {
            flaresolverr_url: String::new(),
            flaresolverr_timeout_ms: 60_000,
        }
    }
}

impl Default for Store {
    fn default() -> Self {
        Self {
            path: PathBuf::from("data"),
        }
    }
}

impl Default for IndexCfg {
    fn default() -> Self {
        Self {
            writer_heap_mb: 256,
            commit_every_docs: 500,
            max_docs: 0,
            max_bytes: 0,
            index_text_bytes: 16 * 1024,
            min_text_chars: 200,
        }
    }
}

impl Default for Api {
    fn default() -> Self {
        Self {
            listen: "0.0.0.0:8091".into(),
        }
    }
}

impl Config {
    pub fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)?;
        let mut cfg: Config = toml::from_str(&text)?;
        cfg.apply_env();
        Ok(cfg)
    }

    /// env overrides so containers can run without a mounted config file
    pub fn apply_env(&mut self) {
        if let Ok(v) = std::env::var("BIND").or_else(|_| std::env::var("LYRA_INDEX_LISTEN")) {
            self.api.listen = v;
        }
        if let Ok(v) = std::env::var("INDEX_DIR").or_else(|_| std::env::var("LYRA_INDEX_DIR")) {
            self.store.path = std::path::PathBuf::from(v);
        }
        if let Ok(v) = std::env::var("LYRA_INDEX_USER_AGENT") {
            self.identity.user_agent = v;
        }
        if let Ok(v) = std::env::var("LYRA_INDEX_FLARESOLVERR") {
            self.challenge.flaresolverr_url = v;
        }
        if let Ok(v) = std::env::var("LYRA_INDEX_PROXY") {
            self.crawl.proxy = v;
        }
        if let Ok(v) = std::env::var("LYRA_INDEX_HISTER_URL") {
            self.hister.url = v;
        }
        if let Ok(v) = std::env::var("LYRA_INDEX_HISTER_TOKEN") {
            self.hister.token = v;
        }
        if let Ok(v) = std::env::var("LYRA_INDEX_MAX_PAGES_PER_SITE")
            && let Ok(n) = v.parse()
        {
            self.crawl.max_pages_per_site = n;
        }
        if let Ok(v) = std::env::var("LYRA_INDEX_WORKERS")
            && let Ok(n) = v.parse()
        {
            self.crawl.workers = n;
        }
        if let Ok(v) = std::env::var("LYRA_INDEX_MAX_DOCS")
            && let Ok(n) = v.parse()
        {
            self.index.max_docs = n;
        }
        if let Ok(v) = std::env::var("LYRA_INDEX_MAX_BYTES")
            && let Ok(n) = v.parse()
        {
            self.index.max_bytes = n;
        }
        if let Ok(v) = std::env::var("LYRA_INDEX_COMMIT_EVERY")
            && let Ok(n) = v.parse()
        {
            self.index.commit_every_docs = n;
        }
        if let Ok(v) = std::env::var("LYRA_INDEX_TEXT_BYTES")
            && let Ok(n) = v.parse()
        {
            self.index.index_text_bytes = n;
        }
        if let Ok(v) = std::env::var("LYRA_INDEX_MIN_TEXT_CHARS")
            && let Ok(n) = v.parse()
        {
            self.index.min_text_chars = n;
        }
    }
}
