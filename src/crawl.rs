use crate::blocklist::Blocklist;
use crate::challenge::{ChallengeKind, ChallengeSolver};
use crate::config::Config;
use crate::extract::extract;
use crate::fetcher::{FetchError, Fetcher};
use crate::frontier::Frontier;
use crate::hister;
use crate::index::{SearchIndex, now_epoch};
use crate::quality;
use crate::robots::RobotsCache;
use crate::store::{DocRecord, FrontierItem, Store};
use crate::urlnorm;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};
use url::Url;

pub struct Crawler {
    cfg: Config,
    frontier: Arc<Frontier>,
    store: Arc<Store>,
    index: Arc<SearchIndex>,
    fetcher: Fetcher,
    robots: Arc<RobotsCache>,
    blocklist: Arc<Blocklist>,
    solver: ChallengeSolver,
    hister: hister::HisterClient,
    authority: Arc<dashmap::DashMap<String, u64>>,
    /// pages stored per site - enforces the per-site crawl budget
    site_pages: Arc<dashmap::DashMap<String, u32>>,
    /// 64-bit simhashes of normalized page text - near-dup skipping with
    /// hamming distance, catches mirrors that differ by a few tokens
    content_sims: Arc<std::sync::RwLock<Vec<u64>>>,
    logged_limit: std::sync::atomic::AtomicBool,
    /// hosts whose robots sitemaps were already enqueued
    sitemaps_seen: Arc<dashmap::DashSet<String>>,
    docs_done: Arc<AtomicU64>,
    dns_ok: Arc<dashmap::DashMap<String, (std::time::Instant, bool)>>,
    /// seed urls kept around so the run loop can recrawl stale ones
    seeds: std::sync::Mutex<Vec<String>>,
    /// consecutive index write/commit failures - pauses crawling when high
    pub io_bad: Arc<AtomicU64>,
    /// process start, for uptime/done-rate metrics
    started_at: std::time::Instant,
    /// cached data-dir byte size plus when it was measured - dir walking is
    /// too expensive to run per leased url
    bytes_cache: Arc<AtomicU64>,
    bytes_at: Arc<AtomicU64>,
}

impl Crawler {
    pub fn new(cfg: Config) -> anyhow::Result<Self> {
        let store = Arc::new(Store::open(&cfg.store.path)?);
        let index = Arc::new(SearchIndex::open(
            &cfg.store.path.join("index"),
            cfg.index.writer_heap_mb,
        )?);
        let fetcher = Fetcher::new(&cfg.crawl, &cfg.identity.user_agent)?;
        let robots = Arc::new(RobotsCache::new(
            fetcher.client().clone(),
            cfg.identity.user_agent.clone(),
        ));
        let blocklist = Arc::new(Blocklist::load(
            &cfg.store.path,
            &cfg.blocklists.extra_files,
        ));
        let frontier = Arc::new(Frontier::new(store.clone(), &cfg.crawl, &cfg.frontier));
        let solver = ChallengeSolver::new(cfg.challenge.clone(), fetcher.client().clone());
        let hister = hister::HisterClient::new(
            &cfg.hister.url,
            &cfg.hister.token,
            fetcher.client().clone(),
            cfg.hister.batch_size,
        );

        Ok(Self {
            cfg,
            frontier,
            store,
            index,
            fetcher,
            robots,
            blocklist,
            solver,
            hister,
            authority: Arc::new(dashmap::DashMap::new()),
            site_pages: Arc::new(dashmap::DashMap::new()),
            content_sims: Arc::new(std::sync::RwLock::new(Vec::new())),
            logged_limit: std::sync::atomic::AtomicBool::new(false),
            sitemaps_seen: Arc::new(dashmap::DashSet::new()),
            docs_done: Arc::new(AtomicU64::new(0)),
            dns_ok: Arc::new(dashmap::DashMap::new()),
            seeds: std::sync::Mutex::new(Vec::new()),
            io_bad: Arc::new(AtomicU64::new(0)),
            started_at: std::time::Instant::now(),
            bytes_cache: Arc::new(AtomicU64::new(0)),
            bytes_at: Arc::new(AtomicU64::new(0)),
        })
    }

    pub fn api_token(&self) -> Option<&str> {
        self.cfg.api.token()
    }
    pub fn api_body_limit(&self) -> usize {
        self.cfg.api.body_limit
    }

    pub fn frontier(&self) -> Arc<Frontier> {
        self.frontier.clone()
    }

    pub fn uptime_secs(&self) -> u64 {
        self.started_at.elapsed().as_secs()
    }

    /// true when the disk keeps failing - workers pause instead of chewing
    /// through a broken drive
    pub fn io_unhealthy(&self) -> bool {
        self.store.unhealthy() || self.io_bad.load(Ordering::Relaxed) >= 3
    }

    fn note_io<T>(&self, r: &anyhow::Result<T>) {
        match r {
            Ok(_) => self.io_bad.store(0, Ordering::Relaxed),
            Err(_) => {
                let n = self.io_bad.fetch_add(1, Ordering::Relaxed) + 1;
                if n == 3 {
                    warn!("index io errors piling up - crawler will pause");
                }
            }
        }
    }

    pub fn index(&self) -> Arc<SearchIndex> {
        self.index.clone()
    }

    pub fn store(&self) -> Arc<Store> {
        self.store.clone()
    }

    pub fn store_doc_count(&self) -> u64 {
        self.store.doc_count()
    }

    pub fn docs_done(&self) -> Arc<AtomicU64> {
        self.docs_done.clone()
    }

    pub fn allow_private_ips(&self) -> bool {
        self.cfg.crawl.allow_private_ips
    }

    pub fn data_bytes(&self) -> u64 {
        crate::index::dir_bytes(&self.cfg.store.path)
    }

    /// cached variant for the hot loop - refreshes at most every 30s
    fn data_bytes_cached(&self) -> u64 {
        let now = now_epoch();
        let last = self.bytes_at.load(Ordering::Relaxed);
        if now.saturating_sub(last) < 30 {
            return self.bytes_cache.load(Ordering::Relaxed);
        }
        let b = self.data_bytes();
        self.bytes_cache.store(b, Ordering::Relaxed);
        self.bytes_at.store(now, Ordering::Relaxed);
        b
    }

    pub fn data_dir(&self) -> &std::path::Path {
        &self.cfg.store.path
    }

    pub fn over_limit(&self) -> bool {
        if self.cfg.index.max_docs > 0 && self.store.doc_count() >= self.cfg.index.max_docs {
            return true;
        }
        self.cfg.index.max_bytes > 0 && self.data_bytes_cached() >= self.cfg.index.max_bytes
    }

    fn accept_outlink(&self, u: &url::Url) -> bool {
        !self.blocklist.blocked(&urlnorm::host_of(u)) && !quality::skip_url(u)
    }

    async fn host_routable(&self, host: &str) -> bool {
        const TTL: Duration = Duration::from_secs(300);
        if let Some(hit) = self.dns_ok.get(host)
            && hit.0.elapsed() < TTL
        {
            return hit.1;
        }
        let ok = match tokio::net::lookup_host((host, 443)).await {
            Ok(addrs) => {
                let addrs: Vec<_> = addrs.collect();
                !addrs.is_empty()
                    && addrs
                        .iter()
                        .any(|a| Blocklist::ip_allowed(a.ip(), self.cfg.crawl.allow_private_ips))
            }
            Err(_) => false,
        };
        self.dns_ok
            .insert(host.to_string(), (std::time::Instant::now(), ok));
        ok
    }

    fn take_outlinks(&self, links: impl IntoIterator<Item = String>, depth: u32, score: f32) {
        let cap = self.cfg.crawl.max_outlinks.max(1);
        let mut n = 0usize;
        for l in links {
            if n >= cap {
                break;
            }
            let Some(nu) = urlnorm::normalize(&l) else {
                continue;
            };
            if !self.accept_outlink(&nu) {
                continue;
            }
            self.frontier.push(FrontierItem {
                attempts: 0,
                url: nu.to_string(),
                depth,
                score,
            });
            n += 1;
        }
    }

    pub fn compact(&self) -> anyhow::Result<usize> {
        self.index.compact()
    }

    pub fn prune(
        &self,
        keep_docs: Option<u64>,
        max_bytes: Option<u64>,
        max_age_days: Option<u64>,
    ) -> anyhow::Result<usize> {
        let mut meta = self.store.iter_doc_meta()?;
        meta.sort_by_key(|d| d.1);
        let now = now_epoch();
        let mut drop: Vec<String> = Vec::new();
        if let Some(days) = max_age_days {
            let cutoff = now.saturating_sub(days.saturating_mul(86_400));
            for (url, fetched) in &meta {
                if *fetched < cutoff {
                    drop.push(url.clone());
                }
            }
        }
        if let Some(keep) = keep_docs {
            let keep = keep as usize;
            if meta.len() > keep {
                let extra = meta.len() - keep;
                for (url, _) in meta.iter().take(extra) {
                    if !drop.iter().any(|u| u == url) {
                        drop.push(url.clone());
                    }
                }
            }
        }
        for url in &drop {
            self.store.delete_doc(url)?;
            self.index.delete_url(url)?;
        }
        if !drop.is_empty() {
            self.index.commit()?;
        }
        let mut n = drop.len();
        if let Some(cap) = max_bytes {
            for _ in 0..4 {
                let bytes = self.data_bytes();
                if bytes <= cap {
                    break;
                }
                let mut rest = self.store.iter_doc_meta()?;
                if rest.is_empty() {
                    break;
                }
                let count = rest.len();
                let keep = ((cap as f64 / bytes as f64) * count as f64).floor() as usize;
                let keep = keep.max(1);
                if keep >= count {
                    break;
                }
                rest.sort_by_key(|d| d.1);
                for (url, _) in rest.iter().take(count - keep) {
                    self.store.delete_doc(url)?;
                    self.index.delete_url(url)?;
                    n += 1;
                }
                self.index.commit()?;
                let _ = self.index.compact();
            }
        }
        if n > 0 {
            let _ = self.index.compact();
        }
        Ok(n)
    }

    /// seed the frontier. seeds go through the live queue at score 1.0 so
    /// they lead the crawl; seeds already seen come back once stale, and
    /// persist_snapshot keeps the queue durable for restarts
    pub fn seed(&self, urls: &[String]) -> usize {
        self.seeds.lock().unwrap().extend(urls.iter().cloned());
        let recrawl_secs = self.cfg.crawl.recrawl_secs;
        let now = crate::index::now_epoch();
        let mut n = 0;
        for raw in urls {
            let Some(u) = urlnorm::normalize(raw) else {
                continue;
            };
            if quality::skip_url(&u) {
                continue;
            }
            let key = urlnorm::url_key(&u);
            let item = FrontierItem {
                attempts: 0,
                url: u.to_string(),
                depth: 0,
                score: 1.0,
            };
            if self.store.seen(key) {
                let stale = self
                    .store
                    .seen_at(key)
                    .map(|ts| now.saturating_sub(ts) >= recrawl_secs)
                    .unwrap_or(true);
                if recrawl_secs > 0 && stale && self.frontier.push_recrawl(item) {
                    n += 1;
                }
                continue;
            }
            if self.frontier.push(item) {
                n += 1;
            }
        }
        n
    }

    /// priority seeding: bypasses seen() so already-queued urls jump the
    /// line and already-fetched ones get an on-demand recrawl
    pub fn seed_priority(&self, urls: &[String]) -> usize {
        self.seeds.lock().unwrap().extend(urls.iter().cloned());
        let mut n = 0;
        for raw in urls {
            let Some(u) = urlnorm::normalize(raw) else {
                continue;
            };
            if quality::skip_url(&u) {
                continue;
            }
            let key = urlnorm::url_key(&u);
            let item = FrontierItem {
                attempts: 0,
                url: u.to_string(),
                depth: 0,
                score: 1.0,
            };
            let _ = self.store.mark_seen(key);
            let site = urlnorm::site_key(&urlnorm::host_of(&u));
            self.frontier.push_priority(site, key, item);
            n += 1;
        }
        n
    }

    /// load persisted simhashes so near-dup checks survive restarts
    fn warm_simhashes(&self) {
        match self.store.all_simhashes() {
            Ok(v) => {
                let mut w = self.content_sims.write().unwrap();
                w.extend(v);
            }
            Err(e) => warn!(%e, "simhash warmup failed"),
        }
    }

    /// requeue stored docs whose last crawl is older than recrawl_secs.
    /// seeds requeue via reseed_stale above; this covers discovered pages so
    /// the whole index gets refreshed, capped per pass so the frontier isn't
    /// swamped at once
    fn requeue_stale_docs(&self) -> usize {
        let recrawl_secs = self.cfg.crawl.recrawl_secs;
        if recrawl_secs == 0 {
            return 0;
        }
        let now = crate::index::now_epoch();
        let cap = self.cfg.frontier.max_pending.max(1000) / 4;
        let n = std::cell::Cell::new(0usize);
        let _ = self.store.for_each_doc(|d| {
            if n.get() >= cap {
                return;
            }
            let Ok(u) = url::Url::parse(&d.url) else {
                return;
            };
            let key = urlnorm::url_key(&u);
            let stale = self
                .store
                .seen_at(key)
                .map(|ts| now.saturating_sub(ts) >= recrawl_secs)
                .unwrap_or(true);
            if !stale || quality::skip_url(&u) {
                return;
            }
            if self.frontier.push_recrawl(FrontierItem {
                attempts: 0,
                url: d.url.clone(),
                depth: 0,
                score: 0.5,
            }) {
                n.set(n.get() + 1);
            }
        });
        let n = n.get();
        if n > 0 {
            info!(n, "stale stored docs requeued for recrawl");
        }
        n
    }

    /// requeue seeds whose last enqueue is older than recrawl_secs
    fn reseed_stale(&self) -> usize {
        let recrawl_secs = self.cfg.crawl.recrawl_secs;
        if recrawl_secs == 0 {
            return 0;
        }
        let now = crate::index::now_epoch();
        let mut n = 0;
        for s in self.seeds.lock().unwrap().iter() {
            let Some(url) = urlnorm::normalize(s) else {
                continue;
            };
            let key = urlnorm::url_key(&url);
            let stale = self
                .store
                .seen_at(key)
                .map(|ts| now.saturating_sub(ts) >= recrawl_secs)
                .unwrap_or(true);
            if stale
                && self.frontier.push_recrawl(FrontierItem {
                attempts: 0,
                url: url.to_string(),
                    depth: 0,
                    score: 1.0,
                })
            {
                n += 1;
            }
        }
        n
    }

    async fn finalize(&self) {
        self.frontier.flush();
        self.note_io(&self.index.commit());
        if self.hister.enabled() {
            let _ = self.hister.flush().await;
        }
    }

    /// one pass over the frontier until it drains
    async fn crawl_pass(self: &Arc<Self>) {
        let idle_guard = Arc::new(Semaphore::new(1));

        // periodic commit so an untimely kill does not lose indexed docs,
        // plus a frontier flush so queued urls survive a restart
        let idx = self.index.clone();
        let fr = self.frontier.clone();
        let hst = self.hister.clone();
        let io_bad = self.io_bad.clone();
        let maint = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;
                match idx.commit() {
                    Ok(_) => io_bad.store(0, Ordering::Relaxed),
                    Err(_) => {
                        io_bad.fetch_add(1, Ordering::Relaxed);
                    }
                }
                fr.persist_snapshot();
                let _ = hst.flush().await;
                // glibc keeps freed memory in per-thread arenas - return it
                // so RSS tracks real use instead of the peak watermark
                unsafe { libc::malloc_trim(0) };
            }
        });

        let mut handles = Vec::new();
        for wid in 0..self.cfg.crawl.workers {
            let this = self.clone();
            let idle = idle_guard.clone();
            handles.push(tokio::spawn(async move {
                this.worker(wid, idle).await;
            }));
        }
        for h in handles {
            let _ = h.await;
        }
        maint.abort();
    }

    /// batch mode: one pass then stop
    pub async fn run(self: Arc<Self>) {
        info!(workers = self.cfg.crawl.workers, "crawler starting");
        self.crawl_pass().await;
        self.finalize().await;
        info!("crawler stopped");
    }

    /// give a failed url another pass, up to the configured retry count
    fn retry_item(&self, mut item: FrontierItem) {
        if item.attempts < self.cfg.crawl.max_retries.min(255) as u8 {
            item.attempts += 1;
            let _ = self.frontier.push_recrawl(item);
        }
    }

    /// re-add every stored doc to tantivy - heals cases where a doc made it
    /// to the store but its index write failed under merge load. docs already
    /// indexed keep their authority and inlink scores
    pub fn reindex(&self) -> anyhow::Result<usize> {
        let n = std::cell::Cell::new(0usize);
        self.store.for_each_doc(|d| {
            if self.index.has_url(&d.url) {
                return;
            }
            let Ok(text) = zstd::decode_all(d.text_z.as_slice()) else {
                return;
            };
            let text = String::from_utf8_lossy(&text);
            if self
                .index
                .add_doc(&d.url, &d.host, &d.title, &text, "", d.fetched_at, 0.0, 0)
                .is_ok()
            {
                n.set(n.get() + 1);
                if n.get() % 500 == 0 {
                    let _ = self.index.commit();
                }
            }
        })?;
        let n = n.get();
        if n > 0 {
            self.index.commit()?;
        }
        Ok(n)
    }

    /// drop stored docs that fail the current skip filters so rule updates
    /// clean the index without waiting for a recrawl
    pub fn scrub_index(&self) -> usize {
        let mut drop_urls: Vec<String> = Vec::new();
        let _ = self.store.for_each_doc(|d| {
            let drop = Url::parse(&d.url)
                .map(|u| quality::skip_index_url(&u))
                .unwrap_or(true)
                || quality::junk_title(&d.title);
            if drop {
                drop_urls.push(d.url.clone());
            }
        });
        let mut n = 0;
        for d_url in &drop_urls {
            let _ = self.store.delete_doc(d_url);
            let _ = self.index.delete_url(d_url);
            n += 1;
        }
        if n > 0 {
            self.note_io(&self.index.commit());
            info!(n, "scrubbed junk docs from index");
        }
        n
    }

    /// continuous mode: after the frontier drains, keep checking for seeds
    /// that went stale and recrawl them
    pub async fn run_forever(self: Arc<Self>) {
        // clear anything the skip rules now reject, once per process start
        let c = self.clone();
        tokio::task::spawn_blocking(move || c.scrub_index());
        self.warm_simhashes();
        loop {
            info!(workers = self.cfg.crawl.workers, "crawl pass starting");
            self.crawl_pass().await;
            self.finalize().await;
            if self.reseed_stale() > 0 {
                continue;
            }
            if self.requeue_stale_docs() > 0 {
                continue;
            }
            let wait = self.cfg.crawl.recrawl_check_secs.max(60);
            info!(wait, "frontier drained, waiting for stale docs");
            tokio::time::sleep(Duration::from_secs(wait)).await;
        }
    }

    async fn worker(&self, _wid: usize, idle_guard: Arc<Semaphore>) {
        loop {
            if self.io_unhealthy() {
                // disk is erroring - back off and keep checking, the api
                // keeps serving whatever is already indexed
                tokio::time::sleep(Duration::from_secs(15)).await;
                continue;
            }
            if self.over_limit() {
                // throttle the notice - each worker hits this line on every
                // loop and a quiet index should not fill the disk with logs
                if !self.logged_limit.swap(true, Ordering::Relaxed) {
                    info!("index at size limit - crawler paused");
                }
                return;
            }
            self.logged_limit.store(false, Ordering::Relaxed);
            if self.frontier.is_idle() {
                // acquire means "i saw it idle"; if it still is, exit
                let _permit = idle_guard.try_acquire();
                tokio::time::sleep(Duration::from_secs(2)).await;
                if self.frontier.is_idle() {
                    return;
                }
                continue;
            }

            let Some((key, item, site)) = self.frontier.lease() else {
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            };

            if item.depth > self.cfg.crawl.max_depth {
                self.frontier.done(&site, true, 0, 0);
                continue;
            }

            let url_str = item.url.clone();
            let Some(url) = urlnorm::normalize(&url_str) else {
                self.frontier.done(&site, true, 0, 0);
                continue;
            };
            // sitemap xml paths are blocked by skip_url extensions, but they
            // are how robots tells us the inventory - let them through so a
            // fetched sitemap can fan out into real page urls
            let sitemap_url = {
                let p = url.path().to_ascii_lowercase();
                p.contains("sitemap") && (p.ends_with(".xml") || p.ends_with(".xml.gz"))
            };
            if !sitemap_url && quality::skip_url(&url) {
                self.frontier.done(&site, true, 0, 0);
                continue;
            }

            let host = urlnorm::host_of(&url);

            // blocklist + ssrf guard before dns
            if self.blocklist.blocked(&host) {
                debug!(host, "blocked host");
                self.frontier.done(&site, true, 0, 0);
                continue;
            }
            // refuse hosts that resolve to private/loopback space
            if !self.host_routable(&host).await {
                debug!(host, "refusing private/unroutable resolution");
                self.frontier.done(&site, true, 0, 0);
                continue;
            }
            if let Some(until) = self.store.host_banned_until(&site)
                && until > now_epoch()
            {
                self.frontier.penalize(&site, until - now_epoch());
                self.frontier.done(&site, true, 0, 0);
                continue;
            }

            // robots.txt
            let (allowed, delay_ms) = self.robots.check(&url).await;
            if !allowed {
                debug!(%url, "disallowed by robots.txt");
                self.frontier.done(&site, true, 0, 0);
                continue;
            }
            if self.cfg.crawl.max_crawl_delay_ms > 0 && delay_ms > self.cfg.crawl.max_crawl_delay_ms
            {
                debug!(%url, delay_ms, "robots crawl-delay too high, skipping host");
                self.frontier.done(&site, true, 0, 0);
                continue;
            }

            // first visit to a host: grab its advertised sitemaps so we
            // learn the url inventory instead of only link-following
            if self.sitemaps_seen.insert(site.clone()) {
                for sm in self.robots.sitemaps_for(&url).await {
                    if let Ok(su) = Url::parse(&sm) {
                        self.frontier.push(FrontierItem {
                attempts: 0,
                url: su.to_string(),
                            depth: item.depth.saturating_add(1),
                            score: item.score,
                        });
                    }
                }
            }

            // per-site crawl budget
            let site_pages = self.site_pages.get(&site).map(|v| *v.value()).unwrap_or(0);
            if site_pages >= self.cfg.crawl.max_pages_per_site {
                self.frontier.done(&site, true, delay_ms, 0);
                continue;
            }

            let prev = self.store.get_doc(url.as_str()).ok().flatten();
            let etag = prev.as_ref().map(|d| d.etag.as_str()).unwrap_or("");
            let last_mod = prev
                .as_ref()
                .map(|d| d.last_modified.as_str())
                .unwrap_or("");
            match self
                .fetcher
                .get_conditional(url.as_str(), etag, last_mod)
                .await
            {
                Ok(resp) => {
                    // redirects can cross hosts - re-check blocklist and
                    // routability on wherever we actually landed
                    let final_host = urlnorm::host_of(&resp.final_url);
                    if final_host != host
                        && (self.blocklist.blocked(&final_host)
                            || !self.host_routable(&final_host).await)
                    {
                        debug!(%url, %final_host, "redirect to blocked host, dropping");
                        self.frontier.done(&site, true, delay_ms, 0);
                        continue;
                    }
                    let body = resp.body;
                    let kind = ChallengeSolver::looks_challenged(&body, resp.status.as_u16());
                    let body = match kind {
                        ChallengeKind::Anubis => {
                            match self.solver.solve_anubis(&body, &resp.final_url).await {
                                Some(cleared) => cleared,
                                None => {
                                    self.frontier.done(&site, false, delay_ms, 0);
                                    continue;
                                }
                            }
                        }
                        ChallengeKind::Cloudflare => {
                            match self.solver.solve_flaresolverr(url.as_str()).await {
                                Some(cleared) => cleared,
                                None => {
                                    self.frontier.done(&site, false, delay_ms, 0);
                                    continue;
                                }
                            }
                        }
                        ChallengeKind::None => body,
                    };

                    // sitemaps are mined for <loc> urls, not indexed
                    if crate::extract::is_sitemap(&body) {
                        self.take_outlinks(
                            crate::extract::sitemap_urls(&body),
                            item.depth + 1,
                            item.score,
                        );
                        self.frontier.done(&site, true, delay_ms, resp.elapsed_ms);
                        continue;
                    }

                    // feeds are mined for links, not indexed
                    let feed_links = crate::extract::feed_links(&body);
                    if !feed_links.is_empty() {
                        self.take_outlinks(feed_links, item.depth + 1, item.score);
                        self.frontier.done(&site, true, delay_ms, resp.elapsed_ms);
                        continue;
                    }

                    let parsed = extract(&body, &resp.final_url);
                    if parsed.noindex {
                        self.frontier.done(&site, true, delay_ms, 0);
                        continue;
                    }
                    if quality::skip_index_url(&url) {
                        if !parsed.nofollow {
                            self.take_outlinks(parsed.links, item.depth + 1, item.score);
                        }
                        self.frontier.done(&site, true, delay_ms, resp.elapsed_ms);
                        continue;
                    }
                    let media = quality::is_media_host(&host);
                    let min_chars = if media {
                        24
                    } else {
                        self.cfg.index.min_text_chars
                    };
                    if !quality::is_indexable(&parsed.title, &parsed.text, min_chars) {
                        debug!(%url, "low quality page skipped");
                        self.frontier.done(&site, true, delay_ms, 0);
                        continue;
                    }
                    // maze check requires most links to stay on the same
                    // host. listy-but-legit pages like blogrolls and wiki
                    // indexes link mostly outward, so they do not trip it
                    let own = urlnorm::site_key(&host);
                    let own_links = parsed
                        .links
                        .iter()
                        .filter(|l| {
                            urlnorm::normalize(l)
                                .map(|nu| urlnorm::site_key(&urlnorm::host_of(&nu)) == own)
                                .unwrap_or(false)
                        })
                        .count();
                    let self_dominated =
                        !parsed.links.is_empty() && own_links * 10 >= parsed.links.len() * 7;
                    if !media
                        && !quality::is_wiki_host(&host)
                        && !quality::is_forum_url(&url)
                        && ((self_dominated
                            && quality::is_link_maze(parsed.text.len(), parsed.links.len()))
                            || quality::is_markov_babble(&parsed.text))
                    {
                        warn!(%url, links = parsed.links.len(), "possible content tarpit");
                        // keep only the cross-host links for discovery so a
                        // maze cannot reseed itself
                        let cross = parsed
                            .links
                            .iter()
                            .filter(|l| {
                                urlnorm::normalize(l)
                                    .map(|nu| urlnorm::site_key(&urlnorm::host_of(&nu)) != own)
                                    .unwrap_or(false)
                            })
                            .cloned();
                        self.take_outlinks(cross, item.depth + 1, item.score);
                        let fails = self.frontier.failures(&site);
                        if fails > 2 {
                            let _ = self.store.ban_host(&site, now_epoch() + 3600);
                        }
                        self.frontier.done(&site, false, delay_ms, 0);
                        continue;
                    }
                    if self.over_limit() {
                        info!("index at size limit");
                        self.frontier.done(&site, true, delay_ms, 0);
                        return;
                    }

                    // inlink-based authority, cheap pagerank-lite
                    for l in &parsed.links {
                        if let Some(nu) = urlnorm::normalize(l) {
                            *self.authority.entry(urlnorm::host_of(&nu)).or_insert(0) += 1;
                        }
                    }

                    // near-dup content check - simhash over token 3-grams;
                    // hamming <= 3 against any stored doc means mirror/parked
                    // content or a tag-page variant, skip indexing
                    let norm: String = parsed
                        .text
                        .split_whitespace()
                        .collect::<Vec<_>>()
                        .join(" ")
                        .to_lowercase();
                    let sim = simhash64(&norm);
                    {
                        let seen = self.content_sims.read().unwrap();
                        if seen.iter().any(|h| (h ^ sim).count_ones() <= 3) {
                            debug!(%url, "near-duplicate content skipped");
                            self.frontier.done(&site, true, delay_ms, 0);
                            continue;
                        }
                    }
                    self.content_sims.write().unwrap().push(sim);

                    let title = quality::display_title(&parsed.title, &parsed.text);
                    let text = quality::clip_text(&parsed.text, self.cfg.index.index_text_bytes);
                    let host_auth =
                        self.authority.get(&host).map(|v| *v.value()).unwrap_or(0) as f64;
                    let authority = (1.0f64 + host_auth).ln();
                    *self.site_pages.entry(site.clone()).or_insert(0) += 1;

                    let mut index_url = resp.final_url.to_string();
                    if !parsed.canonical.is_empty()
                        && let Some(cu) = urlnorm::normalize(&parsed.canonical)
                        && urlnorm::host_of(&cu) == host
                    {
                        index_url = cu.to_string();
                    }
                    if self.store.get_doc(&index_url).ok().flatten().is_some()
                        && index_url != resp.final_url.as_str()
                    {
                        debug!(%index_url, "canonical already indexed");
                        self.frontier.done(&site, true, delay_ms, resp.elapsed_ms);
                        continue;
                    }

                    let doc = DocRecord {
                        url: index_url.clone(),
                        title: title.clone(),
                        host: host.clone(),
                        fetched_at: now_epoch(),
                        text_z: zstd::encode_all(text.as_bytes(), 3).unwrap_or_default(),
                        links: parsed.links.clone(),
                        etag: resp.etag.clone(),
                        last_modified: resp.last_modified.clone(),
                        simhash: sim,
                    };
                    let _ = self.store.put_doc(&doc);
                    if self.hister.enabled() {
                        self.hister
                            .queue(&doc.url, &host, &doc.title, &text, doc.fetched_at);
                    }
                    let add = self.index.add_doc(
                        &doc.url,
                        &host,
                        &title,
                        &text,
                        &parsed.description,
                        doc.fetched_at,
                        authority,
                        parsed.links.len() as u64,
                    );
                    self.note_io(&add);

                    let n = self.docs_done.fetch_add(1, Ordering::Relaxed) + 1;
                    if n.is_multiple_of(self.cfg.index.commit_every_docs as u64) {
                        self.note_io(&self.index.commit());
                    }

                    // enqueue outlinks within depth
                    let depth = item.depth + 1;
                    if depth <= self.cfg.crawl.max_depth && !self.over_limit() {
                        self.take_outlinks(parsed.links, depth, item.score * 0.9);
                    }
                    let _ = key;
                    self.frontier.done(&site, true, delay_ms, resp.elapsed_ms);
                }
                Err(FetchError::NotModified) => {
                    debug!(%url, "not modified");
                    self.frontier.done(&site, true, delay_ms, 0);
                }
                Err(e) => match e.http_code() {
                    Some(403) | Some(503) => {
                        debug!(%url, error = %e, "attempting challenge solvers");
                        if self.solver.solve_flaresolverr(url.as_str()).await.is_none() {
                            self.frontier.done(&site, false, delay_ms, 0);
                            continue;
                        }
                        self.frontier.done(&site, true, delay_ms, 0);
                    }
                    Some(429) => {
                        let wait_s = e
                            .retry_after_ms()
                            .map(|ms| (ms / 1000).max(1))
                            .unwrap_or(60);
                        warn!(host, wait_s, "429 - backing off");
                        self.frontier.done(&site, false, delay_ms, 0);
                        self.frontier.penalize(&site, wait_s);
                    }
                    _ => match &e {
                        // not html (pdf, binary) is a neutral skip, not a
                        // host failure - one pdf must not cost a strike
                        FetchError::NotHtml => {
                            self.frontier.done(&site, true, delay_ms, 0);
                        }
                        FetchError::SlowDrip | FetchError::Timeout => {
                            warn!(host, error = %e, "possible tarpit");
                            let fails = self.frontier.failures(&site);
                            if fails > 3 {
                                let _ = self.store.ban_host(&site, now_epoch() + 3600);
                            }
                            self.retry_item(item);
                            self.frontier.done(&site, false, delay_ms, 0);
                        }
                        _ => {
                            debug!(%url, error = %e, "fetch failed");
                            self.retry_item(item);
                            self.frontier.done(&site, false, delay_ms, 0);
                        }
                    },
                },
            }
        }
    }
}

impl Crawler {
    /// dump the doc store as jsonl - bridge format for hister and friends
    pub fn export_jsonl(&self, out: &std::path::Path) -> anyhow::Result<usize> {
        use std::io::BufWriter;
        let mut n = 0usize;
        let mut w = BufWriter::new(std::fs::File::create(out)?);
        self.store.for_each_doc(|doc| {
            let text = zstd::decode_all(doc.text_z.as_slice()).unwrap_or_default();
            let line = serde_json::json!({
                "url": doc.url,
                "title": doc.title,
                "host": doc.host,
                "fetched_at": doc.fetched_at,
                "text": String::from_utf8_lossy(&text),
            });
            let _ = serde_json::to_writer(&mut w, &line);
            let _ = std::io::Write::write_all(&mut w, b"\n");
            n += 1;
        })?;
        std::io::Write::flush(&mut w)?;
        Ok(n)
    }

    /// hister export layout: a json array where every document sits on one
    /// line starting with { and commas live on their own lines, matching what
    /// `hister import file` parses
    pub fn export_hister(&self, out: &std::path::Path) -> anyhow::Result<usize> {
        use std::io::Write;
        let mut n = 0usize;
        let mut w = std::io::BufWriter::new(std::fs::File::create(out)?);
        writeln!(w, "[")?;
        self.store.for_each_doc(|doc| {
            let text = zstd::decode_all(doc.text_z.as_slice()).unwrap_or_default();
            let line = serde_json::json!({
                "url": doc.url,
                "domain": doc.host,
                "title": doc.title,
                "text": String::from_utf8_lossy(&text),
                "added": doc.fetched_at,
                "updated": doc.fetched_at,
                "type": 0,
                "label": "lyra-index",
            });
            if n > 0 {
                let _ = writeln!(w, ",");
            }
            let _ = serde_json::to_writer(&mut w, &line);
            let _ = writeln!(w);
            n += 1;
        })?;
        writeln!(w, "]")?;
        w.flush()?;
        Ok(n)
    }

    /// import jsonl docs (url/title/host/fetched_at/text) into store+index.
    /// also accepts hister export files - non-{ lines are skipped and the
    /// hister field names domain/text/added map to ours
    pub fn import_jsonl(&self, input: &std::path::Path) -> anyhow::Result<usize> {
        use std::io::BufRead;
        let mut n = 0usize;
        for line in std::io::BufReader::new(std::fs::File::open(input)?).lines() {
            let line = line?;
            let t = line.trim();
            if !t.starts_with('{') {
                continue;
            }
            let v: serde_json::Value = serde_json::from_str(t)?;
            let url = v["url"].as_str().unwrap_or("");
            let title = v["title"].as_str().unwrap_or("");
            let host = v["host"].as_str().or(v["domain"].as_str()).unwrap_or("");
            let fetched = v["fetched_at"]
                .as_u64()
                .or(v["added"].as_u64())
                .or(v["updated"].as_u64())
                .unwrap_or_else(crate::index::now_epoch);
            let text = v["text"].as_str().unwrap_or("");
            if url.is_empty() || !url.starts_with("http") {
                continue;
            }
            let desc = text.get(..160).unwrap_or(text);
            let doc = crate::store::DocRecord {
                url: url.into(),
                title: title.into(),
                host: host.into(),
                fetched_at: fetched,
                text_z: zstd::encode_all(text.as_bytes(), 3)?,
                links: vec![],
                simhash: crate::crawl::simhash64_pub(text),
                ..Default::default()
            };
            self.store.put_doc(&doc)?;
            self.index
                .add_doc(url, host, title, text, desc, fetched, 0.0, 0)?;
            n += 1;
        }
        Ok(n)
    }
}

/// 64-bit simhash over word 3-grams of normalized text. two pages that share
/// most tokens hash within a few bits, so hamming distance is a cheap fuzzy
/// duplicate test
pub(crate) fn simhash64_pub(text: &str) -> u64 {
    simhash64(text)
}

fn simhash64(text: &str) -> u64 {
    let words: Vec<&str> = text.split_whitespace().collect();
    if words.is_empty() {
        return 0;
    }
    let mut bits = [0i64; 64];
    let grams: Box<dyn Iterator<Item = String>> = if words.len() < 3 {
        Box::new(words.iter().map(|w| w.to_string()))
    } else {
        Box::new(
            words
                .windows(3)
                .map(|w| format!("{} {} {}", w[0], w[1], w[2])),
        )
    };
    for g in grams {
        let h = xxhash_rust::xxh3::xxh3_64(g.as_bytes());
        for i in 0..64 {
            if h & (1u64 << i) != 0 {
                bits[i] += 1;
            } else {
                bits[i] -= 1;
            }
        }
    }
    let mut out = 0u64;
    for (i, &b) in bits.iter().enumerate() {
        if b > 0 {
            out |= 1u64 << i;
        }
    }
    out
}
