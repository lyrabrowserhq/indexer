use crate::config::Crawl;
use crate::store::{FrontierItem, Store};
use crate::urlnorm;
use dashmap::DashMap;
use rand::RngExt;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{debug, warn};
use url::Url;

/// per-site politeness bucket: enforces crawl-delay + in-flight limit
struct SiteState {
    next_allowed: Instant,
    failures: u32,
    in_flight: bool,
}

/// politeness-first frontier: urls grouped by site key, a site releases its
/// next url only when its delay has elapsed and nothing is in flight
pub struct Frontier {
    queues: DashMap<String, VecDeque<(u64, FrontierItem)>>,
    /// sites sorted by next-allowed instant - heap so lease() is O(log n)
    /// instead of scanning every site on each request. entries are hints:
    /// stale rows are dropped on pop and sites re-arm themselves in done()
    ready: std::sync::Mutex<BinaryHeap<Reverse<(std::time::Instant, u32, String)>>>,
    /// url keys waiting in queues so persisted snapshots refilled from redb
    /// do not double-lease urls already in memory
    queued: dashmap::DashSet<u64>,
    sites: DashMap<String, SiteState>,
    store: Arc<Store>,
    pending: AtomicUsize,
    /// jump-the-line lane: urls pushed by /v1/seed with priority=true are
    /// leased before everything else. still respects per-site politeness,
    /// so a prio item for a busy site waits its turn for that site
    prio: Mutex<VecDeque<(u64, FrontierItem, String)>>,
    /// in-memory queue cap - overflow goes to the disk frontier so the heap
    /// stays bounded no matter how wide the crawl gets
    max_pending: usize,
    min_delay_ms: u64,
    max_delay_ms: u64,
    delay_factor: f64,
    jitter_ms: u64,
}

impl Frontier {
    pub fn new(store: Arc<Store>, crawl: &Crawl, fcfg: &crate::config::FrontierCfg) -> Self {
        Self {
            queues: DashMap::new(),
            ready: std::sync::Mutex::new(BinaryHeap::new()),
            prio: Mutex::new(VecDeque::new()),
            queued: dashmap::DashSet::new(),
            sites: DashMap::new(),
            store,
            pending: AtomicUsize::new(0),
            max_pending: fcfg.max_pending.max(1000),
            min_delay_ms: crawl.min_delay_ms,
            max_delay_ms: crawl.max_delay_ms.max(crawl.min_delay_ms),
            delay_factor: crawl.delay_factor.max(0.0),
            jitter_ms: crawl.jitter_ms,
        }
    }

    /// add a url after normalization, dedup and blocklist check happen in
    /// the caller; returns true when enqueued
    pub fn push(&self, item: FrontierItem) -> bool {
        let Ok(url) = Url::parse(&item.url) else {
            return false;
        };
        let site = urlnorm::site_key(&urlnorm::host_of(&url));
        let key = urlnorm::url_key(&url);
        // the in-memory queued set rejects same-run dupes before touching
        // the store, which is one read txn plus one write txn saved per
        // duplicate outlink
        if self.queued.contains(&key) || self.store.seen(key) {
            return false;
        }
        let _ = self.store.mark_seen(key);
        if self.pending.load(Ordering::Relaxed) >= self.max_pending {
            // heap full - park on disk, refill() pulls it back later
            return self.store.push_frontier(&item, key).is_ok();
        }
        if self.insert_queued(site, key, item) {
            self.pending.fetch_add(1, Ordering::Relaxed);
            return true;
        }
        false
    }

    /// requeue a url that is already seen, for the periodic recrawl pass.
    /// refreshes the seen timestamp so the next staleness check resets
    pub fn push_recrawl(&self, item: FrontierItem) -> bool {
        let Ok(url) = Url::parse(&item.url) else {
            return false;
        };
        let site = urlnorm::site_key(&urlnorm::host_of(&url));
        let key = urlnorm::url_key(&url);
        let _ = self.store.mark_seen(key);
        if self.pending.load(Ordering::Relaxed) >= self.max_pending {
            return self.store.push_frontier(&item, key).is_ok();
        }
        if self.insert_queued(site, key, item) {
            self.pending.fetch_add(1, Ordering::Relaxed);
            return true;
        }
        false
    }

    /// returns false when the key is already waiting in some queue so
    /// refilled snapshots cannot double-lease a url
    fn insert_queued(&self, site: String, key: u64, item: FrontierItem) -> bool {
        if !self.queued.insert(key) {
            return false;
        }
        let mut q = self.queues.entry(site.clone()).or_default();
        let was_empty = q.is_empty();
        let pos = q
            .iter()
            .position(|(_, it)| it.depth > item.depth)
            .unwrap_or(q.len());
        let depth = item.depth;
        q.insert(pos, (key, item));
        drop(q);
        if was_empty {
            // site just became non-empty - arm it at its politeness deadline
            let at = self
                .sites
                .get(&site)
                .map(|s| s.next_allowed)
                .unwrap_or_else(std::time::Instant::now);
            self.ready.lock().unwrap().push(Reverse((at, depth, site)));
        }
        true
    }

    /// queue a url at the head of the line - used by the priority seed api.
    /// unlike push() this does not consult seen() so it can pull forward a
    /// url that is already parked in the disk backlog or requeue a fetched
    /// page as an on-demand recrawl. the dedupe on fetch in the worker still
    /// prevents double-indexing within a burst
    pub fn push_priority(&self, site: String, key: u64, item: FrontierItem) {
        self.prio.lock().unwrap().push_back((key, item, site));
        self.pending.fetch_add(1, Ordering::Relaxed);
    }

    /// how many priority items are waiting
    pub fn prio_len(&self) -> usize {
        self.prio.lock().unwrap().len()
    }

    /// drain the priority lane before the normal path. blocked sites (in
    /// flight or inside their politeness window) are rotated to the back so
    /// one slow site cannot head-of-line block the lane
    fn lease_prio(&self) -> Option<(u64, FrontierItem, String)> {
        let now = Instant::now();
        let mut dq = self.prio.lock().unwrap();
        let tries = dq.len().min(16);
        for _ in 0..tries {
            let Some((key, item, site)) = dq.pop_front() else {
                break;
            };
            let blocked = self
                .sites
                .get(&site)
                .is_some_and(|s| s.in_flight || s.next_allowed > now);
            if blocked {
                dq.push_back((key, item, site));
                continue;
            }
            drop(dq);
            self.queued.remove(&key);
            let _ = self.store.remove_frontier(key);
            let mut state = self.sites.entry(site.clone()).or_insert_with(|| SiteState {
                next_allowed: Instant::now(),
                failures: 0,
                in_flight: false,
            });
            state.in_flight = true;
            self.pending.fetch_sub(1, Ordering::Relaxed);
            return Some((key, item, site));
        }
        None
    }

    /// lease the next url ready under politeness rules, preferring shallower
    /// depths so seeds are fetched before citation outlinks
    pub fn lease(&self) -> Option<(u64, FrontierItem, String)> {
        if let Some(hit) = self.lease_prio() {
            return Some(hit);
        }
        self.lease_ready().or_else(|| {
            self.refill();
            if let Some(hit) = self.lease_prio() {
                return Some(hit);
            }
            self.lease_ready()
        })
    }

    fn lease_ready(&self) -> Option<(u64, FrontierItem, String)> {
        let now = Instant::now();
        // pop heap entries until one is actually leasable. stale rows (site
        // already in-flight or drained) get dropped; not-yet-ready rows go
        // back with the correct deadline so the heap stays honest
        let mut defer: Vec<Reverse<(std::time::Instant, u32, String)>> = Vec::new();
        let mut chosen: Option<String> = None;
        {
            let mut heap = self.ready.lock().unwrap();
            while let Some(Reverse((at, _d, _s))) = heap.peek() {
                let at = *at;
                if at > now {
                    break;
                }
                let Reverse((_at, depth, s)) = heap.pop().unwrap();
                if self.queues.get(&s).is_none_or(|q| q.is_empty()) {
                    continue;
                }
                match self.sites.get(&s) {
                    None => {
                        chosen = Some(s);
                        break;
                    }
                    Some(st) if st.in_flight => continue,
                    Some(st) if st.next_allowed > now => {
                        defer.push(Reverse((st.next_allowed, depth, s)));
                        continue;
                    }
                    _ => {
                        chosen = Some(s);
                        break;
                    }
                }
            }
            for d in defer {
                heap.push(d);
            }
        }
        chosen.and_then(|s| self.take_front(&s))
    }

    /// pop the front of a site's queue and mark it in-flight - called by
    /// lease_ready once the heap says the site is due
    fn take_front(&self, site: &str) -> Option<(u64, FrontierItem, String)> {
        let mut q = self.queues.get_mut(site)?;
        let (key, item) = q.pop_front()?;
        drop(q);
        self.queued.remove(&key);
        let _ = self.store.remove_frontier(key);
        let mut state = self
            .sites
            .entry(site.to_string())
            .or_insert_with(|| SiteState {
                next_allowed: Instant::now(),
                failures: 0,
                in_flight: false,
            });
        state.in_flight = true;
        self.pending.fetch_sub(1, Ordering::Relaxed);
        Some((key, item, site.to_string()))
    }

    fn refill(&self) {
        if self.pending.load(Ordering::Relaxed) >= self.max_pending {
            return;
        }
        match self.store.pop_frontier(256) {
            Ok(items) => {
                for (key, item) in items {
                    if self.pending.load(Ordering::Relaxed) >= self.max_pending {
                        // put the rest back rather than blowing the cap
                        let _ = self.store.push_frontier(&item, key);
                        continue;
                    }
                    let Ok(url) = Url::parse(&item.url) else {
                        continue;
                    };
                    let site = urlnorm::site_key(&urlnorm::host_of(&url));
                    if self.insert_queued(site, key, item) {
                        self.pending.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            Err(e) => warn!(error = %e, "frontier refill failed"),
        }
    }

    /// mark site done - schedule next allowed fetch using robots delay,
    /// last fetch latency, and a small jitter so hosts are not hit in lockstep
    pub fn done(&self, site: &str, ok: bool, robots_delay_ms: u64, latency_ms: u64) {
        let mut state = match self.sites.get_mut(site) {
            Some(s) => s,
            None => return,
        };
        state.in_flight = false;
        if ok {
            state.failures = 0;
        } else {
            state.failures = state.failures.saturating_add(1);
        }
        let wait = polite_wait_ms(
            self.min_delay_ms,
            self.max_delay_ms,
            self.delay_factor,
            robots_delay_ms,
            latency_ms,
            self.jitter_ms,
            state.failures,
        );
        let at = Instant::now() + Duration::from_millis(wait);
        state.next_allowed = at;
        let has_more = self.queues.get(site).is_some_and(|q| !q.is_empty());
        drop(state);
        if has_more {
            let depth = self
                .queues
                .get(site)
                .and_then(|q| q.front().map(|(_, it)| it.depth))
                .unwrap_or(0);
            self.ready
                .lock()
                .unwrap()
                .push(Reverse((at, depth, site.to_string())));
        }
    }

    /// consecutive failures beyond this mean the site is effectively banned
    /// for a while - backoff handled via done() plus a long cooldown
    pub fn failures(&self, site: &str) -> u32 {
        self.sites.get(site).map(|s| s.failures).unwrap_or(0)
    }

    pub fn penalize(&self, site: &str, secs: u64) {
        if let Some(mut s) = self.sites.get_mut(site) {
            s.next_allowed = Instant::now() + Duration::from_secs(secs);
        }
    }

    pub fn pending(&self) -> usize {
        self.pending.load(Ordering::Relaxed)
    }

    pub fn is_idle(&self) -> bool {
        self.pending() == 0 && self.store.frontier_len() == 0 && !self.any_in_flight()
    }

    /// sites currently mid-fetch, for stats
    pub fn in_flight(&self) -> usize {
        self.sites.iter().filter(|s| s.in_flight).count()
    }

    /// sites queued, for stats
    pub fn sites_queued(&self) -> usize {
        self.queues.len()
    }

    /// persisted backlog length, for stats
    pub fn disk_backlog(&self) -> u64 {
        self.store.frontier_len()
    }

    fn any_in_flight(&self) -> bool {
        self.sites.iter().any(|s| s.in_flight)
    }

    /// persist all queued items to redb (call on shutdown)
    pub fn flush(&self) {
        let mut n = 0usize;
        for mut entry in self.queues.iter_mut() {
            while let Some((key, item)) = entry.pop_front() {
                self.queued.remove(&key);
                if let Err(e) = self.store.push_frontier(&item, key) {
                    warn!(error = %e, "frontier flush failed");
                }
                n += 1;
            }
        }
        debug!(n, "flushed frontier to store");
    }

    /// snapshot queued items to redb without draining - a crash then resumes
    /// from the persisted frontier, duplicates are deduped by the seen set
    /// copy queued items into the persisted frontier in one transaction
    /// instead of one commit per row
    pub fn persist_snapshot(&self) {
        let mut items: Vec<(u64, FrontierItem)> = Vec::new();
        for entry in self.queues.iter() {
            for (key, item) in entry.iter() {
                items.push((*key, item.clone()));
            }
        }
        if let Err(e) = self.store.push_frontier_many(&items) {
            warn!(error = %e, "frontier snapshot failed");
        }
    }
}

/// per-host wait: robots Crawl-delay, a multiple of last fetch latency, and
/// exponential backoff on errors. capped so a hostile crawl-delay cannot stall
/// the worker
pub fn polite_wait_ms(
    min_delay_ms: u64,
    max_delay_ms: u64,
    delay_factor: f64,
    robots_delay_ms: u64,
    latency_ms: u64,
    jitter_ms: u64,
    failures: u32,
) -> u64 {
    let adaptive = (latency_ms as f64 * delay_factor.max(0.0)) as u64;
    let mut wait = min_delay_ms.max(robots_delay_ms).max(adaptive);
    if failures > 0 {
        let mult = 1u64 << failures.min(6);
        wait = wait.saturating_mul(mult);
    }
    let cap = max_delay_ms.max(min_delay_ms);
    wait = wait.min(cap);
    if jitter_ms > 0 {
        wait = wait.saturating_add(rand::rng().random_range(0..=jitter_ms));
    }
    wait
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adaptive_delay_tracks_latency() {
        let w = polite_wait_ms(100, 10_000, 2.0, 0, 400, 0, 0);
        assert_eq!(w, 800);
    }

    #[test]
    fn robots_delay_is_a_floor() {
        let w = polite_wait_ms(100, 10_000, 2.0, 2000, 50, 0, 0);
        assert_eq!(w, 2000);
    }

    #[test]
    fn delay_is_capped() {
        let w = polite_wait_ms(100, 1000, 2.0, 50_000, 9000, 0, 0);
        assert_eq!(w, 1000);
    }

    #[test]
    fn failures_backoff() {
        let w = polite_wait_ms(100, 10_000, 1.0, 0, 0, 0, 3);
        assert_eq!(w, 800);
    }
}
