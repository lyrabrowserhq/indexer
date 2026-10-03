use crate::config::Crawl;
use crate::store::{FrontierItem, Store};
use crate::urlnorm;
use dashmap::DashMap;
use rand::RngExt;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
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
    sites: DashMap<String, SiteState>,
    store: Arc<Store>,
    pending: AtomicUsize,
    min_delay_ms: u64,
    max_delay_ms: u64,
    delay_factor: f64,
    jitter_ms: u64,
}

impl Frontier {
    pub fn new(store: Arc<Store>, crawl: &Crawl) -> Self {
        Self {
            queues: DashMap::new(),
            sites: DashMap::new(),
            store,
            pending: AtomicUsize::new(0),
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
        if self.store.seen(key) {
            return false;
        }
        let _ = self.store.mark_seen(key);
        self.insert_queued(site, key, item);
        self.pending.fetch_add(1, Ordering::Relaxed);
        true
    }

    fn insert_queued(&self, site: String, key: u64, item: FrontierItem) {
        let mut q = self.queues.entry(site).or_default();
        let pos = q
            .iter()
            .position(|(_, it)| it.depth > item.depth)
            .unwrap_or(q.len());
        q.insert(pos, (key, item));
    }

    /// lease the next url ready under politeness rules, preferring shallower
    /// depths so seeds are fetched before citation outlinks
    pub fn lease(&self) -> Option<(u64, FrontierItem, String)> {
        self.lease_ready().or_else(|| {
            self.refill();
            self.lease_ready()
        })
    }

    fn lease_ready(&self) -> Option<(u64, FrontierItem, String)> {
        let now = Instant::now();
        let mut best: Option<(String, u32)> = None;
        for entry in self.queues.iter() {
            let site = entry.key().clone();
            let Some((_, item)) = entry.front() else {
                continue;
            };
            let depth = item.depth;
            let blocked = self
                .sites
                .get(&site)
                .is_some_and(|s| s.in_flight || s.next_allowed > now);
            if blocked {
                continue;
            }
            match &best {
                None => best = Some((site, depth)),
                Some((_, d)) if depth < *d => best = Some((site, depth)),
                _ => {}
            }
        }
        let (site, _) = best?;
        let mut q = self.queues.get_mut(&site)?;
        let mut state = self.sites.entry(site.clone()).or_insert_with(|| SiteState {
            next_allowed: Instant::now(),
            failures: 0,
            in_flight: false,
        });
        if state.in_flight || state.next_allowed > now {
            return None;
        }
        let (key, item) = q.pop_front()?;
        state.in_flight = true;
        self.pending.fetch_sub(1, Ordering::Relaxed);
        Some((key, item, site))
    }

    fn refill(&self) {
        match self.store.pop_frontier(256) {
            Ok(items) => {
                for (key, item) in items {
                    let Ok(url) = Url::parse(&item.url) else {
                        continue;
                    };
                    let site = urlnorm::site_key(&urlnorm::host_of(&url));
                    self.insert_queued(site, key, item);
                    self.pending.fetch_add(1, Ordering::Relaxed);
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
        state.next_allowed = Instant::now() + Duration::from_millis(wait);
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

    fn any_in_flight(&self) -> bool {
        self.sites.iter().any(|s| s.in_flight)
    }

    /// persist all queued items to redb (call on shutdown)
    pub fn flush(&self) {
        let mut n = 0usize;
        for mut entry in self.queues.iter_mut() {
            while let Some((key, item)) = entry.pop_front() {
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
    pub fn persist_snapshot(&self) {
        for entry in self.queues.iter() {
            for (key, item) in entry.iter() {
                let _ = self.store.push_frontier(item, *key);
            }
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
