use dashmap::DashMap;
use reqwest::Client;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, warn};
use url::Url;

const ROBOTS_TTL: Duration = Duration::from_secs(24 * 3600);
const ROBOTS_FETCH_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct RobotsEntry {
    pub text: String,
    pub fetched_at: Instant,
    /// fetch failed hard (5xx or network error) -> be conservative, disallow
    pub deny_all: bool,
    /// robots unreachable with 404/401/403 -> allow everything
    pub allow_all: bool,
}

pub struct RobotsCache {
    cache: DashMap<String, Arc<RobotsEntry>>,
    client: Client,
    ua: String,
}

impl RobotsCache {
    pub fn new(client: Client, ua: String) -> Self {
        Self {
            cache: DashMap::new(),
            client,
            ua,
        }
    }

    /// Returns (allowed, crawl_delay_ms). Never panics; conservative on
    /// hard errors per RFC 9309 (unreachable robots.txt on 5xx means the
    /// origin wants a full disallow).
    pub async fn check(&self, url: &Url) -> (bool, u64) {
        let origin = url.origin().ascii_serialization();
        let entry = self.entry(url).await;

        if entry.deny_all {
            return (false, 0);
        }
        if entry.allow_all || entry.text.is_empty() {
            return (true, 0);
        }
        let _ = origin;
        let mut matcher = robotstxt::DefaultMatcher::default();
        let agents_owned = self.agents();
        let agents: Vec<&str> = agents_owned.iter().map(|s| s.as_str()).collect();
        let allowed = matcher.allowed_by_robots(&entry.text, agents, url.as_str());
        let delay = self.crawl_delay_ms(&entry.text);
        (allowed, delay)
    }

    fn agents(&self) -> Vec<String> {
        // match our own UA plus the wildcard group
        vec!["LyraIndex".to_string(), "*".to_string()]
    }

    fn crawl_delay_ms(&self, text: &str) -> u64 {
        // robotstxt does not expose crawl-delay; parse it ourselves.
        // duplicate user-agent groups merge, so scan the whole file and keep
        // the last delay that applies to us.
        let mut applies = false;
        let mut delay_ms = 0u64;
        for line in text.lines() {
            let line = line.split('#').next().unwrap_or("").trim();
            if line.is_empty() {
                continue;
            }
            if let Some(rest) = line.to_lowercase().strip_prefix("user-agent:") {
                let agent = rest.trim();
                applies = agent == "*" || self.ua.to_lowercase().contains(&agent.to_lowercase());
                continue;
            }
            if applies
                && let Some(rest) = line.to_lowercase().strip_prefix("crawl-delay:")
                && let Ok(secs) = rest.trim().parse::<f64>()
            {
                delay_ms = (secs * 1000.0) as u64;
            }
        }
        delay_ms
    }

    async fn entry(&self, url: &Url) -> Arc<RobotsEntry> {
        let origin = url.origin().ascii_serialization();
        if let Some(e) = self.cache.get(&origin)
            && e.fetched_at.elapsed() < ROBOTS_TTL
        {
            return e.clone();
        }
        let e = self.fetch(&origin).await;
        let e = Arc::new(e);
        self.cache.insert(origin.clone(), e.clone());
        let _ = origin;
        e
    }

    /// sitemap urls advertised by a host's robots.txt - fetched lazily by
    /// callers since entry() may hit the cache
    pub async fn sitemaps_for(&self, url: &Url) -> Vec<String> {
        let e = self.entry(url).await;
        self.sitemaps(&e.text)
    }

    async fn fetch(&self, origin: &str) -> RobotsEntry {
        let robots_url = format!("{origin}/robots.txt");
        let res =
            tokio::time::timeout(ROBOTS_FETCH_TIMEOUT, self.client.get(&robots_url).send()).await;

        match res {
            Ok(Ok(resp)) => {
                let status = resp.status();
                if status.as_u16() == 404 || status.as_u16() == 401 || status.as_u16() == 403 {
                    debug!(origin, "robots.txt absent -> allow all");
                    RobotsEntry {
                        text: String::new(),
                        fetched_at: Instant::now(),
                        deny_all: false,
                        allow_all: true,
                    }
                } else if status.is_success() {
                    let text = resp.text().await.unwrap_or_default();
                    // cap parse size, giant robots.txt files exist in the wild
                    let text: String = text.chars().take(512 * 1024).collect();
                    RobotsEntry {
                        text,
                        fetched_at: Instant::now(),
                        deny_all: false,
                        allow_all: false,
                    }
                } else if status.is_server_error() {
                    warn!(origin, status = %status, "robots.txt 5xx -> disallow all");
                    RobotsEntry {
                        text: String::new(),
                        fetched_at: Instant::now(),
                        deny_all: true,
                        allow_all: false,
                    }
                } else {
                    RobotsEntry {
                        text: String::new(),
                        fetched_at: Instant::now(),
                        deny_all: false,
                        allow_all: true,
                    }
                }
            }
            Ok(Err(e)) => {
                warn!(origin, error = %e, "robots.txt fetch failed -> disallow");
                RobotsEntry {
                    text: String::new(),
                    fetched_at: Instant::now(),
                    deny_all: true,
                    allow_all: false,
                }
            }
            Err(_) => {
                warn!(origin, "robots.txt fetch timed out -> disallow");
                RobotsEntry {
                    text: String::new(),
                    fetched_at: Instant::now(),
                    deny_all: true,
                    allow_all: false,
                }
            }
        }
    }

    /// extract sitemap urls listed in robots.txt for seeding
    pub fn sitemaps(&self, text: &str) -> Vec<String> {
        text.lines()
            .filter_map(|l| {
                l.strip_prefix("Sitemap:")
                    .or_else(|| l.strip_prefix("sitemap:"))
            })
            .map(|s| s.trim().to_string())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crawl_delay_parse() {
        let c = RobotsCache::new(Client::new(), "LyraIndex/0.1".into());
        let text = "User-agent: *\nCrawl-delay: 5\n\nUser-agent: BingBot\nCrawl-delay: 1\n";
        assert_eq!(c.crawl_delay_ms(text), 5000);
    }
}
