use lyra_index::*;
use std::path::Path;

pub fn test_config(dir: &Path) -> Config {
    let mut cfg = Config::default();
    cfg.crawl.allow_private_ips = true;
    cfg.crawl.min_delay_ms = 10;
    cfg.crawl.jitter_ms = 0;
    cfg.crawl.max_delay_ms = 1000;
    cfg.crawl.workers = 4;
    cfg.crawl.fetch_timeout_secs = 10;
    cfg.crawl.max_depth = 3;
    cfg.index.writer_heap_mb = 32;
    cfg.index.min_text_chars = 8;
    cfg.store.path = dir.join("data");
    cfg.api.listen = "127.0.0.1:0".into();
    cfg
}

pub fn make_crawler(cfg: Config) -> anyhow::Result<Crawler> {
    Crawler::new(cfg)
}

pub fn app_state(c: Crawler) -> AppState {
    AppState::from_crawler(&c).expect("app state")
}

pub use lyra_index::server::router;
