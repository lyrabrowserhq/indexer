use crate::blocklist::url_is_public;
use crate::index::{SearchIndex, now_epoch};
use crate::store::Store;
use anyhow::{Result, bail};
use feed_rs::parser;
use reqwest::Client;
use std::time::Duration;
use url::Url;

pub fn http_client() -> Result<Client> {
    Ok(Client::builder()
        .timeout(Duration::from_secs(15))
        .connect_timeout(Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::limited(3))
        .user_agent(concat!("LyraIndex/", env!("CARGO_PKG_VERSION")))
        .build()?)
}

pub async fn ingest_feed(
    index: &SearchIndex,
    store: &Store,
    client: &Client,
    feed_url: &str,
    allow_private: bool,
) -> Result<usize> {
    if !url_is_public(feed_url, allow_private) {
        bail!("feed url is not a public http(s) address");
    }
    let body = client
        .get(feed_url)
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    let feed = parser::parse(body.as_ref())?;
    let mut n = 0usize;
    for entry in feed.entries {
        let link = entry
            .links
            .iter()
            .map(|l| l.href.as_str())
            .find(|h| url_is_public(h, allow_private))
            .unwrap_or("");
        if link.is_empty() {
            continue;
        }
        let title = entry
            .title
            .as_ref()
            .map(|t| t.content.as_str())
            .unwrap_or(link);
        let summary = entry
            .summary
            .as_ref()
            .map(|s| s.content.as_str())
            .or(entry.content.as_ref().and_then(|c| c.body.as_deref()))
            .unwrap_or("");
        let host = Url::parse(link)
            .ok()
            .and_then(|u| u.host_str().map(str::to_string))
            .unwrap_or_default();
        let fetched = entry
            .updated
            .or(entry.published)
            .map(|t| t.timestamp() as u64)
            .unwrap_or_else(now_epoch);
        index.add_doc_kind(link, &host, title, summary, summary, fetched, 0.0, 0, "rss")?;
        n += 1;
    }
    if n > 0 {
        index.commit()?;
    }
    store.add_feed(feed_url)?;
    Ok(n)
}

pub async fn refresh_all(
    index: &SearchIndex,
    store: &Store,
    client: &Client,
    allow_private: bool,
) -> Result<usize> {
    let feeds = store.list_feeds();
    let mut n = 0;
    for url in feeds {
        n += ingest_feed(index, store, client, &url, allow_private)
            .await
            .unwrap_or(0);
    }
    Ok(n)
}
