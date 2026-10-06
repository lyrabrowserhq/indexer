use url::Url;
use xxhash_rust::xxh3::xxh3_64;

static TRACKING_PARAMS: &[&str] = &[
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "utm_id",
    "utm_source_platform",
    "utm_creative_format",
    "utm_marketing_tactic",
    "fbclid",
    "gclid",
    "dclid",
    "gbraid",
    "wbraid",
    "msclkid",
    "twclid",
    "mc_cid",
    "mc_eid",
    "igshid",
    "_ga",
    "_gl",
];

// Params that are only tracking on specific properties. Stripping them
// globally breaks sites that use the same names for sessions and state:
// si=, app=, feature=, ref_= are all legitimate on other hosts.
static SITE_TRACKING_PARAMS: &[(&str, &[&str])] = &[
    // youtube share links append si/app/feature for tracking
    ("youtube.com", &["si", "app", "feature", "persist_app"]),
    ("youtu.be", &["si"]),
    // google result/share links append ved/ei/sclient/referrer
    ("google.", &["ved", "ei", "sclient", "referrer"]),
    // aliexpress internal tracking
    ("aliexpress.", &["spm", "scm"]),
    // amazon and imdb breadcrumb refs
    ("amazon.", &["ref_"]),
    ("imdb.com", &["ref_"]),
];

fn is_tracking_param(site: &str, name: &str) -> bool {
    if TRACKING_PARAMS.contains(&name) {
        return true;
    }
    for (dom, params) in SITE_TRACKING_PARAMS {
        if (site == *dom || site.starts_with(dom)) && params.contains(&name) {
            return true;
        }
    }
    false
}

/// Normalize a URL for dedup and crawling: lowercase host, strip fragment,
/// drop tracking params, sort remaining params, strip default ports and
/// trailing slash on empty paths.
pub fn normalize(u: &str) -> Option<Url> {
    let mut url = Url::parse(u).ok()?;
    if url.scheme() != "http" && url.scheme() != "https" {
        return None;
    }
    if url.username() != "" || url.password().is_some() {
        // never follow URLs carrying credentials
        return None;
    }
    url.set_fragment(None);
    let host = url.host_str()?.to_lowercase();
    url.set_host(Some(&host)).ok()?;
    // drop default ports
    if (url.scheme() == "http" && url.port() == Some(80))
        || (url.scheme() == "https" && url.port() == Some(443))
    {
        let _ = url.set_port(None);
    }
    let site = site_key(&host);
    // strip tracking params, sort the rest
    let pairs: Vec<(String, String)> = url
        .query_pairs()
        .filter(|(k, _)| !is_tracking_param(&site, &k.to_lowercase()))
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    if pairs.is_empty() {
        url.set_query(None);
    } else {
        let mut p = pairs;
        p.sort();
        url.query_pairs_mut()
            .clear()
            .extend_pairs(p.iter().map(|(k, v)| (k.as_str(), v.as_str())));
    }
    // collapse dot segments already handled by Url parser; trim trailing slash
    // on root path only (keep inner slashes, they can be significant)
    if url.path() == "/" {
        url.set_path("");
    }
    Some(url)
}

pub fn url_key(u: &Url) -> u64 {
    xxh3_64(u.as_str().as_bytes())
}

pub fn host_of(u: &Url) -> String {
    u.host_str().unwrap_or_default().to_lowercase()
}

/// registrable domain-ish key for per-site politeness: use the last two
/// labels so subdomains share one politeness bucket
pub fn site_key(host: &str) -> String {
    let parts: Vec<&str> = host.split('.').collect();
    if parts.len() <= 2 {
        return host.to_string();
    }
    // crude public-suffix handling for common two-level TLDs
    const TWO_LEVEL: &[&str] = &[
        "co.uk", "org.uk", "ac.uk", "gov.uk", "com.au", "co.jp", "co.nz", "com.br", "com.mx",
        "co.in", "com.cn", "or.jp",
    ];
    let last2 = format!("{}.{}", parts[parts.len() - 2], parts[parts.len() - 1]);
    if TWO_LEVEL.contains(&last2.as_str()) && parts.len() > 2 {
        return format!("{}.{}", parts[parts.len() - 3], last2);
    }
    last2
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_tracking() {
        let u = normalize("https://Example.COM/path?utm_source=x&b=2&a=1#frag").unwrap();
        assert_eq!(u.as_str(), "https://example.com/path?a=1&b=2");
    }

    #[test]
    fn generic_params_kept_off_platform() {
        // si/app/feature are functional params outside youtube/google
        let u = normalize("https://example.com/app?si=deadbeef-uuid&app=1").unwrap();
        assert_eq!(u.as_str(), "https://example.com/app?app=1&si=deadbeef-uuid");
        let u = normalize("https://tiles.example.org/map?session=abc-123-uuid").unwrap();
        assert_eq!(
            u.as_str(),
            "https://tiles.example.org/map?session=abc-123-uuid"
        );
    }

    #[test]
    fn site_params_stripped_on_platform() {
        let u = normalize("https://www.youtube.com/watch?v=x&si=abc&app=desktop").unwrap();
        assert_eq!(u.as_str(), "https://www.youtube.com/watch?v=x");
        let u = normalize("https://www.google.de/search?q=x&ved=1&ei=2").unwrap();
        assert_eq!(u.as_str(), "https://www.google.de/search?q=x");
    }

    #[test]
    fn rejects_non_http() {
        assert!(normalize("ftp://x/").is_none());
        assert!(normalize("javascript:void(0)").is_none());
        assert!(normalize("https://u:p@x/").is_none());
    }

    #[test]
    fn site_key_groups_subdomains() {
        assert_eq!(site_key("www.example.com"), "example.com");
        assert_eq!(site_key("a.b.example.co.uk"), "example.co.uk");
        assert_eq!(site_key("localhost"), "localhost");
    }
}
