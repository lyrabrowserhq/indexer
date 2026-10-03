use std::collections::HashSet;
use std::net::{IpAddr, ToSocketAddrs};
use std::path::Path;
use url::Url;

/// Host blocklist: exact match or suffix match, entries lowercase, `#` comment
/// lines skipped. Built-in defaults cover the usual tarpit, parked-domain and
/// abuse hosts; operators add more via data/blocklists/*.txt.
pub struct Blocklist {
    entries: HashSet<String>,
}

static BUILTIN: &[&str] = &[
    // search-engine detritus and AI-scraper honeypots
    "pinterest.com",
    "pinterest.co.uk",
    "pin.it",
    "facebook.com",
    "fb.com",
    "instagram.com",
    "threads.net",
    "twitter.com",
    "x.com",
    "t.co",
    "linkedin.com",
    "tiktok.com",
    "snapchat.com",
    "quora.com",
    "slideshare.net",
    "scribd.com",
    "fandom.com",
    "wattpad.com",
    "google.com",
    "googleapis.com",
    "gstatic.com",
    "googleusercontent.com",
    "wikidata.org",
    "wikimedia.org",
    // parked / spammy / malware-ish
    "000webhostapp.com",
    "weebly.com",
    "tripod.com",
];

impl Blocklist {
    pub fn load(data_dir: &Path, extra_files: &[std::path::PathBuf]) -> Self {
        let mut entries: HashSet<String> = BUILTIN.iter().map(|s| s.to_string()).collect();

        let mut files: Vec<std::path::PathBuf> = Vec::new();
        let builtin_dir = data_dir.join("blocklists");
        if let Ok(rd) = std::fs::read_dir(&builtin_dir) {
            for e in rd.flatten() {
                if e.path().extension().is_some_and(|x| x == "txt") {
                    files.push(e.path());
                }
            }
        }
        files.extend(extra_files.iter().cloned());

        for f in files {
            if let Ok(text) = std::fs::read_to_string(&f) {
                for line in text.lines() {
                    let line = line.trim().trim_start_matches("*.").to_lowercase();
                    if !line.is_empty() && !line.starts_with('#') {
                        entries.insert(line);
                    }
                }
            }
        }
        Self { entries }
    }

    pub fn blocked(&self, host: &str) -> bool {
        let host = host.to_lowercase();
        let mut h = host.as_str();
        loop {
            if self.entries.contains(h) {
                return true;
            }
            match h.find('.') {
                Some(i) => h = &h[i + 1..],
                None => return false,
            }
        }
    }

    /// refuse private, loopback, link-local, multicast and unspecified addrs
    pub fn ip_allowed(ip: IpAddr, allow_private: bool) -> bool {
        if allow_private {
            return true;
        }
        match ip {
            IpAddr::V4(v4) => {
                !(v4.is_private()
                    || v4.is_loopback()
                    || v4.is_link_local()
                    || v4.is_multicast()
                    || v4.is_unspecified()
                    || v4.is_broadcast()
                    || v4.is_documentation()
                    || v4.octets()[0] == 0
                    // CGNAT 100.64/10
                    || (v4.octets()[0] == 100 && (v4.octets()[1] & 0xc0) == 64)
                    // benchmarking 198.18/15
                    || (v4.octets()[0] == 198 && (v4.octets()[1] & 0xfe) == 18))
            }
            IpAddr::V6(v6) => {
                !(v6.is_loopback()
                    || v6.is_unspecified()
                    || v6.is_multicast()
                    || (v6.segments()[0] & 0xfe00) == 0xfc00 // ULA fc00::/7
                    || (v6.segments()[0] & 0xffc0) == 0xfe80) // link-local fe80::/10
            }
        }
    }
}

/// true when the url is http(s) and every resolved address is public
pub fn url_is_public(raw: &str, allow_private: bool) -> bool {
    let Ok(u) = Url::parse(raw) else {
        return false;
    };
    if u.scheme() != "http" && u.scheme() != "https" {
        return false;
    }
    let Some(host) = u.host_str() else {
        return false;
    };
    if host.eq_ignore_ascii_case("localhost") || host.ends_with(".localhost") {
        return false;
    }
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Blocklist::ip_allowed(ip, allow_private);
    }
    let port = u.port_or_known_default().unwrap_or(80);
    let addr = format!("{host}:{port}");
    match addr.to_socket_addrs() {
        Ok(iter) => {
            let mut any = false;
            for sa in iter {
                any = true;
                if !Blocklist::ip_allowed(sa.ip(), allow_private) {
                    return false;
                }
            }
            any
        }
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn suffix_match() {
        let b = Blocklist::load(std::path::Path::new("/nonexistent-lyra-bl"), &[]);
        assert!(b.blocked("pinterest.com"));
        assert!(b.blocked("www.pinterest.com"));
        assert!(!b.blocked("example.com"));
    }

    #[test]
    fn private_ips_refused() {
        assert!(!Blocklist::ip_allowed(
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            false
        ));
        assert!(!Blocklist::ip_allowed(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            false
        ));
        assert!(!Blocklist::ip_allowed(
            IpAddr::V4(Ipv4Addr::new(169, 254, 1, 1)),
            false
        ));
        assert!(!Blocklist::ip_allowed(
            IpAddr::V4(Ipv4Addr::new(100, 64, 0, 1)),
            false
        ));
        assert!(Blocklist::ip_allowed(
            IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34)),
            false
        ));
        assert!(Blocklist::ip_allowed(
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            true
        ));
    }

    #[test]
    fn url_is_public_rejects_lan() {
        assert!(!url_is_public("http://127.0.0.1/x", false));
        assert!(!url_is_public("http://192.168.0.1/x", false));
        assert!(!url_is_public("http://localhost/x", false));
        assert!(!url_is_public("file:///etc/passwd", false));
        assert!(url_is_public("http://127.0.0.1/x", true));
    }
}
