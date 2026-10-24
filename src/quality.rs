//! drop pages that would pollute Seek results: challenge walls, empty
//! shells, and documents with too little readable text.

pub fn is_indexable(title: &str, text: &str, min_chars: usize) -> bool {
    if junk_title(title) {
        return false;
    }
    let n = text.chars().filter(|c| c.is_alphanumeric()).count();
    n >= min_chars
}

pub fn junk_title(title: &str) -> bool {
    let t = title.trim().to_lowercase();
    if t.is_empty() {
        return false;
    }
    const BAD: &[&str] = &[
        "just a moment",
        "access denied",
        "attention required",
        "403 forbidden",
        "404 not found",
        "503 service",
        "verify you are human",
        "checking your browser",
        "one moment please",
        "please wait",
        "captcha",
        "enable javascript",
        "pardon our interruption",
        "rate limited",
        "are you a robot",
        "unusual traffic",
    ];
    if BAD.iter().any(|b| t.contains(b)) {
        return true;
    }
    // bare auth titles are never useful results, only exact matches so
    // articles about sign-in still pass
    matches!(
        t.as_str(),
        "sign in" | "log in" | "login" | "signin" | "sign up" | "create account"
    )
}

pub fn display_title(title: &str, text: &str) -> String {
    let t = title.trim();
    if !t.is_empty() && !junk_title(t) {
        return t.to_string();
    }
    let excerpt: String = text
        .split_whitespace()
        .take(8)
        .collect::<Vec<_>>()
        .join(" ");
    if excerpt.is_empty() {
        "untitled".into()
    } else {
        excerpt
    }
}

pub fn clip_text(text: &str, max_bytes: usize) -> String {
    if max_bytes == 0 || text.len() <= max_bytes {
        return text.to_string();
    }
    let mut end = max_bytes;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_string()
}

/// mediawiki namespaces, binary files, and session/cart URLs are not indexed
pub fn skip_url(url: &url::Url) -> bool {
    let path = url.path();
    // wayback machine pages are proxy copies of other sites, not real
    // results, and the host is huge enough to eat a crawl budget
    if let Some(host) = url.host_str() {
        if host.eq_ignore_ascii_case("web.archive.org")
            && (path.starts_with("/web/") || path.starts_with("/save/"))
        {
            return true;
        }
        // wikimedia interlanguage links fan a page out into every sister
        // edition; keep the english ones only
        let h = host.to_ascii_lowercase();
        const FAMILY: &[&str] = &[
            "wikipedia.org",
            "wiktionary.org",
            "wikiquote.org",
            "wikivoyage.org",
            "wikibooks.org",
            "wikisource.org",
        ];
        for d in FAMILY {
            let suffix = format!(".{d}");
            if let Some(sub) = h.strip_suffix(suffix.as_str()) {
                let lang = sub.split('.').next().unwrap_or("");
                if !matches!(lang, "en" | "simple" | "www") {
                    return true;
                }
            }
        }
    }
    if path.contains("/w/index.php") {
        return true;
    }
    let leaf = path.rsplit('/').next().unwrap_or("");
    const NS: &[&str] = &[
        "File:",
        "Special:",
        "Talk:",
        "User:",
        "User_talk:",
        "Template:",
        "Help:",
        "Wikipedia:",
        "MediaWiki:",
        "Draft:",
        "Module:",
        "TimedText:",
        "WP:",
    ];
    if NS.iter().any(|n| leaf.starts_with(n)) {
        return true;
    }
    let lower = path.to_ascii_lowercase();
    const EXT: &[&str] = &[
        ".pdf", ".zip", ".gz", ".tgz", ".tar", ".exe", ".dmg", ".iso", ".mp3", ".mp4", ".avi",
        ".mkv", ".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".ico", ".woff", ".woff2",
        ".ttf", ".css", ".js", ".xml", ".rss", ".atom", ".opml", ".ics",
    ];
    if EXT.iter().any(|e| lower.ends_with(e)) {
        return true;
    }
    const BITS: &[&str] = &[
        "/wp-admin",
        "/wp-login",
        "/cgi-bin/",
        "/checkout",
        "/cart",
        "/logout",
        "/signin",
        "/signup",
        "/register",
        "/cdn-cgi/",
    ];
    if BITS.iter().any(|b| lower.contains(b)) {
        return true;
    }
    if lower.ends_with("/login") {
        return true;
    }
    const TRAPS: &[&str] = &[
        "/nepenthes",
        "/iocaine",
        "/labyrinth",
        "/tarpit",
        "/honeypot",
        "/spidertrap",
        "/crawlertrap",
    ];
    if TRAPS.iter().any(|b| lower.contains(b)) {
        return true;
    }
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    if segments.len() > 10 || path.len() > 220 {
        return true;
    }
    let mut generated = 0usize;
    for seg in &segments {
        if looks_generated_segment(seg) {
            generated += 1;
        }
    }
    if generated >= 2 {
        return true;
    }
    let mut qn = 0usize;
    for (k, _) in url.query_pairs() {
        qn += 1;
        let k = k.to_ascii_lowercase();
        if k == "sessionid" || k == "phpsessid" || k == "jsessionid" || k == "sid" {
            return true;
        }
        if k == "print" || k == "share" {
            return true;
        }
    }
    qn > 10
}

/// hub pages are crawled for outlinks but not stored in the index
pub fn skip_index_url(url: &url::Url) -> bool {
    if let Some(host) = url.host_str()
        && host.eq_ignore_ascii_case("web.archive.org")
        && url.path().starts_with("/web/")
    {
        return true;
    }
    let leaf = url.path().rsplit('/').next().unwrap_or("");
    const HUB: &[&str] = &["Portal:", "Category:"];
    if HUB.iter().any(|n| leaf.starts_with(n)) {
        return true;
    }
    let segs: Vec<&str> = url.path().split('/').filter(|s| !s.is_empty()).collect();
    for (i, s) in segs.iter().enumerate() {
        let sl = s.to_ascii_lowercase();
        // amp mirrors and wordpress-style archive prefixes are listing pages
        if sl == "amp" {
            return true;
        }
        if (sl == "tag" || sl == "category" || sl == "author" || sl == "page") && i + 1 < segs.len()
        {
            return true;
        }
    }
    // auth walls and account flows have no searchable prose. checking the
    // first or last segment only, so docs mentioning accounts still index
    let edge = |s: &str| {
        matches!(
            s,
            "login"
                | "signin"
                | "sign-in"
                | "logout"
                | "oauth"
                | "oauth2"
                | "openid"
                | "sso"
                | "register"
                | "signup"
                | "cart"
                | "checkout"
        )
    };
    if segs.first().is_some_and(|s| edge(&s.to_ascii_lowercase()))
        || segs.last().is_some_and(|s| edge(&s.to_ascii_lowercase()))
    {
        return true;
    }
    // member and user profile pages
    if segs.len() >= 2
        && matches!(
            segs[0].to_ascii_lowercase().as_str(),
            "members" | "member" | "users" | "user" | "u"
        )
    {
        return true;
    }
    // date archives end in /yyyy, /yyyy/mm, or /yyyy/mm/dd. a deeper leaf is
    // a real post and stays indexed
    let all_digits = |s: &str| !s.is_empty() && s.chars().all(|c| c.is_ascii_digit());
    let is_year = |s: &str| {
        s.len() == 4
            && all_digits(s)
            && s.parse::<u32>()
                .map(|y| (1900..=2100).contains(&y))
                .unwrap_or(false)
    };
    let is_monthday = |s: &str| s.len() <= 2 && all_digits(s);
    if let Some(last) = segs.last() {
        let n = segs.len();
        if is_year(last) {
            return true;
        }
        if is_monthday(last) && n >= 2 && (is_year(segs[n - 2]) || is_monthday(segs[n - 2])) {
            return true;
        }
    }
    // search and share endpoints are not content
    if let Some(q) = url.query()
        && (q.contains("replytocom")
            || q.starts_with("s=")
            || q.contains("&s=")
            || q.contains("share="))
    {
        return true;
    }
    false
}

pub fn is_media_host(host: &str) -> bool {
    let h = host.trim_start_matches("www.");
    h == "youtube.com"
        || h == "youtu.be"
        || h == "m.youtube.com"
        || h == "odysee.com"
        || h.ends_with(".odysee.com")
}

pub fn is_wiki_host(host: &str) -> bool {
    let h = host.trim_start_matches("www.");
    h.ends_with("wikipedia.org")
        || h.ends_with("wiktionary.org")
        || h.ends_with("wikibooks.org")
        || h.ends_with("wikisource.org")
        || h.ends_with("wikiversity.org")
        || h.ends_with("wikivoyage.org")
        || h.ends_with("wikinews.org")
        || h.ends_with("wikiquote.org")
        || h.ends_with("wikimedia.org")
        || h.ends_with(".wiki")
        || h.ends_with("wiki.gg")
        || h.ends_with(".wiki.gg")
        || h == "wiki.archlinux.org"
        || h == "wiki.debian.org"
        || h == "wiki.gentoo.org"
        || h == "wiki.nixos.org"
        || h == "minecraft.wiki"
        || h == "pcgamingwiki.com"
}

pub fn is_forum_url(url: &url::Url) -> bool {
    let p = url.path().to_ascii_lowercase();
    p.contains("/forum")
        || p.contains("/viewtopic")
        || p.contains("/viewforum")
        || p.contains("/showthread")
        || p.contains("/thread")
        || p.contains("/t/")
        || p.contains("/discussion")
}

fn looks_generated_segment(seg: &str) -> bool {
    let s = seg.to_ascii_lowercase();
    if s.len() < 16 {
        return false;
    }
    let hex = s.chars().all(|c| c.is_ascii_hexdigit());
    if hex && s.len() >= 20 {
        return true;
    }
    let alnum = s.chars().filter(|c| c.is_ascii_alphanumeric()).count();
    alnum == s.len() && s.bytes().any(|b| b.is_ascii_digit()) && s.len() >= 24
}

/// maze pages pack huge link counts with near-zero prose (Nepenthes,
/// Iocaine, Labyrinth). the bar is intentionally extreme: callers also
/// check same-host dominance so nav-heavy real pages pass
pub fn is_link_maze(text_len: usize, n_links: usize) -> bool {
    n_links >= 48 && text_len < 2000
}

/// markov babble: enough tokens but almost no English function words, or nearly unique tokens
pub fn is_markov_babble(text: &str) -> bool {
    const FW: &[&str] = &[
        "the", "of", "and", "to", "a", "in", "is", "it", "for", "on", "with", "as", "that", "this",
        "be", "are", "was", "by", "or", "from", "at", "an", "not", "have", "has",
    ];
    let mut tokens: Vec<String> = Vec::new();
    let mut cur = String::new();
    for c in text.chars() {
        if c.is_alphanumeric() {
            cur.push(c.to_ascii_lowercase());
        } else if !cur.is_empty() {
            tokens.push(std::mem::take(&mut cur));
        }
    }
    if !cur.is_empty() {
        tokens.push(cur);
    }
    if tokens.len() < 60 {
        return false;
    }
    let fw = tokens.iter().filter(|t| FW.iter().any(|w| *t == w)).count();
    let ratio = fw as f64 / tokens.len() as f64;
    let mut uniq = tokens.clone();
    uniq.sort_unstable();
    uniq.dedup();
    let ttr = uniq.len() as f64 / tokens.len() as f64;
    ratio < 0.12 || ttr > 0.92
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_challenge_titles() {
        assert!(junk_title("Just a moment..."));
        assert!(junk_title("Attention Required! | Cloudflare"));
        assert!(!junk_title("Firefox - Wikipedia"));
    }

    #[test]
    fn requires_enough_text() {
        assert!(!is_indexable("ok", "hi", 50));
        let body = "a ".repeat(80);
        assert!(is_indexable("ok", &body, 50));
        assert!(!is_indexable("Just a moment", &body, 50));
    }

    #[test]
    fn skips_mediawiki_namespaces() {
        let file = url::Url::parse("https://en.wikipedia.org/wiki/File:Foo.png").unwrap();
        let article = url::Url::parse("https://en.wikipedia.org/wiki/Firefox").unwrap();
        let special = url::Url::parse("https://en.wikipedia.org/w/index.php?title=Foo").unwrap();
        let arch_help = url::Url::parse("https://wiki.archlinux.org/title/Help:Editing").unwrap();
        let portal =
            url::Url::parse("https://en.wikipedia.org/wiki/Portal:Current_events").unwrap();
        assert!(skip_url(&file));
        assert!(!skip_url(&article));
        assert!(skip_url(&special));
        assert!(skip_url(&arch_help));
        assert!(!skip_url(&portal));
        assert!(skip_index_url(&portal));
        assert!(!skip_index_url(&article));
        let pdf = url::Url::parse("https://x.test/a.pdf").unwrap();
        let cart = url::Url::parse("https://x.test/cart?id=1").unwrap();
        let article = url::Url::parse("https://x.test/docs/login-guide").unwrap();
        assert!(skip_url(&pdf));
        assert!(skip_url(&cart));
        assert!(!skip_url(&article));
        let trap = url::Url::parse("https://x.test/nepenthes/aa").unwrap();
        let maze =
            url::Url::parse("https://x.test/a1b2c3d4e5f6a7b8c9d0/deadbeefdeadbeefdeadbeef/page")
                .unwrap();
        assert!(skip_url(&trap));
        assert!(skip_url(&maze));
    }

    #[test]
    fn catches_link_mazes_and_babble() {
        assert!(is_link_maze(200, 60));
        assert!(!is_link_maze(200, 40));
        assert!(!is_link_maze(3000, 200));
        assert!(!is_link_maze(8000, 200));
        let babble = "qx zr vw pl mk nj bh gt ys df ".repeat(20);
        assert!(is_markov_babble(&babble));
        let prose = "the cat sat on the mat and the dog sat on the rug with the hat for the rat in the hut as the sun was on the hill and the wind was in the tree for the bird and the fish ".repeat(3);
        assert!(!is_markov_babble(&prose));
    }

    #[test]
    fn media_and_hub_helpers() {
        assert!(is_media_host("www.youtube.com"));
        assert!(is_media_host("odysee.com"));
        assert!(!is_media_host("example.com"));
        assert!(is_wiki_host("en.wikipedia.org"));
        assert!(is_wiki_host("minecraft.wiki"));
        let forum = url::Url::parse("https://www.gbatemp.net/forums/").unwrap();
        assert!(is_forum_url(&forum));
    }
}
