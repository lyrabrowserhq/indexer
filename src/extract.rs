use scraper::{Html, Selector};
use std::sync::OnceLock;
use url::Url;

static SEL_A: OnceLock<Selector> = OnceLock::new();
static SEL_TITLE: OnceLock<Selector> = OnceLock::new();
static SEL_H1: OnceLock<Selector> = OnceLock::new();
static SEL_META_ROBOTS: OnceLock<Selector> = OnceLock::new();
static SEL_META_DESC: OnceLock<Selector> = OnceLock::new();
static SEL_OG_TITLE: OnceLock<Selector> = OnceLock::new();
static SEL_OG_DESC: OnceLock<Selector> = OnceLock::new();
static SEL_DROP: OnceLock<Selector> = OnceLock::new();
static SEL_INFOBOX: OnceLock<Selector> = OnceLock::new();
static SEL_CANON: OnceLock<Selector> = OnceLock::new();
static SEL_LD: OnceLock<Selector> = OnceLock::new();

#[allow(dead_code)]
pub struct Extracted {
    pub title: String,
    pub description: String,
    pub text: String,
    pub links: Vec<String>,
    pub noindex: bool,
    pub nofollow: bool,
    pub canonical: String,
}

/// pull title, meta description, visible text and absolute links out of html.
/// meta robots noindex/nofollow honored.
pub fn extract(body: &str, base: &Url) -> Extracted {
    let sel_a = SEL_A.get_or_init(|| Selector::parse("a[href]").unwrap());
    let sel_title = SEL_TITLE.get_or_init(|| Selector::parse("title").unwrap());
    let sel_h1 = SEL_H1.get_or_init(|| Selector::parse("h1").unwrap());
    let sel_meta_robots =
        SEL_META_ROBOTS.get_or_init(|| Selector::parse("meta[name=robots]").unwrap());
    let sel_meta_desc =
        SEL_META_DESC.get_or_init(|| Selector::parse("meta[name=description]").unwrap());
    let sel_og_title =
        SEL_OG_TITLE.get_or_init(|| Selector::parse("meta[property=\"og:title\"]").unwrap());
    let sel_og_desc =
        SEL_OG_DESC.get_or_init(|| Selector::parse("meta[property=\"og:description\"]").unwrap());
    let sel_drop = SEL_DROP.get_or_init(|| {
        Selector::parse(
            "script, style, noscript, template, iframe, svg, nav, footer, header, form, aside, table.infobox, table.navbox, table.sidebar, .mw-editsection, sup.reference, ol.references, .noprint, #mw-navigation, #mw-panel, #siteNotice, .mw-jump-link, #jump-to-nav, .hatnote, .dablink",
        )
        .unwrap()
    });
    let sel_infobox = SEL_INFOBOX.get_or_init(|| Selector::parse("table.infobox").unwrap());
    let sel_canon = SEL_CANON.get_or_init(|| Selector::parse("link[rel=canonical]").unwrap());
    let sel_ld =
        SEL_LD.get_or_init(|| Selector::parse("script[type=\"application/ld+json\"]").unwrap());

    let doc = Html::parse_document(body);

    let mut title = doc
        .select(sel_title)
        .next()
        .map(|t| t.text().collect::<String>().trim().to_string())
        .unwrap_or_default();
    if title.is_empty() {
        title = doc
            .select(sel_og_title)
            .next()
            .and_then(|m| m.value().attr("content").map(|s| s.trim().to_string()))
            .unwrap_or_default();
    }
    if title.is_empty() {
        title = doc
            .select(sel_h1)
            .next()
            .map(|t| t.text().collect::<String>().trim().to_string())
            .unwrap_or_default();
    }

    let mut description = doc
        .select(sel_meta_desc)
        .next()
        .and_then(|m| m.value().attr("content").map(|s| s.trim().to_string()))
        .unwrap_or_default();
    if description.is_empty() {
        description = doc
            .select(sel_og_desc)
            .next()
            .and_then(|m| m.value().attr("content").map(|s| s.trim().to_string()))
            .unwrap_or_default();
    }

    if title.is_empty() || description.is_empty() {
        let (ld_title, ld_desc) = json_ld_fields(&doc, sel_ld);
        if title.is_empty() {
            title = ld_title;
        }
        if description.is_empty() {
            description = ld_desc;
        }
    }

    let canonical = doc
        .select(sel_canon)
        .next()
        .and_then(|m| m.value().attr("href"))
        .and_then(|href| base.join(href).ok().map(|u| u.to_string()))
        .unwrap_or_default();
    let mut noindex = false;
    let mut nofollow = false;
    if let Some(m) = doc.select(sel_meta_robots).next()
        && let Some(content) = m.value().attr("content")
    {
        let c = content.to_lowercase();
        noindex = c.contains("noindex") || c.contains("none");
        nofollow = c.contains("nofollow");
    }

    let links: Vec<String> = if nofollow {
        Vec::new()
    } else {
        doc.select(sel_a)
            .filter_map(|a| a.value().attr("href"))
            .filter_map(|href| base.join(href).ok().map(|u| u.to_string()))
            .collect()
    };

    // text extraction: walk the tree, skip script/style/nav subtrees
    let mut text = String::with_capacity(body.len() / 4);
    let drop_ids: std::collections::HashSet<ego_tree::NodeId> =
        doc.select(sel_drop).map(|e| e.id()).collect();
    for node in doc.tree.root().descendants() {
        if let Some(t) = node.value().as_text() {
            // skip text nodes inside dropped elements
            let mut skip = false;
            let mut cur = node.parent();
            while let Some(p) = cur {
                if drop_ids.contains(&p.id()) {
                    skip = true;
                    break;
                }
                cur = p.parent();
            }
            if !skip {
                let s = t.trim();
                if !s.is_empty() {
                    text.push_str(s);
                    text.push(' ');
                }
            }
        }
    }

    for lead in [
        "Jump to content ",
        "Jump to navigation ",
        "From Wikipedia, the free encyclopedia ",
    ] {
        if let Some(rest) = text.strip_prefix(lead) {
            text = rest.to_string();
        }
    }

    let infobox: String = doc
        .select(sel_infobox)
        .flat_map(|el| el.text())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    if !infobox.is_empty() {
        text.push_str(&infobox);
        text.push(' ');
    }

    // cap stored text - pages can be huge, ranking only needs the front
    if text.len() > 64 * 1024 {
        text.truncate(64 * 1024);
    }

    Extracted {
        title,
        description,
        text,
        links,
        noindex,
        nofollow,
        canonical,
    }
}

fn json_ld_fields(doc: &Html, sel: &Selector) -> (String, String) {
    for node in doc.select(sel) {
        let raw: String = node.text().collect();
        let Ok(v) = serde_json::from_str::<serde_json::Value>(raw.trim()) else {
            continue;
        };
        if let Some(found) = ld_headline(&v) {
            return found;
        }
    }
    (String::new(), String::new())
}

fn ld_type_ok(ty: &serde_json::Value) -> bool {
    const KINDS: &[&str] = &[
        "Article",
        "NewsArticle",
        "BlogPosting",
        "WebPage",
        "TechArticle",
        "VideoObject",
        "SocialMediaPosting",
        "DiscussionForumPosting",
        "QAPage",
        "FAQPage",
    ];
    match ty {
        serde_json::Value::String(s) => KINDS.iter().any(|k| s.ends_with(k)),
        serde_json::Value::Array(a) => a.iter().any(ld_type_ok),
        _ => false,
    }
}

fn ld_headline(v: &serde_json::Value) -> Option<(String, String)> {
    if let Some(arr) = v.as_array() {
        for item in arr {
            if let Some(found) = ld_headline(item) {
                return Some(found);
            }
        }
        return None;
    }
    if let Some(graph) = v.get("@graph") {
        return ld_headline(graph);
    }
    if !v.get("@type").is_some_and(ld_type_ok) {
        return None;
    }
    let title = v
        .get("headline")
        .or_else(|| v.get("name"))
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    let desc = v
        .get("description")
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if title.is_empty() && desc.is_empty() {
        None
    } else {
        Some((title, desc))
    }
}

fn head_bytes(s: &str, n: usize) -> &str {
    let mut end = n.min(s.len());
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// pull item/entry links out of an rss or atom feed body. returns empty for
/// non-feeds. feeds themselves are not indexed, just mined for urls.
pub fn feed_links(body: &str) -> Vec<String> {
    let head = head_bytes(body, 8192);
    let is_feed = head.contains("<rss") || head.contains("<feed") || head.contains("<rdf:RDF");
    if !is_feed {
        return Vec::new();
    }
    let mut links = Vec::new();
    // rss: <link>https://x</link> inside <item>; atom: <link href="..."/>
    let mut rest = body;
    while let Some(i) = rest.find("<link>") {
        let after = &rest[i + 6..];
        if let Some(j) = after.find("</link>") {
            let l = after[..j].trim();
            if l.starts_with("http") {
                links.push(l.to_string());
            }
            rest = &after[j + 7..];
        } else {
            break;
        }
    }
    for cap in rest.split("<link").skip(1) {
        if let Some(a) = cap.find("href=") {
            let q = &cap[a + 5..];
            let end = q.find('"').or_else(|| q.find('\''));
            let start = if q.starts_with('"') || q.starts_with('\'') {
                1
            } else {
                0
            };
            if let Some(e) = end {
                let l = q[start..e].trim();
                if l.starts_with("http") {
                    links.push(l.to_string());
                }
            }
        }
    }
    links
}

/// pull <loc> urls out of a sitemap xml body (urlset and sitemapindex).
pub fn sitemap_urls(body: &str) -> Vec<String> {
    let head = head_bytes(body, 4096);
    if !head.contains("<urlset") && !head.contains("<sitemapindex") && !head.contains("<loc>") {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut rest = body;
    while let Some(i) = rest.find("<loc>") {
        let after = &rest[i + 5..];
        let Some(j) = after.find("</loc>") else { break };
        let l = after[..j].trim();
        if l.starts_with("http") {
            out.push(l.to_string());
        }
        rest = &after[j + 6..];
    }
    out
}

/// detect a sitemap body without paying for full extraction
pub fn is_sitemap(body: &str) -> bool {
    let head = head_bytes(body, 4096);
    head.contains("<urlset") || head.contains("<sitemapindex")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_basics() {
        let base = Url::parse("https://x.test/dir/").unwrap();
        let html = r#"<html><head><title>Hello</title>
        <meta name="description" content="desc">
        </head><body><p>Visible text</p><script>bad()</script>
        <a href="/a">link</a><a href="rel2">link2</a></body></html>"#;
        let e = extract(html, &base);
        assert_eq!(e.title, "Hello");
        assert_eq!(e.description, "desc");
        assert!(e.text.contains("Visible text"));
        assert!(!e.text.contains("bad()"));
        assert!(e.links.iter().any(|l| l == "https://x.test/a"));
        assert!(e.links.iter().any(|l| l == "https://x.test/dir/rel2"));
        assert!(!e.noindex);
    }

    #[test]
    fn falls_back_to_open_graph() {
        let base = Url::parse("https://x.test/").unwrap();
        let html = r#"<html><head>
        <meta property="og:title" content="OG Title">
        <meta property="og:description" content="OG Desc">
        </head><body><h1>Heading</h1><p>Visible text about rust and privacy</p></body></html>"#;
        let e = extract(html, &base);
        assert_eq!(e.title, "OG Title");
        assert_eq!(e.description, "OG Desc");
    }

    #[test]
    fn respects_nofollow() {
        let base = Url::parse("https://x.test/").unwrap();
        let html = r#"<html><head><meta name="robots" content="noindex,nofollow"></head>
        <body><a href="/x">l</a></body></html>"#;
        let e = extract(html, &base);
        assert!(e.noindex);
        assert!(e.links.is_empty());
    }

    #[test]
    fn drops_infobox_text() {
        let base = Url::parse("https://en.wikipedia.org/wiki/Firefox").unwrap();
        let html = r#"<html><head><title>Firefox - Wikipedia</title></head>
        <body>
        <table class="infobox"><tr><th>Type</th><td>Web browser</td></tr>
        <tr><th>License</th><td>MPL 2.0</td></tr></table>
        <p>Mozilla Firefox is a free and open-source web browser.</p>
        </body></html>"#;
        let e = extract(html, &base);
        assert!(e.text.contains("Mozilla Firefox is a free"));
        let lead = e.text.find("Mozilla Firefox").unwrap();
        let box_at = e
            .text
            .find("MPL 2.0")
            .expect("infobox terms stay searchable");
        assert!(lead < box_at);
    }

    #[test]
    fn canonical_and_json_ld() {
        let base = Url::parse("https://x.test/a?ref=1").unwrap();
        let html = r#"<html><head>
        <link rel="canonical" href="https://x.test/a">
        <script type="application/ld+json">{"@type":"Article","headline":"LD Title","description":"LD Desc"}</script>
        </head><body><p>enough visible text about rust privacy tools here</p></body></html>"#;
        let e = extract(html, &base);
        assert_eq!(e.title, "LD Title");
        assert_eq!(e.description, "LD Desc");
        assert_eq!(e.canonical, "https://x.test/a");
    }

    #[test]
    fn prefix_slices_stay_on_char_boundary() {
        let mut s = "മ".repeat(4000);
        s.push_str("<rss></rss>");
        let _ = feed_links(&s);
        let _ = is_sitemap(&s);
        let _ = sitemap_urls(&s);
    }
}
