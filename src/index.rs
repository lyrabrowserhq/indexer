use anyhow::Context;
use std::path::Path;
use std::sync::{Arc, RwLock};
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{FAST, Field, INDEXED, STORED, STRING, Schema, TEXT, Value};
use tantivy::snippet::SnippetGenerator;
use tantivy::{DocAddress, Index, IndexWriter, TantivyDocument, Term, doc};

/// tantivy index for crawled docs: bm25 on title+body with an authority
/// boost computed from inlinks, freshness weight on top
pub struct SearchIndex {
    pub index: Index,
    pub writer: Arc<RwLock<IndexWriter>>,
    pub url: Field,
    pub host: Field,
    pub title: Field,
    pub body: Field,
    pub description: Field,
    pub fetched_at: Field,
    pub authority: Field,
    pub inlinks: Field,
    pub kind: Field,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SearchHit {
    pub url: String,
    pub host: String,
    pub title: String,
    pub description: String,
    pub snippet: String,
    pub kind: String,
    pub score: f32,
}

impl SearchHit {
    pub fn clipped(&self, max: usize) -> String {
        let t = if !self.snippet.is_empty() {
            self.snippet.as_str()
        } else if !self.description.is_empty() {
            self.description.as_str()
        } else {
            self.title.as_str()
        };
        let t = t.trim();
        if t.chars().count() <= max {
            return t.to_string();
        }
        t.chars().take(max).collect::<String>() + "..."
    }
}

impl SearchIndex {
    pub fn open(dir: &Path, heap_mb: usize) -> anyhow::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let mut schema = Schema::builder();
        let url = schema.add_text_field("url", STRING | STORED);
        let host = schema.add_text_field("host", STRING | STORED | FAST);
        let title = schema.add_text_field("title", TEXT | STORED);
        let body = schema.add_text_field("body", TEXT | STORED);
        let description = schema.add_text_field("description", TEXT | STORED);
        let fetched_at = schema.add_u64_field("fetched_at", STORED | FAST | INDEXED);
        let authority = schema.add_f64_field("authority", STORED | FAST);
        let inlinks = schema.add_u64_field("inlinks", STORED | FAST);
        let kind = schema.add_text_field("kind", STRING | STORED);
        let schema = schema.build();

        let index = Index::open_or_create(tantivy::directory::MmapDirectory::open(dir)?, schema)?;
        let writer = index.writer(heap_mb * 1024 * 1024)?;

        Ok(Self {
            index,
            writer: Arc::new(RwLock::new(writer)),
            url,
            host,
            title,
            body,
            description,
            fetched_at,
            authority,
            inlinks,
            kind,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn add_doc(
        &self,
        url: &str,
        host: &str,
        title: &str,
        body: &str,
        desc: &str,
        fetched_at: u64,
        authority: f64,
        inlinks: u64,
    ) -> anyhow::Result<()> {
        self.add_doc_kind(
            url, host, title, body, desc, fetched_at, authority, inlinks, "page",
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn add_doc_kind(
        &self,
        url: &str,
        host: &str,
        title: &str,
        body: &str,
        desc: &str,
        fetched_at: u64,
        authority: f64,
        inlinks: u64,
        kind: &str,
    ) -> anyhow::Result<()> {
        let w = self.writer.write().unwrap();
        w.delete_term(Term::from_field_text(self.url, url));
        w.add_document(doc!(
            self.url => url,
            self.host => host,
            self.title => title,
            self.body => body,
            self.description => desc,
            self.fetched_at => fetched_at,
            self.authority => authority,
            self.inlinks => inlinks,
            self.kind => kind,
        ))?;
        Ok(())
    }

    pub fn delete_url(&self, url: &str) -> anyhow::Result<()> {
        let w = self.writer.write().unwrap();
        w.delete_term(Term::from_field_text(self.url, url));
        Ok(())
    }

    pub fn commit(&self) -> anyhow::Result<()> {
        self.writer.write().unwrap().commit()?;
        Ok(())
    }

    /// merge segments and drop files no longer referenced after deletes
    pub fn compact(&self) -> anyhow::Result<usize> {
        self.commit()?;
        let ids = self.index.searchable_segment_ids()?;
        let n = ids.len();
        let mut w = self.writer.write().unwrap();
        if ids.len() > 1 {
            w.merge(&ids).wait()?;
        }
        w.garbage_collect_files().wait()?;
        Ok(n)
    }

    /// bm25 search across title (x4) and body (x1) with description (x2),
    /// then rerank top-k with authority and freshness weights
    pub fn search(&self, q: &str, limit: usize) -> anyhow::Result<Vec<SearchHit>> {
        self.search_kind(q, limit, None)
    }

    pub fn search_kind(
        &self,
        q: &str,
        limit: usize,
        kind: Option<&str>,
    ) -> anyhow::Result<Vec<SearchHit>> {
        let reader = self.index.reader()?;
        let searcher = reader.searcher();
        let mut qp =
            QueryParser::for_index(&self.index, vec![self.title, self.body, self.description]);
        qp.set_conjunction_by_default();
        qp.set_field_boost(self.title, 4.0);
        qp.set_field_boost(self.description, 2.0);
        let query = qp.parse_query(q).context("parse query")?;

        let mut snipper = SnippetGenerator::create(&searcher, &*query, self.body)?;
        snipper.set_max_num_chars(240);

        let top_n = (limit * 4).max(50);
        let hits: Vec<(f32, DocAddress)> =
            searcher.search(&*query, &TopDocs::with_limit(top_n).order_by_score())?;
        let mut out: Vec<SearchHit> = Vec::with_capacity(hits.len());
        let now = now_epoch();
        for (bm25, addr) in hits {
            let doc: TantivyDocument = searcher.doc(addr)?;
            let url = doc
                .get_first(self.url)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let host = doc
                .get_first(self.host)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let title = doc
                .get_first(self.title)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let description = doc
                .get_first(self.description)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let doc_kind = doc
                .get_first(self.kind)
                .and_then(|v| v.as_str())
                .unwrap_or("page")
                .to_string();
            if let Some(want) = kind
                && !doc_kind.eq_ignore_ascii_case(want)
            {
                continue;
            }
            let authority = doc
                .get_first(self.authority)
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            let fetched = doc
                .get_first(self.fetched_at)
                .and_then(|v| v.as_u64())
                .unwrap_or(now);

            let authority_boost = 1.0 + authority.min(10.0) * 0.15;
            let age_days = now.saturating_sub(fetched) as f64 / 86400.0;
            let freshness = 1.0 / (1.0 + (age_days / 365.0).max(0.0) * 0.2);
            let host_boost = host_quality(&host);
            let title_boost = title_query_boost(&title, q);

            let generated = snipper.snippet_from_doc(&doc);
            let body_text = doc
                .get_first(self.body)
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let lead: String = body_text.chars().take(240).collect();
            let snippet = if !description.is_empty() && terms_in(&description, q) {
                description.clone()
            } else if terms_in(&lead, q) {
                lead.trim().to_string()
            } else if !generated.is_empty() {
                generated.fragment().trim().to_string()
            } else if !description.is_empty() {
                description.clone()
            } else {
                title.clone()
            };

            let score = bm25 * authority_boost as f32 * freshness as f32 * host_boost * title_boost;
            out.push(SearchHit {
                url,
                host,
                title,
                description,
                snippet,
                kind: doc_kind,
                score,
            });
        }
        out.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        out.truncate(limit);
        Ok(out)
    }

    pub fn num_docs(&self) -> u64 {
        self.index
            .reader()
            .ok()
            .map(|r| r.searcher().num_docs())
            .unwrap_or(0)
    }
}

pub fn now_epoch() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn host_quality(host: &str) -> f32 {
    let h = host.trim_start_matches("www.");
    const DOCS: &[&str] = &[
        "wikipedia.org",
        "wiktionary.org",
        "mozilla.org",
        "developer.mozilla.org",
        "firefox.com",
        "archlinux.org",
        "wiki.archlinux.org",
        "rust-lang.org",
        "doc.rust-lang.org",
        "ietf.org",
        "w3.org",
        "kernel.org",
        "gnu.org",
        "python.org",
        "docs.python.org",
        "nodejs.org",
        "apache.org",
        "mdn.org",
        "stackoverflow.com",
        "stackexchange.com",
        "github.com",
        "gitlab.com",
        "codeberg.org",
        "docs.rs",
        "pkg.go.dev",
        "go.dev",
        "crates.io",
        "pypi.org",
        "cppreference.com",
        "learn.microsoft.com",
        "readthedocs.io",
        "ziglang.org",
        "kernel.org",
        "wiki.gg",
        "minecraft.wiki",
        "pcgamingwiki.com",
        "youtube.com",
        "odysee.com",
    ];
    if DOCS.iter().any(|d| host_eq(h, d)) {
        1.35
    } else {
        1.0
    }
}

fn host_eq(host: &str, domain: &str) -> bool {
    host == domain
        || host
            .strip_suffix(domain)
            .is_some_and(|prefix| prefix.ends_with('.'))
}

fn terms_in(hay: &str, q: &str) -> bool {
    let h = hay.to_lowercase();
    q.split_whitespace()
        .filter(|t| t.chars().any(|c| c.is_alphanumeric()))
        .all(|t| h.contains(&t.to_lowercase()))
}

fn title_query_boost(title: &str, q: &str) -> f32 {
    let t = title.to_lowercase();
    let mut hits = 0u32;
    let mut n = 0u32;
    for term in q.split_whitespace() {
        if !term.chars().any(|c| c.is_alphanumeric()) {
            continue;
        }
        n += 1;
        if t.contains(&term.to_lowercase()) {
            hits += 1;
        }
    }
    if n == 0 {
        return 1.0;
    }
    1.0 + (hits as f32 / n as f32) * 0.4
}

pub fn dir_bytes(path: &Path) -> u64 {
    fn walk(p: &Path) -> u64 {
        let Ok(meta) = std::fs::metadata(p) else {
            return 0;
        };
        if meta.is_file() {
            return meta.len();
        }
        let Ok(rd) = std::fs::read_dir(p) else {
            return 0;
        };
        rd.flatten().map(|e| walk(&e.path())).sum()
    }
    walk(path)
}
