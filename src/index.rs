use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, RwLock};
use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, Occur, QueryParser, TermQuery};
use tantivy::schema::IndexRecordOption;
use tantivy::schema::{FAST, Field, INDEXED, STORED, STRING, Schema, TEXT, Value};
use tantivy::snippet::SnippetGenerator;
use tantivy::{DocAddress, Index, IndexWriter, TantivyDocument, Term, doc};

/// tantivy index for crawled docs: bm25 on title+body with an authority
/// boost computed from inlinks, freshness weight on top
pub struct SearchIndex {
    pub index: Index,
    pub reader: tantivy::IndexReader,
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
    pub fetched_at: u64,
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

/// operators pulled out of the raw query before tantivy sees it
#[derive(Default)]
struct QuerySpec {
    /// query text with operator tokens removed
    text: String,
    site_includes: Vec<String>,
    site_excludes: Vec<String>,
    inurl_terms: Vec<String>,
    before: Option<u64>,
    after: Option<u64>,
}

/// fields tantivy knows about, other word: prefixes get their field part
/// stripped so define:love searches love instead of erroring
const KNOWN_FIELDS: &[&str] = &[
    "title",
    "body",
    "description",
    "host",
    "url",
    "kind",
    "fetched_at",
    "authority",
    "inlinks",
];

/// escape regex metachars for building a host pattern by hand
fn regex_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 8);
    for c in s.chars() {
        if "\\.^$|?*+()[]{}-".contains(c) {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

fn host_matches(host: &str, domain: &str) -> bool {
    host == domain
        || host
            .strip_suffix(domain)
            .is_some_and(|prefix| prefix.ends_with('.'))
}

fn parse_epoch(s: &str, end_of: bool) -> Option<u64> {
    use time::{Date, Month, OffsetDateTime, Time};
    let parts: Vec<&str> = s.split('-').collect();
    if parts.is_empty() || parts.len() > 3 || parts.iter().any(|p| p.is_empty()) {
        return None;
    }
    let year: i32 = parts[0].parse().ok()?;
    if !(1970..=2200).contains(&year) {
        return None;
    }
    let month = if parts.len() > 1 {
        parts[1].parse::<u8>().ok()?
    } else {
        1
    };
    let day = if parts.len() > 2 {
        parts[2].parse::<u8>().ok()?
    } else {
        1
    };
    let month = Month::try_from(month).ok()?;
    let mut date = Date::from_calendar_date(year, month, day).ok()?;
    if parts.len() < 3 && end_of {
        // last day of the month / year when only a coarse date is given
        if parts.len() == 1 {
            date = Date::from_calendar_date(year, Month::December, 31).ok()?;
        } else {
            let (ny, nm) = if month == Month::December {
                (year + 1, Month::January)
            } else {
                (year, Month::try_from(u8::from(month) + 1).ok()?)
            };
            date = Date::from_calendar_date(ny, nm, 1).ok()?.previous_day()?;
        }
    }
    let t = if end_of {
        Time::from_hms(23, 59, 59).ok()?
    } else {
        Time::MIDNIGHT
    };
    let dt = OffsetDateTime::new_utc(date, t);
    Some(dt.unix_timestamp().max(0) as u64)
}

/// split the raw query into text for tantivy plus structured operators.
/// quoted phrases stay intact so site: inside quotes is not treated as
/// an operator.
fn parse_query_spec(q: &str) -> QuerySpec {
    let mut spec = QuerySpec::default();
    let mut text = String::with_capacity(q.len());
    for tok in split_query_tokens(q) {
        let (neg, bare) = match tok.strip_prefix('-') {
            Some(rest) if !rest.is_empty() => (true, rest),
            _ => (false, tok.as_str()),
        };
        let (prefix, rest) = bare.split_once(':').unwrap_or(("", ""));
        match prefix.to_ascii_lowercase().as_str() {
            "site" if !rest.is_empty() => {
                let dom = rest.trim_matches('"').to_lowercase();
                if neg {
                    spec.site_excludes.push(dom);
                } else {
                    spec.site_includes.push(dom);
                }
                continue;
            }
            "intitle" if !rest.is_empty() => {
                text.push_str(if neg { "-title:" } else { "title:" });
                text.push_str(rest);
                text.push(' ');
                continue;
            }
            "inurl" if !rest.is_empty() => {
                spec.inurl_terms.push(rest.to_lowercase());
                continue;
            }
            "before" => {
                if let Some(ts) = parse_epoch(rest, true) {
                    spec.before = Some(ts);
                    continue;
                }
            }
            "after" => {
                if let Some(ts) = parse_epoch(rest, false) {
                    spec.after = Some(ts);
                    continue;
                }
            }
            _ => {}
        }
        text.push_str(&tok);
        text.push(' ');
    }
    spec.text = text.trim().to_string();
    spec
}

/// whitespace split that keeps "quoted phrases" as one token
fn split_query_tokens(q: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_quote = false;
    for c in q.chars() {
        match c {
            '"' => {
                in_quote = !in_quote;
                cur.push(c);
            }
            c if c.is_whitespace() && !in_quote => {
                if !cur.is_empty() {
                    out.push(std::mem::take(&mut cur));
                }
            }
            _ => cur.push(c),
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// strip word: prefixes for fields tantivy does not have so adhoc queries
/// like define:word or cache:url degrade into plain terms
fn strip_unknown_fields(q: &str) -> String {
    let mut out = String::with_capacity(q.len());
    for tok in split_query_tokens(q) {
        let mut t = tok.as_str();
        let neg = t.starts_with('-');
        if neg {
            t = &t[1..];
        }
        if let Some(i) = t.find(':') {
            let field = &t[..i];
            let rest = &t[i + 1..];
            let looks_like_field = !field.is_empty()
                && field.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
                && !rest.is_empty();
            if looks_like_field && !KNOWN_FIELDS.contains(&field.to_ascii_lowercase().as_str()) {
                if neg {
                    out.push('-');
                }
                out.push_str(rest);
                out.push(' ');
                continue;
            }
        }
        out.push_str(&tok);
        out.push(' ');
    }
    out.trim().to_string()
}

/// last-resort cleanup when the parser still rejects the query: drop quotes,
/// parens, and range markers, keep alnum words and hyphens
fn sanitize_query(q: &str) -> String {
    let mut out = String::with_capacity(q.len());
    for tok in split_query_tokens(q) {
        let cleaned: String = tok
            .chars()
            .filter(|c| c.is_alphanumeric() || *c == '-' || *c == '_')
            .collect();
        if !cleaned.is_empty() {
            out.push_str(&cleaned);
            out.push(' ');
        }
    }
    out.trim().to_string()
}

impl QuerySpec {
    fn matches(&self, host: &str, url: &str, fetched: u64) -> bool {
        let bare_host = host.trim_start_matches("www.");
        if !self.site_includes.is_empty()
            && !self
                .site_includes
                .iter()
                .any(|d| host_matches(bare_host, d) || host_matches(host, d))
        {
            return false;
        }
        if self
            .site_excludes
            .iter()
            .any(|d| host_matches(bare_host, d) || host_matches(host, d))
        {
            return false;
        }
        let url_l = url.to_lowercase();
        if self.inurl_terms.iter().any(|t| !url_l.contains(&**t)) {
            return false;
        }
        if let Some(b) = self.before
            && fetched > b
        {
            return false;
        }
        if let Some(a) = self.after
            && fetched < a
        {
            return false;
        }
        true
    }
}

/// at most this many hits per registrable site in one result page
const MAX_PER_SITE: usize = 3;

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
        // one shared reader that reloads on commit; building a fresh reader
        // per query stalls on segment merges
        let reader = index
            .reader_builder()
            .reload_policy(tantivy::ReloadPolicy::OnCommitWithDelay)
            .try_into()?;
        let writer = index.writer(heap_mb * 1024 * 1024)?;

        Ok(Self {
            index,
            reader,
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
        let w = self.writer.read().unwrap();
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

    /// true when a doc with this exact url is in the committed index
    pub fn has_url(&self, url: &str) -> bool {
        let _ = self.reader.reload();
        let searcher = self.reader.searcher();
        let q = TermQuery::new(
            Term::from_field_text(self.url, url),
            IndexRecordOption::WithFreqs,
        );
        searcher
            .search(&q, &TopDocs::with_limit(1).order_by_score())
            .map(|h| !h.is_empty())
            .unwrap_or(false)
    }

    pub fn delete_url(&self, url: &str) -> anyhow::Result<()> {
        let w = self.writer.read().unwrap();
        w.delete_term(Term::from_field_text(self.url, url));
        Ok(())
    }

    pub fn commit(&self) -> anyhow::Result<()> {
        self.writer.write().unwrap().commit()?;
        Ok(())
    }

    /// did-you-mean: for query terms absent from the dictionary, find the
    /// closest indexed term (levenshtein <= 2 via fst automaton) with real
    /// document frequency. returns the corrected query, or None
    pub fn suggest(&self, q: &str) -> Option<String> {
        let searcher = self.reader.searcher();
        let words: Vec<String> = q
            .split(|c: char| !c.is_alphanumeric())
            .map(|w| w.to_lowercase())
            .filter(|w| w.len() >= 4)
            .collect();
        if words.is_empty() {
            return None;
        }

        let df = |w: &str| -> u64 {
            let mut n = 0u64;
            for seg in searcher.segment_readers() {
                if let Ok(inv) = seg.inverted_index(self.body)
                    && let Ok(Some(ti)) = inv.terms().get(w.as_bytes())
                {
                    n += ti.doc_freq as u64;
                }
            }
            n
        };

        let mut fixed = words.clone();
        let mut changed = false;
        for (i, w) in words.iter().enumerate() {
            if df(w) > 0 {
                continue;
            }
            // tighter distance on short words - "cat"->"car" is noise
            let dist = if w.len() <= 5 { 1 } else { 2 };
            let mut best: Option<(u64, String)> = None;
            let prefix = &w[..2.min(w.len())];
            for seg in searcher.segment_readers() {
                let Ok(inv) = seg.inverted_index(self.body) else {
                    continue;
                };
                let Ok(mut stream) = inv.terms().range().ge(prefix.as_bytes()).into_stream() else {
                    continue;
                };
                while let Some((term, info)) = stream.next() {
                    if !term.starts_with(prefix.as_bytes()) {
                        break;
                    }
                    let Ok(ts) = std::str::from_utf8(term) else {
                        continue;
                    };
                    if levenshtein(w, ts, dist) <= dist {
                        let dfv = info.doc_freq as u64;
                        if dfv > best.as_ref().map(|b| b.0).unwrap_or(0) {
                            best = Some((dfv, ts.to_string()));
                        }
                    }
                }
            }
            if let Some((_, cand)) = best
                && cand != *w
            {
                fixed[i] = cand;
                changed = true;
            }
        }
        changed.then(|| fixed.join(" "))
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

    fn build_parser(&self, conjunction: bool) -> QueryParser {
        let mut qp =
            QueryParser::for_index(&self.index, vec![self.title, self.body, self.description]);
        if conjunction {
            qp.set_conjunction_by_default();
        }
        qp.set_field_boost(self.title, 4.0);
        qp.set_field_boost(self.description, 2.0);
        qp
    }

    fn parse_lenient(
        &self,
        text: &str,
        conjunction: bool,
    ) -> Option<Box<dyn tantivy::query::Query>> {
        if text.trim().is_empty() {
            return None;
        }
        let qp = self.build_parser(conjunction);
        if let Ok(q) = qp.parse_query(text) {
            return Some(q);
        }
        let stripped = strip_unknown_fields(text);
        if let Ok(q) = qp.parse_query(&stripped) {
            return Some(q);
        }
        let clean = sanitize_query(&stripped);
        qp.parse_query(&clean).ok()
    }

    pub fn search_kind(
        &self,
        q: &str,
        limit: usize,
        kind: Option<&str>,
    ) -> anyhow::Result<Vec<SearchHit>> {
        let spec = parse_query_spec(q);
        // operator-only queries like site:example.com leave an empty text;
        // match everything on that host with a broad term query per field
        let text = spec.text.clone();
        let all_query = text.is_empty()
            && (!spec.site_includes.is_empty()
                || !spec.site_excludes.is_empty()
                || !spec.inurl_terms.is_empty()
                || spec.after.is_some()
                || spec.before.is_some());

        // operator-only queries bypass the text parser. site: gets a host
        // regex so it matches the whole host set rather than a scored
        // sample; anything else walks the whole index and filters
        let query: Box<dyn tantivy::query::Query> = if all_query && !spec.site_includes.is_empty() {
            let mut clauses: Vec<(Occur, Box<dyn tantivy::query::Query>)> = Vec::new();
            for d in &spec.site_includes {
                let pat = format!("(.*\\.)?{}", regex_escape(d));
                if let Ok(rq) = tantivy::query::RegexQuery::from_pattern(&pat, self.host) {
                    clauses.push((
                        Occur::Should,
                        Box::new(rq) as Box<dyn tantivy::query::Query>,
                    ));
                }
            }
            if clauses.is_empty() {
                Box::new(tantivy::query::AllQuery)
            } else {
                Box::new(BooleanQuery::new(clauses))
            }
        } else if all_query {
            Box::new(tantivy::query::AllQuery)
        } else {
            let Some(q) = self.parse_lenient(&text, true) else {
                return Ok(Vec::new());
            };
            q
        };
        // OnCommitWithDelay reloads async; pull in fresh commits explicitly so
        // searches right after a commit do not see a stale segment list
        let _ = self.reader.reload();
        let searcher = self.reader.searcher();

        // operator-only queries have no score signal to lean on, so scan the
        // whole index rather than a top-k sample
        let top_n = if all_query {
            (searcher.num_docs() as usize).clamp(200, 20_000)
        } else {
            (limit * 6).max(200)
        };
        let mut hits: Vec<(f32, DocAddress)> =
            searcher.search(&*query, &TopDocs::with_limit(top_n).order_by_score())?;

        // navigational lane: a short query that names a site must see that
        // site's docs even when bm25 starves them - the term is ubiquitous
        // in its own corpus so it never scores. pull host-term matches in
        // alongside the scored set and let the nav floor in collect() lift
        // the right one
        let nav_terms: Vec<String> = spec
            .text
            .split(|c: char| !c.is_alphanumeric())
            .filter(|t| t.len() >= 4 && t.len() <= 20)
            .map(|t| t.to_lowercase())
            .collect();
        // multi-term queries can still hit a poisoned host like
        // privacy.com - navigational rules only apply to one-word intent
        if nav_terms.len() == 1 {
            // subdomain-aware: en.wikipedia.org counts for "wikipedia"
            let mut clauses: Vec<(Occur, Box<dyn tantivy::query::Query>)> = Vec::new();
            for t in &nav_terms {
                let pat = format!(
                    "(.*\\.)?{}\\.(com|org|net|io|dev|app|network|co|gg|tv)",
                    regex_escape(t)
                );
                if let Ok(rq) = tantivy::query::RegexQuery::from_pattern(&pat, self.host) {
                    clauses.push((
                        Occur::Should,
                        Box::new(rq) as Box<dyn tantivy::query::Query>,
                    ));
                }
            }
            if !clauses.is_empty() {
                let nav = BooleanQuery::new(clauses);
                if let Ok(nav_hits) =
                    searcher.search(&nav, &TopDocs::with_limit(20).order_by_score())
                {
                    for (s0, addr) in nav_hits {
                        if !hits.iter().any(|(_, a)| *a == addr) {
                            hits.push((s0, addr));
                        }
                    }
                }
            }
        }

        // disjunction fallback only when the strict query finds nothing:
        // a single stray word should never mean "no results". for multi
        // word queries first require at least two terms to match so the
        // fallback does not surface single-word noise
        if hits.is_empty() {
            if let Some(two) = self.min_terms_query(&text, 2) {
                let alt: Vec<(f32, DocAddress)> =
                    searcher.search(&*two, &TopDocs::with_limit(top_n).order_by_score())?;
                if !alt.is_empty() {
                    let mut snipper = SnippetGenerator::create(&searcher, &*two, self.body)?;
                    snipper.set_max_num_chars(240);
                    return self.collect(&searcher, alt, &spec, kind, limit, Some(&snipper));
                }
            }
            if let Some(or_query) = self
                .anchored_query(&text)
                .or_else(|| self.parse_lenient(&text, false))
            {
                let alt: Vec<(f32, DocAddress)> =
                    searcher.search(&*or_query, &TopDocs::with_limit(top_n).order_by_score())?;
                if alt.len() > hits.len() {
                    hits = alt;
                    let mut snipper = SnippetGenerator::create(&searcher, &*or_query, self.body)?;
                    snipper.set_max_num_chars(240);
                    return self.collect(&searcher, hits, &spec, kind, limit, Some(&snipper));
                }
            }
        }

        let mut snipper = SnippetGenerator::create(&searcher, &*query, self.body)?;
        snipper.set_max_num_chars(240);
        self.collect(&searcher, hits, &spec, kind, limit, Some(&snipper))
    }

    /// union of all 2-term conjunctions over the query tokens: the
    /// "most words must match" tier between strict AND and full OR
    fn min_terms_query(&self, text: &str, min: usize) -> Option<Box<dyn tantivy::query::Query>> {
        let mut toks: Vec<String> = text
            .split(|c: char| !c.is_alphanumeric())
            .filter(|t| t.len() >= 2)
            .map(|t| t.to_lowercase())
            .collect();
        toks.sort();
        toks.dedup();
        if toks.len() < min || toks.len() > 6 {
            return None;
        }
        let term_any = |t: &str| -> Box<dyn tantivy::query::Query> {
            Box::new(BooleanQuery::new(vec![
                (
                    Occur::Should,
                    Box::new(TermQuery::new(
                        Term::from_field_text(self.title, t),
                        IndexRecordOption::WithFreqs,
                    )) as Box<dyn tantivy::query::Query>,
                ),
                (
                    Occur::Should,
                    Box::new(TermQuery::new(
                        Term::from_field_text(self.body, t),
                        IndexRecordOption::WithFreqs,
                    )) as Box<dyn tantivy::query::Query>,
                ),
            ]))
        };
        let mut clauses: Vec<(Occur, Box<dyn tantivy::query::Query>)> = Vec::new();
        for i in 0..toks.len() {
            for j in (i + 1)..toks.len() {
                clauses.push((
                    Occur::Should,
                    Box::new(BooleanQuery::new(vec![
                        (Occur::Must, term_any(&toks[i])),
                        (Occur::Must, term_any(&toks[j])),
                    ])) as Box<dyn tantivy::query::Query>,
                ));
            }
        }
        Some(Box::new(BooleanQuery::new(clauses)))
    }

    /// disjunction anchored on the rarest indexed query term - that term
    /// must appear, the rest are SHOULD, so niche entities do not get
    /// drowned by generic pages matching only the common word
    fn anchored_query(&self, text: &str) -> Option<Box<dyn tantivy::query::Query>> {
        let mut toks: Vec<String> = text
            .split(|c: char| !c.is_alphanumeric())
            .filter(|t| t.len() >= 2)
            .map(|t| t.to_lowercase())
            .collect();
        toks.sort();
        toks.dedup();
        if toks.is_empty() {
            return None;
        }
        let searcher = self.reader.searcher();
        let df = |t: &str| -> u64 {
            let mut n = 0u64;
            for seg in searcher.segment_readers() {
                for f in [self.title, self.body] {
                    if let Ok(inv) = seg.inverted_index(f)
                        && let Ok(Some(ti)) = inv.terms().get(t.as_bytes())
                    {
                        n += ti.doc_freq as u64;
                    }
                }
            }
            n
        };
        // rarest term with any docs is the anchor; absent anchors mean
        // there is nothing indexable to anchor on
        toks.sort_by_key(|t| df(t));
        let anchor = toks.iter().find(|t| df(t) > 0)?.clone();
        let term_any = |t: &str| -> Box<dyn tantivy::query::Query> {
            Box::new(BooleanQuery::new(vec![
                (
                    Occur::Should,
                    Box::new(TermQuery::new(
                        Term::from_field_text(self.title, t),
                        IndexRecordOption::WithFreqs,
                    )) as Box<dyn tantivy::query::Query>,
                ),
                (
                    Occur::Should,
                    Box::new(TermQuery::new(
                        Term::from_field_text(self.body, t),
                        IndexRecordOption::WithFreqs,
                    )) as Box<dyn tantivy::query::Query>,
                ),
            ]))
        };
        let mut clauses: Vec<(Occur, Box<dyn tantivy::query::Query>)> =
            vec![(Occur::Must, term_any(&anchor))];
        for t in &toks {
            if *t != anchor {
                clauses.push((Occur::Should, term_any(t)));
            }
        }
        Some(Box::new(BooleanQuery::new(clauses)))
    }

    fn collect(
        &self,
        searcher: &tantivy::Searcher,
        hits: Vec<(f32, DocAddress)>,
        spec: &QuerySpec,
        kind: Option<&str>,
        limit: usize,
        snipper: Option<&SnippetGenerator>,
    ) -> anyhow::Result<Vec<SearchHit>> {
        /* collect more than the page size so the diversity pass can pick a
        spread of senses instead of the raw bm25 order */
        let pool = (limit * 4).max(20);
        let mut out: Vec<SearchHit> = Vec::with_capacity(pool);
        let mut per_site: HashMap<String, usize> = HashMap::new();
        // regional mirrors share title text across site keys (amazon.sg vs
        // amazon.com) so dedupe on normalized title as well as site
        let mut seen_titles: std::collections::HashSet<String> = std::collections::HashSet::new();
        let now = now_epoch();
        for (bm25, addr) in hits {
            if out.len() >= pool {
                break;
            }
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
            let fetched = doc
                .get_first(self.fetched_at)
                .and_then(|v| v.as_u64())
                .unwrap_or(now);
            if !spec.matches(&host, &url, fetched) {
                continue;
            }
            if spec.site_includes.is_empty() {
                let site = crate::urlnorm::site_key(&host);
                let seen = per_site.entry(site).or_insert(0);
                if *seen >= MAX_PER_SITE {
                    continue;
                }
                *seen += 1;
            }

            let title = doc
                .get_first(self.title)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            // regional mirrors differ only by a host suffix like
            // ": Amazon.sg: Books" - strip the doc's own host labels from
            // the normalized key and compare on a prefix so truncation
            // variants collapse too
            let mut title_norm: String = title
                .chars()
                .filter(|c| c.is_alphanumeric() || c.is_whitespace())
                .flat_map(|c| c.to_lowercase())
                .collect();
            for label in host.split('.') {
                let mut stripped = String::new();
                for word in title_norm.split_whitespace() {
                    if word != label && word != "www" {
                        stripped.push_str(word);
                    }
                }
                title_norm = stripped;
            }
            let title_key: String = title_norm.chars().take(48).collect();
            if !title_key.is_empty() && !seen_titles.insert(title_key) {
                continue;
            }
            let description = doc
                .get_first(self.description)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let authority = doc
                .get_first(self.authority)
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);

            let authority_boost = 1.0 + authority.min(10.0) * 0.15;
            let age_days = now.saturating_sub(fetched) as f64 / 86400.0;
            // news-shaped queries get a much steeper freshness weight - a
            // 2019 page is not a good answer for "election results today"
            let newsy = query_is_newsy(&spec.text);
            let freshness = if doc_kind == "rss" {
                let half = if newsy { 7.0 } else { 60.0 };
                1.0 / (1.0 + (age_days / half).max(0.0) * 0.4)
            } else {
                let half = if newsy { 30.0 } else { 365.0 };
                1.0 / (1.0 + (age_days / half).max(0.0) * 0.2)
            };
            let host_boost = host_quality(&host);
            let title_boost = title_query_boost(&title, &spec.text);
            let url_boost = url_query_boost(&url, &spec.text);
            let seo_penalty = seo_title_penalty(&title);
            let nav_boost = nav_host_boost(&host, &url, &spec.text);

            let generated = snipper.map(|s| s.snippet_from_doc(&doc));
            let body_text = doc
                .get_first(self.body)
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let lead: String = body_text.chars().take(240).collect();
            let snippet = if !description.is_empty() && terms_in(&description, &spec.text) {
                description.clone()
            } else if terms_in(&lead, &spec.text) {
                lead.trim().to_string()
            } else if let Some(g) = generated
                && !g.fragment().trim().is_empty()
            {
                g.fragment().trim().to_string()
            } else if !description.is_empty() {
                description.clone()
            } else {
                title.clone()
            };

            let mut score = bm25
                * authority_boost as f32
                * freshness as f32
                * host_boost
                * title_boost
                * url_boost
                * nav_boost
                * seo_penalty;
            // exact navigational match - the named site must rank first even
            // when bm25 is tiny because the term is ubiquitous in its corpus
            if nav_boost >= 6.0 {
                score = score.max(300.0);
            }
            out.push(SearchHit {
                url,
                host,
                title,
                description,
                snippet,
                kind: doc_kind,
                score,
                fetched_at: fetched,
            });
        }
        out.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        diversify_senses(&mut out, &spec.text, limit);
        Ok(out)
    }

    pub fn num_docs(&self) -> u64 {
        let _ = self.reader.reload();
        self.reader.searcher().num_docs()
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
    const REFERENCE: &[&str] = &[
        "wikipedia.org",
        "wiktionary.org",
        "wikibooks.org",
        "wikisource.org",
        "wikiversity.org",
        "wikivoyage.org",
        "wikiquote.org",
        "mozilla.org",
        "developer.mozilla.org",
        "archlinux.org",
        "debian.org",
        "wiki.debian.org",
        "gentoo.org",
        "nixos.org",
        "kernel.org",
        "gnu.org",
        "ietf.org",
        "rfc-editor.org",
        "w3.org",
        "man7.org",
        "archive.org",
    ];
    const DEVDOCS: &[&str] = &[
        "rust-lang.org",
        "doc.rust-lang.org",
        "docs.rs",
        "python.org",
        "docs.python.org",
        "nodejs.org",
        "apache.org",
        "nginx.org",
        "postgresql.org",
        "mysql.com",
        "sqlite.org",
        "go.dev",
        "pkg.go.dev",
        "crates.io",
        "pypi.org",
        "npmjs.com",
        "cppreference.com",
        "learn.microsoft.com",
        "readthedocs.io",
        "ziglang.org",
        "llvm.org",
        "kotlinlang.org",
        "haskell.org",
        "elixir-lang.org",
        "clojure.org",
        "ruby-lang.org",
        "perl.org",
        "php.net",
        "openjdk.org",
        "docker.com",
        "kubernetes.io",
        "manpages.debian.org",
        "devdocs.io",
        "mdn.org",
        "firefox.com",
        "stackoverflow.com",
        "stackexchange.com",
        "serverfault.com",
        "superuser.com",
        "askubuntu.com",
    ];
    const FORGE: &[&str] = &[
        "github.com",
        "gitlab.com",
        "codeberg.org",
        "sr.ht",
        "sourcehut.org",
        "git.kernel.org",
        "bitbucket.org",
    ];
    const GAMEWIKI: &[&str] = &[
        "wiki.gg",
        "minecraft.wiki",
        "pcgamingwiki.com",
        "strategywiki.org",
        "tcrf.net",
    ];
    const VIDEO: &[&str] = &["youtube.com", "odysee.com", "peertube"];
    // engagement-bait and seo-heavy domains get a mild derank, not a ban
    const FARMS: &[&str] = &[
        "medium.com",
        "quora.com",
        "pinterest.com",
        "forbes.com",
        "businessinsider.com",
        "buzzfeed.com",
        "hubpages.com",
        "ehow.com",
        "livestrong.com",
        "w3schools.com",
        "geeksforgeeks.org",
        "tutorialspoint.com",
        "javatpoint.com",
        "chegg.com",
        "coursehero.com",
        "studypool.com",
        "answers.com",
        "wikihow.com",
        "investopedia.com",
        "healthline.com",
        "verywellmind.com",
        "verywellhealth.com",
        "webmd.com",
        "cnet.com",
        "zdnet.com",
        "tomsguide.com",
        "tomshardware.com",
        "techradar.com",
        "livescience.com",
        "space.com",
        "gamesradar.com",
        "pcgamer.com",
        "ign.com",
        "gamespot.com",
        "polygon.com",
        "screenrant.com",
        "gamerant.com",
        "cbr.com",
        "thegamer.com",
        "fandom.com",
        "breezometer.com",
        "weather.com",
        "accuweather.com",
        "msn.com",
        "yahoo.com",
        "aol.com",
        "nypost.com",
        "dailymail.co.uk",
        "the-sun.com",
        "thesun.co.uk",
        "mirror.co.uk",
        "express.co.uk",
        "reddit.com",
    ];
    if REFERENCE.iter().any(|d| host_eq(h, d)) {
        1.4
    } else if DEVDOCS.iter().any(|d| host_eq(h, d)) {
        1.3
    } else if GAMEWIKI.iter().any(|d| host_eq(h, d)) {
        1.25
    } else if FORGE.iter().any(|d| host_eq(h, d)) {
        1.2
    } else if VIDEO.iter().any(|d| host_eq(h, d)) {
        1.1
    } else if FARMS.iter().any(|d| host_eq(h, d)) {
        0.65
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

fn query_terms(q: &str) -> Vec<String> {
    q.split_whitespace()
        .map(|t| {
            t.trim_matches(|c: char| !c.is_alphanumeric())
                .to_lowercase()
        })
        .filter(|t| !t.is_empty())
        .collect()
}

fn terms_in(hay: &str, q: &str) -> bool {
    let h = hay.to_lowercase();
    query_terms(q).iter().all(|t| h.contains(&**t))
}

fn title_query_boost(title: &str, q: &str) -> f32 {
    let t = title.to_lowercase();
    let terms = query_terms(q);
    if terms.is_empty() {
        return 1.0;
    }
    let hits = terms.iter().filter(|term| t.contains(&***term)).count();
    let mut boost = 1.0 + (hits as f32 / terms.len() as f32) * 0.4;
    // exact phrase in the title is the strongest relevance signal we have
    if terms.len() > 1 {
        let phrase = terms.join(" ");
        if t.contains(&phrase) {
            boost += 0.35;
        }
    }
    boost
}

/// terms present in the url slug correlate strongly with intent ("how to x"
/// matching /how-to-x pages)
fn url_query_boost(url: &str, q: &str) -> f32 {
    let terms = query_terms(q);
    if terms.is_empty() {
        return 1.0;
    }
    let slug = url
        .split(&['/', '?', '#'][..])
        .next_back()
        .unwrap_or("")
        .to_lowercase();
    let path = url.to_lowercase();
    let hits = terms
        .iter()
        .filter(|t| slug.contains(&***t) || path.contains(&***t))
        .count();
    1.0 + (hits as f32 / terms.len() as f32) * 0.2
}

/// disambiguation: bucket hits by a cheap sense signature so one meaning
/// of an ambiguous term cannot fill the whole page. the signature is the
/// first long title token that is not part of the query plus the site
/// label - different senses of "ccd" land in different buckets because
/// their titles lead with different words
fn sense_key(hit: &SearchHit, query: &str) -> String {
    let site = {
        let h = hit.host.trim_start_matches("www.");
        let mut parts = h.split('.').collect::<Vec<_>>();
        if parts.len() >= 2 {
            parts.pop();
        }
        parts.join(".")
    };
    let qwords: std::collections::HashSet<String> = query
        .split(|c: char| !c.is_alphanumeric())
        .map(|t| t.to_lowercase())
        .filter(|t| t.len() >= 2)
        .collect();
    let mut sig = String::new();
    for t in hit.title.split(|c: char| !c.is_alphanumeric()) {
        let t = t.to_lowercase();
        if t.len() >= 4 && !qwords.contains(&t) && !is_stopword(&t) {
            sig = t;
            break;
        }
    }
    format!("{}:{}", site, sig)
}

fn is_stopword(t: &str) -> bool {
    matches!(
        t,
        "the"
            | "this"
            | "that"
            | "with"
            | "from"
            | "your"
            | "what"
            | "when"
            | "where"
            | "which"
            | "about"
            | "how"
            | "why"
            | "and"
            | "for"
            | "guide"
            | "best"
            | "top"
            | "review"
            | "reviews"
            | "new"
            | "2024"
            | "2025"
            | "2026"
            | "page"
            | "site"
            | "website"
            | "home"
    )
}

/// interleave so the front of the page samples distinct senses: in the
/// first half of the results no sense bucket may take more than a third
/// of the slots, the rest keep bm25 order in the back
fn diversify_senses(out: &mut Vec<SearchHit>, query: &str, limit: usize) {
    if out.len() <= 6 {
        out.truncate(limit);
        return;
    }
    let front_cap = (limit / 2).max(4);
    let per_sense = ((front_cap + 1) / 3).max(2);
    let mut counts: HashMap<String, usize> = HashMap::new();
    let mut front: Vec<SearchHit> = Vec::with_capacity(front_cap);
    let mut rest: Vec<SearchHit> = Vec::new();
    for h in out.drain(..) {
        let k = sense_key(&h, query);
        let c = counts.entry(k).or_insert(0);
        if front.len() < front_cap && *c < per_sense {
            *c += 1;
            front.push(h);
        } else {
            rest.push(h);
        }
    }
    out.extend(front);
    out.extend(rest);
    out.truncate(limit);
}

/// navigational intent: a short query that names a site should put that
/// site first. exact host-label match wins big; an exact url path segment
/// match ("github.com/owner/Repo") wins moderately
fn nav_host_boost(host: &str, url: &str, q: &str) -> f32 {
    let terms = query_terms(q);
    if terms.is_empty() || terms.len() > 3 {
        return 1.0;
    }
    let labels: Vec<&str> = host.split('.').collect();
    let name = if labels.len() >= 2 {
        labels[labels.len() - 2]
    } else {
        host
    };
    if terms.len() == 1 && name.eq_ignore_ascii_case(&terms[0]) {
        return 6.0;
    }
    for seg in url.split('/') {
        let seg = seg.trim_matches(|c: char| !c.is_alphanumeric());
        for t in &terms {
            if t.len() >= 4 && seg.eq_ignore_ascii_case(t) {
                return 1.9;
            }
        }
    }
    1.0
}

/// title patterns that usually signal listicle/seo content get a gentle
/// penalty: multiple separator-heavy segments plus commerce keywords
fn seo_title_penalty(title: &str) -> f32 {
    let t = title.to_lowercase();
    let seps = t.matches(" - ").count()
        + t.matches(" | ").count()
        + t.matches(": ").count()
        + t.matches("·").count();
    if seps < 2 {
        return 1.0;
    }
    const WORDS: &[&str] = &[
        "best",
        "top 10",
        "top 5",
        "review",
        "reviews",
        "buy",
        "cheap",
        "discount",
        "coupon",
        "deal",
        "deals",
        "vs",
        "alternatives",
        "2025",
        "2026",
        "2027",
    ];
    let hits = WORDS.iter().filter(|w| t.contains(**w)).count();
    if hits >= 2 { 0.7 } else { 1.0 }
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

/// classic levenshtein with an early-out distance cap
fn levenshtein(a: &str, b: &str, max: u32) -> u32 {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    if a.len().abs_diff(b.len()) > max as usize {
        return u32::MAX;
    }
    let mut prev: Vec<u32> = (0..=b.len() as u32).collect();
    let mut cur = vec![0u32; b.len() + 1];
    for (i, ca) in a.iter().enumerate() {
        cur[0] = i as u32 + 1;
        for (j, cb) in b.iter().enumerate() {
            cur[j + 1] = (prev[j + 1] + 1)
                .min(cur[j] + 1)
                .min(prev[j] + if ca == cb { 0 } else { 1 });
        }
        std::mem::swap(&mut prev, &mut cur);
    }
    prev[b.len()]
}

/// queries that mean "what's happening" get a steeper freshness weight
fn query_is_newsy(text: &str) -> bool {
    let t = text.to_lowercase();
    for w in [
        "news",
        "latest",
        "breaking",
        "today",
        "yesterday",
        "tonight",
        "announced",
        "launch",
        "release",
        "deadline",
        "update",
        "election",
        "results",
        "score",
        "weather",
        "forecast",
        "stock",
        "price",
    ] {
        if t.contains(w) {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nav_host_boost_single_term() {
        // single-term entity query - the named site wins
        assert_eq!(
            nav_host_boost(
                "en.wikipedia.org",
                "https://en.wikipedia.org/wiki/X",
                "wikipedia"
            ),
            6.0
        );
        assert_eq!(
            nav_host_boost(
                "reticulum.network",
                "https://reticulum.network/",
                "reticulum"
            ),
            6.0
        );
        // multi-term: no host floor, only path-segment boost
        assert_eq!(
            nav_host_boost("privacy.com", "https://privacy.com/", "bazzell privacy"),
            1.0
        );
        // path segment exact match gives the small boost
        assert_eq!(
            nav_host_boost(
                "github.com",
                "https://github.com/owner/Reticulum",
                "reticulum"
            ),
            1.9
        );
        // unrelated host
        assert_eq!(
            nav_host_boost("example.com", "https://example.com/a", "wikipedia"),
            1.0
        );
    }

    #[test]
    fn site_ops_extracted() {
        let s = parse_query_spec("rust site:docs.rs -site:reddit.com");
        assert_eq!(s.site_includes, vec!["docs.rs"]);
        assert_eq!(s.site_excludes, vec!["reddit.com"]);
        assert_eq!(s.text, "rust");
        let s = parse_query_spec("site:en.wikipedia.org");
        assert_eq!(s.text, "");
    }

    #[test]
    fn quoted_site_not_operator() {
        let s = parse_query_spec("\"site:foo\" bar");
        assert!(s.site_includes.is_empty());
    }

    #[test]
    fn date_ops() {
        let s = parse_query_spec("rust before:2024-06 after:2020");
        assert!(s.before.is_some());
        assert!(s.after.is_some());
        assert_eq!(s.text, "rust");
    }

    #[test]
    fn unknown_fields_stripped() {
        assert_eq!(strip_unknown_fields("define:love foo"), "love foo");
        assert_eq!(strip_unknown_fields("title:rust lang"), "title:rust lang");
        assert_eq!(strip_unknown_fields("-define:love"), "-love");
    }

    #[test]
    fn spec_filters() {
        let s = parse_query_spec("x site:docs.rs inurl:std before:2030-01-01");
        assert!(s.matches("docs.rs", "https://docs.rs/std/x", 1700000000));
        assert!(s.matches("docs.rs", "https://docs.rs/std/x", 0));
        assert!(!s.matches("other.rs", "https://docs.rs/std/x", 0));
        assert!(!s.matches("docs.rs", "https://docs.rs/core/x", 0));
        let s = parse_query_spec("x -site:reddit.com");
        assert!(!s.matches("old.reddit.com", "https://old.reddit.com/a", 0));
        assert!(s.matches("example.com", "https://example.com/a", 0));
    }

    #[test]
    fn sanitize_works() {
        assert_eq!(sanitize_query("foo (bar] \"baz"), "foo bar baz");
    }
}
