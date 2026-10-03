use clap::{Parser, Subcommand};
use lyra_index::config::Config;
use lyra_index::{crawl, extract, fetcher, server};
use std::path::PathBuf;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(
    name = "lyra-index",
    version,
    about = "Crawler and Tantivy index for Seek"
)]
struct Cli {
    #[arg(short, long, default_value = "crawler.toml")]
    config: PathBuf,

    #[command(subcommand)]
    command: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// run the crawl loop
    Crawl {
        /// seed urls file, one per line, or repeat --seed
        #[arg(long)]
        seeds_file: Option<PathBuf>,
        #[arg(long)]
        seed: Vec<String>,
        #[arg(long)]
        max_docs: Option<u64>,
        #[arg(long)]
        max_bytes: Option<u64>,
    },
    /// serve the search api
    Serve,
    /// crawl and serve the api in one process
    Run {
        #[arg(long)]
        seeds_file: Option<PathBuf>,
        #[arg(long)]
        seed: Vec<String>,
        #[arg(long)]
        max_docs: Option<u64>,
        #[arg(long)]
        max_bytes: Option<u64>,
    },
    /// print index/frontier counters
    Stats,
    /// merge tantivy segments, drop dead files, compact redb
    Compact,
    /// drop oldest documents until the keep/size/age caps hold, then compact
    Prune {
        #[arg(long)]
        keep_docs: Option<u64>,
        #[arg(long)]
        max_bytes: Option<u64>,
        #[arg(long)]
        max_age_days: Option<u64>,
    },
    /// one-off fetch of a single url, prints extracted fields
    Fetch { url: String },
    /// dump stored documents - --format jsonl is our pipe format, --format
    /// hister writes the exact bracketed layout hister import file expects
    /// (one {..} per line, commas on their own lines)
    Export {
        #[arg(short, long, default_value = "docs.jsonl")]
        out: PathBuf,
        #[arg(short, long, default_value = "jsonl", value_parser = ["jsonl", "hister"])]
        format: String,
    },
    /// import documents into the index - accepts our jsonl and hister export
    /// files (hister field names url/domain/text/added map onto ours)
    Import {
        #[arg(short, long)]
        input: PathBuf,
    },
    /// push every stored doc to a running hister server - a backfill for
    /// when live push was off or the server was down
    PushHister,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let mut cfg = if cli.config.exists() {
        Config::load(&cli.config)?
    } else {
        let mut c = Config::default();
        c.apply_env();
        c
    };

    match cli.command {
        Cmd::Crawl {
            seeds_file,
            seed,
            max_docs,
            max_bytes,
        } => {
            apply_size_caps(&mut cfg, max_docs, max_bytes);
            let seeds = collect_seeds(&cfg, seeds_file, seed)?;
            let crawler = Arc::new(crawl::Crawler::new(cfg.clone())?);
            let n = crawler.seed(&seeds);
            tracing::info!(n, "seeded frontier");
            lyra_index::sandbox::apply(&cfg.store.path);
            crawler.run().await;
        }
        Cmd::Serve => {
            let crawler = crawl::Crawler::new(cfg.clone())?;
            let state = server::AppState::from_crawler(&crawler)?;
            server::serve(state, &cfg.api.listen).await?;
        }
        Cmd::Run {
            seeds_file,
            seed,
            max_docs,
            max_bytes,
        } => {
            apply_size_caps(&mut cfg, max_docs, max_bytes);
            let seeds = collect_seeds(&cfg, seeds_file, seed)?;
            let crawler = Arc::new(crawl::Crawler::new(cfg.clone())?);
            crawler.seed(&seeds);
            let state = server::AppState::from_crawler(&crawler)?;
            let listen = cfg.api.listen.clone();
            let api = tokio::spawn(async move { server::serve(state, &listen).await });
            let c2 = crawler.clone();
            let crawl_task = tokio::spawn(async move { c2.run().await });
            let _ = tokio::join!(api, crawl_task);
        }
        Cmd::Stats => {
            let crawler = crawl::Crawler::new(cfg)?;
            println!("docs stored: {}", crawler.store_doc_count());
            println!(
                "docs fetched this session: {}",
                crawler
                    .docs_done()
                    .load(std::sync::atomic::Ordering::Relaxed)
            );
            println!("index docs: {}", crawler.index().num_docs());
            println!("data bytes: {}", crawler.data_bytes());
        }
        Cmd::Compact => {
            let data = cfg.store.path.clone();
            {
                let crawler = crawl::Crawler::new(cfg)?;
                let segs = crawler.compact()?;
                println!("tantivy segments before merge: {segs}");
            }
            let shrunk = lyra_index::store::compact_file(&data)?;
            println!("redb compacted: {shrunk}");
        }
        Cmd::Prune {
            keep_docs,
            max_bytes,
            max_age_days,
        } => {
            if keep_docs.is_none() && max_bytes.is_none() && max_age_days.is_none() {
                anyhow::bail!("set --keep-docs, --max-bytes, or --max-age-days");
            }
            let data = cfg.store.path.clone();
            let n = {
                let crawler = crawl::Crawler::new(cfg)?;
                crawler.prune(keep_docs, max_bytes, max_age_days)?
            };
            let shrunk = lyra_index::store::compact_file(&data)?;
            println!("pruned {n} docs, redb compacted: {shrunk}");
        }
        Cmd::Export { out, format } => {
            let crawler = crawl::Crawler::new(cfg)?;
            let n = match format.as_str() {
                "hister" => crawler.export_hister(&out)?,
                _ => crawler.export_jsonl(&out)?,
            };
            println!("exported {n} docs to {}", out.display());
        }
        Cmd::PushHister => {
            let crawler = crawl::Crawler::new(cfg.clone())?;
            let hister = lyra_index::hister::HisterClient::new(
                &cfg.hister.url,
                &cfg.hister.token,
                reqwest::Client::new(),
                cfg.hister.batch_size,
            );
            if !hister.enabled() {
                anyhow::bail!("set [hister] url or LYRA_INDEX_HISTER_URL first");
            }
            let mut n = 0usize;
            for doc in crawler.store().iter_docs()? {
                let text = zstd::decode_all(doc.text_z.as_slice()).unwrap_or_default();
                hister.queue(
                    &doc.url,
                    &doc.host,
                    &doc.title,
                    &String::from_utf8_lossy(&text),
                    doc.fetched_at,
                );
                n += 1;
            }
            let sent = hister.flush().await?;
            println!("pushed {sent}/{n} docs to {}", cfg.hister.url);
        }
        Cmd::Import { input } => {
            let crawler = crawl::Crawler::new(cfg)?;
            let n = crawler.import_jsonl(&input)?;
            crawler.index().commit()?;
            println!("imported {n} docs from {}", input.display());
        }
        Cmd::Fetch { url } => {
            let fetcher = fetcher::Fetcher::new(&cfg.crawl, &cfg.identity.user_agent)?;
            let resp = fetcher.get(&url).await?;
            println!("status: {}", resp.status);
            println!("bytes: {}", resp.bytes);
            println!("ms: {}", resp.elapsed_ms);
            let parsed = extract::extract(&resp.body, &resp.final_url);
            println!("title: {}", parsed.title);
            println!("desc: {}", parsed.description);
            println!("links: {}", parsed.links.len());
        }
    }
    Ok(())
}

/// compiled-in seed list so a bare binary or image crawl does something
/// useful out of the box - same list as seeds.txt at the crate root
fn crawler_seed_defaults() -> Vec<String> {
    include_str!("../seeds.txt")
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .collect()
}

fn collect_seeds(
    cfg: &Config,
    seeds_file: Option<PathBuf>,
    seed: Vec<String>,
) -> anyhow::Result<Vec<String>> {
    let mut seeds = Vec::new();
    seeds.extend(cfg.seeds.iter().cloned());
    seeds.extend(seed);
    if let Some(f) = seeds_file {
        seeds.extend(
            std::fs::read_to_string(f)?
                .lines()
                .map(|l| l.trim().to_string())
                .filter(|l| !l.is_empty() && !l.starts_with('#')),
        );
    }
    if seeds.is_empty() {
        seeds = crawler_seed_defaults();
    }
    Ok(seeds)
}

fn apply_size_caps(cfg: &mut Config, max_docs: Option<u64>, max_bytes: Option<u64>) {
    if let Some(n) = max_docs {
        cfg.index.max_docs = n;
    }
    if let Some(n) = max_bytes {
        cfg.index.max_bytes = n;
    }
}
