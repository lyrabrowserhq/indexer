use crate::config::Challenge as ChallengeCfg;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{debug, info, warn};
use url::Url;

/// Anubis-style proof of work challenge (codeberg, freedesktop).
/// The page returns a JS challenge; we solve it without a browser by
/// replicating the sha256 pow: find nonce such that
/// sha256(challenge + nonce) starts with `difficulty` zero nibbles.
#[derive(Debug)]
pub struct AnubisChallenge {
    pub challenge: String,
    pub difficulty: u32,
    pub pass_url: String,
}

pub struct ChallengeSolver {
    cfg: ChallengeCfg,
    client: Client,
}

#[derive(Debug, Deserialize)]
struct AnubisJson {
    #[serde(default)]
    challenge: String,
    #[serde(default)]
    difficulty: u32,
    #[serde(default, rename = "rules")]
    _rules: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
struct FlareReq<'a> {
    cmd: &'a str,
    url: &'a str,
    #[serde(rename = "maxTimeout")]
    max_timeout: u64,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct FlareResp {
    pub status: String,
    pub solution: Option<FlareSolution>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct FlareSolution {
    pub url: String,
    pub status: u16,
    pub response: Option<String>,
    pub cookies: Vec<FlareCookie>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct FlareCookie {
    pub name: String,
    pub value: String,
}

impl ChallengeSolver {
    pub fn new(cfg: ChallengeCfg, client: Client) -> Self {
        Self { cfg, client }
    }

    /// detect an anubis challenge page and extract the pow parameters
    pub fn detect_anubis(body: &str, page_url: &Url) -> Option<AnubisChallenge> {
        if !(body.contains("anubis") || body.contains("within.website")) {
            return None;
        }
        // anubis embeds a json challenge blob in the page
        let start = body.find("window.__anubis_challenge__")?;
        let after = &body[start..];
        let brace = after.find('{')?;
        let end = find_json_end(&after[brace..])?;
        let json: AnubisJson = serde_json::from_str(&after[brace..brace + end]).ok()?;
        if json.challenge.is_empty() {
            return None;
        }
        let pass_url = format!(
            "{}://{}/.within.website/x/cmd/anubis/api/pass-challenge",
            page_url.scheme(),
            page_url.host_str()?
        );
        Some(AnubisChallenge {
            challenge: json.challenge,
            difficulty: json.difficulty.max(1),
            pass_url,
        })
    }

    /// solve sha256 pow. difficulty n means the hex digest must start with
    /// n zero nibbles (anubis convention).
    fn solve_pow(challenge: &str, difficulty: u32) -> Option<(String, u64)> {
        let target_zeros = difficulty.min(16);
        let mut nonce: u64 = 0;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
        loop {
            if std::time::Instant::now() > deadline || nonce > 1 << 40 {
                return None;
            }
            let input = format!("{challenge}{nonce}");
            let hash = Sha256::digest(input.as_bytes());
            let hex = hex::encode(hash);
            if hex.chars().take(target_zeros as usize).all(|c| c == '0') {
                return Some((hex, nonce));
            }
            nonce += 1;
        }
    }

    /// try to pass an anubis challenge for `page_url`, returns the cleared
    /// response body on success
    pub async fn solve_anubis(&self, body: &str, page_url: &Url) -> Option<String> {
        let ch = Self::detect_anubis(body, page_url)?;
        info!(host = %page_url.host_str().unwrap_or(""), difficulty = ch.difficulty, "solving anubis pow");
        let (hash, nonce) = Self::solve_pow(&ch.challenge, ch.difficulty)?;

        let pass = self
            .client
            .get(&ch.pass_url)
            .query(&[
                ("id", ch.challenge.clone()),
                ("response", hash),
                ("nonce", nonce.to_string()),
                ("redir", page_url.path().to_string()),
            ])
            .send()
            .await
            .ok()?;
        if !pass.status().is_success() && !pass.status().is_redirection() {
            warn!(status = %pass.status(), "anubis pass-challenge failed");
            return None;
        }
        // the cookie is now in the jar; re-fetch the original page
        let resp = self.client.get(page_url.as_str()).send().await.ok()?;
        if resp.status().is_success() {
            let text = resp.text().await.ok()?;
            // verify we actually passed - no challenge still present
            if !Self::is_challenge_page(&text) {
                return Some(text);
            }
        }
        None
    }

    fn is_challenge_page(body: &str) -> bool {
        body.contains("Making sure you're not a bot")
            || body.contains("window.__anubis_challenge__")
    }

    /// flaresolverr fallback for cloudflare-style js challenges
    pub async fn solve_flaresolverr(&self, url: &str) -> Option<String> {
        if self.cfg.flaresolverr_url.is_empty() {
            return None;
        }
        let req = FlareReq {
            cmd: "request.get",
            url,
            max_timeout: self.cfg.flaresolverr_timeout_ms,
        };
        let resp = self
            .client
            .post(format!(
                "{}/v1",
                self.cfg.flaresolverr_url.trim_end_matches('/')
            ))
            .json(&req)
            .send()
            .await
            .ok()?;
        let fr: FlareResp = resp.json().await.ok()?;
        if fr.status != "ok" {
            return None;
        }
        let sol = fr.solution?;
        debug!(status = sol.status, url = sol.url, "flaresolverr solved");
        sol.response
    }

    /// classify a fetched response: looks like a challenge page?
    pub fn looks_challenged(body: &str, status: u16) -> ChallengeKind {
        if body.contains("window.__anubis_challenge__")
            || body.contains("Making sure you're not a bot")
        {
            return ChallengeKind::Anubis;
        }
        if (status == 403 || status == 503)
            && (body.contains("cf-chl")
                || body.contains("challenge-platform")
                || body.contains("Just a moment"))
        {
            return ChallengeKind::Cloudflare;
        }
        ChallengeKind::None
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ChallengeKind {
    None,
    Anubis,
    Cloudflare,
}

fn find_json_end(s: &str) -> Option<usize> {
    let mut depth = 0i32;
    let mut in_str = false;
    let mut esc = false;
    for (i, c) in s.char_indices() {
        match c {
            '"' if !esc => in_str = !in_str,
            '\\' if in_str => esc = !esc,
            '{' if !in_str => depth += 1,
            '}' if !in_str => {
                depth -= 1;
                if depth == 0 {
                    return Some(i + 1);
                }
            }
            _ => {}
        }
        if c != '\\' {
            esc = false;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_anubis() {
        let body = r#"<html><script>window.__anubis_challenge__ = {"challenge":"abc123","difficulty":4,"rules":{}};</script></html>"#;
        let u = Url::parse("https://codeberg.org/x").unwrap();
        let ch = ChallengeSolver::detect_anubis(body, &u).unwrap();
        assert_eq!(ch.challenge, "abc123");
        assert_eq!(ch.difficulty, 4);
    }

    #[test]
    fn pow_roundtrip() {
        let (hash, _nonce) = ChallengeSolver::solve_pow("test-challenge", 2).unwrap();
        assert!(hash.starts_with("00"));
    }
}
