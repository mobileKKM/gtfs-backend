//! Conditional GETs with truncation checks and retries, for the static and realtime pollers.
//!
//! ZTP sometimes serves a file while replacing it: a short body, an empty file or a 404 for a moment. The received
//! size is compared with Content-Length and the caller parses the body (zip / protobuf); those failures, 5xx, 404,
//! 408 and 429 are retried a few times. Other client errors fail at once.

use std::path::Path;
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use futures_util::StreamExt;
use reqwest::{Client, StatusCode, header};
use tokio::io::AsyncWriteExt;
use tracing::{info, warn};

/// Operators like a contact in it (a URL or an e-mail).
pub const USER_AGENT: &str = "mobileKKM-gtfs-backend/0.2";

pub fn client(timeout: f64, connect_timeout: f64) -> Result<Client> {
    Ok(Client::builder()
        .user_agent(USER_AGENT)
        .timeout(Duration::from_secs_f64(timeout))
        .connect_timeout(Duration::from_secs_f64(connect_timeout))
        .build()?)
}

#[derive(Debug)]
pub struct Fetched<T> {
    /// None: 304 Not Modified
    pub value: Option<T>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub size: usize,
    /// downloads tried, including the one that worked
    pub attempts: u32,
}

#[derive(Debug)]
pub struct HttpStatus(pub StatusCode);

impl std::fmt::Display for HttpStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HTTP {}", self.0)
    }
}

impl std::error::Error for HttpStatus {}

fn retryable(status: StatusCode) -> bool {
    status.is_server_error() || matches!(status.as_u16(), 404 | 408 | 429)
}

fn header_value(resp: &reqwest::Response, name: header::HeaderName) -> Option<String> {
    resp.headers().get(name).and_then(|v| v.to_str().ok()).map(str::to_string)
}

fn conditional(
    req: reqwest::RequestBuilder,
    etag: Option<&str>,
    last_modified: Option<&str>,
) -> reqwest::RequestBuilder {
    let req = match etag {
        Some(e) => req.header(header::IF_NONE_MATCH, e),
        None => req,
    };
    match last_modified {
        Some(m) => req.header(header::IF_MODIFIED_SINCE, m),
        None => req,
    }
}

/// Content-Length describes the encoded body: only compared when the body isn't compressed.
fn expected_length(resp: &reqwest::Response) -> Option<usize> {
    if resp.headers().contains_key(header::CONTENT_ENCODING) {
        return None;
    }
    header_value(resp, header::CONTENT_LENGTH).and_then(|v| v.parse().ok())
}

pub struct Retry {
    pub retries: u32,
    pub delay: f64,
}

/// GET a small file into memory and parse it, retrying as the module says. `timeout` limits each attempt.
pub async fn fetch_bytes<T>(
    client: &Client,
    url: &str,
    parse: impl Fn(&[u8]) -> Result<T>,
    etag: Option<&str>,
    last_modified: Option<&str>,
    retry: &Retry,
    timeout: Option<Duration>,
) -> Result<Fetched<T>> {
    let mut last: Option<anyhow::Error> = None;
    for n in 1..=retry.retries {
        let attempt = async {
            let mut req = conditional(client.get(url), etag, last_modified);
            if let Some(t) = timeout {
                req = req.timeout(t);
            }
            let resp = req.send().await?;
            if resp.status() == StatusCode::NOT_MODIFIED {
                let (etag, last_modified) = (etag.map(str::to_string), last_modified.map(str::to_string));
                return Ok(Fetched { value: None, etag, last_modified, size: 0, attempts: n });
            }
            if !resp.status().is_success() {
                return Err(anyhow::Error::new(HttpStatus(resp.status())));
            }
            let (e, m) = (header_value(&resp, header::ETAG), header_value(&resp, header::LAST_MODIFIED));
            let expected = expected_length(&resp);
            let body = resp.bytes().await?;
            if let Some(expected) = expected
                && expected != body.len()
            {
                bail!("truncated: received {} of {expected} bytes", body.len());
            }
            let value = parse(&body)?;
            Ok(Fetched { value: Some(value), etag: e, last_modified: m, size: body.len(), attempts: n })
        };
        match attempt.await {
            Ok(f) => return Ok(f),
            Err(e) => {
                if let Some(HttpStatus(status)) = e.downcast_ref::<HttpStatus>()
                    && !retryable(*status)
                {
                    return Err(e);
                }
                if n < retry.retries {
                    info!(url, attempt = n, error = %format!("{e:#}"), "fetch_retry");
                }
                last = Some(e);
            }
        }
        if n < retry.retries {
            tokio::time::sleep(Duration::from_secs_f64(retry.delay)).await;
        }
    }
    Err(last.unwrap_or_else(|| anyhow!("no attempt made")))
}

/// Stream a (large) file to `dest` and validate it, retrying as the module says. A download that keeps failing leaves
/// no file behind.
pub async fn fetch_to_file(
    client: &Client,
    url: &str,
    dest: &Path,
    validate: impl Fn(&Path) -> Result<()> + Clone + Send + 'static,
    etag: Option<&str>,
    last_modified: Option<&str>,
    retry: &Retry,
) -> Result<Fetched<()>> {
    if let Some(dir) = dest.parent() {
        tokio::fs::create_dir_all(dir).await?;
    }
    let mut last: Option<anyhow::Error> = None;
    for n in 1..=retry.retries {
        let attempt = async {
            let resp = conditional(client.get(url), etag, last_modified).send().await?;
            if resp.status() == StatusCode::NOT_MODIFIED {
                return Ok(Fetched {
                    value: None,
                    etag: etag.map(str::to_string),
                    last_modified: last_modified.map(str::to_string),
                    size: 0,
                    attempts: n,
                });
            }
            if !resp.status().is_success() {
                return Err(anyhow::Error::new(HttpStatus(resp.status())));
            }
            if resp.headers().contains_key(header::CONTENT_ENCODING) {
                bail!("compressed static downloads are not supported");
            }
            let (e, m) = (header_value(&resp, header::ETAG), header_value(&resp, header::LAST_MODIFIED));
            let expected = header_value(&resp, header::CONTENT_LENGTH).and_then(|v| v.parse::<usize>().ok());
            let mut file = tokio::fs::File::create(dest).await?;
            let mut received = 0;
            let mut stream = resp.bytes_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                received += chunk.len();
                file.write_all(&chunk).await?;
            }
            file.flush().await?;
            if let Some(expected) = expected
                && expected != received
            {
                bail!("truncated: received {received} of {expected} bytes");
            }
            let (v, path) = (validate.clone(), dest.to_path_buf());
            tokio::task::spawn_blocking(move || v(&path)).await??;
            Ok(Fetched { value: Some(()), etag: e, last_modified: m, size: received, attempts: n })
        };
        match attempt.await {
            Ok(f) => return Ok(f),
            Err(e) => {
                if let Some(HttpStatus(status)) = e.downcast_ref::<HttpStatus>()
                    && !retryable(*status)
                {
                    return Err(e);
                }
                if n < retry.retries {
                    warn!(url, attempt = n, error = %format!("{e:#}"), "fetch_retry");
                }
                last = Some(e);
            }
        }
        if n < retry.retries {
            tokio::time::sleep(Duration::from_secs_f64(retry.delay)).await;
        }
    }
    let _ = tokio::fs::remove_file(dest).await;
    Err(last.unwrap_or_else(|| anyhow!("no attempt made")))
}

/// A plain download to a file (the OSM extract), checked against Content-Length.
pub async fn download(client: &Client, url: &str, dest: &Path) -> Result<usize> {
    let resp = client.get(url).send().await?.error_for_status()?;
    let expected = header_value(&resp, header::CONTENT_LENGTH).and_then(|v| v.parse::<usize>().ok());
    let mut file = tokio::fs::File::create(dest).await?;
    let mut received = 0;
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        received += chunk.len();
        file.write_all(&chunk).await?;
    }
    file.flush().await?;
    if expected.is_some_and(|e| e != received) {
        bail!("truncated: received {received} of {expected:?} bytes");
    }
    Ok(received)
}
