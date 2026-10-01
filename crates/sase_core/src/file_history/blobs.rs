//! Batched blob reads over one `git cat-file --batch` session.
//!
//! OIDs come from the index, so callers never move file bodies across
//! the core boundary twice. [`BlobCache`] is an optional LRU keyed by
//! OID: content-addressed present entries are never stale, so only
//! hits are stored and misses are always re-queried.

use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use super::runner::looks_like_full_sha;
use super::wire::FileHistoryError;

/// Default entries retained by [`BlobCache::new`].
pub const DEFAULT_BLOB_CACHE_CAPACITY: usize = 128;

/// Budgets for one batched blob read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobReadBudget {
    /// Whole-session timeout in milliseconds.
    pub timeout_ms: u64,
    /// Blobs declaring more bytes are consumed, discarded, and
    /// reported as unavailable.
    pub max_blob_bytes: u64,
}

impl Default for BlobReadBudget {
    fn default() -> Self {
        Self {
            timeout_ms: 10_000,
            max_blob_bytes: 8 * 1024 * 1024,
        }
    }
}

/// Read blobs for *oids* in order, returning `Some(bytes)` for present
/// blobs and `None` for missing, oversize, or malformed OID tokens.
/// Malformed tokens never reach git, so framing stays aligned.
pub fn read_blobs(
    repo: &Path,
    oids: &[String],
    budget: &BlobReadBudget,
) -> Result<Vec<Option<Vec<u8>>>, FileHistoryError> {
    let wanted: Vec<Option<String>> = oids
        .iter()
        .map(|oid| {
            if looks_like_full_sha(oid) {
                Some(oid.to_ascii_lowercase())
            } else {
                None
            }
        })
        .collect();
    if wanted.iter().all(Option::is_none) {
        return Ok(vec![None; oids.len()]);
    }
    let mut child = Command::new("git")
        .arg("--no-optional-locks")
        .arg("-C")
        .arg(repo)
        .arg("cat-file")
        .arg("--batch")
        .env("GIT_OPTIONAL_LOCKS", "0")
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|error| {
            FileHistoryError::GitFailed(format!("spawn cat-file: {error}"))
        })?;
    let deadline =
        Instant::now() + Duration::from_millis(budget.timeout_ms.max(1));
    if let Some(mut stdin) = child.stdin.take() {
        for oid in wanted.iter().flatten() {
            if Instant::now() >= deadline || writeln!(stdin, "{oid}").is_err() {
                break;
            }
        }
    }
    let stdout = child.stdout.take().ok_or_else(|| {
        FileHistoryError::GitFailed("cat-file stdout missing".to_string())
    })?;
    let mut reader = BufReader::new(stdout);
    let mut out = Vec::with_capacity(oids.len());
    for oid in &wanted {
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(FileHistoryError::GitFailed(
                "cat-file --batch timed out".to_string(),
            ));
        }
        let Some(expected) = oid else {
            out.push(None);
            continue;
        };
        out.push(read_one(&mut reader, expected, budget)?);
    }
    let status = child.wait().map_err(|error| {
        FileHistoryError::GitFailed(format!("wait cat-file: {error}"))
    })?;
    if !status.success() {
        return Err(FileHistoryError::GitFailed(format!(
            "cat-file --batch exited with {}",
            status
                .code()
                .map_or("signal".to_string(), |code| code.to_string()),
        )));
    }
    Ok(out)
}

fn read_one<R: Read>(
    reader: &mut BufReader<R>,
    expected: &str,
    budget: &BlobReadBudget,
) -> Result<Option<Vec<u8>>, FileHistoryError> {
    let mut line = String::new();
    reader.read_line(&mut line).map_err(|error| {
        FileHistoryError::GitFailed(format!("read batch line: {error}"))
    })?;
    let line = line.trim_end_matches(['\r', '\n']).to_string();
    if line == format!("{expected} missing") {
        return Ok(None);
    }
    let parts: Vec<&str> = line.split(' ').collect();
    if parts.len() != 3 || parts[0] != expected {
        return Err(FileHistoryError::GitFailed(format!(
            "unexpected batch header: {line}"
        )));
    }
    if parts[1] != "blob" {
        discard_blob(reader, parts[2])?;
        return Ok(None);
    }
    let size: u64 = parts[2].parse().map_err(|_| {
        FileHistoryError::GitFailed(format!("bad batch size: {}", parts[2]))
    })?;
    if size > budget.max_blob_bytes {
        discard_exact(reader, size)?;
        expect_newline(reader)?;
        return Ok(None);
    }
    let count = usize::try_from(size).map_err(|_| {
        FileHistoryError::GitFailed("blob size overflow".to_string())
    })?;
    let mut buf = vec![0u8; count];
    reader.read_exact(&mut buf).map_err(|_| {
        FileHistoryError::GitFailed("short batch body".to_string())
    })?;
    expect_newline(reader)?;
    Ok(Some(buf))
}

fn discard_blob<R: Read>(
    reader: &mut BufReader<R>,
    size_text: &str,
) -> Result<(), FileHistoryError> {
    let size: u64 = size_text.parse().unwrap_or(0);
    discard_exact(reader, size)?;
    expect_newline(reader)
}

fn discard_exact<R: Read>(
    reader: &mut BufReader<R>,
    mut size: u64,
) -> Result<(), FileHistoryError> {
    let mut chunk = [0u8; 8192];
    while size > 0 {
        let want = (size.min(chunk.len() as u64)) as usize;
        reader.read_exact(&mut chunk[..want]).map_err(|_| {
            FileHistoryError::GitFailed("short batch body".to_string())
        })?;
        size -= want as u64;
    }
    Ok(())
}

fn expect_newline<R: Read>(
    reader: &mut BufReader<R>,
) -> Result<(), FileHistoryError> {
    let mut byte = [0u8; 1];
    reader.read_exact(&mut byte).map_err(|_| {
        FileHistoryError::GitFailed("short batch body".to_string())
    })?;
    if byte[0] != b'\n' {
        return Err(FileHistoryError::GitFailed(
            "bad batch framing".to_string(),
        ));
    }
    Ok(())
}

/// Optional LRU blob cache keyed by OID. Only present blobs are
/// stored: a miss today may exist tomorrow, but a hit is
/// content-addressed and never stale.
#[derive(Debug, Clone, Default)]
pub struct BlobCache {
    capacity: usize,
    entries: Vec<(String, Vec<u8>)>,
}

impl BlobCache {
    /// New cache holding up to *capacity* blobs (0 disables storage).
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: Vec::new(),
        }
    }

    /// Whether *oid* is currently cached.
    pub fn contains(&self, oid: &str) -> bool {
        self.entries.iter().any(|(key, _)| key == oid)
    }

    /// Number of blobs currently cached.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the cache currently holds nothing.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Read blobs, serving cache hits and storing fresh hits.
    /// Result order matches *oids*.
    pub fn read(
        &mut self,
        repo: &Path,
        oids: &[String],
        budget: &BlobReadBudget,
    ) -> Result<Vec<Option<Vec<u8>>>, FileHistoryError> {
        let oids: Vec<String> =
            oids.iter().map(|oid| oid.to_ascii_lowercase()).collect();
        let mut out: Vec<Option<Vec<u8>>> = Vec::with_capacity(oids.len());
        let mut missing_idx = Vec::new();
        let mut missing_oids = Vec::new();
        for (index, oid) in oids.iter().enumerate() {
            if let Some(position) =
                self.entries.iter().position(|(key, _)| key == oid)
            {
                let (_, bytes) = self.entries.remove(position);
                let bytes_clone = bytes.clone();
                self.entries.insert(0, (oid.clone(), bytes));
                out.push(Some(bytes_clone));
            } else {
                out.push(None);
                missing_idx.push(index);
                missing_oids.push(oid.clone());
            }
        }
        if !missing_oids.is_empty() {
            let fresh = read_blobs(repo, &missing_oids, budget)?;
            for ((order, slot), bytes) in
                missing_idx.into_iter().enumerate().zip(fresh)
            {
                if let Some(body) = bytes {
                    self.store(
                        missing_oids[order].to_ascii_lowercase(),
                        body.clone(),
                    );
                    out[slot] = Some(body);
                }
            }
        }
        Ok(out)
    }

    fn store(&mut self, oid: String, bytes: Vec<u8>) {
        if self.capacity == 0 {
            return;
        }
        self.entries.insert(0, (oid, bytes));
        self.entries.truncate(self.capacity);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lru_serves_hits_and_evicts_oldest() {
        let mut cache = BlobCache::new(2);
        cache.store("a".to_string(), b"1".to_vec());
        cache.store("b".to_string(), b"2".to_vec());
        assert!(cache.contains("a"));
        cache.store("c".to_string(), b"3".to_vec());
        assert!(!cache.contains("a"));
        assert!(cache.contains("b"));
        assert!(cache.contains("c"));
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn zero_capacity_stores_nothing() {
        let mut cache = BlobCache::new(0);
        cache.store("a".to_string(), b"1".to_vec());
        assert!(cache.is_empty());
    }
}
