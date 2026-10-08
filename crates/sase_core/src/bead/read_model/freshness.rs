//! O(1) freshness token plus the exact full-stat sweep (plan P1).
//!
//! A full stat sweep costs ~4 ms at 1x but ~48 ms at 8x, which alone would
//! break the A1 history-independence budget for a 20 ms point read. Reads
//! therefore check a constant-cost token first: the streams directory's
//! `(mtime_ns, inode)` plus the `events/manifest.json` and `config.json`
//! signatures. Every supported writer changes the directory entry (the
//! Rust writer does temp-file-plus-rename; git unlinks then creates), so
//! an unchanged token means nothing was added, removed, or renamed.
//!
//! A matching token with a fresh sweep serves immediately. Otherwise a
//! full stat sweep runs against the stored per-stream signatures; when it
//! matches, the token and sweep time refresh and the cache serves. Any
//! difference means a full rebuild in this phase (`read-model-tail` adds
//! incremental apply). A full sweep also runs on `--verify-cache` and at
//! least every [`READ_MODEL_SWEEP_INTERVAL_SECS`], so even an unsupported
//! in-place edit that preserves the directory entry is caught within a
//! bounded window.

use std::fs;
use std::path::Path;
use std::time::SystemTime;

use sha2::{Digest, Sha256};

use crate::bead::jsonl::{event_manifest_path, event_streams_dir, file_inode};
#[cfg(test)]
use crate::bead::mutation::store_io_stats;
use crate::bead::wire::BeadError;
use crate::fs_sig::mtime_ns;

/// Minimum age of the last full sweep before a read re-sweeps (seconds).
///
/// A matching O(1) token with a sweep newer than this serves without
/// touching per-stream metadata.
pub const READ_MODEL_SWEEP_INTERVAL_SECS: u64 = 60;

/// Stat-only signature of one covered file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileSignature {
    /// File length in bytes.
    pub size: u64,
    /// Modification time as nanoseconds since the Unix epoch.
    pub mtime_ns: i64,
    /// Filesystem inode (0 where unavailable).
    pub inode: u64,
}

/// Stat-only signatures of everything the freshness check covers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreSignatures {
    /// `events/manifest.json` (`None` when absent).
    pub manifest: Option<FileSignature>,
    /// `config.json` (`None` when absent).
    pub config: Option<FileSignature>,
    /// Every `events/streams/<id>.jsonl`, sorted by stream id.
    pub streams: Vec<(String, FileSignature)>,
}

/// Compute the constant-cost freshness token for the store at `beads_dir`.
///
/// The token covers the streams directory entry plus the manifest and
/// config signatures: three stats no matter how many streams exist.
pub fn freshness_token(beads_dir: &Path) -> Result<String, BeadError> {
    let streams_dir = event_streams_dir(beads_dir);
    let (dir_mtime, dir_inode) = match fs::metadata(&streams_dir) {
        Ok(metadata) => {
            (mtime_ns(metadata.modified().ok()), file_inode(&metadata))
        }
        Err(error) => {
            return Err(BeadError::io(format!(
                "failed to stat bead event streams directory {}: {error}",
                streams_dir.display()
            )));
        }
    };
    let mut hasher = Sha256::new();
    hasher.update(dir_mtime.to_le_bytes());
    hasher.update(dir_inode.to_le_bytes());
    hash_optional_signature(
        &mut hasher,
        "events/manifest.json",
        &stat_optional(&event_manifest_path(beads_dir))?,
    );
    hash_optional_signature(
        &mut hasher,
        "config.json",
        &stat_optional(&beads_dir.join("config.json"))?,
    );
    Ok(hex::encode(hasher.finalize()))
}

/// Stat every covered file: the full sweep behind a changed token.
///
/// The streams directory is listed once. An unreadable directory entry or
/// file (other than not-found, which hashes absent) is an error so the
/// caller falls back to replay rather than serving a partial view.
pub fn sweep_store_signatures(
    beads_dir: &Path,
) -> Result<StoreSignatures, BeadError> {
    #[cfg(test)]
    store_io_stats::record_full_sweep();
    let streams_dir = event_streams_dir(beads_dir);
    let mut stream_names: Vec<String> = vec![];
    match fs::read_dir(&streams_dir) {
        Ok(read) => {
            for entry in read {
                let path = entry
                    .map_err(|err| {
                        BeadError::io(format!(
                            "failed to read bead event stream entry in {}: {err}",
                            streams_dir.display()
                        ))
                    })?
                    .path();
                if path.is_file()
                    && path.extension().and_then(|ext| ext.to_str())
                        == Some("jsonl")
                {
                    if let Some(name) =
                        path.file_stem().and_then(|stem| stem.to_str())
                    {
                        stream_names.push(name.to_string());
                    }
                }
            }
        }
        Err(error) => {
            return Err(BeadError::io(format!(
                "failed to read bead event streams directory {}: {error}",
                streams_dir.display()
            )));
        }
    }
    stream_names.sort();
    let mut streams = Vec::with_capacity(stream_names.len());
    for name in stream_names {
        let path = streams_dir.join(format!("{name}.jsonl"));
        let signature = stat_required(&path)?;
        streams.push((name, signature));
    }
    Ok(StoreSignatures {
        manifest: stat_optional(&event_manifest_path(beads_dir))?,
        config: stat_optional(&beads_dir.join("config.json"))?,
        streams,
    })
}

/// Current Unix time in nanoseconds since the epoch.
pub fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn stat_optional(path: &Path) -> Result<Option<FileSignature>, BeadError> {
    match fs::metadata(path) {
        Ok(metadata) => {
            if !metadata.is_file() {
                return Ok(None);
            }
            Ok(Some(FileSignature {
                size: metadata.len(),
                mtime_ns: mtime_ns(metadata.modified().ok()),
                inode: file_inode(&metadata),
            }))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(BeadError::io(format!(
            "failed to stat bead store file {}: {error}",
            path.display()
        ))),
    }
}

fn stat_required(path: &Path) -> Result<FileSignature, BeadError> {
    stat_optional(path)?.ok_or_else(|| {
        BeadError::io(format!(
            "bead event stream vanished during sweep: {}",
            path.display()
        ))
    })
}

fn hash_optional_signature(
    hasher: &mut Sha256,
    relative: &str,
    signature: &Option<FileSignature>,
) {
    hasher.update(relative.as_bytes());
    hasher.update([0]);
    match signature {
        Some(signature) => {
            hasher.update([1]);
            hasher.update(signature.size.to_le_bytes());
            hasher.update(signature.mtime_ns.to_le_bytes());
            hasher.update(signature.inode.to_le_bytes());
        }
        None => {
            hasher.update([0]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sweep_and_token_agree_on_stable_store() {
        let temp = tempfile::tempdir().unwrap();
        let beads_dir = temp.path().join("beads");
        let streams_dir = beads_dir.join("events/streams");
        fs::create_dir_all(&streams_dir).unwrap();
        fs::write(beads_dir.join("config.json"), "{}\n").unwrap();
        fs::write(beads_dir.join("events/manifest.json"), "{}\n").unwrap();
        fs::write(streams_dir.join("a.jsonl"), "{}\n").unwrap();
        let first = freshness_token(&beads_dir).unwrap();
        let second = freshness_token(&beads_dir).unwrap();
        assert_eq!(first, second);
        let sweep = sweep_store_signatures(&beads_dir).unwrap();
        assert_eq!(sweep.streams.len(), 1);
        assert!(sweep.manifest.is_some());
        assert!(sweep.config.is_some());
    }
}
