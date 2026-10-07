//! Exact stat-only bead store fingerprint.
//!
//! The fingerprint is the change token TUI caches and auto-refresh probes
//! use to decide whether a bead store needs re-reading. It is stat-only
//! (no file contents are opened) and exact over the files the read path
//! consumes:
//!
//! - event stores: `config.json`, `events/manifest.json`, and every stream
//!   file under `events/streams/` (each as `(size, mtime_ns, inode)`);
//! - legacy stores without `events/`: `config.json` and `issues.jsonl`.
//!
//! Regenerating `issues.jsonl` alone never changes an event store's token,
//! which is what lets `projection-off` take the projection off the
//! per-mutation path without blinding these caches.

use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::jsonl::{
    event_manifest_path, event_store_present, event_streams_dir,
};
use super::wire::BeadError;
use crate::fs_sig::mtime_ns;

/// Wire schema version for [`BeadStoreFingerprintWire`].
pub const BEAD_STORE_FINGERPRINT_WIRE_SCHEMA_VERSION: u32 = 1;

/// Layout tag for [`BeadStoreFingerprintWire::layout`]: an event store.
pub const BEAD_STORE_FINGERPRINT_LAYOUT_EVENTS: &str = "events";
/// Layout tag for [`BeadStoreFingerprintWire::layout`]: a legacy store.
pub const BEAD_STORE_FINGERPRINT_LAYOUT_LEGACY: &str = "legacy";

/// Exact stat-only change token for one bead store plus the counts it used.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadStoreFingerprintWire {
    /// Schema version of this wire type.
    pub schema_version: u32,
    /// Stable hex token over the covered `(size, mtime_ns, inode)` triples.
    pub token: String,
    /// `"events"` for event stores, `"legacy"` for stores without `events/`.
    pub layout: String,
    /// Covered files that exist (and were hashed).
    pub files: usize,
    /// Stream files that exist (and were hashed); 0 for legacy stores.
    pub streams: usize,
}

/// Compute the exact stat-only fingerprint of the bead store at `beads_dir`.
///
/// Only file metadata is read; no file contents are opened. Missing covered
/// files hash as absent markers so a store that gains `config.json` (or a
/// manifest) changes token. An unreadable directory or an unreadable file
/// (other than not-found) is an error.
pub fn bead_store_fingerprint(
    beads_dir: &Path,
) -> Result<BeadStoreFingerprintWire, BeadError> {
    if !beads_dir.is_dir() {
        return Err(BeadError::io(format!(
            "No beads directory found at {}",
            beads_dir.display()
        )));
    }
    let layout_events = event_store_present(beads_dir);
    let mut entries: Vec<(String, PathBuf)> = vec![
        ("config.json".to_string(), beads_dir.join("config.json")),
        (
            "events/manifest.json".to_string(),
            event_manifest_path(beads_dir),
        ),
    ];
    let mut stream_count = 0usize;
    if layout_events {
        let streams_dir = event_streams_dir(beads_dir);
        let mut stream_paths: Vec<PathBuf> = vec![];
        match fs::read_dir(&streams_dir) {
            Ok(read) => {
                for entry in read {
                    let path = entry.map_err(|err| {
                        BeadError::io(format!(
                            "failed to read bead event stream entry in {}: {err}",
                            streams_dir.display()
                        ))
                    })?
                    .path();
                    if path.is_file() {
                        stream_paths.push(path);
                    }
                }
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => {
                return Err(BeadError::io(format!(
                    "failed to read bead event streams directory {}: {error}",
                    streams_dir.display()
                )));
            }
        }
        stream_paths.sort();
        for path in stream_paths {
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default()
                .to_string();
            entries.push((format!("events/streams/{name}"), path));
            stream_count += 1;
        }
    } else {
        entries
            .push(("issues.jsonl".to_string(), beads_dir.join("issues.jsonl")));
    }
    entries.sort_by(|left, right| left.0.cmp(&right.0));

    let mut hasher = Sha256::new();
    let mut files = 0usize;
    for (relative, path) in &entries {
        match fs::metadata(path) {
            Ok(metadata) => {
                if !metadata.is_file() {
                    // A directory where a file belongs is a shape change:
                    // hash it as absent so the token moves.
                    hash_entry(&mut hasher, relative, false, 0, 0, 0);
                    continue;
                }
                files += 1;
                hash_entry(
                    &mut hasher,
                    relative,
                    true,
                    metadata.len(),
                    mtime_ns(metadata.modified().ok()),
                    file_inode(&metadata),
                );
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {
                hash_entry(&mut hasher, relative, false, 0, 0, 0);
            }
            Err(error) => {
                return Err(BeadError::io(format!(
                    "failed to stat bead store file {}: {error}",
                    path.display()
                )));
            }
        }
    }

    Ok(BeadStoreFingerprintWire {
        schema_version: BEAD_STORE_FINGERPRINT_WIRE_SCHEMA_VERSION,
        token: hex::encode(hasher.finalize()),
        layout: if layout_events {
            BEAD_STORE_FINGERPRINT_LAYOUT_EVENTS.to_string()
        } else {
            BEAD_STORE_FINGERPRINT_LAYOUT_LEGACY.to_string()
        },
        files,
        streams: stream_count,
    })
}

fn hash_entry(
    hasher: &mut Sha256,
    relative: &str,
    exists: bool,
    size: u64,
    mtime: i64,
    inode: u64,
) {
    hasher.update(relative.as_bytes());
    hasher.update([0]);
    hasher.update(if exists { [1] } else { [0] });
    hasher.update(size.to_le_bytes());
    hasher.update(mtime.to_le_bytes());
    hasher.update(inode.to_le_bytes());
}

fn file_inode(metadata: &fs::Metadata) -> u64 {
    #[cfg(unix)]
    {
        std::os::unix::fs::MetadataExt::ino(metadata)
    }
    #[cfg(windows)]
    {
        std::os::windows::fs::MetadataExt::file_index(metadata)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = metadata;
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;
    use std::time::Duration;
    use tempfile::tempdir;

    fn write(path: &Path, text: &str) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, text).unwrap();
    }

    fn event_beads_dir(root: &Path) -> PathBuf {
        let beads_dir = root.join("beads");
        write(&beads_dir.join("config.json"), "{\"project\":\"demo\"}\n");
        write(
            &beads_dir.join("events").join("manifest.json"),
            "{\"schema_version\":1,\"stream_count\":1}\n",
        );
        write(
            &beads_dir
                .join("events")
                .join("streams")
                .join("demo-1.jsonl"),
            "{\"schema_version\":1}\n",
        );
        write(&beads_dir.join("issues.jsonl"), "[]\n");
        beads_dir
    }

    fn fingerprint_token(beads_dir: &Path) -> BeadStoreFingerprintWire {
        bead_store_fingerprint(beads_dir).unwrap()
    }

    #[test]
    fn event_store_token_is_stable_without_changes() {
        let temp = tempdir().unwrap();
        let beads_dir = event_beads_dir(temp.path());
        let first = fingerprint_token(&beads_dir);
        let second = fingerprint_token(&beads_dir);
        assert_eq!(first, second);
        assert_eq!(first.layout, "events");
        assert_eq!(first.schema_version, 1);
        assert_eq!(first.streams, 1);
        assert_eq!(first.token.len(), 64);
    }

    #[test]
    fn event_store_token_ignores_the_issues_projection() {
        let temp = tempdir().unwrap();
        let beads_dir = event_beads_dir(temp.path());
        let before = fingerprint_token(&beads_dir).token;
        write(&beads_dir.join("issues.jsonl"), "[{\"id\":\"demo-1\"}]\n");
        assert_eq!(fingerprint_token(&beads_dir).token, before);
    }

    #[test]
    fn event_store_token_moves_on_stream_append() {
        let temp = tempdir().unwrap();
        let beads_dir = event_beads_dir(temp.path());
        let before = fingerprint_token(&beads_dir);
        sleep(Duration::from_millis(5));
        write(
            &beads_dir
                .join("events")
                .join("streams")
                .join("demo-1.jsonl"),
            "{\"schema_version\":1}\n{\"schema_version\":1}\n",
        );
        let after = fingerprint_token(&beads_dir);
        assert_ne!(after.token, before.token);
        assert_eq!(after.streams, before.streams);
        assert_eq!(after.files, before.files);
    }

    #[test]
    fn event_store_token_moves_on_new_stream() {
        let temp = tempdir().unwrap();
        let beads_dir = event_beads_dir(temp.path());
        let before = fingerprint_token(&beads_dir);
        write(
            &beads_dir
                .join("events")
                .join("streams")
                .join("demo-2.jsonl"),
            "{\"schema_version\":1}\n",
        );
        let after = fingerprint_token(&beads_dir);
        assert_ne!(after.token, before.token);
        assert_eq!(after.streams, before.streams + 1);
    }

    #[test]
    fn legacy_store_token_covers_issues_jsonl() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("beads");
        write(&beads_dir.join("config.json"), "{\"project\":\"demo\"}\n");
        write(&beads_dir.join("issues.jsonl"), "[]\n");
        let before = fingerprint_token(&beads_dir);
        assert_eq!(before.layout, "legacy");
        assert_eq!(before.streams, 0);
        sleep(Duration::from_millis(5));
        write(&beads_dir.join("issues.jsonl"), "[{\"id\":\"demo-1\"}]\n");
        let after = fingerprint_token(&beads_dir);
        assert_ne!(after.token, before.token);
    }

    #[test]
    fn missing_store_is_an_error() {
        let temp = tempdir().unwrap();
        let error =
            bead_store_fingerprint(&temp.path().join("missing")).unwrap_err();
        assert_eq!(error.kind, "io");
    }
}
