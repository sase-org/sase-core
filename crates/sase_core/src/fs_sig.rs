//! Shared file-signature and atomic-write helpers for derived caches.
//!
//! Extracted from `bead/touch_index.rs` so the goal ledger projection
//! reuses the same stat signatures and atomic-write pattern instead of
//! copying them. Both callers treat these files as rebuildable caches:
//! a missing, truncated, or wrong-schema file is a cache miss, never a
//! data error.

use std::fs;
use std::io::Write;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use tempfile::NamedTempFile;

/// Modification time as nanoseconds since the Unix epoch, clamped to `i64`.
pub(crate) fn mtime_ns(modified: Option<SystemTime>) -> i64 {
    let Some(modified) = modified else {
        return 0;
    };
    let nanos = match modified.duration_since(UNIX_EPOCH) {
        Ok(after) => i128::try_from(after.as_nanos()).unwrap_or(i128::MAX),
        Err(before) => {
            -i128::try_from(before.duration().as_nanos()).unwrap_or(i128::MAX)
        }
    };
    i64::try_from(nanos).unwrap_or(if nanos < 0 { i64::MIN } else { i64::MAX })
}

/// Write a JSON value atomically with temp-file + fsync + rename.
pub(crate) fn write_json_atomic(
    path: &Path,
    value: &serde_json::Value,
) -> std::io::Result<()> {
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => {
            fs::create_dir_all(parent)?;
            parent.to_path_buf()
        }
        _ => Path::new(".").to_path_buf(),
    };
    let mut temporary = NamedTempFile::new_in(parent)?;
    temporary.write_all(&serde_json::to_vec(value)?)?;
    temporary.write_all(b"\n")?;
    temporary.flush()?;
    temporary.as_file().sync_all()?;
    temporary.persist(path).map_err(|error| error.error)?;
    Ok(())
}
