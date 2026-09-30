//! Persisted notification-store generation counter.
//!
//! Every successful notification-store write bumps a sibling of the JSONL
//! file (`notifications.jsonl` -> `notifications.jsonl.generation`). The
//! counter lets readers fence observations: an ack returns the generation
//! its write landed in, and a later index observation at a generation
//! greater than or equal to that value shows the ack's rows dismissed.
//!
//! The file holds one canonical decimal `u64` and a trailing newline. A
//! missing file means `0`; corrupt contents are an error, never a silent
//! `0`. Reads never create the file or its parent directory. Bumps happen
//! only inside the exclusive store lock, after the JSONL mutation they
//! describe has succeeded.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};

/// Return the sibling generation path for a live notifications JSONL file.
fn generation_path_for(path: &Path) -> PathBuf {
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("notifications.jsonl");
    path.with_file_name(format!("{filename}.generation"))
}

/// Read the persisted generation without creating anything on disk.
///
/// A missing file means `0`. A present file must hold exactly one decimal
/// `u64` line (a single trailing newline is accepted); anything else is an
/// error.
pub(crate) fn read_generation(path: &Path) -> Result<u64, String> {
    let gen_path = generation_path_for(path);
    let content = match fs::read_to_string(&gen_path) {
        Ok(content) => content,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e.to_string()),
    };
    parse_generation(&content).ok_or_else(|| {
        format!(
            "invalid notification generation file {}: expected one decimal u64 line",
            gen_path.display()
        )
    })
}

fn parse_generation(content: &str) -> Option<u64> {
    let line = content.strip_suffix('\n').unwrap_or(content);
    if line.is_empty() || line.contains('\n') {
        return None;
    }
    line.parse::<u64>().ok()
}

/// Bump the persisted generation by one and return the new value.
///
/// Call only inside the exclusive store lock, after the JSONL mutation the
/// bump describes has succeeded. Overflow is an error, never a wrap.
pub(crate) fn bump_generation(path: &Path) -> Result<u64, String> {
    let current = read_generation(path)?;
    let next = current
        .checked_add(1)
        .ok_or_else(|| "notification generation overflow".to_string())?;
    write_generation(path, next)?;
    Ok(next)
}

fn write_generation(path: &Path, value: u64) -> Result<(), String> {
    let gen_path = generation_path_for(path);
    if let Some(parent) = gen_path.parent() {
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let tmp_path =
        gen_path.with_file_name(format!(".{}.{}.tmp", process::id(), nanos));
    let write_result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&tmp_path)
            .map_err(|e| e.to_string())?;
        file.write_all(format!("{value}\n").as_bytes())
            .map_err(|e| e.to_string())?;
        file.flush().map_err(|e| e.to_string())?;
        file.sync_all().map_err(|e| e.to_string())?;
        fs::rename(&tmp_path, &gen_path).map_err(|e| e.to_string())?;
        Ok(())
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&tmp_path);
    }
    write_result
}

/// Read the generation for a store whose JSONL may not exist.
///
/// Reports the sibling file's value when it can be read without creating
/// directories, and `0` otherwise. Reads never create anything.
pub(crate) fn generation_for_missing_store(path: &Path) -> u64 {
    read_generation(path).unwrap_or(0)
}
