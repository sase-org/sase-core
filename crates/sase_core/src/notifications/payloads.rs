//! Content-addressed notification payload sidecars.

use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};

use super::wire::NotificationWire;

const REFERENCE_PREFIX: &str = "sase-notification-blob:v1:";
const NOTE_PREVIEW_CHARS: usize = 80;
pub(crate) const BLOB_GC_GRACE: Duration = Duration::from_secs(60 * 60);

#[derive(Debug, Clone, PartialEq, Eq)]
struct PayloadReference {
    sha256: String,
    byte_len: u64,
}

/// Replace oversized action-data values and notes with content references.
pub(crate) fn externalize_notification(
    store_path: &Path,
    notification: &mut NotificationWire,
    limit_bytes: u64,
) -> Result<u64, String> {
    let mut externalized = 0;
    for value in notification.action_data.values_mut() {
        if value.len() as u64 <= limit_bytes || parse_reference(value).is_some()
        {
            continue;
        }
        *value = write_payload(store_path, value)?;
        externalized += 1;
    }
    for note in &mut notification.notes {
        if note.len() as u64 <= limit_bytes || note_reference(note).is_some() {
            continue;
        }
        let reference = write_payload(store_path, note)?;
        let preview = note.chars().take(NOTE_PREVIEW_CHARS).collect::<String>();
        *note = format!("{preview} … {reference}");
        externalized += 1;
    }
    Ok(externalized)
}

/// Resolve action-data references or note references; ordinary values pass through.
/// A recognized reference whose sidecar is absent resolves to `None`.
pub(crate) fn resolve_payloads(
    store_path: &Path,
    values: &[String],
) -> Result<Vec<Option<String>>, String> {
    values
        .iter()
        .map(|value| {
            let reference =
                parse_reference(value).or_else(|| note_reference(value));
            match reference {
                Some(reference) => read_payload(store_path, &reference),
                None => Ok(Some(value.clone())),
            }
        })
        .collect()
}

/// Remove files that do not exist from rows being rewritten by compaction.
pub(crate) fn prune_broken_files(rows: &mut [NotificationWire]) -> u64 {
    let mut dropped = 0;
    for row in rows {
        let before = row.files.len();
        row.files.retain(|file| Path::new(file).exists());
        dropped += (before - row.files.len()) as u64;
    }
    dropped
}

/// Delete unreferenced, old sidecars. Only flat regular files named by a SHA-256
/// digest are eligible; symlinks, directories, temporary files, and fresh files stay.
pub(crate) fn delete_unreferenced(
    store_path: &Path,
    live_rows: &[NotificationWire],
    now: SystemTime,
    grace: Duration,
) -> Result<u64, String> {
    let blobs = blob_dir(store_path)?;
    let metadata = match fs::symlink_metadata(&blobs) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(0)
        }
        Err(error) => return Err(error.to_string()),
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(format!(
            "notification blob path is not a real directory: {}",
            blobs.display()
        ));
    }

    let mut referenced = BTreeSet::new();
    for row in live_rows {
        collect_references(row, &mut referenced);
    }
    let archive = archive_path_for(store_path);
    collect_archive_references(&archive, &mut referenced)?;

    let mut deleted = 0;
    for entry in fs::read_dir(&blobs).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if !is_sha256(&name) || referenced.contains(&name) {
            continue;
        }
        let metadata = match fs::symlink_metadata(entry.path()) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                continue
            }
            Err(error) => return Err(error.to_string()),
        };
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            continue;
        }
        let age = now
            .duration_since(metadata.modified().unwrap_or(now))
            .unwrap_or_default();
        if age < grace {
            continue;
        }
        match fs::remove_file(entry.path()) {
            Ok(()) => deleted += 1,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.to_string()),
        }
    }
    Ok(deleted)
}

fn write_payload(store_path: &Path, value: &str) -> Result<String, String> {
    let bytes = value.as_bytes();
    let sha256 = hex::encode(Sha256::digest(bytes));
    let reference = format!("{REFERENCE_PREFIX}{sha256}:{}", bytes.len());
    let blobs = blob_dir(store_path)?;
    fs::create_dir_all(blobs.parent().ok_or_else(|| {
        format!("notification path has no parent: {}", store_path.display())
    })?)
    .map_err(|error| error.to_string())?;
    match fs::create_dir(&blobs) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.to_string()),
    }
    let directory =
        fs::symlink_metadata(&blobs).map_err(|error| error.to_string())?;
    if directory.file_type().is_symlink() || !directory.is_dir() {
        return Err(format!(
            "notification blob path is not a real directory: {}",
            blobs.display()
        ));
    }

    let target = blobs.join(&sha256);
    match fs::symlink_metadata(&target) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(format!(
                    "notification sidecar is not a regular file: {}",
                    target.display()
                ));
            }
            let existing =
                fs::read(&target).map_err(|error| error.to_string())?;
            if existing != bytes {
                return Err(format!(
                    "notification sidecar content does not match its digest: {}",
                    target.display()
                ));
            }
            return Ok(reference);
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.to_string()),
    }

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let temp = blobs.join(format!(".{sha256}.{}.{}.tmp", process::id(), nanos));
    let write_result = (|| {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp)
            .map_err(|error| error.to_string())?;
        file.write_all(bytes).map_err(|error| error.to_string())?;
        file.flush().map_err(|error| error.to_string())?;
        file.sync_all().map_err(|error| error.to_string())?;
        fs::rename(&temp, &target).map_err(|error| error.to_string())?;
        if let Ok(directory) = File::open(&blobs) {
            let _ = directory.sync_all();
        }
        Ok(())
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&temp);
        return write_result.map(|()| reference);
    }
    Ok(reference)
}

fn read_payload(
    store_path: &Path,
    reference: &PayloadReference,
) -> Result<Option<String>, String> {
    let blobs = blob_dir(store_path)?;
    let directory = match fs::symlink_metadata(&blobs) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(None);
        }
        Err(error) => return Err(error.to_string()),
    };
    if directory.file_type().is_symlink() || !directory.is_dir() {
        return Err(format!(
            "notification blob path is not a real directory: {}",
            blobs.display()
        ));
    }
    let path = blobs.join(&reference.sha256);
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(None);
        }
        Err(error) => return Err(error.to_string()),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(format!(
            "notification sidecar is not a regular file: {}",
            path.display()
        ));
    }
    let bytes = fs::read(&path).map_err(|error| error.to_string())?;
    let actual_sha256 = hex::encode(Sha256::digest(&bytes));
    if bytes.len() as u64 != reference.byte_len
        || actual_sha256 != reference.sha256
    {
        return Err(format!(
            "notification sidecar failed integrity check: {}",
            path.display()
        ));
    }
    String::from_utf8(bytes)
        .map(Some)
        .map_err(|error| format!("notification sidecar is not UTF-8: {error}"))
}

fn parse_reference(value: &str) -> Option<PayloadReference> {
    let rest = value.strip_prefix(REFERENCE_PREFIX)?;
    let (sha256, byte_len) = rest.split_once(':')?;
    if !is_sha256(sha256)
        || byte_len.is_empty()
        || (byte_len.len() > 1 && byte_len.starts_with('0'))
        || !byte_len.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    Some(PayloadReference {
        sha256: sha256.to_string(),
        byte_len: byte_len.parse().ok()?,
    })
}

fn note_reference(note: &str) -> Option<PayloadReference> {
    parse_reference(note.split_whitespace().last()?)
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn collect_references(
    row: &NotificationWire,
    references: &mut BTreeSet<String>,
) {
    for value in row.action_data.values() {
        if let Some(reference) = parse_reference(value) {
            references.insert(reference.sha256);
        }
    }
    for note in &row.notes {
        if let Some(reference) = note_reference(note) {
            references.insert(reference.sha256);
        }
    }
}

fn collect_archive_references(
    archive_path: &Path,
    references: &mut BTreeSet<String>,
) -> Result<(), String> {
    let file = match File::open(archive_path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(())
        }
        Err(error) => return Err(error.to_string()),
    };
    for line in BufReader::new(file).lines() {
        let line = line.map_err(|error| error.to_string())?;
        let Ok(row) = serde_json::from_str::<NotificationWire>(&line) else {
            continue;
        };
        collect_references(&row, references);
    }
    Ok(())
}

fn blob_dir(store_path: &Path) -> Result<PathBuf, String> {
    let parent = store_path.parent().ok_or_else(|| {
        format!("notification path has no parent: {}", store_path.display())
    })?;
    Ok(parent.join("blobs"))
}

fn archive_path_for(path: &Path) -> PathBuf {
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("notifications.jsonl");
    let archive_name =
        if let Some((stem, extension)) = filename.rsplit_once('.') {
            format!("{stem}-archive.{extension}")
        } else {
            format!("{filename}-archive")
        };
    path.with_file_name(archive_name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::FileTimes;

    #[test]
    fn gc_keeps_live_archived_and_fresh_sidecars() {
        let temp = tempfile::tempdir().unwrap();
        let store_path = temp.path().join("notifications.jsonl");
        let live_reference =
            write_payload(&store_path, "live payload").unwrap();
        let archived_reference =
            write_payload(&store_path, "archived payload").unwrap();
        let stale_reference =
            write_payload(&store_path, "stale orphan").unwrap();
        let fresh_reference =
            write_payload(&store_path, "fresh orphan").unwrap();

        let mut live = NotificationWire {
            id: "live".into(),
            timestamp: "2026-05-01T01:02:03+00:00".into(),
            sender: "test".into(),
            ..NotificationWire::default()
        };
        live.action_data
            .insert("payload".into(), live_reference.clone());
        let mut archived = NotificationWire {
            id: "archived".into(),
            timestamp: "2026-05-01T01:02:03+00:00".into(),
            sender: "test".into(),
            ..NotificationWire::default()
        };
        archived
            .notes
            .push(format!("preview … {archived_reference}"));
        let archive = archive_path_for(&store_path);
        fs::write(
            archive,
            format!("{}\n", serde_json::to_string(&archived).unwrap()),
        )
        .unwrap();

        let blobs = blob_dir(&store_path).unwrap();
        let stale_hash = parse_reference(&stale_reference).unwrap().sha256;
        File::options()
            .write(true)
            .open(blobs.join(stale_hash))
            .unwrap()
            .set_times(FileTimes::new().set_modified(UNIX_EPOCH))
            .unwrap();

        let deleted = delete_unreferenced(
            &store_path,
            &[live],
            SystemTime::now(),
            BLOB_GC_GRACE,
        )
        .unwrap();
        assert_eq!(deleted, 1);
        assert!(blobs
            .join(parse_reference(&live_reference).unwrap().sha256)
            .is_file());
        assert!(blobs
            .join(parse_reference(&archived_reference).unwrap().sha256)
            .is_file());
        assert!(!blobs
            .join(parse_reference(&stale_reference).unwrap().sha256)
            .exists());
        assert!(blobs
            .join(parse_reference(&fresh_reference).unwrap().sha256)
            .is_file());
    }
}
