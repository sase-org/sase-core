//! Clock-free, restorable artifact-file trash primitives.

use std::fs::{self, File};
use std::io::{ErrorKind, Write};
use std::path::{Component, Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tempfile::NamedTempFile;

use super::ARTIFACT_FILE_LIFECYCLE_WIRE_SCHEMA_VERSION;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArtifactFileTrashRequestWire {
    pub schema_version: u64,
    pub trash_root: String,
    pub record: JsonValue,
    #[serde(default)]
    pub stored_path: Option<String>,
    pub reason: String,
    pub trashed_at: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArtifactFileTrashEntryWire {
    pub schema_version: u64,
    pub entry_id: String,
    pub artifact_id: String,
    pub trashed_at: String,
    pub reason: String,
    pub size_bytes: Option<u64>,
    pub stored_filename: Option<String>,
    pub stored_path: Option<String>,
    pub record: JsonValue,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArtifactFileTrashListWire {
    pub schema_version: u64,
    pub entries: Vec<ArtifactFileTrashEntryWire>,
    pub unreadable_entries: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactFileTrashRestoreRequestWire {
    pub schema_version: u64,
    pub trash_root: String,
    pub entry_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArtifactFileTrashRestoreWire {
    pub schema_version: u64,
    pub entry_id: String,
    pub artifact_id: String,
    pub restored_path: Option<String>,
    pub record: JsonValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactFileTrashPurgeRequestWire {
    pub schema_version: u64,
    pub trash_root: String,
    pub before: String,
    pub all: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactFileTrashPurgeWire {
    pub schema_version: u64,
    pub purged_entry_ids: Vec<String>,
    pub freed_bytes: u64,
    pub unreadable_entries: u64,
}

pub fn trash_artifact_file(
    request: &ArtifactFileTrashRequestWire,
) -> Result<ArtifactFileTrashEntryWire, String> {
    require_schema(request.schema_version, "trash store request")?;
    let trashed_at = parse_rfc3339(&request.trashed_at, "trashed_at")?;
    let artifact_id = record_string(&request.record, "id")
        .ok_or_else(|| "artifact trash record is missing id".to_string())?;
    if artifact_id.trim().is_empty() {
        return Err("artifact trash record id is empty".to_string());
    }
    if request.reason.trim().is_empty() {
        return Err("artifact trash reason is empty".to_string());
    }

    let trash_root = prepare_root(Path::new(&request.trash_root), true)?;
    let base_id = format!(
        "{}-{}",
        trashed_at.with_timezone(&Utc).format("%Y%m%dT%H%M%S%.6fZ"),
        sanitize_artifact_id(&artifact_id)
    );
    let (entry_id, entry_dir) = reserve_entry_dir(&trash_root, &base_id)?;
    let result = store_in_reserved_entry(
        request,
        &trash_root,
        &entry_id,
        &entry_dir,
        artifact_id,
    );
    if result.is_err() {
        let _ = remove_entry_dir(&trash_root, &entry_dir);
    }
    result
}

pub fn list_artifact_file_trash(
    trash_root: &Path,
) -> Result<ArtifactFileTrashListWire, String> {
    if !trash_root.exists() {
        return Ok(ArtifactFileTrashListWire {
            schema_version: ARTIFACT_FILE_LIFECYCLE_WIRE_SCHEMA_VERSION,
            entries: Vec::new(),
            unreadable_entries: 0,
        });
    }
    let trash_root = prepare_root(trash_root, false)?;
    let mut entries = Vec::new();
    let mut unreadable_entries = 0;
    for candidate in fs::read_dir(&trash_root)
        .map_err(|error| format!("failed to list artifact trash: {error}"))?
    {
        let candidate = match candidate {
            Ok(value) => value,
            Err(_) => {
                unreadable_entries += 1;
                continue;
            }
        };
        match read_entry_dir(&trash_root, &candidate.path()) {
            Ok(entry) => entries.push(entry),
            Err(_) => unreadable_entries += 1,
        }
    }
    entries.sort_by(|left, right| {
        entry_timestamp(right)
            .cmp(&entry_timestamp(left))
            .then_with(|| right.entry_id.cmp(&left.entry_id))
    });
    Ok(ArtifactFileTrashListWire {
        schema_version: ARTIFACT_FILE_LIFECYCLE_WIRE_SCHEMA_VERSION,
        entries,
        unreadable_entries,
    })
}

pub fn restore_artifact_file_trash(
    request: &ArtifactFileTrashRestoreRequestWire,
) -> Result<ArtifactFileTrashRestoreWire, String> {
    require_schema(request.schema_version, "trash restore request")?;
    validate_single_component(&request.entry_id, "trash entry id")?;
    let trash_root = prepare_root(Path::new(&request.trash_root), false)?;
    let entry_dir = checked_existing_child(&trash_root, &request.entry_id)?;
    let entry = read_entry_dir(&trash_root, &entry_dir)?;
    if entry.entry_id != request.entry_id {
        return Err(format!(
            "trash entry id mismatch: directory is {}, entry says {}",
            request.entry_id, entry.entry_id
        ));
    }

    let restored_path = match (
        entry.stored_filename.as_deref(),
        entry.stored_path.as_deref(),
    ) {
        (None, None) => None,
        (Some(filename), Some(destination)) => {
            validate_single_component(filename, "stored filename")?;
            let payload = checked_existing_child(&entry_dir, filename)?;
            ensure_regular_file(&payload, "trash payload")?;
            restore_payload(&payload, Path::new(destination))?;
            Some(destination.to_string())
        }
        _ => {
            return Err(format!(
                "trash entry {} has incomplete payload metadata",
                entry.entry_id
            ))
        }
    };
    remove_entry_dir(&trash_root, &entry_dir)?;
    Ok(ArtifactFileTrashRestoreWire {
        schema_version: ARTIFACT_FILE_LIFECYCLE_WIRE_SCHEMA_VERSION,
        entry_id: entry.entry_id,
        artifact_id: entry.artifact_id,
        restored_path,
        record: entry.record,
    })
}

pub fn purge_artifact_file_trash(
    request: &ArtifactFileTrashPurgeRequestWire,
) -> Result<ArtifactFileTrashPurgeWire, String> {
    require_schema(request.schema_version, "trash purge request")?;
    let cutoff = parse_rfc3339(&request.before, "before")?;
    let trash_root_path = Path::new(&request.trash_root);
    if !trash_root_path.exists() {
        return Ok(ArtifactFileTrashPurgeWire {
            schema_version: ARTIFACT_FILE_LIFECYCLE_WIRE_SCHEMA_VERSION,
            purged_entry_ids: Vec::new(),
            freed_bytes: 0,
            unreadable_entries: 0,
        });
    }
    let trash_root = prepare_root(trash_root_path, false)?;
    let listing = list_artifact_file_trash(&trash_root)?;
    let mut purged_entry_ids = Vec::new();
    let mut freed_bytes = 0;
    for entry in listing.entries {
        let trashed_at = parse_rfc3339(&entry.trashed_at, "trashed_at")?;
        if request.all || trashed_at <= cutoff {
            let entry_dir =
                checked_existing_child(&trash_root, &entry.entry_id)?;
            remove_entry_dir(&trash_root, &entry_dir)?;
            freed_bytes += entry.size_bytes.unwrap_or(0);
            purged_entry_ids.push(entry.entry_id);
        }
    }
    purged_entry_ids.sort();
    Ok(ArtifactFileTrashPurgeWire {
        schema_version: ARTIFACT_FILE_LIFECYCLE_WIRE_SCHEMA_VERSION,
        purged_entry_ids,
        freed_bytes,
        unreadable_entries: listing.unreadable_entries,
    })
}

fn store_in_reserved_entry(
    request: &ArtifactFileTrashRequestWire,
    trash_root: &Path,
    entry_id: &str,
    entry_dir: &Path,
    artifact_id: String,
) -> Result<ArtifactFileTrashEntryWire, String> {
    ensure_within(trash_root, entry_dir)?;
    let stored_filename = request
        .stored_path
        .as_deref()
        .map(Path::new)
        .map(|path| {
            path.file_name()
                .and_then(|value| value.to_str())
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .ok_or_else(|| {
                    format!(
                        "stored artifact path has no UTF-8 filename: {}",
                        path.display()
                    )
                })
        })
        .transpose()?;
    if let Some(filename) = stored_filename.as_deref() {
        validate_single_component(filename, "stored filename")?;
        let source =
            Path::new(request.stored_path.as_deref().unwrap_or_default());
        ensure_regular_file(source, "stored artifact")?;
    }

    let entry = ArtifactFileTrashEntryWire {
        schema_version: ARTIFACT_FILE_LIFECYCLE_WIRE_SCHEMA_VERSION,
        entry_id: entry_id.to_string(),
        artifact_id,
        trashed_at: request.trashed_at.clone(),
        reason: request.reason.clone(),
        size_bytes: record_u64(&request.record, "size_bytes"),
        stored_filename: stored_filename.clone(),
        stored_path: request.stored_path.clone(),
        record: request.record.clone(),
    };
    write_entry_atomically(entry_dir, &entry)?;

    if let Some(filename) = stored_filename {
        let destination = entry_dir.join(filename);
        ensure_new_child(entry_dir, &destination)?;
        move_file(
            Path::new(request.stored_path.as_deref().unwrap_or_default()),
            &destination,
        )?;
    }
    Ok(entry)
}

fn write_entry_atomically(
    entry_dir: &Path,
    entry: &ArtifactFileTrashEntryWire,
) -> Result<(), String> {
    let bytes = serde_json::to_vec_pretty(entry)
        .map_err(|error| format!("failed to serialize trash entry: {error}"))?;
    let mut temporary = NamedTempFile::new_in(entry_dir).map_err(|error| {
        format!("failed to create trash entry temp file: {error}")
    })?;
    temporary
        .write_all(&bytes)
        .and_then(|_| temporary.as_file().sync_all())
        .map_err(|error| format!("failed to write trash entry: {error}"))?;
    let destination = entry_dir.join("entry.json");
    ensure_new_child(entry_dir, &destination)?;
    temporary.persist(&destination).map_err(|error| {
        format!("failed to publish trash entry: {}", error.error)
    })?;
    File::open(entry_dir)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| {
            format!("failed to sync trash entry directory: {error}")
        })
}

fn read_entry_dir(
    trash_root: &Path,
    entry_dir: &Path,
) -> Result<ArtifactFileTrashEntryWire, String> {
    let metadata = fs::symlink_metadata(entry_dir)
        .map_err(|error| format!("failed to inspect trash entry: {error}"))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(format!(
            "trash entry is not a real directory: {}",
            entry_dir.display()
        ));
    }
    let entry_dir = entry_dir
        .canonicalize()
        .map_err(|error| format!("failed to resolve trash entry: {error}"))?;
    ensure_within(trash_root, &entry_dir)?;
    let entry_path = entry_dir.join("entry.json");
    ensure_regular_file(&entry_path, "trash entry metadata")?;
    let bytes = fs::read(&entry_path).map_err(|error| {
        format!("failed to read trash entry metadata: {error}")
    })?;
    let entry: ArtifactFileTrashEntryWire = serde_json::from_slice(&bytes)
        .map_err(|error| format!("invalid trash entry metadata: {error}"))?;
    require_schema(entry.schema_version, "trash entry")?;
    parse_rfc3339(&entry.trashed_at, "trashed_at")?;
    validate_single_component(&entry.entry_id, "trash entry id")?;
    if entry.artifact_id.trim().is_empty() {
        return Err("trash entry artifact id is empty".to_string());
    }
    let directory_name = entry_dir
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| "trash entry directory name is not UTF-8".to_string())?;
    if entry.entry_id != directory_name {
        return Err(format!(
            "trash entry id {} does not match directory {directory_name}",
            entry.entry_id
        ));
    }
    if let Some(filename) = &entry.stored_filename {
        validate_single_component(filename, "stored filename")?;
    }
    Ok(entry)
}

fn prepare_root(root: &Path, create: bool) -> Result<PathBuf, String> {
    if root.as_os_str().is_empty() {
        return Err("artifact trash root is empty".to_string());
    }
    if root.exists() {
        let metadata = fs::symlink_metadata(root).map_err(|error| {
            format!("failed to inspect trash root: {error}")
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(format!(
                "artifact trash root is not a real directory: {}",
                root.display()
            ));
        }
    } else if create {
        fs::create_dir_all(root)
            .map_err(|error| format!("failed to create trash root: {error}"))?;
    } else {
        return Err(format!(
            "artifact trash root does not exist: {}",
            root.display()
        ));
    }
    root.canonicalize()
        .map_err(|error| format!("failed to resolve trash root: {error}"))
}

fn reserve_entry_dir(
    trash_root: &Path,
    base_id: &str,
) -> Result<(String, PathBuf), String> {
    for counter in 0_u64.. {
        let entry_id = if counter == 0 {
            base_id.to_string()
        } else {
            format!("{base_id}-{}", counter + 1)
        };
        validate_single_component(&entry_id, "trash entry id")?;
        let path = trash_root.join(&entry_id);
        ensure_within(trash_root, &path)?;
        match fs::create_dir(&path) {
            Ok(()) => {
                let canonical = path.canonicalize().map_err(|error| {
                    format!("failed to resolve new trash entry: {error}")
                })?;
                ensure_within(trash_root, &canonical)?;
                return Ok((entry_id, canonical));
            }
            Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(format!("failed to create trash entry: {error}"))
            }
        }
    }
    unreachable!("unbounded collision counter")
}

fn checked_existing_child(
    parent: &Path,
    name: &str,
) -> Result<PathBuf, String> {
    validate_single_component(name, "path component")?;
    let child = parent.join(name);
    let metadata = fs::symlink_metadata(&child).map_err(|error| {
        format!("failed to inspect {}: {error}", child.display())
    })?;
    if metadata.file_type().is_symlink() {
        return Err(format!("refusing symlink path: {}", child.display()));
    }
    let canonical = child.canonicalize().map_err(|error| {
        format!("failed to resolve {}: {error}", child.display())
    })?;
    ensure_within(parent, &canonical)?;
    Ok(canonical)
}

fn ensure_new_child(parent: &Path, child: &Path) -> Result<(), String> {
    let child_parent = child
        .parent()
        .ok_or_else(|| format!("path has no parent: {}", child.display()))?
        .canonicalize()
        .map_err(|error| {
            format!("failed to resolve parent of {}: {error}", child.display())
        })?;
    ensure_within(parent, &child_parent)?;
    if child.exists() {
        return Err(format!("refusing to overwrite {}", child.display()));
    }
    Ok(())
}

fn ensure_within(root: &Path, path: &Path) -> Result<(), String> {
    if !path.starts_with(root) {
        return Err(format!(
            "refusing path outside artifact trash root: {}",
            path.display()
        ));
    }
    Ok(())
}

fn validate_single_component(value: &str, label: &str) -> Result<(), String> {
    let path = Path::new(value);
    let mut components = path.components();
    if value.is_empty()
        || !matches!(components.next(), Some(Component::Normal(_)))
        || components.next().is_some()
    {
        return Err(format!("{label} is not a safe single path component"));
    }
    Ok(())
}

fn ensure_regular_file(path: &Path, label: &str) -> Result<(), String> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| format!("failed to inspect {label}: {error}"))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(format!("{label} is not a real file: {}", path.display()));
    }
    Ok(())
}

fn move_file(source: &Path, destination: &Path) -> Result<(), String> {
    match fs::rename(source, destination) {
        Ok(()) => Ok(()),
        Err(rename_error) => {
            fs::copy(source, destination).map_err(|copy_error| {
                format!(
                    "failed to move artifact into trash ({rename_error}); \
                     copy fallback failed: {copy_error}"
                )
            })?;
            File::open(destination)
                .and_then(|file| file.sync_all())
                .and_then(|_| {
                    destination
                        .parent()
                        .map(File::open)
                        .transpose()?
                        .map_or(Ok(()), |directory| directory.sync_all())
                })
                .map_err(|sync_error| {
                    let _ = fs::remove_file(destination);
                    format!(
                        "copied artifact but failed to sync destination: \
                         {sync_error}"
                    )
                })?;
            if let Err(unlink_error) = fs::remove_file(source) {
                let _ = fs::remove_file(destination);
                return Err(format!(
                    "copied artifact into trash but failed to remove source: \
                     {unlink_error}"
                ));
            }
            Ok(())
        }
    }
}

fn restore_payload(payload: &Path, destination: &Path) -> Result<(), String> {
    if destination.exists() {
        ensure_regular_file(destination, "restore destination")?;
        let payload_bytes = fs::read(payload).map_err(|error| {
            format!("failed to read trash payload: {error}")
        })?;
        let destination_bytes = fs::read(destination).map_err(|error| {
            format!("failed to read restore destination: {error}")
        })?;
        if payload_bytes != destination_bytes {
            return Err(format!(
                "restore destination exists with different content: {}",
                destination.display()
            ));
        }
        fs::remove_file(payload).map_err(|error| {
            format!("failed to consume trash payload: {error}")
        })?;
        return Ok(());
    }
    let parent = destination.parent().ok_or_else(|| {
        format!(
            "restore destination has no parent: {}",
            destination.display()
        )
    })?;
    fs::create_dir_all(parent).map_err(|error| {
        format!("failed to create restore destination parent: {error}")
    })?;
    move_file(payload, destination)
}

fn remove_entry_dir(trash_root: &Path, entry_dir: &Path) -> Result<(), String> {
    let metadata = fs::symlink_metadata(entry_dir)
        .map_err(|error| format!("failed to inspect trash entry: {error}"))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(format!(
            "refusing to remove non-directory trash entry: {}",
            entry_dir.display()
        ));
    }
    let canonical = entry_dir
        .canonicalize()
        .map_err(|error| format!("failed to resolve trash entry: {error}"))?;
    ensure_within(trash_root, &canonical)?;
    if canonical == trash_root {
        return Err("refusing to remove artifact trash root".to_string());
    }
    fs::remove_dir_all(&canonical)
        .map_err(|error| format!("failed to remove trash entry: {error}"))
}

fn record_field<'a>(
    record: &'a JsonValue,
    field: &str,
) -> Option<&'a JsonValue> {
    record
        .get(field)
        .or_else(|| record.get("artifact").and_then(|value| value.get(field)))
}

fn record_string(record: &JsonValue, field: &str) -> Option<String> {
    record_field(record, field)
        .and_then(JsonValue::as_str)
        .map(str::to_string)
}

fn record_u64(record: &JsonValue, field: &str) -> Option<u64> {
    record_field(record, field).and_then(JsonValue::as_u64)
}

fn sanitize_artifact_id(artifact_id: &str) -> String {
    let sanitized = artifact_id
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric()
                || matches!(character, '-' | '_' | '.')
            {
                character
            } else {
                '_'
            }
        })
        .collect::<String>();
    let sanitized = sanitized.trim_matches('.').to_string();
    if sanitized.is_empty() {
        "artifact".to_string()
    } else {
        sanitized
    }
}

fn parse_rfc3339(raw: &str, field: &str) -> Result<DateTime<Utc>, String> {
    DateTime::parse_from_rfc3339(raw)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| format!("invalid RFC3339 {field}: {raw}"))
}

fn entry_timestamp(entry: &ArtifactFileTrashEntryWire) -> i64 {
    parse_rfc3339(&entry.trashed_at, "trashed_at")
        .map(|value| value.timestamp_micros())
        .unwrap_or(i64::MIN)
}

fn require_schema(schema_version: u64, wire_name: &str) -> Result<(), String> {
    if schema_version != ARTIFACT_FILE_LIFECYCLE_WIRE_SCHEMA_VERSION {
        return Err(format!(
            "artifact-file lifecycle wire schema mismatch for {wire_name}: \
             got {schema_version}, expected \
             {ARTIFACT_FILE_LIFECYCLE_WIRE_SCHEMA_VERSION}"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

    fn store_request(
        root: &Path,
        record: JsonValue,
        stored_path: Option<&Path>,
        trashed_at: &str,
    ) -> ArtifactFileTrashRequestWire {
        ArtifactFileTrashRequestWire {
            schema_version: ARTIFACT_FILE_LIFECYCLE_WIRE_SCHEMA_VERSION,
            trash_root: root.to_string_lossy().into_owned(),
            record,
            stored_path: stored_path
                .map(|path| path.to_string_lossy().into_owned()),
            reason: "retention".to_string(),
            trashed_at: trashed_at.to_string(),
        }
    }

    #[test]
    fn byte_backed_store_list_restore_and_purge_round_trip() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("trash");
        let stored = temp.path().join("store/file.png");
        fs::create_dir_all(stored.parent().unwrap()).unwrap();
        fs::write(&stored, b"payload").unwrap();
        let record = json!({
            "id":"default:abcdef0123456789abcdef01",
            "path":stored,
            "size_bytes":7,
            "unknown":{"preserved":true}
        });
        let entry = trash_artifact_file(&store_request(
            &root,
            record.clone(),
            Some(&stored),
            "2026-07-30T12:00:00Z",
        ))
        .unwrap();
        assert!(!stored.exists());
        assert!(entry.entry_id.contains("default_abcdef"));

        let listing = list_artifact_file_trash(&root).unwrap();
        assert_eq!(listing.entries, vec![entry.clone()]);
        assert_eq!(listing.unreadable_entries, 0);

        fs::write(&stored, b"different").unwrap();
        let restore_request = ArtifactFileTrashRestoreRequestWire {
            schema_version: ARTIFACT_FILE_LIFECYCLE_WIRE_SCHEMA_VERSION,
            trash_root: root.to_string_lossy().into_owned(),
            entry_id: entry.entry_id.clone(),
        };
        assert!(restore_artifact_file_trash(&restore_request)
            .unwrap_err()
            .contains("different content"));
        fs::remove_file(&stored).unwrap();
        let restored = restore_artifact_file_trash(&restore_request).unwrap();
        assert_eq!(restored.record, record);
        assert_eq!(fs::read(&stored).unwrap(), b"payload");
        assert!(list_artifact_file_trash(&root).unwrap().entries.is_empty());

        let old = trash_artifact_file(&store_request(
            &root,
            json!({"id":"old","path":null,"size_bytes":3}),
            None,
            "2026-07-01T00:00:00Z",
        ))
        .unwrap();
        let boundary = trash_artifact_file(&store_request(
            &root,
            json!({"id":"boundary","path":null,"size_bytes":4}),
            None,
            "2026-07-15T00:00:00Z",
        ))
        .unwrap();
        let newer = trash_artifact_file(&store_request(
            &root,
            json!({"id":"new","path":null,"size_bytes":5}),
            None,
            "2026-07-16T00:00:00Z",
        ))
        .unwrap();
        let purged =
            purge_artifact_file_trash(&ArtifactFileTrashPurgeRequestWire {
                schema_version: ARTIFACT_FILE_LIFECYCLE_WIRE_SCHEMA_VERSION,
                trash_root: root.to_string_lossy().into_owned(),
                before: "2026-07-15T00:00:00Z".to_string(),
                all: false,
            })
            .unwrap();
        assert_eq!(
            purged.purged_entry_ids,
            vec![old.entry_id, boundary.entry_id]
        );
        assert_eq!(purged.freed_bytes, 7);
        assert_eq!(
            list_artifact_file_trash(&root).unwrap().entries[0].entry_id,
            newer.entry_id
        );
    }

    #[test]
    fn byte_free_collision_and_unreadable_listing_are_deterministic() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("trash");
        let request = store_request(
            &root,
            json!({"id":"default:abc","path":null}),
            None,
            "2026-07-30T12:00:00Z",
        );
        let first = trash_artifact_file(&request).unwrap();
        let second = trash_artifact_file(&request).unwrap();
        assert_ne!(first.entry_id, second.entry_id);
        fs::create_dir(root.join("broken")).unwrap();
        fs::write(root.join("broken/entry.json"), b"{broken").unwrap();
        let listing = list_artifact_file_trash(&root).unwrap();
        assert_eq!(listing.entries.len(), 2);
        assert_eq!(listing.unreadable_entries, 1);

        let restored =
            restore_artifact_file_trash(&ArtifactFileTrashRestoreRequestWire {
                schema_version: ARTIFACT_FILE_LIFECYCLE_WIRE_SCHEMA_VERSION,
                trash_root: root.to_string_lossy().into_owned(),
                entry_id: first.entry_id,
            })
            .unwrap();
        assert_eq!(restored.restored_path, None);
        assert_eq!(restored.record["id"], json!("default:abc"));

        let purged =
            purge_artifact_file_trash(&ArtifactFileTrashPurgeRequestWire {
                schema_version: ARTIFACT_FILE_LIFECYCLE_WIRE_SCHEMA_VERSION,
                trash_root: root.to_string_lossy().into_owned(),
                before: "2026-01-01T00:00:00Z".to_string(),
                all: true,
            })
            .unwrap();
        assert_eq!(purged.purged_entry_ids, vec![second.entry_id]);
        assert_eq!(purged.unreadable_entries, 1);
    }

    #[cfg(unix)]
    #[test]
    fn refuses_symlink_escape_and_unsafe_entry_id() {
        use std::os::unix::fs::symlink;

        let temp = tempdir().unwrap();
        let root = temp.path().join("trash");
        let outside = temp.path().join("outside");
        fs::create_dir_all(&root).unwrap();
        fs::create_dir_all(&outside).unwrap();
        symlink(&outside, root.join("escape")).unwrap();

        let listing = list_artifact_file_trash(&root).unwrap();
        assert_eq!(listing.unreadable_entries, 1);
        let error =
            restore_artifact_file_trash(&ArtifactFileTrashRestoreRequestWire {
                schema_version: ARTIFACT_FILE_LIFECYCLE_WIRE_SCHEMA_VERSION,
                trash_root: root.to_string_lossy().into_owned(),
                entry_id: "../outside".to_string(),
            })
            .unwrap_err();
        assert!(error.contains("safe single path component"));
        assert!(outside.exists());
    }
}
