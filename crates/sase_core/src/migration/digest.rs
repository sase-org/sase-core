//! Temporary offline migration digest helpers.
//!
//! Deletion owner: sase-x7.14.

use std::fs::{self, Metadata};
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::MIGRATION_WIRE_SCHEMA_VERSION;

pub const MIGRATION_TREE_DIGEST_ALGORITHM: &str =
    "sase-migration-tree-sha256-v1";
pub const MIGRATION_FINGERPRINT_ALGORITHM: &str =
    "sase-migration-fingerprint-sha256-v1";

#[derive(Debug, Error)]
pub enum MigrationDigestError {
    #[error("failed to read migration path {}: {source}", path.display())]
    Read {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error(
        "failed to compute relative path for {} under {}",
        path.display(),
        root.display()
    )]
    RelativePath { root: PathBuf, path: PathBuf },
    #[error("failed to serialize migration digest payload: {source}")]
    Serialize {
        #[source]
        source: serde_json::Error,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationTreeDigestWire {
    pub schema_version: u32,
    pub algorithm: String,
    pub root: String,
    pub digest: String,
    #[serde(default)]
    pub entries: Vec<MigrationTreeDigestEntryWire>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationTreeDigestEntryWire {
    pub relative_path: String,
    pub kind: String,
    pub mode: u32,
    #[serde(default)]
    pub size: Option<u64>,
    #[serde(default)]
    pub sha256: Option<String>,
    #[serde(default)]
    pub symlink_target: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationFingerprintWire {
    pub schema_version: u32,
    pub algorithm: String,
    pub digest: String,
}

pub fn tree_digest(
    root: &Path,
) -> Result<MigrationTreeDigestWire, MigrationDigestError> {
    let mut entries = Vec::new();
    let metadata = symlink_metadata(root)?;
    if metadata.file_type().is_dir() {
        collect_tree_entries(root, root, &mut entries)?;
    } else {
        entries.push(entry_for(root, root, &metadata)?);
    }
    entries.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    let digest = canonical_json_sha256(&serde_json::json!({
        "algorithm": MIGRATION_TREE_DIGEST_ALGORITHM,
        "entries": entries,
    }))?;
    Ok(MigrationTreeDigestWire {
        schema_version: MIGRATION_WIRE_SCHEMA_VERSION,
        algorithm: MIGRATION_TREE_DIGEST_ALGORITHM.to_string(),
        root: root.to_string_lossy().into_owned(),
        digest,
        entries,
    })
}

pub fn fingerprint(
    record_stream: &JsonValue,
) -> Result<String, MigrationDigestError> {
    canonical_json_sha256(&serde_json::json!({
        "algorithm": MIGRATION_FINGERPRINT_ALGORITHM,
        "records": record_stream,
    }))
}

pub(crate) fn canonical_json_sha256(
    value: &JsonValue,
) -> Result<String, MigrationDigestError> {
    let bytes = canonical_json_bytes(value)?;
    Ok(sha256_hex(&bytes))
}

fn canonical_json_bytes(
    value: &JsonValue,
) -> Result<Vec<u8>, MigrationDigestError> {
    serde_json::to_vec(&canonical_json_value(value))
        .map_err(|source| MigrationDigestError::Serialize { source })
}

fn canonical_json_value(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Array(items) => {
            JsonValue::Array(items.iter().map(canonical_json_value).collect())
        }
        JsonValue::Object(map) => {
            let mut sorted = JsonMap::new();
            let mut entries: Vec<_> = map.iter().collect();
            entries.sort_by(|left, right| left.0.cmp(right.0));
            for (key, value) in entries {
                sorted.insert(key.clone(), canonical_json_value(value));
            }
            JsonValue::Object(sorted)
        }
        _ => value.clone(),
    }
}

fn collect_tree_entries(
    root: &Path,
    current: &Path,
    entries: &mut Vec<MigrationTreeDigestEntryWire>,
) -> Result<(), MigrationDigestError> {
    let mut children = Vec::new();
    for child in
        fs::read_dir(current).map_err(|source| MigrationDigestError::Read {
            path: current.to_path_buf(),
            source,
        })?
    {
        let child = child.map_err(|source| MigrationDigestError::Read {
            path: current.to_path_buf(),
            source,
        })?;
        children.push(child.path());
    }
    children.sort();
    for child in children {
        let metadata = symlink_metadata(&child)?;
        entries.push(entry_for(root, &child, &metadata)?);
        if metadata.file_type().is_dir() {
            collect_tree_entries(root, &child, entries)?;
        }
    }
    Ok(())
}

fn entry_for(
    root: &Path,
    path: &Path,
    metadata: &Metadata,
) -> Result<MigrationTreeDigestEntryWire, MigrationDigestError> {
    let file_type = metadata.file_type();
    let kind = if file_type.is_symlink() {
        "symlink"
    } else if file_type.is_dir() {
        "directory"
    } else if file_type.is_file() {
        "file"
    } else {
        "other"
    };
    let sha256 = file_type.is_file().then(|| sha256_file(path)).transpose()?;
    let symlink_target = file_type
        .is_symlink()
        .then(|| {
            fs::read_link(path)
                .map(|target| target.to_string_lossy().into_owned())
        })
        .transpose()
        .map_err(|source| MigrationDigestError::Read {
            path: path.to_path_buf(),
            source,
        })?;
    Ok(MigrationTreeDigestEntryWire {
        relative_path: relative_path(root, path)?,
        kind: kind.to_string(),
        mode: file_mode(metadata),
        size: file_type.is_file().then_some(metadata.len()),
        sha256,
        symlink_target,
    })
}

fn symlink_metadata(path: &Path) -> Result<Metadata, MigrationDigestError> {
    fs::symlink_metadata(path).map_err(|source| MigrationDigestError::Read {
        path: path.to_path_buf(),
        source,
    })
}

fn sha256_file(path: &Path) -> Result<String, MigrationDigestError> {
    let mut file =
        fs::File::open(path).map_err(|source| MigrationDigestError::Read {
            path: path.to_path_buf(),
            source,
        })?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let bytes = file.read(&mut buffer).map_err(|source| {
            MigrationDigestError::Read {
                path: path.to_path_buf(),
                source,
            }
        })?;
        if bytes == 0 {
            break;
        }
        hasher.update(&buffer[..bytes]);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn relative_path(
    root: &Path,
    path: &Path,
) -> Result<String, MigrationDigestError> {
    let relative = path.strip_prefix(root).map_err(|_| {
        MigrationDigestError::RelativePath {
            root: root.to_path_buf(),
            path: path.to_path_buf(),
        }
    })?;
    if relative.as_os_str().is_empty() {
        return Ok(".".to_string());
    }
    let mut parts = Vec::new();
    for component in relative.components() {
        parts.push(component.as_os_str().to_string_lossy().into_owned());
    }
    Ok(parts.join("/"))
}

#[cfg(unix)]
fn file_mode(metadata: &Metadata) -> u32 {
    use std::os::unix::fs::PermissionsExt;

    metadata.permissions().mode() & 0o7777
}

#[cfg(not(unix))]
fn file_mode(_metadata: &Metadata) -> u32 {
    0
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn fingerprint_is_stable_for_object_key_order() {
        let left = serde_json::json!([{"b": 2, "a": 1}]);
        let right = serde_json::json!([{"a": 1, "b": 2}]);
        assert_eq!(fingerprint(&left).unwrap(), fingerprint(&right).unwrap());
    }

    #[test]
    fn tree_digest_changes_when_file_content_changes() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        fs::create_dir(root.join("nested")).unwrap();
        fs::write(root.join("nested/file.txt"), "before").unwrap();
        let before = tree_digest(root).unwrap();
        fs::write(root.join("nested/file.txt"), "after").unwrap();
        let after = tree_digest(root).unwrap();
        assert_ne!(before.digest, after.digest);
        assert!(after
            .entries
            .iter()
            .any(|entry| entry.relative_path == "nested/file.txt"));
    }
}
