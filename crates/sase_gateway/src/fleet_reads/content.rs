//! Opaque content handles and canonicalized artifact-path reads.
//!
//! Path canonicalization here is a filesystem security boundary: every
//! handle resolves under its record's canonical artifact root.

use std::{
    collections::BTreeSet,
    fs,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use sase_core::{
    agent_scan::AgentArtifactRecordWire,
    fleet_contract::{
        ContentHandleKindWire, ContentHandleWire, FleetContentReadResponseWire,
        ResourceRevisionWire, StoreCursorWire, FLEET_CONTRACT_SCHEMA_VERSION,
    },
};
use sha2::{Digest, Sha256};

use super::errors::FleetReadError;

#[derive(Clone, Debug)]
pub(super) struct FleetContentSource {
    pub(super) handle: ContentHandleWire,
    pub(super) row_revision: ResourceRevisionWire,
    canonical_path: PathBuf,
    artifact_root: PathBuf,
}

pub(super) fn content_handles_for_record(
    record: &AgentArtifactRecordWire,
    logical_key: &str,
    row_revision: &ResourceRevisionWire,
) -> Result<(Vec<ContentHandleWire>, Vec<FleetContentSource>), FleetReadError> {
    let artifact_root = PathBuf::from(&record.artifact_dir);
    let Some(canonical_root) = fs::canonicalize(&artifact_root).ok() else {
        return Ok((Vec::new(), Vec::new()));
    };
    let mut seen = BTreeSet::new();
    let mut handles = Vec::new();
    let mut sources = Vec::new();
    for candidate in content_path_candidates(record) {
        let Some((canonical_path, relative_key)) =
            canonical_content_path(&canonical_root, &candidate.raw_path)
        else {
            continue;
        };
        if !seen.insert(canonical_path.clone()) {
            continue;
        }
        let metadata = fs::metadata(&canonical_path).map_err(|_| {
            FleetReadError::Backend("content_metadata".to_string())
        })?;
        if !metadata.is_file() {
            continue;
        }
        let handle_id = content_handle_id(
            logical_key,
            row_revision.revision,
            candidate.kind,
            &relative_key,
        );
        let handle = ContentHandleWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            id: handle_id,
            kind: candidate.kind,
            revision: Some(row_revision.clone()),
            digest: None,
            byte_len: Some(metadata.len()),
            supports_range: true,
            supports_growth: candidate.supports_growth,
        };
        sources.push(FleetContentSource {
            handle: handle.clone(),
            row_revision: row_revision.clone(),
            canonical_path,
            artifact_root: canonical_root.clone(),
        });
        handles.push(handle);
    }
    handles.sort_by(|left, right| {
        (left.kind, left.id.as_str()).cmp(&(right.kind, right.id.as_str()))
    });
    sources.sort_by(|left, right| left.handle.id.cmp(&right.handle.id));
    Ok((handles, sources))
}

struct ContentPathCandidate {
    kind: ContentHandleKindWire,
    raw_path: String,
    supports_growth: bool,
}

fn content_path_candidates(
    record: &AgentArtifactRecordWire,
) -> Vec<ContentPathCandidate> {
    let mut candidates = Vec::new();
    if let Some(meta) = &record.agent_meta {
        push_path(
            &mut candidates,
            ContentHandleKindWire::Output,
            meta.output_path.as_deref(),
            true,
        );
        push_path(
            &mut candidates,
            ContentHandleKindWire::Question,
            meta.question_response_path.as_deref(),
            true,
        );
        push_path(
            &mut candidates,
            ContentHandleKindWire::Log,
            meta.family_shell
                .as_ref()
                .and_then(|shell| shell.output_path.as_deref()),
            true,
        );
    }
    if let Some(done) = &record.done {
        push_path(
            &mut candidates,
            ContentHandleKindWire::Transcript,
            done.response_path.as_deref(),
            false,
        );
        push_path(
            &mut candidates,
            ContentHandleKindWire::Output,
            done.output_path.as_deref(),
            false,
        );
        push_path(
            &mut candidates,
            ContentHandleKindWire::Diff,
            done.diff_path.as_deref(),
            false,
        );
        push_path(
            &mut candidates,
            ContentHandleKindWire::Log,
            done.family_shell
                .as_ref()
                .and_then(|shell| shell.output_path.as_deref()),
            false,
        );
    }
    candidates
}

fn push_path(
    candidates: &mut Vec<ContentPathCandidate>,
    kind: ContentHandleKindWire,
    raw_path: Option<&str>,
    supports_growth: bool,
) {
    let Some(raw_path) =
        raw_path.map(str::trim).filter(|value| !value.is_empty())
    else {
        return;
    };
    candidates.push(ContentPathCandidate {
        kind,
        raw_path: raw_path.to_string(),
        supports_growth,
    });
}

fn canonical_content_path(
    artifact_root: &Path,
    raw_path: &str,
) -> Option<(PathBuf, String)> {
    let raw = Path::new(raw_path);
    let candidate = if raw.is_absolute() {
        raw.to_path_buf()
    } else {
        artifact_root.join(raw)
    };
    let canonical = fs::canonicalize(candidate).ok()?;
    if !canonical.starts_with(artifact_root) {
        return None;
    }
    let relative = canonical
        .strip_prefix(artifact_root)
        .ok()?
        .to_string_lossy()
        .replace('\\', "/");
    if relative.is_empty() {
        return None;
    }
    Some((canonical, relative))
}

fn content_handle_id(
    logical_key: &str,
    revision: u64,
    kind: ContentHandleKindWire,
    relative_key: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"sase-fleet-content-handle-v1\0");
    hasher.update(logical_key.as_bytes());
    hasher.update(b"\0");
    hasher.update(revision.to_le_bytes());
    hasher.update(b"\0");
    hasher.update(format!("{kind:?}").as_bytes());
    hasher.update(b"\0");
    hasher.update(relative_key.as_bytes());
    format!("ch{}", hex::encode(hasher.finalize()))
}

pub(super) async fn read_content_range(
    source: FleetContentSource,
    offset: u64,
    limit: u64,
    cursor: StoreCursorWire,
) -> Result<FleetContentReadResponseWire, FleetReadError> {
    tokio::task::spawn_blocking(move || {
        let canonical_path = fs::canonicalize(&source.canonical_path)
            .map_err(|_| FleetReadError::NotFound("handle_id".to_string()))?;
        if !canonical_path.starts_with(&source.artifact_root) {
            return Err(FleetReadError::Stale("handle_id".to_string()));
        }
        let metadata = fs::metadata(&canonical_path)
            .map_err(|_| FleetReadError::NotFound("handle_id".to_string()))?;
        let total_byte_len = metadata.len();
        if offset > total_byte_len {
            return Err(FleetReadError::Validation(
                "content offset exceeds current content length".to_string(),
            ));
        }
        let mut file = fs::File::open(&canonical_path)
            .map_err(|_| FleetReadError::NotFound("handle_id".to_string()))?;
        file.seek(SeekFrom::Start(offset))
            .map_err(|_| FleetReadError::Backend("content_seek".to_string()))?;
        let available = total_byte_len.saturating_sub(offset);
        let read_len = available.min(limit);
        let mut bytes = vec![0_u8; read_len as usize];
        file.read_exact(&mut bytes)
            .map_err(|_| FleetReadError::Backend("content_read".to_string()))?;
        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        let returned_bytes = bytes.len() as u64;
        let next_offset = offset.saturating_add(returned_bytes);
        let eof = next_offset >= total_byte_len;
        Ok(FleetContentReadResponseWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            cursor,
            handle: ContentHandleWire {
                byte_len: Some(total_byte_len),
                ..source.handle
            },
            offset,
            returned_bytes,
            total_byte_len,
            next_offset: (!eof).then_some(next_offset),
            eof,
            supports_growth: source.handle.supports_growth,
            sha256: hex::encode(hasher.finalize()),
            data_base64: BASE64.encode(bytes),
        })
    })
    .await
    .map_err(|_| FleetReadError::Backend("content_join".to_string()))?
}
