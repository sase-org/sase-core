use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{
    changespec_handle, source_file_observed_event, source_file_reparsed_event,
    ChangeSpecDetailWire, ChangeSpecListRequestWire, ChangeSpecSummaryWire,
    EventAppendOutcomeWire, EventAppendRequestWire, EventSourceWire,
    ProjectionDb, ProjectionError, CHANGESPEC_PROJECTION_NAME,
};
use crate::{
    parser::parse_project_bytes, project_spec::is_archive_project_spec,
    wire::ChangeSpecWire,
};

pub const INDEXING_WIRE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceChangeOperationWire {
    Upsert,
    Delete,
    Rewrite,
    Reconcile,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceFingerprintWire {
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_size: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modified_at_unix_millis: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inode: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_sha256: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceIdentityWire {
    pub schema_version: u32,
    pub domain: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    pub source_path: String,
    #[serde(default)]
    pub is_archive: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fingerprint: Option<SourceFingerprintWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_indexed_event_seq: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceChangeWire {
    pub schema_version: u32,
    pub identity: SourceIdentityWire,
    pub operation: SourceChangeOperationWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexingDomainReportWire {
    pub schema_version: u32,
    pub domain: String,
    pub queued_changes: u64,
    pub coalesced_changes: u64,
    pub indexed_sources: u64,
    pub failed_parses: u64,
    pub shadow_diff_counts: ShadowDiffCountsWire,
    pub source_paths: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShadowDiffCountsWire {
    pub missing: u64,
    pub stale: u64,
    pub extra: u64,
    pub corrupt: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShadowDiffCategoryWire {
    Missing,
    Stale,
    Extra,
    Corrupt,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShadowDiffRecordWire {
    pub schema_version: u32,
    pub domain: String,
    pub category: ShadowDiffCategoryWire,
    pub source_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handle: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShadowDiffReportWire {
    pub schema_version: u32,
    pub domain: String,
    pub records: Vec<ShadowDiffRecordWire>,
    pub counts: ShadowDiffCountsWire,
}

impl Default for SourceFingerprintWire {
    fn default() -> Self {
        Self {
            schema_version: INDEXING_WIRE_SCHEMA_VERSION,
            file_size: None,
            modified_at_unix_millis: None,
            inode: None,
            content_sha256: None,
        }
    }
}

impl SourceIdentityWire {
    pub fn stable_key(&self) -> String {
        format!(
            "{}:{}:{}",
            self.domain,
            self.project_id.as_deref().unwrap_or(""),
            self.source_path
        )
    }
}

pub fn source_event_idempotency_key(
    operation: &SourceChangeOperationWire,
    identity: &SourceIdentityWire,
) -> String {
    let fingerprint = identity
        .fingerprint
        .as_ref()
        .and_then(|value| value.content_sha256.as_deref())
        .map(str::to_string)
        .or_else(|| {
            identity.fingerprint.as_ref().and_then(|value| {
                Some(format!(
                    "size={}:mtime={}",
                    value.file_size?, value.modified_at_unix_millis?
                ))
            })
        })
        .unwrap_or_else(|| "unknown".to_string());
    let operation = match operation {
        SourceChangeOperationWire::Upsert => "upsert",
        SourceChangeOperationWire::Delete => "delete",
        SourceChangeOperationWire::Rewrite => "rewrite",
        SourceChangeOperationWire::Reconcile => "reconcile",
    };
    format!(
        "indexer:{operation}:{}:{fingerprint}",
        identity.stable_key()
    )
}

pub fn source_fingerprint_from_path(
    path: &Path,
    include_content_hash: bool,
) -> Option<SourceFingerprintWire> {
    let metadata = fs::metadata(path).ok()?;
    let modified_at_unix_millis =
        metadata.modified().ok().and_then(system_time_unix_millis);
    let content_sha256 = include_content_hash
        .then(|| fs::read(path).ok().map(|bytes| sha256_hex(&bytes)))
        .flatten();
    Some(SourceFingerprintWire {
        schema_version: INDEXING_WIRE_SCHEMA_VERSION,
        file_size: Some(metadata.len()),
        modified_at_unix_millis,
        inode: file_inode(&metadata),
        content_sha256,
    })
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

#[cfg(unix)]
fn file_inode(metadata: &fs::Metadata) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    Some(metadata.ino())
}

#[cfg(not(unix))]
fn file_inode(_metadata: &fs::Metadata) -> Option<u64> {
    None
}

fn system_time_unix_millis(value: SystemTime) -> Option<i64> {
    let duration = value.duration_since(UNIX_EPOCH).ok()?;
    Some(duration.as_millis().min(i64::MAX as u128) as i64)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeSpecSourceFileWire {
    pub schema_version: u32,
    pub project_id: String,
    pub source_path: String,
    pub is_archive: bool,
    pub is_legacy: bool,
    pub fingerprint: SourceFingerprintWire,
}

impl ProjectionDb {
    pub fn backfill_changespec_sources(
        &mut self,
        projects_root: &Path,
        source: EventSourceWire,
        host_id: impl Into<String>,
    ) -> Result<Vec<EventAppendOutcomeWire>, ProjectionError> {
        let host_id = host_id.into();
        let sources = discover_changespec_source_files(projects_root)?;
        let mut outcomes = Vec::with_capacity(sources.len());
        for source_file in sources {
            let bytes = fs::read(&source_file.source_path)?;
            let request = source_file_observed_event(
                source.clone(),
                host_id.clone(),
                source_file.project_id.clone(),
                source_file.source_path.clone(),
                &bytes,
                source_file.is_archive,
                Some(source_event_idempotency_key(
                    &SourceChangeOperationWire::Upsert,
                    &source_identity_for_changespec_source(&source_file),
                )),
            );
            outcomes.push(self.append_projected_event(request)?);
        }
        Ok(outcomes)
    }

    pub fn diff_changespec_projection(
        &self,
        projects_root: &Path,
    ) -> Result<ShadowDiffReportWire, ProjectionError> {
        diff_changespec_projection(self, projects_root)
    }
}

pub fn discover_changespec_source_files(
    projects_root: &Path,
) -> Result<Vec<ChangeSpecSourceFileWire>, ProjectionError> {
    let mut files = Vec::new();
    if !projects_root.exists() {
        return Ok(files);
    }
    for entry in fs::read_dir(projects_root)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let project_id = entry.file_name().to_string_lossy().to_string();
        let project_dir = entry.path();
        for candidate in [
            format!("{project_id}.sase"),
            format!("{project_id}-archive.sase"),
            format!("{project_id}.gp"),
            format!("{project_id}-archive.gp"),
        ] {
            let path = project_dir.join(candidate);
            if path.exists() {
                if let Some(source_file) =
                    changespec_source_file_from_path(&path)?
                {
                    files.push(source_file);
                }
            }
        }
    }
    files.sort_by(|left, right| {
        left.project_id
            .cmp(&right.project_id)
            .then(left.source_path.cmp(&right.source_path))
    });
    Ok(files)
}

pub fn changespec_source_file_from_path(
    path: &Path,
) -> Result<Option<ChangeSpecSourceFileWire>, ProjectionError> {
    let Some(file_name) = path.file_name().and_then(|value| value.to_str())
    else {
        return Ok(None);
    };
    let is_legacy = file_name.ends_with(".gp");
    if !file_name.ends_with(".sase") && !is_legacy {
        return Ok(None);
    }
    let Some(project_id) = path
        .parent()
        .and_then(Path::file_name)
        .and_then(|value| value.to_str())
        .map(str::to_string)
    else {
        return Ok(None);
    };
    if file_name != format!("{project_id}.sase")
        && file_name != format!("{project_id}-archive.sase")
        && file_name != format!("{project_id}.gp")
        && file_name != format!("{project_id}-archive.gp")
    {
        return Ok(None);
    }
    Ok(Some(ChangeSpecSourceFileWire {
        schema_version: INDEXING_WIRE_SCHEMA_VERSION,
        project_id,
        source_path: path.to_string_lossy().to_string(),
        is_archive: is_archive_project_spec(&path.to_string_lossy()),
        is_legacy,
        fingerprint: changespec_source_fingerprint(path),
    }))
}

pub fn changespec_source_change_from_path(
    path: &Path,
    operation: SourceChangeOperationWire,
    reason: Option<String>,
) -> Option<SourceChangeWire> {
    let source_file = changespec_source_file_from_path(path).ok().flatten()?;
    Some(SourceChangeWire {
        schema_version: INDEXING_WIRE_SCHEMA_VERSION,
        identity: source_identity_for_changespec_source(&source_file),
        operation,
        reason,
    })
}

pub fn changespec_event_request_for_source_change(
    change: &SourceChangeWire,
    source: EventSourceWire,
    host_id: impl Into<String>,
) -> Result<Option<EventAppendRequestWire>, ProjectionError> {
    let Some(source_file) = changespec_source_file_from_path(Path::new(
        &change.identity.source_path,
    ))?
    else {
        return Ok(None);
    };
    let idempotency_key = Some(source_event_idempotency_key(
        &change.operation,
        &source_identity_for_changespec_source(&source_file),
    ));
    let host_id = host_id.into();
    if matches!(change.operation, SourceChangeOperationWire::Delete)
        || !Path::new(&source_file.source_path).exists()
    {
        return Ok(Some(source_file_reparsed_event(
            source,
            host_id,
            source_file.project_id,
            source_file.source_path,
            &[],
            source_file.is_archive,
            idempotency_key,
        )));
    }
    let bytes = fs::read(&source_file.source_path)?;
    Ok(Some(source_file_reparsed_event(
        source,
        host_id,
        source_file.project_id,
        source_file.source_path,
        &bytes,
        source_file.is_archive,
        idempotency_key,
    )))
}

pub fn source_identity_for_changespec_source(
    source_file: &ChangeSpecSourceFileWire,
) -> SourceIdentityWire {
    SourceIdentityWire {
        schema_version: INDEXING_WIRE_SCHEMA_VERSION,
        domain: CHANGESPEC_PROJECTION_NAME.to_string(),
        project_id: Some(source_file.project_id.clone()),
        source_path: source_file.source_path.clone(),
        is_archive: source_file.is_archive,
        fingerprint: Some(source_file.fingerprint.clone()),
        last_indexed_event_seq: None,
    }
}

fn diff_changespec_projection(
    db: &ProjectionDb,
    projects_root: &Path,
) -> Result<ShadowDiffReportWire, ProjectionError> {
    let mut records = Vec::new();
    let expected = expected_changespec_specs(projects_root, &mut records)?;
    let projected = projected_changespec_specs(db)?;
    let mut keys = BTreeSet::new();
    keys.extend(expected.keys().cloned());
    keys.extend(projected.keys().cloned());

    for key in keys {
        match (expected.get(&key), projected.get(&key)) {
            (Some(expected), None) => records.push(shadow_record(
                ShadowDiffCategoryWire::Missing,
                &expected.file_path,
                &key,
                format!(
                    "ChangeSpec '{}' is present in source but missing from projection",
                    expected.name
                ),
            )),
            (None, Some(projected)) => records.push(shadow_record(
                ShadowDiffCategoryWire::Extra,
                &projected.summary.source_path,
                &key,
                format!(
                    "ChangeSpec '{}' is projected but absent from source",
                    projected.summary.name
                ),
            )),
            (Some(expected), Some(projected)) => {
                let mismatches =
                    changespec_mismatches(expected, &projected.spec, &projected.summary);
                if !mismatches.is_empty() {
                    records.push(shadow_record(
                        ShadowDiffCategoryWire::Stale,
                        &expected.file_path,
                        &key,
                        format!(
                            "ChangeSpec '{}' differs: {}",
                            expected.name,
                            mismatches.join(", ")
                        ),
                    ));
                }
            }
            (None, None) => {}
        }
    }

    let counts = diff_counts(&records);
    Ok(ShadowDiffReportWire {
        schema_version: INDEXING_WIRE_SCHEMA_VERSION,
        domain: CHANGESPEC_PROJECTION_NAME.to_string(),
        records,
        counts,
    })
}

fn expected_changespec_specs(
    projects_root: &Path,
    records: &mut Vec<ShadowDiffRecordWire>,
) -> Result<BTreeMap<String, ChangeSpecWire>, ProjectionError> {
    let mut expected = BTreeMap::new();
    for source_file in discover_changespec_source_files(projects_root)? {
        let bytes = fs::read(&source_file.source_path)?;
        match parse_project_bytes(&source_file.source_path, &bytes) {
            Ok(specs) => {
                for spec in specs {
                    expected.insert(
                        changespec_handle(&source_file.project_id, &spec.name),
                        spec,
                    );
                }
            }
            Err(error) => records.push(ShadowDiffRecordWire {
                schema_version: INDEXING_WIRE_SCHEMA_VERSION,
                domain: CHANGESPEC_PROJECTION_NAME.to_string(),
                category: ShadowDiffCategoryWire::Corrupt,
                source_path: source_file.source_path,
                handle: None,
                message: format!("source parse error: {}", error.message),
            }),
        }
    }
    Ok(expected)
}

fn projected_changespec_specs(
    db: &ProjectionDb,
) -> Result<BTreeMap<String, ChangeSpecDetailWire>, ProjectionError> {
    let mut projected = BTreeMap::new();
    let mut offset = 0;
    loop {
        let page = db.list_changespecs(&ChangeSpecListRequestWire {
            schema_version: INDEXING_WIRE_SCHEMA_VERSION,
            limit: 200,
            offset,
            ..ChangeSpecListRequestWire::default()
        })?;
        for entry in page.entries {
            if let Some(detail) = db.fetch_changespec_detail(&entry.handle)? {
                projected.insert(entry.handle, detail);
            }
        }
        let Some(next_offset) = page.next_offset else {
            break;
        };
        offset = next_offset;
    }
    Ok(projected)
}

fn changespec_mismatches(
    expected: &ChangeSpecWire,
    projected: &ChangeSpecWire,
    summary: &ChangeSpecSummaryWire,
) -> Vec<&'static str> {
    let mut mismatches = Vec::new();
    if expected.project_basename != projected.project_basename {
        mismatches.push("project_basename");
    }
    if expected.file_path != projected.file_path
        || expected.file_path != summary.source_path
    {
        mismatches.push("source_path");
    }
    if expected.source_span != projected.source_span {
        mismatches.push("source_span");
    }
    if expected.status != projected.status {
        mismatches.push("status");
    }
    if expected.parent != projected.parent {
        mismatches.push("parent");
    }
    if expected.cl_or_pr != projected.cl_or_pr {
        mismatches.push("cl_or_pr");
    }
    if expected.bug != projected.bug {
        mismatches.push("bug");
    }
    if expected.description != projected.description {
        mismatches.push("description");
    }
    if expected.commits != projected.commits {
        mismatches.push("commits");
    }
    if expected.hooks != projected.hooks {
        mismatches.push("hooks");
    }
    if expected.comments != projected.comments {
        mismatches.push("comments");
    }
    if expected.mentors != projected.mentors {
        mismatches.push("mentors");
    }
    if expected.timestamps != projected.timestamps {
        mismatches.push("timestamps");
    }
    if expected.deltas != projected.deltas {
        mismatches.push("deltas");
    }
    mismatches
}

fn shadow_record(
    category: ShadowDiffCategoryWire,
    source_path: &str,
    handle: &str,
    message: String,
) -> ShadowDiffRecordWire {
    ShadowDiffRecordWire {
        schema_version: INDEXING_WIRE_SCHEMA_VERSION,
        domain: CHANGESPEC_PROJECTION_NAME.to_string(),
        category,
        source_path: source_path.to_string(),
        handle: Some(handle.to_string()),
        message,
    }
}

fn diff_counts(records: &[ShadowDiffRecordWire]) -> ShadowDiffCountsWire {
    let mut counts = ShadowDiffCountsWire::default();
    for record in records {
        match record.category {
            ShadowDiffCategoryWire::Missing => counts.missing += 1,
            ShadowDiffCategoryWire::Stale => counts.stale += 1,
            ShadowDiffCategoryWire::Extra => counts.extra += 1,
            ShadowDiffCategoryWire::Corrupt => counts.corrupt += 1,
        }
    }
    counts
}

fn changespec_source_fingerprint(path: &Path) -> SourceFingerprintWire {
    source_fingerprint_from_path(path, true).unwrap_or_else(|| {
        SourceFingerprintWire {
            schema_version: INDEXING_WIRE_SCHEMA_VERSION,
            content_sha256: Some("missing".to_string()),
            ..SourceFingerprintWire::default()
        }
    })
}
