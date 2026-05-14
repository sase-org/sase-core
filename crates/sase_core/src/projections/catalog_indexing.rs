use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use crate::{
    load_editor_xprompt_catalog, EditorXpromptCatalogRequestWire,
    MobileHelperStatusWire, MobileXpromptCatalogEntryWire,
    XpromptCatalogLoadOptions,
};

use super::catalogs::{
    catalog_config_source_observed_event_request,
    catalog_config_source_removed_event_request,
    catalog_file_history_removed_event_request,
    catalog_file_history_updated_event_request,
    catalog_memory_source_observed_event_request,
    catalog_memory_source_removed_event_request,
    catalog_xprompt_source_observed_event_request,
    catalog_xprompt_source_removed_event_request,
    CatalogProjectionEventContextWire, ConfigCatalogProjectionWire,
    FileHistoryProjectionWire, MemoryCatalogProjectionWire,
    XpromptCatalogProjectionWire, CATALOG_PROJECTION_NAME,
};
use super::error::ProjectionError;
use super::event::{EventAppendRequestWire, EventSourceWire};
use super::indexing::{
    ShadowDiffCategoryWire, ShadowDiffCountsWire, ShadowDiffRecordWire,
    ShadowDiffReportWire, INDEXING_WIRE_SCHEMA_VERSION,
};

const CATALOG_INDEXING_WIRE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogBackfillOptionsWire {
    #[serde(default = "catalog_indexing_schema_version")]
    pub schema_version: u32,
    pub project_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_dir: Option<String>,
    pub host_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogWatchedSourceWire {
    pub source_id: String,
    pub source_kind: String,
    pub path: String,
    pub watchable: bool,
    #[serde(default)]
    pub requires_resync_on_change: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CatalogBackfillPlanWire {
    #[serde(default = "catalog_indexing_schema_version")]
    pub schema_version: u32,
    pub events: Vec<EventAppendRequestWire>,
    pub watched_sources: Vec<CatalogWatchedSourceWire>,
    #[serde(default)]
    pub resync_required_sources: Vec<String>,
}

pub fn catalog_backfill_plan(
    conn: &Connection,
    options: CatalogBackfillOptionsWire,
) -> Result<CatalogBackfillPlanWire, ProjectionError> {
    let current = current_xprompt_catalog_entries(&options)?;
    let current_config = current_config_catalog_entries(&options)?;
    let current_memory = current_memory_catalog_entries(&options)?;
    let current_file_history = current_file_history_entries(&options)?;
    let projected = projected_xprompt_ids(conn, &options.project_id)?;
    let projected_config = projected_config_ids(conn, &options.project_id)?;
    let projected_memory = projected_memory_ids(conn, &options.project_id)?;
    let projected_file_history =
        projected_file_history_ids(conn, &options.project_id)?;
    let mut events = Vec::new();
    for row in current.values() {
        events.push(catalog_xprompt_source_observed_event_request(
            catalog_context(
                &options,
                row.source_path.clone(),
                Some(format!(
                    "catalog:{}:{}:{}",
                    options.project_id,
                    row.catalog_id,
                    row.content_hash.as_deref().unwrap_or("unknown")
                )),
            ),
            row.clone(),
        )?);
    }
    for catalog_id in projected.keys() {
        if !current.contains_key(catalog_id) {
            events.push(catalog_xprompt_source_removed_event_request(
                catalog_context(
                    &options,
                    None,
                    Some(format!(
                        "catalog:{}:{}:removed",
                        options.project_id, catalog_id
                    )),
                ),
                catalog_id.clone(),
            )?);
        }
    }
    for row in current_config.values() {
        events.push(catalog_config_source_observed_event_request(
            catalog_context(
                &options,
                Some(row.path.clone()),
                Some(format!(
                    "catalog-config:{}:{}:{}",
                    options.project_id,
                    row.config_id,
                    row.metadata
                        .get("content_hash")
                        .map(String::as_str)
                        .unwrap_or("unknown")
                )),
            ),
            row.clone(),
        )?);
    }
    for config_id in projected_config.keys() {
        if !current_config.contains_key(config_id) {
            events.push(catalog_config_source_removed_event_request(
                catalog_context(
                    &options,
                    None,
                    Some(format!(
                        "catalog-config:{}:{}:removed",
                        options.project_id, config_id
                    )),
                ),
                config_id.clone(),
            )?);
        }
    }
    for row in current_memory.values() {
        events.push(catalog_memory_source_observed_event_request(
            catalog_context(
                &options,
                Some(row.path.clone()),
                Some(format!(
                    "catalog-memory:{}:{}:{}",
                    options.project_id,
                    row.memory_id,
                    row.metadata
                        .get("content_hash")
                        .map(String::as_str)
                        .unwrap_or("unknown")
                )),
            ),
            row.clone(),
        )?);
    }
    for memory_id in projected_memory.keys() {
        if !current_memory.contains_key(memory_id) {
            events.push(catalog_memory_source_removed_event_request(
                catalog_context(
                    &options,
                    None,
                    Some(format!(
                        "catalog-memory:{}:{}:removed",
                        options.project_id, memory_id
                    )),
                ),
                memory_id.clone(),
            )?);
        }
    }
    for row in current_file_history.values() {
        events.push(catalog_file_history_updated_event_request(
            catalog_context(
                &options,
                Some(row.path.clone()),
                Some(format!(
                    "catalog-file-history:{}:{}:{}",
                    options.project_id,
                    file_history_id(row),
                    row.metadata
                        .get("content_hash")
                        .map(String::as_str)
                        .unwrap_or("unknown")
                )),
            ),
            row.clone(),
        )?);
    }
    for file_id in projected_file_history.keys() {
        if !current_file_history.contains_key(file_id) {
            let (path, branch_or_workspace) = split_file_history_id(file_id);
            events.push(catalog_file_history_removed_event_request(
                catalog_context(
                    &options,
                    Some(path.clone()),
                    Some(format!(
                        "catalog-file-history:{}:{}:removed",
                        options.project_id, file_id
                    )),
                ),
                path,
                branch_or_workspace,
            )?);
        }
    }

    let watched_sources = watched_catalog_sources(&options);
    let resync_required_sources = watched_sources
        .iter()
        .filter(|source| source.requires_resync_on_change || !source.watchable)
        .map(|source| source.source_id.clone())
        .collect();
    Ok(CatalogBackfillPlanWire {
        schema_version: CATALOG_INDEXING_WIRE_SCHEMA_VERSION,
        events,
        watched_sources,
        resync_required_sources,
    })
}

pub fn catalog_shadow_diff(
    conn: &Connection,
    options: CatalogBackfillOptionsWire,
) -> Result<ShadowDiffReportWire, ProjectionError> {
    let current = current_xprompt_catalog_entries(&options)?;
    let current_config = current_config_catalog_entries(&options)?;
    let current_memory = current_memory_catalog_entries(&options)?;
    let current_file_history = current_file_history_entries(&options)?;
    let projected = projected_xprompt_ids(conn, &options.project_id)?;
    let projected_config = projected_config_ids(conn, &options.project_id)?;
    let projected_memory = projected_memory_ids(conn, &options.project_id)?;
    let projected_file_history =
        projected_file_history_ids(conn, &options.project_id)?;
    let mut records = Vec::new();
    for (catalog_id, expected) in &current {
        match projected.get(catalog_id) {
            None => records.push(catalog_diff_record(
                ShadowDiffCategoryWire::Missing,
                expected
                    .source_path
                    .clone()
                    .unwrap_or_else(|| catalog_id.clone()),
                Some(catalog_id.clone()),
                "missing projected xprompt catalog row",
            )),
            Some(actual_hash)
                if expected.content_hash.as_ref() != Some(actual_hash) =>
            {
                records.push(catalog_diff_record(
                    ShadowDiffCategoryWire::Stale,
                    expected
                        .source_path
                        .clone()
                        .unwrap_or_else(|| catalog_id.clone()),
                    Some(catalog_id.clone()),
                    "stale projected xprompt catalog row",
                ));
            }
            Some(_) => {}
        }
    }
    for catalog_id in projected.keys() {
        if !current.contains_key(catalog_id) {
            records.push(catalog_diff_record(
                ShadowDiffCategoryWire::Extra,
                catalog_id.clone(),
                Some(catalog_id.clone()),
                "extra projected xprompt catalog row",
            ));
        }
    }
    diff_catalog_hashes(
        &mut records,
        "config catalog",
        current_config.iter().map(|(id, row)| {
            (
                id.as_str(),
                row.path.as_str(),
                row.metadata.get("content_hash").map(String::as_str),
            )
        }),
        &projected_config,
    );
    diff_catalog_hashes(
        &mut records,
        "memory catalog",
        current_memory.iter().map(|(id, row)| {
            (
                id.as_str(),
                row.path.as_str(),
                row.metadata.get("content_hash").map(String::as_str),
            )
        }),
        &projected_memory,
    );
    diff_catalog_hashes(
        &mut records,
        "file-history catalog",
        current_file_history.iter().map(|(id, row)| {
            (
                id.as_str(),
                row.path.as_str(),
                row.metadata.get("content_hash").map(String::as_str),
            )
        }),
        &projected_file_history,
    );
    records.sort_by(|a, b| {
        (diff_sort_key(&a.category), &a.source_path, &a.handle).cmp(&(
            diff_sort_key(&b.category),
            &b.source_path,
            &b.handle,
        ))
    });
    Ok(ShadowDiffReportWire {
        schema_version: INDEXING_WIRE_SCHEMA_VERSION,
        domain: CATALOG_PROJECTION_NAME.to_string(),
        counts: catalog_diff_counts(&records),
        records,
    })
}

fn current_xprompt_catalog_entries(
    options: &CatalogBackfillOptionsWire,
) -> Result<BTreeMap<String, XpromptCatalogProjectionWire>, ProjectionError> {
    let load_options = XpromptCatalogLoadOptions::new(
        options.root_dir.as_ref().map(PathBuf::from),
    );
    let response = load_editor_xprompt_catalog(
        &EditorXpromptCatalogRequestWire {
            schema_version: CATALOG_INDEXING_WIRE_SCHEMA_VERSION,
            project: options.project.clone(),
            source: None,
            tag: None,
            query: None,
            include_pdf: false,
            limit: None,
            device_id: None,
        },
        &load_options,
    )
    .map_err(|error| {
        ProjectionError::Invariant(format!(
            "failed to load xprompt catalog: {error}"
        ))
    })?;
    if response.result.status != MobileHelperStatusWire::Success {
        return Err(ProjectionError::Invariant(format!(
            "xprompt catalog load failed: {}",
            response.result.message.unwrap_or_default()
        )));
    }
    let mut rows = BTreeMap::new();
    for entry in response.entries {
        let catalog_id = xprompt_catalog_id(&entry);
        let content_hash = xprompt_entry_hash(&entry)?;
        rows.insert(
            catalog_id.clone(),
            XpromptCatalogProjectionWire {
                schema_version: CATALOG_INDEXING_WIRE_SCHEMA_VERSION,
                catalog_id,
                source_path: entry
                    .definition_path
                    .clone()
                    .or_else(|| entry.source_path_display.clone()),
                content_hash: Some(content_hash),
                exists: true,
                updated_at: options.created_at.clone(),
                entry,
            },
        );
    }
    Ok(rows)
}

fn projected_xprompt_ids(
    conn: &Connection,
    project_id: &str,
) -> Result<BTreeMap<String, String>, ProjectionError> {
    let mut stmt = conn.prepare(
        "SELECT catalog_id, content_hash FROM xprompt_catalog WHERE project_id = ?1 AND is_present = 1",
    )?;
    let rows = stmt.query_map(params![project_id], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
    })?;
    let mut projected = BTreeMap::new();
    for row in rows {
        let (id, hash) = row?;
        projected.insert(id, hash.unwrap_or_default());
    }
    Ok(projected)
}

fn current_config_catalog_entries(
    options: &CatalogBackfillOptionsWire,
) -> Result<BTreeMap<String, ConfigCatalogProjectionWire>, ProjectionError> {
    let mut rows = BTreeMap::new();
    let Some(root_dir) = options.root_dir.as_ref().map(PathBuf::from) else {
        return Ok(rows);
    };
    for (scope, path) in [
        ("project", root_dir.join("sase.yml")),
        ("project", root_dir.join("sase.yaml")),
        ("local", root_dir.join(".sase").join("sase.yml")),
    ] {
        if !path.is_file() {
            continue;
        }
        let bytes = fs::read(&path)?;
        let config_id = stable_path_id("config", &path);
        let mut metadata = BTreeMap::new();
        metadata.insert("content_hash".to_string(), sha256_hex(&bytes));
        rows.insert(
            config_id.clone(),
            ConfigCatalogProjectionWire {
                schema_version: CATALOG_INDEXING_WIRE_SCHEMA_VERSION,
                config_id,
                path: path.to_string_lossy().to_string(),
                scope: scope.to_string(),
                content_preview: Some(content_preview(&bytes)),
                exists: true,
                updated_at: options.created_at.clone(),
                metadata,
            },
        );
    }
    Ok(rows)
}

fn current_memory_catalog_entries(
    options: &CatalogBackfillOptionsWire,
) -> Result<BTreeMap<String, MemoryCatalogProjectionWire>, ProjectionError> {
    let mut rows = BTreeMap::new();
    let Some(root_dir) = options.root_dir.as_ref().map(PathBuf::from) else {
        return Ok(rows);
    };
    for (tier, dir) in [
        ("short", root_dir.join("memory").join("short")),
        ("long", root_dir.join("memory").join("long")),
        ("dynamic", root_dir.join(".sase").join("memory")),
    ] {
        for path in markdown_files(&dir)? {
            let bytes = fs::read(&path)?;
            let memory_id = stable_path_id("memory", &path);
            let mut metadata = BTreeMap::new();
            metadata.insert("content_hash".to_string(), sha256_hex(&bytes));
            rows.insert(
                memory_id.clone(),
                MemoryCatalogProjectionWire {
                    schema_version: CATALOG_INDEXING_WIRE_SCHEMA_VERSION,
                    memory_id,
                    path: path.to_string_lossy().to_string(),
                    tier: tier.to_string(),
                    source_name: path
                        .file_stem()
                        .and_then(|value| value.to_str())
                        .unwrap_or("memory")
                        .to_string(),
                    keywords: memory_keywords(&bytes),
                    content_preview: Some(content_preview(&bytes)),
                    exists: true,
                    updated_at: options.created_at.clone(),
                    metadata,
                },
            );
        }
    }
    Ok(rows)
}

fn current_file_history_entries(
    options: &CatalogBackfillOptionsWire,
) -> Result<BTreeMap<String, FileHistoryProjectionWire>, ProjectionError> {
    let mut rows = BTreeMap::new();
    let Some(root_dir) = options.root_dir.as_ref().map(PathBuf::from) else {
        return Ok(rows);
    };
    for path in [
        root_dir.join(".sase").join("file_reference_history.json"),
        root_dir.join("file_reference_history.json"),
    ] {
        if !path.is_file() {
            continue;
        }
        let bytes = fs::read(&path)?;
        let mut metadata = BTreeMap::new();
        metadata.insert("content_hash".to_string(), sha256_hex(&bytes));
        metadata.insert(
            "source_path".to_string(),
            path.to_string_lossy().to_string(),
        );
        let value: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(|error| {
                ProjectionError::Invariant(format!(
                    "failed to parse file-history store {}: {error}",
                    path.display()
                ))
            })?;
        for file in file_history_rows_from_json(&value, &metadata) {
            rows.insert(file_history_id(&file), file);
        }
    }
    Ok(rows)
}

fn projected_config_ids(
    conn: &Connection,
    project_id: &str,
) -> Result<BTreeMap<String, String>, ProjectionError> {
    projected_hashes(
        conn,
        "SELECT config_id, metadata_json FROM config_catalog WHERE project_id = ?1 AND is_present = 1",
        project_id,
    )
}

fn projected_memory_ids(
    conn: &Connection,
    project_id: &str,
) -> Result<BTreeMap<String, String>, ProjectionError> {
    projected_hashes(
        conn,
        "SELECT memory_id, metadata_json FROM memory_catalog WHERE project_id = ?1 AND is_present = 1",
        project_id,
    )
}

fn projected_file_history_ids(
    conn: &Connection,
    project_id: &str,
) -> Result<BTreeMap<String, String>, ProjectionError> {
    projected_hashes(
        conn,
        "SELECT path || char(0) || branch_or_workspace, metadata_json FROM file_history WHERE project_id = ?1 AND is_present = 1",
        project_id,
    )
}

fn projected_hashes(
    conn: &Connection,
    sql: &str,
    project_id: &str,
) -> Result<BTreeMap<String, String>, ProjectionError> {
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt.query_map(params![project_id], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut projected = BTreeMap::new();
    for row in rows {
        let (id, metadata_json) = row?;
        let metadata: BTreeMap<String, String> =
            serde_json::from_str(&metadata_json)?;
        projected.insert(
            id,
            metadata.get("content_hash").cloned().unwrap_or_default(),
        );
    }
    Ok(projected)
}

fn watched_catalog_sources(
    options: &CatalogBackfillOptionsWire,
) -> Vec<CatalogWatchedSourceWire> {
    let Some(root_dir) = options.root_dir.as_ref() else {
        return Vec::new();
    };
    [
        "xprompts",
        ".xprompts",
        "workflows",
        "sase.yml",
        "sase.yaml",
        "memory/short",
        "memory/long",
        ".sase/memory",
        ".sase/file_reference_history.json",
    ]
    .into_iter()
    .map(|relative| {
        let path = PathBuf::from(root_dir).join(relative);
        CatalogWatchedSourceWire {
            source_id: format!("catalog:{relative}"),
            source_kind: if relative.starts_with("sase.") {
                "project_config"
            } else if relative.contains("memory") {
                "memory_dir"
            } else if relative.contains("file_reference_history") {
                "file_history"
            } else if relative == "workflows" {
                "workflow_dir"
            } else {
                "xprompt_dir"
            }
            .to_string(),
            path: path.to_string_lossy().into_owned(),
            watchable: path.exists(),
            requires_resync_on_change: false,
        }
    })
    .collect()
}

fn catalog_context(
    options: &CatalogBackfillOptionsWire,
    source_path: Option<String>,
    idempotency_key: Option<String>,
) -> CatalogProjectionEventContextWire {
    CatalogProjectionEventContextWire {
        schema_version: CATALOG_INDEXING_WIRE_SCHEMA_VERSION,
        created_at: options.created_at.clone(),
        source: EventSourceWire {
            source_type: "shadow_indexer".to_string(),
            name: "catalog_backfill".to_string(),
            ..EventSourceWire::default()
        },
        host_id: options.host_id.clone(),
        project_id: options.project_id.clone(),
        idempotency_key,
        causality: Vec::new(),
        source_path,
        source_revision: None,
    }
}

fn diff_catalog_hashes<'a>(
    records: &mut Vec<ShadowDiffRecordWire>,
    label: &str,
    current: impl Iterator<Item = (&'a str, &'a str, Option<&'a str>)>,
    projected: &BTreeMap<String, String>,
) {
    let current = current
        .map(|(id, path, hash)| {
            (
                id.to_string(),
                (path.to_string(), hash.unwrap_or("").to_string()),
            )
        })
        .collect::<BTreeMap<_, _>>();
    for (id, (path, expected_hash)) in &current {
        match projected.get(id) {
            None => records.push(catalog_diff_record(
                ShadowDiffCategoryWire::Missing,
                path.clone(),
                Some(id.clone()),
                &format!("missing projected {label} row"),
            )),
            Some(actual_hash) if actual_hash != expected_hash => {
                records.push(catalog_diff_record(
                    ShadowDiffCategoryWire::Stale,
                    path.clone(),
                    Some(id.clone()),
                    &format!("stale projected {label} row"),
                ));
            }
            Some(_) => {}
        }
    }
    for id in projected.keys() {
        if !current.contains_key(id) {
            records.push(catalog_diff_record(
                ShadowDiffCategoryWire::Extra,
                id.clone(),
                Some(id.clone()),
                &format!("extra projected {label} row"),
            ));
        }
    }
}

fn markdown_files(dir: &Path) -> Result<Vec<PathBuf>, ProjectionError> {
    let mut files = Vec::new();
    if !dir.exists() {
        return Ok(files);
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            files.extend(markdown_files(&path)?);
        } else if matches!(
            path.extension().and_then(|value| value.to_str()),
            Some("md" | "markdown")
        ) {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

fn file_history_rows_from_json(
    value: &serde_json::Value,
    metadata: &BTreeMap<String, String>,
) -> Vec<FileHistoryProjectionWire> {
    let mut rows = Vec::new();
    match value {
        serde_json::Value::Array(items) => {
            for item in items {
                if let Some(row) = file_history_row_from_json(item, metadata) {
                    rows.push(row);
                }
            }
        }
        serde_json::Value::Object(map) => {
            for (path, item) in map {
                let mut row = file_history_row_from_json(item, metadata)
                    .unwrap_or_else(|| FileHistoryProjectionWire {
                        schema_version: CATALOG_INDEXING_WIRE_SCHEMA_VERSION,
                        path: path.clone(),
                        branch_or_workspace: None,
                        workspace_name: None,
                        last_used_at: None,
                        use_count: 1,
                        exists: true,
                        metadata: metadata.clone(),
                    });
                if row.path.is_empty() {
                    row.path = path.clone();
                }
                rows.push(row);
            }
        }
        _ => {}
    }
    rows
}

fn file_history_row_from_json(
    item: &serde_json::Value,
    metadata: &BTreeMap<String, String>,
) -> Option<FileHistoryProjectionWire> {
    let object = item.as_object()?;
    let path = object
        .get("path")
        .or_else(|| object.get("file"))
        .or_else(|| object.get("value"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_string();
    if path.is_empty() {
        return None;
    }
    Some(FileHistoryProjectionWire {
        schema_version: CATALOG_INDEXING_WIRE_SCHEMA_VERSION,
        path,
        branch_or_workspace: object
            .get("branch_or_workspace")
            .or_else(|| object.get("branch"))
            .or_else(|| object.get("workspace"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
        workspace_name: object
            .get("workspace_name")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
        last_used_at: object
            .get("last_used_at")
            .or_else(|| object.get("timestamp"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
        use_count: object
            .get("use_count")
            .or_else(|| object.get("count"))
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(1),
        exists: true,
        metadata: metadata.clone(),
    })
}

fn memory_keywords(bytes: &[u8]) -> Vec<String> {
    let text = String::from_utf8_lossy(bytes);
    text.lines()
        .find_map(|line| {
            let rest = line.trim().strip_prefix("keywords:")?.trim();
            Some(
                rest.trim_matches(['[', ']'])
                    .split(',')
                    .map(|item| {
                        item.trim().trim_matches('"').trim_matches('\'')
                    })
                    .filter(|item| !item.is_empty())
                    .map(str::to_string)
                    .collect::<Vec<_>>(),
            )
        })
        .unwrap_or_default()
}

fn content_preview(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes)
        .chars()
        .take(500)
        .collect::<String>()
}

fn stable_path_id(prefix: &str, path: &Path) -> String {
    format!(
        "{prefix}:{}",
        hex::encode(Sha256::digest(path.to_string_lossy().as_bytes()))
    )
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn file_history_id(row: &FileHistoryProjectionWire) -> String {
    format!(
        "{}\0{}",
        row.path,
        row.branch_or_workspace.as_deref().unwrap_or("")
    )
}

fn split_file_history_id(id: &str) -> (String, Option<String>) {
    let mut parts = id.splitn(2, '\0');
    let path = parts.next().unwrap_or_default().to_string();
    let branch = parts
        .next()
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    (path, branch)
}

fn xprompt_catalog_id(entry: &MobileXpromptCatalogEntryWire) -> String {
    let source = entry
        .definition_path
        .as_deref()
        .or(entry.source_path_display.as_deref())
        .unwrap_or("");
    hex::encode(Sha256::digest(
        format!(
            "xprompt\0{}\0{}\0{}\0{}",
            entry.source_bucket,
            entry.project.as_deref().unwrap_or(""),
            entry.name,
            source
        )
        .as_bytes(),
    ))
}

fn xprompt_entry_hash(
    entry: &MobileXpromptCatalogEntryWire,
) -> Result<String, ProjectionError> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(entry)?)))
}

fn catalog_diff_record(
    category: ShadowDiffCategoryWire,
    source_path: String,
    handle: Option<String>,
    message: &str,
) -> ShadowDiffRecordWire {
    ShadowDiffRecordWire {
        schema_version: INDEXING_WIRE_SCHEMA_VERSION,
        domain: CATALOG_PROJECTION_NAME.to_string(),
        category,
        source_path,
        handle,
        message: message.to_string(),
    }
}

fn catalog_diff_counts(
    records: &[ShadowDiffRecordWire],
) -> ShadowDiffCountsWire {
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

fn diff_sort_key(category: &ShadowDiffCategoryWire) -> u8 {
    match category {
        ShadowDiffCategoryWire::Missing => 0,
        ShadowDiffCategoryWire::Stale => 1,
        ShadowDiffCategoryWire::Extra => 2,
        ShadowDiffCategoryWire::Corrupt => 3,
    }
}

fn catalog_indexing_schema_version() -> u32 {
    CATALOG_INDEXING_WIRE_SCHEMA_VERSION
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::projections::{
        catalog_projection_config_sources,
        catalog_projection_file_history_rows,
        catalog_projection_memory_sources, ProjectionDb,
    };
    use tempfile::tempdir;

    #[test]
    fn backfill_and_diff_cover_config_memory_and_file_history() {
        let dir = tempdir().unwrap();
        fs::create_dir_all(dir.path().join("xprompts")).unwrap();
        fs::create_dir_all(dir.path().join("memory").join("short")).unwrap();
        fs::create_dir_all(dir.path().join(".sase")).unwrap();
        fs::write(
            dir.path().join("xprompts").join("review.md"),
            "---\ndescription: Review code\n---\nReview this.",
        )
        .unwrap();
        fs::write(
            dir.path().join("sase.yml"),
            "xprompts:\n  cfg:\n    content: From config\n",
        )
        .unwrap();
        fs::write(
            dir.path().join("memory").join("short").join("build.md"),
            "---\nkeywords: [build]\n---\nRun just check.",
        )
        .unwrap();
        fs::write(
            dir.path().join(".sase").join("file_reference_history.json"),
            r#"[{"path":"src/lib.rs","branch_or_workspace":"main","use_count":3}]"#,
        )
        .unwrap();

        let mut db = ProjectionDb::open_in_memory().unwrap();
        let options = CatalogBackfillOptionsWire {
            schema_version: CATALOG_INDEXING_WIRE_SCHEMA_VERSION,
            project_id: "project-a".to_string(),
            project: Some("project-a".to_string()),
            root_dir: Some(dir.path().to_string_lossy().to_string()),
            host_id: "host-a".to_string(),
            created_at: None,
        };
        let plan =
            catalog_backfill_plan(db.connection(), options.clone()).unwrap();
        assert!(plan
            .watched_sources
            .iter()
            .any(|source| source.source_kind == "file_history"));
        assert!(plan.events.len() >= 4);
        for event in plan.events {
            db.append_projected_event(event).unwrap();
        }

        assert_eq!(
            catalog_shadow_diff(db.connection(), options.clone())
                .unwrap()
                .counts,
            ShadowDiffCountsWire::default()
        );
        assert_eq!(
            catalog_projection_config_sources(
                db.connection(),
                "project-a",
                false
            )
            .unwrap()
            .len(),
            1
        );
        assert_eq!(
            catalog_projection_memory_sources(
                db.connection(),
                "project-a",
                false
            )
            .unwrap()[0]
                .keywords,
            vec!["build".to_string()]
        );
        assert_eq!(
            catalog_projection_file_history_rows(
                db.connection(),
                "project-a",
                Some("main"),
                false
            )
            .unwrap()[0]
                .path,
            "src/lib.rs"
        );

        fs::write(dir.path().join("sase.yml"), "xprompts: {}\n").unwrap();
        let diff = catalog_shadow_diff(db.connection(), options).unwrap();
        assert_eq!(diff.counts.stale, 1);
    }
}
