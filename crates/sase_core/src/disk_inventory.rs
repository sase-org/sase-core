//! Shared disk-footprint inventory classification.
//!
//! Python remains responsible for host-specific observation: resolving
//! configured roots, probing workspace registries, and measuring filesystem
//! usage. This module owns the frontend-neutral classification that every
//! caller needs to agree on: coverage status, overlap relationships, and the
//! physical aggregate that avoids double-counting nested roots.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path};
use thiserror::Error;

pub const DISK_INVENTORY_WIRE_SCHEMA_VERSION: u32 = 1;

pub const DISK_INVENTORY_COVERAGE_COMPLETE: &str = "complete";
pub const DISK_INVENTORY_COVERAGE_PARTIAL: &str = "partial";
pub const DISK_INVENTORY_COVERAGE_UNRESOLVED: &str = "unresolved";

#[derive(Debug, Error)]
pub enum DiskInventoryError {
    #[error("disk inventory requires schema_version {expected}, got {actual}")]
    SchemaVersion { expected: u32, actual: u32 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskInventoryRequestWire {
    pub schema_version: u32,
    pub rows: Vec<DiskInventoryInputRowWire>,
    #[serde(default)]
    pub scan_diagnostics: Vec<String>,
    #[serde(default)]
    pub stray_scan_visited: u64,
    #[serde(default)]
    pub stray_scan_truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskInventoryInputRowWire {
    pub section: String,
    pub name: String,
    pub path: String,
    #[serde(default)]
    pub physical_path: Option<String>,
    pub size_bytes: u64,
    pub owner: String,
    pub horizon: String,
    #[serde(default = "default_owned_status")]
    pub status: String,
    #[serde(default)]
    pub reclaim: Option<String>,
    #[serde(default = "default_complete_coverage")]
    pub coverage: String,
    #[serde(default)]
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DiskInventoryResultWire {
    pub schema_version: u32,
    pub rows: Vec<DiskInventoryRowWire>,
    pub total_bytes: u64,
    pub logical_total_bytes: u64,
    pub owned_total_bytes: u64,
    pub unowned_total_bytes: u64,
    pub coverage_status: String,
    pub scan_diagnostics: Vec<String>,
    pub unresolved_owner_coverage: Vec<String>,
    pub stray_scan_visited: u64,
    pub stray_scan_truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DiskInventoryRowWire {
    pub section: String,
    pub name: String,
    pub path: String,
    pub physical_path: Option<String>,
    pub size_bytes: u64,
    pub exclusive_size_bytes: u64,
    pub owner: String,
    pub horizon: String,
    pub status: String,
    pub reclaim: Option<String>,
    pub coverage: String,
    pub diagnostics: Vec<String>,
    pub overlap_parent_path: Option<String>,
    pub overlap_paths: Vec<String>,
}

#[derive(Debug, Clone)]
struct ClassifiedRow {
    input: DiskInventoryInputRowWire,
    key: String,
    exclusive_size_bytes: u64,
    parent: Option<usize>,
    duplicate: bool,
    overlap_paths: Vec<String>,
    diagnostics: Vec<String>,
}

fn default_owned_status() -> String {
    "owned".to_string()
}

fn default_complete_coverage() -> String {
    DISK_INVENTORY_COVERAGE_COMPLETE.to_string()
}

pub fn classify_disk_inventory(
    request: &DiskInventoryRequestWire,
) -> Result<DiskInventoryResultWire, DiskInventoryError> {
    if request.schema_version != DISK_INVENTORY_WIRE_SCHEMA_VERSION {
        return Err(DiskInventoryError::SchemaVersion {
            expected: DISK_INVENTORY_WIRE_SCHEMA_VERSION,
            actual: request.schema_version,
        });
    }

    let mut rows = request
        .rows
        .iter()
        .cloned()
        .map(|input| {
            let key = inventory_key(&input);
            ClassifiedRow {
                input,
                key,
                exclusive_size_bytes: 0,
                parent: None,
                duplicate: false,
                overlap_paths: Vec::new(),
                diagnostics: Vec::new(),
            }
        })
        .collect::<Vec<_>>();

    classify_overlap_parents(&mut rows);
    calculate_exclusive_sizes(&mut rows);

    let mut diagnostics = request.scan_diagnostics.clone();
    let mut unresolved = Vec::new();
    let mut coverage_status = DISK_INVENTORY_COVERAGE_COMPLETE;
    if request.stray_scan_truncated || !request.scan_diagnostics.is_empty() {
        coverage_status = DISK_INVENTORY_COVERAGE_PARTIAL;
    }

    for row in &rows {
        for diagnostic in &row.input.diagnostics {
            diagnostics.push(format!("{}: {diagnostic}", row_label(row)));
        }
        for diagnostic in &row.diagnostics {
            diagnostics.push(format!("{}: {diagnostic}", row_label(row)));
        }
        if row.input.coverage != DISK_INVENTORY_COVERAGE_COMPLETE
            || !row.input.diagnostics.is_empty()
            || !row.diagnostics.is_empty()
        {
            coverage_status = DISK_INVENTORY_COVERAGE_PARTIAL;
        }
        if row.input.coverage == DISK_INVENTORY_COVERAGE_UNRESOLVED {
            coverage_status = DISK_INVENTORY_COVERAGE_PARTIAL;
            unresolved.push(row_label(row));
        }
    }

    let mut total = 0_u64;
    let mut logical_total = 0_u64;
    let mut owned_total = 0_u64;
    let mut unowned_total = 0_u64;
    for row in &rows {
        logical_total = logical_total.saturating_add(row.input.size_bytes);
        total = total.saturating_add(row.exclusive_size_bytes);
        if row.input.status == "unowned" {
            unowned_total =
                unowned_total.saturating_add(row.exclusive_size_bytes);
        } else {
            owned_total = owned_total.saturating_add(row.exclusive_size_bytes);
        }
    }

    let output_rows = rows
        .iter()
        .map(|row| DiskInventoryRowWire {
            section: row.input.section.clone(),
            name: row.input.name.clone(),
            path: row.input.path.clone(),
            physical_path: row.input.physical_path.clone(),
            size_bytes: row.input.size_bytes,
            exclusive_size_bytes: row.exclusive_size_bytes,
            owner: row.input.owner.clone(),
            horizon: row.input.horizon.clone(),
            status: row.input.status.clone(),
            reclaim: row.input.reclaim.clone(),
            coverage: row.input.coverage.clone(),
            diagnostics: merged_row_diagnostics(row),
            overlap_parent_path: row
                .parent
                .and_then(|index| rows.get(index))
                .map(|parent| parent.input.path.clone()),
            overlap_paths: row.overlap_paths.clone(),
        })
        .collect();

    Ok(DiskInventoryResultWire {
        schema_version: DISK_INVENTORY_WIRE_SCHEMA_VERSION,
        rows: output_rows,
        total_bytes: total,
        logical_total_bytes: logical_total,
        owned_total_bytes: owned_total,
        unowned_total_bytes: unowned_total,
        coverage_status: coverage_status.to_string(),
        scan_diagnostics: dedupe(diagnostics),
        unresolved_owner_coverage: dedupe(unresolved),
        stray_scan_visited: request.stray_scan_visited,
        stray_scan_truncated: request.stray_scan_truncated,
    })
}

fn classify_overlap_parents(rows: &mut [ClassifiedRow]) {
    let mut first_by_key: BTreeMap<String, usize> = BTreeMap::new();
    for index in 0..rows.len() {
        if rows[index].key.is_empty() {
            continue;
        }
        if let Some(parent) = first_by_key.get(&rows[index].key).copied() {
            rows[index].parent = Some(parent);
            rows[index].duplicate = true;
            rows[index].diagnostics.push(format!(
                "duplicates inventory root {}; counted once in aggregate",
                rows[parent].input.path
            ));
        } else {
            first_by_key.insert(rows[index].key.clone(), index);
        }
    }

    for index in 0..rows.len() {
        if rows[index].key.is_empty() || rows[index].parent.is_some() {
            continue;
        }
        rows[index].parent = nearest_ancestor(index, rows);
    }

    for index in 0..rows.len() {
        if let Some(parent) = rows[index].parent {
            let path = rows[index].input.path.clone();
            rows[parent].overlap_paths.push(path);
        }
    }
}

fn calculate_exclusive_sizes(rows: &mut [ClassifiedRow]) {
    let mut direct_child_sizes = vec![0_u64; rows.len()];
    let keys = rows.iter().map(|row| row.key.clone()).collect::<Vec<_>>();
    let sizes = rows
        .iter()
        .map(|row| row.input.size_bytes)
        .collect::<Vec<_>>();

    for (index, row) in rows.iter_mut().enumerate() {
        if row.key.is_empty() {
            row.exclusive_size_bytes = row.input.size_bytes;
            continue;
        }
        if let Some(parent) = row.parent {
            if keys[index] == keys[parent] {
                row.exclusive_size_bytes = 0;
                continue;
            }
            direct_child_sizes[parent] =
                direct_child_sizes[parent].saturating_add(sizes[index]);
        }
        row.exclusive_size_bytes = row.input.size_bytes;
    }

    for index in 0..rows.len() {
        if rows[index].duplicate {
            rows[index].exclusive_size_bytes = 0;
            continue;
        }
        let child_size = direct_child_sizes[index];
        rows[index].exclusive_size_bytes =
            rows[index].input.size_bytes.saturating_sub(child_size);
        if child_size > rows[index].input.size_bytes {
            rows[index].diagnostics.push(format!(
                "overlapping child rows report {} bytes inside {} bytes; aggregate was saturated",
                child_size, rows[index].input.size_bytes
            ));
        }
    }
}

fn nearest_ancestor(index: usize, rows: &[ClassifiedRow]) -> Option<usize> {
    rows.iter()
        .enumerate()
        .filter_map(|(candidate_index, candidate)| {
            if candidate_index == index
                || candidate.key.is_empty()
                || candidate.duplicate
            {
                return None;
            }
            if is_strict_descendant(&rows[index].key, &candidate.key) {
                Some((candidate_index, candidate.key.len()))
            } else {
                None
            }
        })
        .max_by_key(|(_candidate_index, len)| *len)
        .map(|(candidate_index, _len)| candidate_index)
}

fn inventory_key(row: &DiskInventoryInputRowWire) -> String {
    let raw = row.physical_path.as_deref().unwrap_or(row.path.as_str());
    if raw.trim().is_empty() {
        String::new()
    } else {
        clean_path(raw)
    }
}

fn clean_path(value: &str) -> String {
    let path = Path::new(value);
    let mut absolute = false;
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => {
                parts.push(prefix.as_os_str().to_string_lossy().into_owned());
            }
            Component::RootDir => absolute = true,
            Component::CurDir => {}
            Component::ParentDir => {
                if let Some(last) = parts.last() {
                    if last != ".." {
                        parts.pop();
                        continue;
                    }
                }
                if !absolute {
                    parts.push("..".to_string());
                }
            }
            Component::Normal(part) => {
                parts.push(part.to_string_lossy().into_owned());
            }
        }
    }
    if absolute {
        if parts.is_empty() {
            "/".to_string()
        } else {
            format!("/{}", parts.join("/"))
        }
    } else if parts.is_empty() {
        ".".to_string()
    } else {
        parts.join("/")
    }
}

fn is_strict_descendant(path: &str, base: &str) -> bool {
    path != base && is_relative_to(path, base)
}

fn is_relative_to(path: &str, base: &str) -> bool {
    if base == "/" {
        return path.starts_with('/');
    }
    path == base
        || path
            .strip_prefix(base)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn row_label(row: &ClassifiedRow) -> String {
    if row.input.path.is_empty() {
        format!("{}/{}", row.input.section, row.input.name)
    } else {
        format!("{} {}", row.input.owner, row.input.path)
    }
}

fn merged_row_diagnostics(row: &ClassifiedRow) -> Vec<String> {
    row.input
        .diagnostics
        .iter()
        .chain(row.diagnostics.iter())
        .cloned()
        .collect()
}

fn dedupe(values: Vec<String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut result = Vec::new();
    for value in values {
        if seen.insert(value.clone()) {
            result.push(value);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(
        path: &str,
        size_bytes: u64,
        owner: &str,
    ) -> DiskInventoryInputRowWire {
        DiskInventoryInputRowWire {
            section: "test".to_string(),
            name: path.to_string(),
            path: path.to_string(),
            physical_path: None,
            size_bytes,
            owner: owner.to_string(),
            horizon: "test".to_string(),
            status: "owned".to_string(),
            reclaim: None,
            coverage: DISK_INVENTORY_COVERAGE_COMPLETE.to_string(),
            diagnostics: Vec::new(),
        }
    }

    fn request(
        rows: Vec<DiskInventoryInputRowWire>,
    ) -> DiskInventoryRequestWire {
        DiskInventoryRequestWire {
            schema_version: DISK_INVENTORY_WIRE_SCHEMA_VERSION,
            rows,
            scan_diagnostics: Vec::new(),
            stray_scan_visited: 0,
            stray_scan_truncated: false,
        }
    }

    #[test]
    fn nested_roots_count_physical_bytes_once() {
        let result = classify_disk_inventory(&request(vec![
            row("/tmp/root", 100, "workspace"),
            row("/tmp/root/target", 30, "rust-target"),
            row("/tmp/root/target/incremental", 5, "incremental"),
        ]))
        .unwrap();

        assert_eq!(result.logical_total_bytes, 135);
        assert_eq!(result.total_bytes, 100);
        assert_eq!(result.rows[0].exclusive_size_bytes, 70);
        assert_eq!(result.rows[1].exclusive_size_bytes, 25);
        assert_eq!(result.rows[2].exclusive_size_bytes, 5);
        assert_eq!(
            result.rows[1].overlap_parent_path.as_deref(),
            Some("/tmp/root")
        );
    }

    #[test]
    fn duplicate_roots_are_reported_but_counted_once() {
        let result = classify_disk_inventory(&request(vec![
            row("/tmp/root", 100, "first"),
            row("/tmp/root", 100, "second"),
        ]))
        .unwrap();

        assert_eq!(result.logical_total_bytes, 200);
        assert_eq!(result.total_bytes, 100);
        assert_eq!(result.rows[0].exclusive_size_bytes, 100);
        assert_eq!(result.rows[1].exclusive_size_bytes, 0);
        assert_eq!(
            result.rows[1].overlap_parent_path.as_deref(),
            Some("/tmp/root")
        );
        assert!(result.coverage_status == DISK_INVENTORY_COVERAGE_PARTIAL);
        assert!(
            result.scan_diagnostics[0].contains("duplicates inventory root")
        );
    }

    #[test]
    fn unresolved_rows_make_coverage_partial() {
        let mut unresolved = row("", 0, "workspace");
        unresolved.coverage = DISK_INVENTORY_COVERAGE_UNRESOLVED.to_string();
        unresolved.diagnostics.push("registry failed".to_string());

        let result =
            classify_disk_inventory(&request(vec![unresolved])).unwrap();

        assert_eq!(result.coverage_status, DISK_INVENTORY_COVERAGE_PARTIAL);
        assert_eq!(result.unresolved_owner_coverage, vec!["test/".to_string()]);
        assert!(result.scan_diagnostics[0].contains("registry failed"));
    }
}
