//! Deterministic policy for agent-session manifest file-set compatibility.
//!
//! The host owns snapshots, manifests, filesystem reads, and publication writes.
//! This module only derives the canonical explicit file set from typed snapshot
//! facts and classifies one observed explicit list as current, supported legacy,
//! slim, or invalid.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const SESSION_MANIFEST_WIRE_SCHEMA_VERSION: u32 = 1;

pub const SESSION_MANIFEST_CLASS_CURRENT: &str = "current";
pub const SESSION_MANIFEST_CLASS_SUPPORTED_LEGACY: &str = "supported_legacy";
pub const SESSION_MANIFEST_CLASS_SLIM: &str = "slim";
pub const SESSION_MANIFEST_CLASS_INVALID: &str = "invalid";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionManifestContainerWire {
    pub kind: String,
    pub global_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionManifestSnapshotWire {
    pub owner_username: String,
    pub owner_machine: String,
    pub local_hood: String,
    #[serde(default)]
    pub run_global_names: Vec<String>,
    #[serde(default)]
    pub run_file_paths: Vec<String>,
    #[serde(default)]
    pub containers: Vec<SessionManifestContainerWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionManifestClassifyRequestWire {
    pub schema_version: u32,
    pub snapshot: SessionManifestSnapshotWire,
    #[serde(default)]
    pub explicit_files: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionManifestClassifyResponseWire {
    pub schema_version: u32,
    pub canonical_files: Vec<String>,
    pub legacy_files: Vec<String>,
    pub classification: String,
    pub reason: String,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SessionManifestError {
    #[error(
        "unsupported SessionManifestClassifyRequestWire schema_version {0}"
    )]
    UnsupportedSchema(u32),
    #[error("invalid session manifest snapshot: {0}")]
    InvalidSnapshot(String),
}

pub fn canonical_session_manifest_files(
    snapshot: &SessionManifestSnapshotWire,
) -> Result<(Vec<String>, Vec<String>), SessionManifestError> {
    validate_snapshot(snapshot)?;
    let mut files = BTreeSet::new();
    files.insert(format!(
        "users/{}/machines/{}/hoods/{}/snapshot.json",
        snapshot.owner_username, snapshot.owner_machine, snapshot.local_hood
    ));
    files.insert(format!(
        "users/{}/machines/{}/hoods/{}/README.md",
        snapshot.owner_username, snapshot.owner_machine, snapshot.local_hood
    ));
    for global_name in &snapshot.run_global_names {
        files.insert(format!("agents/{global_name}/README.md"));
    }
    for path in &snapshot.run_file_paths {
        files.insert(path.clone());
    }
    let mut session_globals: Vec<&String> = Vec::new();
    for container in &snapshot.containers {
        if container.kind == "session" || container.kind == "family" {
            session_globals.push(&container.global_name);
        } else if container.kind == "clan" {
            continue;
        } else {
            return Err(SessionManifestError::InvalidSnapshot(format!(
                "invalid container kind: {:?}",
                container.kind
            )));
        }
    }
    for global_name in &session_globals {
        files.insert(format!("sessions/{global_name}.md"));
        files.insert(format!("families/{global_name}.md"));
    }
    let canonical: Vec<String> = files.into_iter().collect();
    let legacy: Vec<String> = canonical
        .iter()
        .filter(|path| !path.starts_with("sessions/"))
        .cloned()
        .collect();
    Ok((canonical, legacy))
}

pub fn classify_session_manifest_files(
    request: &SessionManifestClassifyRequestWire,
) -> Result<SessionManifestClassifyResponseWire, SessionManifestError> {
    if request.schema_version != SESSION_MANIFEST_WIRE_SCHEMA_VERSION {
        return Err(SessionManifestError::UnsupportedSchema(
            request.schema_version,
        ));
    }
    let (canonical, legacy) =
        canonical_session_manifest_files(&request.snapshot)?;
    let has_sessions = canonical.len() != legacy.len();
    let Some(explicit) = request.explicit_files.as_ref() else {
        return Ok(SessionManifestClassifyResponseWire {
            schema_version: SESSION_MANIFEST_WIRE_SCHEMA_VERSION,
            canonical_files: canonical,
            legacy_files: legacy,
            classification: SESSION_MANIFEST_CLASS_SLIM.to_string(),
            reason: "explicit file list is omitted (slim manifest)".to_string(),
        });
    };
    if !is_sorted_unique(explicit) {
        return Ok(invalid_response(
            canonical,
            legacy,
            "explicit files must be unique and sorted",
        ));
    }
    for path in explicit {
        if !is_safe_relative_path(path) {
            return Ok(invalid_response(
                canonical,
                legacy,
                &format!("unsafe publication path: {path:?}"),
            ));
        }
    }
    if *explicit == canonical {
        return Ok(SessionManifestClassifyResponseWire {
            schema_version: SESSION_MANIFEST_WIRE_SCHEMA_VERSION,
            canonical_files: canonical,
            legacy_files: legacy,
            classification: SESSION_MANIFEST_CLASS_CURRENT.to_string(),
            reason: "explicit files equal the current canonical set"
                .to_string(),
        });
    }
    if has_sessions && *explicit == legacy {
        return Ok(SessionManifestClassifyResponseWire {
            schema_version: SESSION_MANIFEST_WIRE_SCHEMA_VERSION,
            canonical_files: canonical,
            legacy_files: legacy,
            classification: SESSION_MANIFEST_CLASS_SUPPORTED_LEGACY.to_string(),
            reason: "explicit files equal the supported legacy family-only set"
                .to_string(),
        });
    }
    Ok(invalid_response(
        canonical,
        legacy,
        "explicit files match neither the current nor the supported legacy set",
    ))
}

fn invalid_response(
    canonical: Vec<String>,
    legacy: Vec<String>,
    reason: &str,
) -> SessionManifestClassifyResponseWire {
    SessionManifestClassifyResponseWire {
        schema_version: SESSION_MANIFEST_WIRE_SCHEMA_VERSION,
        canonical_files: canonical,
        legacy_files: legacy,
        classification: SESSION_MANIFEST_CLASS_INVALID.to_string(),
        reason: reason.to_string(),
    }
}

fn validate_snapshot(
    snapshot: &SessionManifestSnapshotWire,
) -> Result<(), SessionManifestError> {
    validate_component(&snapshot.owner_username, "owner username")?;
    validate_component(&snapshot.owner_machine, "owner machine")?;
    validate_component(&snapshot.local_hood, "local hood")?;
    for name in &snapshot.run_global_names {
        validate_component(name, "run global_name")?;
    }
    for path in &snapshot.run_file_paths {
        if !is_safe_relative_path(path) {
            return Err(SessionManifestError::InvalidSnapshot(format!(
                "unsafe run file path: {path:?}"
            )));
        }
    }
    for container in &snapshot.containers {
        validate_component(&container.global_name, "container global_name")?;
        if container.kind != "session"
            && container.kind != "family"
            && container.kind != "clan"
        {
            return Err(SessionManifestError::InvalidSnapshot(format!(
                "invalid container kind: {:?}",
                container.kind
            )));
        }
    }
    Ok(())
}

fn validate_component(
    value: &str,
    label: &str,
) -> Result<(), SessionManifestError> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || value.starts_with('.')
        || value.contains('/')
        || value.contains('\\')
        || value.contains('\0')
        || value.len() > 255
    {
        return Err(SessionManifestError::InvalidSnapshot(format!(
            "invalid {label}: {value:?}"
        )));
    }
    Ok(())
}

fn is_sorted_unique(values: &[String]) -> bool {
    let mut seen = BTreeSet::new();
    for value in values {
        if !seen.insert(value) {
            return false;
        }
    }
    if seen.len() != values.len() {
        return false;
    }
    let sorted: Vec<&String> = seen.into_iter().collect();
    sorted.len() == values.len()
        && sorted.iter().zip(values.iter()).all(|(a, b)| a == &b)
}

fn is_safe_relative_path(path: &str) -> bool {
    if path.is_empty()
        || path.starts_with('/')
        || path.contains('\\')
        || path.contains('\0')
        || path.contains("//")
    {
        return false;
    }
    for part in path.split('/') {
        if part.is_empty() || part == "." || part == ".." {
            return false;
        }
        if part != ".gitkeep" && part.starts_with('.') {
            return false;
        }
        if part.len() > 255 {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot() -> SessionManifestSnapshotWire {
        SessionManifestSnapshotWire {
            owner_username: "alice".to_string(),
            owner_machine: "athena".to_string(),
            local_hood: "foo".to_string(),
            run_global_names: vec!["alice.athena.foo.bar".to_string()],
            run_file_paths: vec![
                "agents/alice.athena.foo.bar/meta.json".to_string()
            ],
            containers: vec![SessionManifestContainerWire {
                kind: "session".to_string(),
                global_name: "alice.athena.foo.bar.baz".to_string(),
            }],
        }
    }

    fn request(
        explicit: Option<Vec<String>>,
    ) -> SessionManifestClassifyRequestWire {
        SessionManifestClassifyRequestWire {
            schema_version: SESSION_MANIFEST_WIRE_SCHEMA_VERSION,
            snapshot: snapshot(),
            explicit_files: explicit,
        }
    }

    #[test]
    fn canonical_set_contains_both_page_paths() {
        let (canonical, legacy) =
            canonical_session_manifest_files(&snapshot()).unwrap();
        assert!(canonical
            .contains(&"sessions/alice.athena.foo.bar.baz.md".to_string()));
        assert!(canonical
            .contains(&"families/alice.athena.foo.bar.baz.md".to_string()));
        assert!(!legacy.iter().any(|path| path.starts_with("sessions/")));
        assert!(legacy
            .contains(&"families/alice.athena.foo.bar.baz.md".to_string()));
    }

    #[test]
    fn legacy_family_kind_counts_as_session_container() {
        let mut snap = snapshot();
        snap.containers[0].kind = "family".to_string();
        let (canonical, legacy) =
            canonical_session_manifest_files(&snap).unwrap();
        assert!(canonical
            .contains(&"sessions/alice.athena.foo.bar.baz.md".to_string()));
        assert_eq!(canonical.len(), legacy.len() + 1);
    }

    #[test]
    fn clan_containers_add_no_pages() {
        let mut snap = snapshot();
        snap.containers[0].kind = "clan".to_string();
        let (canonical, legacy) =
            canonical_session_manifest_files(&snap).unwrap();
        assert_eq!(canonical, legacy);
        assert!(!canonical.iter().any(|path| path.starts_with("sessions/")));
        assert!(!canonical.iter().any(|path| path.starts_with("families/")));
    }

    #[test]
    fn hood_without_sessions_has_no_legacy_exception() {
        let mut snap = snapshot();
        snap.containers.clear();
        let (canonical, legacy) =
            canonical_session_manifest_files(&snap).unwrap();
        assert_eq!(canonical, legacy);
        let response = classify_session_manifest_files(
            &SessionManifestClassifyRequestWire {
                schema_version: SESSION_MANIFEST_WIRE_SCHEMA_VERSION,
                snapshot: snap,
                explicit_files: Some(legacy),
            },
        )
        .unwrap();
        assert_eq!(response.classification, SESSION_MANIFEST_CLASS_CURRENT);
    }

    #[test]
    fn omitted_list_is_slim() {
        let response = classify_session_manifest_files(&request(None)).unwrap();
        assert_eq!(response.classification, SESSION_MANIFEST_CLASS_SLIM);
    }

    #[test]
    fn exact_current_and_legacy_sets_classify() {
        let (canonical, legacy) =
            canonical_session_manifest_files(&snapshot()).unwrap();
        let current =
            classify_session_manifest_files(&request(Some(canonical))).unwrap();
        assert_eq!(current.classification, SESSION_MANIFEST_CLASS_CURRENT);
        let old =
            classify_session_manifest_files(&request(Some(legacy))).unwrap();
        assert_eq!(old.classification, SESSION_MANIFEST_CLASS_SUPPORTED_LEGACY);
    }

    #[test]
    fn partial_migration_is_invalid() {
        let (canonical, legacy) =
            canonical_session_manifest_files(&snapshot()).unwrap();
        let mut partial = legacy[..legacy.len() - 1].to_vec();
        partial.sort();
        let response =
            classify_session_manifest_files(&request(Some(partial))).unwrap();
        assert_eq!(response.classification, SESSION_MANIFEST_CLASS_INVALID);
        let mut extra = canonical.clone();
        extra.push("sessions/alice.athena.foo.bar.nonexistent.md".to_string());
        extra.sort();
        let response =
            classify_session_manifest_files(&request(Some(extra))).unwrap();
        assert_eq!(response.classification, SESSION_MANIFEST_CLASS_INVALID);
    }

    #[test]
    fn unsorted_or_unsafe_lists_are_invalid() {
        let (canonical, _) =
            canonical_session_manifest_files(&snapshot()).unwrap();
        let mut unsorted = canonical.clone();
        unsorted.reverse();
        let response =
            classify_session_manifest_files(&request(Some(unsorted))).unwrap();
        assert_eq!(response.classification, SESSION_MANIFEST_CLASS_INVALID);
        let response = classify_session_manifest_files(&request(Some(vec![
            "../escape".to_string(),
        ])))
        .unwrap();
        assert_eq!(response.classification, SESSION_MANIFEST_CLASS_INVALID);
    }

    #[test]
    fn rejects_bad_schema_and_snapshot() {
        let mut bad = request(None);
        bad.schema_version = 999;
        assert!(classify_session_manifest_files(&bad).is_err());
        let mut snap = snapshot();
        snap.local_hood.clear();
        let mut bad_snapshot = request(None);
        bad_snapshot.snapshot = snap;
        assert!(classify_session_manifest_files(&bad_snapshot).is_err());
    }
}
