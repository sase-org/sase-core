//! Fingerprint canonicalization with optional evidence slots.

use serde::Serialize;

use super::canonical::canonical_digest;
use super::wire::{
    ToolEvidenceCompletenessWire, ToolFingerprintCanonicalizeResultWire,
    ToolFingerprintWire, TOOL_RUN_WIRE_SCHEMA_VERSION,
};
use super::ToolRunError;

#[derive(Serialize)]
struct ToolFingerprintIdentity<'a> {
    project_identity: &'a Option<String>,
    definition_digest: &'a Option<String>,
    extra_args_digest: &'a Option<String>,
    repos: &'a [super::wire::ToolRepoFingerprintWire],
    inputs: &'a [super::wire::ToolInputFingerprintWire],
    env: &'a std::collections::BTreeMap<String, Option<String>>,
    toolchain: &'a std::collections::BTreeMap<
        String,
        super::wire::ToolToolchainProbeWire,
    >,
}

pub fn canonicalize_tool_fingerprint(
    mut fingerprint: ToolFingerprintWire,
) -> Result<ToolFingerprintCanonicalizeResultWire, ToolRunError> {
    if fingerprint.schema_version != TOOL_RUN_WIRE_SCHEMA_VERSION {
        return Err(ToolRunError::SchemaVersion {
            expected: TOOL_RUN_WIRE_SCHEMA_VERSION,
            actual: fingerprint.schema_version,
        });
    }
    fingerprint
        .repos
        .sort_by(|left, right| left.identity.cmp(&right.identity));
    for repo in &mut fingerprint.repos {
        if repo.identity.trim().is_empty() {
            return Err(ToolRunError::invalid(
                "fingerprint repo identity must not be empty",
            ));
        }
        repo.dirty_paths
            .sort_by(|left, right| left.path.cmp(&right.path));
        for dirty in &repo.dirty_paths {
            if dirty.path.starts_with('/') {
                return Err(ToolRunError::invalid(format!(
                    "fingerprint dirty path {:?} must not be a physical checkout path",
                    dirty.path
                )));
            }
        }
    }
    fingerprint
        .inputs
        .sort_by(|left, right| left.pattern.cmp(&right.pattern));
    for input in &mut fingerprint.inputs {
        input
            .matches
            .sort_by(|left, right| left.path.cmp(&right.path));
    }
    if fingerprint.completeness.missing.is_empty()
        && !fingerprint.completeness.complete
    {
        fingerprint.completeness = ToolEvidenceCompletenessWire::default();
    }
    let digest = fingerprint_digest(&fingerprint)?;
    Ok(ToolFingerprintCanonicalizeResultWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        fingerprint,
        digest: digest.clone(),
        diagnostics: vec![format!("canonical fingerprint digest {digest}")],
    })
}

pub fn fingerprint_digest(
    fingerprint: &ToolFingerprintWire,
) -> Result<String, ToolRunError> {
    let identity = ToolFingerprintIdentity {
        project_identity: &fingerprint.project_identity,
        definition_digest: &fingerprint.definition_digest,
        extra_args_digest: &fingerprint.extra_args_digest,
        repos: &fingerprint.repos,
        inputs: &fingerprint.inputs,
        env: &fingerprint.env,
        toolchain: &fingerprint.toolchain,
    };
    canonical_digest(&identity).map_err(ToolRunError::invalid)
}

pub fn unknown_evidence(reason: &str) -> ToolFingerprintWire {
    ToolFingerprintWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        completeness: ToolEvidenceCompletenessWire {
            complete: false,
            missing: vec![reason.to_string()],
        },
        diagnostics: vec![reason.to_string()],
        ..ToolFingerprintWire::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tool_run::wire::{ToolDirtyPathWire, ToolRepoFingerprintWire};

    #[test]
    fn unknown_evidence_is_explicit_not_zero() {
        let fingerprint = unknown_evidence("PSI unavailable on this host");
        assert!(!fingerprint.completeness.complete);
        assert_eq!(
            fingerprint.completeness.missing,
            ["PSI unavailable on this host"]
        );
        assert!(fingerprint.project_identity.is_none());
    }

    #[test]
    fn rejects_physical_checkout_paths_in_dirty_entries() {
        let fingerprint = ToolFingerprintWire {
            repos: vec![ToolRepoFingerprintWire {
                identity: "sase".into(),
                head: None,
                index_tree: None,
                dirty_paths: vec![ToolDirtyPathWire {
                    path: "/home/bryan/src/sase/foo.py".into(),
                    status: "modified".into(),
                    kind: "file".into(),
                    mode: None,
                    content_hash: None,
                    incomplete: None,
                }],
                incomplete: None,
            }],
            ..ToolFingerprintWire::default()
        };
        assert!(canonicalize_tool_fingerprint(fingerprint).is_err());
    }

    #[test]
    fn sorts_repos_and_dirty_paths_before_digest() {
        let mut left = ToolFingerprintWire::default();
        left.completeness.complete = true;
        left.completeness.missing.clear();
        left.repos = vec![
            ToolRepoFingerprintWire {
                identity: "b".into(),
                head: Some("aaa".into()),
                index_tree: None,
                dirty_paths: vec![
                    ToolDirtyPathWire {
                        path: "z.py".into(),
                        status: "modified".into(),
                        kind: "file".into(),
                        mode: None,
                        content_hash: Some("1".into()),
                        incomplete: None,
                    },
                    ToolDirtyPathWire {
                        path: "a.py".into(),
                        status: "modified".into(),
                        kind: "file".into(),
                        mode: None,
                        content_hash: Some("2".into()),
                        incomplete: None,
                    },
                ],
                incomplete: None,
            },
            ToolRepoFingerprintWire {
                identity: "a".into(),
                head: Some("bbb".into()),
                index_tree: None,
                dirty_paths: Vec::new(),
                incomplete: None,
            },
        ];
        let mut right = left.clone();
        right.repos.reverse();
        right.repos[0].dirty_paths.reverse();
        let left = canonicalize_tool_fingerprint(left).unwrap();
        let right = canonicalize_tool_fingerprint(right).unwrap();
        assert_eq!(left.digest, right.digest);
        assert_eq!(left.fingerprint.repos[0].identity, "a");
        assert_eq!(left.fingerprint.repos[1].dirty_paths[0].path, "a.py");
    }

    #[test]
    fn golden_unknown_evidence_fixture_stays_explicit() {
        let fixture: ToolFingerprintWire = serde_json::from_str(include_str!(
            "fixtures/unknown_evidence.json"
        ))
        .unwrap();
        assert!(!fixture.completeness.complete);
        assert!(fixture.project_identity.is_none());
        assert!(fixture.definition_digest.is_none());
        let canonical = canonicalize_tool_fingerprint(fixture).unwrap();
        assert!(!canonical.fingerprint.completeness.complete);
    }
}
