//! Conflict and preservation policy for backup-first stale-replay recovery.
//!
//! The host owns Git, the sidecar lock, backups, and page rendering. This
//! module classifies conflicting sidecar paths, decides whether same-run
//! byte divergences are lossless, decides whether a same-owner hood can
//! merge through snapshot preservation, and admits or refuses apply.

use crate::commit_sha::commit_shas_equivalent;

use super::wire::{
    PublicationRecoveryError, ReplayApplyDecisionRequestWire,
    ReplayApplyDecisionResponseWire, ReplayByteDivergenceRequestWire,
    ReplayByteDivergenceResponseWire, ReplayHoodMergeRequestWire,
    ReplayHoodMergeResponseWire, ReplayPathClassWire,
    ReplayPathClassifyRequestWire, ReplayPathClassifyResponseWire,
    PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
    REPLAY_APPLY_ACTION_ALREADY_COMPLETE, REPLAY_APPLY_ACTION_APPLY,
    REPLAY_APPLY_ACTION_REFUSE, REPLAY_APPLY_ACTION_REPORT,
    REPLAY_BYTES_KEEP_CURRENT, REPLAY_BYTES_KEEP_PREVIOUS, REPLAY_BYTES_REFUSE,
    REPLAY_HOOD_ACTION_MERGE, REPLAY_HOOD_ACTION_REFUSE,
    REPLAY_PATH_ARCHIVE_OBJECT, REPLAY_PATH_BROWSING,
    REPLAY_PATH_OWNER_CONTENT, REPLAY_PATH_UNKNOWN,
};

pub fn classify_replay_paths(
    request: &ReplayPathClassifyRequestWire,
) -> Result<ReplayPathClassifyResponseWire, PublicationRecoveryError> {
    require_schema(request.schema_version)?;
    let mut entries = Vec::with_capacity(request.paths.len());
    let mut browsing = Vec::new();
    let mut owner_content = Vec::new();
    let mut archive_object = Vec::new();
    let mut unknown = Vec::new();
    for path in &request.paths {
        let class = classify_replay_path(path);
        entries.push(ReplayPathClassWire {
            path: path.clone(),
            class: class.to_string(),
        });
        match class {
            REPLAY_PATH_BROWSING => browsing.push(path.clone()),
            REPLAY_PATH_OWNER_CONTENT => owner_content.push(path.clone()),
            REPLAY_PATH_ARCHIVE_OBJECT => archive_object.push(path.clone()),
            _ => unknown.push(path.clone()),
        }
    }
    Ok(ReplayPathClassifyResponseWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        entries,
        browsing,
        owner_content,
        archive_object,
        unknown,
    })
}

pub fn decide_replay_byte_divergence(
    request: &ReplayByteDivergenceRequestWire,
) -> Result<ReplayByteDivergenceResponseWire, PublicationRecoveryError> {
    require_schema(request.schema_version)?;
    let current = decode_hex(&request.current_hex, "current_hex")?;
    let previous = decode_hex(&request.previous_hex, "previous_hex")?;
    if current == previous {
        return Ok(ReplayByteDivergenceResponseWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            action: REPLAY_BYTES_KEEP_CURRENT.to_string(),
            retained: true,
            reason: "both versions are identical".to_string(),
        });
    }
    if previous.is_empty() {
        return Ok(ReplayByteDivergenceResponseWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            action: REPLAY_BYTES_KEEP_CURRENT.to_string(),
            retained: true,
            reason: "the previous version is empty and is retained by the current bytes"
                .to_string(),
        });
    }
    if current.is_empty() {
        return Ok(ReplayByteDivergenceResponseWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            action: REPLAY_BYTES_KEEP_PREVIOUS.to_string(),
            retained: true,
            reason: "the current version is empty and is retained by the previous bytes"
                .to_string(),
        });
    }
    if current.starts_with(&previous) {
        return Ok(ReplayByteDivergenceResponseWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            action: REPLAY_BYTES_KEEP_CURRENT.to_string(),
            retained: true,
            reason: "the previous bytes are a prefix of the current bytes"
                .to_string(),
        });
    }
    if previous.starts_with(&current) {
        return Ok(ReplayByteDivergenceResponseWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            action: REPLAY_BYTES_KEEP_PREVIOUS.to_string(),
            retained: true,
            reason: "the current bytes are a prefix of the previous bytes"
                .to_string(),
        });
    }
    Ok(ReplayByteDivergenceResponseWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        action: REPLAY_BYTES_REFUSE.to_string(),
        retained: false,
        reason: "same-run bytes diverge without a prefix proof that either version is retained"
            .to_string(),
    })
}

pub fn decide_replay_hood_merge(
    request: &ReplayHoodMergeRequestWire,
) -> Result<ReplayHoodMergeResponseWire, PublicationRecoveryError> {
    require_schema(request.schema_version)?;
    if request.local_owner_username.is_empty()
        || request.local_owner_machine.is_empty()
        || request.local_hood.is_empty()
        || request.upstream_owner_username.is_empty()
        || request.upstream_owner_machine.is_empty()
        || request.upstream_hood.is_empty()
    {
        return Err(PublicationRecoveryError::InvalidRequest(
            "replay hood merge requires owner, machine, and hood on both sides"
                .to_string(),
        ));
    }
    if request.local_owner_username != request.upstream_owner_username
        || request.local_owner_machine != request.upstream_owner_machine
    {
        return Ok(ReplayHoodMergeResponseWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            action: REPLAY_HOOD_ACTION_REFUSE.to_string(),
            reason:
                "foreign owner identity cannot merge through local preservation"
                    .to_string(),
        });
    }
    if request.local_hood != request.upstream_hood {
        return Ok(ReplayHoodMergeResponseWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            action: REPLAY_HOOD_ACTION_REFUSE.to_string(),
            reason: "same-owner hood names differ; they are independent hoods"
                .to_string(),
        });
    }
    Ok(ReplayHoodMergeResponseWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        action: REPLAY_HOOD_ACTION_MERGE.to_string(),
        reason: "same owner and hood can merge through snapshot preservation"
            .to_string(),
    })
}

pub fn decide_replay_apply(
    request: &ReplayApplyDecisionRequestWire,
) -> Result<ReplayApplyDecisionResponseWire, PublicationRecoveryError> {
    require_schema(request.schema_version)?;
    if request.already_complete {
        return Ok(ReplayApplyDecisionResponseWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            action: REPLAY_APPLY_ACTION_ALREADY_COMPLETE.to_string(),
            reason: "stale-replay recovery already completed for this backup"
                .to_string(),
        });
    }
    if !request.apply {
        return Ok(ReplayApplyDecisionResponseWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            action: REPLAY_APPLY_ACTION_REPORT.to_string(),
            reason: "report-only recovery does not reset or publish"
                .to_string(),
        });
    }
    if request.unreconcilable {
        return Ok(ReplayApplyDecisionResponseWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            action: REPLAY_APPLY_ACTION_REFUSE.to_string(),
            reason:
                "identity or content cannot be reconciled; backup is intact"
                    .to_string(),
        });
    }
    if !request.backup_complete || !request.backup_verified {
        return Ok(ReplayApplyDecisionResponseWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            action: REPLAY_APPLY_ACTION_REFUSE.to_string(),
            reason: "apply requires a verified complete backup".to_string(),
        });
    }
    if request.mutation_started {
        return Ok(ReplayApplyDecisionResponseWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            action: REPLAY_APPLY_ACTION_APPLY.to_string(),
            reason: "resume replay after an interrupted mutation".to_string(),
        });
    }
    if !commit_shas_equivalent(&request.observed_head, &request.expected_head) {
        return Ok(ReplayApplyDecisionResponseWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            action: REPLAY_APPLY_ACTION_REFUSE.to_string(),
            reason: "observed HEAD changed before mutation; backup is intact"
                .to_string(),
        });
    }
    Ok(ReplayApplyDecisionResponseWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        action: REPLAY_APPLY_ACTION_APPLY.to_string(),
        reason: "verified backup matches the expected HEAD".to_string(),
    })
}

pub fn classify_replay_path(path: &str) -> &'static str {
    let normalized = path.trim_start_matches("./").replace('\\', "/");
    if normalized.is_empty() || normalized.contains('\0') {
        return REPLAY_PATH_UNKNOWN;
    }
    let parts: Vec<&str> = normalized
        .split('/')
        .filter(|part| !part.is_empty() && *part != ".")
        .collect();
    if parts.is_empty() || parts.contains(&"..") {
        return REPLAY_PATH_UNKNOWN;
    }
    match parts.as_slice() {
        ["files", "objects", ..] => REPLAY_PATH_ARCHIVE_OBJECT,
        ["prompts", ..] | ["artifacts", ..] => REPLAY_PATH_BROWSING,
        ["README.md"] | ["schema.json"] => REPLAY_PATH_BROWSING,
        ["sessions", name] if name.ends_with(".md") => REPLAY_PATH_BROWSING,
        ["families", name] if name.ends_with(".md") => REPLAY_PATH_BROWSING,
        ["sessions", ".gitkeep"]
        | ["families", ".gitkeep"]
        | ["agents", ".gitkeep"] => REPLAY_PATH_BROWSING,
        ["agents", _, "README.md"] => REPLAY_PATH_BROWSING,
        ["agents", _, ..] => REPLAY_PATH_OWNER_CONTENT,
        ["users", _, "README.md"] => REPLAY_PATH_BROWSING,
        ["users", _, "machines", _, "README.md"] => REPLAY_PATH_BROWSING,
        ["users", _, "machines", _, "manifest.json"] => {
            REPLAY_PATH_OWNER_CONTENT
        }
        ["users", _, "machines", _, "hoods", _, "README.md"] => {
            REPLAY_PATH_BROWSING
        }
        ["users", _, "machines", _, "hoods", _, "snapshot.json"] => {
            REPLAY_PATH_OWNER_CONTENT
        }
        ["users", _, "machines", _, "hoods", _, ..] => {
            REPLAY_PATH_OWNER_CONTENT
        }
        ["users", _, "machines", _, ..] => REPLAY_PATH_OWNER_CONTENT,
        ["users", ..] => REPLAY_PATH_BROWSING,
        _ => REPLAY_PATH_UNKNOWN,
    }
}

fn require_schema(version: u32) -> Result<(), PublicationRecoveryError> {
    if version != PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION {
        return Err(PublicationRecoveryError::UnsupportedSchema(version));
    }
    Ok(())
}

fn decode_hex(
    value: &str,
    field: &str,
) -> Result<Vec<u8>, PublicationRecoveryError> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    if !value.len().is_multiple_of(2)
        || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(PublicationRecoveryError::InvalidRequest(format!(
            "{field} must be even-length hexadecimal"
        )));
    }
    (0..value.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&value[index..index + 2], 16).map_err(|_| {
                PublicationRecoveryError::InvalidRequest(format!(
                    "{field} must be even-length hexadecimal"
                ))
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn classify(paths: &[&str]) -> ReplayPathClassifyResponseWire {
        classify_replay_paths(&ReplayPathClassifyRequestWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            paths: paths.iter().map(|path| (*path).to_string()).collect(),
        })
        .unwrap()
    }

    fn hex_of(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn classifies_browsing_owner_content_and_archive_paths() {
        let classified = classify(&[
            "README.md",
            "schema.json",
            "sessions/alice.athena.foo.md",
            "families/alice.athena.foo.md",
            "agents/alice.athena.foo/README.md",
            "users/alice/README.md",
            "users/alice/machines/athena/README.md",
            "users/alice/machines/athena/hoods/foo/README.md",
            "users/alice/machines/athena/hoods/foo/snapshot.json",
            "users/alice/machines/athena/manifest.json",
            "agents/alice.athena.foo/chat.md",
            "agents/alice.athena.foo/prompt.md",
            "files/objects/sha256/ab/abc",
            "prompts/stale.md",
            "artifacts/scratch.bin",
            "mystery.bin",
        ]);
        assert_eq!(
            classified.browsing,
            vec![
                "README.md",
                "schema.json",
                "sessions/alice.athena.foo.md",
                "families/alice.athena.foo.md",
                "agents/alice.athena.foo/README.md",
                "users/alice/README.md",
                "users/alice/machines/athena/README.md",
                "users/alice/machines/athena/hoods/foo/README.md",
                "prompts/stale.md",
                "artifacts/scratch.bin",
            ]
        );
        assert_eq!(
            classified.owner_content,
            vec![
                "users/alice/machines/athena/hoods/foo/snapshot.json",
                "users/alice/machines/athena/manifest.json",
                "agents/alice.athena.foo/chat.md",
                "agents/alice.athena.foo/prompt.md",
            ]
        );
        assert_eq!(classified.archive_object.len(), 1);
        assert_eq!(classified.unknown, vec!["mystery.bin"]);
    }

    #[test]
    fn prefix_proof_keeps_the_longer_version() {
        let keep_current =
            decide_replay_byte_divergence(&ReplayByteDivergenceRequestWire {
                schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
                current_hex: hex_of(b"hello world"),
                previous_hex: hex_of(b"hello"),
            })
            .unwrap();
        assert_eq!(keep_current.action, REPLAY_BYTES_KEEP_CURRENT);
        assert!(keep_current.retained);

        let keep_previous =
            decide_replay_byte_divergence(&ReplayByteDivergenceRequestWire {
                schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
                current_hex: hex_of(b"hello"),
                previous_hex: hex_of(b"hello world"),
            })
            .unwrap();
        assert_eq!(keep_previous.action, REPLAY_BYTES_KEEP_PREVIOUS);
        assert!(keep_previous.retained);
    }

    #[test]
    fn longer_unrelated_text_is_not_a_superset() {
        let decision =
            decide_replay_byte_divergence(&ReplayByteDivergenceRequestWire {
                schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
                current_hex: hex_of(b"aaaaaaaaaa"),
                previous_hex: hex_of(b"bbbb"),
            })
            .unwrap();
        assert_eq!(decision.action, REPLAY_BYTES_REFUSE);
        assert!(!decision.retained);
    }

    #[test]
    fn same_owner_hood_merges_and_foreign_identity_refuses() {
        let merge = decide_replay_hood_merge(&ReplayHoodMergeRequestWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            local_owner_username: "alice".to_string(),
            local_owner_machine: "athena".to_string(),
            local_hood: "foo".to_string(),
            upstream_owner_username: "alice".to_string(),
            upstream_owner_machine: "athena".to_string(),
            upstream_hood: "foo".to_string(),
        })
        .unwrap();
        assert_eq!(merge.action, REPLAY_HOOD_ACTION_MERGE);

        let foreign = decide_replay_hood_merge(&ReplayHoodMergeRequestWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            local_owner_username: "alice".to_string(),
            local_owner_machine: "athena".to_string(),
            local_hood: "foo".to_string(),
            upstream_owner_username: "bob".to_string(),
            upstream_owner_machine: "apollo".to_string(),
            upstream_hood: "foo".to_string(),
        })
        .unwrap();
        assert_eq!(foreign.action, REPLAY_HOOD_ACTION_REFUSE);
    }

    #[test]
    fn apply_refuses_changed_head_and_incomplete_backup() {
        let head = "d7e06b77b42d89ecf4bb1538c6f89c6fe700124e";
        let changed = decide_replay_apply(&ReplayApplyDecisionRequestWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            apply: true,
            observed_head: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_string(),
            expected_head: head.to_string(),
            backup_complete: true,
            backup_verified: true,
            unreconcilable: false,
            already_complete: false,
            mutation_started: false,
        })
        .unwrap();
        assert_eq!(changed.action, REPLAY_APPLY_ACTION_REFUSE);

        let incomplete = decide_replay_apply(&ReplayApplyDecisionRequestWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            apply: true,
            observed_head: head.to_string(),
            expected_head: head.to_string(),
            backup_complete: false,
            backup_verified: false,
            unreconcilable: false,
            already_complete: false,
            mutation_started: false,
        })
        .unwrap();
        assert_eq!(incomplete.action, REPLAY_APPLY_ACTION_REFUSE);

        let admitted = decide_replay_apply(&ReplayApplyDecisionRequestWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            apply: true,
            observed_head: head.to_string(),
            expected_head: head.to_string(),
            backup_complete: true,
            backup_verified: true,
            unreconcilable: false,
            already_complete: false,
            mutation_started: false,
        })
        .unwrap();
        assert_eq!(admitted.action, REPLAY_APPLY_ACTION_APPLY);

        let report = decide_replay_apply(&ReplayApplyDecisionRequestWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            apply: false,
            observed_head: head.to_string(),
            expected_head: head.to_string(),
            backup_complete: false,
            backup_verified: false,
            unreconcilable: false,
            already_complete: false,
            mutation_started: false,
        })
        .unwrap();
        assert_eq!(report.action, REPLAY_APPLY_ACTION_REPORT);
    }

    #[test]
    fn unsupported_schema_is_rejected() {
        let error = classify_replay_paths(&ReplayPathClassifyRequestWire {
            schema_version: 99,
            paths: vec!["README.md".to_string()],
        })
        .unwrap_err();
        assert_eq!(error, PublicationRecoveryError::UnsupportedSchema(99));
    }
}
