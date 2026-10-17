use super::classify::{
    classify_file, AttributeState, Classification, CommittedAttributes,
    SkippedEntry, MAX_CLASSIFIED_BLOB_BYTES,
};
use super::wire::{
    ProjectStatsExclusionReasonWire as Reason,
    ProjectStatsFileExclusionWire as Exclusion,
    ProjectStatsFileRoleWire as Role,
};

fn attrs(
    generated: AttributeState,
    vendored: AttributeState,
    documentation: AttributeState,
) -> CommittedAttributes {
    CommittedAttributes {
        generated,
        vendored,
        documentation,
    }
}

fn unspecified() -> CommittedAttributes {
    CommittedAttributes::default()
}

fn classify(path: &[u8], bytes: Option<&[u8]>) -> Classification {
    classify_file(
        path,
        0o100644,
        bytes.map_or(0, |value| value.len() as u64),
        unspecified(),
        bytes,
    )
}

#[test]
fn parses_all_boolean_attribute_states_without_conflating_false_and_missing() {
    assert_eq!(AttributeState::parse(b"set"), Ok(AttributeState::Enabled));
    assert_eq!(AttributeState::parse(b"true"), Ok(AttributeState::Enabled));
    assert_eq!(
        AttributeState::parse(b"unset"),
        Ok(AttributeState::Disabled)
    );
    assert_eq!(
        AttributeState::parse(b"false"),
        Ok(AttributeState::Disabled)
    );
    assert_eq!(
        AttributeState::parse(b"unspecified"),
        Ok(AttributeState::Unspecified)
    );
    assert!(AttributeState::parse(b"maybe").is_err());
}

#[test]
fn explicit_attribute_values_override_or_suppress_path_heuristics() {
    assert_eq!(
        classify_file(
            b"src/lib.rs",
            0o100644,
            3,
            attrs(
                AttributeState::Enabled,
                AttributeState::Unspecified,
                AttributeState::Unspecified
            ),
            Some(b"fn main() {}"),
        ),
        Classification::Excluded {
            kind: Exclusion::Generated,
            reason: Reason::Attribute,
        }
    );
    assert_eq!(
        classify_file(
            b"src/generated/keep.rs",
            0o100644,
            3,
            attrs(
                AttributeState::Disabled,
                AttributeState::Unspecified,
                AttributeState::Unspecified
            ),
            Some(b"fn main() {}"),
        ),
        Classification::Included {
            role: Role::Source,
            language: Some("rust".to_owned()),
        }
    );
    assert_eq!(
        classify_file(
            b"src/lib.rs",
            0o100644,
            3,
            attrs(
                AttributeState::Unspecified,
                AttributeState::Enabled,
                AttributeState::Unspecified
            ),
            Some(b"fn main() {}"),
        ),
        Classification::Excluded {
            kind: Exclusion::Vendored,
            reason: Reason::Attribute,
        }
    );
    assert_eq!(
        classify_file(
            b"vendor/keep.rs",
            0o100644,
            3,
            attrs(
                AttributeState::Unspecified,
                AttributeState::Disabled,
                AttributeState::Unspecified
            ),
            Some(b"fn main() {}"),
        ),
        Classification::Included {
            role: Role::Source,
            language: Some("rust".to_owned()),
        }
    );
    assert_eq!(
        classify_file(
            b"src/generated/vendor.rs",
            0o100644,
            3,
            attrs(
                AttributeState::Enabled,
                AttributeState::Enabled,
                AttributeState::Unspecified
            ),
            Some(b"fn main() {}"),
        ),
        Classification::Excluded {
            kind: Exclusion::Generated,
            reason: Reason::Attribute,
        }
    );
    assert_eq!(
        classify_file(
            b"src/generated/guide.rs",
            0o100644,
            3,
            attrs(
                AttributeState::Disabled,
                AttributeState::Unspecified,
                AttributeState::Enabled
            ),
            Some(b"fn main() {}"),
        ),
        Classification::Included {
            role: Role::Docs,
            language: None,
        }
    );
    assert_eq!(
        classify_file(
            b"README.md",
            0o100644,
            8,
            attrs(
                AttributeState::Unspecified,
                AttributeState::Unspecified,
                AttributeState::Disabled
            ),
            Some(b"# Guide\n"),
        ),
        Classification::Included {
            role: Role::Other,
            language: None,
        }
    );
}

#[test]
fn generated_and_vendored_heuristics_precede_ordinary_roles() {
    assert_eq!(
        classify(b"src/generated/module.rs", Some(b"fn main() {}")),
        Classification::Excluded {
            kind: Exclusion::Generated,
            reason: Reason::PathHeuristic,
        }
    );
    assert_eq!(
        classify(b"third_party/lib.rs", Some(b"fn main() {}")),
        Classification::Excluded {
            kind: Exclusion::Vendored,
            reason: Reason::PathHeuristic,
        }
    );
    assert_eq!(
        classify(b"web/app.min.js", Some(b"const x = 1;")),
        Classification::Excluded {
            kind: Exclusion::Generated,
            reason: Reason::PathHeuristic,
        }
    );
    assert_eq!(
        classify(b"tests/expected.snap", Some(b"snapshot")),
        Classification::Excluded {
            kind: Exclusion::Generated,
            reason: Reason::PathHeuristic,
        }
    );
}

#[test]
fn classifies_agent_context_docs_config_tests_source_and_other() {
    struct RoleCase {
        path: &'static [u8],
        bytes: Option<&'static [u8]>,
        role: Role,
    }
    let cases = [
        RoleCase {
            path: b"sase/memory/AGENTS.md",
            bytes: Some(b"Agent notes"),
            role: Role::AgentContext,
        },
        RoleCase {
            path: b".github/skills/review/SKILL.md",
            bytes: Some(b"Instructions"),
            role: Role::AgentContext,
        },
        RoleCase {
            path: b".claude/settings.json",
            bytes: Some(b"{}"),
            role: Role::ConfigData,
        },
        RoleCase {
            path: b"docs/guide.md",
            bytes: Some(b"Guide"),
            role: Role::Docs,
        },
        RoleCase {
            path: b"tests/fixtures/data.json",
            bytes: Some(b"{}"),
            role: Role::ConfigData,
        },
        RoleCase {
            path: b"tests/test_parser.py",
            bytes: Some(b"def test_it(): pass"),
            role: Role::Tests,
        },
        RoleCase {
            path: b"src/lib.rs",
            bytes: Some(b"fn main() {}"),
            role: Role::Source,
        },
        RoleCase {
            path: b"Cargo.lock",
            bytes: Some(b"version = 3"),
            role: Role::ConfigData,
        },
        RoleCase {
            path: b"misc/data.unknown",
            bytes: Some(b"unclassified"),
            role: Role::Other,
        },
    ];
    for RoleCase { path, bytes, role } in cases {
        let result = classify(path, bytes);
        assert!(
            matches!(result, Classification::Included { role: actual, .. } if actual == role),
            "{path:?} was classified as {result:?}, expected {role:?}"
        );
    }
}

#[test]
fn recognizes_known_and_bounded_env_shebang_languages() {
    assert_eq!(
        classify(
            b"scripts/runner",
            Some(b"#!/usr/bin/env -S python3 -u\nprint(1)")
        ),
        Classification::Included {
            role: Role::Source,
            language: Some("python".to_owned()),
        }
    );
    assert_eq!(
        classify(
            b"scripts/task.unknown",
            Some(b"#!/usr/bin/env -u OLD=1 -S node --trace\nconsole.log(1)"),
        ),
        Classification::Included {
            role: Role::Source,
            language: Some("javascript".to_owned()),
        }
    );
    let mut late_shebang = vec![b'x'; 9000];
    late_shebang.extend_from_slice(b"\n#!/usr/bin/env python3\n");
    assert_eq!(
        classify(b"script.unknown", Some(&late_shebang)),
        Classification::Included {
            role: Role::Other,
            language: None,
        }
    );
}

#[test]
fn safety_exclusions_and_skipped_entry_modes_cannot_be_overridden() {
    assert_eq!(
        classify_file(
            b"docs/blob.md",
            0o100644,
            4,
            attrs(
                AttributeState::Enabled,
                AttributeState::Enabled,
                AttributeState::Enabled
            ),
            Some(b"a\0b"),
        ),
        Classification::Excluded {
            kind: Exclusion::Binary,
            reason: Reason::NulByte,
        }
    );
    let pointer = format!(
        "version https://git-lfs.github.com/spec/v1\noid sha256:{}\nsize 42\n",
        "a".repeat(64)
    );
    assert_eq!(
        classify_file(
            b"src/large.rs",
            0o100644,
            pointer.len() as u64,
            attrs(
                AttributeState::Enabled,
                AttributeState::Unspecified,
                AttributeState::Unspecified
            ),
            Some(pointer.as_bytes()),
        ),
        Classification::Excluded {
            kind: Exclusion::Binary,
            reason: Reason::LfsPointer,
        }
    );
    assert_eq!(
        classify_file(
            b"docs/guide.md",
            0o100644,
            MAX_CLASSIFIED_BLOB_BYTES + 1,
            attrs(
                AttributeState::Unspecified,
                AttributeState::Unspecified,
                AttributeState::Enabled
            ),
            None,
        ),
        Classification::Excluded {
            kind: Exclusion::Generated,
            reason: Reason::Oversize,
        }
    );
    assert_eq!(
        classify_file(b"link", 0o120000, 4, unspecified(), None),
        Classification::Skipped(SkippedEntry::Symlink)
    );
    assert_eq!(
        classify_file(b"submodule", 0o160000, 0, unspecified(), None),
        Classification::Skipped(SkippedEntry::Gitlink)
    );
}

#[test]
fn classification_preserves_non_utf8_paths_without_lossy_collisions() {
    let result = classify(b"src/lib\xff.rs", Some(b"fn main() {}"));
    assert_eq!(
        result,
        Classification::Included {
            role: Role::Source,
            language: Some("rust".to_owned()),
        }
    );
}
