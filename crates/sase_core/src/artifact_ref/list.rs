//! Shared codec and batched resolution for stored artifact-reference lists.

use std::collections::HashSet;
use std::path::Path;

use crate::artifact_file::ArtifactFileWire;

use super::{
    parse_artifact_ref, read_artifact_index, render_artifact_ref,
    resolve_artifact_ref, resolve_file_from_artifacts, ArtifactRefContextWire,
    ArtifactRefError, ArtifactRefKindWire, ArtifactRefListEntryWire,
    ArtifactRefListResolutionWire, ArtifactRefPayloadWire,
    ArtifactRefResolutionWire, ParsedArtifactRefWire,
    ARTIFACT_REF_LIST_RESOLUTION_WIRE_SCHEMA_VERSION,
    ARTIFACT_REF_RESOLUTION_WIRE_SCHEMA_VERSION,
};

/// Parse every stored entry, reporting the first malformed value and its
/// one-based position.
pub fn parse_artifact_ref_list<T: AsRef<str>>(
    entries: &[T],
) -> Result<Vec<ParsedArtifactRefWire>, ArtifactRefError> {
    entries
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let value = value.as_ref();
            parse_artifact_ref(value).map_err(|error| ArtifactRefError {
                kind: error.kind,
                message: format!(
                    "artifact reference list entry {} ({value:?}): {}",
                    index + 1,
                    error.message
                ),
            })
        })
        .collect()
}

/// Canonically render, deduplicate, and preserve the first-occurrence order
/// of a caller-supplied artifact-reference list.
pub fn normalize_artifact_ref_list<T: AsRef<str>>(
    entries: &[T],
) -> Result<Vec<String>, ArtifactRefError> {
    let parsed = parse_artifact_ref_list(entries)?;
    let mut seen = HashSet::with_capacity(parsed.len());
    let mut normalized = Vec::with_capacity(parsed.len());
    for reference in parsed {
        let rendered = render_artifact_ref(&reference)?;
        if seen.insert(rendered.clone()) {
            normalized.push(rendered);
        }
    }
    Ok(normalized)
}

/// Resolve a whole stored list while loading the artifact-file index at most
/// once for all `file:` entries.
pub fn resolve_artifact_ref_list<T: AsRef<str>>(
    entries: &[T],
    context: &ArtifactRefContextWire,
) -> Result<ArtifactRefListResolutionWire, ArtifactRefError> {
    super::validate_artifact_ref_context(context)?;
    resolve_artifact_ref_list_with_loader(entries, context, read_artifact_index)
}

fn resolve_artifact_ref_list_with_loader<T, F>(
    entries: &[T],
    context: &ArtifactRefContextWire,
    mut load_artifact_index: F,
) -> Result<ArtifactRefListResolutionWire, ArtifactRefError>
where
    T: AsRef<str>,
    F: FnMut(&Path) -> Result<Vec<ArtifactFileWire>, ArtifactRefError>,
{
    let parsed = entries
        .iter()
        .map(|entry| parse_artifact_ref(entry.as_ref()))
        .collect::<Vec<_>>();
    let has_file_reference = parsed.iter().any(|reference| {
        matches!(
            reference,
            Ok(ParsedArtifactRefWire {
                kind: ArtifactRefKindWire::File,
                ..
            })
        )
    });
    let artifact_index = if has_file_reference {
        context
            .artifact_index_path
            .as_deref()
            .map(Path::new)
            .map(&mut load_artifact_index)
            .transpose()?
    } else {
        None
    };

    let mut resolved_entries = Vec::with_capacity(entries.len());
    for (raw, parsed) in entries.iter().zip(parsed) {
        let raw = raw.as_ref();
        let entry = match parsed {
            Ok(reference) => {
                let rendered = render_artifact_ref(&reference)?;
                let resolution = match (&reference.kind, &reference.payload) {
                    (
                        ArtifactRefKindWire::File,
                        ArtifactRefPayloadWire::File { source, digest },
                    ) => resolve_file_from_artifacts(
                        *source,
                        digest,
                        rendered.clone(),
                        artifact_index.as_deref(),
                    )?,
                    _ => resolve_artifact_ref(&reference, context)?,
                };
                ArtifactRefListEntryWire {
                    rendered,
                    resolution,
                }
            }
            Err(_) => {
                let rendered = raw.to_string();
                ArtifactRefListEntryWire {
                    rendered: rendered.clone(),
                    resolution: ArtifactRefResolutionWire {
                        schema_version:
                            ARTIFACT_REF_RESOLUTION_WIRE_SCHEMA_VERSION,
                        status: "unknown_kind".to_string(),
                        rendered,
                        locator: None,
                        resolved_path: None,
                        candidates: Vec::new(),
                        diagnostic: None,
                    },
                }
            }
        };
        resolved_entries.push(entry);
    }

    Ok(ArtifactRefListResolutionWire {
        schema_version: ARTIFACT_REF_LIST_RESOLUTION_WIRE_SCHEMA_VERSION,
        entries: resolved_entries,
    })
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;

    const DIGEST: &str = "52895d68931185056fd0e49f";

    #[test]
    fn parse_and_normalize_every_kind_and_fragment() {
        let entries = [
            "plans:202607/plan.md#L2-L4",
            "chat:202607/main.md#page=2",
            "file:default:52895d68931185056fd0e49f#t=90",
            "commit:sase@0123456789abcdef0123456789abcdef01234567",
            "bug:sase#123",
            "bead:sase-bb.1",
            "agent:bbugyi200.athena.9w",
        ];
        let parsed = parse_artifact_ref_list(&entries).unwrap();
        assert_eq!(parsed.len(), entries.len());
        assert_eq!(normalize_artifact_ref_list(&entries).unwrap(), entries);
    }

    #[test]
    fn normalize_deduplicates_in_first_occurrence_order() {
        let entries = [
            "plans:first.md",
            "bead:sase-bb",
            "plans:first.md",
            "plans:second.md",
            "bead:sase-bb",
        ];
        assert_eq!(
            normalize_artifact_ref_list(&entries).unwrap(),
            ["plans:first.md", "bead:sase-bb", "plans:second.md"]
        );
    }

    #[test]
    fn parse_rejects_malformed_sigiled_and_empty_entries_with_position() {
        for value in ["not-a-reference", "@plans:x.md", ""] {
            let error =
                parse_artifact_ref_list(&["plans:ok.md", value]).unwrap_err();
            assert_eq!(error.kind, "validation");
            assert!(error.message.contains("entry 2"), "{error}");
            assert!(error.message.contains(&format!("{value:?}")), "{error}");
        }
    }

    #[test]
    fn batch_resolve_loads_the_artifact_index_once() {
        let count = Cell::new(0);
        let context = ArtifactRefContextWire {
            artifact_index_path: Some("/unused/index.jsonl".to_string()),
            ..Default::default()
        };
        let entries = [
            format!("file:default:{DIGEST}"),
            "plans:missing.md".to_string(),
            format!("file:default:{DIGEST}"),
        ];
        let result =
            resolve_artifact_ref_list_with_loader(&entries, &context, |_| {
                count.set(count.get() + 1);
                Ok(vec![ArtifactFileWire {
                    schema_version: 1,
                    id: format!("default:{DIGEST}"),
                    label: None,
                    kind: None,
                    path: Some("/tmp/image.png".to_string()),
                    vcs_repo: None,
                    vcs_sha: None,
                    vcs_relpath: None,
                    source_path: None,
                    workspace_dir: None,
                    created_at: None,
                    agent_artifacts_dir: None,
                    project: None,
                    workflow: None,
                    raw_timestamp: None,
                    agent_name: None,
                    explicit: false,
                    sha256: None,
                    size_bytes: None,
                    mime_type: None,
                }])
            })
            .unwrap();

        assert_eq!(count.get(), 1);
        assert_eq!(result.entries[0].resolution.status, "exact");
        assert_eq!(result.entries[2].resolution.status, "exact");
    }

    #[test]
    fn malformed_entry_does_not_abort_valid_neighbors() {
        let result = resolve_artifact_ref_list(
            &["plans:missing.md", "broken", "bead:sase-bb"],
            &ArtifactRefContextWire::default(),
        )
        .unwrap();
        assert_eq!(result.entries.len(), 3);
        assert_eq!(result.entries[0].resolution.status, "unknown_kind");
        assert_eq!(result.entries[1].resolution.status, "unknown_kind");
        assert_eq!(result.entries[1].rendered, "broken");
        assert_eq!(result.entries[2].resolution.status, "missing");
    }
}
