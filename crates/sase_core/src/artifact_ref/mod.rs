//! Kind-tagged logical references to SASE artifacts.

mod scanner;
mod wire;

use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::reference_path::{
    path_to_relative_payload, resolve_ordered_root_file,
    validate_relative_payload, DriftPolicy, PathPayloadError,
    RelativePayloadError,
};

pub use scanner::scan_artifact_refs;
pub use wire::{
    ArtifactFileSourceWire, ArtifactRefContextWire,
    ArtifactRefDocumentRootWire, ArtifactRefError, ArtifactRefFragmentWire,
    ArtifactRefKindWire, ArtifactRefPayloadWire, ArtifactRefProjectWire,
    ArtifactRefPromptCandidateWire, ArtifactRefRepositoryWire,
    ArtifactRefResolutionWire, ArtifactRefSpanWire, ParsedArtifactRefWire,
    ARTIFACT_REF_PARSE_WIRE_SCHEMA_VERSION,
    ARTIFACT_REF_RESOLUTION_WIRE_SCHEMA_VERSION,
};

const ARTIFACT_FILE_INDEX_SCHEMA_VERSION: u64 = 1;

/// Parse and validate one canonical `<kind>:<payload>[#<fragment>]` value.
pub fn parse_artifact_ref(
    value: &str,
) -> Result<ParsedArtifactRefWire, ArtifactRefError> {
    if value.starts_with('@') {
        return Err(ArtifactRefError::validation(
            "artifact reference must not include the prompt '@' sigil",
        ));
    }
    let (kind_label, raw_payload) = value.split_once(':').ok_or_else(|| {
        ArtifactRefError::validation(
            "artifact reference must contain a kind separator ':'",
        )
    })?;
    validate_kind(kind_label)?;
    let kind = classify_kind(kind_label);

    let (payload_text, fragment_text) = if kind_label == "bug" {
        (raw_payload, None)
    } else {
        match raw_payload.split_once('#') {
            Some((payload, fragment)) => (payload, Some(fragment)),
            None => (raw_payload, None),
        }
    };
    let payload = parse_payload(&kind, payload_text)?;
    let fragment = match fragment_text {
        Some(fragment) => {
            if matches!(
                kind,
                ArtifactRefKindWire::Commit | ArtifactRefKindWire::Bug
            ) {
                return Err(ArtifactRefError::validation(format!(
                    "{} references do not support fragments",
                    kind.label()
                )));
            }
            Some(parse_fragment(fragment)?)
        }
        None => None,
    };
    let mut parsed = ParsedArtifactRefWire {
        schema_version: ARTIFACT_REF_PARSE_WIRE_SCHEMA_VERSION,
        kind,
        payload,
        fragment,
        rendered: String::new(),
    };
    parsed.rendered = render_artifact_ref(&parsed)?;
    Ok(parsed)
}

/// Render one typed artifact reference deterministically.
pub fn render_artifact_ref(
    reference: &ParsedArtifactRefWire,
) -> Result<String, ArtifactRefError> {
    let kind = reference.kind.label();
    validate_kind(kind)?;
    let payload = match (&reference.kind, &reference.payload) {
        (
            ArtifactRefKindWire::Commit,
            ArtifactRefPayloadWire::Commit { repo, sha },
        ) => {
            validate_repo(repo)?;
            validate_sha(sha, false)?;
            format!("{repo}@{sha}")
        }
        (ArtifactRefKindWire::Chat, ArtifactRefPayloadWire::Chat { path }) => {
            validate_path_payload("chat", path)?;
            path.clone()
        }
        (
            ArtifactRefKindWire::Bug,
            ArtifactRefPayloadWire::Bug { project, number },
        ) => {
            validate_project(project)?;
            if *number == 0 {
                return Err(ArtifactRefError::validation(
                    "bug number must be positive",
                ));
            }
            format!("{project}#{number}")
        }
        (
            ArtifactRefKindWire::File,
            ArtifactRefPayloadWire::File { source, digest },
        ) => {
            validate_digest(digest)?;
            format!("{}:{digest}", source.label())
        }
        (
            ArtifactRefKindWire::Document { .. },
            ArtifactRefPayloadWire::Document { path },
        ) => {
            validate_path_payload("document", path)?;
            path.clone()
        }
        _ => {
            return Err(ArtifactRefError::validation(format!(
                "artifact reference kind '{kind}' does not match its payload"
            )));
        }
    };

    let fragment = match &reference.fragment {
        Some(fragment) => {
            if matches!(
                reference.kind,
                ArtifactRefKindWire::Commit | ArtifactRefKindWire::Bug
            ) {
                return Err(ArtifactRefError::validation(format!(
                    "{kind} references do not support fragments"
                )));
            }
            format!("#{}", render_fragment(fragment)?)
        }
        None => String::new(),
    };
    Ok(format!("{kind}:{payload}{fragment}"))
}

/// Convert an absolute local path into the first matching logical reference.
pub fn canonicalize_artifact_ref(
    path: &Path,
    context: &ArtifactRefContextWire,
) -> Result<Option<String>, ArtifactRefError> {
    if !path.is_absolute() {
        return Err(ArtifactRefError::validation(
            "artifact path must be absolute",
        ));
    }
    for document in &context.document_roots {
        validate_kind(&document.kind)?;
        let root = Path::new(&document.root);
        if let Ok(relative) = path.strip_prefix(root) {
            let payload = artifact_path_payload(relative)?;
            let reference = ParsedArtifactRefWire {
                schema_version: ARTIFACT_REF_PARSE_WIRE_SCHEMA_VERSION,
                kind: ArtifactRefKindWire::Document {
                    role: document.kind.clone(),
                },
                payload: ArtifactRefPayloadWire::Document { path: payload },
                fragment: None,
                rendered: String::new(),
            };
            return render_artifact_ref(&reference).map(Some);
        }
    }
    if let Some(root) = &context.chats_root {
        if let Ok(relative) = path.strip_prefix(root) {
            let payload = artifact_path_payload(relative)?;
            let reference = ParsedArtifactRefWire {
                schema_version: ARTIFACT_REF_PARSE_WIRE_SCHEMA_VERSION,
                kind: ArtifactRefKindWire::Chat,
                payload: ArtifactRefPayloadWire::Chat { path: payload },
                fragment: None,
                rendered: String::new(),
            };
            return render_artifact_ref(&reference).map(Some);
        }
    }
    if let Some(index_path) = &context.artifact_index_path {
        for artifact in read_artifact_index(Path::new(index_path))? {
            if Path::new(&artifact.path) == path {
                return parse_artifact_ref(&format!("file:{}", artifact.id))
                    .map(|reference| Some(reference.rendered));
            }
        }
    }
    Ok(None)
}

/// Resolve a typed reference using only caller-supplied local context.
pub fn resolve_artifact_ref(
    reference: &ParsedArtifactRefWire,
    context: &ArtifactRefContextWire,
) -> Result<ArtifactRefResolutionWire, ArtifactRefError> {
    let rendered = render_artifact_ref(reference)?;
    match (&reference.kind, &reference.payload) {
        (
            ArtifactRefKindWire::Document { role },
            ArtifactRefPayloadWire::Document { path },
        ) => resolve_document(role, path, rendered, context),
        (ArtifactRefKindWire::Chat, ArtifactRefPayloadWire::Chat { path }) => {
            let roots = context
                .chats_root
                .iter()
                .map(PathBuf::from)
                .collect::<Vec<_>>();
            resolve_path_reference(path, rendered, &roots)
        }
        (
            ArtifactRefKindWire::File,
            ArtifactRefPayloadWire::File { source, digest },
        ) => resolve_file(*source, digest, rendered, context),
        (
            ArtifactRefKindWire::Commit,
            ArtifactRefPayloadWire::Commit { repo, sha },
        ) => resolve_commit(repo, sha, rendered, context),
        (
            ArtifactRefKindWire::Bug,
            ArtifactRefPayloadWire::Bug { project, number },
        ) => resolve_bug(project, *number, rendered, context),
        _ => Err(ArtifactRefError::validation(
            "artifact reference kind does not match its payload",
        )),
    }
}

fn classify_kind(kind: &str) -> ArtifactRefKindWire {
    match kind {
        "commit" => ArtifactRefKindWire::Commit,
        "chat" => ArtifactRefKindWire::Chat,
        "bug" => ArtifactRefKindWire::Bug,
        "file" => ArtifactRefKindWire::File,
        role => ArtifactRefKindWire::Document {
            role: role.to_string(),
        },
    }
}

fn parse_payload(
    kind: &ArtifactRefKindWire,
    payload: &str,
) -> Result<ArtifactRefPayloadWire, ArtifactRefError> {
    match kind {
        ArtifactRefKindWire::Commit => {
            let (repo, sha) = payload.rsplit_once('@').ok_or_else(|| {
                ArtifactRefError::validation(
                    "commit payload must have the form <repo>@<sha>",
                )
            })?;
            validate_repo(repo)?;
            validate_sha(sha, false)?;
            Ok(ArtifactRefPayloadWire::Commit {
                repo: repo.to_string(),
                sha: sha.to_string(),
            })
        }
        ArtifactRefKindWire::Chat => {
            validate_path_payload("chat", payload)?;
            Ok(ArtifactRefPayloadWire::Chat {
                path: payload.to_string(),
            })
        }
        ArtifactRefKindWire::Bug => {
            let (project, raw_number) =
                payload.split_once('#').ok_or_else(|| {
                    ArtifactRefError::validation(
                        "bug payload must have the form <project>#<number>",
                    )
                })?;
            if raw_number.contains('#') {
                return Err(ArtifactRefError::validation(
                    "bug payload must contain exactly one issue separator '#'",
                ));
            }
            validate_project(project)?;
            if raw_number.is_empty()
                || !raw_number.bytes().all(|byte| byte.is_ascii_digit())
            {
                return Err(ArtifactRefError::validation(
                    "bug number must be a positive decimal number",
                ));
            }
            let number = raw_number.parse::<u64>().map_err(|_| {
                ArtifactRefError::validation(
                    "bug number must be a positive decimal number",
                )
            })?;
            if number == 0 {
                return Err(ArtifactRefError::validation(
                    "bug number must be positive",
                ));
            }
            Ok(ArtifactRefPayloadWire::Bug {
                project: project.to_string(),
                number,
            })
        }
        ArtifactRefKindWire::File => {
            let (source, digest) =
                payload.split_once(':').ok_or_else(|| {
                    ArtifactRefError::validation(
                        "file payload must have the form (explicit|default):<hex24>",
                    )
                })?;
            let source = match source {
                "explicit" => ArtifactFileSourceWire::Explicit,
                "default" => ArtifactFileSourceWire::Default,
                _ => {
                    return Err(ArtifactRefError::validation(
                        "file source must be 'explicit' or 'default'",
                    ));
                }
            };
            validate_digest(digest)?;
            Ok(ArtifactRefPayloadWire::File {
                source,
                digest: digest.to_string(),
            })
        }
        ArtifactRefKindWire::Document { .. } => {
            validate_path_payload("document", payload)?;
            Ok(ArtifactRefPayloadWire::Document {
                path: payload.to_string(),
            })
        }
    }
}

fn parse_fragment(
    fragment: &str,
) -> Result<ArtifactRefFragmentWire, ArtifactRefError> {
    if let Some(lines) = fragment.strip_prefix('L') {
        let (start, end) = match lines.split_once("-L") {
            Some((start, end)) => {
                (parse_positive(start, "line")?, parse_positive(end, "line")?)
            }
            None => {
                let line = parse_positive(lines, "line")?;
                (line, line)
            }
        };
        if end < start {
            return Err(ArtifactRefError::validation(
                "line fragment end must not precede its start",
            ));
        }
        return Ok(ArtifactRefFragmentWire::Lines { start, end });
    }
    if let Some(page) = fragment.strip_prefix("page=") {
        return Ok(ArtifactRefFragmentWire::Page {
            page: parse_positive(page, "page")?,
        });
    }
    if let Some(seconds) = fragment.strip_prefix("t=") {
        return Ok(ArtifactRefFragmentWire::Time {
            seconds: parse_nonnegative(seconds, "timestamp")?,
        });
    }
    Err(ArtifactRefError::validation(
        "unsupported artifact reference fragment",
    ))
}

fn render_fragment(
    fragment: &ArtifactRefFragmentWire,
) -> Result<String, ArtifactRefError> {
    match fragment {
        ArtifactRefFragmentWire::Lines { start, end } => {
            if *start == 0 || *end == 0 {
                return Err(ArtifactRefError::validation(
                    "line numbers must be positive",
                ));
            }
            if end < start {
                return Err(ArtifactRefError::validation(
                    "line fragment end must not precede its start",
                ));
            }
            if start == end {
                Ok(format!("L{start}"))
            } else {
                Ok(format!("L{start}-L{end}"))
            }
        }
        ArtifactRefFragmentWire::Page { page } => {
            if *page == 0 {
                return Err(ArtifactRefError::validation(
                    "page number must be positive",
                ));
            }
            Ok(format!("page={page}"))
        }
        ArtifactRefFragmentWire::Time { seconds } => Ok(format!("t={seconds}")),
    }
}

fn resolve_document(
    role: &str,
    path: &str,
    rendered: String,
    context: &ArtifactRefContextWire,
) -> Result<ArtifactRefResolutionWire, ArtifactRefError> {
    let matching = context
        .document_roots
        .iter()
        .filter(|document| document.kind == role)
        .collect::<Vec<_>>();
    if matching.is_empty() {
        return Ok(resolution("unknown_kind", rendered));
    }
    let roots = matching
        .into_iter()
        .map(|document| PathBuf::from(&document.root))
        .collect::<Vec<_>>();
    resolve_path_reference(path, rendered, &roots)
}

fn resolve_path_reference(
    path: &str,
    rendered: String,
    roots: &[PathBuf],
) -> Result<ArtifactRefResolutionWire, ArtifactRefError> {
    validate_path_payload("artifact", path)?;
    let outcome = resolve_ordered_root_file(
        Path::new(path),
        roots,
        Vec::new(),
        DriftPolicy::AnyChildBasename,
    );
    Ok(ArtifactRefResolutionWire {
        schema_version: ARTIFACT_REF_RESOLUTION_WIRE_SCHEMA_VERSION,
        status: outcome.status.as_str().to_string(),
        rendered,
        locator: None,
        resolved_path: outcome
            .resolved_path
            .map(|path| path.to_string_lossy().into_owned()),
        candidates: paths_to_strings(outcome.candidates),
    })
}

fn resolve_file(
    source: ArtifactFileSourceWire,
    digest: &str,
    rendered: String,
    context: &ArtifactRefContextWire,
) -> Result<ArtifactRefResolutionWire, ArtifactRefError> {
    let Some(index_path) = &context.artifact_index_path else {
        return Ok(resolution("missing", rendered));
    };
    let id = format!("{}:{digest}", source.label());
    let candidates = read_artifact_index(Path::new(index_path))?
        .into_iter()
        .filter(|artifact| artifact.id == id)
        .map(|artifact| artifact.path)
        .collect::<Vec<_>>();
    match candidates.as_slice() {
        [path] => Ok(ArtifactRefResolutionWire {
            schema_version: ARTIFACT_REF_RESOLUTION_WIRE_SCHEMA_VERSION,
            status: "exact".to_string(),
            rendered,
            locator: None,
            resolved_path: Some(path.clone()),
            candidates,
        }),
        [] => Ok(resolution("missing", rendered)),
        _ => Ok(ArtifactRefResolutionWire {
            schema_version: ARTIFACT_REF_RESOLUTION_WIRE_SCHEMA_VERSION,
            status: "ambiguous".to_string(),
            rendered,
            locator: None,
            resolved_path: None,
            candidates,
        }),
    }
}

fn resolve_commit(
    repo: &str,
    sha: &str,
    rendered: String,
    context: &ArtifactRefContextWire,
) -> Result<ArtifactRefResolutionWire, ArtifactRefError> {
    let Some(repository) = context.repositories.iter().find(|repository| {
        repository.name == repo
            || repository.aliases.iter().any(|alias| alias == repo)
    }) else {
        return Ok(resolution("unknown_repo", rendered));
    };
    let mut matches = Vec::new();
    for canonical_sha in &repository.shas {
        validate_sha(canonical_sha, true)?;
        if canonical_sha.starts_with(sha) {
            matches.push(canonical_sha);
        }
    }
    if matches.len() == 1 {
        let locator = format!("{}@{}", repository.name, matches[0]);
        return Ok(ArtifactRefResolutionWire {
            schema_version: ARTIFACT_REF_RESOLUTION_WIRE_SCHEMA_VERSION,
            status: "exact".to_string(),
            rendered: format!("commit:{locator}"),
            locator: Some(locator.clone()),
            resolved_path: None,
            candidates: vec![locator],
        });
    }
    if matches.len() > 1 {
        return Ok(ArtifactRefResolutionWire {
            schema_version: ARTIFACT_REF_RESOLUTION_WIRE_SCHEMA_VERSION,
            status: "ambiguous".to_string(),
            rendered,
            locator: None,
            resolved_path: None,
            candidates: matches
                .into_iter()
                .map(|sha| format!("{}@{sha}", repository.name))
                .collect(),
        });
    }
    Ok(resolution("missing", rendered))
}

fn resolve_bug(
    project: &str,
    number: u64,
    rendered: String,
    context: &ArtifactRefContextWire,
) -> Result<ArtifactRefResolutionWire, ArtifactRefError> {
    let Some(project_record) = context.projects.iter().find(|candidate| {
        candidate.name == project
            || (!candidate.key.is_empty() && candidate.key == project)
            || candidate.aliases.iter().any(|alias| alias == project)
    }) else {
        return Ok(resolution("unknown_project", rendered));
    };
    validate_project(&project_record.name)?;
    let locator = format!("{}#{number}", project_record.name);
    Ok(ArtifactRefResolutionWire {
        schema_version: ARTIFACT_REF_RESOLUTION_WIRE_SCHEMA_VERSION,
        status: "exact".to_string(),
        rendered: format!("bug:{locator}"),
        locator: Some(locator.clone()),
        resolved_path: None,
        candidates: vec![locator],
    })
}

fn resolution(status: &str, rendered: String) -> ArtifactRefResolutionWire {
    ArtifactRefResolutionWire {
        schema_version: ARTIFACT_REF_RESOLUTION_WIRE_SCHEMA_VERSION,
        status: status.to_string(),
        rendered,
        locator: None,
        resolved_path: None,
        candidates: Vec::new(),
    }
}

fn validate_kind(kind: &str) -> Result<(), ArtifactRefError> {
    let mut bytes = kind.bytes();
    if !bytes.next().is_some_and(|byte| byte.is_ascii_lowercase())
        || !bytes.all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'_' | b'-')
        })
    {
        return Err(ArtifactRefError::validation(
            "artifact reference kind must match [a-z][a-z0-9_-]*",
        ));
    }
    Ok(())
}

fn validate_path_payload(
    label: &str,
    path: &str,
) -> Result<(), ArtifactRefError> {
    validate_relative_payload(path).map_err(|error| {
        ArtifactRefError::validation(match error {
            RelativePayloadError::Empty => {
                format!("{label} path must not be empty")
            }
            RelativePayloadError::Absolute => {
                format!("{label} path must be relative")
            }
            RelativePayloadError::Backslash => {
                format!("{label} path must use forward slashes")
            }
            RelativePayloadError::EmptySegment => {
                format!("{label} path contains an empty segment")
            }
            RelativePayloadError::ParentSegment => {
                format!("{label} path must not contain '..'")
            }
        })
    })
}

fn artifact_path_payload(path: &Path) -> Result<String, ArtifactRefError> {
    let payload = path_to_relative_payload(path).map_err(|error| {
        ArtifactRefError::validation(match error {
            PathPayloadError::InvalidUtf8 => {
                "artifact path must contain valid UTF-8"
            }
            PathPayloadError::EscapesRoot => {
                "artifact path escapes its configured root"
            }
        })
    })?;
    validate_path_payload("artifact", &payload)?;
    Ok(payload)
}

fn validate_repo(repo: &str) -> Result<(), ArtifactRefError> {
    validate_locator_label(repo, "repository")
}

fn validate_project(project: &str) -> Result<(), ArtifactRefError> {
    validate_locator_label(project, "project")
}

fn validate_locator_label(
    value: &str,
    label: &str,
) -> Result<(), ArtifactRefError> {
    let mut bytes = value.bytes();
    if !bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphanumeric())
        || !bytes.all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')
        })
    {
        return Err(ArtifactRefError::validation(format!(
            "{label} name must be path-safe"
        )));
    }
    Ok(())
}

fn validate_sha(sha: &str, full: bool) -> Result<(), ArtifactRefError> {
    let valid_length = if full {
        sha.len() == 40
    } else {
        (7..=40).contains(&sha.len())
    };
    if !valid_length
        || !sha
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        let length = if full { "40" } else { "7 to 40" };
        return Err(ArtifactRefError::validation(format!(
            "commit SHA must contain {length} lowercase hexadecimal characters"
        )));
    }
    Ok(())
}

fn validate_digest(digest: &str) -> Result<(), ArtifactRefError> {
    if digest.len() != 24
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(ArtifactRefError::validation(
            "artifact file digest must contain 24 lowercase hexadecimal characters",
        ));
    }
    Ok(())
}

fn parse_positive(value: &str, label: &str) -> Result<u64, ArtifactRefError> {
    let number = parse_nonnegative(value, label)?;
    if number == 0 {
        return Err(ArtifactRefError::validation(format!(
            "{label} number must be positive"
        )));
    }
    Ok(number)
}

fn parse_nonnegative(
    value: &str,
    label: &str,
) -> Result<u64, ArtifactRefError> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(ArtifactRefError::validation(format!(
            "{label} must be a decimal number"
        )));
    }
    value.parse::<u64>().map_err(|_| {
        ArtifactRefError::validation(format!("{label} is out of range"))
    })
}

fn paths_to_strings(paths: Vec<PathBuf>) -> Vec<String> {
    paths
        .into_iter()
        .map(|path| path.to_string_lossy().into_owned())
        .collect()
}

#[derive(Debug, Deserialize)]
struct ArtifactIndexEnvelope {
    schema_version: u64,
    artifact: ArtifactIndexRow,
}

#[derive(Debug, Deserialize)]
struct ArtifactIndexRow {
    id: String,
    path: String,
}

fn read_artifact_index(
    path: &Path,
) -> Result<Vec<ArtifactIndexRow>, ArtifactRefError> {
    let content = match fs::read_to_string(path) {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Vec::new());
        }
        Err(error) => return Err(error.into()),
    };
    Ok(content
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() {
                return None;
            }
            serde_json::from_str::<ArtifactIndexEnvelope>(line)
                .ok()
                .filter(|envelope| {
                    envelope.schema_version
                        == ARTIFACT_FILE_INDEX_SCHEMA_VERSION
                })
                .map(|envelope| envelope.artifact)
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::*;

    const FULL_SHA: &str = "0123456789abcdef0123456789abcdef01234567";

    #[test]
    fn every_kind_and_fragment_round_trips() {
        for (value, expected) in [
            ("plans:202607/plan.md", "plans:202607/plan.md"),
            ("designs:guide.md#L12", "designs:guide.md#L12"),
            ("chat:202607/main.md#L12-L18", "chat:202607/main.md#L12-L18"),
            ("chat:202607/main.md#page=2", "chat:202607/main.md#page=2"),
            (
                "file:default:52895d68931185056fd0e49f#t=90",
                "file:default:52895d68931185056fd0e49f#t=90",
            ),
            (
                "commit:sase@0123456789abcdef0123456789abcdef01234567",
                "commit:sase@0123456789abcdef0123456789abcdef01234567",
            ),
            ("bug:sase#123", "bug:sase#123"),
        ] {
            let parsed = parse_artifact_ref(value).unwrap();
            assert_eq!(parsed.rendered, expected);
            assert_eq!(render_artifact_ref(&parsed).unwrap(), expected);
        }
    }

    #[test]
    fn parse_rejects_invalid_shapes_and_illegal_fragments() {
        for value in [
            "Plans:x.md",
            "plans:",
            "plans:/tmp/x.md",
            "plans:a//x.md",
            "plans:a/../x.md",
            r"plans:a\x.md",
            "commit:sase@ABCDEF0",
            "commit:sase@012345",
            "commit:sase/other@0123456",
            "commit:sase@0123456#L1",
            "bug:sase#0",
            "bug:sase#nope",
            "bug:sase#1#L2",
            "file:other:52895d68931185056fd0e49f",
            "file:default:52895d68931185056fd0e49",
            "plans:x.md#L0",
            "plans:x.md#L3-L2",
            "plans:x.md#page=0",
            "plans:x.md#t=nope",
        ] {
            assert!(parse_artifact_ref(value).is_err(), "{value}");
        }
    }

    #[test]
    fn canonicalization_uses_order_and_artifact_index() {
        let temp = tempdir().unwrap();
        let first = temp.path().join("first");
        let nested = first.join("nested");
        let chat = temp.path().join("chats");
        let indexed = temp.path().join("stored.png");
        fs::create_dir_all(&nested).unwrap();
        fs::create_dir_all(&chat).unwrap();
        fs::write(&indexed, "png").unwrap();
        let index = temp.path().join("index.jsonl");
        fs::write(
            &index,
            format!(
                "{{\"schema_version\":1,\"artifact\":{{\"id\":\"explicit:52895d68931185056fd0e49f\",\"path\":{}}}}}\n",
                serde_json::to_string(indexed.to_str().unwrap()).unwrap()
            ),
        )
        .unwrap();
        let context = ArtifactRefContextWire {
            document_roots: vec![
                ArtifactRefDocumentRootWire {
                    kind: "plans".to_string(),
                    root: first.to_string_lossy().into_owned(),
                },
                ArtifactRefDocumentRootWire {
                    kind: "designs".to_string(),
                    root: nested.to_string_lossy().into_owned(),
                },
            ],
            chats_root: Some(chat.to_string_lossy().into_owned()),
            artifact_index_path: Some(index.to_string_lossy().into_owned()),
            ..Default::default()
        };
        assert_eq!(
            canonicalize_artifact_ref(&nested.join("guide.md"), &context)
                .unwrap()
                .as_deref(),
            Some("plans:nested/guide.md")
        );
        assert_eq!(
            canonicalize_artifact_ref(&chat.join("main.md"), &context)
                .unwrap()
                .as_deref(),
            Some("chat:main.md")
        );
        assert_eq!(
            canonicalize_artifact_ref(&indexed, &context)
                .unwrap()
                .as_deref(),
            Some("file:explicit:52895d68931185056fd0e49f")
        );
        assert_eq!(
            canonicalize_artifact_ref(
                &temp.path().join("outside.md"),
                &context
            )
            .unwrap(),
            None
        );
    }

    #[test]
    fn path_resolution_covers_order_drift_ambiguity_and_missing() {
        let temp = tempdir().unwrap();
        let first = temp.path().join("first");
        let second = temp.path().join("second");
        for root in [&first, &second] {
            fs::create_dir_all(root.join("202607")).unwrap();
        }
        fs::write(first.join("202607/exact.md"), "one").unwrap();
        fs::write(second.join("202607/exact.md"), "two").unwrap();
        fs::write(first.join("202607/drift.md"), "one").unwrap();
        let context = ArtifactRefContextWire {
            document_roots: vec![
                ArtifactRefDocumentRootWire {
                    kind: "plans".to_string(),
                    root: first.to_string_lossy().into_owned(),
                },
                ArtifactRefDocumentRootWire {
                    kind: "plans".to_string(),
                    root: second.to_string_lossy().into_owned(),
                },
            ],
            ..Default::default()
        };

        let exact = resolve_artifact_ref(
            &parse_artifact_ref("plans:202607/exact.md").unwrap(),
            &context,
        )
        .unwrap();
        assert_eq!(exact.status, "exact");
        assert_eq!(
            exact.resolved_path.as_deref(),
            Some(first.join("202607/exact.md").to_str().unwrap())
        );
        assert_eq!(exact.candidates.len(), 1);

        let drifted = resolve_artifact_ref(
            &parse_artifact_ref("plans:202606/drift.md").unwrap(),
            &context,
        )
        .unwrap();
        assert_eq!(drifted.status, "drifted");

        fs::write(second.join("202607/drift.md"), "two").unwrap();
        let ambiguous = resolve_artifact_ref(
            &parse_artifact_ref("plans:202606/drift.md").unwrap(),
            &context,
        )
        .unwrap();
        assert_eq!(ambiguous.status, "ambiguous");
        assert_eq!(ambiguous.candidates.len(), 2);

        let missing = resolve_artifact_ref(
            &parse_artifact_ref("plans:202607/missing.md").unwrap(),
            &context,
        )
        .unwrap();
        assert_eq!(missing.status, "missing");
        assert_eq!(missing.candidates.len(), 2);
    }

    #[test]
    fn namespace_resolution_is_canonical_and_local() {
        let context = ArtifactRefContextWire {
            repositories: vec![ArtifactRefRepositoryWire {
                name: "sase".to_string(),
                aliases: vec!["core".to_string()],
                shas: vec![FULL_SHA.to_string()],
            }],
            projects: vec![ArtifactRefProjectWire {
                name: "sase".to_string(),
                key: "gh_sase-org__sase".to_string(),
                aliases: Vec::new(),
            }],
            ..Default::default()
        };
        let commit = resolve_artifact_ref(
            &parse_artifact_ref("commit:core@0123456").unwrap(),
            &context,
        )
        .unwrap();
        assert_eq!(commit.status, "exact");
        assert_eq!(commit.rendered, format!("commit:sase@{FULL_SHA}"));
        assert_eq!(commit.locator, Some(format!("sase@{FULL_SHA}")));

        let bug = resolve_artifact_ref(
            &parse_artifact_ref("bug:gh_sase-org__sase#123").unwrap(),
            &context,
        )
        .unwrap();
        assert_eq!(bug.status, "exact");
        assert_eq!(bug.rendered, "bug:sase#123");

        assert_eq!(
            resolve_artifact_ref(
                &parse_artifact_ref("documents:x.md").unwrap(),
                &context
            )
            .unwrap()
            .status,
            "unknown_kind"
        );
        assert_eq!(
            resolve_artifact_ref(
                &parse_artifact_ref("commit:other@0123456").unwrap(),
                &context
            )
            .unwrap()
            .status,
            "unknown_repo"
        );
        assert_eq!(
            resolve_artifact_ref(
                &parse_artifact_ref("bug:other#1").unwrap(),
                &context
            )
            .unwrap()
            .status,
            "unknown_project"
        );
    }

    #[test]
    fn indexed_file_resolution_uses_schema_one_envelopes() {
        let temp = tempdir().unwrap();
        let target = temp.path().join("image.png");
        let index = temp.path().join("index.jsonl");
        fs::write(
            &index,
            format!(
                "{{\"schema_version\":2,\"artifact\":{{\"id\":\"default:52895d68931185056fd0e49f\",\"path\":\"ignored\"}}}}\n\
                 {{\"schema_version\":1,\"artifact\":{{\"id\":\"default:52895d68931185056fd0e49f\",\"path\":{}}}}}\n",
                serde_json::to_string(target.to_str().unwrap()).unwrap()
            ),
        )
        .unwrap();
        let context = ArtifactRefContextWire {
            artifact_index_path: Some(index.to_string_lossy().into_owned()),
            ..Default::default()
        };
        let result = resolve_artifact_ref(
            &parse_artifact_ref("file:default:52895d68931185056fd0e49f#page=2")
                .unwrap(),
            &context,
        )
        .unwrap();
        assert_eq!(result.status, "exact");
        assert_eq!(
            result.resolved_path.as_deref(),
            Some(target.to_str().unwrap())
        );
    }

    #[test]
    fn scanner_reports_utf8_byte_spans_and_malformed_candidates() {
        let text = "é @plans:one.md, and `@chat:202607/two.md#L2` \
                    @commit:sase@bad @plans:three.md: @plans:x@y:z.md";
        let candidates = scan_artifact_refs(text);
        assert_eq!(candidates.len(), 5);
        assert_eq!(
            &text[candidates[0].candidate_span.start
                ..candidates[0].candidate_span.end],
            "@plans:one.md"
        );
        assert_eq!(candidates[0].candidate_span.start, 3);
        assert_eq!(
            &text[candidates[1].candidate_span.start
                ..candidates[1].candidate_span.end],
            "@chat:202607/two.md#L2"
        );
        assert!(candidates[1].well_formed);
        assert!(candidates[1].fragment_span.is_some());
        assert!(!candidates[2].well_formed);
        assert_eq!(candidates[3].text, "@plans:three.md");
        assert!(candidates[4].well_formed);
    }

    #[test]
    fn scanner_enforces_left_context_but_scans_fences() {
        let text = "@plans:first.md x@plans:no.md\n```\n@plans:fenced.md\n```";
        let candidates = scan_artifact_refs(text);
        assert_eq!(
            candidates
                .iter()
                .map(|candidate| candidate.text.as_str())
                .collect::<Vec<_>>(),
            ["@plans:first.md", "@plans:fenced.md"]
        );
    }
}
