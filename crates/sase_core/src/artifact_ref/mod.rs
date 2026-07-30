//! Kind-tagged logical references to SASE artifacts.

mod scanner;
mod wire;

use std::path::{Path, PathBuf};

use crate::agent_identity::{
    globalize_agent_name, validate_agent_reference_name, AgentOwnerIdentity,
};
use crate::artifact_file::read_artifact_file_index;
use crate::reference_path::{
    path_to_relative_payload, resolve_ordered_root_file,
    validate_relative_payload, DriftPolicy, PathPayloadError,
    RelativePayloadError,
};

pub use scanner::scan_artifact_refs;
pub use wire::{
    ArtifactFileSourceWire, ArtifactRefAgentOwnerWire,
    ArtifactRefAgentRootWire, ArtifactRefBeadStoreWire, ArtifactRefContextWire,
    ArtifactRefDocumentRootWire, ArtifactRefError, ArtifactRefFragmentWire,
    ArtifactRefKindWire, ArtifactRefPayloadWire, ArtifactRefProjectWire,
    ArtifactRefPromptCandidateWire, ArtifactRefRepositoryWire,
    ArtifactRefResolutionWire, ArtifactRefSpanWire, ParsedArtifactRefWire,
    ARTIFACT_REF_PARSE_WIRE_SCHEMA_VERSION,
    ARTIFACT_REF_RESOLUTION_WIRE_SCHEMA_VERSION,
};

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
            if kind_rejects_fragments(&kind) {
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
        (ArtifactRefKindWire::Bead, ArtifactRefPayloadWire::Bead { id }) => {
            validate_bead_id(id)?;
            id.clone()
        }
        (
            ArtifactRefKindWire::Agent,
            ArtifactRefPayloadWire::Agent { name },
        ) => {
            validate_agent_payload_name(name)?;
            name.clone()
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
            if kind_rejects_fragments(&reference.kind) {
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
    for store in &context.bead_stores {
        if let Some(id) =
            canonical_bead_id(path, &Path::new(&store.root).join("pages"))
        {
            return parse_artifact_ref(&format!("bead:{id}"))
                .map(|reference| Some(reference.rendered));
        }
    }
    for agent_root in &context.agent_roots {
        if let Some(name) = canonical_agent_name(
            path,
            &Path::new(&agent_root.root).join("agents"),
        ) {
            return parse_artifact_ref(&format!("agent:{name}"))
                .map(|reference| Some(reference.rendered));
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
        (ArtifactRefKindWire::Bead, ArtifactRefPayloadWire::Bead { id }) => {
            resolve_bead(id, rendered, context)
        }
        (
            ArtifactRefKindWire::Agent,
            ArtifactRefPayloadWire::Agent { name },
        ) => resolve_agent(name, rendered, context),
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
        "bead" => ArtifactRefKindWire::Bead,
        "agent" => ArtifactRefKindWire::Agent,
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
        ArtifactRefKindWire::Bead => {
            validate_bead_id(payload)?;
            Ok(ArtifactRefPayloadWire::Bead {
                id: payload.to_string(),
            })
        }
        ArtifactRefKindWire::Agent => {
            validate_agent_payload_name(payload)?;
            Ok(ArtifactRefPayloadWire::Agent {
                name: payload.to_string(),
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

/// Resolve one bead through its generated page address without reading the
/// bead store.
fn resolve_bead(
    id: &str,
    rendered: String,
    context: &ArtifactRefContextWire,
) -> Result<ArtifactRefResolutionWire, ArtifactRefError> {
    if context.bead_stores.is_empty() {
        return Ok(resolution("missing", rendered));
    }
    let prefix = bead_lineage_root(id)
        .rsplit_once('-')
        .map(|(prefix, _)| prefix);
    let stores = context
        .bead_stores
        .iter()
        .filter(|store| match prefix {
            Some(prefix) => store.prefix == prefix,
            None => true,
        })
        .collect::<Vec<_>>();
    if prefix.is_some() && stores.is_empty() {
        return Ok(resolution("unknown_project", rendered));
    }

    let candidates = stores
        .iter()
        .map(|store| Path::new(&store.root).join(bead_page_path(id)))
        .collect::<Vec<_>>();
    let hits = stores
        .iter()
        .zip(&candidates)
        .filter(|(_, path)| path.is_file())
        .collect::<Vec<_>>();
    match hits.as_slice() {
        [(store, path)] => {
            let path = path.to_string_lossy().into_owned();
            Ok(ArtifactRefResolutionWire {
                schema_version: ARTIFACT_REF_RESOLUTION_WIRE_SCHEMA_VERSION,
                status: "exact".to_string(),
                rendered,
                locator: Some(format!("{}/{}", store.project, id)),
                resolved_path: Some(path.clone()),
                candidates: vec![path],
            })
        }
        [] => Ok(ArtifactRefResolutionWire {
            schema_version: ARTIFACT_REF_RESOLUTION_WIRE_SCHEMA_VERSION,
            status: "missing".to_string(),
            rendered,
            locator: None,
            resolved_path: None,
            candidates: paths_to_strings(candidates),
        }),
        _ => Ok(ArtifactRefResolutionWire {
            schema_version: ARTIFACT_REF_RESOLUTION_WIRE_SCHEMA_VERSION,
            status: "ambiguous".to_string(),
            rendered,
            locator: None,
            resolved_path: None,
            candidates: hits
                .into_iter()
                .map(|(_, path)| path.to_string_lossy().into_owned())
                .collect(),
        }),
    }
}

fn resolve_agent(
    name: &str,
    rendered: String,
    context: &ArtifactRefContextWire,
) -> Result<ArtifactRefResolutionWire, ArtifactRefError> {
    if context.agent_roots.is_empty() {
        return Ok(resolution("missing", rendered));
    }
    let mut names = vec![name.to_string()];
    if let Some(owner) = &context.agent_owner {
        let owner =
            AgentOwnerIdentity::new(&owner.username, &owner.machine_name)
                .map_err(|error| {
                    ArtifactRefError::validation(format!(
                        "invalid agent owner: {error}"
                    ))
                })?;
        let global = globalize_agent_name(name, &owner).map_err(|error| {
            ArtifactRefError::validation(format!(
                "could not globalize agent name: {error}"
            ))
        })?;
        if global != name {
            names.push(global);
        }
    }

    let mut attempted = Vec::new();
    let mut hits = Vec::new();
    for root in &context.agent_roots {
        for candidate in &names {
            let path = Path::new(&root.root)
                .join("agents")
                .join(candidate)
                .join("README.md");
            attempted.push(path.clone());
            if path.is_file() {
                hits.push((root, candidate, path));
                break;
            }
        }
    }
    match hits.as_slice() {
        [(root, matched_name, path)] => {
            let path = path.to_string_lossy().into_owned();
            Ok(ArtifactRefResolutionWire {
                schema_version: ARTIFACT_REF_RESOLUTION_WIRE_SCHEMA_VERSION,
                status: "exact".to_string(),
                rendered: format!("agent:{matched_name}"),
                locator: Some(format!("{}/{}", root.project, matched_name)),
                resolved_path: Some(path.clone()),
                candidates: vec![path],
            })
        }
        [] => Ok(ArtifactRefResolutionWire {
            schema_version: ARTIFACT_REF_RESOLUTION_WIRE_SCHEMA_VERSION,
            status: "missing".to_string(),
            rendered,
            locator: None,
            resolved_path: None,
            candidates: paths_to_strings(attempted),
        }),
        _ => Ok(ArtifactRefResolutionWire {
            schema_version: ARTIFACT_REF_RESOLUTION_WIRE_SCHEMA_VERSION,
            status: "ambiguous".to_string(),
            rendered,
            locator: None,
            resolved_path: None,
            candidates: hits
                .into_iter()
                .map(|(_, _, path)| path.to_string_lossy().into_owned())
                .collect(),
        }),
    }
}

/// Return the lineage root used to group one bead's generated pages.
///
/// Bead page addressing is intentionally lexical and offline-derivable: no
/// bead-store read is needed to address a page that may not be published yet.
fn bead_lineage_root(id: &str) -> &str {
    id.split_once('.').map_or(id, |(root, _)| root)
}

/// Return the bead-store-relative path of one generated bead page.
fn bead_page_path(id: &str) -> PathBuf {
    let lineage = bead_lineage_root(id);
    let filename = if id == lineage {
        "README.md".to_string()
    } else {
        format!("{id}.md")
    };
    Path::new("pages").join(lineage).join(filename)
}

fn canonical_bead_id(path: &Path, pages_root: &Path) -> Option<String> {
    let relative = path.strip_prefix(pages_root).ok()?;
    let mut components = relative.components();
    let lineage = components.next()?.as_os_str().to_str()?;
    let filename = components.next()?.as_os_str().to_str()?;
    if components.next().is_some() {
        return None;
    }
    let id = if filename == "README.md" {
        lineage
    } else {
        filename.strip_suffix(".md")?
    };
    (bead_lineage_root(id) == lineage).then(|| id.to_string())
}

fn canonical_agent_name(path: &Path, agents_root: &Path) -> Option<String> {
    let relative = path.strip_prefix(agents_root).ok()?;
    let mut components = relative.components();
    let name = components.next()?.as_os_str().to_str()?;
    let filename = components.next()?.as_os_str().to_str()?;
    if filename != "README.md" || components.next().is_some() {
        return None;
    }
    Some(name.to_string())
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

/// Kinds whose payload names a logical entity rather than a stable document.
///
/// Their Markdown renderings are regenerated wholesale, so a line anchor would
/// silently drift to unrelated content after the next refresh.
fn kind_rejects_fragments(kind: &ArtifactRefKindWire) -> bool {
    matches!(
        kind,
        ArtifactRefKindWire::Commit
            | ArtifactRefKindWire::Bug
            | ArtifactRefKindWire::Bead
            | ArtifactRefKindWire::Agent
    )
}

/// Validate a bead id lexically, with no bead-store reads.
///
/// Stricter than the bead-page path rules on purpose: a reference payload must
/// be path-safe by construction, since resolution turns it straight into a
/// page path.
fn validate_bead_id(id: &str) -> Result<(), ArtifactRefError> {
    if id.is_empty() {
        return Err(ArtifactRefError::validation("bead id must not be empty"));
    }
    for segment in id.split('.') {
        if segment.is_empty() {
            return Err(ArtifactRefError::validation(
                "bead id must not contain an empty '.' segment",
            ));
        }
        if !segment.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')
        }) {
            return Err(ArtifactRefError::validation(
                "bead id segment must contain only letters, digits, '-' and '_'",
            ));
        }
    }
    Ok(())
}

/// Validate an `agent:` payload against the historical semantic-name rules.
fn validate_agent_payload_name(name: &str) -> Result<(), ArtifactRefError> {
    validate_agent_reference_name(name).map_err(|error| {
        ArtifactRefError::validation(format!("invalid agent name: {error}"))
    })
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

fn read_artifact_index(
    path: &Path,
) -> Result<Vec<crate::artifact_file::ArtifactFileWire>, ArtifactRefError> {
    Ok(read_artifact_file_index(path)?)
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
            ("bead:sase-9z", "bead:sase-9z"),
            ("bead:sase-9z.1", "bead:sase-9z.1"),
            ("bead:sase-ag.land", "bead:sase-ag.land"),
            ("agent:bbugyi200.athena.9w", "agent:bbugyi200.athena.9w"),
            (
                "agent:bbugyi200.athena.9w--code",
                "agent:bbugyi200.athena.9w--code",
            ),
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
            "bead:",
            "bead:sase-9z.",
            "bead:.sase-9z",
            "bead:sase-9z..1",
            "bead:sase/9z",
            "bead:sase 9z",
            "bead:sase-9z#L1",
            "agent:",
            "agent:a/b",
            "agent:.9w",
            "agent:9w#L1",
        ] {
            assert!(parse_artifact_ref(value).is_err(), "{value}");
        }
    }

    #[test]
    fn parsed_references_carry_the_current_wire_schema() {
        assert_eq!(parse_artifact_ref("bead:x").unwrap().schema_version, 2);
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
    fn bead_resolution_covers_pages_projects_and_ambiguity() {
        let temp = tempdir().unwrap();
        let first = temp.path().join("beads-first");
        let second = temp.path().join("beads-second");
        fs::create_dir_all(first.join("pages/sase-9z")).unwrap();
        fs::write(first.join("pages/sase-9z/README.md"), "root").unwrap();
        fs::write(first.join("pages/sase-9z/sase-9z.1.md"), "phase").unwrap();
        let mut context = ArtifactRefContextWire {
            bead_stores: vec![ArtifactRefBeadStoreWire {
                project: "sase".to_string(),
                prefix: "sase".to_string(),
                root: first.to_string_lossy().into_owned(),
            }],
            ..Default::default()
        };

        let root = resolve_artifact_ref(
            &parse_artifact_ref("bead:sase-9z").unwrap(),
            &context,
        )
        .unwrap();
        assert_eq!(root.status, "exact");
        assert_eq!(root.locator.as_deref(), Some("sase/sase-9z"));
        assert_eq!(
            root.resolved_path.as_deref(),
            Some(first.join("pages/sase-9z/README.md").to_str().unwrap())
        );

        let descendant = resolve_artifact_ref(
            &parse_artifact_ref("bead:sase-9z.1").unwrap(),
            &context,
        )
        .unwrap();
        assert_eq!(descendant.status, "exact");
        assert_eq!(
            descendant.resolved_path.as_deref(),
            Some(first.join("pages/sase-9z/sase-9z.1.md").to_str().unwrap())
        );

        let unknown = resolve_artifact_ref(
            &parse_artifact_ref("bead:other-1").unwrap(),
            &context,
        )
        .unwrap();
        assert_eq!(unknown.status, "unknown_project");

        let missing = resolve_artifact_ref(
            &parse_artifact_ref("bead:sase-missing").unwrap(),
            &context,
        )
        .unwrap();
        assert_eq!(missing.status, "missing");
        assert_eq!(
            missing.candidates,
            [first
                .join("pages/sase-missing/README.md")
                .to_string_lossy()
                .into_owned()]
        );

        let no_stores = resolve_artifact_ref(
            &parse_artifact_ref("bead:sase-9z").unwrap(),
            &ArtifactRefContextWire::default(),
        )
        .unwrap();
        assert_eq!(no_stores.status, "missing");
        assert!(no_stores.candidates.is_empty());

        fs::create_dir_all(second.join("pages/sase-9z")).unwrap();
        fs::write(second.join("pages/sase-9z/README.md"), "duplicate").unwrap();
        context.bead_stores.push(ArtifactRefBeadStoreWire {
            project: "sase-copy".to_string(),
            prefix: "sase".to_string(),
            root: second.to_string_lossy().into_owned(),
        });
        let ambiguous = resolve_artifact_ref(
            &parse_artifact_ref("bead:sase-9z").unwrap(),
            &context,
        )
        .unwrap();
        assert_eq!(ambiguous.status, "ambiguous");
        assert_eq!(ambiguous.candidates.len(), 2);
    }

    #[test]
    fn agent_resolution_globalizes_and_preserves_historical_names() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("agents-sidecar");
        for name in [
            "bbugyi200.athena.9w",
            "bbugyi200.athena.9w--code",
            "athena.sase-7r.land--code",
        ] {
            let directory = root.join("agents").join(name);
            fs::create_dir_all(&directory).unwrap();
            fs::write(directory.join("README.md"), name).unwrap();
        }
        let context = ArtifactRefContextWire {
            agent_roots: vec![ArtifactRefAgentRootWire {
                project: "sase".to_string(),
                root: root.to_string_lossy().into_owned(),
            }],
            agent_owner: Some(ArtifactRefAgentOwnerWire {
                username: "bbugyi200".to_string(),
                machine_name: "athena".to_string(),
            }),
            ..Default::default()
        };

        let global = resolve_artifact_ref(
            &parse_artifact_ref("agent:bbugyi200.athena.9w").unwrap(),
            &context,
        )
        .unwrap();
        assert_eq!(global.status, "exact");
        assert_eq!(global.rendered, "agent:bbugyi200.athena.9w");
        assert_eq!(global.locator.as_deref(), Some("sase/bbugyi200.athena.9w"));

        let local = resolve_artifact_ref(
            &parse_artifact_ref("agent:9w").unwrap(),
            &context,
        )
        .unwrap();
        assert_eq!(local.status, "exact");
        assert_eq!(local.rendered, "agent:bbugyi200.athena.9w");

        let legacy = resolve_artifact_ref(
            &parse_artifact_ref("agent:athena.sase-7r.land--code").unwrap(),
            &context,
        )
        .unwrap();
        assert_eq!(legacy.status, "exact");
        assert_eq!(legacy.rendered, "agent:athena.sase-7r.land--code");

        let family_member = resolve_artifact_ref(
            &parse_artifact_ref("agent:9w--code").unwrap(),
            &context,
        )
        .unwrap();
        assert_eq!(family_member.status, "exact");
        assert_eq!(family_member.rendered, "agent:bbugyi200.athena.9w--code");

        let missing = resolve_artifact_ref(
            &parse_artifact_ref("agent:absent").unwrap(),
            &context,
        )
        .unwrap();
        assert_eq!(missing.status, "missing");
        assert_eq!(missing.candidates.len(), 2);

        let no_roots = resolve_artifact_ref(
            &parse_artifact_ref("agent:9w").unwrap(),
            &ArtifactRefContextWire::default(),
        )
        .unwrap();
        assert_eq!(no_roots.status, "missing");
        assert!(no_roots.candidates.is_empty());
    }

    #[test]
    fn canonicalization_recognizes_only_canonical_entity_pages() {
        let temp = tempdir().unwrap();
        let beads = temp.path().join("beads");
        let agents = temp.path().join("agents");
        let context = ArtifactRefContextWire {
            bead_stores: vec![ArtifactRefBeadStoreWire {
                project: "sase".to_string(),
                prefix: "sase".to_string(),
                root: beads.to_string_lossy().into_owned(),
            }],
            agent_roots: vec![ArtifactRefAgentRootWire {
                project: "sase".to_string(),
                root: agents.to_string_lossy().into_owned(),
            }],
            ..Default::default()
        };

        assert_eq!(
            canonicalize_artifact_ref(
                &beads.join("pages/sase-9z/README.md"),
                &context
            )
            .unwrap()
            .as_deref(),
            Some("bead:sase-9z")
        );
        assert_eq!(
            canonicalize_artifact_ref(
                &beads.join("pages/sase-9z/sase-9z.1.md"),
                &context
            )
            .unwrap()
            .as_deref(),
            Some("bead:sase-9z.1")
        );
        assert_eq!(
            canonicalize_artifact_ref(
                &agents.join("agents/bbugyi200.athena.9w/README.md"),
                &context
            )
            .unwrap()
            .as_deref(),
            Some("agent:bbugyi200.athena.9w")
        );
        assert_eq!(
            canonicalize_artifact_ref(
                &beads.join("pages/sase-9z/other-1.md"),
                &context
            )
            .unwrap(),
            None
        );
        assert_eq!(
            canonicalize_artifact_ref(
                &agents.join("agents/bbugyi200.athena.9w/chat.md"),
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
    fn indexed_file_resolution_accepts_supported_envelope_range() {
        let temp = tempdir().unwrap();
        let v1_target = temp.path().join("v1.png");
        let v2_target = temp.path().join("v2.png");
        let index = temp.path().join("index.jsonl");
        fs::write(
            &index,
            format!(
                "{{\"schema_version\":3,\"artifact\":{{\"id\":\"default:52895d68931185056fd0e49f\",\"path\":\"ignored\"}}}}\n\
                 {{\"schema_version\":1,\"artifact\":{{\"id\":\"default:52895d68931185056fd0e49f\",\"path\":{}}}}}\n\
                 {{\"schema_version\":2,\"artifact\":{{\"id\":\"explicit:0123456789abcdef01234567\",\"path\":{}}}}}\n",
                serde_json::to_string(v1_target.to_str().unwrap()).unwrap(),
                serde_json::to_string(v2_target.to_str().unwrap()).unwrap(),
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
            Some(v1_target.to_str().unwrap())
        );
        let v2_result = resolve_artifact_ref(
            &parse_artifact_ref("file:explicit:0123456789abcdef01234567")
                .unwrap(),
            &context,
        )
        .unwrap();
        assert_eq!(
            v2_result.resolved_path.as_deref(),
            Some(v2_target.to_str().unwrap())
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
