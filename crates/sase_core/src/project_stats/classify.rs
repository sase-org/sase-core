//! Pure role and exclusion policy for committed regular files.
//!
//! The classifier receives committed-tree facts and optional blob bytes only.
//! It performs no filesystem, configuration, or Git discovery.

#![allow(dead_code)] // The snapshot phase is the first production caller.

use serde::{Deserialize, Serialize};

use crate::project_stats::wire::{
    ProjectStatsExclusionReasonWire as ExclusionReason,
    ProjectStatsFileExclusionWire as Exclusion,
    ProjectStatsFileRoleWire as Role,
};
use crate::source_language::{
    resolve_source_language, SourceCategory, SourceLanguageRequestWire,
    SOURCE_LANGUAGE_WIRE_SCHEMA_VERSION,
};

/// Fixed 8 MiB limit, matching the existing file-history blob helper.
pub(crate) const MAX_CLASSIFIED_BLOB_BYTES: u64 = 8 * 1024 * 1024;
const SHEBANG_PREFIX_BYTES: usize = 8 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AttributeState {
    Enabled,
    Disabled,
    Unspecified,
}

impl AttributeState {
    /// Parse the values emitted by `git check-attr` for boolean Linguist
    /// attributes. Unknown values are errors rather than silently changing
    /// role policy.
    pub(crate) fn parse(value: &[u8]) -> Result<Self, AttributeParseError> {
        match value {
            b"set" | b"true" => Ok(Self::Enabled),
            b"unset" | b"false" => Ok(Self::Disabled),
            b"unspecified" => Ok(Self::Unspecified),
            _ => Err(AttributeParseError),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AttributeParseError;

impl std::fmt::Display for AttributeParseError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("unsupported committed Linguist attribute value")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CommittedAttributes {
    pub generated: AttributeState,
    pub vendored: AttributeState,
    pub documentation: AttributeState,
}

impl Default for CommittedAttributes {
    fn default() -> Self {
        Self {
            generated: AttributeState::Unspecified,
            vendored: AttributeState::Unspecified,
            documentation: AttributeState::Unspecified,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SkippedEntry {
    Symlink,
    Gitlink,
    UnsupportedMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Classification {
    Included {
        role: Role,
        language: Option<String>,
    },
    Excluded {
        kind: Exclusion,
        reason: ExclusionReason,
    },
    Skipped(SkippedEntry),
}

/// Blob-derived facts the classifier needs, without retaining file bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct BlobEvidence {
    pub has_nul: bool,
    pub is_lfs_pointer: bool,
    pub shebang_language: Option<String>,
}

impl BlobEvidence {
    pub(crate) fn from_bytes(bytes: &[u8]) -> Self {
        Self {
            has_nul: bytes.contains(&0),
            is_lfs_pointer: is_lfs_pointer(bytes),
            shebang_language: shebang_language(bytes),
        }
    }
}

/// Classify one `ls-tree` entry. Blob evidence may be omitted before the
/// snapshot reader decides whether a candidate needs content inspection.
pub(crate) fn classify_file(
    path: &[u8],
    mode: u32,
    size: u64,
    attributes: CommittedAttributes,
    blob: Option<&[u8]>,
) -> Classification {
    classify_file_evidenced(
        path,
        mode,
        size,
        attributes,
        blob.map(BlobEvidence::from_bytes).as_ref(),
    )
}

/// Classify using cached blob facts so a renamed path can reuse an OID.
pub(crate) fn classify_file_evidenced(
    path: &[u8],
    mode: u32,
    size: u64,
    attributes: CommittedAttributes,
    evidence: Option<&BlobEvidence>,
) -> Classification {
    match mode {
        0o120000 => return Classification::Skipped(SkippedEntry::Symlink),
        0o160000 => return Classification::Skipped(SkippedEntry::Gitlink),
        0o100644 | 0o100755 => {}
        _ => return Classification::Skipped(SkippedEntry::UnsupportedMode),
    }

    if size > MAX_CLASSIFIED_BLOB_BYTES {
        return excluded(Exclusion::Generated, ExclusionReason::Oversize);
    }
    if let Some(evidence) = evidence {
        if evidence.has_nul {
            return excluded(Exclusion::Binary, ExclusionReason::NulByte);
        }
        if evidence.is_lfs_pointer {
            return excluded(Exclusion::Binary, ExclusionReason::LfsPointer);
        }
    }

    if attribute_or_heuristic(attributes.generated, is_generated_path(path)) {
        return excluded(
            Exclusion::Generated,
            if attributes.generated == AttributeState::Enabled {
                ExclusionReason::Attribute
            } else {
                ExclusionReason::PathHeuristic
            },
        );
    }
    if attribute_or_heuristic(attributes.vendored, is_vendored_path(path)) {
        return excluded(
            Exclusion::Vendored,
            if attributes.vendored == AttributeState::Enabled {
                ExclusionReason::Attribute
            } else {
                ExclusionReason::PathHeuristic
            },
        );
    }

    if is_agent_context(path) {
        return included(Role::AgentContext, None);
    }

    if attributes.documentation == AttributeState::Enabled {
        return included(Role::Docs, None);
    }

    let prose = is_prose_path(path);
    if prose {
        return if attributes.documentation == AttributeState::Disabled {
            included(Role::Other, None)
        } else {
            included(Role::Docs, None)
        };
    }

    if is_config_data_path(path) {
        return included(Role::ConfigData, None);
    }

    let language = detected_language(path, evidence);
    if let Some(language) = language {
        if is_test_path(path) {
            included(Role::Tests, Some(language))
        } else {
            included(Role::Source, Some(language))
        }
    } else {
        included(Role::Other, None)
    }
}

fn excluded(kind: Exclusion, reason: ExclusionReason) -> Classification {
    Classification::Excluded { kind, reason }
}

fn included(role: Role, language: Option<String>) -> Classification {
    Classification::Included { role, language }
}

fn attribute_or_heuristic(state: AttributeState, heuristic: bool) -> bool {
    match state {
        AttributeState::Enabled => true,
        AttributeState::Disabled => false,
        AttributeState::Unspecified => heuristic,
    }
}

fn is_generated_path(path: &[u8]) -> bool {
    let components = path_components(path);
    if components.iter().any(|part| {
        matches!(
            part.as_slice(),
            b"generated"
                | b"gen"
                | b"autogenerated"
                | b"snapshot"
                | b"snapshots"
                | b"golden"
                | b"goldens"
                | b"__snapshots__"
                | b"dist"
                | b"target"
        )
    }) {
        return true;
    }
    let Some(name) = components.last() else {
        return false;
    };
    name.ends_with(b".min.js")
        || name.ends_with(b".min.mjs")
        || name.ends_with(b".min.cjs")
        || name.ends_with(b".min.css")
        || name.ends_with(b".min.html")
        || name.ends_with(b".map")
        || name.ends_with(b".golden")
        || name.ends_with(b".snap")
        || name.ends_with(b".generated")
}

fn is_vendored_path(path: &[u8]) -> bool {
    path_components(path).iter().any(|part| {
        matches!(
            part.as_slice(),
            b"vendor"
                | b"vendors"
                | b"third_party"
                | b"third-party"
                | b"thirdparty"
                | b"node_modules"
                | b"bower_components"
                | b"external"
                | b"deps"
        )
    })
}

fn is_agent_context(path: &[u8]) -> bool {
    let components = path_components(path);
    if components.windows(2).any(|parts| {
        parts[0] == b"sase" && (parts[1] == b"memory" || parts[1] == b"macros")
    }) {
        return true;
    }
    if components.iter().any(|part| part.as_slice() == b"skills") {
        return true;
    }
    let Some(name) = components.last() else {
        return false;
    };
    matches!(
        name.as_slice(),
        b"agents.md"
            | b"claude.md"
            | b"gemini.md"
            | b"codex.md"
            | b"copilot.md"
            | b"cursor.md"
            | b"windsurf.md"
            | b"warp.md"
    ) || name.starts_with(b"agents.") && name.ends_with(b".md")
}

fn is_prose_path(path: &[u8]) -> bool {
    let components = path_components(path);
    let Some(name) = components.last() else {
        return false;
    };
    matches!(
        name.as_slice(),
        b"readme"
            | b"license"
            | b"licence"
            | b"changelog"
            | b"changes"
            | b"contributing"
            | b"code_of_conduct"
            | b"authors"
            | b"contributors"
            | b"notice"
            | b"news"
    ) || has_extension(
        name,
        &[
            b"md",
            b"markdown",
            b"mdown",
            b"mdx",
            b"rst",
            b"txt",
            b"adoc",
            b"asciidoc",
            b"org",
            b"tex",
        ],
    )
}

fn is_config_data_path(path: &[u8]) -> bool {
    let components = path_components(path);
    let Some(name) = components.last() else {
        return false;
    };
    matches!(
        name.as_slice(),
        b"cargo.lock"
            | b"cargo.toml"
            | b"package-lock.json"
            | b"npm-shrinkwrap.json"
            | b"yarn.lock"
            | b"pnpm-lock.yaml"
            | b"poetry.lock"
            | b"pipfile.lock"
            | b"composer.lock"
            | b"gemfile.lock"
            | b"flake.lock"
            | b"go.sum"
            | b"dockerfile"
            | b"makefile"
            | b".editorconfig"
    ) || has_extension(
        name,
        &[
            b"json",
            b"json5",
            b"jsonc",
            b"yaml",
            b"yml",
            b"toml",
            b"ini",
            b"cfg",
            b"conf",
            b"properties",
            b"xml",
            b"csv",
            b"tsv",
            b"ipynb",
            b"lock",
        ],
    )
}

fn is_test_path(path: &[u8]) -> bool {
    let components = path_components(path);
    if components[..components.len().saturating_sub(1)]
        .iter()
        .any(|part| {
            matches!(
                part.as_slice(),
                b"test" | b"tests" | b"spec" | b"__tests__"
            )
        })
    {
        return true;
    }
    let Some(name) = components.last() else {
        return false;
    };
    name.starts_with(b"test_")
        || name == b"test.rs"
        || name == b"test.py"
        || name.ends_with(b"_test.rs")
        || name.ends_with(b"_test.go")
        || name.ends_with(b"_test.py")
        || name.ends_with(b".test.js")
        || name.ends_with(b".test.jsx")
        || name.ends_with(b".test.ts")
        || name.ends_with(b".test.tsx")
        || name.ends_with(b".spec.js")
        || name.ends_with(b".spec.jsx")
        || name.ends_with(b".spec.ts")
        || name.ends_with(b".spec.tsx")
}

fn path_components(path: &[u8]) -> Vec<Vec<u8>> {
    path.split(|byte| *byte == b'/')
        .map(|component| component.iter().map(u8::to_ascii_lowercase).collect())
        .collect()
}

fn has_extension(name: &[u8], extensions: &[&[u8]]) -> bool {
    let Some((_, extension)) = name
        .iter()
        .rposition(|byte| *byte == b'.')
        .map(|dot| (&name[..dot], name[dot + 1..].to_ascii_lowercase()))
    else {
        return false;
    };
    extensions.iter().any(|candidate| *candidate == extension)
}

fn detected_language(
    path: &[u8],
    evidence: Option<&BlobEvidence>,
) -> Option<String> {
    let filename = filename_hint(path)?;
    let detected = resolve_source_language(&SourceLanguageRequestWire {
        schema_version: SOURCE_LANGUAGE_WIRE_SCHEMA_VERSION,
        category: SourceCategory::RawFile,
        logical_filename: Some(filename.clone()),
        prefix: None,
    });
    if detected.language.is_some() {
        return detected.language;
    }

    let unknown_extension = detected.reason == "unknown";
    let extensionless = !filename
        .rsplit('/')
        .next()
        .unwrap_or(&filename)
        .contains('.');
    if !(unknown_extension || extensionless) {
        return None;
    }
    evidence.and_then(|facts| facts.shebang_language.clone())
}

fn filename_hint(path: &[u8]) -> Option<String> {
    if let Ok(path) = std::str::from_utf8(path) {
        return Some(path.to_owned());
    }
    let name = path.rsplit(|byte| *byte == b'/').next()?;
    let dot = name.iter().rposition(|byte| *byte == b'.')?;
    let extension = &name[dot + 1..];
    if extension.is_empty() || !extension.iter().all(u8::is_ascii) {
        return None;
    }
    let extension = std::str::from_utf8(extension).ok()?;
    Some(format!("file.{extension}"))
}

fn shebang_language(blob: &[u8]) -> Option<String> {
    let prefix = &blob[..blob.len().min(SHEBANG_PREFIX_BYTES)];
    let first_line = prefix.split(|byte| *byte == b'\n').next()?;
    let first_line = first_line.strip_suffix(b"\r").unwrap_or(first_line);
    let rest = first_line.strip_prefix(b"#!")?;
    let line = std::str::from_utf8(rest).ok()?.trim();
    let mut words = line.split_ascii_whitespace();
    let mut interpreter = words.next()?.rsplit('/').next()?;
    if interpreter == "env" {
        interpreter = words.find(|word| {
            *word != "--" && !word.starts_with('-') && !word.contains('=')
        })?;
        interpreter = interpreter.rsplit('/').next()?;
    }
    let language = match interpreter {
        "bash" => "bash",
        "sh" | "dash" => "sh",
        "zsh" => "zsh",
        "fish" => "fish",
        "node" | "nodejs" => "javascript",
        "deno" => "typescript",
        "ruby" => "ruby",
        "perl" => "perl",
        "php" => "php",
        "lua" => "lua",
        "awk" | "gawk" => "awk",
        "Rscript" => "r",
        "pwsh" | "powershell" => "powershell",
        name if name == "python" || name == "pypy" => "python",
        name if name.starts_with("python")
            && name[6..].chars().all(|character| {
                character.is_ascii_digit() || character == '.'
            }) =>
        {
            "python"
        }
        _ => return None,
    };
    Some(language.to_owned())
}

fn is_lfs_pointer(blob: &[u8]) -> bool {
    let Some(text) = std::str::from_utf8(blob).ok() else {
        return false;
    };
    let mut lines = text.lines();
    if lines.next() != Some("version https://git-lfs.github.com/spec/v1") {
        return false;
    }
    let mut valid_oid = false;
    let mut valid_size = false;
    for line in lines {
        if let Some(oid) = line.strip_prefix("oid sha256:") {
            valid_oid = oid.len() == 64
                && oid.bytes().all(|byte| byte.is_ascii_hexdigit());
        }
        if let Some(size) = line.strip_prefix("size ") {
            valid_size = !size.is_empty()
                && size.bytes().all(|byte| byte.is_ascii_digit());
        }
    }
    valid_oid && valid_size
}
