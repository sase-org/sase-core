//! Attachment manifests, tombstones, and placement/fetch/sensitive-path policy.
//!
//! Descriptors are location-free (`{name, sha256, size_bytes, mime_type,
//! image?, origin?}`): they are as public as the bead itself, while bytes
//! stay private. Every query and policy input in this module is a pure
//! function over `*Wire` structs so a second frontend can match the TUI
//! exactly.

use serde::{Deserialize, Serialize};

use super::names::is_valid_attachment_name;
use super::scan::{stored_attachment_tokens, NoteAttachmentError};

/// Wire schema version for [`AttachmentTombstoneWire`].
pub const ATTACHMENT_TOMBSTONE_WIRE_SCHEMA_VERSION: u32 = 1;

/// Pixel dimensions of an image attachment, probed at ingest time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttachmentImageDimsWire {
    pub width: u32,
    pub height: u32,
}

/// One content-addressed file attached to a bead note.
///
/// Field names follow the existing core wires (`size_bytes`, `mime_type`).
/// No paths, store locations, or machine-local ids are ever recorded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadNoteAttachmentWire {
    pub name: String,
    pub sha256: String,
    pub size_bytes: u64,
    pub mime_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<AttachmentImageDimsWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
}

impl BeadNoteAttachmentWire {
    /// Validate one descriptor: the name is already sanitized, the digest is
    /// a lowercase SHA-256 hex string, the MIME type is shaped
    /// `type/subtype`, and image dimensions are greater than 0.
    pub fn validate(&self) -> Result<(), NoteAttachmentError> {
        if !is_valid_attachment_name(&self.name) {
            return Err(NoteAttachmentError::InvalidManifest {
                message: format!(
                    "attachment name {:?} is not sanitized",
                    self.name
                ),
            });
        }
        validate_attachment_sha256(&self.sha256)?;
        validate_attachment_mime_type(&self.mime_type)?;
        if let Some(dims) = &self.image {
            if dims.width == 0 || dims.height == 0 {
                return Err(NoteAttachmentError::InvalidManifest {
                    message: format!(
                        "attachment {:?} has non-positive image dimensions {}x{}",
                        self.name, dims.width, dims.height
                    ),
                });
            }
        }
        if self
            .origin
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err(NoteAttachmentError::InvalidManifest {
                message: format!(
                    "attachment {:?} has a blank origin",
                    self.name
                ),
            });
        }
        Ok(())
    }
}

fn validate_attachment_sha256(value: &str) -> Result<(), NoteAttachmentError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(NoteAttachmentError::InvalidManifest {
            message: format!(
                "attachment digest {value:?} must be a lowercase 64-character SHA-256 hex digest"
            ),
        });
    }
    Ok(())
}

fn validate_attachment_mime_type(
    value: &str,
) -> Result<(), NoteAttachmentError> {
    let invalid = || NoteAttachmentError::InvalidManifest {
        message: format!(
            "attachment MIME type {value:?} must be shaped type/subtype"
        ),
    };
    let Some((main, sub)) = value.split_once('/') else {
        return Err(invalid());
    };
    if value.chars().filter(|character| *character == '/').count() != 1 {
        return Err(invalid());
    }
    for part in [main, sub] {
        if part.is_empty()
            || !part.bytes().all(|byte| {
                byte.is_ascii_alphanumeric()
                    || matches!(byte, b'.' | b'-' | b'+')
            })
        {
            return Err(invalid());
        }
    }
    Ok(())
}

/// Validate a full manifest against its note text: every descriptor
/// validates, names are unique within the manifest, and — whenever the
/// manifest is non-empty — every token names a descriptor and every
/// descriptor has at least one token (repeated tokens reuse one descriptor).
pub fn validate_note_attachment_manifest(
    manifest: &[BeadNoteAttachmentWire],
    text: &str,
) -> Result<(), NoteAttachmentError> {
    for attachment in manifest {
        attachment.validate()?;
    }
    let mut names: Vec<&str> =
        manifest.iter().map(|item| item.name.as_str()).collect();
    names.sort_unstable();
    for pair in names.windows(2) {
        if pair[0] == pair[1] {
            return Err(NoteAttachmentError::InvalidManifest {
                message: format!(
                    "attachment manifest has a duplicate name: {:?}",
                    pair[0]
                ),
            });
        }
    }
    if manifest.is_empty() {
        return Ok(());
    }
    let tokens = stored_attachment_tokens(text);
    let mut token_names: Vec<&str> =
        tokens.iter().map(|token| token.name.as_str()).collect();
    token_names.sort_unstable();
    token_names.dedup();
    if token_names != names {
        return Err(NoteAttachmentError::InvalidManifest {
            message: format!(
                "attachment tokens and manifest must match one-to-one: text has {:?}, manifest has {:?}",
                token_names, names
            ),
        });
    }
    Ok(())
}

/// Record that one object was purged from every store.
///
/// Notes keep rendering `(purged)` and fetches refuse tombstoned digests. No
/// bead event changes: the tombstone lives in the stores, not the log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttachmentTombstoneWire {
    pub schema_version: u32,
    pub sha256: String,
    pub purged_at: String,
    pub actor: String,
    pub reason: String,
}

impl AttachmentTombstoneWire {
    /// Validate a tombstone: current schema version, a real digest, and
    /// non-blank provenance fields.
    pub fn validate(&self) -> Result<(), NoteAttachmentError> {
        if self.schema_version != ATTACHMENT_TOMBSTONE_WIRE_SCHEMA_VERSION {
            return Err(NoteAttachmentError::InvalidManifest {
                message: format!(
                    "unsupported attachment tombstone schema_version: {}",
                    self.schema_version
                ),
            });
        }
        validate_attachment_sha256(&self.sha256)?;
        for (field, value) in [
            ("purged_at", self.purged_at.as_str()),
            ("actor", self.actor.as_str()),
            ("reason", self.reason.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(NoteAttachmentError::InvalidManifest {
                    message: format!(
                        "attachment tombstone {field} cannot be empty or blank"
                    ),
                });
            }
        }
        Ok(())
    }
}

/// One placement tier: a named store with an optional size cap.
///
/// Stores form an ordered, digest-keyed configuration list, so a local-only
/// object can be promoted later without editing any note.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttachmentStoreTierWire {
    pub name: String,
    /// `None` accepts any size.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u64>,
}

/// Where an attachment's bytes belong.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AttachmentPlacementWire {
    Store { store: String },
    LocalOnly,
}

/// Pick stores by size from an ordered tier list.
///
/// An explicit local-only request always wins. Otherwise the first tier
/// whose cap accepts the size wins. When no configured store accepts the
/// size, the command fails before writing anything.
pub fn attachment_placement(
    size_bytes: u64,
    tiers: &[AttachmentStoreTierWire],
    local_only: bool,
) -> Result<AttachmentPlacementWire, NoteAttachmentError> {
    if local_only {
        return Ok(AttachmentPlacementWire::LocalOnly);
    }
    for tier in tiers {
        if tier.name.trim().is_empty() {
            return Err(NoteAttachmentError::InvalidManifest {
                message: "attachment store tier name cannot be empty or blank"
                    .to_string(),
            });
        }
        let accepts = tier.max_bytes.is_none_or(|max| size_bytes <= max);
        if accepts {
            return Ok(AttachmentPlacementWire::Store {
                store: tier.name.clone(),
            });
        }
    }
    Err(NoteAttachmentError::NoAcceptingStore { size_bytes })
}

/// True when an object may be fetched automatically under the cap.
/// Explicit `path`/`open`/`-d/--download` always fetches.
pub fn attachment_should_auto_fetch(size_bytes: u64, cap_bytes: u64) -> bool {
    size_bytes <= cap_bytes
}

/// Sensitive path prefixes, in `~`-relative form.
const SENSITIVE_HOME_PREFIX_PATTERNS: &[&str] = &["~/.ssh/**", "~/.gnupg/**"];

/// Sensitive suffix and file-name patterns, in glob form (`*` spans any run,
/// including separators).
const SENSITIVE_GLOB_PATTERNS: &[&str] = &[
    "**/.env",
    "**/.env.*",
    "*.pem",
    "*.key",
    "*id_rsa*",
    "*id_ed25519*",
    "*.kdbx",
    "**/credentials.json",
    "**/.netrc",
    "**/.aws/credentials",
    "**/.git-credentials",
    "**/.config/gh/hosts.yml",
];

/// Refuse sensitive paths unless the caller passes `-S/--allow-sensitive`.
///
/// Returns the refusal reason when `path` matches a built-in sensitive
/// pattern or one of `extra_patterns` (same mini-language), and `None` when
/// the path is safe to attach.
pub fn attachment_sensitive_path_reason(
    path: &str,
    home: &str,
    extra_patterns: &[String],
) -> Option<String> {
    let home = home.trim_end_matches('/');
    let expanded = expand_tilde(path, home);
    for pattern in SENSITIVE_HOME_PREFIX_PATTERNS {
        if home.is_empty() {
            continue;
        }
        let expanded_pattern = expand_tilde(pattern, home);
        if path_has_prefix(&expanded, &expanded_pattern) {
            return Some(sensitive_reason(pattern));
        }
    }
    for pattern in SENSITIVE_GLOB_PATTERNS {
        if glob_sensitive_match(pattern, &expanded) {
            return Some(sensitive_reason(pattern));
        }
    }
    for pattern in extra_patterns {
        if pattern.trim().is_empty() {
            continue;
        }
        if pattern.starts_with("~/") {
            if home.is_empty() {
                continue;
            }
            if path_has_prefix(&expanded, &expand_tilde(pattern, home)) {
                return Some(sensitive_reason(pattern));
            }
        } else if glob_sensitive_match(pattern, &expanded) {
            return Some(sensitive_reason(pattern));
        }
    }
    None
}

fn sensitive_reason(pattern: &str) -> String {
    format!("sensitive path (matches {pattern})")
}

fn expand_tilde(value: &str, home: &str) -> String {
    if home.is_empty() {
        return value.to_string();
    }
    if let Some(rest) = value.strip_prefix("~/") {
        return format!("{home}/{rest}");
    }
    if value == "~" {
        return home.to_string();
    }
    value.to_string()
}

/// True when a `~`-expanded prefix pattern (with an optional `/**` tail)
/// covers `path` as a whole path or a directory prefix.
fn path_has_prefix(path: &str, pattern: &str) -> bool {
    let base = pattern.strip_suffix("/**").unwrap_or(pattern);
    path == base || path.starts_with(&format!("{base}/"))
}

/// Match one glob pattern against an expanded path: a `**/` head matches any
/// ancestor depth (or none), and a pattern without `/` matches the file name.
fn glob_sensitive_match(pattern: &str, path: &str) -> bool {
    if let Some(rest) = pattern.strip_prefix("**/") {
        if glob_match(rest, path) {
            return true;
        }
        // Any ancestor depth: retry after every `/`.
        let mut suffix = path;
        while let Some(slash) = suffix.find('/') {
            suffix = &suffix[slash + 1..];
            if glob_match(rest, suffix) {
                return true;
            }
        }
        return false;
    }
    if pattern.contains('/') {
        return glob_match(pattern, path);
    }
    let file_name = path.rsplit('/').next().unwrap_or(path);
    glob_match(pattern, file_name)
}

/// Full-text glob: `*` spans any run (possibly empty), `?` spans one char.
fn glob_match(pattern: &str, text: &str) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let text: Vec<char> = text.chars().collect();
    let mut prefix = 0;
    let mut matched = 0;
    let mut restart = false;
    let mut text_index = 0;
    let mut pattern_index = 0;
    while text_index < text.len() {
        if pattern_index < pattern.len()
            && (pattern[pattern_index] == '?'
                || pattern[pattern_index] == text[text_index])
        {
            pattern_index += 1;
            text_index += 1;
        } else if pattern_index < pattern.len() && pattern[pattern_index] == '*'
        {
            prefix = pattern_index;
            matched = text_index;
            restart = true;
            pattern_index += 1;
        } else if restart {
            pattern_index = prefix + 1;
            matched += 1;
            text_index = matched;
        } else {
            return false;
        }
    }
    while pattern_index < pattern.len() && pattern[pattern_index] == '*' {
        pattern_index += 1;
    }
    pattern_index == pattern.len()
}

/// One attachment on a bead's current roster: the latest note wins per name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadAttachmentRosterEntryWire {
    pub name: String,
    pub sha256: String,
    pub size_bytes: u64,
    pub mime_type: String,
    pub note_id: String,
    /// 1-based position in the bead's note list, as shown by `sase bead show`.
    pub ordinal: u64,
}

/// One attachment reference from the event store, current or historical.
///
/// Objects stay pinned while any current _or historical_ event references
/// them; this query feeds pinning, purge preview, and doctor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadAttachmentReferenceWire {
    pub issue: String,
    pub note: String,
    pub name: String,
    pub sha256: String,
    pub current: bool,
}
