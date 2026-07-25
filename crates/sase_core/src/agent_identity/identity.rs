use crate::machine_hood::validate_machine_name;
use serde::{Deserialize, Serialize};
use thiserror::Error;

const MAX_AGENT_NAME_BYTES: usize = 512;
const USERNAME_SYNTAX: &str =
    "lowercase ASCII letters or digits with '-' and '_' only internally";
const RESERVED_USERNAMES: &[&str] = &[
    "agent", "agents", "clan", "clans", "families", "family", "internal",
    "repo", "repos", "sase", "sidecar", "sidecars",
];

/// A validated v2 owner identity.
///
/// `Deserialize` is intentionally derived for the wire format, but every
/// public domain operation calls [`AgentOwnerIdentity::validate`] before
/// using a deserialized value.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(deny_unknown_fields)]
pub struct AgentOwnerIdentity {
    pub username: String,
    pub machine_name: String,
}

impl AgentOwnerIdentity {
    pub fn new(
        username: impl Into<String>,
        machine_name: impl Into<String>,
    ) -> Result<Self, AgentIdentityError> {
        let owner = Self {
            username: username.into(),
            machine_name: machine_name.into(),
        };
        owner.validate()?;
        Ok(owner)
    }

    pub fn validate(&self) -> Result<(), AgentIdentityError> {
        validate_agent_username(&self.username)?;
        validate_machine_name(&self.machine_name).map_err(|source| {
            AgentIdentityError::InvalidMachineName {
                machine_name: self.machine_name.clone(),
                reason: source.to_string(),
            }
        })
    }
}

/// Explicit source provenance for v2 and imported v1 names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentSourceOwnerIdentity {
    V2 { owner: AgentOwnerIdentity },
    UsernameUnknownV1 { machine_name: String },
}

impl AgentSourceOwnerIdentity {
    pub fn validate(&self) -> Result<(), AgentIdentityError> {
        match self {
            Self::V2 { owner } => owner.validate(),
            Self::UsernameUnknownV1 { machine_name } => {
                validate_machine_name(machine_name).map_err(|source| {
                    AgentIdentityError::InvalidMachineName {
                        machine_name: machine_name.clone(),
                        reason: source.to_string(),
                    }
                })
            }
        }
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum AgentOwnershipClassification {
    ExactOwner,
    SameUserOtherMachine,
    OtherUser,
    UsernameUnknownV1,
}

impl AgentOwnershipClassification {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ExactOwner => "exact_owner",
            Self::SameUserOtherMachine => "same_user_other_machine",
            Self::OtherUser => "other_user",
            Self::UsernameUnknownV1 => "username_unknown_v1",
        }
    }
}

/// First-party evidence available when classifying one legacy-v1 group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyV1GroupOwnershipEvidence {
    pub v2_hood_published: bool,
    pub proven_entry_count: usize,
    pub total_entry_count: usize,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum LegacyV1GroupOwnershipClassification {
    OwnerObserved,
    Foreign,
}

impl LegacyV1GroupOwnershipClassification {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OwnerObserved => "owner_observed",
            Self::Foreign => "foreign",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentFamilyNameWire {
    pub kind: String,
    pub family_name: String,
    #[serde(default)]
    pub member_role: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentLinkTargetWire {
    pub kind: String,
    pub path: String,
    #[serde(default)]
    pub anchor: Option<String>,
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum AgentIdentityError {
    #[error(
        "invalid username '{username}': expected {USERNAME_SYNTAX}; reserved internal names are not allowed"
    )]
    InvalidUsername { username: String },

    #[error("invalid machine name '{machine_name}': {reason}")]
    InvalidMachineName {
        machine_name: String,
        reason: String,
    },

    #[error("agent name must not be empty")]
    EmptyAgentName,

    #[error(
        "invalid agent name '{name}': {reason}; names must use non-empty dot-separated path-safe ASCII segments"
    )]
    InvalidAgentName { name: String, reason: String },

    #[error(
        "global agent name '{name}' does not belong to explicit owner '{username}.{machine_name}'"
    )]
    GlobalOwnerMismatch {
        name: String,
        username: String,
        machine_name: String,
    },

    #[error(
        "legacy agent name '{name}' has machine hood '{actual_machine}', expected '{expected_machine}'"
    )]
    LegacyMachineMismatch {
        name: String,
        actual_machine: String,
        expected_machine: String,
    },

    #[error(
        "legacy agent name '{name}' must be qualified as '<machine_name>.<local-name>'"
    )]
    MalformedLegacyName { name: String },

    #[error(
        "invalid legacy-v1 group ownership evidence: proven entry count {proven_entry_count} exceeds total entry count {total_entry_count}"
    )]
    InvalidLegacyV1GroupOwnershipEvidence {
        proven_entry_count: usize,
        total_entry_count: usize,
    },

    #[error(
        "invalid family name '{name}': expected a solo name or one terminal '--<role>' suffix"
    )]
    InvalidFamilyName { name: String },
}

pub fn validate_agent_username(
    username: &str,
) -> Result<(), AgentIdentityError> {
    let bytes = username.as_bytes();
    let valid = !bytes.is_empty()
        && !RESERVED_USERNAMES.contains(&username)
        && bytes.first().is_some_and(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit()
        })
        && bytes.last().is_some_and(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit()
        })
        && bytes.iter().all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || *byte == b'-'
                || *byte == b'_'
        })
        && !username.contains("--");
    if valid {
        Ok(())
    } else {
        Err(AgentIdentityError::InvalidUsername {
            username: username.to_string(),
        })
    }
}

/// Strictly validate a newly-created agent name.
///
/// Historical classification helpers intentionally accept legacy family
/// markers in non-terminal segments. Name creation must continue to use this
/// stricter entry point, which permits at most one terminal `--<role>` suffix.
pub fn validate_agent_name(name: &str) -> Result<(), AgentIdentityError> {
    validate_semantic_name(name)
}

pub fn classify_agent_ownership(
    source: &AgentSourceOwnerIdentity,
    target: &AgentOwnerIdentity,
) -> Result<AgentOwnershipClassification, AgentIdentityError> {
    source.validate()?;
    target.validate()?;
    Ok(match source {
        AgentSourceOwnerIdentity::UsernameUnknownV1 { .. } => {
            AgentOwnershipClassification::UsernameUnknownV1
        }
        AgentSourceOwnerIdentity::V2 { owner } if owner == target => {
            AgentOwnershipClassification::ExactOwner
        }
        AgentSourceOwnerIdentity::V2 { owner }
            if owner.username == target.username =>
        {
            AgentOwnershipClassification::SameUserOtherMachine
        }
        AgentSourceOwnerIdentity::V2 { .. } => {
            AgentOwnershipClassification::OtherUser
        }
    })
}

/// Classify one legacy-v1 group using explicit first-party evidence.
///
/// A matching machine token is necessary but never sufficient. The group is
/// owner-observed only when the target owner has already published its hood in
/// v2 or at least one entry is proven against a local, non-imported artifact.
pub fn classify_legacy_v1_group_ownership(
    group_machine_name: &str,
    target: &AgentOwnerIdentity,
    evidence: &LegacyV1GroupOwnershipEvidence,
) -> Result<LegacyV1GroupOwnershipClassification, AgentIdentityError> {
    validate_machine_name(group_machine_name).map_err(|source| {
        AgentIdentityError::InvalidMachineName {
            machine_name: group_machine_name.to_string(),
            reason: source.to_string(),
        }
    })?;
    target.validate()?;
    if evidence.proven_entry_count > evidence.total_entry_count {
        return Err(
            AgentIdentityError::InvalidLegacyV1GroupOwnershipEvidence {
                proven_entry_count: evidence.proven_entry_count,
                total_entry_count: evidence.total_entry_count,
            },
        );
    }

    let owner_observed = group_machine_name == target.machine_name
        && (evidence.v2_hood_published || evidence.proven_entry_count > 0);
    Ok(if owner_observed {
        LegacyV1GroupOwnershipClassification::OwnerObserved
    } else {
        LegacyV1GroupOwnershipClassification::Foreign
    })
}

/// Strip at most one canonical `YYMMDD.` archive prefix and validate that the
/// remaining historical name is non-empty and path-safe.
pub fn normalize_agent_archive_name(
    name: &str,
) -> Result<String, AgentIdentityError> {
    let normalized = match name.split_once('.') {
        Some((prefix, remainder))
            if prefix.len() == 6
                && prefix.bytes().all(|byte| byte.is_ascii_digit()) =>
        {
            if remainder.is_empty() {
                return Err(AgentIdentityError::EmptyAgentName);
            }
            remainder
        }
        _ => name,
    };
    validate_historical_semantic_name(normalized)?;
    Ok(normalized.to_string())
}

/// Construct a v2 global name from explicit owner identity and local
/// semantics. A name already carrying that exact owner is returned unchanged.
pub fn globalize_agent_name(
    local_name: &str,
    owner: &AgentOwnerIdentity,
) -> Result<String, AgentIdentityError> {
    owner.validate()?;
    let normalized = normalize_agent_archive_name(local_name)?;
    let prefix = owner_prefix(owner);
    if let Some(remainder) = normalized.strip_prefix(&prefix) {
        validate_historical_semantic_name(remainder)?;
        return Ok(normalized);
    }
    validate_historical_semantic_name(&normalized)?;
    Ok(format!("{prefix}{normalized}"))
}

/// Verify a legacy machine-qualified name and convert it to a v2 global name.
pub fn globalize_legacy_agent_name(
    legacy_name: &str,
    current_owner: &AgentOwnerIdentity,
) -> Result<String, AgentIdentityError> {
    current_owner.validate()?;
    let normalized = normalize_agent_archive_name(legacy_name)?;
    let Some((actual_machine, local_name)) = normalized.split_once('.') else {
        return Err(AgentIdentityError::MalformedLegacyName {
            name: legacy_name.to_string(),
        });
    };
    validate_machine_name(actual_machine).map_err(|_| {
        AgentIdentityError::MalformedLegacyName {
            name: legacy_name.to_string(),
        }
    })?;
    if actual_machine != current_owner.machine_name {
        return Err(AgentIdentityError::LegacyMachineMismatch {
            name: legacy_name.to_string(),
            actual_machine: actual_machine.to_string(),
            expected_machine: current_owner.machine_name.clone(),
        });
    }
    globalize_agent_name(local_name, current_owner)
}

/// Validate and remove exactly the supplied v2 owner's global prefix.
pub fn strip_global_agent_name(
    global_name: &str,
    source_owner: &AgentOwnerIdentity,
) -> Result<String, AgentIdentityError> {
    source_owner.validate()?;
    let normalized = normalize_agent_archive_name(global_name)?;
    let prefix = owner_prefix(source_owner);
    let Some(local_name) = normalized.strip_prefix(&prefix) else {
        return Err(owner_mismatch(&normalized, source_owner));
    };
    validate_historical_semantic_name(local_name)?;
    Ok(local_name.to_string())
}

/// Localize a verified source-global name for a target owner.
pub fn localize_agent_name(
    global_name: &str,
    source: &AgentSourceOwnerIdentity,
    target: &AgentOwnerIdentity,
) -> Result<String, AgentIdentityError> {
    let classification = classify_agent_ownership(source, target)?;
    let local_name = strip_source_global_name(global_name, source)?;
    Ok(match (classification, source) {
        (AgentOwnershipClassification::ExactOwner, _) => local_name,
        (
            AgentOwnershipClassification::SameUserOtherMachine,
            AgentSourceOwnerIdentity::V2 { owner },
        ) => format!("{}.{}", owner.machine_name, local_name),
        (
            AgentOwnershipClassification::OtherUser,
            AgentSourceOwnerIdentity::V2 { owner },
        ) => {
            format!("{}.{}.{}", owner.username, owner.machine_name, local_name)
        }
        (
            AgentOwnershipClassification::UsernameUnknownV1,
            AgentSourceOwnerIdentity::UsernameUnknownV1 { machine_name },
        ) => format!("{machine_name}.{local_name}"),
        _ => unreachable!("classification and source variants are aligned"),
    })
}

pub fn parse_agent_family_name(
    name: &str,
) -> Result<AgentFamilyNameWire, AgentIdentityError> {
    let normalized = normalize_agent_archive_name(name)?;
    parse_normalized_family_name(&normalized)
}

pub fn agent_local_hood(name: &str) -> Result<String, AgentIdentityError> {
    let parsed = parse_agent_family_name(name)?;
    Ok(historical_hood_segment(
        parsed
            .family_name
            .split('.')
            .next()
            .expect("validated name has a first segment"),
    )
    .to_string())
}

pub fn agent_name_in_hood(
    name: &str,
    hood: &str,
) -> Result<bool, AgentIdentityError> {
    let normalized_hood = normalize_agent_archive_name(hood)?;
    if normalized_hood.contains("--") {
        return Err(AgentIdentityError::InvalidFamilyName {
            name: hood.to_string(),
        });
    }
    let Ok(parsed) = parse_agent_family_name(name) else {
        return Ok(false);
    };
    let family_scope = historical_family_scope(&parsed.family_name);
    Ok(family_scope == normalized_hood
        || family_scope
            .strip_prefix(&normalized_hood)
            .is_some_and(|suffix| suffix.starts_with('.')))
}

pub fn agent_name_ancestors(
    name: &str,
) -> Result<Vec<String>, AgentIdentityError> {
    let parsed = parse_agent_family_name(name)?;
    let mut segments = parsed.family_name.split('.');
    let first = segments.next().expect("validated name has a first segment");
    let hood = historical_hood_segment(first);
    let mut ancestors = vec![hood.to_string()];
    let mut current = first.to_string();
    for segment in segments {
        current.push('.');
        current.push_str(segment);
        ancestors.push(current.clone());
    }
    if ancestors.len() == 1 && hood != first {
        ancestors.push(first.to_string());
    }
    Ok(ancestors)
}

pub fn agent_link_target(
    semantic_name: &str,
    owner: &AgentOwnerIdentity,
) -> Result<AgentLinkTargetWire, AgentIdentityError> {
    owner.validate()?;
    let parsed = parse_agent_family_name(semantic_name)?;
    let global_base = globalize_agent_name(&parsed.family_name, owner)?;
    match parsed.member_role {
        Some(role) => {
            validate_path_component(&global_base)?;
            validate_path_component(&role)?;
            Ok(AgentLinkTargetWire {
                kind: "family".to_string(),
                path: format!("families/{global_base}.md"),
                anchor: Some(format!("member-{role}")),
            })
        }
        None => {
            validate_path_component(&global_base)?;
            Ok(AgentLinkTargetWire {
                kind: "agent".to_string(),
                path: format!("agents/{global_base}/README.md"),
                anchor: None,
            })
        }
    }
}

pub(crate) fn canonical_global_local_name(
    global_name: &str,
    owner: &AgentOwnerIdentity,
) -> Result<String, AgentIdentityError> {
    owner.validate()?;
    let normalized = normalize_agent_archive_name(global_name)?;
    if normalized != global_name {
        return Err(AgentIdentityError::InvalidAgentName {
            name: global_name.to_string(),
            reason: "canonical global names must not carry an archive prefix"
                .to_string(),
        });
    }
    strip_global_agent_name(global_name, owner)
}

fn strip_source_global_name(
    global_name: &str,
    source: &AgentSourceOwnerIdentity,
) -> Result<String, AgentIdentityError> {
    source.validate()?;
    match source {
        AgentSourceOwnerIdentity::V2 { owner } => {
            strip_global_agent_name(global_name, owner)
        }
        AgentSourceOwnerIdentity::UsernameUnknownV1 { machine_name } => {
            let normalized = normalize_agent_archive_name(global_name)?;
            let prefix = format!("{machine_name}.");
            let Some(local_name) = normalized.strip_prefix(&prefix) else {
                return Err(AgentIdentityError::LegacyMachineMismatch {
                    name: global_name.to_string(),
                    actual_machine: normalized
                        .split('.')
                        .next()
                        .unwrap_or_default()
                        .to_string(),
                    expected_machine: machine_name.clone(),
                });
            };
            validate_historical_semantic_name(local_name)?;
            Ok(local_name.to_string())
        }
    }
}

fn owner_prefix(owner: &AgentOwnerIdentity) -> String {
    format!("{}.{}.", owner.username, owner.machine_name)
}

fn owner_mismatch(
    name: &str,
    owner: &AgentOwnerIdentity,
) -> AgentIdentityError {
    AgentIdentityError::GlobalOwnerMismatch {
        name: name.to_string(),
        username: owner.username.clone(),
        machine_name: owner.machine_name.clone(),
    }
}

fn parse_normalized_family_name(
    normalized: &str,
) -> Result<AgentFamilyNameWire, AgentIdentityError> {
    let (family_name, member_role) =
        parse_normalized_family_name_unchecked(normalized);
    Ok(match member_role {
        None => AgentFamilyNameWire {
            kind: "solo".to_string(),
            family_name: family_name.to_string(),
            member_role: None,
        },
        Some(role) => AgentFamilyNameWire {
            kind: "member".to_string(),
            family_name: family_name.to_string(),
            member_role: Some(role.to_string()),
        },
    })
}

fn validate_semantic_name(name: &str) -> Result<(), AgentIdentityError> {
    validate_historical_semantic_name(name)?;
    validate_new_family_name(name)
}

fn validate_historical_semantic_name(
    name: &str,
) -> Result<(), AgentIdentityError> {
    if name.is_empty() {
        return Err(AgentIdentityError::EmptyAgentName);
    }
    if name.len() > MAX_AGENT_NAME_BYTES {
        return Err(AgentIdentityError::InvalidAgentName {
            name: name.to_string(),
            reason: format!(
                "name exceeds the {MAX_AGENT_NAME_BYTES}-byte limit"
            ),
        });
    }
    if name.contains('/') || name.contains('\\') || name.contains('\0') {
        return Err(AgentIdentityError::InvalidAgentName {
            name: name.to_string(),
            reason: "path separators and NUL are forbidden".to_string(),
        });
    }
    if name.chars().any(char::is_control) {
        return Err(AgentIdentityError::InvalidAgentName {
            name: name.to_string(),
            reason: "control characters are forbidden".to_string(),
        });
    }
    validate_dotted_base(name)
}

fn parse_normalized_family_name_unchecked(name: &str) -> (&str, Option<&str>) {
    let terminal_start = name
        .rfind('.')
        .map_or(0, |separator| separator.saturating_add(1));
    let terminal = &name[terminal_start..];
    let mut matches = terminal.match_indices("--");
    match (matches.next(), matches.next()) {
        (Some((relative_index, _)), None) => {
            let index = terminal_start + relative_index;
            let base = &name[..index];
            let role = &name[index + 2..];
            if base.is_empty() || role.is_empty() {
                (name, None)
            } else {
                (base, Some(role))
            }
        }
        _ => (name, None),
    }
}

fn validate_new_family_name(name: &str) -> Result<(), AgentIdentityError> {
    let delimiter_count = name.match_indices("--").count();
    match delimiter_count {
        0 => Ok(()),
        1 => {
            let (base, role) = name.rsplit_once("--").expect("one match");
            if base.is_empty() || role.is_empty() || role.contains('.') {
                return Err(AgentIdentityError::InvalidFamilyName {
                    name: name.to_string(),
                });
            }
            validate_dotted_base(base)?;
            validate_simple_segment(role, name)
        }
        _ => Err(AgentIdentityError::InvalidFamilyName {
            name: name.to_string(),
        }),
    }
}

fn historical_hood_segment(segment: &str) -> &str {
    segment.split_once("--").map_or(segment, |(hood, _)| {
        if hood.is_empty() {
            segment
        } else {
            hood
        }
    })
}

fn historical_family_scope(family_name: &str) -> String {
    let (first, suffix) =
        family_name.split_once('.').unwrap_or((family_name, ""));
    let hood = historical_hood_segment(first);
    if suffix.is_empty() {
        hood.to_string()
    } else {
        format!("{hood}.{suffix}")
    }
}

fn validate_dotted_base(name: &str) -> Result<(), AgentIdentityError> {
    if name.is_empty() {
        return Err(AgentIdentityError::EmptyAgentName);
    }
    for segment in name.split('.') {
        validate_simple_segment(segment, name)?;
    }
    Ok(())
}

fn validate_simple_segment(
    segment: &str,
    full_name: &str,
) -> Result<(), AgentIdentityError> {
    if segment.is_empty() {
        return Err(AgentIdentityError::InvalidAgentName {
            name: full_name.to_string(),
            reason: "empty dot segment or traversal spelling".to_string(),
        });
    }
    if !segment.bytes().all(|byte| {
        byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_'
    }) {
        return Err(AgentIdentityError::InvalidAgentName {
            name: full_name.to_string(),
            reason: format!("unsafe segment '{segment}'"),
        });
    }
    Ok(())
}

fn validate_path_component(value: &str) -> Result<(), AgentIdentityError> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || value.contains('/')
        || value.contains('\\')
        || value.chars().any(char::is_control)
    {
        Err(AgentIdentityError::InvalidAgentName {
            name: value.to_string(),
            reason: "unsafe generated path component".to_string(),
        })
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owner(username: &str, machine_name: &str) -> AgentOwnerIdentity {
        AgentOwnerIdentity::new(username, machine_name).unwrap()
    }

    #[test]
    fn username_and_owner_validation_matrix() {
        for value in ["a", "alice", "alice2", "a-b", "a_b", "2fast"] {
            assert!(validate_agent_username(value).is_ok(), "{value}");
        }
        for value in [
            "", "Alice", ".alice", "alice.", "-alice", "alice-", "_alice",
            "alice_", "a--b", "a/b", "a\\b", "a.b", "a\nb", "agents",
            "internal", "sase",
        ] {
            assert!(validate_agent_username(value).is_err(), "{value}");
        }

        assert!(AgentOwnerIdentity::new("alice", "athena").is_ok());
        for machine in ["", "Athena", "athena1", "a.b", "with-dash"] {
            let error = AgentOwnerIdentity::new("alice", machine).unwrap_err();
            assert!(error.to_string().contains(machine), "{machine}: {error}");
        }
    }

    #[test]
    fn ownership_classification_never_parses_names() {
        let target = owner("alice", "athena");
        let cases = [
            (
                AgentSourceOwnerIdentity::V2 {
                    owner: target.clone(),
                },
                AgentOwnershipClassification::ExactOwner,
            ),
            (
                AgentSourceOwnerIdentity::V2 {
                    owner: owner("alice", "zeus"),
                },
                AgentOwnershipClassification::SameUserOtherMachine,
            ),
            (
                AgentSourceOwnerIdentity::V2 {
                    owner: owner("bob", "athena"),
                },
                AgentOwnershipClassification::OtherUser,
            ),
            (
                AgentSourceOwnerIdentity::UsernameUnknownV1 {
                    machine_name: "athena".to_string(),
                },
                AgentOwnershipClassification::UsernameUnknownV1,
            ),
        ];
        for (source, expected) in cases {
            assert_eq!(
                classify_agent_ownership(&source, &target).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn legacy_v1_group_ownership_evidence_matrix() {
        let target = owner("alice", "athena");
        for (group_machine_name, machine_matches) in
            [("athena", true), ("zeus", false)]
        {
            for v2_hood_published in [false, true] {
                for proven_entry_count in [0, 1, 3] {
                    let evidence = LegacyV1GroupOwnershipEvidence {
                        v2_hood_published,
                        proven_entry_count,
                        total_entry_count: 3,
                    };
                    let expected = if machine_matches
                        && (v2_hood_published || proven_entry_count > 0)
                    {
                        LegacyV1GroupOwnershipClassification::OwnerObserved
                    } else {
                        LegacyV1GroupOwnershipClassification::Foreign
                    };
                    assert_eq!(
                        classify_legacy_v1_group_ownership(
                            group_machine_name,
                            &target,
                            &evidence,
                        )
                        .unwrap(),
                        expected,
                        "machine={group_machine_name}, v2={v2_hood_published}, proven={proven_entry_count}",
                    );
                }
            }
        }
    }

    #[test]
    fn legacy_v1_group_ownership_rejects_impossible_evidence() {
        let error = classify_legacy_v1_group_ownership(
            "athena",
            &owner("alice", "athena"),
            &LegacyV1GroupOwnershipEvidence {
                v2_hood_published: false,
                proven_entry_count: 2,
                total_entry_count: 1,
            },
        )
        .unwrap_err();
        assert!(matches!(
            error,
            AgentIdentityError::InvalidLegacyV1GroupOwnershipEvidence { .. }
        ));
    }

    #[test]
    fn globalization_normalizes_archive_and_round_trips() {
        let alice = owner("alice", "athena");
        for local in ["foo", "foo.bar", "foo.bar--code"] {
            let global = globalize_agent_name(local, &alice).unwrap();
            assert_eq!(global, format!("alice.athena.{local}"));
            assert_eq!(
                strip_global_agent_name(&global, &alice).unwrap(),
                local
            );
            assert_eq!(globalize_agent_name(&global, &alice).unwrap(), global);
        }
        assert_eq!(
            globalize_agent_name("260722.foo", &alice).unwrap(),
            "alice.athena.foo"
        );
        assert_eq!(
            normalize_agent_archive_name("260722.260721.foo").unwrap(),
            "260721.foo"
        );
    }

    #[test]
    fn legacy_globalization_verifies_machine_hood() {
        let alice = owner("alice", "athena");
        assert_eq!(
            globalize_legacy_agent_name("athena.foo.bar", &alice).unwrap(),
            "alice.athena.foo.bar"
        );
        assert!(matches!(
            globalize_legacy_agent_name("zeus.foo", &alice),
            Err(AgentIdentityError::LegacyMachineMismatch { .. })
        ));
        assert!(matches!(
            globalize_legacy_agent_name("foo", &alice),
            Err(AgentIdentityError::MalformedLegacyName { .. })
        ));
        assert!(globalize_legacy_agent_name("bad-machine.foo", &alice).is_err());
    }

    #[test]
    fn explicit_owner_prevents_mismatched_strip() {
        let alice = owner("alice", "athena");
        let bob = owner("bob", "athena");
        assert!(matches!(
            strip_global_agent_name("bob.athena.foo", &alice),
            Err(AgentIdentityError::GlobalOwnerMismatch { .. })
        ));
        assert_eq!(
            strip_global_agent_name("bob.athena.foo", &bob).unwrap(),
            "foo"
        );
    }

    #[test]
    fn localization_covers_all_owner_cases() {
        let target = owner("alice", "athena");
        let exact = AgentSourceOwnerIdentity::V2 {
            owner: target.clone(),
        };
        assert_eq!(
            localize_agent_name("alice.athena.foo", &exact, &target).unwrap(),
            "foo"
        );

        let same_user = AgentSourceOwnerIdentity::V2 {
            owner: owner("alice", "zeus"),
        };
        assert_eq!(
            localize_agent_name("alice.zeus.foo", &same_user, &target).unwrap(),
            "zeus.foo"
        );

        let other_user = AgentSourceOwnerIdentity::V2 {
            owner: owner("bob", "athena"),
        };
        assert_eq!(
            localize_agent_name("bob.athena.foo", &other_user, &target)
                .unwrap(),
            "bob.athena.foo"
        );

        let legacy = AgentSourceOwnerIdentity::UsernameUnknownV1 {
            machine_name: "zeus".to_string(),
        };
        assert_eq!(
            localize_agent_name("zeus.foo", &legacy, &target).unwrap(),
            "zeus.foo"
        );
    }

    #[test]
    fn unsafe_names_and_empty_remainders_fail() {
        let alice = owner("alice", "athena");
        for value in [
            "", ".", "..", "foo..bar", "foo/bar", "foo\\bar", "foo\nbar",
            "260722.",
        ] {
            assert!(globalize_agent_name(value, &alice).is_err(), "{value}");
        }
        assert!(strip_global_agent_name("alice.athena.", &alice).is_err());

        for value in ["foo--", "--code", "foo--code.bar", "foo--code--test"] {
            assert!(matches!(
                validate_agent_name(value),
                Err(AgentIdentityError::InvalidFamilyName { .. })
            ));
        }
        validate_agent_name("foo.bar--code").unwrap();
    }

    #[test]
    fn family_hood_ancestors_and_membership_are_canonical() {
        let parsed = parse_agent_family_name("foo.bar.baz--code").unwrap();
        assert_eq!(parsed.kind, "member");
        assert_eq!(parsed.family_name, "foo.bar.baz");
        assert_eq!(parsed.member_role.as_deref(), Some("code"));
        assert_eq!(
            parse_agent_family_name("foo.bar").unwrap(),
            AgentFamilyNameWire {
                kind: "solo".to_string(),
                family_name: "foo.bar".to_string(),
                member_role: None,
            }
        );
        assert_eq!(agent_local_hood("foo.bar.baz--code").unwrap(), "foo");
        assert_eq!(
            agent_name_ancestors("foo.bar.baz--code").unwrap(),
            ["foo", "foo.bar", "foo.bar.baz"]
        );
        assert!(agent_name_in_hood("foo.bar--code", "foo").unwrap());
        assert!(!agent_name_in_hood("foobar.baz", "foo").unwrap());
    }

    #[test]
    fn historical_family_classification_is_total_and_canonical() {
        let alice = owner("alice", "athena");
        let cases = [
            (
                "4x--epic.f-0",
                "solo",
                "4x--epic.f-0",
                None,
                "4x",
                vec!["4x", "4x--epic.f-0"],
            ),
            (
                "fi--code.f0",
                "solo",
                "fi--code.f0",
                None,
                "fi",
                vec!["fi", "fi--code.f0"],
            ),
            (
                "fi--code.f0--plan",
                "member",
                "fi--code.f0",
                Some("plan"),
                "fi",
                vec!["fi", "fi--code.f0"],
            ),
            (
                "fi--code.f0--code",
                "member",
                "fi--code.f0",
                Some("code"),
                "fi",
                vec!["fi", "fi--code.f0"],
            ),
        ];
        for (name, kind, family_name, member_role, hood, ancestors) in cases {
            let parsed = parse_agent_family_name(name).unwrap();
            assert_eq!(parsed.kind, kind, "{name}");
            assert_eq!(parsed.family_name, family_name, "{name}");
            assert_eq!(parsed.member_role.as_deref(), member_role, "{name}");
            assert_eq!(agent_local_hood(name).unwrap(), hood, "{name}");
            assert_eq!(
                agent_name_ancestors(name).unwrap(),
                ancestors,
                "{name}"
            );
            assert!(agent_name_in_hood(name, hood).unwrap(), "{name}");
            assert!(!agent_name_in_hood(name, "other").unwrap(), "{name}");
            assert!(agent_link_target(name, &alice).is_ok(), "{name}");

            let global = globalize_agent_name(name, &alice).unwrap();
            assert_eq!(
                globalize_agent_name(&global, &alice).unwrap(),
                global,
                "{name}"
            );
            assert_eq!(
                strip_global_agent_name(&global, &alice).unwrap(),
                name,
                "{name}"
            );
            assert_eq!(
                parse_agent_family_name(&parsed.family_name)
                    .unwrap()
                    .family_name,
                parsed.family_name,
                "{name}"
            );
        }
    }

    #[test]
    fn hood_membership_never_raises_for_historical_candidates() {
        for name in [
            "",
            ".",
            "..",
            "foo",
            "foo.bar",
            "foobar",
            "4x--epic.f-0",
            "fi--code.f0",
            "fi--code.f0--plan",
            "fi--code.f0--code",
            "foo--code--test",
            "foo/bar",
            "foo\\bar",
            "foo\nbar",
        ] {
            assert!(agent_name_in_hood(name, "foo").is_ok(), "{name:?}");
        }
        assert!(agent_name_in_hood("foo", "foo--code").is_err());
        assert!(!agent_name_in_hood("foobar", "foo").unwrap());
    }

    #[test]
    fn link_targets_distinguish_family_and_solo() {
        let alice = owner("alice", "athena");
        assert_eq!(
            agent_link_target("foo.bar--code", &alice).unwrap(),
            AgentLinkTargetWire {
                kind: "family".to_string(),
                path: "families/alice.athena.foo.bar.md".to_string(),
                anchor: Some("member-code".to_string()),
            }
        );
        assert_eq!(
            agent_link_target("foo.bar", &alice).unwrap(),
            AgentLinkTargetWire {
                kind: "agent".to_string(),
                path: "agents/alice.athena.foo.bar/README.md".to_string(),
                anchor: None,
            }
        );
    }
}
