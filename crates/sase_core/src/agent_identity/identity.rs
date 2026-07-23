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

/// Strip at most one canonical `YYMMDD.` archive prefix and validate the
/// remaining name.
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
    validate_semantic_name(normalized)?;
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
        validate_semantic_name(remainder)?;
        return Ok(normalized);
    }
    validate_semantic_name(&normalized)?;
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
    validate_semantic_name(local_name)?;
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
    Ok(parsed
        .family_name
        .split('.')
        .next()
        .expect("validated name has a first segment")
        .to_string())
}

pub fn agent_name_in_hood(
    name: &str,
    hood: &str,
) -> Result<bool, AgentIdentityError> {
    let parsed = parse_agent_family_name(name)?;
    let normalized_hood = normalize_agent_archive_name(hood)?;
    if normalized_hood.contains("--") {
        return Err(AgentIdentityError::InvalidFamilyName {
            name: hood.to_string(),
        });
    }
    Ok(parsed.family_name == normalized_hood
        || parsed
            .family_name
            .strip_prefix(&normalized_hood)
            .is_some_and(|suffix| suffix.starts_with('.')))
}

pub fn agent_name_ancestors(
    name: &str,
) -> Result<Vec<String>, AgentIdentityError> {
    let parsed = parse_agent_family_name(name)?;
    let mut current = String::new();
    Ok(parsed
        .family_name
        .split('.')
        .map(|segment| {
            if !current.is_empty() {
                current.push('.');
            }
            current.push_str(segment);
            current.clone()
        })
        .collect())
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
            validate_semantic_name(local_name)?;
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
    let delimiter_count = normalized.match_indices("--").count();
    match delimiter_count {
        0 => Ok(AgentFamilyNameWire {
            kind: "solo".to_string(),
            family_name: normalized.to_string(),
            member_role: None,
        }),
        1 => {
            let (base, role) = normalized.rsplit_once("--").expect("one match");
            if base.is_empty() || role.is_empty() || role.contains('.') {
                return Err(AgentIdentityError::InvalidFamilyName {
                    name: normalized.to_string(),
                });
            }
            validate_dotted_base(base)?;
            validate_simple_segment(role, normalized)?;
            Ok(AgentFamilyNameWire {
                kind: "member".to_string(),
                family_name: base.to_string(),
                member_role: Some(role.to_string()),
            })
        }
        _ => Err(AgentIdentityError::InvalidFamilyName {
            name: normalized.to_string(),
        }),
    }
}

fn validate_semantic_name(name: &str) -> Result<(), AgentIdentityError> {
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
    let parsed = parse_normalized_family_name_unchecked(name)?;
    validate_dotted_base(parsed.0)?;
    if let Some(role) = parsed.1 {
        validate_simple_segment(role, name)?;
    }
    Ok(())
}

fn parse_normalized_family_name_unchecked(
    name: &str,
) -> Result<(&str, Option<&str>), AgentIdentityError> {
    let mut matches = name.match_indices("--");
    match (matches.next(), matches.next()) {
        (None, _) => Ok((name, None)),
        (Some((index, _)), None) => {
            let base = &name[..index];
            let role = &name[index + 2..];
            if base.is_empty() || role.is_empty() || role.contains('.') {
                Err(AgentIdentityError::InvalidFamilyName {
                    name: name.to_string(),
                })
            } else {
                Ok((base, Some(role)))
            }
        }
        _ => Err(AgentIdentityError::InvalidFamilyName {
            name: name.to_string(),
        }),
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
            "",
            ".",
            "..",
            "foo..bar",
            "foo/bar",
            "foo\\bar",
            "foo\nbar",
            "foo--",
            "--code",
            "foo--code--test",
            "260722.",
        ] {
            assert!(globalize_agent_name(value, &alice).is_err(), "{value}");
        }
        assert!(strip_global_agent_name("alice.athena.", &alice).is_err());
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

        for invalid in ["foo--code.bar", "foo--code--test", "--code", "foo--"] {
            assert!(parse_agent_family_name(invalid).is_err(), "{invalid}");
        }
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
