use super::error::FleetContractError;
use super::error::FLEET_CONTRACT_SCHEMA_VERSION;
use super::error::MAX_LABEL_BYTES;
use super::validation::length_key;
use super::validation::validate_identifier;
use super::validation::validate_installation_id;
use super::validation::validate_label;
use super::validation::validate_schema;
use serde::{Deserialize, Serialize};

/// Origin locator: the installation identity only.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(deny_unknown_fields)]
pub struct OriginLocatorWire {
    pub schema_version: u32,
    pub installation_id: String,
}

/// Project locator: origin plus a portable project ID, never a checkout path.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(deny_unknown_fields)]
pub struct ProjectLocatorWire {
    pub schema_version: u32,
    pub origin: OriginLocatorWire,
    pub project_id: String,
}

/// Stable logical agent/agent session locator.
///
/// Human names and provider metadata remain labels outside this identity.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(deny_unknown_fields)]
pub struct LogicalAgentLocatorWire {
    pub schema_version: u32,
    pub project: ProjectLocatorWire,
    pub agent_id: String,
    #[serde(alias = "family_id")]
    pub agent_session_id: Option<String>,
}

/// Exact turn/run/attempt locator required for mutation targets.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(deny_unknown_fields)]
pub struct AgentInstanceLocatorWire {
    pub schema_version: u32,
    pub logical: LogicalAgentLocatorWire,
    // legacy sase-shell spelling; flips in contract-flip
    #[serde(rename = "shell_id", alias = "turn_id")]
    pub turn_id: String,
    pub run_id: String,
    pub attempt_id: String,
}

/// Owner-qualified display labels associated with a locator.
///
/// These labels make machine-hood names useful for lookup and display without
/// deriving the origin identity from those names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerDisplayNameRequestWire {
    pub schema_version: u32,
    pub logical_locator: LogicalAgentLocatorWire,
    pub owner_username: String,
    pub owner_machine_name: String,
    pub display_name: String,
    pub display_alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerDisplayNameWire {
    pub schema_version: u32,
    pub logical_locator: LogicalAgentLocatorWire,
    pub logical_key: String,
    pub owner_label: String,
    pub display_name: String,
    pub display_alias: Option<String>,
}

pub fn logical_locator_key(
    locator: &LogicalAgentLocatorWire,
) -> Result<String, FleetContractError> {
    locator.validate()?;
    Ok(logical_key_unchecked(locator))
}

pub fn instance_locator_key(
    locator: &AgentInstanceLocatorWire,
) -> Result<String, FleetContractError> {
    locator.validate()?;
    Ok(instance_key_unchecked(locator))
}

pub fn associate_owner_display_name(
    request: &OwnerDisplayNameRequestWire,
) -> Result<OwnerDisplayNameWire, FleetContractError> {
    validate_schema("owner display name request", request.schema_version)?;
    request.logical_locator.validate()?;
    validate_identifier("owner_username", &request.owner_username)?;
    validate_identifier("owner_machine_name", &request.owner_machine_name)?;
    validate_label("display_name", &request.display_name, MAX_LABEL_BYTES)?;
    if let Some(alias) = &request.display_alias {
        validate_label("display_alias", alias, MAX_LABEL_BYTES)?;
    }
    let logical_key = logical_key_unchecked(&request.logical_locator);
    Ok(OwnerDisplayNameWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        logical_locator: request.logical_locator.clone(),
        logical_key,
        owner_label: format!(
            "{}.{}",
            request.owner_username.trim(),
            request.owner_machine_name.trim()
        ),
        display_name: request.display_name.trim().to_string(),
        display_alias: request
            .display_alias
            .as_ref()
            .map(|value| value.trim().to_string()),
    })
}

impl OriginLocatorWire {
    pub fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("origin locator", self.schema_version)?;
        validate_installation_id(&self.installation_id)
    }
}

impl ProjectLocatorWire {
    pub fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("project locator", self.schema_version)?;
        self.origin.validate()?;
        validate_identifier("project_id", &self.project_id)
    }
}

impl LogicalAgentLocatorWire {
    pub fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("logical agent locator", self.schema_version)?;
        self.project.validate()?;
        validate_identifier("agent_id", &self.agent_id)?;
        if let Some(agent_session_id) = &self.agent_session_id {
            validate_identifier("agent_session_id", agent_session_id)?;
        }
        Ok(())
    }
}

impl AgentInstanceLocatorWire {
    pub fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("agent instance locator", self.schema_version)?;
        self.logical.validate()?;
        validate_identifier("shell_id", &self.turn_id)?;
        validate_identifier("run_id", &self.run_id)?;
        validate_identifier("attempt_id", &self.attempt_id)?;
        Ok(())
    }
}

pub(crate) const AGENT_SESSION_KEY_SEGMENT: &str = "session";
const LEGACY_AGENT_SESSION_KEY_SEGMENT: &str = "family";

pub(crate) fn logical_key_unchecked(
    locator: &LogicalAgentLocatorWire,
) -> String {
    length_key([
        ("origin", locator.project.origin.installation_id.as_str()),
        ("project", locator.project.project_id.as_str()),
        (
            AGENT_SESSION_KEY_SEGMENT,
            locator.agent_session_id.as_deref().unwrap_or(""),
        ),
        ("agent", locator.agent_id.as_str()),
    ])
}

/// Whether a stored logical key identifies `locator`.
///
/// Emitted keys use the canonical `session:` segment; legacy `family:` keys
/// remain comparable while durable fleet state is migrated.
pub(crate) fn logical_key_matches(
    stored: &str,
    locator: &LogicalAgentLocatorWire,
) -> bool {
    stored == logical_key_unchecked(locator)
        || canonical_logical_key(stored) == logical_key_unchecked(locator)
}

/// Canonical form for comparing stored logical keys: legacy `family:`
/// segments fold to `session`, and `family-<hex>` fallback ids in the agent
/// session segment compare equal to emitted `session-<hex>` values. Plain identifiers
/// compare exactly. Malformed keys compare exactly.
pub(crate) fn canonical_logical_key(key: &str) -> String {
    let Some(segments) = split_length_key(key) else {
        return key.to_string();
    };
    let mut out = String::from("v1");
    for (name, value) in segments {
        let name = if name == LEGACY_AGENT_SESSION_KEY_SEGMENT {
            AGENT_SESSION_KEY_SEGMENT
        } else {
            name.as_str()
        };
        let value = if name == AGENT_SESSION_KEY_SEGMENT {
            canonical_agent_session_id(&value)
        } else {
            value
        };
        out.push('|');
        out.push_str(name);
        out.push(':');
        out.push_str(&value.len().to_string());
        out.push(':');
        out.push_str(&value);
    }
    out
}

/// Fold a legacy `family-<hex>` fallback id to canonical `session-<hex>`.
/// Anything else compares exactly.
fn canonical_agent_session_id(value: &str) -> String {
    if let Some(digest) = value.strip_prefix("family-") {
        if digest.len() == 16
            && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return format!("session-{digest}");
        }
    }
    value.to_string()
}

fn split_length_key(key: &str) -> Option<Vec<(String, String)>> {
    let mut cursor = key.strip_prefix("v1")?;
    let mut segments = Vec::new();
    while let Some(stripped) = cursor.strip_prefix('|') {
        let name_end = stripped.find(':')?;
        let name = &stripped[..name_end];
        let after_name = &stripped[name_end + 1..];
        let len_end = after_name.find(':')?;
        let len: usize = after_name[..len_end].parse().ok()?;
        let value = after_name.get(len_end + 1..)?.get(..len)?;
        segments.push((name.to_string(), value.to_string()));
        cursor = &after_name[len_end + 1 + len..];
    }
    if cursor.is_empty() {
        Some(segments)
    } else {
        None
    }
}

pub(crate) fn instance_key_unchecked(
    locator: &AgentInstanceLocatorWire,
) -> String {
    format!(
        "{}|{}",
        logical_key_unchecked(&locator.logical),
        length_key([
            // legacy sase-shell spelling; flips in contract-flip
            ("shell", locator.turn_id.as_str()),
            ("run", locator.run_id.as_str()),
            ("attempt", locator.attempt_id.as_str()),
        ])
    )
}

/// Whether a stored instance key identifies `locator`.
///
/// Emitted keys use the canonical `shell:` segment; a stored `turn:` segment
/// compares equal while durable fleet state is migrated. Fallback
/// `turn-<hex>` ids compare equal to emitted `shell-<hex>` values.
pub(crate) fn instance_key_matches(
    stored: &str,
    locator: &AgentInstanceLocatorWire,
) -> bool {
    stored == instance_key_unchecked(locator)
        || canonical_instance_key(stored) == instance_key_unchecked(locator)
}

/// Canonical form for comparing stored instance keys: `turn:` segments fold
/// to `shell:`, and `turn-<hex>` fallback ids fold to `shell-<hex>`.
/// Malformed keys compare exactly.
///
/// An instance key is two concatenated length-keys:
/// `<logical-key>|<instance-key>`, each starting with `v1`.
pub(crate) fn canonical_instance_key(key: &str) -> String {
    let Some((logical, instance)) = split_composite_instance_key(key) else {
        return key.to_string();
    };
    let canonical_logical = canonical_logical_key(logical);
    let Some(segments) = split_length_key(instance) else {
        return key.to_string();
    };
    let mut out = String::from("v1");
    for (name, value) in segments {
        let name = if name == "turn" {
            "shell".to_string()
        } else {
            name
        };
        let value = if name == "shell" {
            canonical_turn_fallback_id(&value)
        } else {
            value
        };
        out.push('|');
        out.push_str(&name);
        out.push(':');
        out.push_str(&value.len().to_string());
        out.push(':');
        out.push_str(&value);
    }
    format!("{canonical_logical}|{out}")
}

/// Split a composite `<logical-key>|<instance-key>` into its two parts.
/// Returns `None` when the logical prefix does not parse as four
/// length-key segments followed by a second `v1` key.
fn split_composite_instance_key(key: &str) -> Option<(&str, &str)> {
    let mut cursor = key.strip_prefix("v1")?;
    for _ in 0..4 {
        let stripped = cursor.strip_prefix('|')?;
        let name_end = stripped.find(':')?;
        let after_name = &stripped[name_end + 1..];
        let len_end = after_name.find(':')?;
        let len: usize = after_name[..len_end].parse().ok()?;
        cursor = &after_name[len_end + 1 + len..];
    }
    let instance = cursor.strip_prefix('|')?.strip_prefix("v1")?;
    // Re-slice the logical prefix by length so callers keep the exact
    // original bytes for canonicalization.
    let logical_len = key.len() - cursor.len();
    let (logical, _) = key.split_at(logical_len);
    let instance_key = &key[logical_len + 1..];
    debug_assert!(instance_key.strip_prefix("v1").is_some());
    let _ = instance;
    Some((logical, instance_key))
}

/// Fold a `turn-<hex>` fallback id to canonical `shell-<hex>`.
/// Anything else compares exactly.
pub(crate) fn canonical_turn_fallback_id(value: &str) -> String {
    if let Some(digest) = value.strip_prefix("turn-") {
        if digest.len() == 16
            && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return format!("shell-{digest}");
        }
    }
    value.to_string()
}

/// Whether two fallback ids are equal under `turn-`/`shell-` spelling.
/// `turn-<hex>` and `shell-<hex>` with the same digest are equal.
pub fn fallback_turn_shell_ids_equal(first: &str, second: &str) -> bool {
    first == second
        || canonical_turn_fallback_id(first)
            == canonical_turn_fallback_id(second)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fleet_contract::FLEET_INSTALLATION_ID_PREFIX;

    fn locator(agent_session_id: Option<&str>) -> LogicalAgentLocatorWire {
        LogicalAgentLocatorWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            project: ProjectLocatorWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                origin: OriginLocatorWire {
                    schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                    installation_id: format!(
                        "{FLEET_INSTALLATION_ID_PREFIX}{}",
                        "a".repeat(64)
                    ),
                },
                project_id: "project-1".to_string(),
            },
            agent_id: "worker".to_string(),
            agent_session_id: agent_session_id.map(str::to_string),
        }
    }

    fn locator_json(agent_session_key: &str) -> serde_json::Value {
        serde_json::json!({
            "schema_version": FLEET_CONTRACT_SCHEMA_VERSION,
            "project": {
                "schema_version": FLEET_CONTRACT_SCHEMA_VERSION,
                "origin": {
                    "schema_version": FLEET_CONTRACT_SCHEMA_VERSION,
                    "installation_id": format!(
                        "{FLEET_INSTALLATION_ID_PREFIX}{}",
                        "a".repeat(64)
                    ),
                },
                "project_id": "project-1",
            },
            "agent_id": "worker",
            agent_session_key: "session-1",
        })
    }

    #[test]
    fn locator_accepts_legacy_spelling_but_emits_canonical() {
        let legacy: LogicalAgentLocatorWire =
            serde_json::from_value(locator_json("family_id")).unwrap();
        let new: LogicalAgentLocatorWire =
            serde_json::from_value(locator_json("agent_session_id")).unwrap();
        assert_eq!(new, legacy);
        assert_eq!(new.agent_session_id.as_deref(), Some("session-1"));
        let encoded = serde_json::to_value(&new).unwrap();
        assert_eq!(encoded["agent_session_id"], "session-1");
        assert!(encoded.get("family_id").is_none());
    }

    #[test]
    fn logical_key_matches_accepts_both_segments() {
        let wire = locator(Some("session-1"));
        let emitted = logical_key_unchecked(&wire);
        assert!(emitted.contains("|session:"));
        assert!(logical_key_matches(&emitted, &wire));
        let legacy_spelled = emitted.replacen("|session:", "|family:", 1);
        assert!(logical_key_matches(&legacy_spelled, &wire));
        let other = locator(None);
        assert!(!logical_key_matches(&emitted, &other));
        assert!(!logical_key_matches("not-a-key", &wire));
    }

    /// Rebuild `emitted` with legacy family names, keeping every length prefix
    /// well-formed. (`family-` is one byte shorter than `session-`.)
    fn legacy_spelled_key(emitted: &str, digest: &str) -> String {
        emitted.replacen(
            &format!("|session:24:session-{digest}"),
            &format!("|family:23:family-{digest}"),
            1,
        )
    }

    #[test]
    fn fallback_ids_compare_equal_across_spellings() {
        let digest = "0123456789abcdef";
        let wire = locator(Some(&format!("session-{digest}")));
        let emitted = logical_key_unchecked(&wire);
        let legacy_spelled = legacy_spelled_key(&emitted, digest);
        assert_ne!(legacy_spelled, emitted);
        assert!(logical_key_matches(&legacy_spelled, &wire));
        let other_digest = locator(Some("session-abcdef0123456789"));
        assert!(!logical_key_matches(&legacy_spelled, &other_digest));
        // The segment alias is value-independent, but fallback-id
        // folding only applies to `<prefix>-<16 hex>` values.
        let plain = locator(Some("lane"));
        let plain_key = logical_key_unchecked(&plain);
        assert!(logical_key_matches(
            &plain_key.replace("|session:4:lane", "|family:4:lane"),
            &plain,
        ));
        assert!(!logical_key_matches(
            &plain_key.replace("|session:4:lane", "|session:12:family-lane"),
            &plain,
        ));
        let non_hex = locator(Some("family-xyz"));
        assert!(!logical_key_matches(
            &logical_key_unchecked(&locator(Some("session-xyz"))),
            &non_hex,
        ));
    }

    #[test]
    fn canonical_logical_key_folds_legacy_segment_and_fallback_id() {
        let digest = "0123456789abcdef";
        let wire = locator(Some(&format!("session-{digest}")));
        let emitted = logical_key_unchecked(&wire);
        let legacy_key = legacy_spelled_key(&emitted, digest);
        assert_ne!(legacy_key, emitted);
        assert_eq!(canonical_logical_key(&legacy_key), emitted);
        assert_eq!(canonical_logical_key("not-a-key"), "not-a-key");
    }

    fn instance_locator(turn_id: &str) -> AgentInstanceLocatorWire {
        AgentInstanceLocatorWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            logical: locator(Some("session-1")),
            turn_id: turn_id.to_string(),
            run_id: "run-1".to_string(),
            attempt_id: "attempt-0".to_string(),
        }
    }

    #[test]
    fn instance_locator_accepts_turn_spelling_but_emits_shell() {
        let legacy: AgentInstanceLocatorWire =
            serde_json::from_value(serde_json::json!({
                "schema_version": FLEET_CONTRACT_SCHEMA_VERSION,
                "logical": serde_json::to_value(locator(Some("session-1"))).unwrap(),
                "shell_id": "shell-1",
                "run_id": "run-1",
                "attempt_id": "attempt-0",
            }))
            .unwrap();
        let new: AgentInstanceLocatorWire =
            serde_json::from_value(serde_json::json!({
                "schema_version": FLEET_CONTRACT_SCHEMA_VERSION,
                "logical": serde_json::to_value(locator(Some("session-1"))).unwrap(),
                "turn_id": "shell-1",
                "run_id": "run-1",
                "attempt_id": "attempt-0",
            }))
            .unwrap();
        assert_eq!(legacy, new);
        assert_eq!(new.turn_id, "shell-1");
        let encoded = serde_json::to_value(&new).unwrap();
        assert_eq!(encoded["shell_id"], "shell-1");
        assert!(encoded.get("turn_id").is_none());
    }

    #[test]
    fn instance_key_matches_accepts_turn_segment_and_fallback() {
        let wire = instance_locator("shell-1");
        let emitted = instance_key_unchecked(&wire);
        assert!(emitted.contains("|shell:"));
        assert!(instance_key_matches(&emitted, &wire));
        // Stored `turn:` segment compares equal to the emitted `shell:`.
        let turn_spelled = emitted.replacen("|shell:", "|turn:", 1);
        assert_ne!(turn_spelled, emitted);
        assert!(instance_key_matches(&turn_spelled, &wire));
        assert_eq!(canonical_instance_key(&turn_spelled), emitted);
        // Fallback `turn-<hex>` compares equal to `shell-<hex>`.
        let digest = "0123456789abcdef";
        let fallback_wire = instance_locator(&format!("shell-{digest}"));
        let stored_turn_locator = instance_locator(&format!("turn-{digest}"));
        let stored_turn_emitted = instance_key_unchecked(&stored_turn_locator);
        // Re-spell only the segment name; the value keeps its own length.
        let fallback_turn =
            stored_turn_emitted.replacen("|shell:", "|turn:", 1);
        assert_ne!(fallback_turn, instance_key_unchecked(&fallback_wire));
        assert!(instance_key_matches(&fallback_turn, &fallback_wire));
        assert_eq!(
            canonical_instance_key(&fallback_turn),
            instance_key_unchecked(&fallback_wire)
        );
        assert!(fallback_turn_shell_ids_equal(
            &format!("shell-{digest}"),
            &format!("turn-{digest}"),
        ));
        assert!(!fallback_turn_shell_ids_equal(
            &format!("shell-{digest}"),
            "shell-abcdef0123456789",
        ));
        assert!(!instance_key_matches(&emitted, &instance_locator("other")));
        assert!(!instance_key_matches("not-a-key", &wire));
    }
}
