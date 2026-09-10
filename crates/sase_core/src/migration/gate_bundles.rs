//! Gate request bundle v2→v3 conversion contract.
//!
//! Deletion owner: sase-x7.14. Nothing here runs during startup, import,
//! completion, or an ordinary read; the migration kit calls plan/apply/verify
//! explicitly.

use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use sha2::{Digest, Sha256};

use super::digest::fingerprint;
use super::manifest::MigrationConflictRecord;
use super::MIGRATION_WIRE_SCHEMA_VERSION;

pub const GATE_REQUEST_SCHEMA_VERSION: u64 = 3;
pub const LEGACY_GATE_REQUEST_SCHEMA_VERSION: u64 = 2;

fn current_schema_version() -> u32 {
    MIGRATION_WIRE_SCHEMA_VERSION
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GateBundleConvertFactsWire {
    #[serde(default)]
    pub has_response: bool,
    #[serde(default)]
    pub has_cancellation: bool,
    #[serde(default)]
    pub deadline_passed: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GateBundleConvertPlanWire {
    #[serde(default = "current_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,
    pub source_schema_version: Option<u64>,
    pub intended_action: String,
    #[serde(default)]
    pub semantic_fingerprint: Option<String>,
    #[serde(default)]
    pub conflicts: Vec<MigrationConflictRecord>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GateBundleConvertApplyWire {
    #[serde(default = "current_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,
    pub intended_action: String,
    #[serde(default)]
    pub converted: Option<JsonValue>,
    #[serde(default)]
    pub semantic_fingerprint: Option<String>,
    #[serde(default)]
    pub conflicts: Vec<MigrationConflictRecord>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GateBundleConvertVerifyWire {
    #[serde(default = "current_schema_version")]
    pub schema_version: u32,
    pub identities_unchanged: bool,
    pub equal_semantics: bool,
    #[serde(default)]
    pub source_fingerprint: Option<String>,
    #[serde(default)]
    pub converted_fingerprint: Option<String>,
    #[serde(default)]
    pub conflicts: Vec<MigrationConflictRecord>,
}

pub fn plan(
    envelope: &JsonValue,
    facts: &GateBundleConvertFactsWire,
) -> GateBundleConvertPlanWire {
    let outcome = convert_inner(envelope, facts);
    GateBundleConvertPlanWire {
        schema_version: MIGRATION_WIRE_SCHEMA_VERSION,
        request_id: outcome.request_id,
        kind: outcome.kind,
        source_schema_version: outcome.source_schema_version,
        intended_action: outcome.intended_action,
        semantic_fingerprint: outcome.semantic_fingerprint,
        conflicts: outcome.conflicts,
    }
}

pub fn apply(
    envelope: &JsonValue,
    facts: &GateBundleConvertFactsWire,
) -> GateBundleConvertApplyWire {
    let outcome = convert_inner(envelope, facts);
    GateBundleConvertApplyWire {
        schema_version: MIGRATION_WIRE_SCHEMA_VERSION,
        request_id: outcome.request_id,
        kind: outcome.kind,
        intended_action: outcome.intended_action,
        converted: outcome.converted,
        semantic_fingerprint: outcome.semantic_fingerprint,
        conflicts: outcome.conflicts,
    }
}

pub fn verify(
    original: &JsonValue,
    converted: &JsonValue,
    _facts: &GateBundleConvertFactsWire,
) -> GateBundleConvertVerifyWire {
    let mut conflicts = Vec::new();
    let identities_unchanged = identity_fields_match(original, converted);
    if !identities_unchanged {
        conflicts.push(conflict(
            request_id(original).as_deref().unwrap_or("<unknown>"),
            "identity_changed",
            "request_id, kind, resources, or hashes.resources changed",
            None,
            None,
        ));
    }
    let converted_version = schema_version(converted);
    if converted_version != Some(GATE_REQUEST_SCHEMA_VERSION) {
        conflicts.push(conflict(
            request_id(converted).as_deref().unwrap_or("<unknown>"),
            "unsupported_schema",
            "converted envelope is not schema_version 3",
            None,
            None,
        ));
    }
    if let Some(expected_request) = converted
        .get("hashes")
        .and_then(JsonValue::as_object)
        .and_then(|hashes| hashes.get("request"))
        .and_then(JsonValue::as_str)
    {
        let actual = request_sha256(converted);
        if actual != expected_request {
            conflicts.push(conflict(
                request_id(converted).as_deref().unwrap_or("<unknown>"),
                "hash_mismatch",
                "converted hashes.request does not match the canonical digest",
                Some(expected_request.to_string()),
                Some(actual),
            ));
        }
    }
    if let (Some(branches), Some(primary)) = (
        converted.get("branches").and_then(JsonValue::as_array),
        converted.get("primary_branch"),
    ) {
        if branches.first() != Some(primary) {
            conflicts.push(conflict(
                request_id(converted).as_deref().unwrap_or("<unknown>"),
                "primary_branch_mismatch",
                "primary_branch does not match branches[0]",
                None,
                None,
            ));
        }
    }
    let source_fingerprint = fingerprint(original).ok();
    let converted_fingerprint = fingerprint(converted).ok();
    let equal_semantics = identities_unchanged && conflicts.is_empty();
    GateBundleConvertVerifyWire {
        schema_version: MIGRATION_WIRE_SCHEMA_VERSION,
        identities_unchanged,
        equal_semantics,
        source_fingerprint,
        converted_fingerprint,
        conflicts,
    }
}

struct ConvertOutcome {
    request_id: Option<String>,
    kind: Option<String>,
    source_schema_version: Option<u64>,
    intended_action: String,
    converted: Option<JsonValue>,
    semantic_fingerprint: Option<String>,
    conflicts: Vec<MigrationConflictRecord>,
}

fn convert_inner(
    envelope: &JsonValue,
    facts: &GateBundleConvertFactsWire,
) -> ConvertOutcome {
    let request_id = request_id(envelope);
    let kind = string_field(envelope, "kind");
    let source_schema_version = schema_version(envelope);
    let path = request_id
        .clone()
        .unwrap_or_else(|| "<unknown>".to_string());
    let mut conflicts = Vec::new();

    match source_schema_version {
        Some(version) if version == GATE_REQUEST_SCHEMA_VERSION => {
            return ConvertOutcome {
                request_id,
                kind,
                source_schema_version,
                intended_action: "noop".to_string(),
                converted: Some(envelope.clone()),
                semantic_fingerprint: fingerprint(envelope).ok(),
                conflicts,
            };
        }
        Some(1) => {
            conflicts.push(conflict(
                &path,
                "archive_only",
                "v1 envelopes were never accepted by the current validator",
                None,
                None,
            ));
        }
        Some(version) if version == LEGACY_GATE_REQUEST_SCHEMA_VERSION => {
            if is_in_flight(facts) {
                conflicts.push(conflict(
                    &path,
                    "in_flight",
                    "refusing to rewrite an in-flight gate bundle",
                    None,
                    None,
                ));
            }
            let Some(branches) =
                envelope.get("branches").and_then(JsonValue::as_array)
            else {
                conflicts.push(conflict(
                    &path,
                    "missing_branches",
                    "v2 envelope has no branches array to materialize primary_branch",
                    None,
                    None,
                ));
                return refused(
                    request_id,
                    kind,
                    source_schema_version,
                    conflicts,
                );
            };
            if branches.is_empty() {
                conflicts.push(conflict(
                    &path,
                    "missing_branches",
                    "v2 envelope has an empty branches array",
                    None,
                    None,
                ));
            }
            if !conflicts.is_empty() {
                return refused(
                    request_id,
                    kind,
                    source_schema_version,
                    conflicts,
                );
            }
            let mut converted = envelope.clone();
            if let Some(object) = converted.as_object_mut() {
                object.insert(
                    "schema_version".to_string(),
                    JsonValue::from(GATE_REQUEST_SCHEMA_VERSION),
                );
                object
                    .insert("primary_branch".to_string(), branches[0].clone());
            }
            let digest = request_sha256(&converted);
            if let Some(hashes) = converted
                .get_mut("hashes")
                .and_then(JsonValue::as_object_mut)
            {
                hashes.insert("request".to_string(), JsonValue::String(digest));
            } else if let Some(object) = converted.as_object_mut() {
                let mut hashes = JsonMap::new();
                hashes.insert("request".to_string(), JsonValue::String(digest));
                object.insert("hashes".to_string(), JsonValue::Object(hashes));
            }
            return ConvertOutcome {
                request_id,
                kind,
                source_schema_version,
                intended_action: "convert".to_string(),
                semantic_fingerprint: fingerprint(&converted).ok(),
                converted: Some(converted),
                conflicts,
            };
        }
        Some(version) => {
            conflicts.push(conflict(
                &path,
                "unsupported_schema",
                &format!("schema_version {version} is not a convertible gate envelope"),
                None,
                None,
            ));
        }
        None => {
            conflicts.push(conflict(
                &path,
                "unsupported_schema",
                "schema_version is missing",
                None,
                None,
            ));
        }
    }

    refused(request_id, kind, source_schema_version, conflicts)
}

fn refused(
    request_id: Option<String>,
    kind: Option<String>,
    source_schema_version: Option<u64>,
    conflicts: Vec<MigrationConflictRecord>,
) -> ConvertOutcome {
    ConvertOutcome {
        request_id,
        kind,
        source_schema_version,
        intended_action: "refuse".to_string(),
        converted: None,
        semantic_fingerprint: None,
        conflicts,
    }
}

fn is_in_flight(facts: &GateBundleConvertFactsWire) -> bool {
    !facts.has_response && !facts.has_cancellation && !facts.deadline_passed
}

fn schema_version(envelope: &JsonValue) -> Option<u64> {
    envelope.get("schema_version").and_then(JsonValue::as_u64)
}

fn request_id(envelope: &JsonValue) -> Option<String> {
    string_field(envelope, "request_id")
}

fn string_field(envelope: &JsonValue, key: &str) -> Option<String> {
    envelope
        .get(key)
        .and_then(JsonValue::as_str)
        .map(str::to_string)
}

fn identity_fields_match(original: &JsonValue, converted: &JsonValue) -> bool {
    [
        "request_id",
        "kind",
        "notification_id",
        "created_at",
        "resources",
    ]
    .iter()
    .all(|key| original.get(*key) == converted.get(*key))
        && original
            .get("hashes")
            .and_then(JsonValue::as_object)
            .and_then(|hashes| hashes.get("resources"))
            == converted
                .get("hashes")
                .and_then(JsonValue::as_object)
                .and_then(|hashes| hashes.get("resources"))
}

pub fn request_sha256(envelope: &JsonValue) -> String {
    let mut hashed = envelope.clone();
    if let Some(hashes) =
        hashed.get_mut("hashes").and_then(JsonValue::as_object_mut)
    {
        hashes.remove("request");
    }
    sha256_hex(&python_canonical_json(&hashed))
}

fn python_canonical_json(value: &JsonValue) -> Vec<u8> {
    let mut out = Vec::new();
    write_python_canonical(value, &mut out);
    out
}

fn write_python_canonical(value: &JsonValue, out: &mut Vec<u8>) {
    match value {
        JsonValue::Null => out.extend_from_slice(b"null"),
        JsonValue::Bool(true) => out.extend_from_slice(b"true"),
        JsonValue::Bool(false) => out.extend_from_slice(b"false"),
        JsonValue::Number(number) => {
            out.extend_from_slice(number.to_string().as_bytes())
        }
        JsonValue::String(text) => write_json_string(text, out),
        JsonValue::Array(items) => {
            out.push(b'[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                write_python_canonical(item, out);
            }
            out.push(b']');
        }
        JsonValue::Object(map) => {
            let mut keys: Vec<_> = map.keys().collect();
            keys.sort();
            out.push(b'{');
            for (index, key) in keys.iter().enumerate() {
                if index > 0 {
                    out.push(b',');
                }
                write_json_string(key, out);
                out.push(b':');
                write_python_canonical(&map[*key], out);
            }
            out.push(b'}');
        }
    }
}

fn write_json_string(text: &str, out: &mut Vec<u8>) {
    out.push(b'"');
    for ch in text.chars() {
        match ch {
            '"' => out.extend_from_slice(b"\\\""),
            '\\' => out.extend_from_slice(b"\\\\"),
            '\u{08}' => out.extend_from_slice(b"\\b"),
            '\u{0c}' => out.extend_from_slice(b"\\f"),
            '\n' => out.extend_from_slice(b"\\n"),
            '\r' => out.extend_from_slice(b"\\r"),
            '\t' => out.extend_from_slice(b"\\t"),
            c if (c as u32) < 0x20 => {
                let encoded = format!("\\u{:04x}", c as u32);
                out.extend_from_slice(encoded.as_bytes());
            }
            c => {
                let mut buf = [0_u8; 4];
                out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
            }
        }
    }
    out.push(b'"');
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn conflict(
    path: &str,
    kind: &str,
    detail: &str,
    expected: Option<String>,
    observed: Option<String>,
) -> MigrationConflictRecord {
    MigrationConflictRecord {
        schema_version: MIGRATION_WIRE_SCHEMA_VERSION,
        path: path.to_string(),
        kind: kind.to_string(),
        detail: Some(detail.to_string()),
        expected_fingerprint: expected,
        observed_fingerprint: observed,
        extensions: Default::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn v2_envelope() -> JsonValue {
        json!({
            "schema_version": 2,
            "kind": "plan",
            "request_id": "req-1",
            "branches": [["approve", "commit"], ["reject"]],
            "hashes": {"request": "dead", "resources": {"plan.md": "abc"}},
            "created_at_unix": 1784371900.400462
        })
    }

    fn settled() -> GateBundleConvertFactsWire {
        GateBundleConvertFactsWire {
            has_response: true,
            has_cancellation: false,
            deadline_passed: false,
        }
    }

    fn in_flight() -> GateBundleConvertFactsWire {
        GateBundleConvertFactsWire {
            has_response: false,
            has_cancellation: false,
            deadline_passed: false,
        }
    }

    #[test]
    fn converts_settled_v2_and_recomputes_request_hash() {
        let plan = plan(&v2_envelope(), &settled());
        assert_eq!(plan.intended_action, "convert");
        assert!(plan.conflicts.is_empty());

        let applied = apply(&v2_envelope(), &settled());
        let converted = applied.converted.unwrap();
        assert_eq!(converted["schema_version"], 3);
        assert_eq!(converted["request_id"], "req-1");
        assert_eq!(converted["kind"], "plan");
        assert_eq!(converted["primary_branch"], json!(["approve", "commit"]));
        assert_eq!(converted["hashes"]["resources"]["plan.md"], "abc");
        assert_eq!(
            converted["hashes"]["request"],
            "b8b8f9ddedfc11081674f9b5f7dca570f36e8f1e132a982c704ee82ec508bff1"
        );

        let verified = verify(&v2_envelope(), &converted, &settled());
        assert!(verified.identities_unchanged);
        assert!(verified.equal_semantics);
        assert!(verified.conflicts.is_empty());
    }

    #[test]
    fn refuses_in_flight_v2_and_v1() {
        let in_flight_plan = plan(&v2_envelope(), &in_flight());
        assert_eq!(in_flight_plan.intended_action, "refuse");
        assert!(in_flight_plan
            .conflicts
            .iter()
            .any(|conflict| conflict.kind == "in_flight"));

        let v1 =
            json!({"schema_version": 1, "kind": "plan", "request_id": "old"});
        let v1_plan = plan(&v1, &settled());
        assert_eq!(v1_plan.intended_action, "refuse");
        assert!(v1_plan
            .conflicts
            .iter()
            .any(|conflict| conflict.kind == "archive_only"));
    }

    #[test]
    fn v3_is_noop() {
        let v3 = json!({
            "schema_version": 3,
            "kind": "plan",
            "request_id": "req-1",
            "branches": [["approve"]],
            "primary_branch": ["approve"],
            "hashes": {"request": "x", "resources": {}}
        });
        let plan = plan(&v3, &settled());
        assert_eq!(plan.intended_action, "noop");
        let applied = apply(&v3, &settled());
        assert_eq!(applied.converted.as_ref(), Some(&v3));
    }

    #[test]
    fn python_canonical_hash_matches_known_v2_fixture() {
        let envelope = v2_envelope();
        assert_eq!(
            request_sha256(&envelope),
            "595bd1fcd72ddb248987ba47a8248f034f76b68468f36eaf28bd9200bbb33133"
        );
        let unicode = json!({
            "schema_version": 2,
            "kind": "plan",
            "request_id": "r",
            "label": "Approve ✅",
            "branches": [["a"]],
            "hashes": {"request": "x", "resources": {}}
        });
        assert_eq!(
            request_sha256(&unicode),
            "9c97baa91a3427e0585e5a09e3cee3e911cde71db40add438c8294235e6d4af8"
        );
    }
}
