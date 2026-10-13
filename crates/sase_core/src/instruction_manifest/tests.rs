//! Tests for the instruction manifest v1 and v2 contracts.

use serde_json::{json, Value as JsonValue};

use super::{
    compute_common_digest, normalize_instruction_manifest,
    InstructionManifestError, INSTRUCTION_MANIFEST_WIRE_SCHEMA_VERSION,
};

fn root_fixture() -> JsonValue {
    serde_json::from_str(include_str!(
        "../../tests/fixtures/instruction_manifest_v1.json"
    ))
    .expect("root fixture must parse")
}

fn root_v2_fixture() -> JsonValue {
    serde_json::from_str(include_str!(
        "../../tests/fixtures/instruction_manifest_v2.json"
    ))
    .expect("v2 golden fixture must parse")
}

fn helper_v2_fixture() -> JsonValue {
    serde_json::from_str(include_str!(
        "../../tests/fixtures/instruction_manifest_v2_helper.json"
    ))
    .expect("v2 helper fixture must parse")
}

fn fallback_v2_fixture() -> JsonValue {
    serde_json::from_str(include_str!(
        "../../tests/fixtures/instruction_manifest_v2_fallback.json"
    ))
    .expect("v2 fallback fixture must parse")
}

fn helper_fixture() -> JsonValue {
    serde_json::from_str(include_str!(
        "../../tests/fixtures/instruction_manifest_helper_v1.json"
    ))
    .expect("helper fixture must parse")
}

fn interactive_fixture() -> JsonValue {
    serde_json::from_str(include_str!(
        "../../tests/fixtures/instruction_manifest_interactive_v1.json"
    ))
    .expect("interactive fixture must parse")
}

#[test]
fn wire_schema_version_is_two() {
    assert_eq!(INSTRUCTION_MANIFEST_WIRE_SCHEMA_VERSION, 2);
    assert_eq!(super::instruction_manifest_wire_schema_version(), 2);
}

#[test]
fn valid_root_normalizes_unchanged() {
    let fixture = root_fixture();
    let normalized = normalize_instruction_manifest(&fixture)
        .expect("valid root must normalize");
    assert_eq!(normalized, fixture);
}

#[test]
fn valid_helper_normalizes() {
    let fixture = helper_fixture();
    let normalized = normalize_instruction_manifest(&fixture)
        .expect("valid helper must normalize");
    assert_eq!(normalized, fixture);
}

#[test]
fn valid_interactive_normalizes() {
    let fixture = interactive_fixture();
    let normalized = normalize_instruction_manifest(&fixture)
        .expect("valid interactive must normalize");
    assert_eq!(normalized, fixture);
}

#[test]
fn valid_v2_root_with_explicit_subagent_normalizes_unchanged() {
    let fixture = root_v2_fixture();
    let normalized = normalize_instruction_manifest(&fixture)
        .expect("valid v2 root must normalize");
    assert_eq!(normalized, fixture);
}

#[test]
fn valid_v2_root_with_inherits_root_subagent_normalizes() {
    let mut fixture = root_v2_fixture();
    fixture["delivery"]["subagent"]["status"] = json!("inherits_root");
    fixture["delivery"]["subagent"]
        .as_object_mut()
        .unwrap()
        .remove("channel");
    fixture["delivery"]["subagent"]
        .as_object_mut()
        .unwrap()
        .remove("bundle_sha256");
    let normalized = normalize_instruction_manifest(&fixture)
        .expect("inherits_root subagent slot must normalize");
    assert_eq!(normalized, fixture);
}

#[test]
fn valid_v2_helper_with_parent_normalizes_unchanged() {
    let fixture = helper_v2_fixture();
    let normalized = normalize_instruction_manifest(&fixture)
        .expect("valid v2 helper must normalize");
    assert_eq!(normalized, fixture);
}

#[test]
fn valid_v2_legacy_fallback_normalizes_unchanged() {
    let fixture = fallback_v2_fixture();
    let normalized = normalize_instruction_manifest(&fixture)
        .expect("valid v2 fallback must normalize");
    assert_eq!(normalized, fixture);
}

#[test]
fn v1_golden_still_normalizes_unchanged() {
    let fixture = root_fixture();
    assert_eq!(
        normalize_instruction_manifest(&fixture)
            .expect("v1 golden must remain readable"),
        fixture
    );
}

fn assert_v2_rejected(fixture: JsonValue, needle: &str) {
    let error = normalize_instruction_manifest(&fixture).unwrap_err();
    assert!(
        error.to_string().contains(needle),
        "expected {needle:?} in {error}"
    );
}

#[test]
fn v2_delivery_invariants_are_enforced() {
    let mut fixture = root_v2_fixture();
    fixture["delivery"]["channel"] = json!("Codex.Developer");
    assert_v2_rejected(fixture, "delivery.channel");

    let mut fixture = root_v2_fixture();
    fixture["delivery"]
        .as_object_mut()
        .unwrap()
        .remove("channel");
    assert_v2_rejected(fixture, "delivery.status explicit requires channel");

    let mut fixture = root_v2_fixture();
    fixture["delivery"]["fallback"] = json!({"reason": "flag_disabled"});
    assert_v2_rejected(fixture, "delivery.status explicit forbids fallback");

    let mut fixture = root_v2_fixture();
    fixture["delivery"]["status"] = json!("inherits_root");
    fixture["delivery"]["fallback"] = json!({"reason": "flag_disabled"});
    assert_v2_rejected(
        fixture,
        "delivery.fallback requires status inherits_native",
    );

    let mut fixture = root_v2_fixture();
    fixture["facts"]["actor"] = json!("native_helper");
    assert_v2_rejected(fixture, "delivery.subagent requires facts.actor");

    let mut fixture = root_v2_fixture();
    fixture["delivery"]["status"] = json!("shadow");
    assert_v2_rejected(fixture, "delivery.subagent requires facts.actor");

    let mut fixture = root_v2_fixture();
    fixture["delivery"]["subagent"]["status"] = json!("shadow");
    assert_v2_rejected(
        fixture,
        "delivery.subagent.status must not be preview or shadow",
    );

    let mut fixture = root_v2_fixture();
    fixture["delivery"]["subagent"]
        .as_object_mut()
        .unwrap()
        .remove("channel");
    assert_v2_rejected(
        fixture,
        "delivery.subagent.status explicit requires channel",
    );

    let mut fixture = root_v2_fixture();
    fixture["delivery"]["subagent"]
        .as_object_mut()
        .unwrap()
        .remove("bundle_sha256");
    assert_v2_rejected(
        fixture,
        "delivery.subagent status explicit requires bundle_sha256",
    );

    let mut fixture = root_v2_fixture();
    fixture["delivery"]["subagent"]["bundle_sha256"] = json!("A".repeat(64));
    assert_v2_rejected(fixture, "delivery.subagent.bundle_sha256");

    let mut fixture = root_v2_fixture();
    fixture["delivery"]["subagent"]["status"] = json!("inherits_root");
    assert_v2_rejected(fixture, "bundle_sha256 is only allowed");

    let mut fixture = helper_v2_fixture();
    fixture["delivery"]
        .as_object_mut()
        .unwrap()
        .remove("parent_invocation_id");
    assert_v2_rejected(fixture, "requires delivery.parent_invocation_id");

    let mut fixture = root_fixture();
    fixture["delivery"]["subagent"] = JsonValue::Null;
    assert_v2_rejected(
        fixture,
        "schema_version 1 cannot contain v2-only field",
    );
}

#[test]
fn common_digest_filled_when_null() {
    let mut fixture = root_fixture();
    fixture["bundle"]["common_digest"] = JsonValue::Null;
    let normalized = normalize_instruction_manifest(&fixture)
        .expect("null common_digest must be filled");
    let expected = root_fixture()["bundle"]["common_digest"].clone();
    assert_eq!(normalized["bundle"]["common_digest"], expected);
    assert_ne!(normalized["bundle"]["common_digest"], JsonValue::Null);
}

#[test]
fn common_digest_rejected_when_wrong() {
    let mut fixture = root_fixture();
    fixture["bundle"]["common_digest"] = JsonValue::String("0".repeat(64));
    let error = normalize_instruction_manifest(&fixture).unwrap_err();
    assert!(
        error.to_string().contains("common_digest mismatch"),
        "{error}"
    );
}

#[test]
fn provider_only_change_keeps_common_digest() {
    let mut fixture = root_fixture();
    let before = fixture["bundle"]["common_digest"].clone();
    fixture["sections"][3]["sha256"] = JsonValue::String("e".repeat(64));
    let normalized = normalize_instruction_manifest(&fixture)
        .expect("provider-only sha change must normalize");
    assert_eq!(normalized["bundle"]["common_digest"], before);
}

#[test]
fn common_digest_ignores_provider_section() {
    let fixture = root_fixture();
    let manifest: super::InstructionManifestWire =
        serde_json::from_value(fixture).expect("fixture parses");
    let digest = compute_common_digest(&manifest);
    assert_eq!(
        digest,
        "35e3de246e801813311923851b16818b87e1c18588db05dc9dc04f4bcc0793c2"
    );
}

#[test]
fn invalid_fixtures_are_rejected() {
    let cases: Vec<(&str, &str)> = vec![
        (
            include_str!(
                "../../tests/fixtures/instruction_manifest_invalid_duplicate_id.json"
            ),
            "duplicate section id",
        ),
        (
            include_str!(
                "../../tests/fixtures/instruction_manifest_invalid_section_id.json"
            ),
            "section id",
        ),
        (
            include_str!(
                "../../tests/fixtures/instruction_manifest_invalid_layer_prefix.json"
            ),
            "does not match layer",
        ),
        (
            include_str!(
                "../../tests/fixtures/instruction_manifest_invalid_offsets.json"
            ),
            "not contiguous",
        ),
        (
            include_str!(
                "../../tests/fixtures/instruction_manifest_invalid_excluded_offset.json"
            ),
            "must not have offset",
        ),
        (
            include_str!(
                "../../tests/fixtures/instruction_manifest_invalid_excluded_reason.json"
            ),
            "must have a reason",
        ),
        (
            include_str!(
                "../../tests/fixtures/instruction_manifest_invalid_digest.json"
            ),
            "sha256",
        ),
        (
            include_str!(
                "../../tests/fixtures/instruction_manifest_invalid_blob_oid.json"
            ),
            "blob_oid",
        ),
        (
            include_str!(
                "../../tests/fixtures/instruction_manifest_invalid_facts.json"
            ),
            "invalid fact combination",
        ),
        (
            include_str!(
                "../../tests/fixtures/instruction_manifest_invalid_lifecycle.json"
            ),
            "requires runtime",
        ),
        (
            include_str!(
                "../../tests/fixtures/instruction_manifest_invalid_provider_specific.json"
            ),
            "provider_specific",
        ),
        (
            include_str!(
                "../../tests/fixtures/instruction_manifest_invalid_required.json"
            ),
            "required section",
        ),
        (
            include_str!(
                "../../tests/fixtures/instruction_manifest_invalid_common_digest.json"
            ),
            "common_digest mismatch",
        ),
        (
            include_str!(
                "../../tests/fixtures/instruction_manifest_invalid_unknown_field.json"
            ),
            "valid InstructionManifestWire",
        ),
        (
            include_str!(
                "../../tests/fixtures/instruction_manifest_invalid_budget_layer.json"
            ),
            "not a layer",
        ),
    ];
    for (raw, needle) in cases {
        let value: JsonValue =
            serde_json::from_str(raw).expect("invalid fixture parses");
        let error = normalize_instruction_manifest(&value).unwrap_err();
        assert!(
            error.to_string().contains(needle),
            "expected {needle:?} in {error}"
        );
    }
}

#[test]
fn unsupported_schema_is_reported() {
    let raw = include_str!(
        "../../tests/fixtures/instruction_manifest_invalid_schema.json"
    );
    let value: JsonValue =
        serde_json::from_str(raw).expect("schema fixture parses");
    let error = normalize_instruction_manifest(&value).unwrap_err();
    assert_eq!(error, InstructionManifestError::UnsupportedSchema(99));
}
