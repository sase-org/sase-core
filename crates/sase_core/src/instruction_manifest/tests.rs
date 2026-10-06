//! Tests for the instruction manifest v1 contract.

use serde_json::Value as JsonValue;

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
fn wire_schema_version_is_one() {
    assert_eq!(INSTRUCTION_MANIFEST_WIRE_SCHEMA_VERSION, 1);
    assert_eq!(super::instruction_manifest_wire_schema_version(), 1);
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
