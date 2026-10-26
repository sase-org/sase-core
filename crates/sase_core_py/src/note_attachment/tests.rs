//! Binding round-trip tests for note attachments.

use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use serde_json::json;

#[test]
fn note_attachment_bindings_are_registered() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        register_note_attachment(&module).unwrap();
        for name in [
            "classify_attachment",
            "scan_note_attachment_refs",
            "compose_note_attachment_text",
            "note_attachment_source_text",
            "stored_attachment_tokens",
            "sanitize_attachment_name",
            "unique_attachment_name",
            "attachment_placement",
            "attachment_should_auto_fetch",
            "attachment_sensitive_path_reason",
            "attachment_audience_decision",
            "attachment_scan_file",
            "attachment_scanner_rules_version",
            "attachment_canonical_extension",
            "attachment_public_object_relpath",
            "attachment_object_digest_from_relpath",
            "agent_publication_protection_wire_schema_version",
            "publication_scan_disposition",
            "publication_protection_ledger_empty",
            "publication_protection_ledger_validate",
            "publication_protection_record_from_scan",
            "publication_protection_record_from_rejection",
            "publication_protection_upsert",
            "publication_protection_lookup",
            "publication_protection_retry_allowed",
            "publication_protection_resolve_verified_clean",
        ] {
            assert!(module.getattr(name).is_ok(), "{name} is registered");
        }
    });
}

#[test]
fn publication_protection_bindings_round_trip_content_blocks() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        register_note_attachment(&module).unwrap();
        assert_eq!(py_agent_publication_protection_wire_schema_version(), 1);
        let ledger = py_publication_protection_ledger_empty(py).unwrap();
        let mut ledger = py_to_json_value(ledger.bind(py)).unwrap();
        assert_eq!(ledger["schema_version"], json!(1));

        let disposition_input = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "scan": {
                    "outcome": "hit",
                    "hit": {
                        "kind": "credential_pattern",
                        "rule_id": "private-key-header",
                        "line": 4
                    },
                    "bytes_scanned": 128,
                    "rules_version": 1
                },
                "failure": null
            }),
        )
        .unwrap();
        let disposition_input =
            disposition_input.bind(py).downcast::<PyDict>().unwrap();
        let disposition =
            py_publication_scan_disposition(py, disposition_input).unwrap();
        let disposition = py_to_json_value(disposition.bind(py)).unwrap();
        assert_eq!(disposition["status"], json!("blocked"));
        assert_eq!(disposition["hit"]["rule_id"], json!("private-key-header"));

        let record_input = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "identity": {
                    "repository_identity": "sase-org/sase--agents",
                    "relative_destination": "agents/a/run/chat.md",
                    "sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                },
                "affected_requests": ["request-a"],
                "disposition": disposition,
                "timestamp": 42.0
            }),
        )
        .unwrap();
        let record_input = record_input.bind(py).downcast::<PyDict>().unwrap();
        let record =
            py_publication_protection_record_from_scan(py, record_input)
                .unwrap();
        let record = py_to_json_value(record.bind(py)).unwrap();

        let upsert_input = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "ledger": ledger,
                "record": record
            }),
        )
        .unwrap();
        let upsert_input = upsert_input.bind(py).downcast::<PyDict>().unwrap();
        let upserted =
            py_publication_protection_upsert(py, upsert_input).unwrap();
        ledger = py_to_json_value(upserted.bind(py)).unwrap();
        assert_eq!(ledger["records"].as_array().unwrap().len(), 1);

        let lookup_input = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "ledger": ledger,
                "identity": {
                    "repository_identity": "sase-org/sase--agents",
                    "relative_destination": "agents/a/run/chat.md",
                    "sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                }
            }),
        )
        .unwrap();
        let lookup_input = lookup_input.bind(py).downcast::<PyDict>().unwrap();
        assert!(!py_publication_protection_retry_allowed(lookup_input).unwrap());
        let resolve_input = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "ledger": ledger,
                "identity": {
                    "repository_identity": "sase-org/sase--agents",
                    "relative_destination": "agents/a/run/chat.md",
                    "sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                }
            }),
        )
        .unwrap();
        let resolve_input =
            resolve_input.bind(py).downcast::<PyDict>().unwrap();
        let ledger =
            py_publication_protection_resolve_verified_clean(py, resolve_input)
                .unwrap();
        let ledger = py_to_json_value(ledger.bind(py)).unwrap();
        assert_eq!(ledger["records"].as_array().unwrap().len(), 1);
        let found = py_publication_protection_lookup(py, lookup_input).unwrap();
        let found = py_to_json_value(found.bind(py)).unwrap();
        assert_eq!(found["status"], json!("blocked"));
        assert!(!found.to_string().contains("SYNTHETIC_SECRET"));
    });
}

#[test]
fn scan_compose_source_cycle_round_trips() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let scan = py_scan_note_attachment_refs(
            py,
            "@./shots/login.png and @attachment:login.png",
            vec![],
        )
        .unwrap();
        let scan_value = py_to_json_value(scan.bind(py)).unwrap();
        assert_eq!(scan_value["path_refs"].as_array().unwrap().len(), 1);
        let composed = py_compose_note_attachment_text(
            "@./shots/login.png and @attachment:login.png",
            scan.bind(py),
            vec!["login.png".to_string()],
        )
        .unwrap();
        assert_eq!(composed, "@attachment:login.png and @attachment:login.png");
        let source = py_note_attachment_source_text(
            &composed,
            vec!["login.png".to_string()],
        )
        .unwrap();
        assert_eq!(source, composed);
        let tokens = py_stored_attachment_tokens(py, &composed).unwrap();
        let tokens_value = py_to_json_value(tokens.bind(py)).unwrap();
        assert_eq!(tokens_value.as_array().unwrap().len(), 2);
        let sanitized =
            py_sanitize_attachment_name("My Screenshot (1).PNG").unwrap();
        assert_eq!(sanitized, "My_Screenshot_1.PNG");
    });
}

#[test]
fn classify_png_bytes_wins_over_txt_extension() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let head = PyBytes::new_bound(
            py,
            &[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A],
        );
        let classification =
            py_classify_attachment(py, "note.txt", &head).unwrap();
        let value = py_to_json_value(classification.bind(py)).unwrap();
        assert_eq!(value["mime_type"], json!("image/png"));
        assert_eq!(value["class"], json!("image"));
    });
}

#[test]
fn uniquify_bumps_on_digest_conflict() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let existing = json_value_to_py(
            py,
            &json!([{"name": "login.png", "sha256": "b".repeat(64)}]),
        )
        .unwrap();
        let existing = existing.bind(py).downcast::<PyList>().unwrap();
        let uniquified =
            py_unique_attachment_name("login.png", &"a".repeat(64), existing)
                .unwrap();
        assert_eq!(uniquified, "login-2.png");
    });
}

#[test]
fn audience_and_object_bindings_round_trip() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        assert_eq!(py_attachment_scanner_rules_version(), 1);
        assert_eq!(
            py_attachment_canonical_extension("image/png").unwrap(),
            "png"
        );
        assert!(
            py_attachment_canonical_extension("application/octet-stream")
                .is_none()
        );
        let digest =
            "9f2c1e0b77aa4c10d5e6f3a2b1c9d8e7f6a5b4c3d2e1f0a9b8c7d6e5f4a3b2c1";
        let relpath =
            py_attachment_public_object_relpath(digest, "image/png").unwrap();
        assert_eq!(relpath, format!("files/objects/sha256/9f/{digest}.png"));
        let back = py_attachment_object_digest_from_relpath(&relpath).unwrap();
        assert_eq!(back, digest);
        py_attachment_public_object_relpath("not-a-digest", "image/png")
            .unwrap_err();
        py_attachment_object_digest_from_relpath("files/objects/sha256/xx/yy")
            .unwrap_err();

        // Audience decision parses facts and returns a decision.
        let facts = json_value_to_py(
            py,
            &json!({
                "bead_store_visibility": "public",
                "requested": "auto",
                "actor": "human",
                "confirmed": false,
                "allow_sensitive": false,
                "path": "/work/repo/build.log",
                "home": "/home/bryan",
                "sase_home": "/home/bryan/.sase",
                "extra_sensitive_patterns": [],
                "size_bytes": 1024,
                "public_max_bytes": 26214400,
                "class": "text",
                "scan": {"outcome": "clean", "bytes_scanned": 1024, "rules_version": 1},
                "owner_only": false,
                "workspace_root": "/work/repo",
                "scratch_roots": [],
            }),
        )
        .unwrap();
        let facts = facts.bind(py).downcast::<PyDict>().unwrap();
        let decision = py_attachment_audience_decision(py, facts).unwrap();
        let value = py_to_json_value(decision.bind(py)).unwrap();
        assert_eq!(value["outcome"], json!("public"));
        assert_eq!(value["rule"], json!("public_evidence"));

        // Scan file parses arguments and returns clean/skipped.
        let dir = tempfile::tempdir().unwrap();
        let candidate = dir.path().join("clean.log");
        std::fs::write(&candidate, b"hello\n").unwrap();
        let scan = py_attachment_scan_file(
            py,
            candidate.to_str().unwrap(),
            1024 * 1024,
            None,
            "/home/bryan",
            "/home/bryan/.sase",
        )
        .unwrap();
        let value = py_to_json_value(scan.bind(py)).unwrap();
        assert_eq!(value["outcome"], json!("clean"));
        assert_eq!(value["rules_version"], json!(1));
    });
}

#[test]
fn policy_bindings_round_trip() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let tiers = json_value_to_py(
            py,
            &json!([
                {"name": "git", "max_bytes": 52428800},
                {"name": "large", "max_bytes": null},
            ]),
        )
        .unwrap();
        let tiers = tiers.bind(py).downcast::<PyList>().unwrap();
        let placement =
            py_attachment_placement(py, 1024, tiers, false).unwrap();
        let value = py_to_json_value(placement.bind(py)).unwrap();
        assert_eq!(value, json!({"kind": "store", "store": "git"}));
        let local = py_attachment_placement(py, 1024, tiers, true).unwrap();
        let value = py_to_json_value(local.bind(py)).unwrap();
        assert_eq!(value, json!({"kind": "local_only"}));
        assert!(py_attachment_should_auto_fetch(1024, 2048));
        assert!(!py_attachment_should_auto_fetch(2049, 2048));
        let reason = py_attachment_sensitive_path_reason(
            "/home/bryan/.ssh/id_ed25519",
            "/home/bryan",
            None,
        )
        .unwrap();
        assert!(reason.contains("~/.ssh/**"), "{reason}");
        let clean = py_attachment_sensitive_path_reason(
            "/tmp/crash.log",
            "/home/bryan",
            None,
        );
        assert!(clean.is_none());
    });
}
