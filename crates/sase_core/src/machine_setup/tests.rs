use serde_json::{json, Value};

use super::*;
use crate::fleet_contract::FLEET_PROTOCOL_VERSION;

fn pin(hex_char: char) -> String {
    format!("sase_inst_v1_{}", hex_char.to_string().repeat(64))
}

fn health_request(payload: Value) -> TailnetHealthRequestWire {
    TailnetHealthRequestWire {
        schema_version: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
        alias: "apollo".to_string(),
        payload: Some(payload),
        error_code: String::new(),
        error_reason: String::new(),
    }
}

fn basic_status() -> Value {
    json!({
        "Self": {
            "ID": "self-node",
            "PublicKey": "nodekey:self",
            "DNSName": "athena.tail297af1.ts.net.",
            "HostName": "athena",
            "Online": true,
            "OS": "linux"
        },
        "Peer": {
            "self-node": {
                "ID": "self-node",
                "PublicKey": "nodekey:self",
                "DNSName": "athena.tail297af1.ts.net.",
                "HostName": "athena",
                "Online": true,
                "OS": "linux"
            },
            "peer-apollo": {
                "ID": "peer-apollo",
                "PublicKey": "nodekey:apollo",
                "DNSName": "apollo.tail297af1.ts.net.",
                "HostName": "apollo",
                "Online": true,
                "OS": "linux",
                "ExtraFutureField": "ignored"
            },
            "peer-pixel": {
                "ID": "peer-pixel",
                "PublicKey": "nodekey:pixel",
                "DNSName": "pixel.tail297af1.ts.net.",
                "HostName": "pixel",
                "Online": false,
                "OS": "android"
            }
        }
    })
}

fn missing_fields_status() -> Value {
    json!({
        "Self": {
            "ID": "self-node",
            "DNSName": "athena.tail297af1.ts.net."
        },
        "Peer": {
            "peer-no-dns": {
                "ID": "peer-no-dns",
                "HostName": "no-dns",
                "Online": true
            },
            "peer-invalid-dns": {
                "ID": "peer-invalid-dns",
                "DNSName": "bad_name.tail297af1.ts.net.",
                "HostName": "bad-name",
                "Online": true
            },
            "peer-extra": {
                "ID": "peer-extra",
                "DNSName": "extra.tail297af1.ts.net.",
                "HostName": "extra",
                "Online": true,
                "Unexpected": {"nested": true}
            }
        }
    })
}

fn observation(endpoint: &str, payload: Value) -> TailnetHealthObservationWire {
    TailnetHealthObservationWire {
        endpoint: endpoint.to_string(),
        payload: Some(payload),
        error_code: String::new(),
        error_reason: String::new(),
    }
}

fn compatible_payload() -> Value {
    json!({
        "status": "ok",
        "service": SASE_GATEWAY_HEALTH_SERVICE,
        "fleet": {"supported_protocol_versions": [FLEET_PROTOCOL_VERSION]}
    })
}

fn review_candidate(
    endpoint: &str,
    installation_pin: &str,
) -> DiscoveryCandidateWire {
    DiscoveryCandidateWire {
        provider_ref: "builtin@https".to_string(),
        endpoint: endpoint.to_string(),
        display_name: "ignored display".to_string(),
        machine_selector: "ignored-selector".to_string(),
        installation_pin: installation_pin.to_string(),
        detail: "ignored detail".to_string(),
    }
}

fn review_state(
    reviewed: Vec<MachineInitReviewEntryWire>,
) -> MachineInitReviewStateWire {
    MachineInitReviewStateWire {
        schema_version: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
        initial_review_completed: true,
        reviewed,
    }
}

#[test]
fn health_classifies_compatible_fleet_advertisement() {
    let result = classify_tailnet_health(&health_request(json!({
        "status": "ok",
        "fleet": {"supported_protocol_versions": [FLEET_PROTOCOL_VERSION]}
    })))
    .unwrap();
    assert_eq!(result.compatibility, COMPATIBILITY_COMPATIBLE);
    assert!(result.diagnostic.is_none());
    assert!(result.reason.contains("fleet protocol v"));
}

#[test]
fn review_assessment_requires_initial_review_without_completed_state() {
    let result =
        assess_machine_init_review(&MachineInitReviewAssessmentRequestWire {
            schema_version: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
            state: None,
            candidates: vec![],
            enrolled: vec![],
        })
        .unwrap();
    assert!(result.offer_enrollment);
    assert!(result.initial_review_required);

    let incomplete =
        assess_machine_init_review(&MachineInitReviewAssessmentRequestWire {
            schema_version: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
            state: Some(MachineInitReviewStateWire {
                schema_version: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
                initial_review_completed: false,
                reviewed: vec![],
            }),
            candidates: vec![review_candidate(
                "https://fleet.example.test",
                "",
            )],
            enrolled: vec![],
        })
        .unwrap();
    assert!(incomplete.offer_enrollment);
    assert!(incomplete.initial_review_required);
}

#[test]
fn review_merge_records_empty_and_deduplicated_completed_reviews() {
    let empty = merge_machine_init_review(&MachineInitReviewMergeRequestWire {
        schema_version: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
        existing_state: None,
        presented_candidates: vec![],
    })
    .unwrap();
    assert!(empty.initial_review_completed);
    assert!(empty.reviewed.is_empty());

    let pin_a = pin('a');
    let merged =
        merge_machine_init_review(&MachineInitReviewMergeRequestWire {
            schema_version: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
            existing_state: None,
            presented_candidates: vec![
                review_candidate("https://fleet.example.test", ""),
                review_candidate("https://fleet.example.test", &pin_a),
                review_candidate("https://fleet.example.test", ""),
            ],
        })
        .unwrap();
    assert_eq!(merged.reviewed.len(), 1);
    assert_eq!(merged.reviewed[0].installation_pin, pin_a);
}

#[test]
fn review_assessment_ignores_display_order_and_duplicate_observations() {
    let pin_a = pin('a');
    let state = review_state(vec![MachineInitReviewEntryWire {
        provider_ref: "builtin@https".to_string(),
        endpoint: "https://old-name.example.test".to_string(),
        installation_pin: pin_a.clone(),
    }]);
    let mut changed_display =
        review_candidate("https://renamed.example.test", &pin_a);
    changed_display.display_name = "a new friendly name".to_string();

    let result =
        assess_machine_init_review(&MachineInitReviewAssessmentRequestWire {
            schema_version: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
            state: Some(state),
            candidates: vec![changed_display.clone(), changed_display],
            enrolled: vec![],
        })
        .unwrap();
    assert!(!result.offer_enrollment);
    assert!(result.unreviewed_candidates.is_empty());
}

#[test]
fn review_assessment_uses_endpoint_when_either_pin_is_absent() {
    let state = review_state(vec![MachineInitReviewEntryWire {
        provider_ref: "builtin@https".to_string(),
        endpoint: "https://tailnet.example.test".to_string(),
        installation_pin: String::new(),
    }]);
    let reviewed =
        assess_machine_init_review(&MachineInitReviewAssessmentRequestWire {
            schema_version: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
            state: Some(state.clone()),
            candidates: vec![review_candidate(
                "https://tailnet.example.test",
                "",
            )],
            enrolled: vec![],
        })
        .unwrap();
    assert!(!reviewed.offer_enrollment);

    let new_endpoint =
        assess_machine_init_review(&MachineInitReviewAssessmentRequestWire {
            schema_version: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
            state: Some(state),
            candidates: vec![review_candidate("https://new.example.test", "")],
            enrolled: vec![],
        })
        .unwrap();
    assert!(new_endpoint.offer_enrollment);
    assert_eq!(new_endpoint.unreviewed_candidates.len(), 1);
}

#[test]
fn review_assessment_treats_different_nonempty_pins_as_new_installations() {
    let state = review_state(vec![MachineInitReviewEntryWire {
        provider_ref: "builtin@https".to_string(),
        endpoint: "https://fleet.example.test".to_string(),
        installation_pin: pin('a'),
    }]);
    let result =
        assess_machine_init_review(&MachineInitReviewAssessmentRequestWire {
            schema_version: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
            state: Some(state),
            candidates: vec![review_candidate(
                "https://fleet.example.test",
                &pin('b'),
            )],
            enrolled: vec![],
        })
        .unwrap();
    assert!(result.offer_enrollment);
    assert_eq!(result.unreviewed_candidates.len(), 1);
}

#[test]
fn review_assessment_excludes_enrolled_and_repair_candidates() {
    let pin_a = pin('a');
    let pin_b = pin('b');
    let result =
        assess_machine_init_review(&MachineInitReviewAssessmentRequestWire {
            schema_version: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
            state: Some(review_state(vec![])),
            candidates: vec![
                review_candidate("https://apollo.example.test", &pin_a),
                review_candidate("https://apollo.example.test", &pin_b),
                review_candidate("https://fleet.example.test", ""),
            ],
            enrolled: vec![EnrolledMachineWire {
                alias: "apollo".to_string(),
                provider_ref: "builtin@https".to_string(),
                endpoint: "https://apollo.example.test".to_string(),
                pinned_installation_id: pin_a,
            }],
        })
        .unwrap();
    assert!(result.offer_enrollment);
    assert_eq!(result.unreviewed_candidates.len(), 1);
    assert_eq!(
        result.unreviewed_candidates[0].endpoint,
        "https://fleet.example.test"
    );
}

#[test]
fn review_state_rejects_unsupported_schema_version() {
    let error = merge_machine_init_review(&MachineInitReviewMergeRequestWire {
        schema_version: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
        existing_state: Some(MachineInitReviewStateWire {
            schema_version: 9,
            initial_review_completed: true,
            reviewed: vec![],
        }),
        presented_candidates: vec![],
    })
    .unwrap_err();
    assert!(matches!(error, MachineSetupError::Validation(_)));
}

#[test]
fn health_classifies_legacy_sase_without_fleet_as_unknown() {
    let missing_service = classify_tailnet_health(&health_request(json!({
        "status": "ok"
    })))
    .unwrap();
    assert_eq!(missing_service.compatibility, COMPATIBILITY_UNKNOWN);
    assert_eq!(
        missing_service
            .diagnostic
            .as_ref()
            .map(|row| row.code.as_str()),
        Some("tailnet_probe_fleet_unknown")
    );

    let named = classify_tailnet_health(&health_request(json!({
        "status": "ok",
        "service": SASE_GATEWAY_HEALTH_SERVICE
    })))
    .unwrap();
    assert_eq!(named.compatibility, COMPATIBILITY_UNKNOWN);
    assert_eq!(
        named.diagnostic.as_ref().map(|row| row.code.as_str()),
        Some("tailnet_probe_fleet_unknown")
    );
}

#[test]
fn health_distinguishes_unrelated_healthy_service_from_legacy_sase() {
    let result = classify_tailnet_health(&health_request(json!({
        "status": "ok",
        "service": "unrelated"
    })))
    .unwrap();
    assert_eq!(result.compatibility, COMPATIBILITY_INCOMPATIBLE);
    assert_eq!(
        result.diagnostic.as_ref().map(|row| row.code.as_str()),
        Some("tailnet_probe_unrelated_service")
    );
}

#[test]
fn health_classifies_non_sase_payload_as_unrelated() {
    let result = classify_tailnet_health(&health_request(json!({
        "hello": "world"
    })))
    .unwrap();
    assert_eq!(result.compatibility, COMPATIBILITY_INCOMPATIBLE);
    assert_eq!(
        result.diagnostic.as_ref().map(|row| row.code.as_str()),
        Some("tailnet_probe_unrelated_service")
    );
}

#[test]
fn health_rejects_malformed_protocol_version_types() {
    for payload in [
        json!({"status": "ok", "fleet": {"supported_protocol_versions": ["1"]}}),
        json!({"status": "ok", "fleet": {"supported_protocol_versions": [true]}}),
        json!({"status": "ok", "fleet": {"supported_protocol_versions": [1.5]}}),
        json!({"status": "ok", "fleet": {"supported_protocol_versions": [1, "x"]}}),
        json!({"status": "ok", "fleet": {"supported_protocol_versions": "1"}}),
    ] {
        let result = classify_tailnet_health(&health_request(payload)).unwrap();
        assert_eq!(result.compatibility, COMPATIBILITY_INCOMPATIBLE);
        assert_eq!(
            result.diagnostic.as_ref().map(|row| row.code.as_str()),
            Some("tailnet_probe_fleet_malformed")
        );
    }
}

#[test]
fn health_incompatible_when_protocol_constant_is_absent() {
    let result = classify_tailnet_health(&health_request(json!({
        "status": "ok",
        "fleet": {"supported_protocol_versions": [99]}
    })))
    .unwrap();
    assert_eq!(result.compatibility, COMPATIBILITY_INCOMPATIBLE);
    assert_eq!(
        result.diagnostic.as_ref().map(|row| row.code.as_str()),
        Some("tailnet_probe_fleet_incompatible")
    );
}

#[test]
fn health_rejects_unsupported_schema_version() {
    let mut request = health_request(json!({"status": "ok"}));
    request.schema_version = 2;
    let error = classify_tailnet_health(&request).unwrap_err();
    assert!(matches!(
        error,
        MachineSetupError::UnsupportedSchema { actual: 2, .. }
    ));
}

#[test]
fn discovery_parses_status_excludes_self_and_normalizes_dns() {
    let result = classify_tailnet_discovery(&TailnetDiscoveryRequestWire {
        schema_version: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
        status: basic_status(),
        endpoint_overrides: Value::Null,
        health_observations: vec![
            observation(
                "https://apollo.tail297af1.ts.net",
                compatible_payload(),
            ),
            TailnetHealthObservationWire {
                endpoint: "https://pixel.tail297af1.ts.net".to_string(),
                payload: None,
                error_code: "tailnet_probe_timeout".to_string(),
                error_reason: "health probe timed out".to_string(),
            },
        ],
    })
    .unwrap();

    let endpoints: Vec<&str> = result
        .candidates
        .iter()
        .map(|candidate| candidate.endpoint.as_str())
        .collect();
    assert_eq!(
        endpoints,
        vec![
            "https://apollo.tail297af1.ts.net",
            "https://pixel.tail297af1.ts.net"
        ]
    );
    assert!(result
        .candidates
        .iter()
        .all(|candidate| !candidate.endpoint.contains("athena")));
    assert!(result.candidates.iter().all(|candidate| {
        candidate.machine_selector.is_empty()
            && candidate.installation_pin.is_empty()
    }));
    assert_eq!(result.candidates[0].provider_ref, TAILNET_PROVIDER_REF);
    assert!(result.candidates[0]
        .detail
        .contains("compatibility=compatible"));
    assert!(result.candidates[1].detail.contains("tailscale=offline"));
    let codes: Vec<&str> = result
        .diagnostics
        .iter()
        .map(|row| row.code.as_str())
        .collect();
    assert!(codes.contains(&"tailnet_peer_offline"));
    assert!(codes.contains(&"tailnet_peer_os_advisory"));
    assert!(codes.contains(&"tailnet_probe_timeout"));
}

#[test]
fn discovery_handles_missing_and_extra_peer_fields() {
    let result = classify_tailnet_discovery(&TailnetDiscoveryRequestWire {
        schema_version: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
        status: missing_fields_status(),
        endpoint_overrides: json!({}),
        health_observations: vec![observation(
            "https://extra.tail297af1.ts.net",
            compatible_payload(),
        )],
    })
    .unwrap();

    assert_eq!(
        result
            .candidates
            .iter()
            .map(|candidate| candidate.endpoint.as_str())
            .collect::<Vec<_>>(),
        vec!["https://extra.tail297af1.ts.net"]
    );
    assert_eq!(
        result
            .diagnostics
            .iter()
            .filter(|row| row.code == "tailnet_peer_dns_invalid")
            .count(),
        2
    );
}

#[test]
fn discovery_applies_https_endpoint_overrides() {
    let result = classify_tailnet_discovery(&TailnetDiscoveryRequestWire {
        schema_version: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
        status: basic_status(),
        endpoint_overrides: json!({
            "apollo": "https://apollo.example.test:8443/gateway"
        }),
        health_observations: vec![observation(
            "https://apollo.example.test:8443/gateway",
            compatible_payload(),
        )],
    })
    .unwrap();

    assert_eq!(
        result.candidates[0].endpoint,
        "https://apollo.example.test:8443/gateway"
    );
    assert!(result.candidates[0].detail.contains("endpoint=override"));
}

#[test]
fn discovery_rejects_invalid_endpoint_override() {
    let result = classify_tailnet_discovery(&TailnetDiscoveryRequestWire {
        schema_version: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
        status: basic_status(),
        endpoint_overrides: json!({"apollo": "http://insecure.example.test"}),
        health_observations: vec![],
    })
    .unwrap();

    assert!(result
        .candidates
        .iter()
        .all(|candidate| candidate.display_name != "apollo"));
    assert!(result
        .diagnostics
        .iter()
        .any(|row| row.code == "tailnet_endpoint_override_invalid"));
}

#[test]
fn discovery_reports_non_mapping_peers_and_peer_payload() {
    let not_mapping =
        classify_tailnet_discovery(&TailnetDiscoveryRequestWire {
            schema_version: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
            status: json!({
                "Self": {"ID": "self-node"},
                "Peer": {"bad": ["not", "an", "object"]}
            }),
            endpoint_overrides: Value::Null,
            health_observations: vec![],
        })
        .unwrap();
    assert_eq!(not_mapping.diagnostics[0].code, "tailnet_peer_not_mapping");

    let peer_invalid =
        classify_tailnet_discovery(&TailnetDiscoveryRequestWire {
            schema_version: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
            status: json!({"Peer": ["x"]}),
            endpoint_overrides: Value::Null,
            health_observations: vec![],
        })
        .unwrap();
    assert_eq!(
        peer_invalid.diagnostics[0].code,
        "tailnet_status_peer_invalid"
    );

    let malformed = classify_tailnet_discovery(&TailnetDiscoveryRequestWire {
        schema_version: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
        status: json!(["not", "an", "object"]),
        endpoint_overrides: Value::Null,
        health_observations: vec![],
    })
    .unwrap();
    assert_eq!(malformed.diagnostics[0].code, "tailnet_status_malformed");
}

#[test]
fn discovery_excludes_self_by_identity_overlap_and_self_flag() {
    let result = classify_tailnet_discovery(&TailnetDiscoveryRequestWire {
        schema_version: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
        status: json!({
            "Self": {"ID": "node-a", "HostName": "athena"},
            "Peer": {
                "other-key": {"ID": "node-a", "DNSName": "athena.tail.test.ts.net."},
                "flagged": {
                    "Self": true,
                    "DNSName": "other.tail.test.ts.net.",
                    "HostName": "other"
                },
                "keep": {
                    "ID": "node-b",
                    "DNSName": "keep.tail.test.ts.net.",
                    "HostName": "keep"
                }
            }
        }),
        endpoint_overrides: Value::Null,
        health_observations: vec![],
    })
    .unwrap();
    assert_eq!(
        result
            .candidates
            .iter()
            .map(|candidate| candidate.display_name.as_str())
            .collect::<Vec<_>>(),
        vec!["keep"]
    );
}

#[test]
fn reconcile_skips_matching_identities_and_routes_pin_change_to_repair() {
    let pin_a = pin('a');
    let pin_b = pin('b');
    let enrolled = EnrolledMachineWire {
        alias: "apollo".to_string(),
        provider_ref: "builtin@https".to_string(),
        endpoint: "https://apollo.example.test".to_string(),
        pinned_installation_id: pin_a.clone(),
    };
    let result = reconcile_machine_enrollments(&MachineReconcileRequestWire {
        schema_version: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
        enrolled: vec![enrolled.clone()],
        candidates: vec![
            DiscoveryCandidateWire {
                provider_ref: "builtin@https".to_string(),
                endpoint: "https://apollo.example.test".to_string(),
                display_name: "apollo".to_string(),
                installation_pin: pin_a.clone(),
                ..DiscoveryCandidateWire::default()
            },
            DiscoveryCandidateWire {
                provider_ref: "builtin@https".to_string(),
                endpoint: "https://apollo.example.test".to_string(),
                display_name: "apollo-new".to_string(),
                installation_pin: pin_b.clone(),
                ..DiscoveryCandidateWire::default()
            },
            DiscoveryCandidateWire {
                provider_ref: "builtin@https".to_string(),
                endpoint: "https://fleet.example.test".to_string(),
                display_name: "fleet".to_string(),
                installation_pin: pin_b,
                ..DiscoveryCandidateWire::default()
            },
        ],
    })
    .unwrap();

    assert_eq!(
        result
            .items
            .iter()
            .map(|item| item.status.as_str())
            .collect::<Vec<_>>(),
        vec![
            RECONCILE_STATUS_ENROLLED,
            RECONCILE_STATUS_REPAIR,
            RECONCILE_STATUS_NEW
        ]
    );
    assert_eq!(result.items[0].alias, "apollo");
    assert_eq!(result.items[1].alias, "apollo");
    assert!(result.items[1]
        .reason
        .contains("sase machine repair apollo"));
    assert_eq!(result.items[2].status, RECONCILE_STATUS_NEW);
    assert_eq!(enrolled.pinned_installation_id, pin_a);
}

#[test]
fn reconcile_does_not_let_untrusted_hint_overwrite_enrolled_pin() {
    let pin_a = pin('a');
    let enrolled = EnrolledMachineWire {
        alias: "apollo".to_string(),
        provider_ref: TAILNET_PROVIDER_REF.to_string(),
        endpoint: "https://apollo.tail297af1.ts.net".to_string(),
        pinned_installation_id: pin_a.clone(),
    };
    let result = reconcile_machine_enrollments(&MachineReconcileRequestWire {
        schema_version: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
        enrolled: vec![enrolled.clone()],
        candidates: vec![DiscoveryCandidateWire {
            provider_ref: TAILNET_PROVIDER_REF.to_string(),
            endpoint: "https://apollo.tail297af1.ts.net".to_string(),
            display_name: "apollo".to_string(),
            machine_selector: "untrusted-selector".to_string(),
            installation_pin: String::new(),
            detail: "compatibility=unknown".to_string(),
        }],
    })
    .unwrap();

    assert_eq!(result.items[0].status, RECONCILE_STATUS_ENROLLED);
    assert_eq!(result.items[0].alias, "apollo");
    assert_eq!(enrolled.pinned_installation_id, pin_a);
    assert!(result.items[0].candidate.installation_pin.is_empty());
}

#[test]
fn reconcile_rejects_unsupported_schema_version() {
    let error = reconcile_machine_enrollments(&MachineReconcileRequestWire {
        schema_version: 9,
        candidates: vec![],
        enrolled: vec![],
    })
    .unwrap_err();
    assert!(matches!(
        error,
        MachineSetupError::UnsupportedSchema { actual: 9, .. }
    ));
}
