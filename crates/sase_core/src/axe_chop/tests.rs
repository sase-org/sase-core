use serde_json::json;

use super::*;

#[test]
fn result_validation_accepts_proposals_and_rejects_workflows() {
    let parsed = parse_chop_result(
        &json!({
            "schema_version": 1,
            "status": "ok",
            "summary": "two actions",
            "counters": {"files": 2},
            "evidence": ["reports/findings.json"],
            "proposed_launches": [
                {
                    "id": "scan",
                    "prompt": "Inspect the repository.\n#review",
                    "workspace": "gh:sase-org/sase",
                    "env": {"MODE": "careful"}
                },
                {
                    "prompt": "Apply the findings.",
                    "workspace_ref": "gh:sase-org/sase",
                    "wait_on": "scan"
                }
            ]
        })
        .to_string(),
    )
    .unwrap();
    assert_eq!(parsed.status, ChopResultStatusWire::Ok);
    assert_eq!(parsed.proposed_launches.len(), 2);

    let error = parse_chop_result(
        &json!({
            "schema_version": 1,
            "status": "ok",
            "proposed_launches": [{
                "prompt": "  #!refresh_docs\nrun it",
                "workspace": "git:sase"
            }]
        })
        .to_string(),
    )
    .unwrap_err();
    assert_eq!(error.code, "workflow_reference_forbidden");
    assert!(error.path.contains("proposed_launches[0].prompt"));
}

#[test]
fn result_validation_rejects_forward_wait_and_unknown_fields() {
    let forward = parse_chop_result(
        &json!({
            "schema_version": 1,
            "status": "ok",
            "proposed_launches": [{
                "prompt": "Do work",
                "workspace": "git:sase",
                "wait_on": 0
            }]
        })
        .to_string(),
    )
    .unwrap_err();
    assert_eq!(forward.code, "invalid_wait_on");

    let unknown = parse_chop_result(
        r#"{"schema_version":1,"status":"ok","surprise":true}"#,
    )
    .unwrap_err();
    assert_eq!(unknown.code, "invalid_result");
    assert!(unknown.message.contains("unknown field"));
}

#[test]
fn derived_agent_names_include_target_and_order() {
    assert_eq!(
        derive_chop_agent_name("Refresh Docs", Some("sase/core"), 1, None)
            .unwrap(),
        "chop.refresh-docs.sase-core.2"
    );
}

#[test]
fn derived_agent_names_include_sanitized_bounded_run_token() {
    assert_eq!(
        derive_chop_agent_name(
            "Refresh Docs",
            Some("sase/core"),
            1,
            Some("20260719T072506_123456")
        )
        .unwrap(),
        "chop.refresh-docs.sase-core.6_123456.2"
    );
}

#[test]
fn derived_agent_names_reject_empty_sanitized_run_token() {
    let error =
        derive_chop_agent_name("docs", None, 0, Some("///")).unwrap_err();
    assert_eq!(error.code, "invalid_run_token");
    assert_eq!(error.path, "$.run_token");
}

#[test]
fn derived_agent_names_keep_length_and_trailing_separator_guards() {
    let first = derive_chop_agent_name(
        &"very-long-chop_".repeat(12),
        Some(&"very-long-target_".repeat(12)),
        0,
        Some("20260719T072506_123456"),
    )
    .unwrap();
    let second = derive_chop_agent_name(
        &"very-long-chop_".repeat(12),
        Some(&"very-long-target_".repeat(12)),
        1,
        Some("20260719T072507_654321"),
    )
    .unwrap();

    assert!(first.len() <= 120);
    assert!(second.len() <= 120);
    assert!(first.ends_with(".6_123456.1"));
    assert!(second.ends_with(".7_654321.2"));
    assert_ne!(first, second);
}

#[test]
fn guards_short_circuit_triggers() {
    let request: ChopDecisionRequestWire = serde_json::from_value(json!({
        "schema_version": 1,
        "inhibit_if": [{
            "provider": "changespec",
            "name_prefix": "fix_just",
            "statuses": ["WIP"]
        }],
        "trigger": {"provider": "always"},
        "changespecs": [{"name": "fix_just_rollout", "status": "WIP"}],
        "now": "2026-07-18T12:00:00Z"
    }))
    .unwrap();
    let decision = evaluate_chop_decision(&request).unwrap();
    assert_eq!(decision.outcome, "skip");
    assert_eq!(decision.provider.as_deref(), Some("changespec"));
}

#[test]
fn git_trigger_returns_checkpoint_observation() {
    let request: ChopDecisionRequestWire = serde_json::from_value(json!({
        "schema_version": 1,
        "trigger": {
            "provider": "git.commits_since",
            "project": "sase",
            "threshold": 3,
            "checkpoint_policy": "on_action_success"
        },
        "git": [{
            "project": "sase",
            "head": "abc123",
            "commits_since_checkpoint": 4,
            "checkpoint_found": true
        }],
        "now": "2026-07-18T12:00:00Z"
    }))
    .unwrap();
    let decision = evaluate_chop_decision(&request).unwrap();
    assert_eq!(decision.outcome, "fire");
    assert_eq!(
        decision.checkpoint_key.as_deref(),
        Some("git.commits_since:sase")
    );
    assert_eq!(decision.checkpoint_cursor.as_deref(), Some("abc123"));
}

#[test]
fn checkpoint_success_policy_commits_only_after_success() {
    let observed: ChopCheckpointUpdateRequestWire =
        serde_json::from_value(json!({
            "schema_version": 1,
            "document": {"schema_version": 1, "entries": {}},
            "key": "git:sase",
            "cursor": "abc",
            "now": "t1",
            "policy": "on_action_success",
            "event": "observed"
        }))
        .unwrap();
    let pending = apply_checkpoint_update(&observed).unwrap();
    assert_eq!(pending.entries["git:sase"].cursor, "");
    assert_eq!(
        pending.entries["git:sase"].pending_cursor.as_deref(),
        Some("abc")
    );

    let succeeded = ChopCheckpointUpdateRequestWire {
        document: pending,
        event: ChopCheckpointEventWire::ActionSucceeded,
        now: "t2".to_string(),
        ..observed
    };
    let committed = apply_checkpoint_update(&succeeded).unwrap();
    assert_eq!(committed.entries["git:sase"].cursor, "abc");
    assert_eq!(committed.entries["git:sase"].pending_cursor, None);
}

#[test]
fn once_per_store_rejects_duplicates_and_evicts_oldest() {
    let first: ChopOncePerRequestWire = serde_json::from_value(json!({
        "schema_version": 1,
        "document": {"schema_version": 1, "entries": [
            {"key": "old", "seen_at": "t0"}
        ]},
        "key": "new",
        "now": "t1",
        "capacity": 1
    }))
    .unwrap();
    let accepted = check_and_record_once_per(&first).unwrap();
    assert_eq!(accepted.outcome, "accept");
    assert_eq!(accepted.document.entries[0].key, "new");

    let duplicate = check_and_record_once_per(&ChopOncePerRequestWire {
        document: accepted.document,
        ..first
    })
    .unwrap();
    assert_eq!(duplicate.outcome, "duplicate");
    assert_eq!(duplicate.document.entries.len(), 1);
}

#[test]
fn target_expansion_filters_projects_and_separates_overrides() {
    let request: ChopTargetExpansionRequestWire =
        serde_json::from_value(json!({
            "schema_version": 1,
            "chop_name": "refresh_docs",
            "for_each": {
                "source": "projects",
                "filters": {"names": ["sase"], "vcs": ["gh"]}
            },
            "source_rows": [
                {
                    "name": "sase",
                    "vcs": "gh",
                    "enabled": true,
                    "overrides": {"run_every": "1h30m"}
                },
                {"name": "hidden", "vcs": "gh", "enabled": false}
            ]
        }))
        .unwrap();
    let expansion = expand_chop_targets(&request).unwrap();
    assert_eq!(expansion.instances.len(), 1);
    assert_eq!(expansion.instances[0].instance_id, "refresh_docs[sase]");
    assert_eq!(
        expansion.instances[0].overrides["run_every"],
        json!("1h30m")
    );
    assert!(!expansion.instances[0].target.contains_key("overrides"));
}

#[test]
fn target_expansion_uses_stable_hash_without_identity_field() {
    let request: ChopTargetExpansionRequestWire =
        serde_json::from_value(json!({
            "schema_version": 1,
            "chop_name": "audit",
            "for_each": [{"region": "west", "priority": 2}]
        }))
        .unwrap();
    let left = expand_chop_targets(&request).unwrap();
    let right = expand_chop_targets(&request).unwrap();
    assert_eq!(left, right);
    assert!(left.instances[0].instance_id.starts_with("audit[target-"));
}

#[test]
fn compound_durations_are_strict_and_positive() {
    assert_eq!(parse_chop_duration("1d2h30m5s").unwrap(), 95_405);
    assert_eq!(parse_chop_duration("90m").unwrap(), 5_400);
    assert_eq!(
        parse_chop_duration("1m1h").unwrap_err().code,
        "invalid_duration"
    );
    assert_eq!(
        parse_chop_duration("0s").unwrap_err().code,
        "non_positive_duration"
    );
}

#[test]
fn strict_axe_validation_accepts_new_shape() {
    let request: AxeConfigValidationRequestWire =
        serde_json::from_value(json!({
            "schema_version": 1,
            "config": {"axe": {
                "max_hook_runners": 3,
                "lumberjacks": {"docs": {
                    "interval": 5,
                    "chop_timeout": "1m30s",
                    "env": {"TOKEN": {"env": "DOCS_TOKEN"}},
                    "chops": {"refresh_docs": {
                        "script": "sase_chop_refresh_docs",
                        "run_every": "1d",
                        "trigger": {"git.commits_since": {
                            "project": "sase",
                            "threshold": 5,
                            "checkpoint_policy": "on_action_success"
                        }},
                        "inhibit_if": {"agent_hood": {"hood": "refresh_docs"}},
                        "once_per": {"key": "docs:{target.name}"},
                        "for_each": {"source": "projects", "names": ["sase"]}
                    }}
                }}
            }}
        }))
        .unwrap();
    assert_eq!(validate_axe_config(&request).unwrap(), vec![]);
}

#[test]
fn strict_axe_validation_reports_migrations_duplicates_and_provenance() {
    let request: AxeConfigValidationRequestWire =
        serde_json::from_value(json!({
            "schema_version": 1,
            "config": {"lumberjacks": {"bad": {
                "interval": 0,
                "surprise": true,
                "chops": [
                    {"name": "audit", "agent": "#!audit"},
                    {"name": "audit", "run_every": "never"}
                ]
            }}},
            "provenance": {"lumberjacks.bad": "user:sase.yml"}
        }))
        .unwrap();
    let diagnostics = validate_axe_config(&request).unwrap();
    let codes: Vec<_> =
        diagnostics.iter().map(|item| item.code.as_str()).collect();
    assert!(codes.contains(&"agent_chop_removed"));
    assert!(codes.contains(&"duplicate_chop_identity"));
    assert!(codes.contains(&"invalid_duration"));
    assert!(codes.contains(&"non_positive_integer"));
    assert!(codes.contains(&"unknown_key"));
    assert!(diagnostics
        .iter()
        .all(|item| item.layer.as_deref() == Some("user:sase.yml")));
}
