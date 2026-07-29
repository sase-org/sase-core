use serde_json::json;

use super::*;

#[test]
fn chop_report_round_trips_every_block_kind() {
    let parsed = parse_chop_result(
        &json!({
            "schema_version": 1,
            "status": "ok",
            "summary": "report ready",
            "report": {
                "title": "CI WATCH",
                "blocks": [
                    {
                        "kind": "headline",
                        "text": "4 green · 1 red",
                        "tone": "warn"
                    },
                    {"kind": "heading", "text": "REPOSITORIES"},
                    {
                        "kind": "text",
                        "text": "One repository needs attention.",
                        "tone": "info"
                    },
                    {
                        "kind": "kv",
                        "items": [
                            {
                                "key": "mode",
                                "value": "dry run",
                                "tone": "muted"
                            }
                        ]
                    },
                    {
                        "kind": "rows",
                        "columns": ["REPOSITORY", "STATE"],
                        "rows": [
                            {
                                "cells": ["sase-org/sase", "red"],
                                "tone": "error",
                                "glyph": "▲"
                            },
                            {
                                "cells": ["sase-org/sase-core", "green"],
                                "tone": "ok"
                            }
                        ]
                    },
                    {
                        "kind": "bullets",
                        "items": [
                            {
                                "text": "Open the failing job",
                                "tone": "accent",
                                "glyph": "↗"
                            }
                        ]
                    },
                    {
                        "kind": "gauge",
                        "label": "budget",
                        "value": 12,
                        "max": 10,
                        "tone": "neutral"
                    },
                    {"kind": "divider"}
                ]
            }
        })
        .to_string(),
    )
    .unwrap();

    let report = parsed.report.as_ref().unwrap();
    assert_eq!(report.title.as_deref(), Some("CI WATCH"));
    assert_eq!(report.blocks.len(), 8);
    let encoded = serde_json::to_string(&parsed).unwrap();
    assert_eq!(parse_chop_result(&encoded).unwrap(), parsed);
}

#[test]
fn chop_result_without_report_remains_valid_and_serializes_null() {
    let parsed = parse_chop_result(
        r#"{"schema_version":1,"status":"ok","summary":"legacy"}"#,
    )
    .unwrap();

    assert_eq!(parsed.report, None);
    assert_eq!(serde_json::to_value(parsed).unwrap()["report"], json!(null));
}

#[test]
fn chop_report_rejects_unknown_block_kinds_and_tones() {
    let invalid_reports = [
        json!({"blocks": [{"kind": "chart", "text": "future"}]}),
        json!({
            "blocks": [{
                "kind": "headline",
                "text": "status",
                "tone": "purple"
            }]
        }),
    ];

    for report in invalid_reports {
        let error = parse_chop_result(
            &json!({
                "schema_version": 1,
                "status": "ok",
                "report": report
            })
            .to_string(),
        )
        .unwrap_err();
        assert_eq!(error.code, "invalid_result");
    }
}

#[test]
fn chop_report_rejects_control_characters_and_overlong_text() {
    let invalid = [
        (
            json!({"blocks": [{"kind": "text", "text": "line one\nline two"}]}),
            "invalid_report_text",
        ),
        (
            json!({
                "blocks": [{
                    "kind": "headline",
                    "text": "x".repeat(513)
                }]
            }),
            "report_text_too_long",
        ),
    ];

    for (report, expected_code) in invalid {
        let error = parse_chop_result(
            &json!({
                "schema_version": 1,
                "status": "ok",
                "report": report
            })
            .to_string(),
        )
        .unwrap_err();
        assert_eq!(error.code, expected_code);
        assert_eq!(error.path, "$.report.blocks[0].text");
    }
}

#[test]
fn chop_report_rejects_oversize_documents() {
    let items: Vec<_> =
        (0..64).map(|_| json!({"text": "x".repeat(512)})).collect();
    let error = parse_chop_result(
        &json!({
            "schema_version": 1,
            "status": "ok",
            "report": {
                "blocks": [{"kind": "bullets", "items": items}]
            }
        })
        .to_string(),
    )
    .unwrap_err();

    assert_eq!(error.code, "report_too_large");
    assert_eq!(error.path, "$.report");
    assert!(error.message.contains(&CHOP_REPORT_MAX_BYTES.to_string()));
}

#[test]
fn chop_report_rejects_excess_blocks_and_rows() {
    let blocks = vec![json!({"kind": "divider"}); 49];
    let too_many_blocks = parse_chop_result(
        &json!({
            "schema_version": 1,
            "status": "ok",
            "report": {"blocks": blocks}
        })
        .to_string(),
    )
    .unwrap_err();
    assert_eq!(too_many_blocks.code, "invalid_report_count");
    assert_eq!(too_many_blocks.path, "$.report.blocks");

    let rows = vec![json!({"cells": ["value"]}); 65];
    let too_many_rows = parse_chop_result(
        &json!({
            "schema_version": 1,
            "status": "ok",
            "report": {
                "blocks": [{
                    "kind": "rows",
                    "rows": rows
                }]
            }
        })
        .to_string(),
    )
    .unwrap_err();
    assert_eq!(too_many_rows.code, "invalid_report_count");
    assert_eq!(too_many_rows.path, "$.report.blocks[0].rows");
}

#[test]
fn chop_report_rejects_ragged_rows_disallowed_glyphs_and_invalid_gauges() {
    let invalid = [
        (
            json!({
                "blocks": [{
                    "kind": "rows",
                    "columns": ["A", "B"],
                    "rows": [{"cells": ["only one"]}]
                }]
            }),
            "ragged_report_rows",
            "$.report.blocks[0].rows[0].cells",
        ),
        (
            json!({
                "blocks": [{
                    "kind": "bullets",
                    "items": [{"text": "item", "glyph": "🚀"}]
                }]
            }),
            "invalid_report_glyph",
            "$.report.blocks[0].items[0].glyph",
        ),
        (
            json!({
                "blocks": [{
                    "kind": "gauge",
                    "label": "budget",
                    "value": 0,
                    "max": 0
                }]
            }),
            "invalid_report_gauge",
            "$.report.blocks[0].max",
        ),
    ];

    for (report, expected_code, expected_path) in invalid {
        let error = parse_chop_result(
            &json!({
                "schema_version": 1,
                "status": "ok",
                "report": report
            })
            .to_string(),
        )
        .unwrap_err();
        assert_eq!(error.code, expected_code);
        assert_eq!(error.path, expected_path);
    }
}

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
fn clan_scoped_proposals_round_trip_without_changing_legacy_proposals() {
    let parsed = parse_chop_result(
        &json!({
            "schema_version": 1,
            "status": "ok",
            "proposed_launches": [
                {
                    "id": "first",
                    "prompt": "Split the file.",
                    "workspace": "gh:sase-org/sase",
                    "agent_name": "split_file.src_lib.rs.a1b2",
                    "clan": "toobig-@",
                    "clan_summary": "[bold]Large modules[/bold]\nSplit by responsibility."
                },
                {
                    "prompt": "Legacy launch.",
                    "workspace": "git:sase"
                }
            ]
        })
        .to_string(),
    )
    .unwrap();

    assert_eq!(
        parsed.proposed_launches[0].clan.as_deref(),
        Some("toobig-@")
    );
    assert_eq!(
        parsed.proposed_launches[0].clan_summary.as_deref(),
        Some("[bold]Large modules[/bold]\nSplit by responsibility.")
    );
    assert_eq!(parsed.proposed_launches[1].clan, None);
    assert_eq!(parsed.proposed_launches[1].clan_summary, None);
    let encoded = serde_json::to_value(&parsed).unwrap();
    assert_eq!(encoded["proposed_launches"][0]["clan"], json!("toobig-@"));
    assert_eq!(
        encoded["proposed_launches"][0]["clan_summary"],
        json!("[bold]Large modules[/bold]\nSplit by responsibility.")
    );
    assert_eq!(encoded["proposed_launches"][1]["clan"], json!(null));
    assert!(encoded["proposed_launches"][1]
        .as_object()
        .is_some_and(|proposal| !proposal.contains_key("clan_summary")));
}

#[test]
fn clan_summary_validation_is_field_specific_and_text_block_safe() {
    let valid_boundary = "é".repeat(16_384);
    parse_chop_result(
        &json!({
            "schema_version": 1,
            "status": "ok",
            "proposed_launches": [{
                "prompt": "Do work.",
                "workspace": "git:sase",
                "agent_name": "worker",
                "clan": "research",
                "clan_summary": valid_boundary
            }]
        })
        .to_string(),
    )
    .unwrap();

    let invalid = [
        (json!("  \n\t"), "blank_value"),
        (json!("contains\u{0000}nul"), "invalid_clan_summary"),
        (json!("x".repeat(32 * 1024 + 1)), "clan_summary_too_large"),
        (json!("closes ]] early"), "unrepresentable_clan_summary"),
        (json!("one+two"), "unrepresentable_clan_summary"),
    ];
    for (summary, expected_code) in invalid {
        let error = parse_chop_result(
            &json!({
                "schema_version": 1,
                "status": "ok",
                "proposed_launches": [{
                    "prompt": "Do work.",
                    "workspace": "git:sase",
                    "agent_name": "worker",
                    "clan": "research",
                    "clan_summary": summary
                }]
            })
            .to_string(),
        )
        .unwrap_err();
        assert_eq!(error.code, expected_code);
        assert_eq!(error.path, "$.proposed_launches[0].clan_summary");
    }

    let missing_clan = parse_chop_result(
        &json!({
            "schema_version": 1,
            "status": "ok",
            "proposed_launches": [{
                "prompt": "Do work.",
                "workspace": "git:sase",
                "clan_summary": "No clan"
            }]
        })
        .to_string(),
    )
    .unwrap_err();
    assert_eq!(missing_clan.code, "clan_summary_requires_clan");
    assert_eq!(missing_clan.path, "$.proposed_launches[0].clan_summary");
}

#[test]
fn clan_summaries_agree_per_raw_clan_before_launch() {
    parse_chop_result(
        &json!({
            "schema_version": 1,
            "status": "ok",
            "proposed_launches": [
                {
                    "prompt": "Research first.",
                    "workspace": "git:sase",
                    "agent_name": "first",
                    "clan": "research-@",
                    "clan_summary": "[bold]Research[/bold]"
                },
                {
                    "prompt": "Research second.",
                    "workspace": "git:sase",
                    "agent_name": "second",
                    "clan": "research-@"
                },
                {
                    "prompt": "Research third.",
                    "workspace": "git:sase",
                    "agent_name": "third",
                    "clan": "research-@",
                    "clan_summary": "[bold]Research[/bold]"
                },
                {
                    "prompt": "Review independently.",
                    "workspace": "git:sase",
                    "agent_name": "reviewer",
                    "clan": "review-@",
                    "clan_summary": "[italic]Review[/italic]"
                }
            ]
        })
        .to_string(),
    )
    .unwrap();

    let conflict = parse_chop_result(
        &json!({
            "schema_version": 1,
            "status": "ok",
            "proposed_launches": [
                {
                    "prompt": "Research first.",
                    "workspace": "git:sase",
                    "agent_name": "first",
                    "clan": "research-@",
                    "clan_summary": "Research"
                },
                {
                    "prompt": "Research second.",
                    "workspace": "git:sase",
                    "agent_name": "second",
                    "clan": "research-@",
                    "clan_summary": "Different"
                }
            ]
        })
        .to_string(),
    )
    .unwrap_err();
    assert_eq!(conflict.code, "conflicting_clan_summary");
    assert_eq!(conflict.path, "$.proposed_launches[1].clan_summary");
}

#[test]
fn clan_scoped_proposals_validate_member_and_directive_shapes() {
    let valid_shapes = [
        ("research", "worker"),
        ("research.@", "worker.deep"),
        ("research", "@"),
        // One marker in the clan plus one in the member is legal: each is
        // allocated in its own stage by the planner.
        ("research.@", "@"),
        ("toobig-@", "split_file.src.pkg.large.@"),
    ];
    for (clan, member) in valid_shapes {
        parse_chop_result(
            &json!({
                "schema_version": 1,
                "status": "ok",
                "proposed_launches": [{
                    "prompt": "Do work.",
                    "workspace": "git:sase",
                    "agent_name": member,
                    "clan": clan
                }]
            })
            .to_string(),
        )
        .unwrap();
    }

    let invalid = [
        (None, "research", None, "clan_member_required"),
        (Some(""), "research", None, "blank_value"),
        (
            Some("worker"),
            "research@@",
            None,
            "invalid_agent_name_template",
        ),
        (
            Some("split_file.src.pkg.large.@"),
            "toobig-@@",
            None,
            "invalid_agent_name_template",
        ),
        (Some("worker"), ".research", None, "malformed_agent_hood"),
        (
            Some("worker..review"),
            "research",
            None,
            "malformed_agent_hood",
        ),
        (
            Some("worker"),
            "research,other",
            None,
            "unrepresentable_clan_directive",
        ),
        (
            Some("worker"),
            "research",
            Some("review"),
            "clan_tribe_conflict",
        ),
        (
            Some("worker--review"),
            "research",
            None,
            "invalid_clan_member_name",
        ),
    ];
    for (member, clan, tribe, expected_code) in invalid {
        let error = parse_chop_result(
            &json!({
                "schema_version": 1,
                "status": "ok",
                "proposed_launches": [{
                    "prompt": "Do work.",
                    "workspace": "git:sase",
                    "agent_name": member,
                    "clan": clan,
                    "tribe": tribe
                }]
            })
            .to_string(),
        )
        .unwrap_err();
        assert_eq!(
            error.code, expected_code,
            "clan={clan:?}, member={member:?}"
        );
    }
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
    let name = derive_chop_agent_name(
        &"very-long-chop_".repeat(12),
        Some(&"very-long-target_".repeat(12)),
        0,
        Some("run-token"),
    )
    .unwrap();
    assert!(name.len() <= 120);
    assert!(!name.ends_with(['.', '-', '_']));
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
fn agent_clan_guard_matches_only_explicit_active_case_sensitive_clans() {
    let request = |agents: serde_json::Value| -> ChopDecisionRequestWire {
        serde_json::from_value(json!({
            "schema_version": 1,
            "inhibit_if": [{
                "provider": "agent_clan",
                "name_prefix": "toobig-"
            }],
            "trigger": {"provider": "always"},
            "agents": agents,
            "now": "2026-07-19T12:00:00Z"
        }))
        .unwrap()
    };

    let matching = evaluate_chop_decision(&request(json!([{
        "name": "toobig-0.split_file.src",
        "hood": "toobig-0",
        "agent_clan": "toobig-0",
        "active": true
    }])))
    .unwrap();
    assert_eq!(matching.outcome, "skip");
    assert_eq!(matching.provider.as_deref(), Some("agent_clan"));
    assert_eq!(
        matching.reason,
        "inhibited by active agent clan `toobig-0` member `toobig-0.split_file.src`"
    );

    for agents in [
        json!([{
            "name": "toobig-0.inferred_only",
            "hood": "toobig-0",
            "active": true
        }]),
        json!([{
            "name": "toobig-0.inactive",
            "agent_clan": "toobig-0",
            "active": false
        }]),
        json!([{
            "name": "Toobig-0.case_sensitive",
            "agent_clan": "Toobig-0",
            "active": true
        }]),
        json!([{
            "name": "other-0.member",
            "agent_clan": "other-0",
            "active": true
        }]),
    ] {
        let decision = evaluate_chop_decision(&request(agents)).unwrap();
        assert_eq!(decision.outcome, "fire");
    }
}

#[test]
fn agent_clan_guard_short_circuits_trigger_errors() {
    let request: ChopDecisionRequestWire = serde_json::from_value(json!({
        "schema_version": 1,
        "inhibit_if": [{
            "provider": "agent_clan",
            "name_prefix": "toobig-"
        }],
        "trigger": {
            "provider": "git.commits_since",
            "project": "",
            "threshold": 0
        },
        "agents": [{
            "name": "toobig-0.worker",
            "agent_clan": "toobig-0",
            "active": true
        }],
        "now": "2026-07-19T12:00:00Z"
    }))
    .unwrap();

    let decision = evaluate_chop_decision(&request).unwrap();
    assert_eq!(decision.outcome, "skip");
    assert_eq!(decision.provider.as_deref(), Some("agent_clan"));
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
fn once_per_release_removes_exact_keys_and_is_idempotent() {
    let request: ChopOncePerReleaseRequestWire =
        serde_json::from_value(json!({
            "schema_version": 1,
            "document": {"schema_version": 1, "entries": [
                {"key": "first", "seen_at": "t0"},
                {"key": "second", "seen_at": "t1"}
            ]},
            "keys": ["first", "missing", "first"]
        }))
        .unwrap();

    let released = release_chop_once_per(&request).unwrap();
    assert_eq!(released.released, 1);
    assert_eq!(released.document.entries.len(), 1);
    assert_eq!(released.document.entries[0].key, "second");

    let repeated = release_chop_once_per(&ChopOncePerReleaseRequestWire {
        document: released.document,
        ..request
    })
    .unwrap();
    assert_eq!(repeated.released, 0);
    assert_eq!(repeated.document.entries[0].key, "second");
}

#[test]
fn once_per_release_validates_engine_and_document_schemas() {
    let request = ChopOncePerReleaseRequestWire {
        schema_version: CHOP_ENGINE_SCHEMA_VERSION + 1,
        document: ChopSeenStoreDocumentWire::default(),
        keys: vec!["first".to_string()],
    };
    let engine_error = release_chop_once_per(&request).unwrap_err();
    assert_eq!(engine_error.code, "schema_version_mismatch");

    let state_error = release_chop_once_per(&ChopOncePerReleaseRequestWire {
        schema_version: CHOP_ENGINE_SCHEMA_VERSION,
        document: ChopSeenStoreDocumentWire {
            schema_version: CHOP_STATE_SCHEMA_VERSION + 1,
            entries: Vec::new(),
        },
        ..request
    })
    .unwrap_err();
    assert_eq!(state_error.code, "state_schema_version_mismatch");
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
fn axe_descriptions_split_into_normalized_summary_and_body() {
    for (description, expected) in [
        ("Run checks", ("Run checks", "")),
        (
            "Run checks\n\nExplain when the checks run.",
            ("Run checks", "Explain when the checks run."),
        ),
        (
            "Run checks\n\n- First check\n- Second check",
            ("Run checks", "- First check\n- Second check"),
        ),
        (
            "Run checks\r\n\r\nFirst line\r\nSecond line",
            ("Run checks", "First line\nSecond line"),
        ),
        (
            "  Run checks  \n \nBody line   \n  indented body  ",
            ("Run checks", "Body line\n  indented body"),
        ),
        (
            "Run checks\n\n \nFirst body line\n\nSecond body line\n \n",
            ("Run checks", "First body line\n\nSecond body line"),
        ),
    ] {
        assert_eq!(
            split_axe_description(description),
            (expected.0.to_string(), expected.1.to_string())
        );
    }
}

fn validate_one_axe_description(
    description: &str,
    require_description_shape: bool,
) -> Vec<crate::config::ConfigDiagnosticWire> {
    let request: AxeConfigValidationRequestWire =
        serde_json::from_value(json!({
            "schema_version": 1,
            "require_description_shape": require_description_shape,
            "config": {"axe": {"lumberjacks": {"checks": {
                "description": description
            }}}}
        }))
        .unwrap();
    validate_axe_config(&request).unwrap()
}

#[test]
fn strict_axe_validation_reports_each_description_shape_error_precisely() {
    let too_long_summary = "x".repeat(101);
    let too_long_description = format!("Summary\n\n{}", "x".repeat(1992));
    for (description, code, message) in [
        (
            "\n\nBody",
            "description_summary_blank",
            "description must start with a non-blank summary line".to_string(),
        ),
        (
            too_long_summary.as_str(),
            "description_summary_too_long",
            "description summary line must be at most 100 characters (found 101)"
                .to_string(),
        ),
        (
            "Summary\nBody",
            "description_body_separator_required",
            "description must leave line 2 blank to separate the summary from the body"
                .to_string(),
        ),
        (
            too_long_description.as_str(),
            "description_too_long",
            "description must be at most 2000 characters (found 2001)"
                .to_string(),
        ),
    ] {
        let diagnostics = validate_one_axe_description(description, true);
        assert_eq!(diagnostics.len(), 1, "{description:?}");
        assert_eq!(diagnostics[0].severity, "error");
        assert_eq!(diagnostics[0].code, code);
        assert_eq!(diagnostics[0].message, message);
        assert_eq!(
            diagnostics[0].path.as_deref(),
            Some("axe.lumberjacks.checks.description")
        );
    }
}

#[test]
fn strict_axe_validation_gates_description_shape_and_accepts_single_lines() {
    let too_long_summary = "x".repeat(101);
    let too_long_description = format!("Summary\n\n{}", "x".repeat(1992));
    for description in [
        "\n\nBody",
        too_long_summary.as_str(),
        "Summary\nBody",
        too_long_description.as_str(),
    ] {
        assert_eq!(validate_one_axe_description(description, false), vec![]);
    }
    assert_eq!(validate_one_axe_description("Run checks", true), vec![]);
    let blank = validate_one_axe_description(" \t", true);
    assert_eq!(blank.len(), 1);
    assert_eq!(blank[0].code, "blank_value");
}

#[test]
fn strict_axe_validation_counts_description_limits_in_characters() {
    assert_eq!(validate_one_axe_description(&"é".repeat(100), true), vec![]);
    let summary_diagnostics =
        validate_one_axe_description(&"é".repeat(101), true);
    assert_eq!(summary_diagnostics.len(), 1);
    assert_eq!(
        summary_diagnostics[0].message,
        "description summary line must be at most 100 characters (found 101)"
    );

    let description = format!("Summary\n\n{}", "界".repeat(1991));
    assert_eq!(description.chars().count(), 2000);
    assert_eq!(validate_one_axe_description(&description, true), vec![]);
}

#[test]
fn strict_axe_validation_emits_only_the_first_description_shape_error() {
    let diagnostics = validate_one_axe_description(
        &format!("{}\nBody", "x".repeat(2001)),
        true,
    );
    assert_eq!(diagnostics.len(), 1);
    assert_eq!(diagnostics[0].code, "description_summary_too_long");
}

#[test]
fn strict_axe_validation_accepts_new_shape() {
    let request: AxeConfigValidationRequestWire =
        serde_json::from_value(json!({
            "schema_version": 1,
            "config": {"axe": {
                "max_hook_runners": 3,
                "lumberjack_log_temp_max_age_seconds": 300,
                "lumberjack_restart_backoff_max_seconds": 60,
                "lumberjacks": {"docs": {
                    "description": "Refresh documentation on a daily cadence",
                    "interval": 5,
                    "chop_timeout": "1m30s",
                    "env": {"TOKEN": {"env": "DOCS_TOKEN"}},
                    "chops": {"refresh_docs": {
                        "description": "Refresh generated documentation",
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
fn strict_axe_validation_accepts_missing_descriptions_by_default() {
    let request: AxeConfigValidationRequestWire =
        serde_json::from_value(json!({
            "schema_version": 1,
            "config": {"axe": {"lumberjacks": {"checks": {
                "chops": {"hook_checks": {"script": "run-hooks"}}
            }}}}
        }))
        .unwrap();

    assert!(!request.require_descriptions);
    assert_eq!(validate_axe_config(&request).unwrap(), vec![]);
}

#[test]
fn strict_axe_validation_accepts_nonnegative_or_missing_wait_runners() {
    let request: AxeConfigValidationRequestWire =
        serde_json::from_value(json!({
            "schema_version": 1,
            "config": {"axe": {"lumberjacks": {
                "exclusive": {"wait_runners": 0},
                "background": {"wait_runners": 3},
                "default": {}
            }}}
        }))
        .unwrap();

    assert_eq!(validate_axe_config(&request).unwrap(), vec![]);
}

#[test]
fn strict_axe_validation_rejects_invalid_wait_runners() {
    for value in [json!(-1), json!("1"), json!(1.5)] {
        let request: AxeConfigValidationRequestWire =
            serde_json::from_value(json!({
                "schema_version": 1,
                "config": {"axe": {"lumberjacks": {"checks": {
                    "wait_runners": value
                }}}},
                "provenance": {
                    "axe.lumberjacks.checks.wait_runners": "overlay:test.yml"
                }
            }))
            .unwrap();

        let diagnostics = validate_axe_config(&request).unwrap();
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].code, "negative_integer");
        assert_eq!(
            diagnostics[0].message,
            "value must be a non-negative integer"
        );
        assert_eq!(
            diagnostics[0].path.as_deref(),
            Some("axe.lumberjacks.checks.wait_runners")
        );
        assert_eq!(diagnostics[0].layer.as_deref(), Some("overlay:test.yml"));
    }
}

#[test]
fn strict_axe_validation_rejects_blank_descriptions() {
    let request: AxeConfigValidationRequestWire =
        serde_json::from_value(json!({
            "schema_version": 1,
            "config": {"axe": {"lumberjacks": {"checks": {
                "description": " ",
                "chops": {"hook_checks": {"description": "\t"}}
            }}}}
        }))
        .unwrap();

    let diagnostics = validate_axe_config(&request).unwrap();
    assert_eq!(diagnostics.len(), 2);
    assert!(diagnostics.iter().all(|item| item.code == "blank_value"));
    assert_eq!(
        diagnostics
            .iter()
            .filter_map(|item| item.path.as_deref())
            .collect::<Vec<_>>(),
        vec![
            "axe.lumberjacks.checks.chops.hook_checks.description",
            "axe.lumberjacks.checks.description",
        ]
    );
}

#[test]
fn strict_axe_validation_requires_lumberjack_and_chop_descriptions_when_enabled(
) {
    let request: AxeConfigValidationRequestWire =
        serde_json::from_value(json!({
            "schema_version": 1,
            "require_descriptions": true,
            "config": {"axe": {"lumberjacks": {
                "list_lane": {
                    "description": "Run list-form checks",
                    "chops": ["bare_check"]
                },
                "map_lane": {
                    "description": "Run map-form checks",
                    "chops": {"map_check": {}}
                },
                "missing_lane": {}
            }}}
        }))
        .unwrap();

    let diagnostics = validate_axe_config(&request).unwrap();
    assert_eq!(diagnostics.len(), 3);
    assert!(diagnostics
        .iter()
        .all(|item| item.code == "required_missing"));
    assert_eq!(
        diagnostics
            .iter()
            .map(|item| (
                item.path.as_deref().unwrap(),
                item.message.as_str()
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                "axe.lumberjacks.list_lane.chops.bare_check.description",
                "chop `bare_check` requires a non-empty `description`; list-form string entries cannot carry one, so use the map form",
            ),
            (
                "axe.lumberjacks.map_lane.chops.map_check.description",
                "chop `map_check` requires a non-empty `description`; list-form string entries cannot carry one, so use the map form",
            ),
            (
                "axe.lumberjacks.missing_lane.description",
                "lumberjack `missing_lane` requires a non-empty `description`",
            ),
        ]
    );
}

#[test]
fn strict_axe_validation_accepts_keyed_and_tagged_agent_clan_guards() {
    let request: AxeConfigValidationRequestWire =
        serde_json::from_value(json!({
            "schema_version": 1,
            "config": {"lumberjacks": {"guards": {"chops": {
                "keyed": {
                    "inhibit_if": {
                        "agent_clan": {"name_prefix": "toobig-"}
                    }
                },
                "tagged": {
                    "inhibit_if": [{
                        "provider": "agent_clan",
                        "name_prefix": "toobig-"
                    }]
                }
            }}}}
        }))
        .unwrap();

    assert_eq!(validate_axe_config(&request).unwrap(), vec![]);
}

#[test]
fn strict_axe_validation_rejects_invalid_agent_clan_guards_fail_closed() {
    let request: AxeConfigValidationRequestWire =
        serde_json::from_value(json!({
            "schema_version": 1,
            "config": {"lumberjacks": {"guards": {"chops": {
                "missing": {"inhibit_if": {"agent_clan": {}}},
                "blank": {"inhibit_if": [{
                    "provider": "agent_clan",
                    "name_prefix": "  "
                }]},
                "unknown": {"inhibit_if": {
                    "agent_clan": {
                        "name_prefix": "toobig-",
                        "hood": "toobig"
                    }
                }}
            }}}}
        }))
        .unwrap();

    let diagnostics = validate_axe_config(&request).unwrap();
    let codes: Vec<_> =
        diagnostics.iter().map(|item| item.code.as_str()).collect();
    assert!(codes.contains(&"nonblank_string_required"));
    assert!(codes.contains(&"required_missing"));
    assert!(codes.contains(&"unknown_key"));
    assert!(diagnostics.iter().any(|item| {
        item.path.as_deref()
            == Some(
                "lumberjacks.guards.chops.unknown.inhibit_if.agent_clan.hood",
            )
    }));
}

#[test]
fn strict_axe_validation_rejects_non_positive_log_temp_max_age() {
    let request: AxeConfigValidationRequestWire =
        serde_json::from_value(json!({
            "schema_version": 1,
            "config": {"axe": {"lumberjack_log_temp_max_age_seconds": 0}}
        }))
        .unwrap();

    let diagnostics = validate_axe_config(&request).unwrap();

    assert_eq!(diagnostics.len(), 1);
    assert_eq!(diagnostics[0].code, "non_positive_integer");
    assert_eq!(
        diagnostics[0].path.as_deref(),
        Some("axe.lumberjack_log_temp_max_age_seconds")
    );
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
