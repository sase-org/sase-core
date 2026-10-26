use super::*;

fn retry_row(
    global_agent: &str,
    revision: &str,
    terminal: bool,
    quarantined: bool,
    failure: &str,
) -> PublicationRetryRowWire {
    retry_row_at(global_agent, revision, terminal, quarantined, failure, 0.0)
}

fn retry_row_at(
    global_agent: &str,
    revision: &str,
    terminal: bool,
    quarantined: bool,
    failure: &str,
    created_at: f64,
) -> PublicationRetryRowWire {
    PublicationRetryRowWire {
        global_agent: global_agent.to_string(),
        primary_revision: revision.to_string(),
        terminal,
        quarantined,
        last_error: Some(failure.to_string()),
        terminal_reason: terminal.then(|| failure.to_string()),
        created_at,
    }
}

fn key(global_agent: &str, revision: &str) -> PublicationLogicalKeyWire {
    PublicationLogicalKeyWire {
        global_agent: global_agent.to_string(),
        primary_revision: revision.to_string(),
    }
}

fn sha(ch: char) -> String {
    ch.to_string().repeat(40)
}

#[test]
fn select_retries_revives_only_the_requested_terminal_classes() {
    let request = PublicationRetrySelectionRequestWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        retry_retired: true,
        retry_quarantined: true,
        rows: vec![
            retry_row(
                "alice.athena.foo",
                sha('a').as_str(),
                true,
                false,
                "legacy mismatch",
            ),
            retry_row(
                "alice.athena.bar",
                sha('b').as_str(),
                false,
                true,
                "push rejected",
            ),
            retry_row(
                "alice.athena.active",
                sha('c').as_str(),
                false,
                false,
                "still working",
            ),
            retry_row(
                "alice.athena.other",
                sha('d').as_str(),
                true,
                false,
                "legacy mismatch",
            ),
        ],
        limit: None,
        error_classes: vec![],
    };
    let selected = select_publication_retries(&request).unwrap().selected;
    assert_eq!(selected.len(), 3);
    assert_eq!(selected[0].prior_class, "retired");
    assert_eq!(selected[0].prior_failure, "legacy mismatch");
    assert_eq!(selected[0].error_class, PUBLICATION_ERROR_OTHER);
    assert_eq!(selected[1].prior_class, "quarantined");
    assert!(selected
        .iter()
        .all(|row| row.global_agent != "alice.athena.active"));
}

#[test]
fn select_retries_retired_only_leaves_quarantined_rows() {
    let request = PublicationRetrySelectionRequestWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        retry_retired: true,
        retry_quarantined: false,
        rows: vec![
            retry_row(
                "alice.athena.foo",
                sha('a').as_str(),
                true,
                false,
                "legacy",
            ),
            retry_row(
                "alice.athena.bar",
                sha('b').as_str(),
                false,
                true,
                "temp",
            ),
        ],
        limit: None,
        error_classes: vec![],
    };
    let selected = select_publication_retries(&request).unwrap().selected;
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].global_agent, "alice.athena.foo");
}

#[test]
fn missing_retry_fields_keep_the_existing_unbounded_contract() {
    let request: PublicationRetrySelectionRequestWire =
        serde_json::from_value(serde_json::json!({
            "schema_version": 1,
            "retry_retired": true,
            "retry_quarantined": true,
            "rows": [
                {
                    "global_agent": "alice.athena.foo",
                    "primary_revision": sha('a'),
                    "terminal": true,
                    "quarantined": false,
                    "last_error": "legacy mismatch",
                    "terminal_reason": "legacy mismatch"
                },
                {
                    "global_agent": "alice.athena.bar",
                    "primary_revision": sha('b'),
                    "terminal": false,
                    "quarantined": true,
                    "last_error": "push rejected"
                }
            ]
        }))
        .unwrap();
    assert!(request.limit.is_none());
    assert!(request.error_classes.is_empty());
    assert_eq!(request.rows[1].created_at, 0.0);
    let selected = select_publication_retries(&request).unwrap().selected;
    assert_eq!(selected.len(), 2);
    assert_eq!(selected[0].global_agent, "alice.athena.foo");
    assert_eq!(selected[1].global_agent, "alice.athena.bar");
}

#[test]
fn duplicate_logical_keys_keep_the_first_row() {
    let request = PublicationRetrySelectionRequestWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        retry_retired: true,
        retry_quarantined: true,
        rows: vec![
            retry_row(
                "alice.athena.foo",
                sha('a').as_str(),
                true,
                false,
                "first",
            ),
            retry_row(
                "alice.athena.foo",
                sha('a').as_str(),
                false,
                true,
                "duplicate",
            ),
        ],
        limit: None,
        error_classes: vec![],
    };
    let selected = select_publication_retries(&request).unwrap().selected;
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].prior_class, PUBLICATION_LIFECYCLE_RETIRED);
    assert_eq!(selected[0].prior_failure, "first");
}

#[test]
fn limit_filters_then_prefers_retired_over_quarantined() {
    let request = PublicationRetrySelectionRequestWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        retry_retired: true,
        retry_quarantined: true,
        rows: vec![
            retry_row_at(
                "alice.athena.q1",
                sha('b').as_str(),
                false,
                true,
                "push rejected",
                1.0,
            ),
            retry_row_at(
                "alice.athena.r2",
                sha('d').as_str(),
                true,
                false,
                "legacy mismatch",
                4.0,
            ),
            retry_row_at(
                "alice.athena.r1",
                sha('a').as_str(),
                true,
                false,
                "legacy mismatch",
                2.0,
            ),
            retry_row_at(
                "alice.athena.q2",
                sha('c').as_str(),
                false,
                true,
                "push rejected",
                3.0,
            ),
        ],
        limit: Some(3),
        error_classes: vec![],
    };
    let selected = select_publication_retries(&request).unwrap().selected;
    let names: Vec<&str> = selected
        .iter()
        .map(|row| row.global_agent.as_str())
        .collect();
    assert_eq!(
        names,
        vec!["alice.athena.r1", "alice.athena.r2", "alice.athena.q1",]
    );
}

#[test]
fn error_class_filter_applies_before_the_limit() {
    let request = PublicationRetrySelectionRequestWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        retry_retired: true,
        retry_quarantined: true,
        rows: vec![
            retry_row_at(
                "alice.athena.timeout",
                sha('a').as_str(),
                false,
                true,
                "agent-hood publication did not complete within 120s",
                1.0,
            ),
            retry_row_at(
                "alice.athena.other",
                sha('b').as_str(),
                true,
                false,
                "legacy mismatch",
                0.0,
            ),
            retry_row_at(
                "alice.athena.lock",
                sha('c').as_str(),
                false,
                true,
                "agents sync lock is busy",
                2.0,
            ),
        ],
        limit: Some(1),
        error_classes: vec![
            PUBLICATION_ERROR_TIMEOUT.to_string(),
            PUBLICATION_ERROR_LOCK_BUSY.to_string(),
        ],
    };
    let selected = select_publication_retries(&request).unwrap().selected;
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].global_agent, "alice.athena.timeout");
    assert_eq!(selected[0].error_class, PUBLICATION_ERROR_TIMEOUT);
}

#[test]
fn invalid_limit_and_unknown_error_class_are_rejected() {
    let mut request = PublicationRetrySelectionRequestWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        retry_retired: true,
        retry_quarantined: false,
        rows: vec![retry_row(
            "alice.athena.foo",
            sha('a').as_str(),
            true,
            false,
            "legacy",
        )],
        limit: Some(0),
        error_classes: vec![],
    };
    let err = select_publication_retries(&request).unwrap_err();
    assert!(err.to_string().contains("limit must be a positive integer"));

    request.limit = Some(1);
    request.error_classes = vec!["not_a_class".to_string()];
    let err = select_publication_retries(&request).unwrap_err();
    assert!(err.to_string().contains("unknown publication error class"));
}

#[test]
fn classifies_report_strings_with_existing_precedence() {
    let cases = [
        (None, PUBLICATION_ERROR_UNKNOWN),
        (Some(""), PUBLICATION_ERROR_UNKNOWN),
        (
            Some("agent-hood publication did not complete within 120s"),
            PUBLICATION_ERROR_TIMEOUT,
        ),
        (
            Some("agents sync lock is busy; lock holder: pid 1, age 3s"),
            PUBLICATION_ERROR_LOCK_BUSY,
        ),
        (
            Some("agents sync clone lock is busy"),
            PUBLICATION_ERROR_LOCK_BUSY,
        ),
        (
            Some("could not stage: .git/index.lock exists"),
            PUBLICATION_ERROR_INDEX_LOCK,
        ),
        (
            Some("git pull --rebase failed: unstaged changes"),
            PUBLICATION_ERROR_UNSTAGED_CHANGES,
        ),
        (
            Some("error: Your index contains uncommitted changes."),
            PUBLICATION_ERROR_UNSTAGED_CHANGES,
        ),
        (
            Some("rebase conflict on agents/alice/page.md"),
            PUBLICATION_ERROR_REBASE_CONFLICT,
        ),
        (
            Some("publication payload exceeds the byte limit"),
            PUBLICATION_ERROR_MANIFEST_MISMATCH,
        ),
        (
            Some("owner manifest does not decode"),
            PUBLICATION_ERROR_MANIFEST_MISMATCH,
        ),
        (Some("weird new failure"), PUBLICATION_ERROR_OTHER),
    ];
    for (error, expected) in cases {
        assert_eq!(
            classify_publication_error_class(error),
            expected,
            "{error:?}"
        );
        let classified =
            classify_publication_error(&PublicationErrorClassifyRequestWire {
                schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
                error: error.map(str::to_string),
            })
            .unwrap();
        assert_eq!(classified.error_class, expected);
    }
}

#[test]
fn missing_classify_error_field_is_unknown() {
    let request: PublicationErrorClassifyRequestWire =
        serde_json::from_value(serde_json::json!({"schema_version": 1}))
            .unwrap();
    let classified = classify_publication_error(&request).unwrap();
    assert_eq!(classified.error_class, PUBLICATION_ERROR_UNKNOWN);
}

#[test]
fn repository_and_unknown_scope_do_not_charge_rows() {
    let attempted = vec![key("alice.athena.foo", sha('a').as_str())];
    let rows = vec![PublicationFailureRowWire {
        global_agent: "alice.athena.foo".to_string(),
        primary_revision: sha('a'),
        attempts: 2,
        last_error: Some("prior".to_string()),
        terminal: false,
        quarantined: false,
        terminal_reason: None,
    }];
    for scope in [Some("repository"), Some("mystery"), None] {
        let decision = decide_publication_failure_accounting(
            &PublicationFailureAccountingRequestWire {
                schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
                scope: scope.map(str::to_string),
                error: "agents sync lock is busy".to_string(),
                format_error: false,
                attempted_keys: attempted.clone(),
                rows: rows.clone(),
                quarantine_threshold: Some(3),
            },
        )
        .unwrap();
        assert!(!decision.charge, "{scope:?}");
        assert!(decision.charges.is_empty(), "{scope:?}");
        assert_eq!(decision.error_class, PUBLICATION_ERROR_LOCK_BUSY);
        if scope == Some("repository") {
            assert_eq!(decision.scope, PUBLICATION_FAILURE_SCOPE_REPOSITORY);
        } else {
            assert_eq!(decision.scope, PUBLICATION_FAILURE_SCOPE_UNKNOWN);
        }
    }
}

#[test]
fn hood_scope_charges_only_attempted_keys_and_retires_repeated_format_errors() {
    let foo = sha('a');
    let bar = sha('b');
    let extra = sha('c');
    let error = "hood 'foo' has no publishable runs";
    let decision = decide_publication_failure_accounting(
        &PublicationFailureAccountingRequestWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            scope: Some(PUBLICATION_FAILURE_SCOPE_HOOD.to_string()),
            error: error.to_string(),
            format_error: true,
            attempted_keys: vec![
                key("alice.athena.foo", foo.as_str()),
                key("alice.athena.foo", foo.as_str()),
                key("alice.athena.bar", bar.as_str()),
            ],
            rows: vec![
                PublicationFailureRowWire {
                    global_agent: "alice.athena.foo".to_string(),
                    primary_revision: foo.clone(),
                    attempts: 1,
                    last_error: Some(error.to_string()),
                    terminal: false,
                    quarantined: false,
                    terminal_reason: None,
                },
                PublicationFailureRowWire {
                    global_agent: "alice.athena.bar".to_string(),
                    primary_revision: bar.clone(),
                    attempts: 0,
                    last_error: None,
                    terminal: false,
                    quarantined: false,
                    terminal_reason: None,
                },
                PublicationFailureRowWire {
                    global_agent: "alice.athena.extra".to_string(),
                    primary_revision: extra,
                    attempts: 2,
                    last_error: Some(error.to_string()),
                    terminal: false,
                    quarantined: false,
                    terminal_reason: None,
                },
            ],
            quarantine_threshold: Some(3),
        },
    )
    .unwrap();
    assert!(decision.charge);
    assert_eq!(decision.scope, PUBLICATION_FAILURE_SCOPE_HOOD);
    assert_eq!(decision.charges.len(), 2);
    assert!(decision.charges[0].retire);
    assert!(!decision.charges[0].quarantine);
    assert_eq!(decision.charges[0].attempts, 2);
    assert_eq!(decision.charges[0].terminal_reason.as_deref(), Some(error));
    assert!(!decision.charges[1].retire);
    assert!(!decision.charges[1].quarantine);
    assert_eq!(decision.charges[1].attempts, 1);
    assert!(decision
        .charges
        .iter()
        .all(|charge| charge.global_agent != "alice.athena.extra"));
}

#[test]
fn hood_scope_quarantines_at_the_attempt_threshold() {
    let decision = decide_publication_failure_accounting(
        &PublicationFailureAccountingRequestWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            scope: Some(PUBLICATION_FAILURE_SCOPE_HOOD.to_string()),
            error: "push rejected".to_string(),
            format_error: false,
            attempted_keys: vec![key("alice.athena.foo", sha('a').as_str())],
            rows: vec![PublicationFailureRowWire {
                global_agent: "alice.athena.foo".to_string(),
                primary_revision: sha('a'),
                attempts: 2,
                last_error: Some("timeout".to_string()),
                terminal: false,
                quarantined: false,
                terminal_reason: None,
            }],
            quarantine_threshold: Some(3),
        },
    )
    .unwrap();
    assert_eq!(decision.charges.len(), 1);
    assert!(decision.charges[0].quarantine);
    assert!(!decision.charges[0].retire);
    assert_eq!(decision.charges[0].attempts, 3);
}

#[test]
fn drain_selects_preferred_then_oldest_active_then_stopped() {
    let request = PublicationDrainSelectionRequestWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        limit: 4,
        retry_retired: true,
        retry_quarantined: true,
        rows: vec![
            retry_row_at(
                "alice.athena.old",
                sha('a').as_str(),
                false,
                false,
                "active",
                1.0,
            ),
            retry_row_at(
                "alice.athena.new",
                sha('b').as_str(),
                false,
                false,
                "active",
                5.0,
            ),
            retry_row_at(
                "alice.athena.mid",
                sha('c').as_str(),
                false,
                false,
                "active",
                3.0,
            ),
            retry_row_at(
                "alice.athena.retired",
                sha('d').as_str(),
                true,
                false,
                "legacy mismatch",
                0.5,
            ),
            retry_row_at(
                "alice.athena.quarantined",
                sha('e').as_str(),
                false,
                true,
                "push rejected",
                0.25,
            ),
        ],
        error_classes: vec![],
        preferred_key: Some(key("alice.athena.new", sha('b').as_str())),
        exact_key: None,
    };
    let selected = select_publication_drain(&request).unwrap();
    let active: Vec<&str> = selected
        .active
        .iter()
        .map(|row| row.global_agent.as_str())
        .collect();
    let revive: Vec<&str> = selected
        .revive
        .iter()
        .map(|row| row.global_agent.as_str())
        .collect();
    assert_eq!(
        active,
        vec!["alice.athena.new", "alice.athena.old", "alice.athena.mid",]
    );
    assert_eq!(revive, vec!["alice.athena.retired"]);
    assert!(selected
        .active
        .iter()
        .all(|row| row.prior_class == PUBLICATION_LIFECYCLE_ACTIVE));
    assert_eq!(
        selected.revive[0].prior_class,
        PUBLICATION_LIFECYCLE_RETIRED
    );
}

#[test]
fn drain_active_rows_can_exhaust_the_budget() {
    let request = PublicationDrainSelectionRequestWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        limit: 1,
        retry_retired: true,
        retry_quarantined: true,
        rows: vec![
            retry_row_at(
                "alice.athena.active",
                sha('a').as_str(),
                false,
                false,
                "active",
                2.0,
            ),
            retry_row_at(
                "alice.athena.retired",
                sha('b').as_str(),
                true,
                false,
                "legacy mismatch",
                1.0,
            ),
        ],
        error_classes: vec![],
        preferred_key: None,
        exact_key: None,
    };
    let selected = select_publication_drain(&request).unwrap();
    assert_eq!(selected.active.len(), 1);
    assert!(selected.revive.is_empty());
    assert_eq!(selected.active[0].global_agent, "alice.athena.active");
}

#[test]
fn drain_no_match_is_empty_not_unbounded() {
    let request = PublicationDrainSelectionRequestWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        limit: 25,
        retry_retired: true,
        retry_quarantined: false,
        rows: vec![retry_row(
            "alice.athena.bar",
            sha('b').as_str(),
            false,
            true,
            "push rejected",
        )],
        error_classes: vec![PUBLICATION_ERROR_TIMEOUT.to_string()],
        preferred_key: None,
        exact_key: None,
    };
    let selected = select_publication_drain(&request).unwrap();
    assert!(selected.active.is_empty());
    assert!(selected.revive.is_empty());
}

#[test]
fn drain_error_class_filter_ignores_active_rows() {
    let request = PublicationDrainSelectionRequestWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        limit: 3,
        retry_retired: true,
        retry_quarantined: true,
        rows: vec![
            retry_row_at(
                "alice.athena.active",
                sha('a').as_str(),
                false,
                false,
                "weird new failure",
                1.0,
            ),
            retry_row_at(
                "alice.athena.timeout",
                sha('b').as_str(),
                false,
                true,
                "agent-hood publication did not complete within 120s",
                2.0,
            ),
            retry_row_at(
                "alice.athena.other",
                sha('c').as_str(),
                true,
                false,
                "legacy mismatch",
                0.5,
            ),
        ],
        error_classes: vec![PUBLICATION_ERROR_TIMEOUT.to_string()],
        preferred_key: None,
        exact_key: None,
    };
    let selected = select_publication_drain(&request).unwrap();
    assert_eq!(selected.active.len(), 1);
    assert_eq!(selected.active[0].global_agent, "alice.athena.active");
    assert_eq!(selected.revive.len(), 1);
    assert_eq!(selected.revive[0].global_agent, "alice.athena.timeout");
}

#[test]
fn drain_key_tie_break_is_stable() {
    let request = PublicationDrainSelectionRequestWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        limit: 2,
        retry_retired: false,
        retry_quarantined: false,
        rows: vec![
            retry_row_at(
                "alice.athena.b",
                sha('b').as_str(),
                false,
                false,
                "active",
                1.0,
            ),
            retry_row_at(
                "alice.athena.a",
                sha('a').as_str(),
                false,
                false,
                "active",
                1.0,
            ),
        ],
        error_classes: vec![],
        preferred_key: None,
        exact_key: None,
    };
    let selected = select_publication_drain(&request).unwrap();
    assert_eq!(selected.active[0].global_agent, "alice.athena.a");
    assert_eq!(selected.active[1].global_agent, "alice.athena.b");
}

#[test]
fn drain_exact_key_selects_only_the_matching_stopped_row() {
    let target_revision = sha('t');
    let request = PublicationDrainSelectionRequestWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        limit: 3,
        retry_retired: false,
        retry_quarantined: true,
        rows: vec![
            retry_row_at(
                "alice.athena.active",
                sha('a').as_str(),
                false,
                false,
                "active",
                0.5,
            ),
            retry_row_at(
                "alice.athena.target",
                target_revision.as_str(),
                false,
                true,
                "agent-hood publication did not complete within 120s",
                2.0,
            ),
            retry_row_at(
                "alice.athena.other",
                sha('o').as_str(),
                false,
                true,
                "agent-hood publication did not complete within 120s",
                1.0,
            ),
        ],
        error_classes: vec![PUBLICATION_ERROR_TIMEOUT.to_string()],
        preferred_key: None,
        exact_key: Some(key("alice.athena.target", target_revision.as_str())),
    };
    let selected = select_publication_drain(&request).unwrap();
    assert!(selected.active.is_empty());
    assert_eq!(selected.revive.len(), 1);
    assert_eq!(selected.revive[0].global_agent, "alice.athena.target");
    assert_eq!(selected.revive[0].primary_revision, target_revision);
    assert_eq!(selected.revive[0].error_class, PUBLICATION_ERROR_TIMEOUT);
}

#[test]
fn drain_exact_key_missing_key_selects_nothing() {
    let request = PublicationDrainSelectionRequestWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        limit: 3,
        retry_retired: true,
        retry_quarantined: true,
        rows: vec![
            retry_row_at(
                "alice.athena.active",
                sha('a').as_str(),
                false,
                false,
                "active",
                0.5,
            ),
            retry_row_at(
                "alice.athena.timeout",
                sha('b').as_str(),
                false,
                true,
                "agent-hood publication did not complete within 120s",
                1.0,
            ),
        ],
        error_classes: vec![PUBLICATION_ERROR_TIMEOUT.to_string()],
        preferred_key: None,
        exact_key: Some(key("alice.athena.gone", sha('z').as_str())),
    };
    let selected = select_publication_drain(&request).unwrap();
    assert!(selected.active.is_empty());
    assert!(selected.revive.is_empty());
}

#[test]
fn drain_exact_key_wrong_error_class_selects_nothing() {
    let target_revision = sha('t');
    let request = PublicationDrainSelectionRequestWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        limit: 2,
        retry_retired: false,
        retry_quarantined: true,
        rows: vec![retry_row_at(
            "alice.athena.target",
            target_revision.as_str(),
            false,
            true,
            "agent-hood publication did not complete within 120s",
            1.0,
        )],
        error_classes: vec![PUBLICATION_ERROR_REBASE_CONFLICT.to_string()],
        preferred_key: None,
        exact_key: Some(key("alice.athena.target", target_revision.as_str())),
    };
    let selected = select_publication_drain(&request).unwrap();
    assert!(selected.active.is_empty());
    assert!(selected.revive.is_empty());
}

#[test]
fn drain_exact_key_retired_row_needs_retired_permission() {
    let target_revision = sha('t');
    let rows = || {
        vec![retry_row_at(
            "alice.athena.target",
            target_revision.as_str(),
            true,
            false,
            "owner manifest does not decode",
            1.0,
        )]
    };
    let denied = PublicationDrainSelectionRequestWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        limit: 1,
        retry_retired: false,
        retry_quarantined: true,
        rows: rows(),
        error_classes: vec![PUBLICATION_ERROR_MANIFEST_MISMATCH.to_string()],
        preferred_key: None,
        exact_key: Some(key("alice.athena.target", target_revision.as_str())),
    };
    let selected = select_publication_drain(&denied).unwrap();
    assert!(selected.active.is_empty());
    assert!(selected.revive.is_empty());

    let allowed = PublicationDrainSelectionRequestWire {
        retry_retired: true,
        ..denied
    };
    let selected = select_publication_drain(&allowed).unwrap();
    assert!(selected.active.is_empty());
    assert_eq!(selected.revive.len(), 1);
    assert_eq!(selected.revive[0].global_agent, "alice.athena.target");
}

#[test]
fn drain_exact_key_rejects_simultaneous_preferred_key() {
    let request = PublicationDrainSelectionRequestWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        limit: 1,
        retry_retired: true,
        retry_quarantined: true,
        rows: vec![],
        error_classes: vec![],
        preferred_key: Some(key("alice.athena.foo", sha('a').as_str())),
        exact_key: Some(key("alice.athena.foo", sha('a').as_str())),
    };
    let err = select_publication_drain(&request).unwrap_err();
    assert!(err.to_string().contains("mutually exclusive"));
}

#[test]
fn drain_exact_key_rejects_empty_identity() {
    let request = PublicationDrainSelectionRequestWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        limit: 1,
        retry_retired: true,
        retry_quarantined: true,
        rows: vec![],
        error_classes: vec![],
        preferred_key: None,
        exact_key: Some(key("", sha('a').as_str())),
    };
    let err = select_publication_drain(&request).unwrap_err();
    assert!(err.to_string().contains("global_agent must not be empty"));

    let request = PublicationDrainSelectionRequestWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        limit: 1,
        retry_retired: true,
        retry_quarantined: true,
        rows: vec![],
        error_classes: vec![],
        preferred_key: None,
        exact_key: Some(key("alice.athena.foo", "   ")),
    };
    let err = select_publication_drain(&request).unwrap_err();
    assert!(err
        .to_string()
        .contains("primary_revision must not be empty"));
}

#[test]
fn drain_exact_key_deduplicates_repeated_rows() {
    let target_revision = sha('t');
    let request = PublicationDrainSelectionRequestWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        limit: 2,
        retry_retired: false,
        retry_quarantined: true,
        rows: vec![
            retry_row_at(
                "alice.athena.target",
                target_revision.as_str(),
                false,
                true,
                "agent-hood publication did not complete within 120s",
                1.0,
            ),
            retry_row_at(
                "alice.athena.target",
                target_revision.as_str(),
                false,
                true,
                "agent-hood publication did not complete within 120s",
                2.0,
            ),
        ],
        error_classes: vec![PUBLICATION_ERROR_TIMEOUT.to_string()],
        preferred_key: None,
        exact_key: Some(key("alice.athena.target", target_revision.as_str())),
    };
    let selected = select_publication_drain(&request).unwrap();
    assert!(selected.active.is_empty());
    assert_eq!(selected.revive.len(), 1);
}

#[test]
fn drain_exact_key_resumes_only_the_matching_active_row() {
    let target_revision = sha('t');
    let request = PublicationDrainSelectionRequestWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        limit: 5,
        retry_retired: false,
        retry_quarantined: false,
        rows: vec![
            retry_row_at(
                "alice.athena.active",
                sha('a').as_str(),
                false,
                false,
                "active",
                0.5,
            ),
            retry_row_at(
                "alice.athena.target",
                target_revision.as_str(),
                false,
                false,
                "active",
                1.0,
            ),
        ],
        error_classes: vec![],
        preferred_key: None,
        exact_key: Some(key("alice.athena.target", target_revision.as_str())),
    };
    let selected = select_publication_drain(&request).unwrap();
    assert_eq!(selected.active.len(), 1);
    assert_eq!(selected.active[0].global_agent, "alice.athena.target");
    assert!(selected.revive.is_empty());
}

#[test]
fn drain_old_wire_without_exact_key_keeps_default_selection() {
    let request: PublicationDrainSelectionRequestWire =
        serde_json::from_value(serde_json::json!({
            "schema_version": PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            "limit": 1,
            "retry_retired": true,
            "retry_quarantined": true,
            "rows": [
                {
                    "global_agent": "alice.athena.active",
                    "primary_revision": sha('a'),
                    "terminal": false,
                    "quarantined": false,
                    "last_error": "active",
                    "created_at": 0.5
                },
                {
                    "global_agent": "alice.athena.timeout",
                    "primary_revision": sha('b'),
                    "terminal": false,
                    "quarantined": true,
                    "last_error": "agent-hood publication did not complete within 120s",
                    "created_at": 0.25
                }
            ],
            "error_classes": [PUBLICATION_ERROR_TIMEOUT],
        }))
        .unwrap();
    assert!(request.exact_key.is_none());
    let selected = select_publication_drain(&request).unwrap();
    assert_eq!(selected.active.len(), 1);
    assert_eq!(selected.active[0].global_agent, "alice.athena.active");
    assert!(selected.revive.is_empty());
}

#[test]
fn completion_requires_session_page_and_revision_not_an_unrelated_readme() {
    let revision = sha('a');
    let request = PublicationCompletionRequestWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        request: PublicationRequestIdentityWire {
            global_agent: "alice.athena.foo.bar".to_string(),
            local_agent: "foo.bar".to_string(),
            primary_revision: revision.clone(),
            scope: String::new(),
        },
        pages: vec![
            PublicationPageObservationWire {
                path: "agents/alice.athena.foo.bar/README.md".to_string(),
                exists: true,
            },
            PublicationPageObservationWire {
                path: "sessions/alice.athena.foo.bar.md".to_string(),
                exists: true,
            },
        ],
        runs: vec![PublicationRunObservationWire {
            global_name: "alice.athena.foo.bar--code".to_string(),
            local_name: "foo.bar--code".to_string(),
            commit_shas: vec![revision],
            has_prompt_file: true,
        }],
        containers: vec![PublicationContainerObservationWire {
            kind: "family".to_string(),
            global_name: "alice.athena.foo.bar".to_string(),
            member_global_names: vec!["alice.athena.foo.bar--code".to_string()],
            commit_shas: vec![],
        }],
    };
    let decision = decide_publication_request_completion(&request).unwrap();
    assert_eq!(decision.kind, PUBLICATION_KIND_SESSION);
    assert_eq!(decision.required_page, "sessions/alice.athena.foo.bar.md");
    assert!(decision.fulfilled);
}

#[test]
fn completion_rejects_preexisting_run_page_without_revision() {
    let request = PublicationCompletionRequestWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        request: PublicationRequestIdentityWire {
            global_agent: "alice.athena.foo".to_string(),
            local_agent: "foo".to_string(),
            primary_revision: sha('a'),
            scope: String::new(),
        },
        pages: vec![PublicationPageObservationWire {
            path: "agents/alice.athena.foo/README.md".to_string(),
            exists: true,
        }],
        runs: vec![PublicationRunObservationWire {
            global_name: "alice.athena.foo".to_string(),
            local_name: "foo".to_string(),
            commit_shas: vec![sha('b')],
            has_prompt_file: false,
        }],
        containers: vec![],
    };
    let decision = decide_publication_request_completion(&request).unwrap();
    assert_eq!(decision.kind, PUBLICATION_KIND_RUN);
    assert!(!decision.fulfilled);
    assert!(decision.reason.contains("primary revision"));
}

#[test]
fn historical_member_spelling_requires_the_run_page() {
    let revision = sha('c');
    let request = PublicationCompletionRequestWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        request: PublicationRequestIdentityWire {
            global_agent: "alice.athena.foo--code".to_string(),
            local_agent: "foo--code".to_string(),
            primary_revision: revision.clone(),
            scope: String::new(),
        },
        pages: vec![
            PublicationPageObservationWire {
                path: "agents/alice.athena.foo--code/README.md".to_string(),
                exists: true,
            },
            PublicationPageObservationWire {
                path: "sessions/alice.athena.foo.md".to_string(),
                exists: true,
            },
        ],
        runs: vec![PublicationRunObservationWire {
            global_name: "alice.athena.foo--code".to_string(),
            local_name: "foo--code".to_string(),
            commit_shas: vec![revision],
            has_prompt_file: true,
        }],
        containers: vec![PublicationContainerObservationWire {
            kind: "session".to_string(),
            global_name: "alice.athena.foo".to_string(),
            member_global_names: vec!["alice.athena.foo--code".to_string()],
            commit_shas: vec![],
        }],
    };
    let decision = decide_publication_request_completion(&request).unwrap();
    assert_eq!(decision.kind, PUBLICATION_KIND_RUN);
    assert_eq!(
        decision.required_page,
        "agents/alice.athena.foo--code/README.md"
    );
    assert!(decision.fulfilled);
}

#[test]
fn omitted_scope_deserializes_as_revision_bound() {
    let identity: PublicationRequestIdentityWire =
        serde_json::from_value(serde_json::json!({
            "global_agent": "alice.athena.foo",
            "local_agent": "foo",
            "primary_revision": sha('a'),
        }))
        .unwrap();
    assert!(identity.scope.is_empty());
}

#[test]
fn linked_scope_fulfills_when_the_required_page_exists_without_revision() {
    let request = PublicationCompletionRequestWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        request: PublicationRequestIdentityWire {
            global_agent: "alice.athena.foo".to_string(),
            local_agent: "foo".to_string(),
            primary_revision: sha('a'),
            scope: PUBLICATION_SCOPE_LINKED.to_string(),
        },
        pages: vec![PublicationPageObservationWire {
            path: "agents/alice.athena.foo/README.md".to_string(),
            exists: true,
        }],
        runs: vec![PublicationRunObservationWire {
            global_name: "alice.athena.foo".to_string(),
            local_name: "foo".to_string(),
            commit_shas: vec![sha('b')],
            has_prompt_file: false,
        }],
        containers: vec![],
    };
    let decision = decide_publication_request_completion(&request).unwrap();
    assert_eq!(decision.kind, PUBLICATION_KIND_RUN);
    assert!(decision.fulfilled);
    assert!(decision.reason.contains("linked-scope"));
}

#[test]
fn linked_scope_still_requires_the_publication_page() {
    let request = PublicationCompletionRequestWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        request: PublicationRequestIdentityWire {
            global_agent: "alice.athena.foo".to_string(),
            local_agent: "foo".to_string(),
            primary_revision: sha('a'),
            scope: PUBLICATION_SCOPE_LINKED.to_string(),
        },
        pages: vec![PublicationPageObservationWire {
            path: "agents/alice.athena.foo/README.md".to_string(),
            exists: false,
        }],
        runs: vec![],
        containers: vec![],
    };
    let decision = decide_publication_request_completion(&request).unwrap();
    assert!(!decision.fulfilled);
    assert!(decision.reason.contains("absent"));
}

#[test]
fn missing_local_prompt_source_does_not_clear_an_unknown_obligation() {
    let decision = classify_deferred_prompt_obligation(
        &DeferredPromptClassifyRequestWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            restore_wrote: false,
            restore_error: None,
            local_source_present: false,
            archive_present: false,
            prompt_file_in_snapshot: None,
        },
    )
    .unwrap();
    assert_eq!(decision.status, PROMPT_STATUS_UNAVAILABLE);
    assert!(decision.blocks_acknowledgment);
}

#[test]
fn existing_archive_or_snapshot_without_prompt_file_are_not_losses() {
    let archived = classify_deferred_prompt_obligation(
        &DeferredPromptClassifyRequestWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            restore_wrote: false,
            restore_error: None,
            local_source_present: false,
            archive_present: true,
            prompt_file_in_snapshot: Some(true),
        },
    )
    .unwrap();
    assert_eq!(archived.status, PROMPT_STATUS_ALREADY_ARCHIVED);
    assert!(!archived.blocks_acknowledgment);

    let not_applicable = classify_deferred_prompt_obligation(
        &DeferredPromptClassifyRequestWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            restore_wrote: false,
            restore_error: None,
            local_source_present: false,
            archive_present: false,
            prompt_file_in_snapshot: Some(false),
        },
    )
    .unwrap();
    assert_eq!(not_applicable.status, PROMPT_STATUS_NOT_APPLICABLE);
    assert!(!not_applicable.blocks_acknowledgment);
}

fn stale_request(
    requested: &str,
    scope: &str,
    run_commits: &[String],
    ancestors: &[String],
    linked: bool,
) -> StalePublicationRepairRequestWire {
    StalePublicationRepairRequestWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        requested_revision: requested.to_string(),
        scope: scope.to_string(),
        run_commit_shas: run_commits.to_vec(),
        ancestor_commit_shas: ancestors.to_vec(),
        linked_revision: linked,
    }
}

#[test]
fn stale_repair_keeps_rows_whose_revision_is_already_recorded() {
    let decision = classify_stale_publication_request(&stale_request(
        &sha('a'),
        "revision",
        &[sha('b'), sha('a')],
        &[],
        false,
    ))
    .unwrap();
    assert_eq!(decision.action, STALE_REPAIR_ACTION_UNCHANGED);
    assert_eq!(decision.fulfilling_revision, None);
}

#[test]
fn stale_repair_keeps_linked_scope_rows_without_evidence() {
    let decision = classify_stale_publication_request(&stale_request(
        &sha('c'),
        "linked",
        &[],
        &[],
        false,
    ))
    .unwrap();
    assert_eq!(decision.action, STALE_REPAIR_ACTION_UNCHANGED);
}

#[test]
fn stale_repair_routes_linked_repo_revisions_to_linked_scope() {
    let decision = classify_stale_publication_request(&stale_request(
        &sha('c'),
        "revision",
        &[sha('a')],
        &[],
        true,
    ))
    .unwrap();
    assert_eq!(decision.action, STALE_REPAIR_ACTION_LINKED);
    assert_eq!(decision.fulfilling_revision, None);
}

#[test]
fn stale_repair_selects_the_earliest_descendant_run_commit() {
    let commits = vec![sha('x'), sha('b'), sha('y')];
    let ancestors = vec![sha('y'), sha('b')];
    let decision = classify_stale_publication_request(&stale_request(
        &sha('a'),
        "",
        &commits,
        &ancestors,
        false,
    ))
    .unwrap();
    assert_eq!(decision.action, STALE_REPAIR_ACTION_REPAIRED);
    assert_eq!(decision.fulfilling_revision, Some(sha('b')));
    assert_eq!(decision.evidence_commit, Some(sha('b')));
}

#[test]
fn stale_repair_leaves_rows_without_evidence_untouched() {
    let without_runs = classify_stale_publication_request(&stale_request(
        &sha('a'),
        "revision",
        &[],
        &[],
        false,
    ))
    .unwrap();
    assert_eq!(without_runs.action, STALE_REPAIR_ACTION_UNRESOLVED);
    assert_eq!(without_runs.fulfilling_revision, None);

    let without_ancestry = classify_stale_publication_request(&stale_request(
        &sha('a'),
        "revision",
        &[sha('b')],
        &[],
        false,
    ))
    .unwrap();
    assert_eq!(without_ancestry.action, STALE_REPAIR_ACTION_UNRESOLVED);
}

#[test]
fn stale_repair_rejects_unknown_scopes() {
    let result = classify_stale_publication_request(&stale_request(
        &sha('a'),
        "sidecar",
        &[sha('a')],
        &[],
        false,
    ));
    assert!(result.is_err());
}
