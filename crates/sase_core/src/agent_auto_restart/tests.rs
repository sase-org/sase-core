//! Golden tests for the auto-restart classifier, ledger, and episodes.

use super::classify::{
    classify_agent_failure, MODE_ASK, MODE_DECLINE, MODE_DEFER,
    MODE_NOTIFY_POST_PROVIDER, MODE_RELAUNCH, PHASE_PLAN_HANDOFF,
    PHASE_POST_PROVIDER, PHASE_PRE_PROVIDER,
};
use super::episode::derive_auto_restart_episode;
use super::ledger::{
    advance_auto_restart_ledger, auto_restart_lineage_root,
    auto_restart_recovery_is_in_flight, claim_auto_restart_ledger,
    LEDGER_CLAIMED, LEDGER_DECLINED, LEDGER_DEFERRED,
    LEDGER_EVENT_BEGIN_LAUNCH, LEDGER_EVENT_DECLINE, LEDGER_EVENT_DEFER,
    LEDGER_EVENT_LAUNCHED, LEDGER_EVENT_RECLAIM, LEDGER_EVENT_SETTLED_FAILED,
    LEDGER_EVENT_SETTLED_OK, LEDGER_LAUNCHED, LEDGER_LAUNCHING,
    LEDGER_SETTLED_FAILED, LEDGER_SETTLED_OK,
};
use super::wire::{
    AutoRestartContextWire, AutoRestartFileProofWire, AutoRestartProbeWire,
    AutoRestartRefreshLogLineWire, AutoRestartWitnessesWire,
};

const INCIDENT_ERROR: &str = "ImportError: cannot import name 'auto_launch_prefix' from 'sase.monitor.continuation_delivery'";

fn incident_traceback() -> String {
    format!(
        "Traceback (most recent call last):\n  File \"/opt/sase/src/sase/axe/run_agent_runner_refresh.py\", line 42, in refresh_runner_code_after_wait\n    cont_mod = importlib.import_module(mod)\n  File \"/opt/sase/src/sase/axe/run_agent_runner.py\", line 18, in run\n    refresh_runner_code_after_wait()\n{INCIDENT_ERROR}"
    )
}

fn managed_context() -> AutoRestartContextWire {
    AutoRestartContextWire {
        schema_version: 1,
        managed_roots: vec![super::wire::AutoRestartManagedRootWire {
            name: "sase".to_string(),
            root: "/opt/sase".to_string(),
        }],
        workspace_dir: Some("/home/user/work/project".to_string()),
        outcome: Some("failed".to_string()),
        kill_source: None,
        lifecycle_phase: Some("waiting".to_string()),
        has_pending_question: false,
        has_pending_handoff: false,
        is_remote: false,
        error_text: INCIDENT_ERROR.to_string(),
        traceback_text: incident_traceback(),
        log_tail: "Refreshing sase runner code after dependency wait"
            .to_string(),
    }
}

fn strong_witnesses() -> AutoRestartWitnessesWire {
    AutoRestartWitnessesWire {
        schema_version: 1,
        boot_identity: Some("sase@9c5000f".to_string()),
        current_identity: Some("sase@9fd8a08".to_string()),
        journal_updates: vec!["9fd8a08".to_string()],
        file_proof: Some(AutoRestartFileProofWire {
            symbol: Some("auto_launch_prefix".to_string()),
            module: Some("sase.monitor.continuation_delivery".to_string()),
            boot_has: Some(true),
            head_has: Some(false),
            culprit_commit: Some("9fd8a081f4".to_string()),
            culprit_subject: Some(
                "feat(autonomy): structural inheritance".to_string(),
            ),
        }),
        probe: Some(AutoRestartProbeWire {
            ok: true,
            failures: Vec::new(),
        }),
        refresh_log_line: Some(AutoRestartRefreshLogLineWire {
            from: "9c5000f".to_string(),
            to: "9fd8a08".to_string(),
        }),
    }
}

#[test]
fn incident_relaunch_when_w1_and_w4_hold() {
    let verdict =
        classify_agent_failure(None, &managed_context(), &strong_witnesses());
    assert_eq!(verdict.mode, MODE_RELAUNCH);
    assert_eq!(verdict.phase_class, PHASE_PRE_PROVIDER);
    assert_eq!(verdict.tier, "tier1_torn_python");
    assert_eq!(
        verdict.missing_symbol.as_deref(),
        Some("auto_launch_prefix")
    );
    assert!(verdict.witnesses_fired.contains(&"W1".to_string()));
    assert!(verdict.witnesses_fired.contains(&"W4".to_string()));
    assert_eq!(verdict.episode_id.as_deref(), Some("sase@9fd8a08"));
}

#[test]
fn incident_defers_without_w4() {
    let mut witnesses = strong_witnesses();
    witnesses.probe = None;
    let verdict = classify_agent_failure(None, &managed_context(), &witnesses);
    assert_eq!(verdict.mode, MODE_DEFER);
    assert_eq!(verdict.reason, "probe_pending");
}

#[test]
fn same_import_error_with_no_witness_declines() {
    let witnesses = AutoRestartWitnessesWire {
        schema_version: 1,
        ..Default::default()
    };
    let verdict = classify_agent_failure(None, &managed_context(), &witnesses);
    assert_eq!(verdict.mode, MODE_DECLINE);
    assert_eq!(verdict.reason, "no_update_witness");
}

#[test]
fn workspace_origin_import_error_never_matches() {
    let mut context = managed_context();
    context.workspace_dir = Some("/opt/sase".to_string());
    context.error_text =
        "ImportError: cannot import name 'helper' from 'sase.monitor.continuation_delivery'"
            .to_string();
    context.traceback_text = format!(
        "Traceback (most recent call last):\n  File \"/opt/sase/src/sase/monitor/continuation_delivery.py\", line 3, in <module>\n{}",
        context.error_text
    );
    let witnesses = AutoRestartWitnessesWire {
        schema_version: 1,
        ..Default::default()
    };
    let verdict = classify_agent_failure(None, &context, &witnesses);
    assert_eq!(verdict.mode, MODE_DECLINE);
}

#[test]
fn traceback_quoted_in_agent_output_does_not_match() {
    let mut context = managed_context();
    // The signature lives only in the log tail (quoted output), not in
    // the failure's own error/traceback text.
    context.error_text = "agent reported an unexpected result".to_string();
    context.traceback_text = String::new();
    context.log_tail = format!("agent said: {INCIDENT_ERROR}");
    let verdict = classify_agent_failure(None, &context, &strong_witnesses());
    assert_eq!(verdict.mode, MODE_DECLINE);
}

#[test]
fn provider_429_and_usage_limit_never_restart() {
    for error in [
        "ProviderError: 429 rate limit exceeded",
        "UsageLimit: monthly usage limit reached",
        "Authentication failed: invalid api key",
        "context length exceeded max_tokens",
    ] {
        let mut context = managed_context();
        context.error_text = error.to_string();
        context.traceback_text = error.to_string();
        context.log_tail = String::new();
        let verdict =
            classify_agent_failure(None, &context, &strong_witnesses());
        assert_eq!(verdict.mode, MODE_DECLINE, "for {error}");
        assert_eq!(verdict.reason, "provider_error", "for {error}");
    }
}

#[test]
fn killed_rows_never_restart() {
    let mut context = managed_context();
    context.outcome = Some("killed".to_string());
    let verdict = classify_agent_failure(None, &context, &strong_witnesses());
    assert_eq!(verdict.mode, MODE_DECLINE);
    assert_eq!(verdict.reason, "killed_or_stopped");
}

#[test]
fn directive_errors_never_restart() {
    let mut context = managed_context();
    context.error_text = "unknown directive %frobnicate".to_string();
    context.traceback_text = context.error_text.clone();
    context.log_tail = String::new();
    let verdict = classify_agent_failure(None, &context, &strong_witnesses());
    assert_eq!(verdict.mode, MODE_DECLINE);
    assert_eq!(verdict.reason, "directive_or_macro_error");
}

#[test]
fn finalizer_failures_never_restart() {
    let mut context = managed_context();
    context.error_text = "commit-finalizer failed to land".to_string();
    context.traceback_text = context.error_text.clone();
    context.log_tail = String::new();
    let verdict = classify_agent_failure(None, &context, &strong_witnesses());
    assert_eq!(verdict.mode, MODE_DECLINE);
    assert_eq!(verdict.reason, "finalizer_or_publish_failure");
}

#[test]
fn tier4_type_error_is_annotate_only() {
    let mut context = managed_context();
    context.error_text =
        "TypeError: run() got an unexpected signature mismatch in sase.agent.scope"
            .to_string();
    context.traceback_text = context.error_text.clone();
    context.log_tail = String::new();
    // Structured facts path: a TypeError chain link with "signature".
    let facts = super::wire::AgentFailureFactsWire {
        schema_version: 1,
        lifecycle_phase: Some("waiting".to_string()),
        exception_chain: vec![super::wire::AgentFailureChainLinkWire {
            r#type: "TypeError".to_string(),
            qualname: "TypeError".to_string(),
            module: "builtins".to_string(),
            message: context.error_text.clone(),
        }],
        ..Default::default()
    };
    let verdict =
        classify_agent_failure(Some(&facts), &context, &strong_witnesses());
    assert_eq!(verdict.mode, MODE_DECLINE);
    assert_eq!(verdict.reason, "tier4_real_bug");
}

#[test]
fn post_provider_import_error_notifies() {
    let mut context = managed_context();
    context.lifecycle_phase = Some("provider_done".to_string());
    let verdict = classify_agent_failure(None, &context, &strong_witnesses());
    assert_eq!(verdict.mode, MODE_NOTIFY_POST_PROVIDER);
    assert_eq!(verdict.phase_class, PHASE_POST_PROVIDER);
}

#[test]
fn plan_handoff_import_error_asks() {
    let mut context = managed_context();
    context.has_pending_question = true;
    let verdict = classify_agent_failure(None, &context, &strong_witnesses());
    assert_eq!(verdict.mode, MODE_ASK);
    assert_eq!(verdict.phase_class, PHASE_PLAN_HANDOFF);
}

#[test]
fn legacy_provider_frames_mean_post_provider() {
    let mut context = managed_context();
    context.lifecycle_phase = None;
    context.log_tail = "run_execution_loop started\nprovider start".to_string();
    let verdict = classify_agent_failure(None, &context, &strong_witnesses());
    assert_eq!(verdict.phase_class, PHASE_POST_PROVIDER);
    assert_eq!(verdict.mode, MODE_NOTIFY_POST_PROVIDER);
}

#[test]
fn every_ledger_transition_legal_and_illegal() {
    let record = claim_auto_restart_ledger("k", "root");
    assert_eq!(record.state, LEDGER_CLAIMED);

    let deferred =
        advance_auto_restart_ledger(&record, LEDGER_EVENT_DEFER).unwrap();
    assert_eq!(deferred.state, LEDGER_DEFERRED);
    assert_eq!(deferred.deferrals, 1);

    let reclaimed =
        advance_auto_restart_ledger(&deferred, LEDGER_EVENT_RECLAIM).unwrap();
    assert_eq!(reclaimed.state, LEDGER_CLAIMED);

    let launching =
        advance_auto_restart_ledger(&reclaimed, LEDGER_EVENT_BEGIN_LAUNCH)
            .unwrap();
    assert_eq!(launching.state, LEDGER_LAUNCHING);

    let launched =
        advance_auto_restart_ledger(&launching, LEDGER_EVENT_LAUNCHED).unwrap();
    assert_eq!(launched.state, LEDGER_LAUNCHED);

    let settled =
        advance_auto_restart_ledger(&launched, LEDGER_EVENT_SETTLED_OK)
            .unwrap();
    assert_eq!(settled.state, LEDGER_SETTLED_OK);

    // Illegal: claimed cannot settle, launched cannot reclaim, settled
    // is terminal, unknown events fail.
    assert!(
        advance_auto_restart_ledger(&record, LEDGER_EVENT_SETTLED_OK).is_err()
    );
    assert!(
        advance_auto_restart_ledger(&launched, LEDGER_EVENT_RECLAIM).is_err()
    );
    assert!(
        advance_auto_restart_ledger(&settled, LEDGER_EVENT_DECLINE).is_err()
    );
    assert!(advance_auto_restart_ledger(&record, "bogus").is_err());

    // launching → settled_failed and launched → settled_failed.
    let failed =
        advance_auto_restart_ledger(&launching, LEDGER_EVENT_SETTLED_FAILED)
            .unwrap();
    assert_eq!(failed.state, LEDGER_SETTLED_FAILED);
    assert!(
        advance_auto_restart_ledger(&launched, LEDGER_EVENT_SETTLED_FAILED)
            .unwrap()
            .state
            == LEDGER_SETTLED_FAILED
    );

    // deferred → declined.
    assert_eq!(
        advance_auto_restart_ledger(&deferred, LEDGER_EVENT_DECLINE)
            .unwrap()
            .state,
        LEDGER_DECLINED
    );
}

#[test]
fn lineage_root_prefers_auto_restart_then_chain_then_row() {
    assert_eq!(
        auto_restart_lineage_root(Some("lineage"), Some("chain"), "row"),
        "lineage"
    );
    assert_eq!(
        auto_restart_lineage_root(None, Some("chain"), "row"),
        "chain"
    );
    assert_eq!(auto_restart_lineage_root(None, None, "row"), "row");
}

#[test]
fn episode_uses_culprit_commit_then_range() {
    let episode = derive_auto_restart_episode(&strong_witnesses());
    assert_eq!(episode.id, "sase@9fd8a08");
    assert_eq!(episode.slug, "sase-9fd8a08");
    assert_eq!(episode.from_rev.as_deref(), Some("9c5000f"));
    assert_eq!(episode.to_rev.as_deref(), Some("9fd8a08"));

    let mut witnesses = strong_witnesses();
    witnesses.file_proof = None;
    let episode = derive_auto_restart_episode(&witnesses);
    assert_eq!(episode.id, "sase@9c5000f-9fd8a08");
}

#[test]
fn in_flight_states_map_to_restarting() {
    assert!(auto_restart_recovery_is_in_flight(Some("pending")));
    assert!(auto_restart_recovery_is_in_flight(Some("deferred")));
    assert!(auto_restart_recovery_is_in_flight(Some("launching")));
    assert!(!auto_restart_recovery_is_in_flight(Some("declined")));
    assert!(!auto_restart_recovery_is_in_flight(Some("launched")));
    assert!(!auto_restart_recovery_is_in_flight(None));
}
