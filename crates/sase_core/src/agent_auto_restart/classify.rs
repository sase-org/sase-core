//! `classify_agent_failure`: catalog + witnesses + phase gate.
//!
//! Structured facts win when present; otherwise the regex fallback over
//! `error_text` / `traceback_text` / `log_tail` applies. A missing W4 on
//! a Tier 1-2 match yields `defer`, not `relaunch`, so the Python side
//! runs the fresh-interpreter probe only when it matters.

use super::catalog::{
    candidate_files, combined_text, looks_like_import_or_attribute_error,
    match_family, never_restart_reason, origin_is_workspace, TIER_DATA_FORMAT,
    TIER_REAL_BUG, TIER_RUST_BINDING, TIER_TORN_PYTHON,
};
use super::episode::derive_auto_restart_episode;
use super::wire::{
    AgentFailureFactsWire, AutoRestartContextWire, AutoRestartWitnessesWire,
    RecoveryVerdictWire, AGENT_AUTO_RESTART_WIRE_SCHEMA_VERSION,
};

/// Phase classes.
pub const PHASE_PRE_PROVIDER: &str = "pre_provider";
pub const PHASE_POST_PROVIDER: &str = "post_provider";
pub const PHASE_PLAN_HANDOFF: &str = "plan_handoff";
pub const PHASE_UNKNOWN: &str = "unknown";

/// Verdict modes.
pub const MODE_RELAUNCH: &str = "relaunch";
pub const MODE_DEFER: &str = "defer";
pub const MODE_NOTIFY_POST_PROVIDER: &str = "notify_post_provider";
pub const MODE_ASK: &str = "ask";
pub const MODE_DECLINE: &str = "decline";

/// Lifecycle phases that died before any model work.
const PRE_PROVIDER_PHASES: &[&str] = &["booting", "waiting", "preparing"];
/// Lifecycle phases that died after model work started.
const POST_PROVIDER_PHASES: &[&str] =
    &["provider_running", "provider_done", "finalizing"];

/// Frames that prove the provider loop or a finalizer ran (legacy rows).
const PROVIDER_FRAMES: &[&str] =
    &["run_execution_loop", "invoke_agent", "finaliz"];

/// Classify one failed agent into a recovery verdict.
pub fn classify_agent_failure(
    facts: Option<&AgentFailureFactsWire>,
    context: &AutoRestartContextWire,
    witnesses: &AutoRestartWitnessesWire,
) -> RecoveryVerdictWire {
    let witnesses_fired = fired_witnesses(witnesses);
    let episode_id = derive_auto_restart_episode(witnesses).id;
    let episode_id = if episode_id.is_empty() {
        None
    } else {
        Some(episode_id)
    };

    if let Some(reason) = never_restart_reason(facts, context) {
        return decline(
            "",
            "",
            "",
            None,
            None,
            PHASE_UNKNOWN.to_string(),
            reason,
            &never_restart_text(reason),
            witnesses_fired,
            None,
        );
    }

    let matched = match_family(facts, context);
    let matched = match matched {
        Some(matched) => matched,
        None => {
            let files = candidate_files(facts, context);
            if looks_like_import_or_attribute_error(facts, context)
                && origin_is_workspace(&files, context)
            {
                return decline(
                    "",
                    "",
                    "workspace origin",
                    None,
                    None,
                    classify_phase(facts, context),
                    "workspace_origin",
                    "ImportError originated in the agent workspace, not managed sase code",
                    witnesses_fired,
                    episode_id,
                );
            }
            return decline(
                "",
                "",
                "no skew signature",
                None,
                None,
                classify_phase(facts, context),
                "no_update_signature",
                "no sase update signature; looks like a real bug",
                witnesses_fired,
                episode_id,
            );
        }
    };

    // Tier 4 is annotate-only: never restart.
    if matched.tier == TIER_REAL_BUG {
        return decline(
            matched.tier,
            matched.family,
            &matched.signature,
            matched.origin_module.clone(),
            matched.missing_symbol.clone(),
            classify_phase(facts, context),
            "tier4_real_bug",
            "signature-mismatch or name error in managed code; annotate only",
            witnesses_fired,
            episode_id,
        );
    }

    let phase_class = classify_phase(facts, context);
    match phase_class.as_str() {
        PHASE_POST_PROVIDER => {
            return RecoveryVerdictWire {
                schema_version: AGENT_AUTO_RESTART_WIRE_SCHEMA_VERSION,
                tier: matched.tier.to_string(),
                family: matched.family.to_string(),
                signature: matched.signature.clone(),
                origin_module: matched.origin_module.clone(),
                missing_symbol: matched.missing_symbol.clone(),
                phase_class,
                mode: MODE_NOTIFY_POST_PROVIDER.to_string(),
                reason: "died_after_model_turn".to_string(),
                reason_text:
                    "died after its model turn; workspace held with its changes"
                        .to_string(),
                witnesses_fired,
                episode_id,
            };
        }
        PHASE_PLAN_HANDOFF => {
            return RecoveryVerdictWire {
                schema_version: AGENT_AUTO_RESTART_WIRE_SCHEMA_VERSION,
                tier: matched.tier.to_string(),
                family: matched.family.to_string(),
                signature: matched.signature.clone(),
                origin_module: matched.origin_module.clone(),
                missing_symbol: matched.missing_symbol.clone(),
                phase_class,
                mode: MODE_ASK.to_string(),
                reason: "plan_or_gate_handoff".to_string(),
                reason_text: "died during a plan, question, or gate handoff; ask the user".to_string(),
                witnesses_fired,
                episode_id,
            };
        }
        PHASE_UNKNOWN => {
            return RecoveryVerdictWire {
                schema_version: AGENT_AUTO_RESTART_WIRE_SCHEMA_VERSION,
                tier: matched.tier.to_string(),
                family: matched.family.to_string(),
                signature: matched.signature.clone(),
                origin_module: matched.origin_module.clone(),
                missing_symbol: matched.missing_symbol.clone(),
                phase_class,
                mode: MODE_ASK.to_string(),
                reason: "phase_unknown".to_string(),
                reason_text: "couldn't tell whether it reached its model turn"
                    .to_string(),
                witnesses_fired,
                episode_id,
            };
        }
        _ => {}
    }

    // Witness gate: a matching pattern alone is never enough.
    let has_w1 = witnesses_fired.iter().any(|w| w == "W1");
    let has_w2 = witnesses_fired.iter().any(|w| w == "W2");
    let probe_ok = witnesses.probe.as_ref().is_some_and(|p| p.ok);
    let probe_present = witnesses.probe.is_some();
    let witness_ok = match matched.tier {
        TIER_TORN_PYTHON => {
            // signature ∧ (W1 ∨ W2) ∧ W4
            (has_w1 || has_w2) && probe_ok
        }
        TIER_RUST_BINDING => {
            // signature ∧ W4, after quiescence (quiescence is Python-owned)
            probe_ok
        }
        TIER_DATA_FORMAT => {
            // signature ∧ (W1 ∨ W2)
            has_w1 || has_w2
        }
        _ => false,
    };
    if !witness_ok {
        // A missing W4 on a Tier 1-2 match with an update witness
        // (W1/W2) yields defer so the Python side runs the probe only
        // when it matters. With no update witness at all, decline.
        let tier12 = matched.tier == TIER_TORN_PYTHON
            || matched.tier == TIER_RUST_BINDING;
        if tier12 && !probe_present && (has_w1 || has_w2) {
            return RecoveryVerdictWire {
                schema_version: AGENT_AUTO_RESTART_WIRE_SCHEMA_VERSION,
                tier: matched.tier.to_string(),
                family: matched.family.to_string(),
                signature: matched.signature.clone(),
                origin_module: matched.origin_module.clone(),
                missing_symbol: matched.missing_symbol.clone(),
                phase_class,
                mode: MODE_DEFER.to_string(),
                reason: "probe_pending".to_string(),
                reason_text: "skew signature with an update witness; waiting for the fresh-interpreter probe".to_string(),
                witnesses_fired,
                episode_id,
            };
        }
        if tier12 && probe_present && !probe_ok {
            // Tier 1 still needs an update witness. A failed probe
            // without W1/W2 is a real bug, not a deferral.
            let can_defer_failed_probe =
                matched.tier == TIER_RUST_BINDING || has_w1 || has_w2;
            if can_defer_failed_probe {
                return RecoveryVerdictWire {
                    schema_version: AGENT_AUTO_RESTART_WIRE_SCHEMA_VERSION,
                    tier: matched.tier.to_string(),
                    family: matched.family.to_string(),
                    signature: matched.signature.clone(),
                    origin_module: matched.origin_module.clone(),
                    missing_symbol: matched.missing_symbol.clone(),
                    phase_class,
                    mode: MODE_DEFER.to_string(),
                    reason: "probe_failed".to_string(),
                    reason_text:
                        "fresh-interpreter probe failed; deferred, not relaunched"
                            .to_string(),
                    witnesses_fired,
                    episode_id,
                };
            }
        }
        return decline(
            matched.tier,
            matched.family,
            &matched.signature,
            matched.origin_module.clone(),
            matched.missing_symbol.clone(),
            phase_class,
            "no_update_witness",
            "no sase update during this run (looks like a real bug)",
            witnesses_fired,
            episode_id,
        );
    }

    RecoveryVerdictWire {
        schema_version: AGENT_AUTO_RESTART_WIRE_SCHEMA_VERSION,
        tier: matched.tier.to_string(),
        family: matched.family.to_string(),
        signature: matched.signature.clone(),
        origin_module: matched.origin_module.clone(),
        missing_symbol: matched.missing_symbol.clone(),
        phase_class,
        mode: MODE_RELAUNCH.to_string(),
        reason: "update_skew_pre_provider".to_string(),
        reason_text:
            "broke before its model turn during a sase update; nothing lost"
                .to_string(),
        witnesses_fired,
        episode_id,
    }
}

#[allow(clippy::too_many_arguments)]
fn decline(
    tier: &str,
    family: &str,
    signature: &str,
    origin_module: Option<String>,
    missing_symbol: Option<String>,
    phase_class: String,
    reason: &str,
    reason_text: &str,
    witnesses_fired: Vec<String>,
    episode_id: Option<String>,
) -> RecoveryVerdictWire {
    RecoveryVerdictWire {
        schema_version: AGENT_AUTO_RESTART_WIRE_SCHEMA_VERSION,
        tier: tier.to_string(),
        family: family.to_string(),
        signature: signature.to_string(),
        origin_module,
        missing_symbol,
        phase_class,
        mode: MODE_DECLINE.to_string(),
        reason: reason.to_string(),
        reason_text: reason_text.to_string(),
        witnesses_fired,
        episode_id,
    }
}

fn never_restart_text(reason: &str) -> String {
    match reason {
        "remote_row" => "remote rows are never restarted".to_string(),
        "killed_or_stopped" => {
            "killed, stopped, or cancelled; user intent wins".to_string()
        }
        "provider_error" => {
            "provider, usage-limit, auth, or context error; the provider drain owns these".to_string()
        }
        "resource_exhausted" => {
            "out of memory, timeout, disk, or permission failure".to_string()
        }
        "directive_or_macro_error" => {
            "directive, macro, or alias error".to_string()
        }
        "finalizer_or_publish_failure" => {
            "finalizer, publish, gate-dispatch, or materialization failure"
                .to_string()
        }
        _ => "not a restart candidate".to_string(),
    }
}

/// Classify the death phase from breadcrumbs first, legacy heuristics
/// second.
pub fn classify_phase(
    facts: Option<&AgentFailureFactsWire>,
    context: &AutoRestartContextWire,
) -> String {
    if context.has_pending_question || context.has_pending_handoff {
        return PHASE_PLAN_HANDOFF.to_string();
    }
    let phase = facts
        .and_then(|f| f.lifecycle_phase.clone())
        .or_else(|| context.lifecycle_phase.clone())
        .unwrap_or_default()
        .to_ascii_lowercase();
    if PRE_PROVIDER_PHASES.contains(&phase.as_str()) {
        return PHASE_PRE_PROVIDER.to_string();
    }
    if POST_PROVIDER_PHASES.contains(&phase.as_str()) {
        return PHASE_POST_PROVIDER.to_string();
    }
    if phase == "handoff" {
        return PHASE_PLAN_HANDOFF.to_string();
    }
    legacy_phase_class(facts, context)
}

/// Legacy rows have no breadcrumbs: treat the row as pre-provider only
/// when its frames include no provider/finalizer frames, or when the log
/// shows the refresh line and no provider start. Otherwise stay
/// `unknown` — that class never relaunches.
fn legacy_phase_class(
    facts: Option<&AgentFailureFactsWire>,
    context: &AutoRestartContextWire,
) -> String {
    if let Some(facts) = facts {
        if !facts.frames.is_empty() {
            for frame in &facts.frames {
                let function = frame.function.to_ascii_lowercase();
                if PROVIDER_FRAMES.iter().any(|m| function.contains(m)) {
                    return PHASE_POST_PROVIDER.to_string();
                }
            }
            return PHASE_PRE_PROVIDER.to_string();
        }
    }
    let haystack = combined_text(context);
    if haystack.contains("run_execution_loop")
        || haystack.contains("invoke_agent")
    {
        return PHASE_POST_PROVIDER.to_string();
    }
    if haystack.contains("refreshing sase runner code after dependency wait")
        && !haystack.contains("provider start")
        && !haystack.contains("model turn")
    {
        return PHASE_PRE_PROVIDER.to_string();
    }
    PHASE_UNKNOWN.to_string()
}

/// Which witnesses fired: W1 (boot identity drift), W2 (journal row),
/// W3 (file-level proof with culprit), W4 (probe passed).
pub fn fired_witnesses(witnesses: &AutoRestartWitnessesWire) -> Vec<String> {
    let mut fired = Vec::new();
    match (&witnesses.boot_identity, &witnesses.current_identity) {
        (Some(boot), Some(current)) if boot != current => {
            fired.push("W1".to_string())
        }
        _ => {
            if witnesses.journal_updates.is_empty()
                && witnesses.file_proof.is_none()
            {
                // No identity evidence at all: nothing fires here.
            }
        }
    }
    if !witnesses.journal_updates.is_empty() {
        fired.push("W2".to_string());
    }
    if let Some(proof) = witnesses.file_proof.as_ref() {
        let drift = match (proof.boot_has, proof.head_has) {
            (Some(boot), Some(head)) => boot != head,
            _ => proof.culprit_commit.is_some(),
        };
        if drift {
            fired.push("W3".to_string());
        }
    }
    if witnesses.probe.as_ref().is_some_and(|p| p.ok) {
        fired.push("W4".to_string());
    }
    fired
}
