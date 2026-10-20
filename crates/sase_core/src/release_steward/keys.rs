//! Stable idempotency keys and exact argv arrays for proposed actions.

use super::wire::{EscalationOpWire, ProposedActionWire, TrainActionKindWire};

pub fn train_action_key(action: &TrainActionKindWire) -> String {
    match action {
        TrainActionKindWire::MergeReleasePr {
            pr_number,
            head_sha,
        } => format!("merge:pr={pr_number}:head={head_sha}"),
        TrainActionKindWire::DispatchRelease {
            tag,
            sha,
            mode,
            generation,
        } => format!("release:tag={tag}:sha={sha}:gen={generation}:mode={mode}"),
        TrainActionKindWire::DispatchPublishMetadata {
            pr_number,
            head_sha,
            pin,
            ..
        } => {
            let pr = pr_number
                .map(|n| n.to_string())
                .unwrap_or_else(|| "none".to_string());
            let head = head_sha.as_deref().unwrap_or("none");
            let pin = pin.as_deref().unwrap_or("none");
            format!("metadata:pr={pr}:head={head}:pin={pin}")
        }
        TrainActionKindWire::RerunFailedJobs { run_id, attempt, .. } => {
            format!("rerun:run={run_id}:attempt={attempt}")
        }
        TrainActionKindWire::DispatchCoreCut { source_pin } => {
            format!("core-cut:pin={source_pin}")
        }
        TrainActionKindWire::AbandonCandidate {
            candidate_id,
            generation,
            observation_asset,
        } => format!(
            "abandon:candidate={candidate_id}:gen={generation}:asset={observation_asset}"
        ),
    }
}

pub fn train_action_argv(action: &TrainActionKindWire) -> Vec<String> {
    match action {
        TrainActionKindWire::MergeReleasePr {
            pr_number,
            head_sha,
        } => vec![
            "gh".into(),
            "pr".into(),
            "merge".into(),
            pr_number.to_string(),
            "--merge".into(),
            "--match-head-commit".into(),
            head_sha.clone(),
        ],
        TrainActionKindWire::DispatchRelease { tag, mode, .. } => vec![
            "gh".into(),
            "workflow".into(),
            "run".into(),
            "release.yml".into(),
            "--ref".into(),
            tag.clone(),
            "-f".into(),
            format!("mode={mode}"),
        ],
        TrainActionKindWire::DispatchPublishMetadata { .. } => vec![
            "gh".into(),
            "workflow".into(),
            "run".into(),
            "publish.yml".into(),
        ],
        TrainActionKindWire::RerunFailedJobs { run_id, .. } => vec![
            "gh".into(),
            "run".into(),
            "rerun".into(),
            run_id.clone(),
            "--failed".into(),
        ],
        TrainActionKindWire::DispatchCoreCut { .. } => vec![
            "gh".into(),
            "workflow".into(),
            "run".into(),
            "release-plz.yml".into(),
            "-f".into(),
            "dry_run=false".into(),
        ],
        TrainActionKindWire::AbandonCandidate { .. } => Vec::new(),
    }
}

pub fn proposed_action(
    action: TrainActionKindWire,
    disposition: super::wire::ActionDispositionWire,
    preconditions: Vec<String>,
    reason: impl Into<String>,
) -> ProposedActionWire {
    let key = train_action_key(&action);
    let argv = train_action_argv(&action);
    ProposedActionWire {
        action,
        disposition,
        key,
        preconditions,
        argv,
        reason: reason.into(),
    }
}

pub fn escalation_key(op: &EscalationOpWire) -> String {
    match op {
        EscalationOpWire::Open {
            family,
            identity_marker,
            ..
        } => format!("esc-open:{family}:{identity_marker}"),
        EscalationOpWire::Comment {
            family,
            issue_number,
            tick_identity,
            ..
        } => format!("esc-comment:{family}:{issue_number}:{tick_identity}"),
        EscalationOpWire::Close {
            family,
            issue_number,
            ..
        } => format!("esc-close:{family}:{issue_number}"),
    }
}

pub fn observation_asset(candidate_id: &str, generation: u64) -> String {
    format!("{candidate_id}.gen{generation}.abandoned.json")
}
