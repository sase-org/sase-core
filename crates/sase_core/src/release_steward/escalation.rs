//! Escalation: one issue per state family, immediate / aged / recovery /
//! advisory, keyed by tick identity so re-execution does not double-comment.

use chrono::{DateTime, Duration, Utc};

use super::cadence::parse_observed_at;
use super::facts::{IssueFactsWire, ReleaseStewardSnapshotWire};
use super::keys::escalation_key;
use super::wire::{
    ActionDispositionWire, EscalationOpWire, EscalationSeverityWire,
    PrimaryStateWire, ProposedEscalationWire, ReleaseStewardPolicyWire,
};

fn aged_after() -> Duration {
    Duration::hours(24)
}
const STEWARD_LABEL: &str = "release-steward";
const CI_LABEL: &str = "ci";

pub struct EscalationInput<'a> {
    pub observed_at: DateTime<Utc>,
    pub policy: &'a ReleaseStewardPolicyWire,
    pub snapshot: &'a ReleaseStewardSnapshotWire,
    pub primary: &'a PrimaryStateWire,
    pub secondary: &'a [super::wire::SecondaryFindingWire],
}

fn title_for(family: &str, identity: &str) -> String {
    format!("release-steward: {family} ({identity})")
}

fn labels_for(state: &PrimaryStateWire) -> Vec<String> {
    let mut labels = vec![STEWARD_LABEL.to_string()];
    if state.is_advisory_qualification() {
        labels.push(CI_LABEL.to_string());
    }
    labels
}

fn identity_marker(family: &str, identity: &str) -> String {
    format!("release-steward:{family}:{identity}")
}

fn open_issue<'a>(
    issues: &'a [IssueFactsWire],
    family: &str,
    identity: &str,
) -> Option<&'a IssueFactsWire> {
    issues.iter().find(|issue| {
        issue.family == family
            && issue.identity_marker == identity_marker(family, identity)
            && issue.state == "open"
    })
}

fn already_commented(issue: &IssueFactsWire, tick: &str) -> bool {
    issue
        .comments
        .iter()
        .any(|comment| comment.tick_identity == tick)
}

fn propose(
    op: EscalationOpWire,
    severity: EscalationSeverityWire,
    disposition: ActionDispositionWire,
) -> ProposedEscalationWire {
    let key = escalation_key(&op);
    ProposedEscalationWire {
        operation: op,
        disposition,
        key,
        severity,
    }
}

fn open_or_plus_one(
    issues: &[IssueFactsWire],
    state: &PrimaryStateWire,
    tick: &str,
    severity: EscalationSeverityWire,
    body: &str,
) -> ProposedEscalationWire {
    let family = state.family();
    let identity = state.identity();
    let marker = identity_marker(family, &identity);
    if let Some(issue) = open_issue(issues, family, &identity) {
        if already_commented(issue, tick) {
            return propose(
                EscalationOpWire::Comment {
                    family: family.to_string(),
                    issue_number: issue.number,
                    body: body.to_string(),
                    tick_identity: tick.to_string(),
                },
                severity,
                ActionDispositionWire::AlreadySatisfied,
            );
        }
        return propose(
            EscalationOpWire::Comment {
                family: family.to_string(),
                issue_number: issue.number,
                body: format!("+1 {body}"),
                tick_identity: tick.to_string(),
            },
            severity,
            ActionDispositionWire::Proposed,
        );
    }
    propose(
        EscalationOpWire::Open {
            family: family.to_string(),
            title: title_for(family, &identity),
            labels: labels_for(state),
            body: body.to_string(),
            identity_marker: marker,
        },
        severity,
        ActionDispositionWire::Proposed,
    )
}

fn states_to_consider<'a>(
    primary: &'a PrimaryStateWire,
    secondary: &'a [super::wire::SecondaryFindingWire],
) -> Vec<&'a PrimaryStateWire> {
    let mut out = vec![primary];
    for finding in secondary {
        out.push(&finding.state);
    }
    out
}

fn recovery_close(
    issues: &[IssueFactsWire],
    active: &[&PrimaryStateWire],
) -> Option<ProposedEscalationWire> {
    let active_markers: Vec<String> = active
        .iter()
        .map(|state| identity_marker(state.family(), &state.identity()))
        .collect();
    for issue in issues {
        if issue.state != "open" {
            continue;
        }
        if !issue.labels.iter().any(|label| label == STEWARD_LABEL) {
            continue;
        }
        if active_markers
            .iter()
            .any(|marker| marker == &issue.identity_marker)
        {
            continue;
        }
        if issue.family == "released"
            || issue.family == "releasable"
            || issue.family == "waiting"
            || issue.family == "nothing_to_release"
        {
            continue;
        }
        return Some(propose(
            EscalationOpWire::Close {
                family: issue.family.clone(),
                issue_number: issue.number,
                reason: "state recovered".to_string(),
            },
            EscalationSeverityWire::Recovery,
            ActionDispositionWire::Proposed,
        ));
    }
    None
}

fn aged_enough(
    observed_at: DateTime<Utc>,
    snapshot: &ReleaseStewardSnapshotWire,
    state: &PrimaryStateWire,
) -> (bool, Option<String>, bool) {
    let Some(lineage) =
        snapshot.blocker_lineage.as_ready().and_then(Option::as_ref)
    else {
        return (false, None, true);
    };
    if lineage.family != state.family() || lineage.identity != state.identity()
    {
        return (false, None, true);
    }
    if lineage.identity_changed || !lineage.continuous {
        return (false, None, true);
    }
    let Some(started) = lineage.started_at.as_ref() else {
        return (false, None, true);
    };
    let Ok(started_at) = parse_observed_at(started) else {
        return (false, Some(started.clone()), true);
    };
    let aged = observed_at - started_at >= aged_after();
    (aged, Some(started.clone()), false)
}

/// Pick at most one escalation: immediate, then aged, then recovery,
/// then advisory. `WATCHER_BROKEN` never writes.
pub fn decide_escalation(
    input: EscalationInput<'_>,
) -> (Option<ProposedEscalationWire>, Option<String>, bool) {
    if matches!(input.primary, PrimaryStateWire::WatcherBroken { .. }) {
        return (None, None, false);
    }
    let Some(issues) = input.snapshot.issues.as_ready() else {
        return (None, None, true);
    };
    let active = states_to_consider(input.primary, input.secondary);
    let tick = input.policy.tick_identity.as_str();

    for state in &active {
        if state.escalates_immediately() {
            let body =
                format!("immediate {} {}", state.family(), state.identity());
            return (
                Some(open_or_plus_one(
                    issues,
                    state,
                    tick,
                    EscalationSeverityWire::Immediate,
                    &body,
                )),
                None,
                false,
            );
        }
    }

    let (aged, started, uncertain) =
        aged_enough(input.observed_at, input.snapshot, input.primary);
    if aged && input.primary.is_blocking() {
        let body = format!(
            "blocked 24h {} {}",
            input.primary.family(),
            input.primary.identity()
        );
        return (
            Some(open_or_plus_one(
                issues,
                input.primary,
                tick,
                EscalationSeverityWire::Aged,
                &body,
            )),
            started,
            false,
        );
    }

    if let Some(close) = recovery_close(issues, &active) {
        return (Some(close), started, uncertain);
    }

    for state in &active {
        if state.is_advisory_qualification() {
            let body = format!(
                "advisory qualification {} {}",
                state.family(),
                state.identity()
            );
            return (
                Some(open_or_plus_one(
                    issues,
                    state,
                    tick,
                    EscalationSeverityWire::Advisory,
                    &body,
                )),
                started,
                uncertain,
            );
        }
    }

    (None, started, uncertain)
}
