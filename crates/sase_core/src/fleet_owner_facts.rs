//! Owner-resolved presentation facts for fleet rows.
//!
//! Mirrors the per-record half of the owner's agent enrichment
//! (`enrich_agent_from_meta_wire`): a rich base status plus the shell, plan,
//! question, retry, and lifecycle facts the agent session status pass consumes. The
//! agent-session-level policy (`TALE DONE`, `EPIC CREATED`, root mirroring, ...) stays
//! with the viewer's shared status pipeline; this module only supplies the
//! inputs it reads.
//!
//! Two facts depend on owner-side filesystem state and are therefore behind
//! [`OwnerFileObserver`]: whether a pending question already has a persisted
//! response, and the tier of a submitted plan file. The summary never carries
//! the underlying paths, only the derived `question_answered` / `plan_tier`.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::Path;
use std::sync::Mutex;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::agent_scan::{AgentArtifactRecordWire, AgentSessionShellWire};
use crate::fleet_agent_session::agent_session_shell;
use crate::fleet_contract::{
    reject_secretish, trim_to_limit, validate_label, validate_timestamp,
    FleetContractError, OwnerLivenessWire, MAX_LABEL_BYTES,
};

const ACTIVE_STATUSES: [&str; 2] = ["STARTING", "RUNNING"];
const PLAN_TIER_CACHE_MAX_ENTRIES: usize = 256;

/// Owner-side filesystem observations that are not part of the record.
pub trait OwnerFileObserver: Send + Sync {
    /// Whether the record's pending question already has a persisted response.
    fn question_answered(&self, record: &AgentArtifactRecordWire) -> bool;
    /// The `tier` frontmatter (`tale` / `epic`) of the record's plan file.
    fn plan_tier(&self, record: &AgentArtifactRecordWire) -> Option<String>;
}

type PlanTierCacheEntry = (Option<(u64, SystemTime)>, Option<String>);

/// Production observer: stats the sibling response file and reads plan
/// frontmatter through a bounded, file-signature-aware cache.
#[derive(Debug, Default)]
pub struct HostOwnerFileObserver {
    plan_tiers: Mutex<HashMap<String, PlanTierCacheEntry>>,
}

impl OwnerFileObserver for HostOwnerFileObserver {
    fn question_answered(&self, record: &AgentArtifactRecordWire) -> bool {
        let Some(request_path) = record
            .pending_question
            .as_ref()
            .and_then(|marker| marker.request_path.as_deref())
            .filter(|value| !value.is_empty())
        else {
            return false;
        };
        Path::new(request_path)
            .parent()
            .map(|dir| dir.join("question_response.json").exists())
            .unwrap_or(false)
    }

    fn plan_tier(&self, record: &AgentArtifactRecordWire) -> Option<String> {
        let path = record
            .agent_meta
            .as_ref()
            .and_then(|meta| meta.plan_path.as_deref())
            .filter(|value| !value.is_empty())?;
        let signature = fs::metadata(path)
            .ok()
            .and_then(|meta| Some((meta.len(), meta.modified().ok()?)));
        let mut cache = self.plan_tiers.lock().ok()?;
        if let Some((cached_signature, tier)) = cache.get(path) {
            if signature.is_some() && *cached_signature == signature {
                return tier.clone();
            }
        }
        let tier = fs::read_to_string(path)
            .ok()
            .and_then(|content| plan_tier_from_content(&content));
        if cache.len() >= PLAN_TIER_CACHE_MAX_ENTRIES {
            cache.clear();
        }
        cache.insert(path.to_string(), (signature, tier.clone()));
        tier
    }
}

/// Hermetic observer keyed by artifact directory or agent name.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InjectedOwnerFilesWire {
    #[serde(default)]
    pub question_answered: BTreeSet<String>,
    #[serde(default)]
    pub plan_tiers: BTreeMap<String, String>,
}

fn injected_keys(record: &AgentArtifactRecordWire) -> Vec<&str> {
    let mut keys = vec![record.artifact_dir.as_str()];
    if let Some(name) = record
        .agent_meta
        .as_ref()
        .and_then(|meta| meta.name.as_deref())
    {
        keys.push(name);
    }
    keys
}

impl OwnerFileObserver for InjectedOwnerFilesWire {
    fn question_answered(&self, record: &AgentArtifactRecordWire) -> bool {
        injected_keys(record)
            .into_iter()
            .any(|key| self.question_answered.contains(key))
    }

    fn plan_tier(&self, record: &AgentArtifactRecordWire) -> Option<String> {
        injected_keys(record)
            .into_iter()
            .find_map(|key| self.plan_tiers.get(key))
            .and_then(|tier| normalize_plan_tier(tier))
    }
}

fn normalize_plan_tier(value: &str) -> Option<String> {
    let normalized = value.trim().to_ascii_lowercase();
    matches!(normalized.as_str(), "tale" | "epic").then_some(normalized)
}

fn plan_tier_from_content(content: &str) -> Option<String> {
    let (frontmatter, _) = crate::plan::read::split_frontmatter(content);
    let parsed: serde_yaml::Value = serde_yaml::from_str(&frontmatter?).ok()?;
    normalize_plan_tier(parsed.get("tier")?.as_str()?)
}

/// Owner presentation facts carried beside the rich status. Every field is
/// optional/defaulted so older payloads still deserialize, and none is a path.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerPresentationFactsWire {
    /// Marks the summary `status` as the owner's rich resolved status rather
    /// than a coarse legacy one. Absent (false) on older payloads.
    #[serde(default)]
    pub owner_status: bool,
    #[serde(
        default,
        rename = "agent_family_role",
        alias = "agent_session_role"
    )]
    pub agent_session_role: Option<String>,
    #[serde(default)]
    pub role_suffix: Option<String>,
    // legacy agent-family spelling; flips in core-contract
    #[serde(default, rename = "agent_family_parallel")]
    pub agent_session_parallel: bool,
    #[serde(default)]
    pub plan_chain_root: bool,
    #[serde(default)]
    pub plan_action: Option<String>,
    #[serde(default)]
    pub plan_committed: Option<bool>,
    #[serde(default)]
    pub plan_tier: Option<String>,
    #[serde(default)]
    pub plan_submitted_at_unix: Vec<f64>,
    #[serde(default)]
    pub questions_submitted_at_unix: Vec<f64>,
    #[serde(default)]
    pub epic_started_at_unix: Option<f64>,
    #[serde(default)]
    pub question_answered: bool,
    #[serde(default)]
    pub retry_of_timestamp: Option<String>,
    #[serde(default)]
    pub retry_attempt: Option<i64>,
    #[serde(default)]
    pub retry_terminal: bool,
    #[serde(default)]
    pub reasoning_effort: Option<String>,
    #[serde(default)]
    pub monitor_id: Option<String>,
    #[serde(default)]
    pub monitor_state: Option<String>,
    #[serde(default)]
    pub monitor_label: Option<String>,
    #[serde(default)]
    pub monitor_command: Option<String>,
    #[serde(default)]
    pub gate_id: Option<String>,
    #[serde(default)]
    pub gate_kind: Option<String>,
    #[serde(default)]
    pub gate_state: Option<String>,
    #[serde(default)]
    pub gate_label: Option<String>,
    #[serde(default)]
    pub gate_accent: Option<String>,
    #[serde(default)]
    pub proc_id: Option<String>,
    #[serde(default)]
    pub proc_status: Option<String>,
    #[serde(default)]
    pub proc_label: Option<String>,
    #[serde(default)]
    pub shell_start_status: Option<String>,
    #[serde(default)]
    pub shell_stop_status: Option<String>,
}

impl OwnerPresentationFactsWire {
    pub fn is_empty(&self) -> bool {
        self == &Self::default()
    }

    /// Trim labels to the contract bound and drop blank strings.
    pub(crate) fn sanitized(&self) -> Result<Self, FleetContractError> {
        let mut facts = self.clone();
        for value in facts.string_fields_mut() {
            *value = label(value.as_deref());
        }
        facts
            .plan_submitted_at_unix
            .retain(|value| value.is_finite());
        facts
            .questions_submitted_at_unix
            .retain(|value| value.is_finite());
        Ok(facts)
    }

    /// Validate timestamps and label bounds of an already-sanitized value.
    pub(crate) fn validate(&self) -> Result<(), FleetContractError> {
        for value in self
            .plan_submitted_at_unix
            .iter()
            .chain(&self.questions_submitted_at_unix)
            .chain(self.epic_started_at_unix.iter())
        {
            validate_timestamp("presentation timestamp", *value)?;
        }
        let mut copy = self.clone();
        for value in copy.string_fields_mut() {
            if let Some(value) = value.as_deref() {
                validate_label("presentation label", value, MAX_LABEL_BYTES)?;
            }
        }
        Ok(())
    }

    fn string_fields_mut(&mut self) -> [&mut Option<String>; 20] {
        [
            &mut self.agent_session_role,
            &mut self.role_suffix,
            &mut self.plan_action,
            &mut self.plan_tier,
            &mut self.retry_of_timestamp,
            &mut self.reasoning_effort,
            &mut self.monitor_id,
            &mut self.monitor_state,
            &mut self.monitor_label,
            &mut self.monitor_command,
            &mut self.gate_id,
            &mut self.gate_kind,
            &mut self.gate_state,
            &mut self.gate_label,
            &mut self.gate_accent,
            &mut self.proc_id,
            &mut self.proc_status,
            &mut self.proc_label,
            &mut self.shell_start_status,
            &mut self.shell_stop_status,
        ]
    }
}

/// A record's derived rich status plus its presentation facts.
#[derive(Debug, Clone, PartialEq)]
pub struct OwnerRecordFacts {
    pub status: String,
    pub facts: OwnerPresentationFactsWire,
}

fn label(value: Option<&str>) -> Option<String> {
    let value = value?.trim();
    (!value.is_empty()).then(|| trim_to_limit(value, MAX_LABEL_BYTES))
}

fn unix_times(values: &[String]) -> Vec<f64> {
    values
        .iter()
        .filter_map(|value| crate::fleet_catalog::parse_rfc3339_unix(value))
        .collect()
}

fn base_status(record: &AgentArtifactRecordWire) -> String {
    if let Some(done) = &record.done {
        if done.repeat_stopped {
            return "STOPPED".to_string();
        }
        if let Some(status) = label(done.status_label.as_deref()) {
            return status;
        }
        if done
            .error
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
            || done
                .outcome
                .as_deref()
                .is_some_and(|value| value.starts_with("failed"))
        {
            return "FAILED".to_string();
        }
        return "DONE".to_string();
    }
    match record
        .workflow_state
        .as_ref()
        .map(|state| state.status.to_ascii_lowercase())
        .as_deref()
    {
        Some("completed" | "noop") => "DONE".to_string(),
        Some("failed") => "FAILED".to_string(),
        Some("cancelled") => "STOPPED".to_string(),
        Some("starting")
            if record
                .workflow_state
                .as_ref()
                .is_some_and(|state| state.appears_as_agent) =>
        {
            "STARTING".to_string()
        }
        _ => "RUNNING".to_string(),
    }
}

fn plan_status(
    plan_approved: bool,
    plan_action: Option<&str>,
    plan_submitted: bool,
    auto_approved: bool,
    plan_tier: Option<&str>,
    plan_committed: Option<bool>,
) -> Option<&'static str> {
    if matches!(plan_action, Some("failed" | "epic_failed")) {
        return Some(if plan_action == Some("epic_failed") {
            "EPIC FAILED"
        } else {
            "PLAN FAILED"
        });
    }
    if plan_approved {
        return match plan_action {
            Some("commit") if plan_committed == Some(false) => None,
            Some("commit") => Some("PLAN COMMITTED"),
            Some("tale") => Some("TALE APPROVED"),
            Some("epic") => Some("EPIC APPROVED"),
            _ => Some("PLAN APPROVED"),
        };
    }
    if plan_submitted && !auto_approved {
        return Some(match plan_tier {
            Some("tale") => "TALE",
            Some("epic") => "EPIC",
            _ => "PLAN",
        });
    }
    None
}

/// Derive the owner's rich base status and presentation facts for one record.
pub fn derive_owner_record_facts(
    record: &AgentArtifactRecordWire,
    liveness: OwnerLivenessWire,
    files: &dyn OwnerFileObserver,
) -> OwnerRecordFacts {
    let mut status = base_status(record);
    let mut facts = OwnerPresentationFactsWire {
        owner_status: true,
        ..Default::default()
    };
    let Some(meta) = record.agent_meta.as_ref() else {
        return OwnerRecordFacts { status, facts };
    };
    let active = |status: &str| ACTIVE_STATUSES.contains(&status);

    let run_started = meta
        .run_started_at
        .as_deref()
        .filter(|value| !value.is_empty());
    if status == "STARTING" {
        let promoted = match run_started {
            Some(value) => {
                crate::fleet_catalog::parse_rfc3339_unix(value).is_some()
            }
            None => meta
                .wait_completed_at
                .as_deref()
                .is_some_and(|value| !value.is_empty()),
        };
        if promoted {
            status = "RUNNING".to_string();
        }
    }
    if record.waiting.is_some() && active(&status) {
        status = "WAITING".to_string();
    }
    let question_answered =
        record.pending_question.is_some() && files.question_answered(record);
    if record.pending_question.is_some() && active(&status) {
        status = if question_answered {
            "ANSWERED"
        } else {
            "QUESTION"
        }
        .to_string();
    }

    let auto_approved = meta.approve
        || meta
            .auto_approve_plan_action
            .as_deref()
            .is_some_and(|value| !value.is_empty());
    let mut plan_tier = None;
    if meta.plan {
        let plan_submitted = !meta.plan_submitted_at.is_empty();
        let plan_action = meta
            .plan_action
            .as_deref()
            .filter(|value| !value.is_empty());
        let mut eligible = active(&status);
        if !eligible && status == "DONE" {
            // Deliberately tolerant of gate_id / gate_member_agent_name, as
            // the owner's wire path is: the creator's own record never
            // carries them.
            eligible = plan_submitted
                && !meta.plan_approved
                && plan_action.is_none()
                && !auto_approved
                && !meta
                    .stopped_at
                    .as_deref()
                    .is_some_and(|value| !value.is_empty())
                && !record.has_done_marker
                && record.done.is_none()
                && liveness == OwnerLivenessWire::Alive;
        }
        if plan_submitted && !meta.plan_approved && !auto_approved {
            plan_tier = files.plan_tier(record);
        }
        if eligible {
            if let Some(next) = plan_status(
                meta.plan_approved,
                plan_action,
                plan_submitted,
                auto_approved,
                plan_tier.as_deref(),
                meta.plan_committed,
            ) {
                status = next.to_string();
            }
        }
    }

    facts.agent_session_role = label(meta.agent_session_role.as_deref());
    facts.role_suffix = label(meta.role_suffix.as_deref());
    facts.agent_session_parallel = meta.agent_session_parallel;
    facts.plan_chain_root = meta.plan_chain_root;
    facts.plan_action = label(meta.plan_action.as_deref());
    facts.plan_committed = meta.plan_committed;
    facts.plan_tier = plan_tier;
    facts.plan_submitted_at_unix = unix_times(&meta.plan_submitted_at);
    facts.questions_submitted_at_unix =
        unix_times(&meta.questions_submitted_at);
    facts.epic_started_at_unix = meta
        .epic_started_at
        .as_deref()
        .and_then(crate::fleet_catalog::parse_rfc3339_unix);
    // Local rows key the answered policy off a persisted response path; ship
    // only the derived boolean.
    facts.question_answered = question_answered
        || meta
            .question_response_path
            .as_deref()
            .is_some_and(|value| !value.is_empty());
    facts.retry_of_timestamp = label(meta.retry_of_timestamp.as_deref());
    facts.retry_attempt = meta.retry_attempt;
    facts.retry_terminal = meta.retry_terminal;
    facts.reasoning_effort = label(meta.reasoning_effort.as_deref());
    facts.proc_id = label(meta.proc_id.as_deref());
    if let Some(shell) = agent_session_shell(Some(meta), record.done.as_ref()) {
        apply_shell_facts(&mut facts, shell);
    }
    OwnerRecordFacts { status, facts }
}

fn apply_shell_facts(
    facts: &mut OwnerPresentationFactsWire,
    shell: &AgentSessionShellWire,
) {
    let kind = shell.kind.trim().to_ascii_lowercase();
    let id = label(shell.id.as_deref());
    let state = label(shell.state.as_deref());
    let shell_label = label(shell.label.as_deref());
    match kind.as_str() {
        "monitor" | "mon" => {
            facts.monitor_id = id;
            facts.monitor_state = state;
            facts.monitor_label = shell_label;
            facts.monitor_command = shell
                .monitor
                .as_ref()
                .and_then(|monitor| label(monitor.command.as_deref()))
                .filter(|command| {
                    reject_secretish("monitor_command", command).is_ok()
                });
        }
        "gate" => {
            facts.gate_id = id;
            facts.gate_state = state;
            facts.gate_label = shell_label;
            let gate = shell.gate.as_ref();
            facts.gate_kind = gate.and_then(|gate| label(gate.kind.as_deref()));
            facts.gate_accent =
                gate.and_then(|gate| label(gate.accent.as_deref()));
        }
        "proc" => {
            if facts.proc_id.is_none() {
                facts.proc_id = id;
            }
            facts.proc_status = state;
            facts.proc_label = shell_label;
        }
        _ => return,
    }
    facts.shell_start_status = label(shell.start_status.as_deref());
    facts.shell_stop_status = label(shell.stop_status.as_deref());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_scan::wire::PendingQuestionMarkerWire;
    use crate::agent_scan::{
        AgentMetaWire, AgentSessionShellGateWire, AgentSessionShellMonitorWire,
        DoneMarkerWire, WaitingMarkerWire,
    };

    fn record(meta: AgentMetaWire) -> AgentArtifactRecordWire {
        AgentArtifactRecordWire {
            project_name: "p".into(),
            project_dir: "/p".into(),
            project_file: "/p/p.gp".into(),
            workflow_dir_name: "ace-run".into(),
            artifact_dir: "/p/artifacts/1".into(),
            timestamp: "20260920060000".into(),
            agent_meta: Some(meta),
            done: None,
            running: None,
            waiting: None,
            pending_question: None,
            workflow_state: None,
            plan_path: None,
            prompt_steps: Vec::new(),
            raw_prompt_snippet: None,
            used_xprompts: Vec::new(),
            has_done_marker: false,
            record_shape: Default::default(),
        }
    }

    #[derive(Default)]
    struct Files {
        answered: bool,
        tier: Option<&'static str>,
    }

    impl OwnerFileObserver for Files {
        fn question_answered(&self, _: &AgentArtifactRecordWire) -> bool {
            self.answered
        }
        fn plan_tier(&self, _: &AgentArtifactRecordWire) -> Option<String> {
            self.tier.map(str::to_string)
        }
    }

    fn derive(
        rec: &AgentArtifactRecordWire,
        files: &Files,
    ) -> OwnerRecordFacts {
        derive_owner_record_facts(rec, OwnerLivenessWire::Alive, files)
    }

    #[test]
    fn done_status_rules_in_owner_order() {
        let mut rec = record(AgentMetaWire::default());
        rec.done = Some(DoneMarkerWire {
            outcome: Some("failed: x".into()),
            ..Default::default()
        });
        assert_eq!(derive(&rec, &Files::default()).status, "FAILED");
        rec.done.as_mut().unwrap().status_label = Some("TALE DONE".into());
        assert_eq!(derive(&rec, &Files::default()).status, "TALE DONE");
        rec.done.as_mut().unwrap().repeat_stopped = true;
        assert_eq!(derive(&rec, &Files::default()).status, "STOPPED");
        rec.done = Some(DoneMarkerWire::default());
        assert_eq!(derive(&rec, &Files::default()).status, "DONE");
    }

    #[test]
    fn waiting_beats_question_and_question_answers() {
        let mut rec = record(AgentMetaWire::default());
        rec.pending_question = Some(PendingQuestionMarkerWire::default());
        assert_eq!(derive(&rec, &Files::default()).status, "QUESTION");
        let answered = Files {
            answered: true,
            ..Default::default()
        };
        let facts = derive(&rec, &answered);
        assert_eq!(facts.status, "ANSWERED");
        assert!(facts.facts.question_answered);
        rec.waiting = Some(WaitingMarkerWire::default());
        assert_eq!(derive(&rec, &answered).status, "WAITING");
    }

    #[test]
    fn plan_statuses_follow_owner_rules() {
        let mut meta = AgentMetaWire {
            plan: true,
            plan_approved: true,
            plan_action: Some("tale".into()),
            ..Default::default()
        };
        assert_eq!(
            derive(&record(meta.clone()), &Files::default()).status,
            "TALE APPROVED"
        );
        meta.plan_action = Some("commit".into());
        meta.plan_committed = Some(false);
        assert_eq!(
            derive(&record(meta.clone()), &Files::default()).status,
            "RUNNING"
        );
        meta.plan_action = Some("epic_failed".into());
        assert_eq!(
            derive(&record(meta), &Files::default()).status,
            "EPIC FAILED"
        );
    }

    #[test]
    fn pending_review_uses_injected_tier_and_skips_auto_approved() {
        let mut meta = AgentMetaWire {
            plan: true,
            plan_submitted_at: vec!["2026-09-20T06:00:00Z".into()],
            ..Default::default()
        };
        let tale = Files {
            tier: Some("tale"),
            ..Default::default()
        };
        let derived = derive(&record(meta.clone()), &tale);
        assert_eq!(derived.status, "TALE");
        assert_eq!(derived.facts.plan_tier.as_deref(), Some("tale"));
        assert_eq!(derived.facts.plan_submitted_at_unix.len(), 1);
        assert_eq!(
            derive(&record(meta.clone()), &Files::default()).status,
            "PLAN"
        );
        meta.approve = true;
        assert_eq!(derive(&record(meta), &tale).status, "RUNNING");
    }

    #[test]
    fn done_row_reopens_only_inside_pending_review_window() {
        let meta = AgentMetaWire {
            plan: true,
            plan_submitted_at: vec!["2026-09-20T06:00:00Z".into()],
            ..Default::default()
        };
        let mut rec = record(meta);
        rec.workflow_state = Some(crate::agent_scan::WorkflowStateWire {
            status: "completed".into(),
            ..Default::default()
        });
        assert_eq!(derive(&rec, &Files::default()).status, "PLAN");
        assert_eq!(
            derive_owner_record_facts(
                &rec,
                OwnerLivenessWire::Dead,
                &Files::default()
            )
            .status,
            "DONE"
        );
        rec.has_done_marker = true;
        assert_eq!(derive(&rec, &Files::default()).status, "DONE");
    }

    #[test]
    fn starting_promotes_on_run_start() {
        let mut rec = record(AgentMetaWire {
            run_started_at: Some("2026-09-20T06:00:00Z".into()),
            ..Default::default()
        });
        rec.workflow_state = Some(crate::agent_scan::WorkflowStateWire {
            status: "starting".into(),
            appears_as_agent: true,
            ..Default::default()
        });
        assert_eq!(derive(&rec, &Files::default()).status, "RUNNING");
        rec.agent_meta.as_mut().unwrap().run_started_at = None;
        assert_eq!(derive(&rec, &Files::default()).status, "STARTING");
    }

    #[test]
    fn gate_monitor_and_proc_shell_facts() {
        let gate = record(AgentMetaWire {
            agent_session_shell: Some(AgentSessionShellWire {
                kind: "gate".into(),
                id: Some("g1".into()),
                state: Some("pending".into()),
                label: Some("plan review".into()),
                start_status: Some("PLAN REVIEW".into()),
                gate: Some(AgentSessionShellGateWire {
                    kind: Some("approval".into()),
                    accent: Some("blue".into()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            role_suffix: Some("plan".into()),
            ..Default::default()
        });
        let facts = derive(&gate, &Files::default()).facts;
        assert_eq!(facts.gate_id.as_deref(), Some("g1"));
        assert_eq!(facts.gate_kind.as_deref(), Some("approval"));
        assert_eq!(facts.gate_accent.as_deref(), Some("blue"));
        assert_eq!(facts.shell_start_status.as_deref(), Some("PLAN REVIEW"));
        assert_eq!(facts.role_suffix.as_deref(), Some("plan"));
        assert!(facts.monitor_id.is_none());

        let monitor = record(AgentMetaWire {
            agent_session_shell: Some(AgentSessionShellWire {
                kind: "monitor".into(),
                id: Some("m1".into()),
                state: Some("running".into()),
                monitor: Some(AgentSessionShellMonitorWire {
                    command: Some("sleep 1".into()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        });
        let facts = derive(&monitor, &Files::default()).facts;
        assert_eq!(facts.monitor_id.as_deref(), Some("m1"));
        assert_eq!(facts.monitor_command.as_deref(), Some("sleep 1"));

        let proc = record(AgentMetaWire {
            proc_id: Some("p1".into()),
            agent_session_shell: Some(AgentSessionShellWire {
                kind: "proc".into(),
                state: Some("running".into()),
                label: Some("build".into()),
                ..Default::default()
            }),
            ..Default::default()
        });
        let facts = derive(&proc, &Files::default()).facts;
        assert_eq!(facts.proc_id.as_deref(), Some("p1"));
        assert_eq!(facts.proc_label.as_deref(), Some("build"));
    }

    #[test]
    fn plan_tier_frontmatter_parses_and_normalizes() {
        assert_eq!(
            plan_tier_from_content("---\ntier: ' EPIC '\n---\n# x\n"),
            Some("epic".to_string())
        );
        assert_eq!(plan_tier_from_content("---\ntier: other\n---\n"), None);
        assert_eq!(plan_tier_from_content("# no frontmatter"), None);
    }

    #[test]
    fn injected_observer_matches_by_dir_or_name() {
        let rec = record(AgentMetaWire {
            name: Some("0n".into()),
            ..Default::default()
        });
        let mut injected = InjectedOwnerFilesWire::default();
        injected.question_answered.insert("0n".into());
        injected.plan_tiers.insert("0n".into(), "Epic".into());
        assert!(injected.question_answered(&rec));
        assert_eq!(injected.plan_tier(&rec).as_deref(), Some("epic"));
    }
}
