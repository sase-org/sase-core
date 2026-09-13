//! Continuation ancestry retention decisions.
//!
//! Python collects per-run markers, parent IDs, starter dirs, portable
//! locators, and capture/delivery state. This module computes the
//! live/recoverable dependency closure that run-directory retention must
//! keep. It does not walk filesystems or apply deletions.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

use serde::{Deserialize, Serialize};

use super::schema::{
    validate_identifier, validate_non_empty_text, validate_reference,
    validate_schema, ContinuationError, CONTINUATION_WIRE_SCHEMA_VERSION,
    MAX_NODES, MAX_PARENTS_PER_NODE,
};

const MAX_PATH_BYTES: usize = 4096;

const REASON_LIVE: &str = "continuation_live";
const REASON_RECOVERABLE: &str = "continuation_recoverable";
const REASON_ANCESTRY: &str = "continuation_ancestry";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationRetentionRunWire {
    pub artifact_dir: String,
    #[serde(default)]
    pub timestamp: String,
    #[serde(default)]
    pub node_id: Option<String>,
    #[serde(default)]
    pub parent_node_ids: Vec<String>,
    #[serde(default)]
    pub starter_artifact_dir: Option<String>,
    #[serde(default)]
    pub live: bool,
    #[serde(default)]
    pub recoverable: bool,
    #[serde(default)]
    pub portable_refs: Vec<String>,
    #[serde(default)]
    pub required_ref_failures: Vec<String>,
    #[serde(default)]
    pub metadata_unavailable: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationRetentionRequestWire {
    pub schema_version: u32,
    #[serde(default)]
    pub runs: Vec<ContinuationRetentionRunWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationRetentionPlanWire {
    pub schema_version: u32,
    #[serde(default)]
    pub protected_dirs: Vec<String>,
    #[serde(default)]
    pub reasons_by_dir: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub protected_portable_ids: Vec<String>,
    #[serde(default)]
    pub required_failures: Vec<String>,
    #[serde(default)]
    pub sources_unavailable: Vec<String>,
}

pub fn plan_continuation_retention(
    request: ContinuationRetentionRequestWire,
) -> Result<ContinuationRetentionPlanWire, ContinuationError> {
    validate_schema(
        request.schema_version,
        "ContinuationRetentionRequestWire",
    )?;
    if request.runs.len() > MAX_NODES {
        return Err(ContinuationError::validation(format!(
            "runs has {} entries; maximum is {MAX_NODES}",
            request.runs.len()
        )));
    }

    let mut by_dir: HashMap<String, usize> = HashMap::new();
    let mut by_node: HashMap<String, Vec<String>> = HashMap::new();
    for (index, run) in request.runs.iter().enumerate() {
        validate_run(run, index)?;
        let dir = run.artifact_dir.clone();
        by_dir.entry(dir.clone()).or_insert(index);
        if let Some(node_id) = run.node_id.as_ref() {
            by_node
                .entry(node_id.clone())
                .or_default()
                .push(dir.clone());
        }
    }

    let mut reasons: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut queue: VecDeque<String> = VecDeque::new();
    let mut required_failures = BTreeSet::new();
    let mut sources_unavailable = BTreeSet::new();
    let mut portable = BTreeSet::new();

    for run in &request.runs {
        if !(run.live || run.recoverable) {
            continue;
        }
        let reason = if run.live {
            REASON_LIVE
        } else {
            REASON_RECOVERABLE
        };
        enqueue_protected(
            &mut reasons,
            &mut queue,
            run.artifact_dir.clone(),
            reason,
        );
        record_run_failures(
            run,
            &mut required_failures,
            &mut sources_unavailable,
        );
    }

    let mut visited = 0usize;
    while let Some(dir) = queue.pop_front() {
        visited += 1;
        if visited > MAX_NODES {
            return Err(ContinuationError::validation(
                "continuation retention closure exceeded the node bound"
                    .to_string(),
            ));
        }
        let Some(&index) = by_dir.get(&dir) else {
            continue;
        };
        let run = &request.runs[index];
        for portable_ref in &run.portable_refs {
            portable.insert(portable_ref.clone());
        }
        for parent_id in &run.parent_node_ids {
            if let Some(parent_dirs) = by_node.get(parent_id) {
                for parent_dir in parent_dirs.clone() {
                    enqueue_protected(
                        &mut reasons,
                        &mut queue,
                        parent_dir,
                        REASON_ANCESTRY,
                    );
                }
            }
        }
        if let Some(starter) = run.starter_artifact_dir.as_ref() {
            if !starter.is_empty() {
                enqueue_protected(
                    &mut reasons,
                    &mut queue,
                    starter.clone(),
                    REASON_ANCESTRY,
                );
            }
        }
    }

    Ok(ContinuationRetentionPlanWire {
        schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
        protected_dirs: reasons.keys().cloned().collect(),
        reasons_by_dir: reasons
            .into_iter()
            .map(|(dir, values)| (dir, values.into_iter().collect()))
            .collect(),
        protected_portable_ids: portable.into_iter().collect(),
        required_failures: required_failures.into_iter().collect(),
        sources_unavailable: sources_unavailable.into_iter().collect(),
    })
}

fn validate_run(
    run: &ContinuationRetentionRunWire,
    index: usize,
) -> Result<(), ContinuationError> {
    validate_non_empty_text(
        &run.artifact_dir,
        &format!("runs[{index}].artifact_dir"),
        MAX_PATH_BYTES,
    )?;
    if !run.timestamp.is_empty() {
        validate_identifier(
            &run.timestamp,
            &format!("runs[{index}].timestamp"),
        )?;
    }
    if let Some(node_id) = &run.node_id {
        validate_identifier(node_id, &format!("runs[{index}].node_id"))?;
    }
    if run.parent_node_ids.len() > MAX_PARENTS_PER_NODE {
        return Err(ContinuationError::validation(format!(
            "runs[{index}].parent_node_ids has {} entries; maximum is {MAX_PARENTS_PER_NODE}",
            run.parent_node_ids.len()
        )));
    }
    for (parent_index, parent_id) in run.parent_node_ids.iter().enumerate() {
        validate_identifier(
            parent_id,
            &format!("runs[{index}].parent_node_ids[{parent_index}]"),
        )?;
    }
    if let Some(starter) = &run.starter_artifact_dir {
        if !starter.is_empty() {
            validate_non_empty_text(
                starter,
                &format!("runs[{index}].starter_artifact_dir"),
                MAX_PATH_BYTES,
            )?;
        }
    }
    for (ref_index, portable_ref) in run.portable_refs.iter().enumerate() {
        validate_reference(
            portable_ref,
            &format!("runs[{index}].portable_refs[{ref_index}]"),
        )?;
    }
    Ok(())
}

fn enqueue_protected(
    reasons: &mut BTreeMap<String, BTreeSet<String>>,
    queue: &mut VecDeque<String>,
    dir: String,
    reason: &str,
) {
    let entry = reasons.entry(dir.clone()).or_default();
    let inserted_dir = entry.is_empty();
    entry.insert(reason.to_string());
    if inserted_dir {
        queue.push_back(dir);
    }
}

fn record_run_failures(
    run: &ContinuationRetentionRunWire,
    required_failures: &mut BTreeSet<String>,
    sources_unavailable: &mut BTreeSet<String>,
) {
    for failure in &run.required_ref_failures {
        if !failure.is_empty() {
            required_failures
                .insert(format!("{}: {failure}", run.artifact_dir));
        }
    }
    if let Some(unavailable) = &run.metadata_unavailable {
        if !unavailable.is_empty() {
            sources_unavailable
                .insert(format!("{}: {unavailable}", run.artifact_dir));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(
        dir: &str,
        node_id: Option<&str>,
        parents: &[&str],
        starter: Option<&str>,
        live: bool,
        recoverable: bool,
    ) -> ContinuationRetentionRunWire {
        ContinuationRetentionRunWire {
            artifact_dir: dir.to_string(),
            timestamp: dir
                .rsplit('/')
                .next()
                .unwrap_or("20260101000000")
                .to_string(),
            node_id: node_id.map(str::to_string),
            parent_node_ids: parents
                .iter()
                .map(|value| value.to_string())
                .collect(),
            starter_artifact_dir: starter.map(str::to_string),
            live,
            recoverable,
            portable_refs: Vec::new(),
            required_ref_failures: Vec::new(),
            metadata_unavailable: None,
        }
    }

    fn plan(
        runs: Vec<ContinuationRetentionRunWire>,
    ) -> ContinuationRetentionPlanWire {
        plan_continuation_retention(ContinuationRetentionRequestWire {
            schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
            runs,
        })
        .unwrap()
    }

    #[test]
    fn live_monitor_protects_referenced_parent_and_starter() {
        let old = "/tmp/proj/artifacts/ace-run/202605/01/20260501000000";
        let live = "/tmp/proj/artifacts/ace-run/202609/01/20260901000000";
        let unrelated = "/tmp/proj/artifacts/ace-run/202604/01/20260401000000";
        let result = plan(vec![
            run(old, Some("agent-delta:old"), &[], None, false, false),
            run(live, None, &["agent-delta:old"], Some(old), true, false),
            run(unrelated, None, &[], None, false, false),
        ]);

        assert!(result.protected_dirs.contains(&old.to_string()));
        assert!(result.protected_dirs.contains(&live.to_string()));
        assert!(!result.protected_dirs.contains(&unrelated.to_string()));
        assert_eq!(
            result.reasons_by_dir.get(old).unwrap(),
            &vec![REASON_ANCESTRY.to_string()]
        );
        assert_eq!(
            result.reasons_by_dir.get(live).unwrap(),
            &vec![REASON_LIVE.to_string()]
        );
    }

    #[test]
    fn terminal_settled_run_does_not_pin_unrelated_ancestry() {
        let old = "/tmp/old";
        let settled = "/tmp/settled";
        let result = plan(vec![
            run(old, Some("agent-delta:old"), &[], None, false, false),
            run(
                settled,
                Some("monitor-result:done"),
                &["agent-delta:old"],
                Some(old),
                false,
                false,
            ),
        ]);

        assert!(result.protected_dirs.is_empty());
    }

    #[test]
    fn recoverable_pending_delivery_protects_ancestry() {
        let old = "/tmp/old";
        let pending = "/tmp/pending";
        let result = plan(vec![
            run(old, Some("agent-delta:old"), &[], None, false, false),
            run(
                pending,
                Some("monitor-result:pending"),
                &["agent-delta:old"],
                Some(old),
                false,
                true,
            ),
        ]);

        assert!(result.protected_dirs.contains(&old.to_string()));
        assert_eq!(
            result.reasons_by_dir.get(pending).unwrap(),
            &vec![REASON_RECOVERABLE.to_string()]
        );
    }

    #[test]
    fn required_failures_and_unavailable_metadata_surface() {
        let live = "/tmp/live";
        let mut live_run = run(live, None, &[], None, true, false);
        live_run.required_ref_failures =
            vec!["checkpoint portable registration failed".to_string()];
        live_run.metadata_unavailable =
            Some("delivery dir unreadable".to_string());
        live_run.portable_refs = vec!["file:explicit:keep".to_string()];
        let result = plan(vec![live_run]);

        assert_eq!(
            result.required_failures,
            vec![format!("{live}: checkpoint portable registration failed")]
        );
        assert_eq!(
            result.sources_unavailable,
            vec![format!("{live}: delivery dir unreadable")]
        );
        assert_eq!(
            result.protected_portable_ids,
            vec!["file:explicit:keep".to_string()]
        );
    }

    #[test]
    fn starter_dir_absent_from_scan_is_still_protected() {
        let missing_starter = "/tmp/missing-starter";
        let live = "/tmp/live";
        let result = plan(vec![run(
            live,
            None,
            &[],
            Some(missing_starter),
            true,
            false,
        )]);

        assert!(result.protected_dirs.contains(&missing_starter.to_string()));
        assert_eq!(
            result.reasons_by_dir.get(missing_starter).unwrap(),
            &vec![REASON_ANCESTRY.to_string()]
        );
    }
}
