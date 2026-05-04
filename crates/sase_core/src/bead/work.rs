//! Deterministic work planning for `sase bead work`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use serde::{Deserialize, Serialize};

use super::read::read_store_issues;
use super::wire::{
    BeadError, BeadTierWire, IssueTypeWire, IssueWire, StatusWire,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhaseAssignmentWire {
    pub bead_id: String,
    pub agent_name: String,
    pub waits_on: Vec<String>,
    pub wave: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpicWorkPlanWire {
    pub epic_id: String,
    pub waves: Vec<Vec<PhaseAssignmentWire>>,
    pub land_agent_name: String,
    pub land_waits_on: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegendEpicAssignmentWire {
    pub epic_number: usize,
    pub agent_name: String,
    pub waits_on: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegendWorkPlanWire {
    pub legend_id: String,
    pub plan_file: String,
    pub assignments: Vec<LegendEpicAssignmentWire>,
}

pub fn build_epic_work_plan(
    beads_dir: &Path,
    epic_id: &str,
) -> Result<EpicWorkPlanWire, BeadError> {
    build_epic_work_plan_from_issues(read_store_issues(beads_dir)?, epic_id)
}

pub fn build_legend_work_plan(
    beads_dir: &Path,
    legend_id: &str,
) -> Result<LegendWorkPlanWire, BeadError> {
    build_legend_work_plan_from_issues(read_store_issues(beads_dir)?, legend_id)
}

pub fn build_epic_work_plan_from_issues(
    issues: Vec<IssueWire>,
    epic_id: &str,
) -> Result<EpicWorkPlanWire, BeadError> {
    let issue_by_id: BTreeMap<&str, &IssueWire> = issues
        .iter()
        .map(|issue| (issue.id.as_str(), issue))
        .collect();
    let epic = issue_by_id.get(epic_id).ok_or_else(|| BeadError {
        kind: "not_found".to_string(),
        message: format!("Epic '{epic_id}' not found"),
    })?;
    if epic.issue_type != IssueTypeWire::Plan {
        return Err(BeadError::validation(format!(
            "'{epic_id}' is not a plan/epic bead"
        )));
    }
    if epic.tier != Some(BeadTierWire::Epic) {
        return Err(BeadError::validation(format!(
            "'{epic_id}' is not an epic bead"
        )));
    }

    let children: Vec<&IssueWire> = issues
        .iter()
        .filter(|issue| issue.parent_id.as_deref() == Some(epic_id))
        .collect();
    let open_phases: Vec<&IssueWire> = children
        .iter()
        .copied()
        .filter(|issue| {
            issue.issue_type == IssueTypeWire::Phase
                && issue.status != StatusWire::Closed
        })
        .collect();
    if open_phases.is_empty() {
        return Err(BeadError::validation(format!(
            "Epic '{epic_id}' has no non-closed phase children"
        )));
    }

    let in_epic_phase_ids: BTreeSet<&str> = children
        .iter()
        .filter(|issue| issue.issue_type == IssueTypeWire::Phase)
        .map(|issue| issue.id.as_str())
        .collect();
    let open_phase_ids: BTreeSet<&str> =
        open_phases.iter().map(|issue| issue.id.as_str()).collect();

    let mut deps: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for phase in &open_phases {
        let mut in_epic_open: BTreeSet<&str> = BTreeSet::new();
        for dep in &phase.dependencies {
            let blocker_id = dep.depends_on_id.as_str();
            if in_epic_phase_ids.contains(blocker_id) {
                if open_phase_ids.contains(blocker_id) {
                    in_epic_open.insert(blocker_id);
                }
                continue;
            }
            let blocker = issue_by_id.get(blocker_id);
            let blocker_is_active = match blocker {
                Some(issue) => issue.status != StatusWire::Closed,
                None => true,
            };
            if blocker_is_active {
                return Err(BeadError {
                    kind: "cross_epic_blocker".to_string(),
                    message: format!(
                        "Phase '{}' depends on out-of-epic blocker '{}' that is not closed",
                        phase.id, blocker_id
                    ),
                });
            }
        }
        deps.insert(phase.id.as_str(), in_epic_open);
    }

    let sort_key: BTreeMap<&str, (&str, &str)> = open_phases
        .iter()
        .map(|phase| {
            (
                phase.id.as_str(),
                (phase.created_at.as_str(), phase.id.as_str()),
            )
        })
        .collect();
    let mut waves: Vec<Vec<&str>> = Vec::new();
    let mut placed: BTreeSet<&str> = BTreeSet::new();
    let mut remaining = open_phase_ids.clone();
    while !remaining.is_empty() {
        let mut ready: Vec<&str> = remaining
            .iter()
            .copied()
            .filter(|pid| {
                deps.get(pid)
                    .is_some_and(|blockers| blockers.is_subset(&placed))
            })
            .collect();
        ready.sort_by_key(|pid| sort_key.get(pid).copied().unwrap_or(("", "")));
        if ready.is_empty() {
            return Err(BeadError {
                kind: "cycle".to_string(),
                message: format!(
                    "Cycle detected among phases of epic '{}': {}",
                    epic_id,
                    format_id_list(remaining.iter().copied())
                ),
            });
        }
        placed.extend(ready.iter().copied());
        for pid in &ready {
            remaining.remove(pid);
        }
        waves.push(ready);
    }

    let assigned_waves = waves
        .iter()
        .enumerate()
        .map(|(wave_index, wave_ids)| {
            wave_ids
                .iter()
                .map(|pid| {
                    let waits_on = deps
                        .get(pid)
                        .into_iter()
                        .flat_map(|ids| ids.iter().copied())
                        .map(phase_agent_name)
                        .collect();
                    PhaseAssignmentWire {
                        bead_id: (*pid).to_string(),
                        agent_name: phase_agent_name(pid),
                        waits_on,
                        wave: wave_index,
                    }
                })
                .collect()
        })
        .collect();

    let mut has_dependent: BTreeSet<&str> = BTreeSet::new();
    for blockers in deps.values() {
        has_dependent.extend(blockers.iter().copied());
    }
    let land_waits_on = open_phase_ids
        .difference(&has_dependent)
        .copied()
        .map(phase_agent_name)
        .collect();

    Ok(EpicWorkPlanWire {
        epic_id: epic_id.to_string(),
        waves: assigned_waves,
        land_agent_name: land_agent_name(epic_id),
        land_waits_on,
    })
}

pub fn build_legend_work_plan_from_issues(
    issues: Vec<IssueWire>,
    legend_id: &str,
) -> Result<LegendWorkPlanWire, BeadError> {
    let issue_by_id: BTreeMap<&str, &IssueWire> = issues
        .iter()
        .map(|issue| (issue.id.as_str(), issue))
        .collect();
    let legend = issue_by_id.get(legend_id).ok_or_else(|| BeadError {
        kind: "not_found".to_string(),
        message: format!("Legend '{legend_id}' not found"),
    })?;
    if legend.issue_type != IssueTypeWire::Plan {
        return Err(BeadError::validation(format!(
            "'{legend_id}' is not a plan/legend bead"
        )));
    }
    if legend.tier != Some(BeadTierWire::Legend) {
        return Err(BeadError::validation(format!(
            "'{legend_id}' is not a legend bead"
        )));
    }

    let Some(epic_count) = legend.epic_count else {
        return Err(BeadError::validation(format!(
            "Legend '{legend_id}' is missing epic_count"
        )));
    };
    if epic_count <= 0 {
        return Err(BeadError::validation(format!(
            "Legend '{legend_id}' has invalid epic_count {epic_count}"
        )));
    }
    if legend.design.trim().is_empty() {
        return Err(BeadError::validation(format!(
            "Legend '{legend_id}' is missing a design/plan file"
        )));
    }

    let assignments = (1..=epic_count as usize)
        .map(|epic_number| LegendEpicAssignmentWire {
            epic_number,
            agent_name: legend_epic_agent_name(legend_id, epic_number),
            waits_on: legend_epic_waits_on(legend_id, epic_number),
        })
        .collect();

    Ok(LegendWorkPlanWire {
        legend_id: legend_id.to_string(),
        plan_file: legend.design.clone(),
        assignments,
    })
}

fn phase_agent_name(bead_id: &str) -> String {
    bead_id.to_string()
}

fn land_agent_name(epic_id: &str) -> String {
    epic_id.to_string()
}

fn legend_epic_agent_name(legend_id: &str, epic_number: usize) -> String {
    format!("{legend_id}.{epic_number}.0")
}

fn legend_epic_land_agent_name(legend_id: &str, epic_number: usize) -> String {
    format!("{legend_id}.{epic_number}")
}

fn legend_epic_waits_on(legend_id: &str, epic_number: usize) -> Vec<String> {
    if epic_number <= 1 {
        vec![]
    } else {
        vec![legend_epic_land_agent_name(legend_id, epic_number - 1)]
    }
}

fn format_id_list<'a>(ids: impl Iterator<Item = &'a str>) -> String {
    format!(
        "[{}]",
        ids.map(|id| format!("'{id}'"))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bead::wire::DependencyWire;

    fn epic(id: &str) -> IssueWire {
        issue(id, IssueTypeWire::Plan, None)
    }

    fn legend(id: &str, epic_count: Option<i64>, design: &str) -> IssueWire {
        let mut issue = issue(id, IssueTypeWire::Plan, None);
        issue.tier = Some(BeadTierWire::Legend);
        issue.epic_count = epic_count;
        issue.design = design.to_string();
        issue
    }

    fn phase(id: &str, parent_id: &str) -> IssueWire {
        issue(id, IssueTypeWire::Phase, Some(parent_id))
    }

    fn issue(
        id: &str,
        issue_type: IssueTypeWire,
        parent_id: Option<&str>,
    ) -> IssueWire {
        let tier =
            (issue_type == IssueTypeWire::Plan).then_some(BeadTierWire::Epic);
        IssueWire {
            id: id.to_string(),
            title: id.to_string(),
            status: StatusWire::Open,
            issue_type,
            tier,
            parent_id: parent_id.map(str::to_string),
            owner: String::new(),
            assignee: String::new(),
            created_at: format!("2026-01-01T00:00:{id}Z"),
            created_by: String::new(),
            updated_at: String::new(),
            closed_at: None,
            close_reason: None,
            description: String::new(),
            notes: String::new(),
            design: String::new(),
            is_ready_to_work: false,
            epic_count: None,
            changespec_name: String::new(),
            changespec_bug_id: String::new(),
            dependencies: vec![],
        }
    }

    fn depends(issue: &mut IssueWire, blocker_id: &str) {
        issue.dependencies.push(DependencyWire {
            issue_id: issue.id.clone(),
            depends_on_id: blocker_id.to_string(),
            created_at: String::new(),
            created_by: String::new(),
        });
    }

    #[test]
    fn plans_diamond_dag_in_waves() {
        let p1 = phase("p1", "e1");
        let mut p2 = phase("p2", "e1");
        let mut p3 = phase("p3", "e1");
        let mut p4 = phase("p4", "e1");
        depends(&mut p2, "p1");
        depends(&mut p3, "p1");
        depends(&mut p4, "p2");
        depends(&mut p4, "p3");

        let plan = build_epic_work_plan_from_issues(
            vec![epic("e1"), p1, p2, p3, p4],
            "e1",
        )
        .unwrap();

        assert_eq!(plan.waves.len(), 3);
        assert_eq!(plan.waves[0][0].bead_id, "p1");
        assert_eq!(
            plan.waves[1].iter().map(|a| &a.bead_id).collect::<Vec<_>>(),
            vec!["p2", "p3"]
        );
        assert_eq!(plan.waves[2][0].waits_on, vec!["p2", "p3"]);
        assert_eq!(plan.land_agent_name, "e1");
        assert_eq!(plan.land_waits_on, vec!["p4"]);
    }

    #[test]
    fn rejects_open_out_of_epic_blocker() {
        let mut p1 = phase("p1", "e1");
        depends(&mut p1, "ext");

        let err = build_epic_work_plan_from_issues(
            vec![epic("e1"), epic("e2"), phase("ext", "e2"), p1],
            "e1",
        )
        .unwrap_err();

        assert_eq!(err.kind, "cross_epic_blocker");
        assert!(err.message.contains("'ext'"));
    }

    #[test]
    fn detects_cycles() {
        let mut p1 = phase("p1", "e1");
        let mut p2 = phase("p2", "e1");
        depends(&mut p1, "p2");
        depends(&mut p2, "p1");

        let err =
            build_epic_work_plan_from_issues(vec![epic("e1"), p1, p2], "e1")
                .unwrap_err();

        assert_eq!(err.kind, "cycle");
    }

    #[test]
    fn plans_legend_epics_in_linear_wait_chain() {
        let plan = build_legend_work_plan_from_issues(
            vec![legend("l1", Some(3), "sdd/legends/l1.md")],
            "l1",
        )
        .unwrap();

        assert_eq!(plan.legend_id, "l1");
        assert_eq!(plan.plan_file, "sdd/legends/l1.md");
        assert_eq!(plan.assignments.len(), 3);
        assert_eq!(plan.assignments[0].epic_number, 1);
        assert_eq!(plan.assignments[0].agent_name, "l1.1.0");
        assert_eq!(plan.assignments[0].waits_on, Vec::<String>::new());
        assert_eq!(plan.assignments[1].agent_name, "l1.2.0");
        assert_eq!(plan.assignments[1].waits_on, vec!["l1.1"]);
        assert_eq!(plan.assignments[2].agent_name, "l1.3.0");
        assert_eq!(plan.assignments[2].waits_on, vec!["l1.2"]);
    }

    #[test]
    fn rejects_missing_legend_epic_count() {
        let err = build_legend_work_plan_from_issues(
            vec![legend("l1", None, "legend.md")],
            "l1",
        )
        .unwrap_err();

        assert_eq!(err.kind, "validation");
        assert!(err.message.contains("missing epic_count"));
    }

    #[test]
    fn rejects_non_positive_legend_epic_count() {
        let err = build_legend_work_plan_from_issues(
            vec![legend("l1", Some(0), "legend.md")],
            "l1",
        )
        .unwrap_err();

        assert_eq!(err.kind, "validation");
        assert!(err.message.contains("invalid epic_count 0"));
    }

    #[test]
    fn rejects_legend_without_design_path() {
        let err = build_legend_work_plan_from_issues(
            vec![legend("l1", Some(1), "")],
            "l1",
        )
        .unwrap_err();

        assert_eq!(err.kind, "validation");
        assert!(err.message.contains("missing a design/plan file"));
    }

    #[test]
    fn rejects_non_legend_plan_for_legend_work() {
        let err = build_legend_work_plan_from_issues(vec![epic("e1")], "e1")
            .unwrap_err();

        assert_eq!(err.kind, "validation");
        assert!(err.message.contains("not a legend bead"));
    }

    #[test]
    fn rejects_phase_for_legend_work() {
        let err = build_legend_work_plan_from_issues(
            vec![epic("e1"), phase("p1", "e1")],
            "p1",
        )
        .unwrap_err();

        assert_eq!(err.kind, "validation");
        assert!(err.message.contains("not a plan/legend bead"));
    }
}
