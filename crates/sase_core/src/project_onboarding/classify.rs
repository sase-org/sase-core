//! Candidate classification: merge identities, hide, group, and rank.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use unicode_casefold::UnicodeCaseFold;

use super::wire::{
    ProjectCandidateGroupWire, ProjectCandidateHiddenCountWire,
    ProjectCandidateInputWire, ProjectCandidateInventoryWire,
    ProjectCandidateJournalWire, ProjectCandidateListWire,
    ProjectCandidateProjectWire, ProjectCandidateRowWire, GROUP_DISABLED,
    GROUP_LINKED, GROUP_LOCAL, GROUP_RESUME, GROUP_SUGGESTED, HIDDEN_ARCHIVED,
    HIDDEN_ENABLED_PROJECT, HIDDEN_FORK, HIDDEN_NUMBERED_COPY, HIDDEN_SIDECAR,
    HIDDEN_THIRD_PARTY_LOCAL, INVENTORY_LINKED, INVENTORY_SIDECAR,
    JOURNAL_FAILED, JOURNAL_RUNNING, PROJECT_ONBOARDING_WIRE_SCHEMA_VERSION,
    PROJECT_STATE_DISABLED, PROJECT_STATE_ENABLED, PROJECT_STATE_SIBLING,
};
use super::ProjectOnboardingError;

const GROUP_ORDER: [&str; 5] = [
    GROUP_RESUME,
    GROUP_LOCAL,
    GROUP_SUGGESTED,
    GROUP_LINKED,
    GROUP_DISABLED,
];

const HIDDEN_ORDER: [&str; 6] = [
    HIDDEN_ENABLED_PROJECT,
    HIDDEN_SIDECAR,
    HIDDEN_ARCHIVED,
    HIDDEN_FORK,
    HIDDEN_NUMBERED_COPY,
    HIDDEN_THIRD_PARTY_LOCAL,
];

struct RankedRow {
    row: ProjectCandidateRowWire,
    open_count: u64,
    last_open_at: String,
}

#[derive(Clone, Default)]
struct MergedCandidate {
    identity_key: String,
    repo_ref: String,
    display: String,
    name: String,
    owner: String,
    description: String,
    visibility: String,
    pushed_at: String,
    is_empty: Option<bool>,
    viewer_permission: String,
    local_path: String,
    numbered_copy: bool,
    is_fork: bool,
    is_archived: bool,
    has_local: bool,
}

/// Classify remote, local, project, inventory, and journal facts into
/// grouped rows plus search-only hidden rows.
pub fn classify_project_candidates(
    input: &ProjectCandidateInputWire,
) -> Result<ProjectCandidateListWire, ProjectOnboardingError> {
    ProjectOnboardingError::check_schema(input.schema_version)?;

    let sidecar_roles = fold_set(&input.sidecar_roles);
    let owned_namespaces = fold_set(&input.owned_namespaces);
    let projects_by_identity = index_projects(&input.projects);
    let inventory_by_identity = index_inventory(&input.inventory);
    let journals_by_identity = index_journals(&input.journals);

    let mut merged = BTreeMap::<String, MergedCandidate>::new();
    for remote in &input.remote {
        let key = fold_key(&remote.identity_key);
        if key.is_empty() {
            continue;
        }
        let entry = merged.entry(key).or_default();
        merge_remote(entry, remote);
    }
    for local in &input.local {
        let key = fold_key(&local.identity_key);
        if key.is_empty() {
            continue;
        }
        let entry = merged.entry(key).or_default();
        merge_local(entry, local);
    }
    seed_from_journals(&mut merged, &input.journals);
    seed_from_projects(&mut merged, &input.projects);
    seed_from_inventory(&mut merged, &input.inventory);

    let mut grouped: HashMap<String, Vec<RankedRow>> = HashMap::new();
    let mut search_only = Vec::new();
    let mut hidden_counts: BTreeMap<String, u64> = BTreeMap::new();

    for (folded, candidate) in merged {
        let inventory = inventory_by_identity.get(&folded).cloned();
        let projects = projects_by_identity.get(&folded).cloned();
        let journal = journals_by_identity.get(&folded).cloned();
        let ranked = classify_one(
            candidate,
            inventory.as_deref().unwrap_or(&[]),
            projects.as_deref().unwrap_or(&[]),
            journal.as_ref(),
            &sidecar_roles,
            &owned_namespaces,
        );
        if ranked.row.hidden_reason.is_empty() {
            grouped
                .entry(ranked.row.group.clone())
                .or_default()
                .push(ranked);
        } else {
            *hidden_counts
                .entry(ranked.row.hidden_reason.clone())
                .or_insert(0) += 1;
            search_only.push(ranked);
        }
    }

    let groups = GROUP_ORDER
        .iter()
        .filter_map(|id| {
            let mut rows = grouped.remove(*id)?;
            if rows.is_empty() {
                return None;
            }
            sort_ranked(&mut rows);
            Some(ProjectCandidateGroupWire {
                id: (*id).to_string(),
                rows: rows.into_iter().map(|item| item.row).collect(),
            })
        })
        .collect();
    sort_ranked(&mut search_only);
    let search_only: Vec<ProjectCandidateRowWire> =
        search_only.into_iter().map(|item| item.row).collect();
    let hidden = HIDDEN_ORDER
        .iter()
        .filter_map(|reason| {
            let count = *hidden_counts.get(*reason)?;
            (count > 0).then(|| ProjectCandidateHiddenCountWire {
                reason: (*reason).to_string(),
                count,
            })
        })
        .collect();

    Ok(ProjectCandidateListWire {
        schema_version: PROJECT_ONBOARDING_WIRE_SCHEMA_VERSION,
        groups,
        search_only,
        hidden,
        namespaces: input.namespaces.clone(),
    })
}

fn classify_one(
    candidate: MergedCandidate,
    inventory: &[ProjectCandidateInventoryWire],
    projects: &[ProjectCandidateProjectWire],
    journal: Option<&ProjectCandidateJournalWire>,
    sidecar_roles: &HashSet<String>,
    owned_namespaces: &HashSet<String>,
) -> RankedRow {
    let linked_by = linked_by_keys(inventory);
    let (open_count, last_open_at) = inventory_rank(inventory);
    let project = pick_project(projects);
    let (group, hidden_reason, project_key, project_state, journal_status) =
        classify_rules(
            &candidate,
            inventory,
            project,
            journal,
            sidecar_roles,
            owned_namespaces,
        );
    let mut row = ProjectCandidateRowWire {
        identity_key: candidate.identity_key,
        repo_ref: candidate.repo_ref,
        display: candidate.display,
        group,
        hidden_reason,
        description: candidate.description,
        visibility: candidate.visibility,
        pushed_at: candidate.pushed_at,
        is_empty: candidate.is_empty,
        viewer_permission: candidate.viewer_permission,
        local_path: candidate.local_path,
        project_key,
        project_state,
        linked_by,
        journal_status,
        stopped_at: journal
            .map(|item| item.stopped_at.clone())
            .unwrap_or_default(),
    };
    if row.display.is_empty() {
        row.display = row.identity_key.clone();
    }
    RankedRow {
        row,
        open_count,
        last_open_at,
    }
}

fn classify_rules(
    candidate: &MergedCandidate,
    inventory: &[ProjectCandidateInventoryWire],
    project: Option<&ProjectCandidateProjectWire>,
    journal: Option<&ProjectCandidateJournalWire>,
    sidecar_roles: &HashSet<String>,
    owned_namespaces: &HashSet<String>,
) -> (String, String, String, String, String) {
    if let Some(journal) = journal {
        return (
            GROUP_RESUME.to_string(),
            String::new(),
            project.map(|item| item.key.clone()).unwrap_or_default(),
            project_state_label(project),
            journal.status.clone(),
        );
    }
    if let Some(project) = project {
        let state = normalize_project_state(&project.state);
        if state == PROJECT_STATE_DISABLED {
            return (
                GROUP_DISABLED.to_string(),
                String::new(),
                project.key.clone(),
                PROJECT_STATE_DISABLED.to_string(),
                String::new(),
            );
        }
        return (
            String::new(),
            HIDDEN_ENABLED_PROJECT.to_string(),
            project.key.clone(),
            if state == PROJECT_STATE_SIBLING {
                PROJECT_STATE_SIBLING.to_string()
            } else {
                PROJECT_STATE_ENABLED.to_string()
            },
            String::new(),
        );
    }
    if inventory.iter().any(|item| item.kind == INVENTORY_SIDECAR)
        || is_sidecar_name(&candidate.name, sidecar_roles)
    {
        return (
            String::new(),
            HIDDEN_SIDECAR.to_string(),
            String::new(),
            String::new(),
            String::new(),
        );
    }
    if candidate.is_archived {
        return (
            String::new(),
            HIDDEN_ARCHIVED.to_string(),
            String::new(),
            String::new(),
            String::new(),
        );
    }
    if candidate.is_fork {
        return (
            String::new(),
            HIDDEN_FORK.to_string(),
            String::new(),
            String::new(),
            String::new(),
        );
    }
    if candidate.has_local {
        if candidate.numbered_copy {
            return (
                String::new(),
                HIDDEN_NUMBERED_COPY.to_string(),
                String::new(),
                String::new(),
                String::new(),
            );
        }
        if !candidate.owner.is_empty()
            && !owned_namespaces.contains(&fold_key(&candidate.owner))
        {
            return (
                String::new(),
                HIDDEN_THIRD_PARTY_LOCAL.to_string(),
                String::new(),
                String::new(),
                String::new(),
            );
        }
        return (
            GROUP_LOCAL.to_string(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
        );
    }
    if inventory.iter().any(|item| item.kind == INVENTORY_LINKED) {
        return (
            GROUP_LINKED.to_string(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
        );
    }
    (
        GROUP_SUGGESTED.to_string(),
        String::new(),
        String::new(),
        String::new(),
        String::new(),
    )
}

fn merge_remote(
    entry: &mut MergedCandidate,
    remote: &super::wire::ProjectCandidateRemoteWire,
) {
    if entry.identity_key.is_empty() {
        entry.identity_key = remote.identity_key.clone();
    }
    if entry.repo_ref.is_empty() {
        entry.repo_ref = remote.repo_ref.clone();
    }
    if entry.display.is_empty() {
        entry.display = remote.display.clone();
    }
    if !remote.name.is_empty() {
        entry.name = remote.name.clone();
    }
    if !remote.description.is_empty() {
        entry.description = remote.description.clone();
    }
    if !remote.visibility.is_empty() {
        entry.visibility = remote.visibility.clone();
    }
    if !remote.pushed_at.is_empty() {
        entry.pushed_at = remote.pushed_at.clone();
    }
    if remote.is_empty.is_some() {
        entry.is_empty = remote.is_empty;
    }
    if !remote.viewer_permission.is_empty() {
        entry.viewer_permission = remote.viewer_permission.clone();
    }
    entry.is_fork |= remote.is_fork;
    entry.is_archived |= remote.is_archived;
}

fn merge_local(
    entry: &mut MergedCandidate,
    local: &super::wire::ProjectCandidateLocalWire,
) {
    if entry.identity_key.is_empty() {
        entry.identity_key = local.identity_key.clone();
    }
    if !local.repo_ref.is_empty() {
        entry.repo_ref = local.repo_ref.clone();
    }
    if !local.display.is_empty() {
        entry.display = local.display.clone();
    }
    if !local.name.is_empty() && entry.name.is_empty() {
        entry.name = local.name.clone();
    }
    if !local.owner.is_empty() {
        entry.owner = local.owner.clone();
    }
    if !local.path.is_empty() {
        entry.local_path = local.path.clone();
    }
    entry.numbered_copy |= local.numbered_copy;
    entry.has_local = true;
}

fn seed_from_journals(
    merged: &mut BTreeMap<String, MergedCandidate>,
    journals: &[ProjectCandidateJournalWire],
) {
    for journal in journals {
        if !is_live_journal(&journal.status) {
            continue;
        }
        let key = fold_key(&journal.identity_key);
        if key.is_empty() {
            continue;
        }
        let entry = merged.entry(key).or_default();
        if entry.identity_key.is_empty() {
            entry.identity_key = journal.identity_key.clone();
        }
        if entry.repo_ref.is_empty() {
            entry.repo_ref = journal.repo_ref.clone();
        }
        if entry.display.is_empty() {
            entry.display = journal.display.clone();
        }
    }
}

fn seed_from_projects(
    merged: &mut BTreeMap<String, MergedCandidate>,
    projects: &[ProjectCandidateProjectWire],
) {
    for project in projects {
        for identity in &project.identity_keys {
            let key = fold_key(identity);
            if key.is_empty() {
                continue;
            }
            let entry = merged.entry(key).or_default();
            if entry.identity_key.is_empty() {
                entry.identity_key = identity.clone();
            }
            if entry.display.is_empty() {
                entry.display = if project.name.is_empty() {
                    identity.clone()
                } else {
                    project.name.clone()
                };
            }
        }
    }
}

fn seed_from_inventory(
    merged: &mut BTreeMap<String, MergedCandidate>,
    inventory: &[ProjectCandidateInventoryWire],
) {
    for item in inventory {
        let key = fold_key(&item.identity_key);
        if key.is_empty() {
            continue;
        }
        let entry = merged.entry(key).or_default();
        if entry.identity_key.is_empty() {
            entry.identity_key = item.identity_key.clone();
        }
    }
}

fn index_projects(
    projects: &[ProjectCandidateProjectWire],
) -> HashMap<String, Vec<ProjectCandidateProjectWire>> {
    let mut index = HashMap::<String, Vec<ProjectCandidateProjectWire>>::new();
    for project in projects {
        let mut seen = HashSet::new();
        for identity in &project.identity_keys {
            let key = fold_key(identity);
            if key.is_empty() || !seen.insert(key.clone()) {
                continue;
            }
            index.entry(key).or_default().push(project.clone());
        }
    }
    index
}

fn index_inventory(
    inventory: &[ProjectCandidateInventoryWire],
) -> HashMap<String, Vec<ProjectCandidateInventoryWire>> {
    let mut index =
        HashMap::<String, Vec<ProjectCandidateInventoryWire>>::new();
    for item in inventory {
        let key = fold_key(&item.identity_key);
        if key.is_empty() {
            continue;
        }
        index.entry(key).or_default().push(item.clone());
    }
    index
}

fn index_journals(
    journals: &[ProjectCandidateJournalWire],
) -> HashMap<String, ProjectCandidateJournalWire> {
    let mut index = HashMap::<String, ProjectCandidateJournalWire>::new();
    for journal in journals {
        if !is_live_journal(&journal.status) {
            continue;
        }
        let key = fold_key(&journal.identity_key);
        if key.is_empty() {
            continue;
        }
        match index.get(&key) {
            Some(existing)
                if existing.updated_at >= journal.updated_at
                    && !(existing.updated_at.is_empty()
                        && !journal.updated_at.is_empty()) => {}
            _ => {
                index.insert(key, journal.clone());
            }
        }
    }
    index
}

fn pick_project(
    projects: &[ProjectCandidateProjectWire],
) -> Option<&ProjectCandidateProjectWire> {
    if projects.is_empty() {
        return None;
    }
    projects
        .iter()
        .find(|project| {
            normalize_project_state(&project.state) == PROJECT_STATE_DISABLED
        })
        .or_else(|| {
            projects.iter().find(|project| {
                normalize_project_state(&project.state) == PROJECT_STATE_SIBLING
            })
        })
        .or(projects.first())
}

fn linked_by_keys(inventory: &[ProjectCandidateInventoryWire]) -> Vec<String> {
    let mut keys = BTreeSet::new();
    for item in inventory {
        if item.kind == INVENTORY_LINKED && !item.project_key.is_empty() {
            keys.insert(item.project_key.clone());
        }
    }
    keys.into_iter().collect()
}

fn inventory_rank(
    inventory: &[ProjectCandidateInventoryWire],
) -> (u64, String) {
    let mut open_count = 0_u64;
    let mut last_open_at = String::new();
    for item in inventory {
        open_count = open_count.max(item.open_count);
        if item.last_open_at > last_open_at {
            last_open_at = item.last_open_at.clone();
        }
    }
    (open_count, last_open_at)
}

fn sort_ranked(rows: &mut [RankedRow]) {
    rows.sort_by(|left, right| {
        right
            .open_count
            .cmp(&left.open_count)
            .then_with(|| right.last_open_at.cmp(&left.last_open_at))
            .then_with(|| right.row.pushed_at.cmp(&left.row.pushed_at))
            .then_with(|| {
                fold_key(&left.row.display).cmp(&fold_key(&right.row.display))
            })
    });
}

fn is_live_journal(status: &str) -> bool {
    status == JOURNAL_RUNNING || status == JOURNAL_FAILED
}

fn is_sidecar_name(name: &str, roles: &HashSet<String>) -> bool {
    let Some((base, role)) = name.rsplit_once("--") else {
        return false;
    };
    !base.is_empty() && roles.contains(&fold_key(role))
}

fn normalize_project_state(state: &str) -> &'static str {
    match state.trim() {
        PROJECT_STATE_DISABLED => PROJECT_STATE_DISABLED,
        PROJECT_STATE_SIBLING => PROJECT_STATE_SIBLING,
        _ => PROJECT_STATE_ENABLED,
    }
}

fn project_state_label(
    project: Option<&ProjectCandidateProjectWire>,
) -> String {
    project
        .map(|item| normalize_project_state(&item.state).to_string())
        .unwrap_or_default()
}

fn fold_key(value: &str) -> String {
    value.trim().case_fold().collect()
}

fn fold_set(values: &[String]) -> HashSet<String> {
    values
        .iter()
        .map(|value| fold_key(value))
        .filter(|value| !value.is_empty())
        .collect()
}
