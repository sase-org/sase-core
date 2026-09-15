//! Git object-sharing alternates policy for managed SASE workspaces.
//!
//! Python owns host-coupled Git subprocesses and project locks.  This module
//! owns the deterministic policy over an already-observed alternates file:
//! resolving relative entries the way Git does, deciding which line is
//! SASE-owned, and planning rewrites without discarding foreign alternates.

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::path::{Component, Path, PathBuf};
use thiserror::Error;

pub const GIT_OBJECT_SHARING_WIRE_SCHEMA_VERSION: u32 = 3;

pub const GIT_OBJECT_SHARING_ACTION_NONE: &str = "none";
pub const GIT_OBJECT_SHARING_ACTION_WRITE: &str = "write";
pub const GIT_OBJECT_SHARING_ACTION_DELETE: &str = "delete";
pub const GIT_OBJECT_SHARING_ACTION_FAIL: &str = "fail";

pub const GIT_OBJECT_SHARING_CONTEXT_NEW_CHECKOUT: &str = "new_checkout";
pub const GIT_OBJECT_SHARING_CONTEXT_EXISTING_REUSE: &str = "existing_reuse";
pub const GIT_OBJECT_SHARING_CONTEXT_MAINTENANCE_REPAIR: &str =
    "maintenance_repair";
pub const GIT_OBJECT_SHARING_CONTEXT_MAINTENANCE_COMPACT: &str =
    "maintenance_compact";
pub const GIT_OBJECT_SHARING_CONTEXT_MAINTENANCE_DISSOCIATE: &str =
    "maintenance_dissociate";

#[derive(Debug, Error)]
pub enum GitObjectSharingError {
    #[error(
        "git object sharing requires schema_version {expected}, got {actual}"
    )]
    Schema { expected: u32, actual: u32 },
    #[error("unsupported git object sharing operation: {0}")]
    Operation(String),
    #[error("unsupported git object sharing mutation context: {0}")]
    MutationContext(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitObjectSharingPlanRequestWire {
    pub schema_version: u32,
    pub operation: String,
    pub checkout_dir: String,
    pub object_dir: String,
    pub alternates_file: String,
    pub primary_checkout_dir: String,
    pub primary_object_dir: String,
    #[serde(default)]
    pub alternates: Vec<String>,
    #[serde(default)]
    pub config_enabled: bool,
    #[serde(default)]
    pub config_primary_objects: Option<String>,
    #[serde(default)]
    pub mutation_context: Option<String>,
    #[serde(default)]
    pub checkout_clean: Option<bool>,
    #[serde(default)]
    pub fresh_claim_status: Option<String>,
    #[serde(default)]
    pub fresh_occupant_status: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitObjectSharingPlanWire {
    pub schema_version: u32,
    pub action: String,
    pub status: String,
    pub checkout_dir: String,
    pub object_dir: String,
    pub alternates_file: String,
    pub expected_object_dir: String,
    pub raw_alternates: Vec<String>,
    pub alternates: Vec<String>,
    pub owned_alternates: Vec<String>,
    pub foreign_alternates: Vec<String>,
    pub write_alternates: Option<Vec<String>>,
    pub dependency_mutation: bool,
    pub sase_owned: bool,
    pub detail: String,
}

#[derive(Debug, Clone)]
struct ClassifiedAlternates {
    request: GitObjectSharingPlanRequestWire,
    raw: Vec<String>,
    resolved: Vec<String>,
    owned_indexes: BTreeSet<usize>,
    foreign_indexes: BTreeSet<usize>,
    status: String,
    detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MutationContext {
    NewCheckout,
    ExistingReuse,
    MaintenanceRepair,
    MaintenanceCompact,
    MaintenanceDissociate,
}

pub fn plan_git_object_sharing(
    request: &GitObjectSharingPlanRequestWire,
) -> Result<GitObjectSharingPlanWire, GitObjectSharingError> {
    if request.schema_version != GIT_OBJECT_SHARING_WIRE_SCHEMA_VERSION {
        return Err(GitObjectSharingError::Schema {
            expected: GIT_OBJECT_SHARING_WIRE_SCHEMA_VERSION,
            actual: request.schema_version,
        });
    }

    let classified = classify(request);
    match request.operation.trim() {
        "classify" => Ok(classified.into_wire(
            GIT_OBJECT_SHARING_ACTION_NONE,
            None,
            None,
        )),
        "install" => plan_install(classified),
        "remove" => plan_remove(classified),
        other => Err(GitObjectSharingError::Operation(other.to_string())),
    }
}

fn classify(request: &GitObjectSharingPlanRequestWire) -> ClassifiedAlternates {
    let raw = clean_alternates(&request.alternates);
    let object_dir = normalize_path(Path::new(&request.object_dir));
    let expected = normalize_path(Path::new(&request.primary_object_dir))
        .to_string_lossy()
        .into_owned();
    let stored_primary =
        clean_option(&request.config_primary_objects).map(|value| {
            normalize_path(Path::new(value))
                .to_string_lossy()
                .into_owned()
        });
    let resolved = raw
        .iter()
        .map(|line| resolve_alternate(line, &object_dir))
        .map(|path| path.to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    let owned_indexes = owned_alternate_indexes(
        &raw,
        &resolved,
        &expected,
        stored_primary.as_deref(),
        request.config_enabled,
    );
    let foreign_indexes = (0..raw.len())
        .filter(|index| !owned_indexes.contains(index))
        .collect::<BTreeSet<_>>();
    let (status, detail) = classify_status(
        &raw,
        &resolved,
        &owned_indexes,
        &foreign_indexes,
        &expected,
    );

    ClassifiedAlternates {
        request: request.clone(),
        raw,
        resolved,
        owned_indexes,
        foreign_indexes,
        status,
        detail,
    }
}

fn clean_alternates(values: &[String]) -> Vec<String> {
    values
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect()
}

fn clean_option(value: &Option<String>) -> Option<&str> {
    value
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn owned_alternate_indexes(
    raw: &[String],
    resolved: &[String],
    expected: &str,
    stored_primary: Option<&str>,
    config_enabled: bool,
) -> BTreeSet<usize> {
    let mut owned = BTreeSet::new();
    let expected_indexes = indexes_matching(resolved, expected);
    let stored_indexes = stored_primary
        .map(|stored| indexes_matching(resolved, stored))
        .unwrap_or_default();
    let config_marks_sase = config_enabled || stored_primary.is_some();

    if config_marks_sase {
        if !stored_indexes.is_empty() {
            owned.extend(stored_indexes);
        } else if !expected_indexes.is_empty() {
            owned.extend(expected_indexes);
        } else if raw.len() == 1 {
            owned.insert(0);
        }
    } else if raw.len() == 1 && expected_indexes.len() == 1 {
        owned.insert(0);
    }
    owned
}

fn indexes_matching(values: &[String], expected: &str) -> BTreeSet<usize> {
    values
        .iter()
        .enumerate()
        .filter_map(|(index, value)| (value == expected).then_some(index))
        .collect()
}

fn classify_status(
    raw: &[String],
    resolved: &[String],
    owned_indexes: &BTreeSet<usize>,
    foreign_indexes: &BTreeSet<usize>,
    expected: &str,
) -> (String, String) {
    if raw.is_empty() {
        return ("absent".to_string(), String::new());
    }
    if owned_indexes.is_empty() {
        if let Some(missing) = first_missing(resolved, foreign_indexes) {
            return (
                "broken".to_string(),
                format!("missing non-SASE alternate object dir: {missing}"),
            );
        }
        return (
            "unexpected".to_string(),
            "alternate is not SASE-managed".to_string(),
        );
    }
    if owned_indexes
        .iter()
        .any(|index| resolved[*index] == expected)
    {
        return (
            "expected".to_string(),
            expected_detail(foreign_indexes.len()),
        );
    }
    if let Some(missing) = first_missing(resolved, owned_indexes) {
        return (
            "broken".to_string(),
            format!("missing SASE-owned alternate object dir: {missing}"),
        );
    }
    (
        "stale".to_string(),
        "SASE-owned alternate points at a different object dir".to_string(),
    )
}

fn expected_detail(foreign_count: usize) -> String {
    if foreign_count == 0 {
        String::new()
    } else {
        format!("SASE alternate is expected; {foreign_count} foreign alternate(s) preserved")
    }
}

fn first_missing(
    resolved: &[String],
    indexes: &BTreeSet<usize>,
) -> Option<String> {
    indexes
        .iter()
        .map(|index| &resolved[*index])
        .find(|path| !Path::new(path.as_str()).is_dir())
        .cloned()
}

fn plan_install(
    classified: ClassifiedAlternates,
) -> Result<GitObjectSharingPlanWire, GitObjectSharingError> {
    let context = mutation_context(&classified.request)?;
    if context == MutationContext::ExistingReuse {
        return Ok(plan_existing_reuse_install(classified));
    }
    if let Some(detail) =
        guarded_maintenance_refusal(&classified.request, context)
    {
        return Ok(classified.into_wire(
            GIT_OBJECT_SHARING_ACTION_FAIL,
            None,
            Some(detail),
        ));
    }
    if classified.status == "unexpected"
        || (classified.status == "broken"
            && classified.owned_indexes.is_empty())
    {
        let detail = format!(
            "refusing to overwrite non-SASE alternate: {}",
            classified.detail
        );
        return Ok(classified.into_wire(
            GIT_OBJECT_SHARING_ACTION_FAIL,
            None,
            Some(detail),
        ));
    }
    let lines = install_lines(&classified);
    let dependency_mutation = lines != classified.raw;
    if dependency_mutation {
        Ok(classified.into_wire(
            GIT_OBJECT_SHARING_ACTION_WRITE,
            Some(lines),
            None,
        ))
    } else {
        Ok(classified.into_wire(GIT_OBJECT_SHARING_ACTION_NONE, None, None))
    }
}

fn plan_remove(
    classified: ClassifiedAlternates,
) -> Result<GitObjectSharingPlanWire, GitObjectSharingError> {
    let context = mutation_context(&classified.request)?;
    if context == MutationContext::ExistingReuse
        && !classified.owned_indexes.is_empty()
    {
        return Ok(classified.into_wire_with_mutation(
            GIT_OBJECT_SHARING_ACTION_FAIL,
            None,
            Some(
                "refusing to remove an existing checkout's Git object dependency during ordinary reuse"
                    .to_string(),
            ),
            false,
        ));
    }
    if let Some(detail) =
        guarded_maintenance_refusal(&classified.request, context)
    {
        return Ok(classified.into_wire(
            GIT_OBJECT_SHARING_ACTION_FAIL,
            None,
            Some(detail),
        ));
    }
    if classified.owned_indexes.is_empty() {
        return Ok(classified.into_wire(
            GIT_OBJECT_SHARING_ACTION_NONE,
            None,
            None,
        ));
    }
    let lines = classified
        .raw
        .iter()
        .enumerate()
        .filter_map(|(index, line)| {
            (!classified.owned_indexes.contains(&index)).then_some(line.clone())
        })
        .collect::<Vec<_>>();
    let action = if lines.is_empty() {
        GIT_OBJECT_SHARING_ACTION_DELETE
    } else {
        GIT_OBJECT_SHARING_ACTION_WRITE
    };
    Ok(classified.into_wire_with_mutation(action, Some(lines), None, true))
}

fn mutation_context(
    request: &GitObjectSharingPlanRequestWire,
) -> Result<MutationContext, GitObjectSharingError> {
    match clean_option(&request.mutation_context) {
        Some(GIT_OBJECT_SHARING_CONTEXT_NEW_CHECKOUT) => {
            Ok(MutationContext::NewCheckout)
        }
        Some(GIT_OBJECT_SHARING_CONTEXT_EXISTING_REUSE)
        | Some("existing_checkout") => Ok(MutationContext::ExistingReuse),
        Some(GIT_OBJECT_SHARING_CONTEXT_MAINTENANCE_REPAIR)
        | Some("repair") => Ok(MutationContext::MaintenanceRepair),
        Some(GIT_OBJECT_SHARING_CONTEXT_MAINTENANCE_COMPACT) => {
            Ok(MutationContext::MaintenanceCompact)
        }
        Some(GIT_OBJECT_SHARING_CONTEXT_MAINTENANCE_DISSOCIATE) => {
            Ok(MutationContext::MaintenanceDissociate)
        }
        None => Err(GitObjectSharingError::MutationContext(
            "missing".to_string(),
        )),
        Some(other) => {
            Err(GitObjectSharingError::MutationContext(other.to_string()))
        }
    }
}

fn plan_existing_reuse_install(
    classified: ClassifiedAlternates,
) -> GitObjectSharingPlanWire {
    let lines = install_lines(&classified);
    if lines == classified.raw {
        return classified.into_wire(
            GIT_OBJECT_SHARING_ACTION_NONE,
            None,
            None,
        );
    }
    if classified.status == "broken" {
        return classified.into_wire_with_mutation(
            GIT_OBJECT_SHARING_ACTION_FAIL,
            None,
            Some(
                "existing checkout Git object dependency is broken; left unchanged for explicit maintenance repair"
                    .to_string(),
            ),
            false,
        );
    }
    let detail = match classified.status.as_str() {
        "absent" => {
            "existing checkout has no Git object dependency; left unchanged"
        }
        "stale" => {
            "existing checkout Git object dependency differs from the configured primary; usable dependency preserved"
        }
        "unexpected" => {
            "existing checkout non-SASE Git object dependency preserved"
        }
        _ => "existing checkout Git object dependency preserved",
    };
    let mut wire = classified.into_wire_with_mutation(
        GIT_OBJECT_SHARING_ACTION_NONE,
        None,
        Some(detail.to_string()),
        false,
    );
    if wire.status == "stale" {
        wire.status = "preserved".to_string();
    }
    wire
}

fn guarded_maintenance_refusal(
    request: &GitObjectSharingPlanRequestWire,
    context: MutationContext,
) -> Option<String> {
    if !matches!(
        context,
        MutationContext::MaintenanceRepair
            | MutationContext::MaintenanceCompact
            | MutationContext::MaintenanceDissociate
    ) {
        return None;
    }
    if let Some(detail) =
        fresh_observation_refusal(&request.fresh_claim_status, "claim")
    {
        return Some(detail);
    }
    if let Some(detail) =
        fresh_observation_refusal(&request.fresh_occupant_status, "occupant")
    {
        return Some(detail);
    }
    if context == MutationContext::MaintenanceCompact
        && request.checkout_clean != Some(true)
    {
        return Some(
            "refusing maintenance compaction without a successful clean status"
                .to_string(),
        );
    }
    None
}

fn fresh_observation_refusal(
    value: &Option<String>,
    label: &str,
) -> Option<String> {
    match clean_option(value) {
        Some("clear") => None,
        Some("present" | "positive" | "occupied" | "active") => Some(format!(
            "fresh {label} observation is positive; refusing mutation"
        )),
        Some(_) | None => Some(format!(
            "fresh {label} observation is unavailable; refusing mutation"
        )),
    }
}

fn install_lines(classified: &ClassifiedAlternates) -> Vec<String> {
    let expected = normalized_expected(&classified.request);
    let retained_has_expected = classified
        .foreign_indexes
        .iter()
        .any(|index| classified.resolved[*index] == expected);
    let mut inserted = false;
    let mut lines = Vec::new();
    for (index, line) in classified.raw.iter().enumerate() {
        if classified.owned_indexes.contains(&index) {
            if !inserted && !retained_has_expected {
                lines.push(classified.request.primary_object_dir.clone());
                inserted = true;
            }
        } else {
            lines.push(line.clone());
        }
    }
    if !inserted && !retained_has_expected {
        lines.push(classified.request.primary_object_dir.clone());
    }
    lines
}

fn normalized_expected(request: &GitObjectSharingPlanRequestWire) -> String {
    normalize_path(Path::new(&request.primary_object_dir))
        .to_string_lossy()
        .into_owned()
}

fn resolve_alternate(line: &str, object_dir: &Path) -> PathBuf {
    let path = Path::new(line);
    if path.is_absolute() {
        normalize_path(path)
    } else {
        normalize_path(&object_dir.join(path))
    }
}

fn normalize_path(path: &Path) -> PathBuf {
    if let Ok(canonical) = path.canonicalize() {
        return canonical;
    }
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() {
                    normalized.push("..");
                }
            }
            Component::Normal(value) => normalized.push(value),
        }
    }
    normalized
}

impl ClassifiedAlternates {
    fn into_wire(
        self,
        action: &str,
        write_alternates: Option<Vec<String>>,
        detail_override: Option<String>,
    ) -> GitObjectSharingPlanWire {
        self.into_wire_with_mutation(
            action,
            write_alternates,
            detail_override,
            action != GIT_OBJECT_SHARING_ACTION_NONE
                && action != GIT_OBJECT_SHARING_ACTION_FAIL,
        )
    }

    fn into_wire_with_mutation(
        self,
        action: &str,
        write_alternates: Option<Vec<String>>,
        detail_override: Option<String>,
        dependency_mutation: bool,
    ) -> GitObjectSharingPlanWire {
        let owned_alternates = self
            .owned_indexes
            .iter()
            .map(|index| self.resolved[*index].clone())
            .collect::<Vec<_>>();
        let foreign_alternates = self
            .foreign_indexes
            .iter()
            .map(|index| self.resolved[*index].clone())
            .collect::<Vec<_>>();
        GitObjectSharingPlanWire {
            schema_version: GIT_OBJECT_SHARING_WIRE_SCHEMA_VERSION,
            action: action.to_string(),
            status: self.status,
            checkout_dir: self.request.checkout_dir,
            object_dir: self.request.object_dir,
            alternates_file: self.request.alternates_file,
            expected_object_dir: self.request.primary_object_dir,
            raw_alternates: self.raw,
            alternates: self.resolved,
            owned_alternates,
            foreign_alternates,
            write_alternates,
            dependency_mutation,
            sase_owned: !self.owned_indexes.is_empty(),
            detail: detail_override.unwrap_or(self.detail),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn request(
        object_dir: &Path,
        primary_object_dir: &Path,
    ) -> GitObjectSharingPlanRequestWire {
        GitObjectSharingPlanRequestWire {
            schema_version: GIT_OBJECT_SHARING_WIRE_SCHEMA_VERSION,
            operation: "classify".to_string(),
            checkout_dir: "/work/repo_10".to_string(),
            object_dir: object_dir.to_string_lossy().into_owned(),
            alternates_file: object_dir
                .join("info/alternates")
                .to_string_lossy()
                .into_owned(),
            primary_checkout_dir: "/work/repo".to_string(),
            primary_object_dir: primary_object_dir
                .to_string_lossy()
                .into_owned(),
            alternates: Vec::new(),
            config_enabled: false,
            config_primary_objects: None,
            mutation_context: None,
            checkout_clean: None,
            fresh_claim_status: None,
            fresh_occupant_status: None,
        }
    }

    #[test]
    fn relative_alternates_resolve_from_object_database() {
        let temp = tempdir().unwrap();
        let objects = temp.path().join("borrower/.git/objects");
        let primary = temp.path().join("primary/.git/objects");
        std::fs::create_dir_all(&primary).unwrap();
        let mut req = request(&objects, &primary);
        req.alternates = vec!["../../../primary/.git/objects".to_string()];

        let plan = plan_git_object_sharing(&req).unwrap();

        assert_eq!(plan.status, "expected");
        assert_eq!(
            plan.alternates,
            vec![primary.to_string_lossy().into_owned()]
        );
    }

    #[test]
    fn primary_among_foreign_entries_without_config_is_not_claimed() {
        let temp = tempdir().unwrap();
        let objects = temp.path().join("borrower/.git/objects");
        let primary = temp.path().join("primary/.git/objects");
        let foreign = temp.path().join("foreign/objects");
        std::fs::create_dir_all(&primary).unwrap();
        std::fs::create_dir_all(&foreign).unwrap();
        let mut req = request(&objects, &primary);
        req.alternates = vec![
            foreign.to_string_lossy().into_owned(),
            primary.to_string_lossy().into_owned(),
        ];

        let plan = plan_git_object_sharing(&req).unwrap();

        assert_eq!(plan.status, "unexpected");
        assert!(!plan.sase_owned);
    }

    #[test]
    fn install_replaces_sase_line_and_preserves_foreign_lines() {
        let temp = tempdir().unwrap();
        let objects = temp.path().join("borrower/.git/objects");
        let old = temp.path().join("old/.git/objects");
        let primary = temp.path().join("primary/.git/objects");
        let foreign = temp.path().join("foreign/objects");
        for path in [&old, &primary, &foreign] {
            std::fs::create_dir_all(path).unwrap();
        }
        let mut req = request(&objects, &primary);
        req.operation = "install".to_string();
        req.mutation_context =
            Some(GIT_OBJECT_SHARING_CONTEXT_NEW_CHECKOUT.to_string());
        req.alternates = vec![
            foreign.to_string_lossy().into_owned(),
            old.to_string_lossy().into_owned(),
        ];
        req.config_enabled = true;
        req.config_primary_objects = Some(old.to_string_lossy().into_owned());

        let plan = plan_git_object_sharing(&req).unwrap();

        assert_eq!(plan.action, GIT_OBJECT_SHARING_ACTION_WRITE);
        assert!(plan.dependency_mutation);
        assert_eq!(
            plan.write_alternates.unwrap(),
            vec![
                foreign.to_string_lossy().into_owned(),
                primary.to_string_lossy().into_owned()
            ]
        );
    }

    #[test]
    fn remove_deletes_only_sase_owned_line() {
        let temp = tempdir().unwrap();
        let objects = temp.path().join("borrower/.git/objects");
        let primary = temp.path().join("primary/.git/objects");
        let foreign = temp.path().join("foreign/objects");
        for path in [&primary, &foreign] {
            std::fs::create_dir_all(path).unwrap();
        }
        let mut req = request(&objects, &primary);
        req.operation = "remove".to_string();
        req.mutation_context =
            Some(GIT_OBJECT_SHARING_CONTEXT_MAINTENANCE_DISSOCIATE.to_string());
        req.fresh_claim_status = Some("clear".to_string());
        req.fresh_occupant_status = Some("clear".to_string());
        req.alternates = vec![
            primary.to_string_lossy().into_owned(),
            foreign.to_string_lossy().into_owned(),
        ];
        req.config_enabled = true;
        req.config_primary_objects =
            Some(primary.to_string_lossy().into_owned());

        let plan = plan_git_object_sharing(&req).unwrap();

        assert_eq!(plan.action, GIT_OBJECT_SHARING_ACTION_WRITE);
        assert!(plan.dependency_mutation);
        assert_eq!(
            plan.write_alternates.unwrap(),
            vec![foreign.to_string_lossy().into_owned()]
        );
    }

    #[test]
    fn foreign_only_alternate_is_not_overwritten_by_install() {
        let temp = tempdir().unwrap();
        let objects = temp.path().join("borrower/.git/objects");
        let primary = temp.path().join("primary/.git/objects");
        let foreign = temp.path().join("foreign/objects");
        for path in [&primary, &foreign] {
            std::fs::create_dir_all(path).unwrap();
        }
        let mut req = request(&objects, &primary);
        req.operation = "install".to_string();
        req.mutation_context =
            Some(GIT_OBJECT_SHARING_CONTEXT_NEW_CHECKOUT.to_string());
        req.alternates = vec![foreign.to_string_lossy().into_owned()];

        let plan = plan_git_object_sharing(&req).unwrap();

        assert_eq!(plan.action, GIT_OBJECT_SHARING_ACTION_FAIL);
        assert_eq!(plan.status, "unexpected");
        assert!(plan.write_alternates.is_none());
        assert!(!plan.dependency_mutation);
    }

    #[test]
    fn install_noops_when_expected_dependency_is_unchanged() {
        let temp = tempdir().unwrap();
        let objects = temp.path().join("borrower/.git/objects");
        let primary = temp.path().join("primary/.git/objects");
        std::fs::create_dir_all(&primary).unwrap();
        let mut req = request(&objects, &primary);
        req.operation = "install".to_string();
        req.mutation_context =
            Some(GIT_OBJECT_SHARING_CONTEXT_EXISTING_REUSE.to_string());
        req.alternates = vec![primary.to_string_lossy().into_owned()];

        let plan = plan_git_object_sharing(&req).unwrap();

        assert_eq!(plan.action, GIT_OBJECT_SHARING_ACTION_NONE);
        assert!(!plan.dependency_mutation);
        assert!(plan.write_alternates.is_none());
    }

    #[test]
    fn existing_reuse_preserves_healthy_stale_dependency() {
        let temp = tempdir().unwrap();
        let objects = temp.path().join("borrower/.git/objects");
        let old = temp.path().join("old/.git/objects");
        let primary = temp.path().join("primary/.git/objects");
        for path in [&old, &primary] {
            std::fs::create_dir_all(path).unwrap();
        }
        let mut req = request(&objects, &primary);
        req.operation = "install".to_string();
        req.alternates = vec![old.to_string_lossy().into_owned()];
        req.config_enabled = true;
        req.config_primary_objects = Some(old.to_string_lossy().into_owned());
        req.mutation_context =
            Some(GIT_OBJECT_SHARING_CONTEXT_EXISTING_REUSE.to_string());

        let plan = plan_git_object_sharing(&req).unwrap();

        assert_eq!(plan.action, GIT_OBJECT_SHARING_ACTION_NONE);
        assert_eq!(plan.status, "preserved");
        assert!(!plan.dependency_mutation);
        assert!(plan.write_alternates.is_none());
        assert!(plan.detail.contains("usable dependency preserved"));
    }

    #[test]
    fn existing_reuse_refuses_broken_dependency_without_repair() {
        let temp = tempdir().unwrap();
        let objects = temp.path().join("borrower/.git/objects");
        let old = temp.path().join("old/.git/objects");
        let primary = temp.path().join("primary/.git/objects");
        std::fs::create_dir_all(&primary).unwrap();
        let mut req = request(&objects, &primary);
        req.operation = "install".to_string();
        req.alternates = vec![old.to_string_lossy().into_owned()];
        req.config_enabled = true;
        req.config_primary_objects = Some(old.to_string_lossy().into_owned());
        req.mutation_context =
            Some(GIT_OBJECT_SHARING_CONTEXT_EXISTING_REUSE.to_string());

        let plan = plan_git_object_sharing(&req).unwrap();

        assert_eq!(plan.action, GIT_OBJECT_SHARING_ACTION_FAIL);
        assert_eq!(plan.status, "broken");
        assert!(!plan.dependency_mutation);
        assert!(plan.detail.contains("maintenance repair"));
        assert!(plan.write_alternates.is_none());
    }

    #[test]
    fn maintenance_repair_allows_broken_dependency_without_clean_status() {
        let temp = tempdir().unwrap();
        let objects = temp.path().join("borrower/.git/objects");
        let old = temp.path().join("old/.git/objects");
        let primary = temp.path().join("primary/.git/objects");
        std::fs::create_dir_all(&primary).unwrap();
        let mut req = request(&objects, &primary);
        req.operation = "install".to_string();
        req.alternates = vec![old.to_string_lossy().into_owned()];
        req.config_enabled = true;
        req.config_primary_objects = Some(old.to_string_lossy().into_owned());
        req.mutation_context =
            Some(GIT_OBJECT_SHARING_CONTEXT_MAINTENANCE_REPAIR.to_string());
        req.fresh_claim_status = Some("clear".to_string());
        req.fresh_occupant_status = Some("clear".to_string());

        let plan = plan_git_object_sharing(&req).unwrap();

        assert_eq!(plan.action, GIT_OBJECT_SHARING_ACTION_WRITE);
        assert_eq!(plan.status, "broken");
        assert!(plan.dependency_mutation);
        assert_eq!(
            plan.write_alternates.unwrap(),
            vec![primary.to_string_lossy().into_owned()]
        );
    }

    #[test]
    fn maintenance_refuses_missing_fresh_observations() {
        let temp = tempdir().unwrap();
        let objects = temp.path().join("borrower/.git/objects");
        let old = temp.path().join("old/.git/objects");
        let primary = temp.path().join("primary/.git/objects");
        for path in [&old, &primary] {
            std::fs::create_dir_all(path).unwrap();
        }
        let mut req = request(&objects, &primary);
        req.operation = "install".to_string();
        req.alternates = vec![old.to_string_lossy().into_owned()];
        req.config_enabled = true;
        req.config_primary_objects = Some(old.to_string_lossy().into_owned());
        req.mutation_context =
            Some(GIT_OBJECT_SHARING_CONTEXT_MAINTENANCE_REPAIR.to_string());

        let plan = plan_git_object_sharing(&req).unwrap();

        assert_eq!(plan.action, GIT_OBJECT_SHARING_ACTION_FAIL);
        assert!(plan.detail.contains("claim observation is unavailable"));
        assert!(!plan.dependency_mutation);
    }

    #[test]
    fn maintenance_compaction_requires_clean_status() {
        let temp = tempdir().unwrap();
        let objects = temp.path().join("borrower/.git/objects");
        let old = temp.path().join("old/.git/objects");
        let primary = temp.path().join("primary/.git/objects");
        for path in [&old, &primary] {
            std::fs::create_dir_all(path).unwrap();
        }
        let mut req = request(&objects, &primary);
        req.operation = "install".to_string();
        req.alternates = vec![old.to_string_lossy().into_owned()];
        req.config_enabled = true;
        req.config_primary_objects = Some(old.to_string_lossy().into_owned());
        req.mutation_context =
            Some(GIT_OBJECT_SHARING_CONTEXT_MAINTENANCE_COMPACT.to_string());
        req.fresh_claim_status = Some("clear".to_string());
        req.fresh_occupant_status = Some("clear".to_string());
        req.checkout_clean = Some(false);

        let plan = plan_git_object_sharing(&req).unwrap();

        assert_eq!(plan.action, GIT_OBJECT_SHARING_ACTION_FAIL);
        assert!(plan.detail.contains("clean status"));
        assert!(!plan.dependency_mutation);
    }
}
