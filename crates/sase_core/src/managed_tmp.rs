//! Managed SASE temp-root reaping.
//!
//! The host owns resolving the managed temp root, but the retention/pressure
//! decision belongs here so every frontend gets the same safety rules.  The
//! reaper only removes files or whole directories whose own metadata and all
//! descendants are stale, never follows symlinks, and refuses broad roots.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

pub const MANAGED_TMP_REAP_WIRE_SCHEMA_VERSION: u32 = 3;

const TOP_LEVEL_BUCKET: &str = "<root>";

#[derive(Debug, Error)]
pub enum ManagedTmpReapError {
    #[error("managed SASE temp root must be a dedicated directory, not {0}")]
    UnsafeRoot(String),
    #[error("could not resolve current directory: {0}")]
    CurrentDir(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagedTmpReapRequestWire {
    pub schema_version: u32,
    pub root: String,
    pub apply: bool,
    pub now_epoch_seconds: f64,
    #[serde(default = "default_true")]
    pub age_reap: bool,
    pub horizons: BTreeMap<String, f64>,
    pub default_horizon_seconds: f64,
    pub max_removals: u32,
    #[serde(default = "default_true")]
    pub pressure_reap: bool,
    pub pressure_max_bytes: Option<u64>,
    pub pressure_target_bytes: u64,
    pub pressure_min_available_bytes: Option<u64>,
    pub pressure_recovery_available_bytes: u64,
    pub pressure_min_age_seconds: f64,
    #[serde(default)]
    pub pressure_low_free_space_min_age_seconds: Option<f64>,
    pub pressure_min_entry_bytes: u64,
    pub pressure_reap_buckets: Vec<String>,
    pub filesystem_available_bytes: Option<u64>,
    #[serde(default)]
    pub launch_scratch: Option<ManagedTmpLaunchScratchRequestWire>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagedTmpLaunchScratchRequestWire {
    pub scratch_key: String,
    pub buckets: Vec<String>,
    pub live: bool,
    pub liveness_complete: bool,
    #[serde(default)]
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ManagedTmpReapResultWire {
    pub schema_version: u32,
    pub root: String,
    pub apply: bool,
    pub scanned: u64,
    pub selected: u64,
    pub removed: u64,
    pub selected_bytes: u64,
    pub removed_bytes: u64,
    pub selected_by_subdir: BTreeMap<String, u64>,
    pub removed_by_subdir: BTreeMap<String, u64>,
    pub selected_directories: Vec<String>,
    pub removed_directories: Vec<String>,
    pub capped: bool,
    pub ordinary_selected: u64,
    pub ordinary_removed: u64,
    pub ordinary_reclaimable_bytes: u64,
    pub ordinary_reclaimed_bytes: u64,
    pub launch_selected: u64,
    pub launch_removed: u64,
    pub launch_reclaimable_bytes: u64,
    pub launch_reclaimed_bytes: u64,
    pub pressure_selected: u64,
    pub pressure_removed: u64,
    pub pressure_reclaimable_bytes: u64,
    pub pressure_reclaimed_bytes: u64,
    pub pressure_trigger: Option<String>,
    pub pressure_root_size_bytes: u64,
    pub pressure_available_bytes: Option<u64>,
    pub pressure_recovery_available_bytes: u64,
    #[serde(default)]
    pub pressure_effective_min_age_seconds: Option<f64>,
    pub skipped: u64,
    pub failed: u64,
    pub incomplete_observations: u64,
    pub skip_reasons: Vec<String>,
    pub removal_errors: Vec<String>,
}

#[derive(Debug, Clone)]
struct RemovalSelection {
    kind: RemovedKind,
    size_bytes: u64,
    status: RemovalStatus,
}

#[derive(Debug, Clone)]
struct RemovalSkip {
    reason: String,
    incomplete: bool,
}

#[derive(Debug, Clone)]
enum RemovalAttempt {
    Selected(RemovalSelection),
    Skipped(RemovalSkip),
}

#[derive(Debug, Clone)]
enum RemovalStatus {
    DryRun,
    Removed,
    Failed(String),
}

impl RemovalSelection {
    fn removed(&self) -> bool {
        matches!(self.status, RemovalStatus::Removed)
    }

    fn failure_message(&self) -> Option<&str> {
        match &self.status {
            RemovalStatus::Failed(message) => Some(message.as_str()),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RemovedKind {
    File,
    Directory,
}

#[derive(Debug, Clone)]
struct EntrySnapshot {
    is_file: bool,
    is_dir: bool,
    is_symlink: bool,
    size_bytes: u64,
    latest_mtime: f64,
    complete: bool,
}

#[derive(Debug, Clone)]
struct ChildListing {
    children: Vec<PathBuf>,
    complete: bool,
}

#[derive(Debug, Clone)]
struct PressureCandidate {
    path: PathBuf,
    bucket: String,
    size_bytes: u64,
    latest_mtime: f64,
}

#[derive(Debug, Clone)]
struct PressurePlan {
    trigger: Option<&'static str>,
    root_size: u64,
    available: Option<u64>,
    target_size: u64,
    recovery_available: u64,
    effective_min_age_seconds: f64,
}

#[derive(Debug, Default)]
struct PressureReapResult {
    scanned: u64,
    selected: u64,
    removed: u64,
    selected_by_subdir: BTreeMap<String, u64>,
    removed_by_subdir: BTreeMap<String, u64>,
    selected_directories: Vec<PathBuf>,
    removed_directories: Vec<PathBuf>,
    reclaimable_bytes: u64,
    reclaimed_bytes: u64,
    capped: bool,
    skipped: u64,
    failed: u64,
    incomplete_observations: u64,
    skip_reasons: Vec<String>,
    removal_errors: Vec<String>,
    trigger: Option<String>,
    root_size: u64,
    available: Option<u64>,
    recovery_available: u64,
    effective_min_age_seconds: Option<f64>,
}

#[derive(Debug, Default)]
struct LaunchScratchReapResult {
    scanned: u64,
    selected: u64,
    removed: u64,
    selected_by_subdir: BTreeMap<String, u64>,
    removed_by_subdir: BTreeMap<String, u64>,
    selected_directories: Vec<PathBuf>,
    removed_directories: Vec<PathBuf>,
    reclaimable_bytes: u64,
    reclaimed_bytes: u64,
    capped: bool,
    skipped: u64,
    failed: u64,
    incomplete_observations: u64,
    skip_reasons: Vec<String>,
    removal_errors: Vec<String>,
}

#[derive(Debug, Default)]
struct CleanupDiagnostics {
    skipped: u64,
    failed: u64,
    incomplete_observations: u64,
    skip_reasons: Vec<String>,
    removal_errors: Vec<String>,
}

impl CleanupDiagnostics {
    fn record_skip(&mut self, skip: RemovalSkip) {
        self.skipped += 1;
        if skip.incomplete {
            self.incomplete_observations += 1;
        }
        self.skip_reasons.push(skip.reason);
    }

    fn record_failure(&mut self, message: &str) {
        self.failed += 1;
        self.removal_errors.push(message.to_string());
    }

    fn merge_pressure(&mut self, pressure: &mut PressureReapResult) {
        self.skipped += pressure.skipped;
        self.failed += pressure.failed;
        self.incomplete_observations += pressure.incomplete_observations;
        self.skip_reasons.append(&mut pressure.skip_reasons);
        self.removal_errors.append(&mut pressure.removal_errors);
    }

    fn merge_launch(&mut self, launch: &mut LaunchScratchReapResult) {
        self.skipped += launch.skipped;
        self.failed += launch.failed;
        self.incomplete_observations += launch.incomplete_observations;
        self.skip_reasons.append(&mut launch.skip_reasons);
        self.removal_errors.append(&mut launch.removal_errors);
    }
}

fn default_true() -> bool {
    true
}

pub fn reap_managed_tmpdir(
    request: &ManagedTmpReapRequestWire,
) -> Result<ManagedTmpReapResultWire, ManagedTmpReapError> {
    let root = validate_reap_root(Path::new(&request.root))?;
    let root_string = root.to_string_lossy().into_owned();
    let clock = request.now_epoch_seconds;
    let mut scanned = 0_u64;
    let mut ordinary_selected = 0_u64;
    let mut ordinary_removed = 0_u64;
    let mut ordinary_reclaimable_bytes = 0_u64;
    let mut ordinary_reclaimed_bytes = 0_u64;
    let mut selected_by_subdir: BTreeMap<String, u64> = BTreeMap::new();
    let mut removed_by_subdir: BTreeMap<String, u64> = BTreeMap::new();
    let mut selected_directories: Vec<PathBuf> = Vec::new();
    let mut removed_directories: Vec<PathBuf> = Vec::new();
    let mut budget = u64::from(request.max_removals);
    let mut capped = false;
    let mut diagnostics = CleanupDiagnostics::default();

    if request.age_reap {
        let root_children = iter_children(&root);
        if !root_children.complete {
            diagnostics.record_skip(RemovalSkip {
                reason: format!(
                    "could not fully list managed tmp root {}",
                    root.display()
                ),
                incomplete: true,
            });
        }
        for entry in root_children.children {
            if budget == 0 {
                capped = true;
                break;
            }

            let candidates = match entry_snapshot(&entry) {
                Some(snapshot) if snapshot.is_dir && !snapshot.is_symlink => {
                    let bucket = path_name(&entry);
                    let horizon = request
                        .horizons
                        .get(bucket.as_str())
                        .copied()
                        .unwrap_or(request.default_horizon_seconds);
                    let children = iter_children(&entry);
                    if !children.complete {
                        diagnostics.record_skip(RemovalSkip {
                            reason: format!(
                                "could not fully list managed tmp bucket {}",
                                entry.display()
                            ),
                            incomplete: true,
                        });
                    }
                    children
                        .children
                        .into_iter()
                        .map(|child| (child, clock - horizon, bucket.clone()))
                        .collect::<Vec<_>>()
                }
                _ => vec![(
                    entry,
                    clock - request.default_horizon_seconds,
                    TOP_LEVEL_BUCKET.to_string(),
                )],
            };

            for (candidate, cutoff, bucket) in candidates {
                if budget == 0 {
                    capped = true;
                    break;
                }
                scanned += 1;
                let Some(attempt) =
                    remove_if_stale(&candidate, cutoff, request.apply)
                else {
                    continue;
                };
                let outcome = match attempt {
                    RemovalAttempt::Selected(outcome) => outcome,
                    RemovalAttempt::Skipped(skip) => {
                        diagnostics.record_skip(skip);
                        continue;
                    }
                };
                ordinary_selected += 1;
                ordinary_reclaimable_bytes = ordinary_reclaimable_bytes
                    .saturating_add(outcome.size_bytes);
                if outcome.kind == RemovedKind::Directory {
                    selected_directories.push(candidate.clone());
                    if outcome.removed() {
                        removed_directories.push(candidate);
                    }
                }
                *selected_by_subdir.entry(bucket.clone()).or_insert(0) += 1;
                if outcome.removed() {
                    ordinary_removed += 1;
                    ordinary_reclaimed_bytes = ordinary_reclaimed_bytes
                        .saturating_add(outcome.size_bytes);
                    *removed_by_subdir.entry(bucket).or_insert(0) += 1;
                }
                if let Some(message) = outcome.failure_message() {
                    diagnostics.record_failure(message);
                }
                budget -= 1;
            }
        }
    }

    let mut launch = LaunchScratchReapResult::default();
    if budget > 0 {
        launch = reap_launch_scratch(&root, request, budget);
        budget = budget.saturating_sub(launch.selected);
        scanned += launch.scanned;
        capped = capped || launch.capped;
        selected_directories.extend(launch.selected_directories.clone());
        removed_directories.extend(launch.removed_directories.clone());
        for (bucket, count) in &launch.selected_by_subdir {
            *selected_by_subdir.entry(bucket.clone()).or_insert(0) += *count;
        }
        for (bucket, count) in &launch.removed_by_subdir {
            *removed_by_subdir.entry(bucket.clone()).or_insert(0) += *count;
        }
        diagnostics.merge_launch(&mut launch);
    } else if request.launch_scratch.is_some() {
        capped = true;
    }

    let mut pressure = PressureReapResult::default();
    if request.pressure_reap && budget > 0 {
        pressure = reap_pressure_candidates(&root, request, budget);
        scanned += pressure.scanned;
        capped = capped || pressure.capped;
        selected_directories.extend(pressure.selected_directories.clone());
        removed_directories.extend(pressure.removed_directories.clone());
        for (bucket, count) in &pressure.selected_by_subdir {
            *selected_by_subdir.entry(bucket.clone()).or_insert(0) += *count;
        }
        for (bucket, count) in &pressure.removed_by_subdir {
            *removed_by_subdir.entry(bucket.clone()).or_insert(0) += *count;
        }
        diagnostics.merge_pressure(&mut pressure);
    } else if request.pressure_reap {
        pressure.root_size = tree_snapshot(&root)
            .map(|snapshot| snapshot.size_bytes)
            .unwrap_or(0);
        pressure.available = filesystem_available_bytes(&root)
            .or(request.filesystem_available_bytes);
        pressure.recovery_available = request.pressure_recovery_available_bytes;
    }

    let selected = selected_by_subdir.values().sum();
    let removed = removed_by_subdir.values().sum();
    let selected_bytes = ordinary_reclaimable_bytes
        .saturating_add(launch.reclaimable_bytes)
        .saturating_add(pressure.reclaimable_bytes);
    let removed_bytes = ordinary_reclaimed_bytes
        .saturating_add(launch.reclaimed_bytes)
        .saturating_add(pressure.reclaimed_bytes);
    Ok(ManagedTmpReapResultWire {
        schema_version: MANAGED_TMP_REAP_WIRE_SCHEMA_VERSION,
        root: root_string,
        apply: request.apply,
        scanned,
        selected,
        removed,
        selected_bytes,
        removed_bytes,
        selected_by_subdir,
        removed_by_subdir,
        selected_directories: selected_directories
            .into_iter()
            .map(|path| path.to_string_lossy().into_owned())
            .collect(),
        removed_directories: removed_directories
            .into_iter()
            .map(|path| path.to_string_lossy().into_owned())
            .collect(),
        capped,
        ordinary_selected,
        ordinary_removed,
        ordinary_reclaimable_bytes,
        ordinary_reclaimed_bytes,
        launch_selected: launch.selected,
        launch_removed: launch.removed,
        launch_reclaimable_bytes: launch.reclaimable_bytes,
        launch_reclaimed_bytes: launch.reclaimed_bytes,
        pressure_selected: pressure.selected,
        pressure_removed: pressure.removed,
        pressure_reclaimable_bytes: pressure.reclaimable_bytes,
        pressure_reclaimed_bytes: pressure.reclaimed_bytes,
        pressure_trigger: pressure.trigger,
        pressure_root_size_bytes: pressure.root_size,
        pressure_available_bytes: pressure.available,
        pressure_recovery_available_bytes: pressure.recovery_available,
        pressure_effective_min_age_seconds: pressure.effective_min_age_seconds,
        skipped: diagnostics.skipped,
        failed: diagnostics.failed,
        incomplete_observations: diagnostics.incomplete_observations,
        skip_reasons: diagnostics.skip_reasons,
        removal_errors: diagnostics.removal_errors,
    })
}

fn reap_launch_scratch(
    root: &Path,
    request: &ManagedTmpReapRequestWire,
    current_budget: u64,
) -> LaunchScratchReapResult {
    let Some(launch) = &request.launch_scratch else {
        return LaunchScratchReapResult::default();
    };
    if current_budget == 0 {
        return LaunchScratchReapResult {
            capped: true,
            ..LaunchScratchReapResult::default()
        };
    }

    let mut result = LaunchScratchReapResult::default();
    if !is_single_path_segment(&launch.scratch_key) {
        result.skipped += 1;
        result.skip_reasons.push(format!(
            "launch scratch key {:?} is not one path segment",
            launch.scratch_key
        ));
        return result;
    }

    if !launch.liveness_complete {
        result.incomplete_observations += 1;
        result
            .skip_reasons
            .extend(launch.diagnostics.iter().cloned());
    }

    let mut budget = current_budget;
    let mut buckets = launch.buckets.clone();
    buckets.sort();
    buckets.dedup();
    for bucket in buckets {
        if budget == 0 {
            result.capped = true;
            break;
        }
        if !is_launch_scratch_bucket(&bucket) {
            result.skipped += 1;
            result
                .skip_reasons
                .push(format!("launch scratch bucket {bucket:?} is not owned"));
            continue;
        }
        let Some(candidate) =
            launch_scratch_candidate_path(root, &bucket, &launch.scratch_key)
        else {
            result.skipped += 1;
            result.skip_reasons.push(format!(
                "launch scratch candidate {bucket}/{key} is outside its bucket",
                key = launch.scratch_key
            ));
            continue;
        };
        result.scanned += 1;
        if launch.live {
            result.skipped += 1;
            result.skip_reasons.push(format!(
                "launch scratch {bucket}/{key} is still referenced by a live process",
                key = launch.scratch_key
            ));
            continue;
        }
        if !launch.liveness_complete {
            result.skipped += 1;
            result.skip_reasons.push(format!(
                "launch scratch {bucket}/{key} preserved because liveness was incomplete",
                key = launch.scratch_key
            ));
            continue;
        }

        let Some(attempt) =
            remove_directory_candidate(&candidate, request.apply)
        else {
            continue;
        };
        let outcome = match attempt {
            RemovalAttempt::Selected(outcome) => outcome,
            RemovalAttempt::Skipped(skip) => {
                result.skipped += 1;
                if skip.incomplete {
                    result.incomplete_observations += 1;
                }
                result.skip_reasons.push(skip.reason);
                continue;
            }
        };

        result.selected += 1;
        result.reclaimable_bytes =
            result.reclaimable_bytes.saturating_add(outcome.size_bytes);
        result.selected_directories.push(candidate.clone());
        *result.selected_by_subdir.entry(bucket.clone()).or_insert(0) += 1;
        if outcome.removed() {
            result.removed += 1;
            result.reclaimed_bytes =
                result.reclaimed_bytes.saturating_add(outcome.size_bytes);
            result.removed_directories.push(candidate);
            *result.removed_by_subdir.entry(bucket).or_insert(0) += 1;
        }
        if let Some(message) = outcome.failure_message() {
            result.failed += 1;
            result.removal_errors.push(message.to_string());
        }
        budget -= 1;
    }

    result
}

fn is_launch_scratch_bucket(bucket: &str) -> bool {
    matches!(bucket, "agent-tmp" | "cargo-targets")
}

fn is_single_path_segment(value: &str) -> bool {
    let mut components = Path::new(value).components();
    let Some(std::path::Component::Normal(component)) = components.next()
    else {
        return false;
    };
    components.next().is_none() && component.to_str() == Some(value)
}

fn launch_scratch_candidate_path(
    root: &Path,
    bucket: &str,
    scratch_key: &str,
) -> Option<PathBuf> {
    let bucket_dir = root.join(bucket);
    let candidate = bucket_dir.join(scratch_key);
    if candidate.parent() != Some(bucket_dir.as_path()) {
        return None;
    }
    Some(candidate)
}

fn reap_pressure_candidates(
    root: &Path,
    request: &ManagedTmpReapRequestWire,
    current_budget: u64,
) -> PressureReapResult {
    let Some(plan) = pressure_plan(root, request) else {
        let root_size = tree_snapshot(root)
            .map(|snapshot| snapshot.size_bytes)
            .unwrap_or(0);
        return PressureReapResult {
            root_size,
            available: request
                .filesystem_available_bytes
                .or_else(|| filesystem_available_bytes(root)),
            recovery_available: request.pressure_recovery_available_bytes,
            ..PressureReapResult::default()
        };
    };
    if current_budget == 0 {
        return PressureReapResult {
            capped: true,
            trigger: plan.trigger.map(str::to_string),
            root_size: plan.root_size,
            available: plan.available,
            recovery_available: plan.recovery_available,
            effective_min_age_seconds: Some(plan.effective_min_age_seconds),
            ..PressureReapResult::default()
        };
    }

    let buckets = request
        .pressure_reap_buckets
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let cutoff = request.now_epoch_seconds - plan.effective_min_age_seconds;
    let mut candidates = Vec::new();
    let mut scanned = 0_u64;
    let mut diagnostics = CleanupDiagnostics::default();
    let root_children = iter_children(root);
    if !root_children.complete {
        diagnostics.record_skip(RemovalSkip {
            reason: format!(
                "could not fully list managed tmp root {} for pressure",
                root.display()
            ),
            incomplete: true,
        });
    }
    for entry in root_children.children {
        let name = path_name(&entry);
        if buckets.contains(&name)
            && entry_snapshot(&entry)
                .is_some_and(|snapshot| snapshot.is_dir && !snapshot.is_symlink)
        {
            let children = iter_children(&entry);
            if !children.complete {
                diagnostics.record_skip(RemovalSkip {
                    reason: format!(
                        "could not fully list managed tmp pressure bucket {}",
                        entry.display()
                    ),
                    incomplete: true,
                });
            }
            for child in children.children {
                scanned += 1;
                match pressure_candidate(
                    &child,
                    &name,
                    cutoff,
                    request.pressure_min_entry_bytes,
                ) {
                    Ok(Some(candidate)) => candidates.push(candidate),
                    Ok(None) => {}
                    Err(skip) => diagnostics.record_skip(skip),
                }
            }
            continue;
        }

        if !request.horizons.contains_key(&name)
            && is_build_target_residue_name(&name)
        {
            scanned += 1;
            match pressure_candidate(
                &entry,
                TOP_LEVEL_BUCKET,
                cutoff,
                request.pressure_min_entry_bytes,
            ) {
                Ok(Some(candidate)) => candidates.push(candidate),
                Ok(None) => {}
                Err(skip) => diagnostics.record_skip(skip),
            }
        }
    }

    candidates.sort_by(|left, right| {
        right
            .size_bytes
            .cmp(&left.size_bytes)
            .then_with(|| {
                left.latest_mtime
                    .partial_cmp(&right.latest_mtime)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| left.path.cmp(&right.path))
    });

    let mut budget = current_budget;
    let mut estimated_size = plan.root_size;
    let mut estimated_available = plan.available;
    let mut selected = 0_u64;
    let mut removed = 0_u64;
    let mut reclaimable_bytes = 0_u64;
    let mut reclaimed_bytes = 0_u64;
    let mut selected_by_subdir = BTreeMap::new();
    let mut removed_by_subdir = BTreeMap::new();
    let mut selected_directories = Vec::new();
    let mut removed_directories = Vec::new();
    let mut capped = false;

    for candidate in candidates {
        if pressure_recovered(
            plan.trigger,
            estimated_size,
            plan.target_size,
            estimated_available,
            plan.recovery_available,
        ) {
            break;
        }
        if budget == 0 {
            capped = true;
            break;
        }
        let Some(attempt) =
            remove_if_stale(&candidate.path, cutoff, request.apply)
        else {
            continue;
        };
        let outcome = match attempt {
            RemovalAttempt::Selected(outcome) => outcome,
            RemovalAttempt::Skipped(skip) => {
                diagnostics.record_skip(skip);
                continue;
            }
        };
        if outcome.kind == RemovedKind::Directory {
            selected_directories.push(candidate.path.clone());
            if outcome.removed() {
                removed_directories.push(candidate.path);
            }
        }
        selected += 1;
        reclaimable_bytes =
            reclaimable_bytes.saturating_add(candidate.size_bytes);
        *selected_by_subdir
            .entry(candidate.bucket.clone())
            .or_insert(0) += 1;
        if outcome.removed() {
            removed += 1;
            reclaimed_bytes =
                reclaimed_bytes.saturating_add(candidate.size_bytes);
            *removed_by_subdir.entry(candidate.bucket).or_insert(0) += 1;
            estimated_size = estimated_size.saturating_sub(outcome.size_bytes);
            estimated_available = estimated_available
                .map(|free| free.saturating_add(outcome.size_bytes));
        } else if !request.apply {
            estimated_size =
                estimated_size.saturating_sub(candidate.size_bytes);
            estimated_available = estimated_available
                .map(|free| free.saturating_add(candidate.size_bytes));
        }
        if let Some(message) = outcome.failure_message() {
            diagnostics.record_failure(message);
        }
        budget -= 1;
    }

    PressureReapResult {
        scanned,
        selected,
        removed,
        selected_by_subdir,
        removed_by_subdir,
        selected_directories,
        removed_directories,
        reclaimable_bytes,
        reclaimed_bytes,
        capped,
        skipped: diagnostics.skipped,
        failed: diagnostics.failed,
        incomplete_observations: diagnostics.incomplete_observations,
        skip_reasons: diagnostics.skip_reasons,
        removal_errors: diagnostics.removal_errors,
        trigger: plan.trigger.map(str::to_string),
        root_size: plan.root_size,
        available: plan.available,
        recovery_available: plan.recovery_available,
        effective_min_age_seconds: Some(plan.effective_min_age_seconds),
    }
}

fn pressure_plan(
    root: &Path,
    request: &ManagedTmpReapRequestWire,
) -> Option<PressurePlan> {
    let root_size = tree_snapshot(root)
        .map(|snapshot| snapshot.size_bytes)
        .unwrap_or(0);
    let available = request
        .filesystem_available_bytes
        .or_else(|| filesystem_available_bytes(root));
    let size_pressure = request
        .pressure_max_bytes
        .is_some_and(|max| root_size > max);
    let free_pressure = request
        .pressure_min_available_bytes
        .zip(available)
        .is_some_and(|(minimum, observed)| observed < minimum);

    if !size_pressure && !free_pressure {
        return None;
    }

    let target_size = request
        .pressure_max_bytes
        .map(|max| request.pressure_target_bytes.min(max))
        .unwrap_or(request.pressure_target_bytes);
    let trigger = match (size_pressure, free_pressure) {
        (true, true) => "size_and_free_space",
        (true, false) => "size",
        (false, true) => "free_space",
        (false, false) => unreachable!(),
    };
    let effective_min_age_seconds =
        pressure_effective_min_age_seconds(request, free_pressure);

    Some(PressurePlan {
        trigger: Some(trigger),
        root_size,
        available,
        target_size,
        recovery_available: request.pressure_recovery_available_bytes,
        effective_min_age_seconds,
    })
}

fn pressure_effective_min_age_seconds(
    request: &ManagedTmpReapRequestWire,
    free_space_floor_breached: bool,
) -> f64 {
    if !free_space_floor_breached {
        return request.pressure_min_age_seconds;
    }
    request
        .pressure_low_free_space_min_age_seconds
        .map(|low_space_min_age| {
            request
                .pressure_min_age_seconds
                .min(low_space_min_age.max(0.0))
        })
        .unwrap_or(request.pressure_min_age_seconds)
}

fn pressure_recovered(
    trigger: Option<&str>,
    estimated_size: u64,
    target_size: u64,
    estimated_available: Option<u64>,
    recovery_available: u64,
) -> bool {
    match trigger {
        Some("size") => estimated_size <= target_size,
        Some("free_space") => estimated_available
            .is_some_and(|available| available >= recovery_available),
        Some("size_and_free_space") => {
            estimated_size <= target_size
                && estimated_available
                    .is_some_and(|available| available >= recovery_available)
        }
        _ => true,
    }
}

/// Push a denylist entry plus its canonicalized form, so the guard fires
/// whether or not canonicalization succeeds. Entries that do not exist
/// canonicalize to themselves and are still compared raw; canonicalization
/// failure never errors.
fn push_denied(denied: &mut Vec<PathBuf>, raw: &Path) {
    denied.push(raw.to_path_buf());
    if let Ok(canonical) = raw.canonicalize() {
        if canonical != raw {
            denied.push(canonical);
        }
    }
}

fn validate_reap_root(root: &Path) -> Result<PathBuf, ManagedTmpReapError> {
    let resolved = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    let cwd = std::env::current_dir()
        .map_err(|error| ManagedTmpReapError::CurrentDir(error.to_string()))?
        .canonicalize()
        .unwrap_or_else(|_| std::env::current_dir().unwrap_or_default());
    // Canonicalize both sides of the comparison (or neither): every entry is
    // stored raw and canonicalized, so `/tmp` still matches its resolved
    // `/private/tmp` form on macOS, and the raw form still matches when
    // canonicalization fails on either side.
    let mut denied: Vec<PathBuf> = Vec::new();
    for raw in ["/", "/tmp", "/var/tmp", "/private/tmp", "/private/var/tmp"] {
        push_denied(&mut denied, Path::new(raw));
    }
    for key in ["TMPDIR", "HOME"] {
        if let Some(value) = std::env::var_os(key) {
            let value = PathBuf::from(value);
            if !value.as_os_str().is_empty() {
                push_denied(&mut denied, &value);
            }
        }
    }
    for unsafe_root in &denied {
        if resolved == *unsafe_root {
            return Err(ManagedTmpReapError::UnsafeRoot(
                resolved.to_string_lossy().into_owned(),
            ));
        }
    }
    if resolved == cwd || cwd.starts_with(&resolved) {
        return Err(ManagedTmpReapError::UnsafeRoot(
            resolved.to_string_lossy().into_owned(),
        ));
    }
    Ok(resolved)
}

fn pressure_candidate(
    path: &Path,
    bucket: &str,
    cutoff: f64,
    min_entry_bytes: u64,
) -> Result<Option<PressureCandidate>, RemovalSkip> {
    let Some(snapshot) = tree_snapshot(path) else {
        return Ok(None);
    };
    if snapshot.is_symlink || !(snapshot.is_file || snapshot.is_dir) {
        return Ok(None);
    }
    if !snapshot.complete {
        return Err(RemovalSkip {
            reason: format!(
                "could not fully inspect pressure candidate {}",
                path.display()
            ),
            incomplete: true,
        });
    }
    if snapshot.latest_mtime >= cutoff {
        return Ok(None);
    }
    if snapshot.size_bytes < min_entry_bytes {
        return Ok(None);
    }
    Ok(Some(PressureCandidate {
        path: path.to_path_buf(),
        bucket: bucket.to_string(),
        size_bytes: snapshot.size_bytes,
        latest_mtime: snapshot.latest_mtime,
    }))
}

fn remove_if_stale(
    path: &Path,
    cutoff: f64,
    apply: bool,
) -> Option<RemovalAttempt> {
    let snapshot = tree_snapshot(path)?;
    if snapshot.is_symlink || snapshot.latest_mtime >= cutoff {
        return None;
    }
    if !snapshot.complete {
        return Some(RemovalAttempt::Skipped(RemovalSkip {
            reason: format!(
                "could not fully inspect stale candidate {}",
                path.display()
            ),
            incomplete: true,
        }));
    }

    remove_snapshot(path, snapshot, apply)
}

fn remove_directory_candidate(
    path: &Path,
    apply: bool,
) -> Option<RemovalAttempt> {
    let snapshot = tree_snapshot(path)?;
    if snapshot.is_symlink || !snapshot.is_dir {
        return None;
    }
    if !snapshot.complete {
        return Some(RemovalAttempt::Skipped(RemovalSkip {
            reason: format!(
                "could not fully inspect launch scratch {}",
                path.display()
            ),
            incomplete: true,
        }));
    }
    remove_snapshot(path, snapshot, apply)
}

fn remove_snapshot(
    path: &Path,
    snapshot: EntrySnapshot,
    apply: bool,
) -> Option<RemovalAttempt> {
    if !apply {
        if snapshot.is_dir {
            return Some(RemovalAttempt::Selected(RemovalSelection {
                kind: RemovedKind::Directory,
                size_bytes: snapshot.size_bytes,
                status: RemovalStatus::DryRun,
            }));
        }
        if snapshot.is_file {
            return Some(RemovalAttempt::Selected(RemovalSelection {
                kind: RemovedKind::File,
                size_bytes: snapshot.size_bytes,
                status: RemovalStatus::DryRun,
            }));
        }
        return None;
    }

    if snapshot.is_dir {
        let status = match fs::remove_dir_all(path) {
            Ok(()) => RemovalStatus::Removed,
            Err(error) => RemovalStatus::Failed(format!(
                "failed to remove directory {}: {error}",
                path.display()
            )),
        };
        return Some(RemovalAttempt::Selected(RemovalSelection {
            kind: RemovedKind::Directory,
            size_bytes: snapshot.size_bytes,
            status,
        }));
    }
    if snapshot.is_file {
        let status = match fs::remove_file(path) {
            Ok(()) => RemovalStatus::Removed,
            Err(error) => RemovalStatus::Failed(format!(
                "failed to remove file {}: {error}",
                path.display()
            )),
        };
        return Some(RemovalAttempt::Selected(RemovalSelection {
            kind: RemovedKind::File,
            size_bytes: snapshot.size_bytes,
            status,
        }));
    }
    None
}

fn tree_snapshot(path: &Path) -> Option<EntrySnapshot> {
    let metadata = fs::symlink_metadata(path).ok()?;
    let is_file = metadata.is_file();
    let is_dir = metadata.is_dir();
    let is_symlink = metadata.file_type().is_symlink();
    let own_mtime = metadata_mtime_seconds(&metadata).unwrap_or(0.0);
    if is_symlink {
        return Some(EntrySnapshot {
            is_file: false,
            is_dir: false,
            is_symlink: true,
            size_bytes: 0,
            latest_mtime: own_mtime,
            complete: true,
        });
    }
    if is_file {
        return Some(EntrySnapshot {
            is_file,
            is_dir,
            is_symlink,
            size_bytes: metadata.len(),
            latest_mtime: own_mtime,
            complete: true,
        });
    }
    if !is_dir {
        return Some(EntrySnapshot {
            is_file,
            is_dir,
            is_symlink,
            size_bytes: 0,
            latest_mtime: own_mtime,
            complete: true,
        });
    }

    let mut size_bytes = 0_u64;
    let mut latest_mtime = own_mtime;
    let mut complete = true;
    let children = iter_children(path);
    if !children.complete {
        complete = false;
    }
    for child in children.children {
        let Some(child_snapshot) = tree_snapshot(&child) else {
            complete = false;
            continue;
        };
        if !child_snapshot.complete {
            complete = false;
        }
        size_bytes = size_bytes.saturating_add(child_snapshot.size_bytes);
        latest_mtime = latest_mtime.max(child_snapshot.latest_mtime);
    }
    Some(EntrySnapshot {
        is_file,
        is_dir,
        is_symlink,
        size_bytes,
        latest_mtime,
        complete,
    })
}

fn entry_snapshot(path: &Path) -> Option<EntrySnapshot> {
    let metadata = fs::symlink_metadata(path).ok()?;
    Some(EntrySnapshot {
        is_file: metadata.is_file(),
        is_dir: metadata.is_dir(),
        is_symlink: metadata.file_type().is_symlink(),
        size_bytes: metadata.len(),
        latest_mtime: metadata_mtime_seconds(&metadata).unwrap_or(0.0),
        complete: true,
    })
}

fn iter_children(directory: &Path) -> ChildListing {
    let Ok(entries) = fs::read_dir(directory) else {
        return ChildListing {
            children: Vec::new(),
            complete: false,
        };
    };
    let mut children = Vec::new();
    let mut complete = true;
    for entry in entries {
        match entry {
            Ok(entry) => children.push(entry.path()),
            Err(_) => complete = false,
        }
    }
    ChildListing { children, complete }
}

fn path_name(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_string()
}

fn is_build_target_residue_name(name: &str) -> bool {
    name.contains("cargo-target")
        || name.contains("core-target")
        || name.ends_with("-target")
        || name == "target"
}

#[cfg(unix)]
fn metadata_mtime_seconds(metadata: &fs::Metadata) -> Option<f64> {
    use std::os::unix::fs::MetadataExt;
    Some(
        metadata.mtime() as f64
            + metadata.mtime_nsec() as f64 / 1_000_000_000.0,
    )
}

#[cfg(not(unix))]
fn metadata_mtime_seconds(metadata: &fs::Metadata) -> Option<f64> {
    metadata
        .modified()
        .ok()?
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs_f64())
}

#[cfg(unix)]
fn filesystem_available_bytes(path: &Path) -> Option<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    let rc = unsafe { libc::statvfs(c_path.as_ptr(), stats.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    let stats = unsafe { stats.assume_init() };
    // statvfs field widths are platform-dependent (f_bavail is u32 on
    // macOS, u64 on Linux); widen before multiplying, then saturate back.
    let available = u128::from(stats.f_bavail) * u128::from(stats.f_frsize);
    Some(u64::try_from(available).unwrap_or(u64::MAX))
}

#[cfg(not(unix))]
fn filesystem_available_bytes(_path: &Path) -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    const NOW: f64 = 1_800_000_000.0;
    const HOUR: f64 = 3600.0;
    const DAY: f64 = 24.0 * HOUR;

    fn request(root: &Path) -> ManagedTmpReapRequestWire {
        ManagedTmpReapRequestWire {
            schema_version: MANAGED_TMP_REAP_WIRE_SCHEMA_VERSION,
            root: root.to_string_lossy().into_owned(),
            apply: true,
            now_epoch_seconds: NOW,
            age_reap: true,
            horizons: BTreeMap::from([
                ("agent-tmp".to_string(), 12.0 * HOUR),
                ("cargo-targets".to_string(), 3.0 * DAY),
                ("handoff".to_string(), 3.0 * DAY),
                ("workflow-artifacts".to_string(), 14.0 * DAY),
            ]),
            default_horizon_seconds: 3.0 * DAY,
            max_removals: 2000,
            pressure_reap: true,
            pressure_max_bytes: Some(16 * 1024),
            pressure_target_bytes: 8 * 1024,
            pressure_min_available_bytes: Some(32 * 1024),
            pressure_recovery_available_bytes: 48 * 1024,
            pressure_min_age_seconds: 12.0 * HOUR,
            pressure_low_free_space_min_age_seconds: None,
            pressure_min_entry_bytes: 1024,
            pressure_reap_buckets: vec![
                "build-targets".to_string(),
                "cargo-targets".to_string(),
            ],
            filesystem_available_bytes: None,
            launch_scratch: None,
        }
    }

    fn aged_file(
        root: &Path,
        relative: &str,
        age_seconds: f64,
        bytes: usize,
    ) -> PathBuf {
        let path = root.join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut file = File::create(&path).unwrap();
        file.write_all(&vec![b'x'; bytes]).unwrap();
        set_mtime(&path, NOW - age_seconds);
        path
    }

    fn aged_dir(
        root: &Path,
        relative: &str,
        age_seconds: f64,
        bytes: usize,
    ) -> PathBuf {
        let path = root.join(relative);
        fs::create_dir_all(&path).unwrap();
        aged_file(root, &format!("{relative}/payload.bin"), age_seconds, bytes);
        set_mtime(&path, NOW - age_seconds);
        path
    }

    #[cfg(unix)]
    fn set_mtime(path: &Path, timestamp: f64) {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let seconds = timestamp.trunc() as libc::time_t;
        let nanos = ((timestamp.fract()) * 1_000_000_000.0) as libc::c_long;
        let times = [
            libc::timespec {
                tv_sec: seconds,
                tv_nsec: nanos,
            },
            libc::timespec {
                tv_sec: seconds,
                tv_nsec: nanos,
            },
        ];
        let c_path = CString::new(path.as_os_str().as_bytes()).unwrap();
        let rc = unsafe {
            libc::utimensat(libc::AT_FDCWD, c_path.as_ptr(), times.as_ptr(), 0)
        };
        assert_eq!(rc, 0, "utimensat({}) failed", path.display());
    }

    #[cfg(not(unix))]
    fn set_mtime(_path: &Path, _timestamp: f64) {}

    #[test]
    fn unknown_bucket_survives_while_stale_child_is_pruned() {
        let temp = tempdir().unwrap();
        let stale = aged_file(temp.path(), "future/old.tmp", 4.0 * DAY, 8);
        let fresh = aged_file(temp.path(), "future/new.tmp", HOUR, 8);
        set_mtime(&temp.path().join("future"), NOW - 13.0 * HOUR);

        let result = reap_managed_tmpdir(&request(temp.path())).unwrap();

        assert!(temp.path().join("future").is_dir());
        assert!(!stale.exists());
        assert!(fresh.exists());
        assert_eq!(result.removed_by_subdir.get("future"), Some(&1));
    }

    #[test]
    fn dry_run_selects_stale_entries_without_removing_them() {
        let temp = tempdir().unwrap();
        let stale = aged_file(temp.path(), "future/old.tmp", 4.0 * DAY, 8);
        let mut req = request(temp.path());
        req.apply = false;

        let result = reap_managed_tmpdir(&req).unwrap();

        assert!(stale.exists());
        assert!(!result.apply);
        assert_eq!(result.selected, 1);
        assert_eq!(result.removed, 0);
        assert_eq!(result.selected_bytes, 8);
        assert_eq!(result.removed_bytes, 0);
        assert_eq!(result.ordinary_reclaimable_bytes, 8);
        assert_eq!(result.selected_by_subdir.get("future"), Some(&1));
        assert!(result.removed_by_subdir.is_empty());
    }

    #[test]
    fn age_pass_reports_successful_reclaimed_bytes() {
        let temp = tempdir().unwrap();
        let stale = aged_file(temp.path(), "editors/note.md", 4.0 * DAY, 512);
        let mut req = request(temp.path());
        req.pressure_reap = false;

        let result = reap_managed_tmpdir(&req).unwrap();

        assert!(!stale.exists());
        assert_eq!(result.selected, 1);
        assert_eq!(result.removed, 1);
        assert_eq!(result.selected_bytes, 512);
        assert_eq!(result.removed_bytes, 512);
        assert_eq!(result.ordinary_selected, 1);
        assert_eq!(result.ordinary_removed, 1);
        assert_eq!(result.ordinary_reclaimable_bytes, 512);
        assert_eq!(result.ordinary_reclaimed_bytes, 512);
        assert_eq!(result.pressure_reclaimable_bytes, 0);
    }

    #[cfg(unix)]
    #[test]
    fn failed_age_removal_is_reported_without_claiming_reclaimed_bytes() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempdir().unwrap();
        let stale = aged_file(temp.path(), "editors/note.md", 4.0 * DAY, 128);
        let bucket = temp.path().join("editors");
        fs::set_permissions(&bucket, fs::Permissions::from_mode(0o500))
            .unwrap();
        let mut req = request(temp.path());
        req.pressure_reap = false;

        let result = reap_managed_tmpdir(&req).unwrap();

        fs::set_permissions(&bucket, fs::Permissions::from_mode(0o700))
            .unwrap();
        assert!(stale.exists());
        assert_eq!(result.selected, 1);
        assert_eq!(result.removed, 0);
        assert_eq!(result.selected_bytes, 128);
        assert_eq!(result.removed_bytes, 0);
        assert_eq!(result.failed, 1);
        assert_eq!(result.removal_errors.len(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn incomplete_tree_read_preserves_candidate_and_reports_incomplete() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempdir().unwrap();
        let stale = aged_dir(temp.path(), "editors/opaque", 4.0 * DAY, 128);
        fs::set_permissions(&stale, fs::Permissions::from_mode(0o000)).unwrap();
        let mut req = request(temp.path());
        req.pressure_reap = false;

        let result = reap_managed_tmpdir(&req).unwrap();

        fs::set_permissions(&stale, fs::Permissions::from_mode(0o700)).unwrap();
        assert!(stale.exists());
        assert_eq!(result.selected, 0);
        assert_eq!(result.removed, 0);
        assert_eq!(result.skipped, 1);
        assert_eq!(result.incomplete_observations, 1);
        assert!(result.skip_reasons[0].contains("could not fully inspect"));
    }

    #[test]
    fn launch_scratch_removes_only_assigned_bucket_and_key() {
        let temp = tempdir().unwrap();
        let assigned = aged_dir(temp.path(), "agent-tmp/launch-key", HOUR, 256);
        let unassigned =
            aged_dir(temp.path(), "agent-tmp/other-key", HOUR, 256);
        let cargo_same_key =
            aged_dir(temp.path(), "cargo-targets/launch-key", HOUR, 256);
        let mut req = request(temp.path());
        req.age_reap = false;
        req.pressure_reap = false;
        req.launch_scratch = Some(ManagedTmpLaunchScratchRequestWire {
            scratch_key: "launch-key".to_string(),
            buckets: vec!["agent-tmp".to_string()],
            live: false,
            liveness_complete: true,
            diagnostics: Vec::new(),
        });

        let result = reap_managed_tmpdir(&req).unwrap();

        assert!(!assigned.exists());
        assert!(unassigned.exists());
        assert!(cargo_same_key.exists());
        assert_eq!(result.launch_selected, 1);
        assert_eq!(result.launch_removed, 1);
        assert_eq!(result.selected_bytes, 256);
        assert_eq!(result.removed_bytes, 256);
        assert_eq!(result.ordinary_selected, 0);
    }

    #[test]
    fn launch_scratch_preserves_live_or_incomplete_candidates() {
        let temp = tempdir().unwrap();
        let live = aged_dir(temp.path(), "agent-tmp/live-key", HOUR, 64);
        let incomplete =
            aged_dir(temp.path(), "cargo-targets/incomplete-key", HOUR, 64);

        let mut live_req = request(temp.path());
        live_req.age_reap = false;
        live_req.pressure_reap = false;
        live_req.launch_scratch = Some(ManagedTmpLaunchScratchRequestWire {
            scratch_key: "live-key".to_string(),
            buckets: vec!["agent-tmp".to_string()],
            live: true,
            liveness_complete: true,
            diagnostics: Vec::new(),
        });
        let live_result = reap_managed_tmpdir(&live_req).unwrap();

        let mut incomplete_req = request(temp.path());
        incomplete_req.age_reap = false;
        incomplete_req.pressure_reap = false;
        incomplete_req.launch_scratch =
            Some(ManagedTmpLaunchScratchRequestWire {
                scratch_key: "incomplete-key".to_string(),
                buckets: vec!["cargo-targets".to_string()],
                live: false,
                liveness_complete: false,
                diagnostics: vec!["proc scan incomplete".to_string()],
            });
        let incomplete_result = reap_managed_tmpdir(&incomplete_req).unwrap();

        assert!(live.exists());
        assert!(incomplete.exists());
        assert_eq!(live_result.launch_selected, 0);
        assert_eq!(live_result.skipped, 1);
        assert_eq!(incomplete_result.launch_selected, 0);
        assert_eq!(incomplete_result.skipped, 1);
        assert_eq!(incomplete_result.incomplete_observations, 1);
        assert!(incomplete_result
            .skip_reasons
            .iter()
            .any(|reason| reason.contains("proc scan incomplete")));
    }

    #[test]
    fn pressure_does_not_delete_unknown_bucket_with_fresh_child() {
        let temp = tempdir().unwrap();
        let bucket =
            aged_dir(temp.path(), "unknown-bucket/fresh-handoff", HOUR, 4096);
        set_mtime(&temp.path().join("unknown-bucket"), NOW - 13.0 * HOUR);
        let mut req = request(temp.path());
        req.horizons.clear();
        req.pressure_max_bytes = Some(100);
        req.pressure_target_bytes = 50;
        req.pressure_min_entry_bytes = 1;

        let result = reap_managed_tmpdir(&req).unwrap();

        assert!(bucket.exists());
        assert_eq!(result.pressure_removed, 0);
        assert_eq!(result.pressure_trigger.as_deref(), Some("size"));
    }

    #[test]
    fn free_space_pressure_reaps_build_scratch_below_size_ceiling() {
        let temp = tempdir().unwrap();
        let old_large =
            aged_dir(temp.path(), "cargo-targets/run-old", DAY, 8192);
        let mut req = request(temp.path());
        req.pressure_max_bytes = Some(64 * 1024);
        req.pressure_target_bytes = 32 * 1024;
        req.pressure_min_available_bytes = Some(10 * 1024);
        req.pressure_recovery_available_bytes = 16 * 1024;
        req.filesystem_available_bytes = Some(8 * 1024);

        let result = reap_managed_tmpdir(&req).unwrap();

        assert!(!old_large.exists());
        assert_eq!(result.pressure_removed, 1);
        assert_eq!(result.pressure_trigger.as_deref(), Some("free_space"));
        assert_eq!(
            result.pressure_effective_min_age_seconds,
            Some(12.0 * HOUR)
        );
    }

    #[test]
    fn low_free_space_age_reaps_recent_large_target_when_size_also_triggers() {
        let temp = tempdir().unwrap();
        let recent_large =
            aged_dir(temp.path(), "cargo-targets/run-recent", 2.0 * HOUR, 8192);
        let mut req = request(temp.path());
        req.pressure_max_bytes = Some(1024);
        req.pressure_target_bytes = 0;
        req.pressure_min_available_bytes = Some(10 * 1024);
        req.pressure_recovery_available_bytes = 16 * 1024;
        req.pressure_low_free_space_min_age_seconds = Some(HOUR);
        req.filesystem_available_bytes = Some(8 * 1024);

        let result = reap_managed_tmpdir(&req).unwrap();

        assert!(!recent_large.exists());
        assert_eq!(result.pressure_removed, 1);
        assert_eq!(
            result.pressure_trigger.as_deref(),
            Some("size_and_free_space")
        );
        assert_eq!(result.pressure_effective_min_age_seconds, Some(HOUR));
    }

    #[test]
    fn low_free_space_age_reaps_recent_large_target_under_free_space_trigger() {
        let temp = tempdir().unwrap();
        let recent_large =
            aged_dir(temp.path(), "cargo-targets/run-recent", 2.0 * HOUR, 8192);
        let mut req = request(temp.path());
        req.pressure_max_bytes = Some(64 * 1024);
        req.pressure_target_bytes = 32 * 1024;
        req.pressure_min_available_bytes = Some(10 * 1024);
        req.pressure_recovery_available_bytes = 16 * 1024;
        req.pressure_low_free_space_min_age_seconds = Some(HOUR);
        req.filesystem_available_bytes = Some(8 * 1024);

        let result = reap_managed_tmpdir(&req).unwrap();

        assert!(!recent_large.exists());
        assert_eq!(result.pressure_removed, 1);
        assert_eq!(result.pressure_trigger.as_deref(), Some("free_space"));
        assert_eq!(result.pressure_effective_min_age_seconds, Some(HOUR));
    }

    #[test]
    fn low_free_space_age_does_not_apply_to_size_only_pressure() {
        let temp = tempdir().unwrap();
        let recent_large =
            aged_dir(temp.path(), "cargo-targets/run-recent", 2.0 * HOUR, 8192);
        let mut req = request(temp.path());
        req.pressure_max_bytes = Some(1024);
        req.pressure_target_bytes = 0;
        req.pressure_min_available_bytes = Some(1024);
        req.pressure_low_free_space_min_age_seconds = Some(HOUR);
        req.filesystem_available_bytes = Some(8 * 1024);

        let result = reap_managed_tmpdir(&req).unwrap();

        assert!(recent_large.exists());
        assert_eq!(result.pressure_removed, 0);
        assert_eq!(result.pressure_trigger.as_deref(), Some("size"));
        assert_eq!(
            result.pressure_effective_min_age_seconds,
            Some(12.0 * HOUR)
        );

        set_mtime(&recent_large, NOW - DAY);
        set_mtime(&recent_large.join("payload.bin"), NOW - DAY);
        let result = reap_managed_tmpdir(&req).unwrap();

        assert!(!recent_large.exists());
        assert_eq!(result.pressure_removed, 1);
        assert_eq!(
            result.pressure_effective_min_age_seconds,
            Some(12.0 * HOUR)
        );
    }

    #[test]
    fn low_free_space_age_still_respects_fresh_descendant() {
        let temp = tempdir().unwrap();
        let target =
            aged_dir(temp.path(), "cargo-targets/run-live", 2.0 * HOUR, 8192);
        let fresh = aged_file(
            temp.path(),
            "cargo-targets/run-live/deep/object.o",
            0.5 * HOUR,
            1,
        );
        let mut req = request(temp.path());
        req.pressure_max_bytes = Some(1024);
        req.pressure_target_bytes = 0;
        req.pressure_min_available_bytes = Some(10 * 1024);
        req.pressure_recovery_available_bytes = 16 * 1024;
        req.pressure_low_free_space_min_age_seconds = Some(HOUR);
        req.pressure_min_entry_bytes = 1;
        req.filesystem_available_bytes = Some(8 * 1024);

        let result = reap_managed_tmpdir(&req).unwrap();

        assert!(target.exists());
        assert!(fresh.exists());
        assert_eq!(result.pressure_removed, 0);
        assert_eq!(result.pressure_effective_min_age_seconds, Some(HOUR));
    }

    #[test]
    fn absent_low_free_space_age_preserves_base_pressure_age() {
        let temp = tempdir().unwrap();
        let recent_large =
            aged_dir(temp.path(), "cargo-targets/run-recent", 2.0 * HOUR, 8192);
        let mut req = request(temp.path());
        req.pressure_max_bytes = Some(1024);
        req.pressure_target_bytes = 0;
        req.pressure_min_available_bytes = Some(10 * 1024);
        req.pressure_recovery_available_bytes = 16 * 1024;
        req.filesystem_available_bytes = Some(8 * 1024);

        let result = reap_managed_tmpdir(&req).unwrap();

        assert!(recent_large.exists());
        assert_eq!(result.pressure_removed, 0);
        assert_eq!(
            result.pressure_trigger.as_deref(),
            Some("size_and_free_space")
        );
        assert_eq!(
            result.pressure_effective_min_age_seconds,
            Some(12.0 * HOUR)
        );
    }

    #[test]
    fn low_space_age_never_lengthens_base_pressure_age() {
        let temp = tempdir().unwrap();
        let two_hour_target =
            aged_dir(temp.path(), "cargo-targets/two-hour", 2.0 * HOUR, 8192);
        let mut req = request(temp.path());
        req.pressure_max_bytes = Some(1024);
        req.pressure_target_bytes = 0;
        req.pressure_min_available_bytes = Some(10 * 1024);
        req.pressure_low_free_space_min_age_seconds = Some(DAY);
        req.filesystem_available_bytes = Some(8 * 1024);
        req.pressure_min_age_seconds = HOUR;

        let result = reap_managed_tmpdir(&req).unwrap();

        assert!(!two_hour_target.exists());
        assert_eq!(result.pressure_removed, 1);
        assert_eq!(result.pressure_effective_min_age_seconds, Some(HOUR));
    }

    #[test]
    fn pressure_stops_at_free_space_recovery_threshold() {
        let temp = tempdir().unwrap();
        let largest = aged_dir(temp.path(), "cargo-targets/largest", DAY, 8192);
        let smaller = aged_dir(temp.path(), "cargo-targets/smaller", DAY, 4096);
        let mut req = request(temp.path());
        req.pressure_max_bytes = Some(64 * 1024);
        req.pressure_target_bytes = 32 * 1024;
        req.pressure_min_available_bytes = Some(10 * 1024);
        req.pressure_recovery_available_bytes = 15 * 1024;
        req.filesystem_available_bytes = Some(8 * 1024);

        let result = reap_managed_tmpdir(&req).unwrap();

        assert!(!largest.exists());
        assert!(smaller.exists());
        assert_eq!(result.pressure_removed, 1);
    }

    #[test]
    fn pressure_orders_largest_first_and_oldest_within_equal_size() {
        let temp = tempdir().unwrap();
        let smaller =
            aged_dir(temp.path(), "cargo-targets/smaller", 3.0 * DAY, 4096);
        let newer_equal =
            aged_dir(temp.path(), "cargo-targets/newer-equal", DAY, 8192);
        let older_equal =
            aged_dir(temp.path(), "cargo-targets/older-equal", 2.0 * DAY, 8192);
        let mut req = request(temp.path());
        req.pressure_max_bytes = Some(16 * 1024);
        req.pressure_target_bytes = 12 * 1024;
        req.pressure_min_available_bytes = None;
        req.max_removals = 2;

        let result = reap_managed_tmpdir(&req).unwrap();

        assert!(smaller.exists());
        assert!(newer_equal.exists());
        assert!(!older_equal.exists());
        assert_eq!(result.pressure_removed, 1);
    }

    #[test]
    fn live_build_tree_with_fresh_descendant_survives_pressure() {
        let temp = tempdir().unwrap();
        let target = aged_dir(temp.path(), "cargo-targets/run-live", DAY, 8192);
        let fresh = aged_file(
            temp.path(),
            "cargo-targets/run-live/deep/object.o",
            HOUR,
            1,
        );
        let mut req = request(temp.path());
        req.pressure_max_bytes = Some(100);
        req.pressure_target_bytes = 50;
        req.pressure_min_entry_bytes = 1;

        let result = reap_managed_tmpdir(&req).unwrap();

        assert!(target.exists());
        assert!(fresh.exists());
        assert_eq!(result.pressure_removed, 0);
    }

    #[test]
    fn expired_build_tree_without_fresh_descendant_is_removed() {
        let temp = tempdir().unwrap();
        let target = aged_dir(temp.path(), "cargo-targets/run-dead", DAY, 8192);
        let mut req = request(temp.path());
        req.pressure_max_bytes = Some(100);
        req.pressure_target_bytes = 50;

        let result = reap_managed_tmpdir(&req).unwrap();

        assert!(!target.exists());
        assert_eq!(result.pressure_removed, 1);
    }

    #[test]
    fn pressure_skips_generic_agent_tmp_and_handoff_buckets() {
        let temp = tempdir().unwrap();
        let agent_tmp =
            aged_dir(temp.path(), "agent-tmp/payload", 2.0 * HOUR, 8192);
        let handoff =
            aged_dir(temp.path(), "handoff/payload", 2.0 * HOUR, 8192);
        let mut req = request(temp.path());
        req.pressure_max_bytes = Some(100);
        req.pressure_target_bytes = 50;
        req.pressure_min_age_seconds = HOUR;

        let result = reap_managed_tmpdir(&req).unwrap();

        assert!(agent_tmp.exists());
        assert!(handoff.exists());
        assert_eq!(result.pressure_removed, 0);
    }

    #[test]
    fn pressure_keeps_symlink_entries() {
        let temp = tempdir().unwrap();
        let outside = tempdir().unwrap();
        let bucket = temp.path().join("cargo-targets");
        fs::create_dir(&bucket).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(outside.path(), bucket.join("linked"))
            .unwrap();
        let mut req = request(temp.path());
        req.pressure_max_bytes = Some(100);
        req.pressure_target_bytes = 50;

        let result = reap_managed_tmpdir(&req).unwrap();

        assert!(bucket.join("linked").exists());
        assert_eq!(result.pressure_removed, 0);
    }

    #[test]
    fn pressure_respects_removal_budget() {
        let temp = tempdir().unwrap();
        aged_dir(temp.path(), "cargo-targets/one", DAY, 8192);
        aged_dir(temp.path(), "cargo-targets/two", DAY, 8192);
        let mut req = request(temp.path());
        req.pressure_max_bytes = Some(100);
        req.pressure_target_bytes = 50;
        req.max_removals = 1;

        let result = reap_managed_tmpdir(&req).unwrap();

        assert_eq!(result.pressure_removed, 1);
        assert!(result.capped);
    }

    #[test]
    fn broad_cleanup_roots_are_rejected() {
        // Dry run: even a future guard regression must not delete anything.
        let mut denied = vec![
            "/".to_string(),
            "/tmp".to_string(),
            "/var/tmp".to_string(),
            "/private/tmp".to_string(),
            "/private/var/tmp".to_string(),
        ];
        for key in ["TMPDIR", "HOME"] {
            if let Some(value) = std::env::var_os(key) {
                let value = value.to_string_lossy().into_owned();
                if !value.is_empty() && !denied.contains(&value) {
                    denied.push(value);
                }
            }
        }
        denied.sort();
        denied.dedup();
        assert!(!denied.is_empty());

        for root in denied {
            let mut req = request(Path::new(&root));
            req.root = root.clone();
            req.apply = false;

            let error = reap_managed_tmpdir(&req).unwrap_err();

            assert!(
                error.to_string().contains("dedicated directory"),
                "root {root} was not rejected: {error}"
            );
        }
    }
}
