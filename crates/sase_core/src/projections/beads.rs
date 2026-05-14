use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};

use crate::bead::read::{
    blocked_issues_in_issues, get_epic_children_in_issues,
    list_issues_in_issues, read_store_issues, ready_issues_in_issues,
    show_issue_in_issues, stats_for_issues,
};
use crate::bead::wire::BeadTierWire;
use crate::bead::work::{
    build_epic_work_plan_from_issues, build_legend_work_plan_from_issues,
    EpicWorkPlanWire, LegendWorkPlanWire,
};
use crate::bead::{
    import_issues_from_jsonl, BeadMutationOutcomeWire,
    BeadPreclaimRollbackWire, DependencyWire, IssueTypeWire, IssueWire,
    JsonlLoadOutcome, StatusWire,
};

use super::db::{append_event_tx, set_projection_last_seq_tx, ProjectionDb};
use super::error::ProjectionError;
use super::event::{
    EventAppendOutcomeWire, EventAppendRequestWire, EventCausalityWire,
    EventEnvelopeWire, EventSourceWire,
};
use super::indexing::{
    source_event_idempotency_key, source_fingerprint_from_path,
    ShadowDiffCategoryWire, ShadowDiffCountsWire, ShadowDiffRecordWire,
    ShadowDiffReportWire, SourceChangeOperationWire, SourceIdentityWire,
    INDEXING_WIRE_SCHEMA_VERSION,
};
use super::replay::ProjectionApplier;

pub const BEAD_PROJECTION_NAME: &str = "beads";
pub const BEAD_INDEXING_DOMAIN: &str = "beads";

pub const BEAD_EVENT_SNAPSHOT_OBSERVED: &str = "bead.snapshot_observed";
pub const BEAD_EVENT_CREATED: &str = "bead.created";
pub const BEAD_EVENT_UPDATED: &str = "bead.updated";
pub const BEAD_EVENT_CLOSED: &str = "bead.closed";
pub const BEAD_EVENT_REOPENED: &str = "bead.reopened";
pub const BEAD_EVENT_REMOVED: &str = "bead.removed";
pub const BEAD_EVENT_DEPENDENCY_ADDED: &str = "bead.dependency_added";
pub const BEAD_EVENT_DEPENDENCY_REMOVED: &str = "bead.dependency_removed";
pub const BEAD_EVENT_WORK_PRECLAIMED: &str = "bead.work_preclaimed";
pub const BEAD_EVENT_READY_TO_WORK_CHANGED: &str = "bead.ready_to_work_changed";

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct BeadProjectionEventContextWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    pub source: EventSourceWire,
    pub host_id: String,
    pub project_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub causality: Vec<EventCausalityWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_revision: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BeadSnapshotObservedEventPayloadWire {
    pub issues: Vec<IssueWire>,
    pub loaded_rows: usize,
    pub blank_lines: usize,
    pub invalid_json_lines: usize,
    pub invalid_record_lines: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadIssueEventPayloadWire {
    pub issue: IssueWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadIssuesEventPayloadWire {
    pub issues: Vec<IssueWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadDependencyEventPayloadWire {
    pub dependency: DependencyWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadDependencyRemovedEventPayloadWire {
    pub issue_id: String,
    pub depends_on_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadRemovedEventPayloadWire {
    pub issue_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadWorkPreclaimedEventPayloadWire {
    pub issues: Vec<IssueWire>,
    #[serde(default)]
    pub rollback_preclaims: Vec<BeadPreclaimRollbackWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadProjectedEventWire {
    pub seq: i64,
    pub event_type: String,
    pub bead_id: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadStoreSourceWire {
    pub schema_version: u32,
    pub project_id: String,
    pub layout: String,
    pub beads_dir: String,
    pub issues_path: String,
    pub config_path: String,
    pub sqlite_paths: Vec<String>,
    pub source_paths: Vec<String>,
}

pub struct BeadProjectionApplier;

impl ProjectionApplier for BeadProjectionApplier {
    fn projection_name(&self) -> &str {
        BEAD_PROJECTION_NAME
    }

    fn apply(
        &mut self,
        event: &EventEnvelopeWire,
        conn: &Connection,
    ) -> Result<(), ProjectionError> {
        apply_bead_event_tx(conn, event)
    }
}

impl ProjectionDb {
    pub fn append_bead_event(
        &mut self,
        request: EventAppendRequestWire,
    ) -> Result<EventAppendOutcomeWire, ProjectionError> {
        self.with_immediate_transaction(|conn| {
            let outcome = append_event_tx(conn, request)?;
            if !outcome.duplicate {
                apply_bead_event_tx(conn, &outcome.event)?;
                set_projection_last_seq_tx(
                    conn,
                    BEAD_PROJECTION_NAME,
                    outcome.event.seq,
                )?;
            }
            Ok(outcome)
        })
    }
}

pub fn discover_bead_stores(
    project_root: &Path,
    project_id: Option<&str>,
) -> Vec<BeadStoreSourceWire> {
    let project_id = project_id
        .map(str::to_string)
        .unwrap_or_else(|| inferred_project_id(project_root));
    let candidates = [
        ("vc", project_root.join("sdd").join("beads")),
        (
            "non_vc",
            project_root.join(".sase").join("sdd").join("beads"),
        ),
    ];
    candidates
        .into_iter()
        .filter_map(|(layout, beads_dir)| {
            beads_dir.is_dir().then(|| {
                bead_store_source(project_id.clone(), layout, beads_dir)
            })
        })
        .collect()
}

pub fn bead_store_source(
    project_id: String,
    layout: impl Into<String>,
    beads_dir: PathBuf,
) -> BeadStoreSourceWire {
    let issues_path = beads_dir.join("issues.jsonl");
    let config_path = beads_dir.join("config.json");
    let sqlite_candidates = [
        beads_dir.join("beads.db"),
        beads_dir.join("beads.db-wal"),
        beads_dir.join("beads.db-shm"),
    ];
    let sqlite_paths: Vec<String> = sqlite_candidates
        .iter()
        .filter(|path| path.exists())
        .map(|path| display_path(path))
        .collect();
    let mut source_paths =
        vec![display_path(&issues_path), display_path(&config_path)];
    source_paths.extend(sqlite_paths.iter().cloned());
    BeadStoreSourceWire {
        schema_version: INDEXING_WIRE_SCHEMA_VERSION,
        project_id,
        layout: layout.into(),
        beads_dir: display_path(&beads_dir),
        issues_path: display_path(&issues_path),
        config_path: display_path(&config_path),
        sqlite_paths,
        source_paths,
    }
}

pub fn bead_source_identity(
    project_id: impl Into<String>,
    path: &Path,
) -> SourceIdentityWire {
    SourceIdentityWire {
        schema_version: INDEXING_WIRE_SCHEMA_VERSION,
        domain: BEAD_INDEXING_DOMAIN.to_string(),
        project_id: Some(project_id.into()),
        source_path: display_path(path),
        is_archive: false,
        fingerprint: source_fingerprint_from_path(path, true),
        last_indexed_event_seq: None,
    }
}

pub fn bead_backfill_snapshot_event_request(
    mut context: BeadProjectionEventContextWire,
    beads_dir: &Path,
) -> Result<EventAppendRequestWire, ProjectionError> {
    let issues_path = beads_dir.join("issues.jsonl");
    let outcome = import_issues_from_jsonl(&issues_path).map_err(bead_error)?;
    if context.source_path.is_none() {
        context.source_path = Some(display_path(&issues_path));
    }
    if context.idempotency_key.is_none() {
        let identity =
            bead_source_identity(context.project_id.clone(), &issues_path);
        context.idempotency_key = Some(source_event_idempotency_key(
            &SourceChangeOperationWire::Rewrite,
            &identity,
        ));
    }
    bead_snapshot_observed_event_request(context, outcome)
}

pub fn bead_backfill_snapshot(
    db: &mut ProjectionDb,
    context: BeadProjectionEventContextWire,
    beads_dir: &Path,
) -> Result<EventAppendOutcomeWire, ProjectionError> {
    let request = bead_backfill_snapshot_event_request(context, beads_dir)?;
    db.append_bead_event(request)
}

pub fn bead_snapshot_observed_event_request(
    context: BeadProjectionEventContextWire,
    outcome: JsonlLoadOutcome,
) -> Result<EventAppendRequestWire, ProjectionError> {
    event_request(
        context,
        BEAD_EVENT_SNAPSHOT_OBSERVED,
        BeadSnapshotObservedEventPayloadWire {
            issues: outcome.issues,
            loaded_rows: outcome.loaded_rows,
            blank_lines: outcome.blank_lines,
            invalid_json_lines: outcome.invalid_json_lines,
            invalid_record_lines: outcome.invalid_record_lines,
        },
    )
}

pub fn bead_created_event_request(
    context: BeadProjectionEventContextWire,
    issue: IssueWire,
) -> Result<EventAppendRequestWire, ProjectionError> {
    issue_event_request(context, BEAD_EVENT_CREATED, issue)
}

pub fn bead_updated_event_request(
    context: BeadProjectionEventContextWire,
    issue: IssueWire,
) -> Result<EventAppendRequestWire, ProjectionError> {
    issue_event_request(context, BEAD_EVENT_UPDATED, issue)
}

pub fn bead_closed_event_request(
    context: BeadProjectionEventContextWire,
    issues: Vec<IssueWire>,
) -> Result<EventAppendRequestWire, ProjectionError> {
    issues_event_request(context, BEAD_EVENT_CLOSED, issues)
}

pub fn bead_reopened_event_request(
    context: BeadProjectionEventContextWire,
    issue: IssueWire,
) -> Result<EventAppendRequestWire, ProjectionError> {
    issue_event_request(context, BEAD_EVENT_REOPENED, issue)
}

pub fn bead_removed_event_request(
    context: BeadProjectionEventContextWire,
    issue_ids: Vec<String>,
) -> Result<EventAppendRequestWire, ProjectionError> {
    event_request(
        context,
        BEAD_EVENT_REMOVED,
        BeadRemovedEventPayloadWire { issue_ids },
    )
}

pub fn bead_dependency_added_event_request(
    context: BeadProjectionEventContextWire,
    dependency: DependencyWire,
) -> Result<EventAppendRequestWire, ProjectionError> {
    event_request(
        context,
        BEAD_EVENT_DEPENDENCY_ADDED,
        BeadDependencyEventPayloadWire { dependency },
    )
}

pub fn bead_dependency_removed_event_request(
    context: BeadProjectionEventContextWire,
    issue_id: String,
    depends_on_id: String,
) -> Result<EventAppendRequestWire, ProjectionError> {
    event_request(
        context,
        BEAD_EVENT_DEPENDENCY_REMOVED,
        BeadDependencyRemovedEventPayloadWire {
            issue_id,
            depends_on_id,
        },
    )
}

pub fn bead_work_preclaimed_event_request(
    context: BeadProjectionEventContextWire,
    issues: Vec<IssueWire>,
    rollback_preclaims: Vec<BeadPreclaimRollbackWire>,
) -> Result<EventAppendRequestWire, ProjectionError> {
    event_request(
        context,
        BEAD_EVENT_WORK_PRECLAIMED,
        BeadWorkPreclaimedEventPayloadWire {
            issues,
            rollback_preclaims,
        },
    )
}

pub fn bead_ready_to_work_changed_event_request(
    context: BeadProjectionEventContextWire,
    issue: IssueWire,
) -> Result<EventAppendRequestWire, ProjectionError> {
    issue_event_request(context, BEAD_EVENT_READY_TO_WORK_CHANGED, issue)
}

pub fn bead_mutation_event_request(
    context: BeadProjectionEventContextWire,
    outcome: BeadMutationOutcomeWire,
) -> Result<Option<EventAppendRequestWire>, ProjectionError> {
    match outcome.operation.as_str() {
        "create" => Ok(outcome
            .issue
            .map(|issue| bead_created_event_request(context, issue))
            .transpose()?),
        "update" => Ok(outcome
            .issue
            .map(|issue| bead_updated_event_request(context, issue))
            .transpose()?),
        "open" => Ok(outcome
            .issue
            .map(|issue| bead_reopened_event_request(context, issue))
            .transpose()?),
        "close" => {
            Ok(Some(bead_closed_event_request(context, outcome.issues)?))
        }
        "rm" => Ok(Some(bead_removed_event_request(
            context,
            outcome.issue_ids,
        )?)),
        "dep_add" => Ok(outcome
            .dependency
            .map(|dependency| {
                bead_dependency_added_event_request(context, dependency)
            })
            .transpose()?),
        "preclaim_epic_work" => Ok(Some(bead_work_preclaimed_event_request(
            context,
            outcome.issues,
            outcome.rollback_preclaims,
        )?)),
        "mark_ready_to_work" | "unmark_ready_to_work" | "ready_to_work" => {
            Ok(outcome
                .issue
                .map(|issue| {
                    bead_ready_to_work_changed_event_request(context, issue)
                })
                .transpose()?)
        }
        _ => Ok(None),
    }
}

pub fn bead_projection_issues(
    conn: &Connection,
    project_id: &str,
) -> Result<Vec<IssueWire>, ProjectionError> {
    let mut stmt = conn.prepare(
        r#"
        SELECT bead_id, issue_json
        FROM beads
        WHERE project_id = ?1
        ORDER BY created_at ASC, bead_id ASC
        "#,
    )?;
    let rows = stmt.query_map([project_id], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut issues = Vec::new();
    for row in rows {
        let (issue_id, issue_json) = row?;
        let mut issue: IssueWire = serde_json::from_str(&issue_json)?;
        issue.dependencies = Vec::new();
        issues.push((issue_id, issue));
    }

    let dependencies = bead_projection_dependencies(conn, project_id)?;
    for (issue_id, issue) in &mut issues {
        if let Some(deps) = dependencies.get(issue_id) {
            issue.dependencies = deps.clone();
        }
    }

    Ok(issues.into_iter().map(|(_, issue)| issue).collect())
}

pub fn bead_projection_show(
    conn: &Connection,
    project_id: &str,
    issue_id: &str,
) -> Result<IssueWire, ProjectionError> {
    show_issue_in_issues(bead_projection_issues(conn, project_id)?, issue_id)
        .map_err(bead_error)
}

pub fn bead_projection_list(
    conn: &Connection,
    project_id: &str,
    statuses: Option<&[String]>,
    issue_types: Option<&[String]>,
    tiers: Option<&[String]>,
) -> Result<Vec<IssueWire>, ProjectionError> {
    list_issues_in_issues(
        bead_projection_issues(conn, project_id)?,
        statuses,
        issue_types,
        tiers,
    )
    .map_err(bead_error)
}

pub fn bead_projection_ready(
    conn: &Connection,
    project_id: &str,
) -> Result<Vec<IssueWire>, ProjectionError> {
    ready_issues_in_issues(bead_projection_issues(conn, project_id)?)
        .map_err(bead_error)
}

pub fn bead_projection_blocked(
    conn: &Connection,
    project_id: &str,
) -> Result<Vec<IssueWire>, ProjectionError> {
    blocked_issues_in_issues(bead_projection_issues(conn, project_id)?)
        .map_err(bead_error)
}

pub fn bead_projection_stats(
    conn: &Connection,
    project_id: &str,
) -> Result<BTreeMap<String, usize>, ProjectionError> {
    Ok(stats_for_issues(&bead_projection_issues(conn, project_id)?))
}

pub fn bead_projection_epic_children(
    conn: &Connection,
    project_id: &str,
    epic_id: &str,
) -> Result<Vec<IssueWire>, ProjectionError> {
    get_epic_children_in_issues(
        bead_projection_issues(conn, project_id)?,
        epic_id,
    )
    .map_err(bead_error)
}

pub fn bead_projection_epic_work_plan(
    conn: &Connection,
    project_id: &str,
    epic_id: &str,
) -> Result<EpicWorkPlanWire, ProjectionError> {
    build_epic_work_plan_from_issues(
        bead_projection_issues(conn, project_id)?,
        epic_id,
    )
    .map_err(bead_error)
}

pub fn bead_projection_legend_work_plan(
    conn: &Connection,
    project_id: &str,
    legend_id: &str,
) -> Result<LegendWorkPlanWire, ProjectionError> {
    build_legend_work_plan_from_issues(
        bead_projection_issues(conn, project_id)?,
        legend_id,
    )
    .map_err(bead_error)
}

pub fn bead_projection_events(
    conn: &Connection,
    project_id: &str,
    bead_id: Option<&str>,
) -> Result<Vec<BeadProjectedEventWire>, ProjectionError> {
    let mut sql = r#"
        SELECT seq, event_type, bead_id, created_at
        FROM bead_events
        WHERE project_id = ?1
    "#
    .to_string();
    if bead_id.is_some() {
        sql.push_str(" AND bead_id = ?2");
    }
    sql.push_str(" ORDER BY seq ASC");
    let mut stmt = conn.prepare(&sql)?;
    let rows = if let Some(bead_id) = bead_id {
        stmt.query_map(params![project_id, bead_id], event_row)?
    } else {
        stmt.query_map([project_id], event_row)?
    };
    let mut events = Vec::new();
    for row in rows {
        events.push(row?);
    }
    Ok(events)
}

pub fn bead_projection_shadow_diff(
    conn: &Connection,
    project_id: &str,
    beads_dir: &Path,
) -> Result<ShadowDiffReportWire, ProjectionError> {
    let issues_path = beads_dir.join("issues.jsonl");
    let source_path = display_path(&issues_path);
    let source_outcome =
        import_issues_from_jsonl(&issues_path).map_err(bead_error)?;
    let mut source_issues = source_outcome.issues.clone();
    normalize_issues_for_diff(&mut source_issues);
    let mut projected_issues = bead_projection_issues(conn, project_id)?;
    normalize_issues_for_diff(&mut projected_issues);

    let mut records = Vec::new();
    if source_outcome.invalid_json_lines > 0 {
        records.push(diff_record(
            ShadowDiffCategoryWire::Corrupt,
            &source_path,
            Some("issues.jsonl"),
            format!(
                "{} invalid JSONL line(s)",
                source_outcome.invalid_json_lines
            ),
        ));
    }
    if source_outcome.invalid_record_lines > 0 {
        records.push(diff_record(
            ShadowDiffCategoryWire::Corrupt,
            &source_path,
            Some("issues.jsonl"),
            format!(
                "{} invalid bead record line(s)",
                source_outcome.invalid_record_lines
            ),
        ));
    }

    let source_by_id = issues_by_id(&source_issues);
    let projected_by_id = issues_by_id(&projected_issues);
    for (issue_id, source_issue) in &source_by_id {
        match projected_by_id.get(issue_id) {
            None => records.push(diff_record(
                ShadowDiffCategoryWire::Missing,
                &source_path,
                Some(issue_id),
                "bead exists in source store but not projection",
            )),
            Some(projected_issue) if projected_issue != source_issue => {
                records.push(diff_record(
                    ShadowDiffCategoryWire::Stale,
                    &source_path,
                    Some(issue_id),
                    "projected bead row differs from source store",
                ));
            }
            Some(_) => {}
        }
    }
    for issue_id in projected_by_id.keys() {
        if !source_by_id.contains_key(issue_id) {
            records.push(diff_record(
                ShadowDiffCategoryWire::Extra,
                &source_path,
                Some(issue_id),
                "projection contains bead missing from source store",
            ));
        }
    }

    compare_issue_id_sets(
        &mut records,
        &source_path,
        "list",
        source_issues.iter().map(|issue| issue.id.clone()).collect(),
        projected_issues
            .iter()
            .map(|issue| issue.id.clone())
            .collect(),
    );
    compare_issue_id_sets(
        &mut records,
        &source_path,
        "ready",
        ready_issues_in_issues(source_issues.clone())
            .map_err(bead_error)?
            .into_iter()
            .map(|issue| issue.id)
            .collect(),
        bead_projection_ready(conn, project_id)?
            .into_iter()
            .map(|issue| issue.id)
            .collect(),
    );
    compare_issue_id_sets(
        &mut records,
        &source_path,
        "blocked",
        blocked_issues_in_issues(source_issues.clone())
            .map_err(bead_error)?
            .into_iter()
            .map(|issue| issue.id)
            .collect(),
        bead_projection_blocked(conn, project_id)?
            .into_iter()
            .map(|issue| issue.id)
            .collect(),
    );

    let source_stats = stats_for_issues(&source_issues);
    let projected_stats = bead_projection_stats(conn, project_id)?;
    if source_stats != projected_stats {
        records.push(diff_record(
            ShadowDiffCategoryWire::Stale,
            &source_path,
            Some("stats"),
            "projected bead stats differ from source store",
        ));
    }

    let counts = diff_counts(&records);
    Ok(ShadowDiffReportWire {
        schema_version: INDEXING_WIRE_SCHEMA_VERSION,
        domain: BEAD_INDEXING_DOMAIN.to_string(),
        records,
        counts,
    })
}

pub fn bead_store_shadow_diff(
    conn: &Connection,
    project_id: &str,
    beads_dir: &Path,
) -> Result<ShadowDiffReportWire, ProjectionError> {
    match read_store_issues(beads_dir) {
        Ok(_) => bead_projection_shadow_diff(conn, project_id, beads_dir),
        Err(error) => {
            let source_path = display_path(&beads_dir.join("issues.jsonl"));
            let records = vec![diff_record(
                ShadowDiffCategoryWire::Corrupt,
                &source_path,
                Some("issues.jsonl"),
                format!("failed to load bead store: {error}"),
            )];
            Ok(ShadowDiffReportWire {
                schema_version: INDEXING_WIRE_SCHEMA_VERSION,
                domain: BEAD_INDEXING_DOMAIN.to_string(),
                counts: diff_counts(&records),
                records,
            })
        }
    }
}

fn apply_bead_event_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
) -> Result<(), ProjectionError> {
    if !is_bead_event(&event.event_type) {
        return Ok(());
    }

    insert_bead_event_tx(conn, event)?;
    match event.event_type.as_str() {
        BEAD_EVENT_SNAPSHOT_OBSERVED => {
            let payload: BeadSnapshotObservedEventPayloadWire =
                serde_json::from_value(event.payload.clone())?;
            replace_project_issues_tx(
                conn,
                &event.project_id,
                &payload.issues,
                event.seq,
            )?;
        }
        BEAD_EVENT_CREATED
        | BEAD_EVENT_UPDATED
        | BEAD_EVENT_REOPENED
        | BEAD_EVENT_READY_TO_WORK_CHANGED => {
            let payload: BeadIssueEventPayloadWire =
                serde_json::from_value(event.payload.clone())?;
            upsert_issue_tx(
                conn,
                &event.project_id,
                &payload.issue,
                event.seq,
            )?;
        }
        BEAD_EVENT_CLOSED => {
            let payload: BeadIssuesEventPayloadWire =
                serde_json::from_value(event.payload.clone())?;
            upsert_issues_tx(
                conn,
                &event.project_id,
                &payload.issues,
                event.seq,
            )?;
        }
        BEAD_EVENT_REMOVED => {
            let payload: BeadRemovedEventPayloadWire =
                serde_json::from_value(event.payload.clone())?;
            remove_issues_tx(conn, &event.project_id, &payload.issue_ids)?;
        }
        BEAD_EVENT_DEPENDENCY_ADDED => {
            let payload: BeadDependencyEventPayloadWire =
                serde_json::from_value(event.payload.clone())?;
            upsert_dependency_tx(
                conn,
                &event.project_id,
                &payload.dependency,
                event.seq,
            )?;
        }
        BEAD_EVENT_DEPENDENCY_REMOVED => {
            let payload: BeadDependencyRemovedEventPayloadWire =
                serde_json::from_value(event.payload.clone())?;
            remove_dependency_tx(
                conn,
                &event.project_id,
                &payload.issue_id,
                &payload.depends_on_id,
            )?;
        }
        BEAD_EVENT_WORK_PRECLAIMED => {
            let payload: BeadWorkPreclaimedEventPayloadWire =
                serde_json::from_value(event.payload.clone())?;
            upsert_issues_tx(
                conn,
                &event.project_id,
                &payload.issues,
                event.seq,
            )?;
        }
        _ => {}
    }
    Ok(())
}

fn replace_project_issues_tx(
    conn: &Connection,
    project_id: &str,
    issues: &[IssueWire],
    seq: i64,
) -> Result<(), ProjectionError> {
    conn.execute(
        "DELETE FROM bead_dependencies WHERE project_id = ?1",
        [project_id],
    )?;
    conn.execute("DELETE FROM beads WHERE project_id = ?1", [project_id])?;
    upsert_issues_tx(conn, project_id, issues, seq)
}

fn upsert_issues_tx(
    conn: &Connection,
    project_id: &str,
    issues: &[IssueWire],
    seq: i64,
) -> Result<(), ProjectionError> {
    for issue in issues {
        upsert_issue_tx(conn, project_id, issue, seq)?;
    }
    Ok(())
}

fn upsert_issue_tx(
    conn: &Connection,
    project_id: &str,
    issue: &IssueWire,
    seq: i64,
) -> Result<(), ProjectionError> {
    issue.validate().map_err(bead_error)?;
    let issue_json = serde_json::to_string(issue)?;
    conn.execute(
        r#"
        INSERT INTO beads (
            bead_id, project_id, issue_json, title, status, issue_type, tier,
            parent_id, owner, assignee, created_at, created_by, updated_at,
            closed_at, close_reason, description, notes, design, model,
            is_ready_to_work, epic_count, changespec_name, changespec_bug_id,
            last_seq
        ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14,
            ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24
        )
        ON CONFLICT(project_id, bead_id) DO UPDATE SET
            project_id = excluded.project_id,
            issue_json = excluded.issue_json,
            title = excluded.title,
            status = excluded.status,
            issue_type = excluded.issue_type,
            tier = excluded.tier,
            parent_id = excluded.parent_id,
            owner = excluded.owner,
            assignee = excluded.assignee,
            created_at = excluded.created_at,
            created_by = excluded.created_by,
            updated_at = excluded.updated_at,
            closed_at = excluded.closed_at,
            close_reason = excluded.close_reason,
            description = excluded.description,
            notes = excluded.notes,
            design = excluded.design,
            model = excluded.model,
            is_ready_to_work = excluded.is_ready_to_work,
            epic_count = excluded.epic_count,
            changespec_name = excluded.changespec_name,
            changespec_bug_id = excluded.changespec_bug_id,
            last_seq = excluded.last_seq
        "#,
        params![
            issue.id.as_str(),
            project_id,
            issue_json.as_str(),
            issue.title.as_str(),
            status_value(&issue.status),
            issue_type_value(&issue.issue_type),
            issue.tier.as_ref().map(tier_value),
            issue.parent_id.as_deref(),
            issue.owner.as_str(),
            issue.assignee.as_str(),
            issue.created_at.as_str(),
            issue.created_by.as_str(),
            issue.updated_at.as_str(),
            issue.closed_at.as_deref(),
            issue.close_reason.as_deref(),
            issue.description.as_str(),
            issue.notes.as_str(),
            issue.design.as_str(),
            issue.model.as_str(),
            issue.is_ready_to_work,
            issue.epic_count,
            issue.changespec_name.as_str(),
            issue.changespec_bug_id.as_str(),
            seq,
        ],
    )?;
    conn.execute(
        "DELETE FROM bead_dependencies WHERE project_id = ?1 AND issue_id = ?2",
        params![project_id, issue.id],
    )?;
    for dependency in &issue.dependencies {
        upsert_dependency_tx(conn, project_id, dependency, seq)?;
    }
    Ok(())
}

fn remove_issues_tx(
    conn: &Connection,
    project_id: &str,
    issue_ids: &[String],
) -> Result<(), ProjectionError> {
    for issue_id in issue_ids {
        conn.execute(
            r#"
            DELETE FROM bead_dependencies
            WHERE project_id = ?1
              AND (issue_id = ?2 OR depends_on_id = ?2)
            "#,
            params![project_id, issue_id],
        )?;
        conn.execute(
            "DELETE FROM beads WHERE project_id = ?1 AND bead_id = ?2",
            params![project_id, issue_id],
        )?;
    }
    Ok(())
}

fn upsert_dependency_tx(
    conn: &Connection,
    project_id: &str,
    dependency: &DependencyWire,
    seq: i64,
) -> Result<(), ProjectionError> {
    conn.execute(
        r#"
        INSERT INTO bead_dependencies (
            project_id, issue_id, depends_on_id, created_at, created_by, last_seq
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
        ON CONFLICT(project_id, issue_id, depends_on_id) DO UPDATE SET
            created_at = excluded.created_at,
            created_by = excluded.created_by,
            last_seq = excluded.last_seq
        "#,
        params![
            project_id,
            dependency.issue_id,
            dependency.depends_on_id,
            dependency.created_at,
            dependency.created_by,
            seq,
        ],
    )?;
    Ok(())
}

fn remove_dependency_tx(
    conn: &Connection,
    project_id: &str,
    issue_id: &str,
    depends_on_id: &str,
) -> Result<(), ProjectionError> {
    conn.execute(
        r#"
        DELETE FROM bead_dependencies
        WHERE project_id = ?1 AND issue_id = ?2 AND depends_on_id = ?3
        "#,
        params![project_id, issue_id, depends_on_id],
    )?;
    Ok(())
}

fn bead_projection_dependencies(
    conn: &Connection,
    project_id: &str,
) -> Result<BTreeMap<String, Vec<DependencyWire>>, ProjectionError> {
    let mut stmt = conn.prepare(
        r#"
        SELECT issue_id, depends_on_id, created_at, created_by
        FROM bead_dependencies
        WHERE project_id = ?1
        ORDER BY issue_id ASC, created_at ASC, depends_on_id ASC
        "#,
    )?;
    let rows = stmt.query_map([project_id], |row| {
        Ok(DependencyWire {
            issue_id: row.get(0)?,
            depends_on_id: row.get(1)?,
            created_at: row.get(2)?,
            created_by: row.get(3)?,
        })
    })?;
    let mut by_issue: BTreeMap<String, Vec<DependencyWire>> = BTreeMap::new();
    for row in rows {
        let dependency = row?;
        by_issue
            .entry(dependency.issue_id.clone())
            .or_default()
            .push(dependency);
    }
    Ok(by_issue)
}

fn insert_bead_event_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
) -> Result<(), ProjectionError> {
    let payload_json = serde_json::to_string(&event.payload)?;
    conn.execute(
        r#"
        INSERT OR REPLACE INTO bead_events (
            seq, project_id, event_type, bead_id, payload_json, created_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
        "#,
        params![
            event.seq,
            event.project_id,
            event.event_type,
            bead_id_for_event(event)?,
            payload_json,
            event.created_at,
        ],
    )?;
    Ok(())
}

fn bead_id_for_event(
    event: &EventEnvelopeWire,
) -> Result<Option<String>, ProjectionError> {
    let value = &event.payload;
    let issue_id = value
        .get("issue")
        .and_then(|issue| issue.get("id"))
        .and_then(|id| id.as_str())
        .map(str::to_string)
        .or_else(|| {
            value
                .get("dependency")
                .and_then(|dep| dep.get("issue_id"))
                .and_then(|id| id.as_str())
                .map(str::to_string)
        })
        .or_else(|| {
            value
                .get("issue_id")
                .and_then(|id| id.as_str())
                .map(str::to_string)
        })
        .or_else(|| {
            value
                .get("issues")
                .and_then(|issues| issues.as_array())
                .and_then(|issues| issues.first())
                .and_then(|issue| issue.get("id"))
                .and_then(|id| id.as_str())
                .map(str::to_string)
        })
        .or_else(|| {
            value
                .get("issue_ids")
                .and_then(|ids| ids.as_array())
                .and_then(|ids| ids.first())
                .and_then(|id| id.as_str())
                .map(str::to_string)
        });
    Ok(issue_id)
}

fn event_request<T>(
    context: BeadProjectionEventContextWire,
    event_type: &str,
    payload: T,
) -> Result<EventAppendRequestWire, ProjectionError>
where
    T: Serialize,
{
    Ok(EventAppendRequestWire {
        created_at: context.created_at,
        source: context.source,
        host_id: context.host_id,
        project_id: context.project_id,
        event_type: event_type.to_string(),
        payload: serde_json::to_value(payload)?,
        idempotency_key: context.idempotency_key,
        causality: context.causality,
        source_path: context.source_path,
        source_revision: context.source_revision,
    })
}

fn issue_event_request(
    context: BeadProjectionEventContextWire,
    event_type: &str,
    issue: IssueWire,
) -> Result<EventAppendRequestWire, ProjectionError> {
    event_request(context, event_type, BeadIssueEventPayloadWire { issue })
}

fn issues_event_request(
    context: BeadProjectionEventContextWire,
    event_type: &str,
    issues: Vec<IssueWire>,
) -> Result<EventAppendRequestWire, ProjectionError> {
    event_request(context, event_type, BeadIssuesEventPayloadWire { issues })
}

fn event_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<BeadProjectedEventWire> {
    Ok(BeadProjectedEventWire {
        seq: row.get(0)?,
        event_type: row.get(1)?,
        bead_id: row.get(2)?,
        created_at: row.get(3)?,
    })
}

fn is_bead_event(event_type: &str) -> bool {
    matches!(
        event_type,
        BEAD_EVENT_SNAPSHOT_OBSERVED
            | BEAD_EVENT_CREATED
            | BEAD_EVENT_UPDATED
            | BEAD_EVENT_CLOSED
            | BEAD_EVENT_REOPENED
            | BEAD_EVENT_REMOVED
            | BEAD_EVENT_DEPENDENCY_ADDED
            | BEAD_EVENT_DEPENDENCY_REMOVED
            | BEAD_EVENT_WORK_PRECLAIMED
            | BEAD_EVENT_READY_TO_WORK_CHANGED
    )
}

fn bead_error(error: crate::bead::BeadError) -> ProjectionError {
    ProjectionError::Invariant(format!("bead projection error: {error}"))
}

fn status_value(status: &StatusWire) -> &'static str {
    match status {
        StatusWire::Open => "open",
        StatusWire::InProgress => "in_progress",
        StatusWire::Closed => "closed",
    }
}

fn issue_type_value(issue_type: &IssueTypeWire) -> &'static str {
    match issue_type {
        IssueTypeWire::Plan => "plan",
        IssueTypeWire::Phase => "phase",
    }
}

fn tier_value(tier: &BeadTierWire) -> &'static str {
    match tier {
        BeadTierWire::Plan => "plan",
        BeadTierWire::Epic => "epic",
        BeadTierWire::Legend => "legend",
    }
}

fn inferred_project_id(project_root: &Path) -> String {
    project_root
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("default")
        .to_string()
}

fn display_path(path: &Path) -> String {
    path.to_string_lossy().to_string()
}

fn normalize_issues_for_diff(issues: &mut [IssueWire]) {
    for issue in issues.iter_mut() {
        issue.dependencies.sort_by(|a, b| {
            (
                a.issue_id.as_str(),
                a.depends_on_id.as_str(),
                a.created_at.as_str(),
            )
                .cmp(&(
                    b.issue_id.as_str(),
                    b.depends_on_id.as_str(),
                    b.created_at.as_str(),
                ))
        });
    }
    issues.sort_by(|a, b| a.id.cmp(&b.id));
}

fn issues_by_id(issues: &[IssueWire]) -> BTreeMap<String, IssueWire> {
    issues
        .iter()
        .map(|issue| (issue.id.clone(), issue.clone()))
        .collect()
}

fn compare_issue_id_sets(
    records: &mut Vec<ShadowDiffRecordWire>,
    source_path: &str,
    handle: &str,
    mut source_ids: Vec<String>,
    mut projected_ids: Vec<String>,
) {
    source_ids.sort();
    projected_ids.sort();
    if source_ids != projected_ids {
        records.push(diff_record(
            ShadowDiffCategoryWire::Stale,
            source_path,
            Some(handle),
            format!("projected bead {handle} result differs from source store"),
        ));
    }
}

fn diff_record(
    category: ShadowDiffCategoryWire,
    source_path: &str,
    handle: Option<&str>,
    message: impl Into<String>,
) -> ShadowDiffRecordWire {
    ShadowDiffRecordWire {
        schema_version: INDEXING_WIRE_SCHEMA_VERSION,
        domain: BEAD_INDEXING_DOMAIN.to_string(),
        category,
        source_path: source_path.to_string(),
        handle: handle.map(str::to_string),
        message: message.into(),
    }
}

fn diff_counts(records: &[ShadowDiffRecordWire]) -> ShadowDiffCountsWire {
    let mut counts = ShadowDiffCountsWire::default();
    for record in records {
        match record.category {
            ShadowDiffCategoryWire::Missing => counts.missing += 1,
            ShadowDiffCategoryWire::Stale => counts.stale += 1,
            ShadowDiffCategoryWire::Extra => counts.extra += 1,
            ShadowDiffCategoryWire::Corrupt => counts.corrupt += 1,
        }
    }
    counts
}

#[cfg(test)]
mod tests {
    use crate::bead::jsonl::parse_issues_jsonl;
    use crate::bead::wire::{BeadTierWire, DependencyWire};
    use crate::bead::work::build_epic_work_plan_from_issues;

    use super::*;

    const PROJECT_ID: &str = "project-a";

    fn context() -> BeadProjectionEventContextWire {
        BeadProjectionEventContextWire {
            created_at: Some("2026-05-13T21:00:00.000Z".to_string()),
            source: EventSourceWire {
                source_type: "test".to_string(),
                name: "bead-test".to_string(),
                ..EventSourceWire::default()
            },
            host_id: "host-a".to_string(),
            project_id: PROJECT_ID.to_string(),
            idempotency_key: None,
            causality: vec![],
            source_path: Some("sdd/beads/issues.jsonl".to_string()),
            source_revision: None,
        }
    }

    fn issue(
        id: &str,
        title: &str,
        issue_type: IssueTypeWire,
        parent_id: Option<&str>,
        status: StatusWire,
        created_at: &str,
    ) -> IssueWire {
        IssueWire {
            id: id.to_string(),
            title: title.to_string(),
            status,
            issue_type: issue_type.clone(),
            tier: (issue_type == IssueTypeWire::Plan)
                .then_some(BeadTierWire::Epic),
            parent_id: parent_id.map(str::to_string),
            owner: String::new(),
            assignee: String::new(),
            created_at: created_at.to_string(),
            created_by: String::new(),
            updated_at: created_at.to_string(),
            closed_at: None,
            close_reason: None,
            description: String::new(),
            notes: String::new(),
            design: String::new(),
            model: String::new(),
            is_ready_to_work: false,
            epic_count: None,
            changespec_name: String::new(),
            changespec_bug_id: String::new(),
            dependencies: vec![],
        }
    }

    #[test]
    fn snapshot_projection_matches_bead_read_helpers() {
        let input = r#"{"id":"gold-1","title":"Epic","status":"open","issue_type":"plan","tier":"epic","parent_id":null,"owner":"","assignee":"","created_at":"2026-01-01T00:00:00Z","created_by":"","updated_at":"2026-01-01T00:00:00Z","closed_at":null,"close_reason":null,"description":"","notes":"","design":"","model":"","is_ready_to_work":false,"changespec_name":"","changespec_bug_id":"","dependencies":[]}
{"id":"gold-1.1","title":"First","status":"open","issue_type":"phase","parent_id":"gold-1","owner":"","assignee":"","created_at":"2026-01-01T00:01:00Z","created_by":"","updated_at":"2026-01-01T00:01:00Z","closed_at":null,"close_reason":null,"description":"","notes":"","design":"","model":"","is_ready_to_work":false,"changespec_name":"","changespec_bug_id":"","dependencies":[]}
{"id":"gold-1.2","title":"Second","status":"open","issue_type":"phase","parent_id":"gold-1","owner":"","assignee":"","created_at":"2026-01-01T00:02:00Z","created_by":"","updated_at":"2026-01-01T00:02:00Z","closed_at":null,"close_reason":null,"description":"","notes":"","design":"","model":"","is_ready_to_work":false,"changespec_name":"","changespec_bug_id":"","dependencies":[{"issue_id":"gold-1.2","depends_on_id":"gold-1.1","created_at":"2026-01-01T00:02:00Z","created_by":""}]}
"#;
        let outcome = parse_issues_jsonl(input);
        let expected = outcome.issues.clone();
        let mut db = ProjectionDb::open_in_memory().unwrap();
        db.append_bead_event(
            bead_snapshot_observed_event_request(context(), outcome).unwrap(),
        )
        .unwrap();

        assert_eq!(
            bead_projection_list(db.connection(), PROJECT_ID, None, None, None)
                .unwrap(),
            list_issues_in_issues(expected.clone(), None, None, None).unwrap()
        );
        assert_eq!(
            bead_projection_ready(db.connection(), PROJECT_ID).unwrap(),
            ready_issues_in_issues(expected.clone()).unwrap()
        );
        assert_eq!(
            bead_projection_blocked(db.connection(), PROJECT_ID).unwrap(),
            blocked_issues_in_issues(expected.clone()).unwrap()
        );
        assert_eq!(
            bead_projection_epic_children(
                db.connection(),
                PROJECT_ID,
                "gold-1"
            )
            .unwrap(),
            get_epic_children_in_issues(expected.clone(), "gold-1").unwrap()
        );
        assert_eq!(
            bead_projection_stats(db.connection(), PROJECT_ID).unwrap(),
            stats_for_issues(&expected)
        );
    }

    #[test]
    fn dependency_events_drive_ready_blocked_and_work_plan_behavior() {
        let mut db = ProjectionDb::open_in_memory().unwrap();
        let epic = issue(
            "gold-1",
            "Epic",
            IssueTypeWire::Plan,
            None,
            StatusWire::Open,
            "2026-01-01T00:00:00Z",
        );
        let first = issue(
            "gold-1.1",
            "First",
            IssueTypeWire::Phase,
            Some("gold-1"),
            StatusWire::Open,
            "2026-01-01T00:01:00Z",
        );
        let second = issue(
            "gold-1.2",
            "Second",
            IssueTypeWire::Phase,
            Some("gold-1"),
            StatusWire::Open,
            "2026-01-01T00:02:00Z",
        );
        for issue in [epic.clone(), first.clone(), second.clone()] {
            db.append_bead_event(
                bead_created_event_request(context(), issue).unwrap(),
            )
            .unwrap();
        }
        let dependency = DependencyWire {
            issue_id: second.id.clone(),
            depends_on_id: first.id.clone(),
            created_at: "2026-01-01T00:03:00Z".to_string(),
            created_by: String::new(),
        };
        db.append_bead_event(
            bead_dependency_added_event_request(context(), dependency.clone())
                .unwrap(),
        )
        .unwrap();

        assert_eq!(
            bead_projection_ready(db.connection(), PROJECT_ID)
                .unwrap()
                .into_iter()
                .map(|issue| issue.id)
                .collect::<Vec<_>>(),
            vec!["gold-1".to_string(), "gold-1.1".to_string()]
        );
        assert_eq!(
            bead_projection_blocked(db.connection(), PROJECT_ID)
                .unwrap()
                .into_iter()
                .map(|issue| issue.id)
                .collect::<Vec<_>>(),
            vec!["gold-1.2".to_string()]
        );

        let projected_plan = bead_projection_epic_work_plan(
            db.connection(),
            PROJECT_ID,
            "gold-1",
        )
        .unwrap();
        let mut expected_second = second.clone();
        expected_second.dependencies = vec![dependency];
        let expected_plan = build_epic_work_plan_from_issues(
            vec![epic, first, expected_second],
            "gold-1",
        )
        .unwrap();
        assert_eq!(projected_plan, expected_plan);
    }

    #[test]
    fn replayed_mutation_events_match_live_projection_rows() {
        let mut db = ProjectionDb::open_in_memory().unwrap();
        let epic = issue(
            "gold-1",
            "Epic",
            IssueTypeWire::Plan,
            None,
            StatusWire::Open,
            "2026-01-01T00:00:00Z",
        );
        let phase = issue(
            "gold-1.1",
            "Phase",
            IssueTypeWire::Phase,
            Some("gold-1"),
            StatusWire::Open,
            "2026-01-01T00:01:00Z",
        );
        db.append_bead_event(
            bead_created_event_request(context(), epic).unwrap(),
        )
        .unwrap();
        db.append_bead_event(
            bead_created_event_request(context(), phase.clone()).unwrap(),
        )
        .unwrap();

        let mut closed_phase = phase;
        closed_phase.status = StatusWire::Closed;
        closed_phase.closed_at = Some("2026-01-01T00:02:00Z".to_string());
        closed_phase.updated_at = "2026-01-01T00:02:00Z".to_string();
        db.append_bead_event(
            bead_closed_event_request(context(), vec![closed_phase]).unwrap(),
        )
        .unwrap();
        let live = bead_projection_issues(db.connection(), PROJECT_ID).unwrap();

        db.connection()
            .execute("DELETE FROM bead_dependencies", [])
            .unwrap();
        db.connection()
            .execute("DELETE FROM bead_events", [])
            .unwrap();
        db.connection().execute("DELETE FROM beads", []).unwrap();
        let mut applier = BeadProjectionApplier;
        db.replay_events(0, &mut [&mut applier]).unwrap();

        assert_eq!(
            bead_projection_issues(db.connection(), PROJECT_ID).unwrap(),
            live
        );
        assert_eq!(
            bead_projection_events(db.connection(), PROJECT_ID, None)
                .unwrap()
                .len(),
            3
        );
    }

    #[test]
    fn dependency_remove_and_bead_remove_update_edges() {
        let mut db = ProjectionDb::open_in_memory().unwrap();
        let first = issue(
            "gold-1.1",
            "First",
            IssueTypeWire::Phase,
            Some("gold-1"),
            StatusWire::Open,
            "2026-01-01T00:01:00Z",
        );
        let second = issue(
            "gold-1.2",
            "Second",
            IssueTypeWire::Phase,
            Some("gold-1"),
            StatusWire::Open,
            "2026-01-01T00:02:00Z",
        );
        for issue in [first.clone(), second.clone()] {
            db.append_bead_event(
                bead_created_event_request(context(), issue).unwrap(),
            )
            .unwrap();
        }
        db.append_bead_event(
            bead_dependency_added_event_request(
                context(),
                DependencyWire {
                    issue_id: second.id.clone(),
                    depends_on_id: first.id.clone(),
                    created_at: "2026-01-01T00:03:00Z".to_string(),
                    created_by: String::new(),
                },
            )
            .unwrap(),
        )
        .unwrap();
        db.append_bead_event(
            bead_dependency_removed_event_request(
                context(),
                second.id.clone(),
                first.id.clone(),
            )
            .unwrap(),
        )
        .unwrap();
        assert!(bead_projection_blocked(db.connection(), PROJECT_ID)
            .unwrap()
            .is_empty());

        db.append_bead_event(
            bead_removed_event_request(context(), vec![first.id.clone()])
                .unwrap(),
        )
        .unwrap();
        assert!(bead_projection_show(db.connection(), PROJECT_ID, &first.id)
            .is_err());
    }
}
