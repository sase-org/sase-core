use super::cursors::StoreCursorWire;
use super::error::FleetContractError;
use super::error::FLEET_CATALOG_CURSOR_PREFIX;
use super::error::FLEET_CATALOG_SNAPSHOT_ID_PREFIX;
use super::error::FLEET_CONTRACT_SCHEMA_VERSION;
use super::error::FLEET_READ_DEFAULT_PAGE_ROWS;
use super::error::FLEET_READ_MAX_BATCH_IDS;
use super::error::FLEET_READ_MAX_FILTER_BYTES;
use super::error::FLEET_READ_MAX_PAGE_ROWS;
use super::error::FLEET_READ_MAX_PROJECT_IDS;
use super::error::FLEET_READ_MAX_QUERY_BYTES;
use super::error::MAX_KEY_BYTES;
use super::projection::terminal_lifecycle;
use super::reads::validate_fleet_logical_agent_counts;
use super::reads::FleetLogicalAgentCountsWire;
use super::resolution::validate_resolved_agent_summary;
use super::resolution::ResolvedAgentSummaryWire;
use super::snapshot::fleet_count_revision;
use super::snapshot::validate_fleet_snapshot_freshness;
use super::snapshot::FleetSnapshotFreshnessWire;
use super::status::FleetStatusBucketWire;
use super::validation::reject_path_like;
use super::validation::reject_secretish;
use super::validation::validate_key;
use super::validation::validate_label;
use super::validation::validate_reference_id;
use super::validation::validate_schema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FleetCatalogScopeWire {
    #[default]
    Presentation,
    History,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetCatalogQueryWire {
    pub schema_version: u32,
    /// Served catalog scope. Missing scope is a safe presentation request.
    #[serde(default)]
    pub scope: FleetCatalogScopeWire,
    /// Optional snapshot evidence from a previous page. Continuation cursors
    /// also carry this ID and are authoritative for their own offset.
    #[serde(default)]
    pub snapshot_id: Option<String>,
    /// Opaque offset cursor returned by a previous catalog page.
    pub cursor: Option<String>,
    /// Requested row limit. `None` means [`FLEET_READ_DEFAULT_PAGE_ROWS`].
    pub limit: Option<u32>,
    #[serde(default)]
    pub project_ids: Vec<String>,
    pub query: Option<String>,
    #[serde(default)]
    pub status_buckets: Vec<FleetStatusBucketWire>,
    #[serde(default)]
    pub include_terminal: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetCatalogPageSelectionWire {
    pub schema_version: u32,
    pub scope: FleetCatalogScopeWire,
    pub snapshot_id: String,
    pub rows: Vec<ResolvedAgentSummaryWire>,
    pub limit: u32,
    pub total_matching_rows: u64,
    pub next_cursor: Option<String>,
    pub has_more: bool,
    pub state: FleetCatalogContinuationStateWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reset_reason: Option<FleetCatalogResetReasonWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetCatalogPageWire {
    pub schema_version: u32,
    pub cursor: StoreCursorWire,
    pub counts: FleetLogicalAgentCountsWire,
    pub count_revision: Option<u64>,
    pub freshness: FleetSnapshotFreshnessWire,
    pub page: FleetCatalogPageSelectionWire,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetLogicalBatchRequestWire {
    pub schema_version: u32,
    pub logical_keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetLogicalBatchEntryWire {
    pub schema_version: u32,
    pub requested_logical_key: String,
    pub summary: Option<ResolvedAgentSummaryWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetLogicalBatchResponseWire {
    pub schema_version: u32,
    pub cursor: StoreCursorWire,
    pub counts: FleetLogicalAgentCountsWire,
    pub count_revision: Option<u64>,
    pub freshness: FleetSnapshotFreshnessWire,
    pub entries: Vec<FleetLogicalBatchEntryWire>,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FleetCatalogContinuationStateWire {
    Ready,
    Finished,
    ResyncRequired,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FleetCatalogResetReasonWire {
    ScopeMismatch,
    SnapshotMismatch,
    RestartRequired,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetCatalogContinuationWire {
    pub schema_version: u32,
    pub snapshot_cursor: Option<StoreCursorWire>,
    pub scope: FleetCatalogScopeWire,
    pub snapshot_id: String,
    pub limit: u32,
    pub total_matching_rows: u64,
    pub next_cursor: Option<String>,
    pub has_more: bool,
    pub state: FleetCatalogContinuationStateWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reset_reason: Option<FleetCatalogResetReasonWire>,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FleetCatalogAccumulationActionWire {
    IgnoredOlderRequest,
    Replaced,
    Merged,
    RestartRequired,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetCatalogAccumulationStateWire {
    pub schema_version: u32,
    pub request_generation: u64,
    pub scope: FleetCatalogScopeWire,
    pub snapshot_id: Option<String>,
    pub rows: Vec<ResolvedAgentSummaryWire>,
    pub snapshot_cursor: Option<StoreCursorWire>,
    pub counts: Option<FleetLogicalAgentCountsWire>,
    pub count_revision: Option<u64>,
    pub freshness: Option<FleetSnapshotFreshnessWire>,
    pub limit: u32,
    pub total_matching_rows: u64,
    pub next_cursor: Option<String>,
    pub has_more: bool,
    pub continuation_state: FleetCatalogContinuationStateWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reset_reason: Option<FleetCatalogResetReasonWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetCatalogAccumulationRequestWire {
    pub schema_version: u32,
    pub current: Option<FleetCatalogAccumulationStateWire>,
    pub request_generation: u64,
    pub requested_scope: FleetCatalogScopeWire,
    pub requested_snapshot_id: Option<String>,
    pub requested_cursor: Option<String>,
    pub incoming: FleetCatalogPageWire,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetCatalogAccumulationDecisionWire {
    pub schema_version: u32,
    pub action: FleetCatalogAccumulationActionWire,
    pub state: FleetCatalogAccumulationStateWire,
}

pub fn validate_fleet_catalog_query(
    query: &FleetCatalogQueryWire,
) -> Result<FleetCatalogQueryWire, FleetContractError> {
    validate_schema("fleet catalog query", query.schema_version)?;
    if let Some(snapshot_id) = &query.snapshot_id {
        validate_fleet_catalog_snapshot_id(snapshot_id)?;
    }
    if let Some(cursor) = &query.cursor {
        parse_catalog_cursor(cursor)?;
    }
    normalize_catalog_limit(query.limit)?;
    validate_identifier_vec(
        "project_ids",
        &query.project_ids,
        FLEET_READ_MAX_PROJECT_IDS,
        FLEET_READ_MAX_FILTER_BYTES,
    )?;
    if let Some(text) = &query.query {
        validate_label(
            "fleet catalog query",
            text,
            FLEET_READ_MAX_QUERY_BYTES,
        )?;
        reject_secretish("fleet catalog query", text)?;
    }
    let mut prior = None;
    for bucket in &query.status_buckets {
        if prior.is_some_and(|value| value >= *bucket) {
            return Err(FleetContractError::Validation(
                "fleet catalog status_buckets must be sorted and deduplicated"
                    .to_string(),
            ));
        }
        prior = Some(*bucket);
    }
    Ok(query.clone())
}

pub fn validate_fleet_catalog_cursor(
    cursor: &str,
) -> Result<String, FleetContractError> {
    parse_catalog_cursor(cursor)?;
    Ok(cursor.to_string())
}

pub fn validate_fleet_catalog_snapshot_id(
    snapshot_id: &str,
) -> Result<String, FleetContractError> {
    validate_reference_id("fleet catalog snapshot_id", snapshot_id)?;
    reject_path_like("fleet catalog snapshot_id", snapshot_id)?;
    let Some(suffix) =
        snapshot_id.strip_prefix(FLEET_CATALOG_SNAPSHOT_ID_PREFIX)
    else {
        return Err(FleetContractError::Validation(
            "fleet catalog snapshot_id is not recognized".to_string(),
        ));
    };
    if suffix.len() != 64
        || !suffix
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(FleetContractError::Validation(
            "fleet catalog snapshot_id must end with 64 lowercase hex characters"
                .to_string(),
        ));
    }
    Ok(snapshot_id.to_string())
}

pub fn fleet_catalog_snapshot_id(
    scope: FleetCatalogScopeWire,
    summaries: &[ResolvedAgentSummaryWire],
) -> Result<String, FleetContractError> {
    let mut rows = summaries
        .iter()
        .map(|summary| {
            let summary = validate_resolved_agent_summary(summary)?;
            Ok::<_, FleetContractError>(catalog_snapshot_summary_value(
                &summary,
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    rows.sort_by(|left, right| {
        let left_key = (
            left.get("logical_key")
                .and_then(Value::as_str)
                .unwrap_or(""),
            left.get("exact_key").and_then(Value::as_str).unwrap_or(""),
        );
        let right_key = (
            right
                .get("logical_key")
                .and_then(Value::as_str)
                .unwrap_or(""),
            right.get("exact_key").and_then(Value::as_str).unwrap_or(""),
        );
        left_key.cmp(&right_key)
    });
    let payload = json!({
        "domain": "sase-fleet-catalog-snapshot-v1",
        "scope": scope,
        "rows": rows,
    });
    let bytes = serde_json::to_vec(&payload).map_err(|error| {
        FleetContractError::Validation(format!(
            "fleet catalog snapshot could not be serialized: {error}"
        ))
    })?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    Ok(format!(
        "{}{}",
        FLEET_CATALOG_SNAPSHOT_ID_PREFIX,
        hex::encode(hasher.finalize())
    ))
}

pub fn select_fleet_catalog_page(
    query: &FleetCatalogQueryWire,
    summaries: &[ResolvedAgentSummaryWire],
) -> Result<FleetCatalogPageSelectionWire, FleetContractError> {
    let query = validate_fleet_catalog_query(query)?;
    let limit = normalize_catalog_limit(query.limit)?;
    let snapshot_id = fleet_catalog_snapshot_id(query.scope, summaries)?;
    let cursor = query
        .cursor
        .as_deref()
        .map(parse_catalog_cursor)
        .transpose()?;
    let requested_scope = cursor
        .as_ref()
        .map(|cursor| cursor.scope)
        .unwrap_or(query.scope);
    let requested_snapshot_id = cursor
        .as_ref()
        .map(|cursor| cursor.snapshot_id.as_str())
        .or(query.snapshot_id.as_deref());
    if requested_scope != query.scope {
        return Ok(fleet_catalog_restart_page(
            query.scope,
            snapshot_id,
            limit,
            FleetCatalogResetReasonWire::ScopeMismatch,
        ));
    }
    if requested_snapshot_id.is_some_and(|requested| requested != snapshot_id) {
        return Ok(fleet_catalog_restart_page(
            query.scope,
            snapshot_id,
            limit,
            FleetCatalogResetReasonWire::SnapshotMismatch,
        ));
    }
    let start = cursor.map(|cursor| cursor.offset).unwrap_or(0);
    let mut rows = Vec::new();
    for summary in summaries {
        let summary = validate_resolved_agent_summary(summary)?;
        if !summary_matches_catalog_query(&summary, &query)? {
            continue;
        }
        rows.push(summary);
    }
    rows.sort_by(compare_catalog_summaries);
    let total_matching_rows = rows.len() as u64;
    let start = start.min(rows.len());
    let end = start.saturating_add(limit as usize).min(rows.len());
    let has_more = end < rows.len();
    let next_cursor =
        has_more.then(|| format_catalog_cursor(query.scope, &snapshot_id, end));
    Ok(FleetCatalogPageSelectionWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        scope: query.scope,
        snapshot_id,
        rows: rows[start..end].to_vec(),
        limit,
        total_matching_rows,
        next_cursor,
        has_more,
        state: if has_more {
            FleetCatalogContinuationStateWire::Ready
        } else {
            FleetCatalogContinuationStateWire::Finished
        },
        reset_reason: None,
    })
}

pub fn accumulate_fleet_catalog_page(
    request: &FleetCatalogAccumulationRequestWire,
) -> Result<FleetCatalogAccumulationDecisionWire, FleetContractError> {
    validate_schema(
        "fleet catalog accumulation request",
        request.schema_version,
    )?;
    if let Some(current) = &request.current {
        validate_fleet_catalog_accumulation_state(current)?;
        if request.request_generation < current.request_generation {
            return Ok(FleetCatalogAccumulationDecisionWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                action: FleetCatalogAccumulationActionWire::IgnoredOlderRequest,
                state: current.clone(),
            });
        }
    }
    if let Some(snapshot_id) = &request.requested_snapshot_id {
        validate_fleet_catalog_snapshot_id(snapshot_id)?;
    }
    let parsed_cursor = request
        .requested_cursor
        .as_deref()
        .map(parse_catalog_cursor)
        .transpose()?;
    if parsed_cursor
        .as_ref()
        .is_some_and(|cursor| cursor.scope != request.requested_scope)
    {
        return Ok(catalog_accumulation_restart(
            request,
            FleetCatalogResetReasonWire::ScopeMismatch,
        ));
    }
    let incoming = validate_fleet_catalog_page_wire(&request.incoming)?;
    let incoming_page = &incoming.page;
    if incoming_page.scope != request.requested_scope {
        return Ok(catalog_accumulation_restart(
            request,
            FleetCatalogResetReasonWire::ScopeMismatch,
        ));
    }
    let requested_snapshot_id = parsed_cursor
        .as_ref()
        .map(|cursor| cursor.snapshot_id.as_str())
        .or(request.requested_snapshot_id.as_deref());
    if requested_snapshot_id
        .is_some_and(|snapshot_id| snapshot_id != incoming_page.snapshot_id)
    {
        return Ok(catalog_accumulation_restart(
            request,
            FleetCatalogResetReasonWire::SnapshotMismatch,
        ));
    }
    if incoming_page.state == FleetCatalogContinuationStateWire::ResyncRequired
    {
        return Ok(catalog_accumulation_restart(
            request,
            incoming_page
                .reset_reason
                .unwrap_or(FleetCatalogResetReasonWire::RestartRequired),
        ));
    }

    let is_initial_request = parsed_cursor.is_none();
    if is_initial_request {
        let rows = incoming_page.rows.clone();
        return Ok(FleetCatalogAccumulationDecisionWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            action: FleetCatalogAccumulationActionWire::Replaced,
            state: catalog_accumulation_state_from_page(
                request.request_generation,
                incoming,
                rows,
            )?,
        });
    }

    let Some(current) = &request.current else {
        return Ok(catalog_accumulation_restart(
            request,
            FleetCatalogResetReasonWire::RestartRequired,
        ));
    };
    if current.scope != incoming_page.scope
        || current.snapshot_id.as_deref() != Some(&incoming_page.snapshot_id)
    {
        return Ok(catalog_accumulation_restart(
            request,
            FleetCatalogResetReasonWire::SnapshotMismatch,
        ));
    }
    let rows = merge_catalog_rows(&current.rows, &incoming_page.rows)?;
    Ok(FleetCatalogAccumulationDecisionWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        action: FleetCatalogAccumulationActionWire::Merged,
        state: catalog_accumulation_state_from_page(
            request.request_generation,
            incoming,
            rows,
        )?,
    })
}

fn catalog_accumulation_restart(
    request: &FleetCatalogAccumulationRequestWire,
    reset_reason: FleetCatalogResetReasonWire,
) -> FleetCatalogAccumulationDecisionWire {
    FleetCatalogAccumulationDecisionWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        action: FleetCatalogAccumulationActionWire::RestartRequired,
        state: FleetCatalogAccumulationStateWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            request_generation: request.request_generation,
            scope: request.requested_scope,
            snapshot_id: None,
            rows: Vec::new(),
            snapshot_cursor: None,
            counts: None,
            count_revision: None,
            freshness: None,
            limit: request
                .incoming
                .page
                .limit
                .clamp(1, FLEET_READ_MAX_PAGE_ROWS),
            total_matching_rows: 0,
            next_cursor: None,
            has_more: false,
            continuation_state:
                FleetCatalogContinuationStateWire::ResyncRequired,
            reset_reason: Some(reset_reason),
        },
    }
}

fn catalog_accumulation_state_from_page(
    request_generation: u64,
    page: FleetCatalogPageWire,
    rows: Vec<ResolvedAgentSummaryWire>,
) -> Result<FleetCatalogAccumulationStateWire, FleetContractError> {
    Ok(FleetCatalogAccumulationStateWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        request_generation,
        scope: page.page.scope,
        snapshot_id: Some(page.page.snapshot_id),
        rows,
        snapshot_cursor: Some(page.cursor),
        counts: Some(page.counts),
        count_revision: page.count_revision,
        freshness: Some(page.freshness),
        limit: page.page.limit,
        total_matching_rows: page.page.total_matching_rows,
        next_cursor: page.page.next_cursor,
        has_more: page.page.has_more,
        continuation_state: page.page.state,
        reset_reason: page.page.reset_reason,
    })
}

fn merge_catalog_rows(
    current: &[ResolvedAgentSummaryWire],
    incoming: &[ResolvedAgentSummaryWire],
) -> Result<Vec<ResolvedAgentSummaryWire>, FleetContractError> {
    let mut rows = BTreeMap::<String, ResolvedAgentSummaryWire>::new();
    for summary in current.iter().chain(incoming) {
        let summary = validate_resolved_agent_summary(summary)?;
        let key = catalog_row_identity(&summary);
        match rows.get(&key) {
            Some(existing)
                if existing.row_revision.revision
                    >= summary.row_revision.revision => {}
            _ => {
                rows.insert(key, summary);
            }
        }
    }
    let mut rows = rows.into_values().collect::<Vec<_>>();
    rows.sort_by(compare_catalog_summaries);
    Ok(rows)
}

fn catalog_row_identity(summary: &ResolvedAgentSummaryWire) -> String {
    format!(
        "{}\0{}",
        summary.logical_key,
        summary.exact_key.as_deref().unwrap_or("")
    )
}

fn validate_fleet_catalog_page_wire(
    page: &FleetCatalogPageWire,
) -> Result<FleetCatalogPageWire, FleetContractError> {
    validate_schema("fleet catalog page", page.schema_version)?;
    page.cursor.validate()?;
    validate_fleet_logical_agent_counts(
        &page.counts,
        "fleet catalog page counts",
    )?;
    if page.count_revision != fleet_count_revision(&page.counts) {
        return Err(FleetContractError::Validation(
            "fleet catalog page count_revision does not match counts"
                .to_string(),
        ));
    }
    validate_fleet_snapshot_freshness(&page.freshness)?;
    validate_fleet_catalog_page_selection(&page.page)?;
    Ok(page.clone())
}

fn validate_fleet_catalog_page_selection(
    page: &FleetCatalogPageSelectionWire,
) -> Result<FleetCatalogPageSelectionWire, FleetContractError> {
    validate_schema("fleet catalog page selection", page.schema_version)?;
    validate_fleet_catalog_snapshot_id(&page.snapshot_id)?;
    normalize_catalog_limit(Some(page.limit))?;
    for row in &page.rows {
        validate_resolved_agent_summary(row)?;
    }
    match page.state {
        FleetCatalogContinuationStateWire::Ready => {
            if !page.has_more || page.next_cursor.is_none() {
                return Err(FleetContractError::Validation(
                    "fleet catalog ready page requires has_more and next_cursor"
                        .to_string(),
                ));
            }
        }
        FleetCatalogContinuationStateWire::Finished => {
            if page.has_more || page.next_cursor.is_some() {
                return Err(FleetContractError::Validation(
                    "fleet catalog finished page must not have a continuation"
                        .to_string(),
                ));
            }
        }
        FleetCatalogContinuationStateWire::ResyncRequired => {
            if page.has_more
                || page.next_cursor.is_some()
                || !page.rows.is_empty()
                || page.reset_reason.is_none()
            {
                return Err(FleetContractError::Validation(
                    "fleet catalog resync page must have no rows or continuation and must carry a reset_reason"
                        .to_string(),
                ));
            }
        }
    }
    if let Some(cursor) = &page.next_cursor {
        let cursor = parse_catalog_cursor(cursor)?;
        if cursor.scope != page.scope || cursor.snapshot_id != page.snapshot_id
        {
            return Err(FleetContractError::Validation(
                "fleet catalog next_cursor does not match page scope and snapshot_id"
                    .to_string(),
            ));
        }
    }
    Ok(page.clone())
}

fn validate_fleet_catalog_accumulation_state(
    state: &FleetCatalogAccumulationStateWire,
) -> Result<FleetCatalogAccumulationStateWire, FleetContractError> {
    validate_schema("fleet catalog accumulation state", state.schema_version)?;
    if let Some(snapshot_id) = &state.snapshot_id {
        validate_fleet_catalog_snapshot_id(snapshot_id)?;
    }
    if let Some(cursor) = &state.snapshot_cursor {
        cursor.validate()?;
    }
    if let Some(counts) = &state.counts {
        validate_fleet_logical_agent_counts(
            counts,
            "fleet catalog accumulation counts",
        )?;
    }
    if let Some(freshness) = &state.freshness {
        validate_fleet_snapshot_freshness(freshness)?;
    }
    normalize_catalog_limit(Some(state.limit))?;
    for row in &state.rows {
        validate_resolved_agent_summary(row)?;
    }
    if let Some(cursor) = &state.next_cursor {
        let parsed = parse_catalog_cursor(cursor)?;
        if parsed.scope != state.scope
            || state.snapshot_id.as_deref() != Some(parsed.snapshot_id.as_str())
        {
            return Err(FleetContractError::Validation(
                "fleet catalog accumulation next_cursor does not match state"
                    .to_string(),
            ));
        }
    }
    Ok(state.clone())
}

pub fn validate_fleet_logical_batch_request(
    request: &FleetLogicalBatchRequestWire,
) -> Result<FleetLogicalBatchRequestWire, FleetContractError> {
    validate_schema("fleet logical batch request", request.schema_version)?;
    validate_identifier_vec(
        "logical_keys",
        &request.logical_keys,
        FLEET_READ_MAX_BATCH_IDS,
        MAX_KEY_BYTES,
    )?;
    let mut seen = BTreeSet::new();
    for logical_key in &request.logical_keys {
        validate_key("logical_key", logical_key)?;
        if !seen.insert(logical_key) {
            return Err(FleetContractError::Validation(
                "fleet logical batch request contains duplicate logical_key"
                    .to_string(),
            ));
        }
    }
    Ok(request.clone())
}

pub fn select_fleet_logical_batch(
    request: &FleetLogicalBatchRequestWire,
    summaries: &[ResolvedAgentSummaryWire],
) -> Result<Vec<FleetLogicalBatchEntryWire>, FleetContractError> {
    let request = validate_fleet_logical_batch_request(request)?;
    let mut by_key = BTreeMap::new();
    for summary in summaries {
        let summary = validate_resolved_agent_summary(summary)?;
        by_key.insert(summary.logical_key.clone(), summary);
    }
    Ok(request
        .logical_keys
        .iter()
        .map(|logical_key| FleetLogicalBatchEntryWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            requested_logical_key: logical_key.clone(),
            summary: by_key.get(logical_key).cloned(),
        })
        .collect())
}

pub(crate) fn normalize_catalog_limit(
    limit: Option<u32>,
) -> Result<u32, FleetContractError> {
    let limit = limit.unwrap_or(FLEET_READ_DEFAULT_PAGE_ROWS);
    if limit == 0 {
        return Err(FleetContractError::Validation(
            "fleet catalog limit must be positive".to_string(),
        ));
    }
    if limit > FLEET_READ_MAX_PAGE_ROWS {
        return Err(FleetContractError::Validation(format!(
            "fleet catalog limit exceeds {FLEET_READ_MAX_PAGE_ROWS}"
        )));
    }
    Ok(limit)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedCatalogCursor {
    pub(crate) scope: FleetCatalogScopeWire,
    pub(crate) snapshot_id: String,
    pub(crate) offset: usize,
}

pub(crate) fn parse_catalog_cursor(
    cursor: &str,
) -> Result<ParsedCatalogCursor, FleetContractError> {
    validate_reference_id("fleet catalog cursor", cursor)?;
    reject_path_like("fleet catalog cursor", cursor)?;
    let Some(body) = cursor.strip_prefix(FLEET_CATALOG_CURSOR_PREFIX) else {
        return Err(FleetContractError::Validation(
            "fleet catalog cursor is not recognized".to_string(),
        ));
    };
    let Some(body) = body.strip_prefix(':') else {
        return Err(FleetContractError::Validation(
            "fleet catalog cursor is malformed".to_string(),
        ));
    };
    let mut parts = body.split(':');
    let Some(scope_token) = parts.next() else {
        return Err(FleetContractError::Validation(
            "fleet catalog cursor scope is missing".to_string(),
        ));
    };
    let Some(snapshot_id) = parts.next() else {
        return Err(FleetContractError::Validation(
            "fleet catalog cursor snapshot_id is missing".to_string(),
        ));
    };
    let Some(offset) = parts.next() else {
        return Err(FleetContractError::Validation(
            "fleet catalog cursor offset is missing".to_string(),
        ));
    };
    if parts.next().is_some() {
        return Err(FleetContractError::Validation(
            "fleet catalog cursor is malformed".to_string(),
        ));
    }
    let scope = match scope_token {
        "p" => FleetCatalogScopeWire::Presentation,
        "h" => FleetCatalogScopeWire::History,
        _ => {
            return Err(FleetContractError::Validation(
                "fleet catalog cursor scope is not recognized".to_string(),
            ))
        }
    };
    validate_fleet_catalog_snapshot_id(snapshot_id)?;
    if offset.is_empty()
        || !offset.bytes().all(|byte| byte.is_ascii_digit())
        || (offset.len() > 1 && offset.starts_with('0'))
    {
        return Err(FleetContractError::Validation(
            "fleet catalog cursor offset is malformed".to_string(),
        ));
    }
    let offset = offset.parse::<usize>().map_err(|_| {
        FleetContractError::Validation(
            "fleet catalog cursor offset is out of range".to_string(),
        )
    })?;
    Ok(ParsedCatalogCursor {
        scope,
        snapshot_id: snapshot_id.to_string(),
        offset,
    })
}

fn format_catalog_cursor(
    scope: FleetCatalogScopeWire,
    snapshot_id: &str,
    offset: usize,
) -> String {
    let scope = match scope {
        FleetCatalogScopeWire::Presentation => "p",
        FleetCatalogScopeWire::History => "h",
    };
    format!("{FLEET_CATALOG_CURSOR_PREFIX}:{scope}:{snapshot_id}:{offset}")
}

fn fleet_catalog_restart_page(
    scope: FleetCatalogScopeWire,
    snapshot_id: String,
    limit: u32,
    reset_reason: FleetCatalogResetReasonWire,
) -> FleetCatalogPageSelectionWire {
    FleetCatalogPageSelectionWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        scope,
        snapshot_id,
        rows: Vec::new(),
        limit,
        total_matching_rows: 0,
        next_cursor: None,
        has_more: false,
        state: FleetCatalogContinuationStateWire::ResyncRequired,
        reset_reason: Some(reset_reason),
    }
}

fn catalog_snapshot_summary_value(summary: &ResolvedAgentSummaryWire) -> Value {
    let mut value = serde_json::to_value(summary).unwrap_or_else(|_| {
        json!({
            "logical_key": summary.logical_key,
            "exact_key": summary.exact_key,
            "row_revision": summary.row_revision.revision,
        })
    });
    if let Value::Object(object) = &mut value {
        object.remove("observed_at_unix");
        object.remove("freshness");
    }
    value
}

pub(crate) fn validate_identifier_vec(
    field: &str,
    values: &[String],
    max_count: usize,
    max_bytes: usize,
) -> Result<(), FleetContractError> {
    if values.len() > max_count {
        return Err(FleetContractError::Validation(format!(
            "{field} exceeds {max_count} entries"
        )));
    }
    let mut seen = BTreeSet::new();
    for value in values {
        validate_label(field, value, max_bytes)?;
        reject_secretish(field, value)?;
        if !seen.insert(value.trim()) {
            return Err(FleetContractError::Validation(format!(
                "{field} contains duplicate entry"
            )));
        }
    }
    Ok(())
}

fn summary_matches_catalog_query(
    summary: &ResolvedAgentSummaryWire,
    query: &FleetCatalogQueryWire,
) -> Result<bool, FleetContractError> {
    if !query.project_ids.is_empty()
        && !query.project_ids.iter().any(|project| {
            project == &summary.logical_locator.project.project_id
        })
    {
        return Ok(false);
    }
    if !query.include_terminal && terminal_lifecycle(summary.lifecycle) {
        return Ok(false);
    }
    if !query.status_buckets.is_empty()
        && !query.status_buckets.contains(&summary.status_bucket)
    {
        return Ok(false);
    }
    if let Some(text) = query.query.as_deref().map(str::trim) {
        if text.is_empty() {
            return Ok(true);
        }
        let needle = text.to_ascii_lowercase();
        let haystacks = [
            Some(summary.project_name.as_str()),
            Some(summary.status.as_str()),
            summary.model.as_deref(),
            summary.provider.as_deref(),
            summary.intent.as_deref(),
            Some(summary.labels.project_label.as_str()),
            summary.labels.agent_label.as_deref(),
            summary.labels.agent_session_label.as_deref(),
            summary.labels.owner_label.as_deref(),
            summary.labels.alias.as_deref(),
        ];
        return Ok(haystacks
            .into_iter()
            .flatten()
            .any(|value| value.to_ascii_lowercase().contains(&needle)));
    }
    Ok(true)
}

fn compare_catalog_summaries(
    left: &ResolvedAgentSummaryWire,
    right: &ResolvedAgentSummaryWire,
) -> std::cmp::Ordering {
    let left_rank = catalog_status_rank(left);
    let right_rank = catalog_status_rank(right);
    left_rank
        .cmp(&right_rank)
        .then_with(|| left.project_name.cmp(&right.project_name))
        .then_with(|| {
            left.labels
                .agent_label
                .as_deref()
                .unwrap_or("")
                .cmp(right.labels.agent_label.as_deref().unwrap_or(""))
        })
        .then_with(|| {
            left.labels
                .agent_session_label
                .as_deref()
                .unwrap_or("")
                .cmp(right.labels.agent_session_label.as_deref().unwrap_or(""))
        })
        .then_with(|| right.observed_at_unix.total_cmp(&left.observed_at_unix))
        .then_with(|| left.logical_key.cmp(&right.logical_key))
}

fn catalog_status_rank(summary: &ResolvedAgentSummaryWire) -> u8 {
    if summary.needs_attention {
        return 0;
    }
    match summary.status_bucket {
        FleetStatusBucketWire::Running => 1,
        FleetStatusBucketWire::Starting => 2,
        FleetStatusBucketWire::Waiting => 3,
        FleetStatusBucketWire::Queued => 4,
        FleetStatusBucketWire::Failed => 5,
        FleetStatusBucketWire::Stopped => 6,
        FleetStatusBucketWire::Done => 7,
    }
}

pub(crate) fn actionable_capability(value: &str) -> bool {
    matches!(
        value,
        "approve" | "answer" | "kill" | "resume" | "retry" | "stop"
    )
}

pub(crate) fn content_capability(value: &str) -> bool {
    matches!(value, "content.read" | "content.tail" | "content.range")
}
