//! Artifact-link legacy-index cutover marker and migration policy.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};

#[cfg(test)]
use super::events::{
    artifact_link_event_canonical_json, artifact_link_event_path_for_digest,
};
use super::events::{
    artifact_link_stable_operation_id, canonical_json_for_serializable,
    canonical_json_for_value, canonicalize_artifact_link_event,
    ensure_integer_only_json, sha256_hex, validate_operation_id,
    validate_project_key, validate_sha256_digest, validate_single_line,
    ArtifactLinkEventEdgeWire, ArtifactLinkEventKindWire,
    ArtifactLinkEventWire, ARTIFACT_LINK_EVENT_WIRE_SCHEMA_VERSION,
};
use super::wire::{
    artifact_link_dedup_key, validate_artifact_link_row,
    ArtifactLinkDedupKeyWire, ArtifactLinkError, ArtifactLinkOriginWire,
    ArtifactLinkRowWire,
};

pub const ARTIFACT_LINK_CUTOVER_WIRE_SCHEMA_VERSION: u64 = 1;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactLinkCutoverStateWire {
    Fenced,
    Imported,
}

impl ArtifactLinkCutoverStateWire {
    fn as_str(self) -> &'static str {
        match self {
            Self::Fenced => "fenced",
            Self::Imported => "imported",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactLinkCutoverRoleWire {
    pub role: String,
    pub kind: String,
    pub head: String,
    pub links_tree: String,
    pub remote_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactLinkCutoverRoleInputWire {
    pub role: String,
    pub kind: String,
    pub head: String,
    pub links_tree: String,
    pub remote_url: String,
    pub commit_time: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactLinkCutoverImportIdentityWire {
    pub import_id: String,
    pub operation_id: String,
    pub source_head: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactLinkCutoverBaselineEventWire {
    pub digest: String,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactLinkCutoverEventStoreWire {
    pub schema_version: u64,
    pub minimum_event_schema_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactLinkCutoverMarkerWire {
    pub schema_version: u64,
    pub state: ArtifactLinkCutoverStateWire,
    pub project_key: String,
    pub event_store: ArtifactLinkCutoverEventStoreWire,
    #[serde(rename = "import")]
    pub import_identity: ArtifactLinkCutoverImportIdentityWire,
    pub roles: Vec<ArtifactLinkCutoverRoleWire>,
    pub baseline_event: ArtifactLinkCutoverBaselineEventWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactLinkCutoverImportRequestWire {
    pub project_key: String,
    pub roles: Vec<ArtifactLinkCutoverRoleInputWire>,
    pub rows: Vec<ArtifactLinkRowWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactLinkCutoverBaselineEventRequestWire {
    pub project_key: String,
    #[serde(rename = "import")]
    pub import_identity: ArtifactLinkCutoverImportIdentityWire,
    pub rows: Vec<ArtifactLinkRowWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactLinkCutoverReadRootWire {
    pub role: String,
    pub marker: Option<ArtifactLinkCutoverMarkerWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactLinkCutoverReadStateWire {
    pub schema_version: u64,
    pub state: String,
    pub marker: Option<ArtifactLinkCutoverMarkerWire>,
    pub incomplete_roles: Vec<String>,
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactLinkCutoverRootObservationWire {
    pub role: String,
    pub marker: Option<ArtifactLinkCutoverMarkerWire>,
    pub marker_committed: bool,
    pub baseline_durable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactLinkCutoverProgressRequestWire {
    pub expected: ArtifactLinkCutoverMarkerWire,
    pub roots: Vec<ArtifactLinkCutoverRootObservationWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactLinkCutoverProgressWire {
    pub schema_version: u64,
    pub phase: String,
    pub roles_needing_fence_marker: Vec<String>,
    pub roles_needing_baseline_event: Vec<String>,
    pub roles_needing_imported_marker: Vec<String>,
    pub conflicts: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactLinkOutboxLineClassificationWire {
    pub schema_version: u64,
    pub kind: String,
    pub operation_id: Option<String>,
    pub diagnostic: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactLinkOutboxLegacyConversionWire {
    pub schema_version: u64,
    pub outcome: String,
    pub event: Option<ArtifactLinkEventWire>,
}

type LegacyOutboxEntryParts = (
    JsonMap<String, JsonValue>,
    ArtifactLinkRowWire,
    String,
    String,
    String,
    String,
);

pub fn parse_artifact_link_cutover_marker(
    json: &str,
) -> Result<ArtifactLinkCutoverMarkerWire, ArtifactLinkError> {
    let value: JsonValue = serde_json::from_str(json).map_err(|error| {
        validation(format!(
            "artifact-link cutover marker JSON is invalid: {error}"
        ))
    })?;
    ensure_integer_only_json(&value, "$")?;
    let marker: ArtifactLinkCutoverMarkerWire = serde_json::from_value(value)
        .map_err(|error| {
        validation(format!(
            "artifact-link cutover marker has invalid fields: {error}"
        ))
    })?;
    validate_marker(&marker)
}

pub fn artifact_link_cutover_marker_canonical_json(
    marker: &ArtifactLinkCutoverMarkerWire,
) -> Result<String, ArtifactLinkError> {
    canonical_json_for_serializable(&validate_marker(marker)?)
}

pub fn artifact_link_cutover_import_identity(
    request: &ArtifactLinkCutoverImportRequestWire,
) -> Result<ArtifactLinkCutoverImportIdentityWire, ArtifactLinkError> {
    let project_key = validate_project_key(&request.project_key)?;
    let roles = canonical_role_inputs(&request.roles)?;
    let rows = canonical_rows(&request.rows)?;
    let marker_roles: Vec<ArtifactLinkCutoverRoleWire> =
        roles.iter().map(marker_role_from_input).collect();
    let source_head = format!(
        "sha256:{}",
        sha256_hex(
            canonical_json_for_value(&serde_json::json!({
                "project_key": project_key,
                "roles": marker_roles,
            }))?
            .as_bytes(),
        )
    );
    let import_digest = sha256_hex(
        canonical_json_for_value(&serde_json::json!({
            "project_key": project_key,
            "roles": marker_roles,
            "rows": rows,
            "source_head": source_head,
        }))?
        .as_bytes(),
    );
    let import_id = format!("legacy-v2-links-{}", &import_digest[..32]);
    let operation_id = artifact_link_stable_operation_id(&[
        serde_json::json!("baseline-import"),
        serde_json::json!(project_key),
        serde_json::json!(import_id),
        serde_json::json!(source_head),
    ])?;
    Ok(ArtifactLinkCutoverImportIdentityWire {
        import_id,
        operation_id,
        source_head,
        created_at: max_commit_time(&roles),
    })
}

pub fn artifact_link_cutover_baseline_event(
    request: &ArtifactLinkCutoverBaselineEventRequestWire,
) -> Result<ArtifactLinkEventWire, ArtifactLinkError> {
    let project_key = validate_project_key(&request.project_key)?;
    let identity = validate_import_identity(&request.import_identity)?;
    let rows = canonical_rows(&request.rows)?;
    canonicalize_artifact_link_event(&ArtifactLinkEventWire {
        schema_version: ARTIFACT_LINK_EVENT_WIRE_SCHEMA_VERSION,
        project_key,
        operation_id: identity.operation_id.clone(),
        created_by: "sase".to_string(),
        origin: ArtifactLinkOriginWire::Migrated,
        created_at: identity.created_at.clone(),
        kind: ArtifactLinkEventKindWire::BaselineImport {
            import_id: identity.import_id,
            source_head: identity.source_head,
            rows,
        },
    })
}

pub fn artifact_link_cutover_marker(
    state: ArtifactLinkCutoverStateWire,
    project_key: &str,
    event_store: &ArtifactLinkCutoverEventStoreWire,
    import_identity: &ArtifactLinkCutoverImportIdentityWire,
    roles: &[ArtifactLinkCutoverRoleWire],
    baseline_event: &ArtifactLinkCutoverBaselineEventWire,
) -> Result<ArtifactLinkCutoverMarkerWire, ArtifactLinkError> {
    validate_marker(&ArtifactLinkCutoverMarkerWire {
        schema_version: ARTIFACT_LINK_CUTOVER_WIRE_SCHEMA_VERSION,
        state,
        project_key: project_key.to_string(),
        event_store: event_store.clone(),
        import_identity: import_identity.clone(),
        roles: roles.to_vec(),
        baseline_event: baseline_event.clone(),
    })
}

pub fn artifact_link_cutover_attestation(
    marker: &ArtifactLinkCutoverMarkerWire,
) -> Result<String, ArtifactLinkError> {
    let mut fenced = validate_marker(marker)?;
    fenced.state = ArtifactLinkCutoverStateWire::Fenced;
    let canonical = artifact_link_cutover_marker_canonical_json(&fenced)?;
    Ok(format!(
        "fleet-capable-{}",
        &sha256_hex(canonical.as_bytes())[..12]
    ))
}

pub fn artifact_link_cutover_read_state(
    roots: &[ArtifactLinkCutoverReadRootWire],
) -> Result<ArtifactLinkCutoverReadStateWire, ArtifactLinkError> {
    let mut roots = canonical_read_roots(roots)?;
    let mut present = Vec::new();
    let mut missing = Vec::new();
    for root in roots.drain(..) {
        match root.marker {
            Some(marker) => {
                present.push((root.role, validate_marker(&marker)?))
            }
            None => missing.push(root.role),
        }
    }
    if present.is_empty() {
        return Ok(read_state("none", None, missing, Vec::new()));
    }
    if !missing.is_empty() {
        let mut incomplete = missing;
        incomplete.extend(present.iter().map(|(role, _)| role.clone()));
        incomplete.sort();
        incomplete.dedup();
        return Ok(read_state(
            "incomplete",
            Some(present[0].1.clone()),
            incomplete,
            vec!["artifact-link cutover markers are partial".to_string()],
        ));
    }
    let canonical_payloads = marker_payload_signatures(&present)?;
    let states: BTreeSet<ArtifactLinkCutoverStateWire> =
        present.iter().map(|(_, marker)| marker.state).collect();
    if canonical_payloads.len() == 1 && states.len() == 1 {
        return Ok(read_state(
            present[0].1.state.as_str(),
            Some(present[0].1.clone()),
            Vec::new(),
            Vec::new(),
        ));
    }
    let roles = present.iter().map(|(role, _)| role.clone()).collect();
    Ok(read_state(
        "incomplete",
        Some(present[0].1.clone()),
        roles,
        vec!["artifact-link cutover markers are inconsistent".to_string()],
    ))
}

pub fn artifact_link_cutover_progress(
    request: &ArtifactLinkCutoverProgressRequestWire,
) -> Result<ArtifactLinkCutoverProgressWire, ArtifactLinkError> {
    let expected = validate_marker(&request.expected)?;
    let observations = canonical_observations(&request.roots)?;
    let mut conflicts = Vec::new();
    for observation in &observations {
        if let Some(marker) = &observation.marker {
            let marker = validate_marker(marker)?;
            conflicts.extend(marker_conflicts(
                &observation.role,
                &marker,
                &expected,
            ));
        }
    }
    if !conflicts.is_empty() {
        conflicts.sort();
        conflicts.dedup();
        return Ok(progress(
            "conflict",
            Vec::new(),
            Vec::new(),
            Vec::new(),
            conflicts,
        ));
    }

    let mut fence = Vec::new();
    for observation in &observations {
        match &observation.marker {
            None => fence.push(observation.role.clone()),
            Some(marker)
                if marker.state == ArtifactLinkCutoverStateWire::Fenced
                    && !observation.marker_committed =>
            {
                fence.push(observation.role.clone());
            }
            _ => {}
        }
    }
    if !fence.is_empty() {
        return Ok(progress(
            "fence",
            sorted(fence),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ));
    }

    let baseline: Vec<String> = observations
        .iter()
        .filter(|observation| !observation.baseline_durable)
        .map(|observation| observation.role.clone())
        .collect();
    if !baseline.is_empty() {
        return Ok(progress(
            "publish_baseline",
            Vec::new(),
            sorted(baseline),
            Vec::new(),
            Vec::new(),
        ));
    }

    let mut imported = Vec::new();
    for observation in &observations {
        match &observation.marker {
            Some(marker)
                if marker.state == ArtifactLinkCutoverStateWire::Fenced
                    || !observation.marker_committed =>
            {
                imported.push(observation.role.clone());
            }
            None => imported.push(observation.role.clone()),
            _ => {}
        }
    }
    if !imported.is_empty() {
        return Ok(progress(
            "mark_imported",
            Vec::new(),
            Vec::new(),
            sorted(imported),
            Vec::new(),
        ));
    }

    Ok(progress(
        "complete",
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    ))
}

pub fn artifact_link_outbox_classify_line(
    line: &str,
    project_key: &str,
) -> ArtifactLinkOutboxLineClassificationWire {
    match classify_outbox_line(line, project_key) {
        Ok(classification) => classification,
        Err(error) => ArtifactLinkOutboxLineClassificationWire {
            schema_version: ARTIFACT_LINK_CUTOVER_WIRE_SCHEMA_VERSION,
            kind: "invalid".to_string(),
            operation_id: None,
            diagnostic: Some(error.message),
        },
    }
}

pub fn artifact_link_outbox_legacy_conversion(
    entry: &JsonValue,
    project_key: &str,
    baseline_rows: &[ArtifactLinkRowWire],
) -> Result<ArtifactLinkOutboxLegacyConversionWire, ArtifactLinkError> {
    let project_key = validate_project_key(project_key)?;
    let (data, row, created_at, agent_name, run_id, legacy_id) =
        parse_legacy_outbox_entry(entry, &project_key)?;
    let row = validate_artifact_link_row(&row)?;
    let baseline = canonical_rows(baseline_rows)?;
    let identity = artifact_link_dedup_key(&row)?;
    if baseline
        .iter()
        .map(artifact_link_dedup_key)
        .collect::<Result<Vec<_>, _>>()?
        .contains(&identity)
    {
        return Ok(ArtifactLinkOutboxLegacyConversionWire {
            schema_version: ARTIFACT_LINK_CUTOVER_WIRE_SCHEMA_VERSION,
            outcome: "covered".to_string(),
            event: None,
        });
    }
    let operation_id = artifact_link_stable_operation_id(&[
        serde_json::json!("legacy-outbox-import"),
        serde_json::json!(project_key),
        serde_json::json!(legacy_id),
        data.get("created_at").cloned().unwrap_or(JsonValue::Null),
        serde_json::json!(agent_name),
        serde_json::json!(run_id),
        serde_json::to_value(&row).map_err(|error| {
            validation(format!(
                "unable to normalize artifact-link legacy row: {error}"
            ))
        })?,
    ])?;
    let event =
        event_from_legacy_row(project_key, operation_id, row, created_at)?;
    Ok(ArtifactLinkOutboxLegacyConversionWire {
        schema_version: ARTIFACT_LINK_CUTOVER_WIRE_SCHEMA_VERSION,
        outcome: "convert".to_string(),
        event: Some(event),
    })
}

fn classify_outbox_line(
    line: &str,
    project_key: &str,
) -> Result<ArtifactLinkOutboxLineClassificationWire, ArtifactLinkError> {
    let project_key = validate_project_key(project_key)?;
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Err(validation("artifact-link outbox line is blank"));
    }
    let value: JsonValue = serde_json::from_str(trimmed)
        .map_err(|error| validation(format!("invalid JSON: {error}")))?;
    let object = value.as_object().ok_or_else(|| {
        validation("artifact-link outbox line must be an object")
    })?;
    if object.get("event").is_some() {
        let operation_id = classify_event_entry(&value, &project_key)?;
        return Ok(ArtifactLinkOutboxLineClassificationWire {
            schema_version: ARTIFACT_LINK_CUTOVER_WIRE_SCHEMA_VERSION,
            kind: "event".to_string(),
            operation_id: Some(operation_id),
            diagnostic: None,
        });
    }
    parse_legacy_outbox_entry(&value, &project_key)?;
    Ok(ArtifactLinkOutboxLineClassificationWire {
        schema_version: ARTIFACT_LINK_CUTOVER_WIRE_SCHEMA_VERSION,
        kind: "legacy_row".to_string(),
        operation_id: None,
        diagnostic: None,
    })
}

fn classify_event_entry(
    value: &JsonValue,
    project_key: &str,
) -> Result<String, ArtifactLinkError> {
    let object = value.as_object().ok_or_else(|| {
        validation("artifact-link outbox line must be an object")
    })?;
    let schema = object
        .get("schema_version")
        .and_then(JsonValue::as_u64)
        .ok_or_else(|| {
            validation("artifact-link outbox schema_version must be an integer")
        })?;
    if schema != 2 {
        return Err(validation(
            "unsupported artifact-link event outbox schema",
        ));
    }
    number_field(object.get("created_at"), "created_at")?;
    text_field(object.get("agent_name"), "agent_name")?;
    let event_value = object.get("event").ok_or_else(|| {
        validation("artifact-link outbox event must be an object")
    })?;
    let event: ArtifactLinkEventWire =
        serde_json::from_value(event_value.clone()).map_err(|error| {
            validation(format!(
                "artifact-link outbox event is malformed: {error}"
            ))
        })?;
    let event = canonicalize_artifact_link_event(&event)?;
    if event.project_key != project_key {
        return Err(validation("artifact-link outbox event project mismatch"));
    }
    let id = text_field(object.get("id"), "id")?;
    if id != event.operation_id {
        return Err(validation(
            "artifact-link outbox id must match operation_id",
        ));
    }
    Ok(event.operation_id)
}

fn parse_legacy_outbox_entry(
    value: &JsonValue,
    project_key: &str,
) -> Result<LegacyOutboxEntryParts, ArtifactLinkError> {
    let object = value.as_object().ok_or_else(|| {
        validation("artifact-link outbox line must be an object")
    })?;
    let schema = object
        .get("schema_version")
        .and_then(JsonValue::as_u64)
        .ok_or_else(|| {
            validation("artifact-link outbox schema_version must be an integer")
        })?;
    if schema != 1 {
        return Err(validation(
            "artifact-link outbox row-only entries must be schema-v1",
        ));
    }
    let entry_project = text_field(object.get("project_key"), "project_key")?;
    if entry_project != project_key {
        return Err(validation("artifact-link outbox project mismatch"));
    }
    let created_at_value = object.get("created_at");
    let created_at = number_field(created_at_value, "created_at")?;
    let agent_name = text_field(object.get("agent_name"), "agent_name")?;
    let run_id = object
        .get("run_id")
        .and_then(JsonValue::as_str)
        .unwrap_or("")
        .to_string();
    let legacy_id = object
        .get("id")
        .and_then(JsonValue::as_str)
        .unwrap_or("")
        .to_string();
    let row_value = object.get("row").ok_or_else(|| {
        validation("artifact-link outbox row must be an object")
    })?;
    let row: ArtifactLinkRowWire = serde_json::from_value(row_value.clone())
        .map_err(|error| {
            validation(format!(
                "artifact-link outbox row is malformed: {error}"
            ))
        })?;
    Ok((
        object.clone(),
        validate_artifact_link_row(&row)?,
        created_at,
        agent_name,
        run_id,
        legacy_id,
    ))
}

fn event_from_legacy_row(
    project_key: String,
    operation_id: String,
    row: ArtifactLinkRowWire,
    _created_at: String,
) -> Result<ArtifactLinkEventWire, ArtifactLinkError> {
    let edge = edge_from_row(&row)?;
    let description = row.description.clone();
    let kind = if row.origin.increments_uses() {
        ArtifactLinkEventKindWire::Observation {
            edge,
            description,
            occurrences: row.uses,
        }
    } else {
        ArtifactLinkEventKindWire::EdgePut {
            edge,
            description,
            observed_operation_ids: Vec::new(),
        }
    };
    canonicalize_artifact_link_event(&ArtifactLinkEventWire {
        schema_version: ARTIFACT_LINK_EVENT_WIRE_SCHEMA_VERSION,
        project_key,
        operation_id,
        created_by: row.created_by,
        origin: row.origin,
        created_at: row.created_at,
        kind,
    })
}

fn validate_marker(
    marker: &ArtifactLinkCutoverMarkerWire,
) -> Result<ArtifactLinkCutoverMarkerWire, ArtifactLinkError> {
    if marker.schema_version != ARTIFACT_LINK_CUTOVER_WIRE_SCHEMA_VERSION {
        return Err(validation(format!(
            "unsupported artifact-link cutover marker schema_version {}; expected {}",
            marker.schema_version, ARTIFACT_LINK_CUTOVER_WIRE_SCHEMA_VERSION
        )));
    }
    let project_key = validate_project_key(&marker.project_key)?;
    let event_store = validate_event_store(&marker.event_store)?;
    let import_identity = validate_import_identity(&marker.import_identity)?;
    let roles = canonical_roles(&marker.roles)?;
    let baseline_event = validate_baseline_event(&marker.baseline_event)?;
    Ok(ArtifactLinkCutoverMarkerWire {
        schema_version: ARTIFACT_LINK_CUTOVER_WIRE_SCHEMA_VERSION,
        state: marker.state,
        project_key,
        event_store,
        import_identity,
        roles,
        baseline_event,
    })
}

fn validate_event_store(
    event_store: &ArtifactLinkCutoverEventStoreWire,
) -> Result<ArtifactLinkCutoverEventStoreWire, ArtifactLinkError> {
    if event_store.schema_version != 1 {
        return Err(validation(format!(
            "unsupported artifact-link cutover event_store schema_version {}",
            event_store.schema_version
        )));
    }
    if event_store.minimum_event_schema_version
        > ARTIFACT_LINK_EVENT_WIRE_SCHEMA_VERSION
    {
        return Err(validation(format!(
            "artifact-link cutover marker requires event schema {}; current is {}",
            event_store.minimum_event_schema_version,
            ARTIFACT_LINK_EVENT_WIRE_SCHEMA_VERSION
        )));
    }
    Ok(event_store.clone())
}

fn validate_import_identity(
    import_identity: &ArtifactLinkCutoverImportIdentityWire,
) -> Result<ArtifactLinkCutoverImportIdentityWire, ArtifactLinkError> {
    Ok(ArtifactLinkCutoverImportIdentityWire {
        import_id: validate_single_line(
            "import.import_id",
            &import_identity.import_id,
        )?,
        operation_id: validate_operation_id(
            "import.operation_id",
            &import_identity.operation_id,
        )?,
        source_head: validate_single_line(
            "import.source_head",
            &import_identity.source_head,
        )?,
        created_at: validate_single_line(
            "import.created_at",
            &import_identity.created_at,
        )?,
    })
}

fn canonical_roles(
    roles: &[ArtifactLinkCutoverRoleWire],
) -> Result<Vec<ArtifactLinkCutoverRoleWire>, ArtifactLinkError> {
    if roles.is_empty() {
        return Err(validation(
            "artifact-link cutover marker roles must be non-empty",
        ));
    }
    let mut by_role = BTreeMap::new();
    for role in roles {
        let raw_role = role.role.clone();
        let role = ArtifactLinkCutoverRoleWire {
            role: validate_single_line("role.role", &raw_role)?,
            kind: validate_single_line("role.kind", &role.kind)?,
            head: validate_single_line("role.head", &role.head)?,
            links_tree: validate_single_line(
                "role.links_tree",
                &role.links_tree,
            )?,
            remote_url: validate_single_line(
                "role.remote_url",
                &role.remote_url,
            )?,
        };
        if kind_for_role(&role.role) != role.kind {
            return Err(validation(format!(
                "artifact-link cutover role `{}` cannot store kind `{}`",
                role.role, role.kind
            )));
        }
        if by_role.insert(role.role.clone(), role).is_some() {
            return Err(validation(format!(
                "duplicate artifact-link cutover role `{}`",
                raw_role
            )));
        }
    }
    Ok(by_role.into_values().collect())
}

fn canonical_role_inputs(
    roles: &[ArtifactLinkCutoverRoleInputWire],
) -> Result<Vec<ArtifactLinkCutoverRoleInputWire>, ArtifactLinkError> {
    if roles.is_empty() {
        return Err(validation(
            "artifact-link cutover import roles must be non-empty",
        ));
    }
    let mut by_role = BTreeMap::new();
    for role in roles {
        let role = ArtifactLinkCutoverRoleInputWire {
            role: validate_single_line("role.role", &role.role)?,
            kind: validate_single_line("role.kind", &role.kind)?,
            head: validate_single_line("role.head", &role.head)?,
            links_tree: validate_single_line(
                "role.links_tree",
                &role.links_tree,
            )?,
            remote_url: validate_single_line(
                "role.remote_url",
                &role.remote_url,
            )?,
            commit_time: validate_single_line(
                "role.commit_time",
                &role.commit_time,
            )?,
        };
        if kind_for_role(&role.role) != role.kind {
            return Err(validation(format!(
                "artifact-link cutover role `{}` cannot store kind `{}`",
                role.role, role.kind
            )));
        }
        if by_role.insert(role.role.clone(), role).is_some() {
            return Err(validation("duplicate artifact-link cutover role"));
        }
    }
    Ok(by_role.into_values().collect())
}

fn marker_role_from_input(
    role: &ArtifactLinkCutoverRoleInputWire,
) -> ArtifactLinkCutoverRoleWire {
    ArtifactLinkCutoverRoleWire {
        role: role.role.clone(),
        kind: role.kind.clone(),
        head: role.head.clone(),
        links_tree: role.links_tree.clone(),
        remote_url: role.remote_url.clone(),
    }
}

fn validate_baseline_event(
    baseline_event: &ArtifactLinkCutoverBaselineEventWire,
) -> Result<ArtifactLinkCutoverBaselineEventWire, ArtifactLinkError> {
    let digest = validate_sha256_digest(&baseline_event.digest)?;
    let path = validate_relative_path(&baseline_event.path)?;
    Ok(ArtifactLinkCutoverBaselineEventWire { digest, path })
}

fn validate_relative_path(value: &str) -> Result<String, ArtifactLinkError> {
    let value = validate_single_line("baseline_event.path", value)?;
    if value.starts_with('/') {
        return Err(validation(
            "artifact-link cutover baseline_event.path must be relative",
        ));
    }
    for part in value.split('/') {
        if part.is_empty() || matches!(part, "." | "..") {
            return Err(validation(
                "artifact-link cutover baseline_event.path must be relative",
            ));
        }
    }
    Ok(value)
}

fn canonical_rows(
    rows: &[ArtifactLinkRowWire],
) -> Result<Vec<ArtifactLinkRowWire>, ArtifactLinkError> {
    let mut by_edge =
        BTreeMap::<ArtifactLinkDedupKeyWire, ArtifactLinkRowWire>::new();
    for row in rows {
        let row = validate_artifact_link_row(row)?;
        let key = artifact_link_dedup_key(&row)?;
        if by_edge.insert(key, row).is_some() {
            return Err(validation(
                "artifact-link cutover baseline rows contain a duplicate edge",
            ));
        }
    }
    Ok(by_edge.into_values().collect())
}

fn edge_from_row(
    row: &ArtifactLinkRowWire,
) -> Result<ArtifactLinkEventEdgeWire, ArtifactLinkError> {
    match artifact_link_dedup_key(row)? {
        ArtifactLinkDedupKeyWire::Directed {
            source_ref,
            relation,
            target_ref,
        } => Ok(ArtifactLinkEventEdgeWire::Directed {
            source_ref,
            relation,
            target_ref,
        }),
        ArtifactLinkDedupKeyWire::Undirected {
            relation,
            left_ref,
            right_ref,
        } => Ok(ArtifactLinkEventEdgeWire::Undirected {
            relation,
            left_ref,
            right_ref,
        }),
    }
}

fn canonical_read_roots(
    roots: &[ArtifactLinkCutoverReadRootWire],
) -> Result<Vec<ArtifactLinkCutoverReadRootWire>, ArtifactLinkError> {
    let mut by_role = BTreeMap::new();
    for root in roots {
        let role = validate_single_line("role", &root.role)?;
        if by_role
            .insert(
                role.clone(),
                ArtifactLinkCutoverReadRootWire {
                    role,
                    marker: root.marker.clone(),
                },
            )
            .is_some()
        {
            return Err(validation(
                "duplicate artifact-link cutover root role",
            ));
        }
    }
    Ok(by_role.into_values().collect())
}

fn canonical_observations(
    roots: &[ArtifactLinkCutoverRootObservationWire],
) -> Result<Vec<ArtifactLinkCutoverRootObservationWire>, ArtifactLinkError> {
    let mut by_role = BTreeMap::new();
    for root in roots {
        let role = validate_single_line("role", &root.role)?;
        if by_role
            .insert(
                role.clone(),
                ArtifactLinkCutoverRootObservationWire {
                    role,
                    marker: root.marker.clone(),
                    marker_committed: root.marker_committed,
                    baseline_durable: root.baseline_durable,
                },
            )
            .is_some()
        {
            return Err(validation(
                "duplicate artifact-link cutover root role",
            ));
        }
    }
    Ok(by_role.into_values().collect())
}

fn marker_payload_signatures(
    present: &[(String, ArtifactLinkCutoverMarkerWire)],
) -> Result<BTreeSet<String>, ArtifactLinkError> {
    let mut signatures = BTreeSet::new();
    for (_, marker) in present {
        signatures.insert(artifact_link_cutover_marker_canonical_json(marker)?);
    }
    Ok(signatures)
}

fn marker_conflicts(
    role: &str,
    marker: &ArtifactLinkCutoverMarkerWire,
    expected: &ArtifactLinkCutoverMarkerWire,
) -> Vec<String> {
    let mut conflicts = Vec::new();
    if marker.project_key != expected.project_key {
        conflicts
            .push(format!("{role}: project_key differs from expected import"));
    }
    if marker.event_store != expected.event_store {
        conflicts
            .push(format!("{role}: event_store differs from expected import"));
    }
    if marker.import_identity != expected.import_identity {
        conflicts.push(format!(
            "{role}: import identity differs from expected import"
        ));
    }
    if marker.roles != expected.roles {
        conflicts
            .push(format!("{role}: source roles differ from expected import"));
    }
    if marker.baseline_event != expected.baseline_event {
        conflicts.push(format!(
            "{role}: baseline event differs from expected import"
        ));
    }
    conflicts
}

fn read_state(
    state: &str,
    marker: Option<ArtifactLinkCutoverMarkerWire>,
    incomplete_roles: Vec<String>,
    diagnostics: Vec<String>,
) -> ArtifactLinkCutoverReadStateWire {
    ArtifactLinkCutoverReadStateWire {
        schema_version: ARTIFACT_LINK_CUTOVER_WIRE_SCHEMA_VERSION,
        state: state.to_string(),
        marker,
        incomplete_roles,
        diagnostics,
    }
}

fn progress(
    phase: &str,
    roles_needing_fence_marker: Vec<String>,
    roles_needing_baseline_event: Vec<String>,
    roles_needing_imported_marker: Vec<String>,
    conflicts: Vec<String>,
) -> ArtifactLinkCutoverProgressWire {
    ArtifactLinkCutoverProgressWire {
        schema_version: ARTIFACT_LINK_CUTOVER_WIRE_SCHEMA_VERSION,
        phase: phase.to_string(),
        roles_needing_fence_marker,
        roles_needing_baseline_event,
        roles_needing_imported_marker,
        conflicts,
    }
}

fn sorted(mut values: Vec<String>) -> Vec<String> {
    values.sort();
    values.dedup();
    values
}

fn kind_for_role(role: &str) -> String {
    if role == "plans" {
        "plan".to_string()
    } else {
        role.to_string()
    }
}

fn max_commit_time(roles: &[ArtifactLinkCutoverRoleInputWire]) -> String {
    roles
        .iter()
        .map(|role| role.commit_time.clone())
        .max()
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string())
}

fn number_field(
    value: Option<&JsonValue>,
    field: &str,
) -> Result<String, ArtifactLinkError> {
    let value = value.ok_or_else(|| {
        validation(format!("artifact-link outbox {field} is required"))
    })?;
    match value {
        JsonValue::Number(number)
            if number.is_i64() || number.is_u64() || number.is_f64() =>
        {
            Ok(value.to_string())
        }
        _ => Err(validation(format!(
            "artifact-link outbox {field} must be a number"
        ))),
    }
}

fn text_field(
    value: Option<&JsonValue>,
    field: &str,
) -> Result<String, ArtifactLinkError> {
    let Some(JsonValue::String(value)) = value else {
        return Err(validation(format!(
            "artifact-link outbox {field} must be a non-empty string"
        )));
    };
    validate_single_line(field, value)
}

fn validation(message: impl Into<String>) -> ArtifactLinkError {
    ArtifactLinkError::validation(message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn role(role: &str, head: &str) -> ArtifactLinkCutoverRoleInputWire {
        ArtifactLinkCutoverRoleInputWire {
            role: role.to_string(),
            kind: kind_for_role(role),
            head: head.to_string(),
            links_tree: format!("sha256:{head}"),
            remote_url: "<none>".to_string(),
            commit_time: "2026-09-10T00:00:00Z".to_string(),
        }
    }

    fn row() -> ArtifactLinkRowWire {
        ArtifactLinkRowWire {
            schema_version: 2,
            source_ref: "agent:reader".to_string(),
            relation: "read".to_string(),
            target_ref: "plan:202609/a.md".to_string(),
            description: "read".to_string(),
            origin: ArtifactLinkOriginWire::Read,
            created_by: "agent:reader".to_string(),
            created_at: "2026-09-10T00:00:00Z".to_string(),
            uses: 1,
        }
    }

    fn marker(
        state: ArtifactLinkCutoverStateWire,
    ) -> ArtifactLinkCutoverMarkerWire {
        let request = ArtifactLinkCutoverImportRequestWire {
            project_key: "gh_acme__widget".to_string(),
            roles: vec![role("plans", "aaaaaaaa")],
            rows: vec![row()],
        };
        let identity = artifact_link_cutover_import_identity(&request).unwrap();
        let baseline = artifact_link_cutover_baseline_event(
            &ArtifactLinkCutoverBaselineEventRequestWire {
                project_key: request.project_key.clone(),
                import_identity: identity.clone(),
                rows: request.rows.clone(),
            },
        )
        .unwrap();
        let canonical = artifact_link_event_canonical_json(&baseline).unwrap();
        let digest = sha256_hex(canonical.as_bytes());
        let baseline_event = ArtifactLinkCutoverBaselineEventWire {
            path: artifact_link_event_path_for_digest(&digest).unwrap(),
            digest,
        };
        artifact_link_cutover_marker(
            state,
            &request.project_key,
            &ArtifactLinkCutoverEventStoreWire {
                schema_version: 1,
                minimum_event_schema_version: 1,
            },
            &identity,
            &[marker_role_from_input(&request.roles[0])],
            &baseline_event,
        )
        .unwrap()
    }

    #[test]
    fn marker_round_trips_with_canonical_json() {
        let marker = marker(ArtifactLinkCutoverStateWire::Fenced);
        let json =
            artifact_link_cutover_marker_canonical_json(&marker).unwrap();
        let parsed = parse_artifact_link_cutover_marker(&json).unwrap();
        assert_eq!(parsed, marker);
    }

    #[test]
    fn import_identity_is_order_stable_and_head_sensitive() {
        let mut roles =
            vec![role("research", "bbbbbbbb"), role("plans", "aaaaaaaa")];
        let rows = vec![row()];
        let left = artifact_link_cutover_import_identity(
            &ArtifactLinkCutoverImportRequestWire {
                project_key: "gh_acme__widget".to_string(),
                roles: roles.clone(),
                rows: rows.clone(),
            },
        )
        .unwrap();
        roles.reverse();
        let right = artifact_link_cutover_import_identity(
            &ArtifactLinkCutoverImportRequestWire {
                project_key: "gh_acme__widget".to_string(),
                roles,
                rows,
            },
        )
        .unwrap();
        assert_eq!(left, right);
        let moved = artifact_link_cutover_import_identity(
            &ArtifactLinkCutoverImportRequestWire {
                project_key: "gh_acme__widget".to_string(),
                roles: vec![role("plans", "cccccccc")],
                rows: vec![row()],
            },
        )
        .unwrap();
        assert_ne!(left.source_head, moved.source_head);
    }

    #[test]
    fn attestation_is_stable_and_head_sensitive() {
        let marker = marker(ArtifactLinkCutoverStateWire::Fenced);
        let token = artifact_link_cutover_attestation(&marker).unwrap();
        assert!(token.starts_with("fleet-capable-"));
        assert_eq!(token, artifact_link_cutover_attestation(&marker).unwrap());
        let mut moved = marker.clone();
        moved.roles[0].head = "bbbbbbbb".to_string();
        assert_ne!(token, artifact_link_cutover_attestation(&moved).unwrap());
    }

    #[test]
    fn progress_walks_recovery_phases() {
        let fenced = marker(ArtifactLinkCutoverStateWire::Fenced);
        let imported = marker(ArtifactLinkCutoverStateWire::Imported);
        let missing = artifact_link_cutover_progress(
            &ArtifactLinkCutoverProgressRequestWire {
                expected: fenced.clone(),
                roots: vec![ArtifactLinkCutoverRootObservationWire {
                    role: "plans".to_string(),
                    marker: None,
                    marker_committed: false,
                    baseline_durable: false,
                }],
            },
        )
        .unwrap();
        assert_eq!(missing.phase, "fence");
        let baseline = artifact_link_cutover_progress(
            &ArtifactLinkCutoverProgressRequestWire {
                expected: fenced.clone(),
                roots: vec![ArtifactLinkCutoverRootObservationWire {
                    role: "plans".to_string(),
                    marker: Some(fenced.clone()),
                    marker_committed: true,
                    baseline_durable: false,
                }],
            },
        )
        .unwrap();
        assert_eq!(baseline.phase, "publish_baseline");
        let mark = artifact_link_cutover_progress(
            &ArtifactLinkCutoverProgressRequestWire {
                expected: fenced.clone(),
                roots: vec![ArtifactLinkCutoverRootObservationWire {
                    role: "plans".to_string(),
                    marker: Some(fenced),
                    marker_committed: true,
                    baseline_durable: true,
                }],
            },
        )
        .unwrap();
        assert_eq!(mark.phase, "mark_imported");
        let complete = artifact_link_cutover_progress(
            &ArtifactLinkCutoverProgressRequestWire {
                expected: imported.clone(),
                roots: vec![ArtifactLinkCutoverRootObservationWire {
                    role: "plans".to_string(),
                    marker: Some(imported),
                    marker_committed: true,
                    baseline_durable: true,
                }],
            },
        )
        .unwrap();
        assert_eq!(complete.phase, "complete");
    }

    #[test]
    fn read_state_reports_four_states() {
        let fenced = marker(ArtifactLinkCutoverStateWire::Fenced);
        let imported = marker(ArtifactLinkCutoverStateWire::Imported);
        assert_eq!(
            artifact_link_cutover_read_state(&[]).unwrap().state,
            "none"
        );
        assert_eq!(
            artifact_link_cutover_read_state(&[
                ArtifactLinkCutoverReadRootWire {
                    role: "plans".to_string(),
                    marker: Some(fenced.clone()),
                }
            ])
            .unwrap()
            .state,
            "fenced"
        );
        assert_eq!(
            artifact_link_cutover_read_state(&[
                ArtifactLinkCutoverReadRootWire {
                    role: "plans".to_string(),
                    marker: Some(imported),
                }
            ])
            .unwrap()
            .state,
            "imported"
        );
        assert_eq!(
            artifact_link_cutover_read_state(&[
                ArtifactLinkCutoverReadRootWire {
                    role: "plans".to_string(),
                    marker: Some(fenced),
                },
                ArtifactLinkCutoverReadRootWire {
                    role: "research".to_string(),
                    marker: None,
                },
            ])
            .unwrap()
            .state,
            "incomplete"
        );
    }

    #[test]
    fn classifies_and_converts_legacy_outbox_lines() {
        let legacy = json!({
            "schema_version": 1,
            "id": "old12345",
            "created_at": 1.0,
            "project_key": "gh_acme__widget",
            "agent_name": "reader",
            "run_id": "run",
            "row": row(),
        });
        let line = serde_json::to_string(&legacy).unwrap();
        let classified =
            artifact_link_outbox_classify_line(&line, "gh_acme__widget");
        assert_eq!(classified.kind, "legacy_row");
        let converted = artifact_link_outbox_legacy_conversion(
            &legacy,
            "gh_acme__widget",
            &[],
        )
        .unwrap();
        assert_eq!(converted.outcome, "convert");
        let event = converted.event.unwrap();
        assert_eq!(event.operation_id.len(), 32);
        let converted_again = artifact_link_outbox_legacy_conversion(
            &legacy,
            "gh_acme__widget",
            &[],
        )
        .unwrap();
        assert_eq!(
            converted_again.event.unwrap().operation_id,
            event.operation_id
        );
        let covered = artifact_link_outbox_legacy_conversion(
            &legacy,
            "gh_acme__widget",
            &[row()],
        )
        .unwrap();
        assert_eq!(covered.outcome, "covered");
    }
}
