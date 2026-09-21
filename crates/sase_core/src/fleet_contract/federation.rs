use super::catalog::normalize_catalog_limit;
use super::catalog::parse_catalog_cursor;
use super::catalog::validate_fleet_catalog_snapshot_id;
use super::catalog::FleetCatalogContinuationStateWire;
use super::catalog::FleetCatalogContinuationWire;
use super::catalog::FleetCatalogResetReasonWire;
use super::catalog::FleetCatalogScopeWire;
use super::catalog::FleetLogicalBatchEntryWire;
use super::cursors::StoreCursorWire;
use super::error::FleetContractError;
use super::error::FLEET_CONTRACT_SCHEMA_VERSION;
use super::error::MAX_IDENTIFIER_BYTES;
use super::error::MAX_LABEL_BYTES;
use super::follows::count_focus_and_fleet;
use super::follows::FleetHostCountInputWire;
use super::follows::FocusFleetCountsRequestWire;
use super::follows::FocusFleetCountsWire;
use super::locators::OriginLocatorWire;
use super::projection::replace_control_characters;
use super::reads::max_optional_f64;
use super::reads::validate_fleet_logical_agent_counts;
use super::reads::FleetLogicalAgentCountsWire;
use super::resolution::validate_resolved_agent_summary;
use super::resolution::ResolvedAgentSummaryWire;
use super::snapshot::fleet_count_revision;
use super::snapshot::validate_fleet_snapshot_freshness;
use super::snapshot::FleetSnapshotFreshnessWire;
use super::status::ObservationFreshnessWire;
use super::validation::array_field;
use super::validation::optional_bool_field;
use super::validation::optional_f64_value;
use super::validation::optional_non_negative_seconds_field;
use super::validation::optional_string_field;
use super::validation::optional_u64_field;
use super::validation::reject_secretish;
use super::validation::required_bool_field;
use super::validation::required_u64_field;
use super::validation::trim_to_limit;
use super::validation::validate_allowed_fields;
use super::validation::validate_installation_id;
use super::validation::validate_key;
use super::validation::validate_label;
use super::validation::validate_optional_schema;
use super::validation::validate_reference_id;
use super::validation::validate_schema;
use super::validation::wire_from_json_value;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Request to normalize a federation worker read result.
///
/// The payload is transport-independent JSON from the federation worker result
/// object or from a successful IPC response envelope. Host envelopes are
/// normalized independently so a malformed host degrades to diagnostics
/// without discarding other healthy hosts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetFederationNormalizeRequestWire {
    pub schema_version: u32,
    pub response: Value,
}

/// Count request for the current Focus/Fleet bridge over real federation
/// envelopes.
///
/// `followed_response` must be a followed-batch federation response and is
/// counted only from resolved requested summaries. `fleet_response` may carry
/// catalog or summary responses and may use authoritative host counts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FocusFleetFederationCountsRequestWire {
    pub schema_version: u32,
    pub local_summaries: Vec<ResolvedAgentSummaryWire>,
    pub followed_response: Option<Value>,
    pub fleet_response: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetEnvelopeDiagnosticWire {
    pub schema_version: u32,
    pub alias: Option<String>,
    pub operation: Option<String>,
    pub code: String,
    pub severity: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetNormalizedHostWire {
    pub schema_version: u32,
    pub alias: Option<String>,
    pub origin: Option<OriginLocatorWire>,
    pub status: String,
    pub cached: bool,
    pub age_seconds: Option<f64>,
    pub partial: bool,
    pub freshness: FleetSnapshotFreshnessWire,
    pub observed_at_unix: Option<f64>,
    pub summaries: Vec<ResolvedAgentSummaryWire>,
    pub authoritative_counts: Option<FleetLogicalAgentCountsWire>,
    pub count_revision: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_scope: Option<FleetCatalogScopeWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalog_snapshot_id: Option<String>,
    pub catalog: Option<FleetCatalogContinuationWire>,
    pub unresolved_logical_keys: Vec<String>,
    pub diagnostics: Vec<FleetEnvelopeDiagnosticWire>,
    pub count_input: Option<FleetHostCountInputWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetNormalizedReadResponseWire {
    pub schema_version: u32,
    pub operation: Option<String>,
    pub configured_host_count: u64,
    pub partial: bool,
    pub summaries: Vec<ResolvedAgentSummaryWire>,
    pub diagnostics: Vec<FleetEnvelopeDiagnosticWire>,
    pub hosts: Vec<FleetNormalizedHostWire>,
    pub count_hosts: Vec<FleetHostCountInputWire>,
}

pub fn normalize_fleet_federation_response(
    request: &FleetFederationNormalizeRequestWire,
) -> Result<FleetNormalizedReadResponseWire, FleetContractError> {
    validate_schema(
        "fleet federation normalize request",
        request.schema_version,
    )?;
    normalize_fleet_federation_response_value(&request.response)
}

pub fn count_focus_and_fleet_from_federation(
    request: &FocusFleetFederationCountsRequestWire,
) -> Result<FocusFleetCountsWire, FleetContractError> {
    validate_schema(
        "focus/fleet federation counts request",
        request.schema_version,
    )?;
    for summary in &request.local_summaries {
        validate_resolved_agent_summary(summary)?;
    }
    let followed = match &request.followed_response {
        Some(response) if !response.is_null() => {
            normalize_fleet_federation_response_value(response)?
        }
        _ => empty_normalized_federation_response(None),
    };
    let fleet = match &request.fleet_response {
        Some(response) if !response.is_null() => {
            normalize_fleet_federation_response_value(response)?
        }
        _ => empty_normalized_federation_response(None),
    };
    count_focus_and_fleet(&FocusFleetCountsRequestWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        local_summaries: request.local_summaries.clone(),
        followed_remote_hosts: followed.count_hosts,
        fleet_hosts: fleet.count_hosts,
    })
}

fn normalize_fleet_federation_response_value(
    response: &Value,
) -> Result<FleetNormalizedReadResponseWire, FleetContractError> {
    let response = nested_federation_result(response);
    let object = response.as_object().ok_or_else(|| {
        FleetContractError::Validation(
            "fleet federation response must be a JSON object".to_string(),
        )
    })?;
    validate_allowed_fields(
        object,
        "fleet federation response",
        &[
            "schema_version",
            "operation",
            "configured_hosts",
            "configured_host_count",
            "partial",
            "disabled",
            "diagnostics",
            "hosts",
        ],
    )?;
    validate_optional_schema(object, "fleet federation response")?;
    let operation = optional_string_field(
        object,
        "operation",
        "fleet federation operation",
        MAX_IDENTIFIER_BYTES,
    )?;
    if let Some(operation) = &operation {
        validate_reference_id("fleet federation operation", operation)?;
    }
    let disabled = optional_bool_field(object, "disabled")?.unwrap_or(false);
    let host_values = array_field(object, "hosts")?;
    let configured_hosts = optional_u64_field(object, "configured_hosts")?;
    let configured_host_count =
        optional_u64_field(object, "configured_host_count")?;
    if let (Some(left), Some(right)) = (configured_hosts, configured_host_count)
    {
        if left != right {
            return Err(FleetContractError::Validation(
                "configured_hosts and configured_host_count disagree"
                    .to_string(),
            ));
        }
    }
    let configured_host_count = configured_hosts
        .or(configured_host_count)
        .unwrap_or(host_values.len() as u64);
    if configured_host_count < host_values.len() as u64 {
        return Err(FleetContractError::Validation(
            "configured_host_count is smaller than returned hosts".to_string(),
        ));
    }
    let response_partial =
        optional_bool_field(object, "partial")?.unwrap_or(false);
    let mut diagnostics = diagnostics_array(
        object.get("diagnostics"),
        None,
        operation.as_deref(),
    )?;
    let mut hosts = Vec::new();
    let mut summaries = Vec::new();
    let mut count_hosts = Vec::new();
    for (index, value) in host_values.iter().enumerate() {
        let host = match normalize_federation_host_strict(
            value,
            operation.as_deref(),
            disabled,
            index,
        ) {
            Ok(host) => host,
            Err(error) => invalid_federation_host(
                value,
                operation.as_deref(),
                index,
                &error.to_string(),
            )?,
        };
        diagnostics.extend(host.diagnostics.clone());
        summaries.extend(host.summaries.clone());
        if let Some(input) = &host.count_input {
            count_hosts.push(input.clone());
        }
        hosts.push(host);
    }
    Ok(FleetNormalizedReadResponseWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        operation,
        configured_host_count,
        partial: response_partial
            || configured_host_count > hosts.len() as u64
            || hosts.iter().any(|host| host.partial),
        summaries,
        diagnostics,
        hosts,
        count_hosts,
    })
}

fn empty_normalized_federation_response(
    operation: Option<String>,
) -> FleetNormalizedReadResponseWire {
    FleetNormalizedReadResponseWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        operation,
        configured_host_count: 0,
        partial: false,
        summaries: Vec::new(),
        diagnostics: Vec::new(),
        hosts: Vec::new(),
        count_hosts: Vec::new(),
    }
}

fn normalize_federation_host_strict(
    value: &Value,
    operation: Option<&str>,
    disabled: bool,
    index: usize,
) -> Result<FleetNormalizedHostWire, FleetContractError> {
    let object = value.as_object().ok_or_else(|| {
        FleetContractError::Validation(format!(
            "fleet federation hosts[{index}] must be a JSON object"
        ))
    })?;
    validate_allowed_fields(
        object,
        &format!("fleet federation hosts[{index}]"),
        &[
            "schema_version",
            "alias",
            "origin",
            "provider_ref",
            "installation_id",
            "endpoint",
            "status",
            "cached",
            "age_seconds",
            "payload",
            "error",
            "freshness",
            "observed_at",
            "observed_at_unix",
            "diagnostics",
        ],
    )?;
    validate_optional_schema(
        object,
        &format!("fleet federation hosts[{index}]"),
    )?;
    let alias = optional_string_field(
        object,
        "alias",
        "fleet federation host alias",
        MAX_LABEL_BYTES,
    )?;
    let origin = origin_from_host(object)?;
    let status = optional_string_field(
        object,
        "status",
        "fleet federation host status",
        MAX_IDENTIFIER_BYTES,
    )?
    .unwrap_or_else(|| {
        if disabled {
            "disabled".to_string()
        } else {
            "unknown".to_string()
        }
    });
    validate_reference_id("fleet federation host status", &status)?;
    let cached = optional_bool_field(object, "cached")?.unwrap_or(false);
    let age_seconds =
        optional_non_negative_seconds_field(object, "age_seconds")?;
    let mut diagnostics = diagnostics_array(
        object.get("diagnostics"),
        alias.as_deref(),
        operation,
    )?;
    if let Some(error) = diagnostic_from_error_value(
        object.get("error"),
        alias.as_deref(),
        operation,
        "fleet_host_error",
    )? {
        diagnostics.push(error);
    }

    let payload = object
        .get("payload")
        .filter(|payload| !payload.is_null())
        .map(|payload| {
            payload.as_object().ok_or_else(|| {
                FleetContractError::Validation(
                    "fleet federation host payload must be a JSON object"
                        .to_string(),
                )
            })
        })
        .transpose()?;
    let payload_normalization =
        normalize_host_payload(payload, operation, alias.as_deref())?;
    diagnostics.extend(payload_normalization.diagnostics);
    if payload.is_none() && host_status_healthy(&status) {
        diagnostics.push(fleet_envelope_diagnostic(
            alias.as_deref(),
            operation,
            "fleet_payload_missing",
            "error",
            "healthy host did not include a fleet payload",
        )?);
    }
    if !disabled && !host_status_healthy(&status) && diagnostics.is_empty() {
        diagnostics.push(fleet_envelope_diagnostic(
            alias.as_deref(),
            operation,
            "fleet_host_error",
            "warning",
            &format!("host status {status}"),
        )?);
    }
    let freshness = match payload_normalization.freshness {
        Some(freshness) => freshness,
        None => {
            normalized_host_freshness(object, &status, diagnostics.first())?
        }
    };
    if let Some(reason) = &freshness.error {
        if !diagnostics
            .iter()
            .any(|diagnostic| diagnostic.message == *reason)
        {
            diagnostics.push(fleet_envelope_diagnostic(
                alias.as_deref(),
                operation,
                if freshness.freshness == ObservationFreshnessWire::Stale {
                    "fleet_host_stale"
                } else {
                    "fleet_host_partial"
                },
                "warning",
                reason,
            )?);
        }
    }
    let observed_at_unix = normalized_host_observed_at(
        object,
        &freshness,
        payload_normalization.authoritative_counts.as_ref(),
        &payload_normalization.summaries,
    )?;
    let partial = payload_normalization.partial
        || freshness.partial
        || !host_status_healthy(&status)
        || (payload.is_none() && host_status_healthy(&status));
    let count_input = origin.clone().map(|origin| FleetHostCountInputWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        origin,
        summaries: payload_normalization.summaries.clone(),
        observed_at_unix,
        freshness: freshness.freshness,
        authoritative_counts: payload_normalization
            .authoritative_counts
            .clone(),
        partial,
    });
    Ok(FleetNormalizedHostWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        alias,
        origin,
        status,
        cached,
        age_seconds,
        partial,
        freshness,
        observed_at_unix,
        summaries: payload_normalization.summaries,
        authoritative_counts: payload_normalization.authoritative_counts,
        count_revision: payload_normalization.count_revision,
        catalog_scope: payload_normalization.catalog_scope,
        catalog_snapshot_id: payload_normalization.catalog_snapshot_id,
        catalog: payload_normalization.catalog,
        unresolved_logical_keys: payload_normalization.unresolved_logical_keys,
        diagnostics,
        count_input,
    })
}

fn invalid_federation_host(
    value: &Value,
    operation: Option<&str>,
    index: usize,
    reason: &str,
) -> Result<FleetNormalizedHostWire, FleetContractError> {
    let object = value.as_object();
    let alias = object.and_then(safe_alias_from_host);
    let origin = object
        .and_then(|object| origin_from_host(object).ok())
        .flatten();
    let cached = object
        .and_then(|object| optional_bool_field(object, "cached").ok())
        .flatten()
        .unwrap_or(false);
    let age_seconds = object
        .and_then(|object| {
            optional_non_negative_seconds_field(object, "age_seconds").ok()
        })
        .flatten();
    let diagnostics = vec![fleet_envelope_diagnostic(
        alias.as_deref(),
        operation,
        "fleet_envelope_invalid",
        "error",
        &format!("hosts[{index}] could not be normalized: {reason}"),
    )?];
    let freshness = FleetSnapshotFreshnessWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        freshness: ObservationFreshnessWire::Unknown,
        partial: true,
        refreshed_at_unix: None,
        error: Some("invalid_envelope".to_string()),
    };
    let count_input = origin.clone().map(|origin| FleetHostCountInputWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        origin,
        summaries: Vec::new(),
        observed_at_unix: None,
        freshness: ObservationFreshnessWire::Unknown,
        authoritative_counts: None,
        partial: true,
    });
    Ok(FleetNormalizedHostWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        alias,
        origin,
        status: "invalid".to_string(),
        cached,
        age_seconds,
        partial: true,
        freshness,
        observed_at_unix: None,
        summaries: Vec::new(),
        authoritative_counts: None,
        count_revision: None,
        catalog_scope: None,
        catalog_snapshot_id: None,
        catalog: None,
        unresolved_logical_keys: Vec::new(),
        diagnostics,
        count_input,
    })
}

struct PayloadNormalization {
    summaries: Vec<ResolvedAgentSummaryWire>,
    authoritative_counts: Option<FleetLogicalAgentCountsWire>,
    count_revision: Option<u64>,
    catalog_scope: Option<FleetCatalogScopeWire>,
    catalog_snapshot_id: Option<String>,
    freshness: Option<FleetSnapshotFreshnessWire>,
    catalog: Option<FleetCatalogContinuationWire>,
    unresolved_logical_keys: Vec<String>,
    diagnostics: Vec<FleetEnvelopeDiagnosticWire>,
    partial: bool,
}

fn normalize_host_payload(
    payload: Option<&Map<String, Value>>,
    operation: Option<&str>,
    alias: Option<&str>,
) -> Result<PayloadNormalization, FleetContractError> {
    let Some(payload) = payload else {
        return Ok(PayloadNormalization {
            summaries: Vec::new(),
            authoritative_counts: None,
            count_revision: None,
            catalog_scope: None,
            catalog_snapshot_id: None,
            freshness: None,
            catalog: None,
            unresolved_logical_keys: Vec::new(),
            diagnostics: Vec::new(),
            partial: false,
        });
    };
    validate_allowed_fields(
        payload,
        "fleet federation host payload",
        &[
            "schema_version",
            "cursor",
            "catalog_scope",
            "catalog_snapshot_id",
            "counts",
            "count_revision",
            "freshness",
            "page",
            "entries",
        ],
    )?;
    validate_optional_schema(payload, "fleet federation host payload")?;
    let mut diagnostics = Vec::new();
    let (snapshot_cursor, cursor_partial) = optional_store_cursor_value(
        payload.get("cursor"),
        "fleet federation payload cursor",
        alias,
        operation,
        &mut diagnostics,
    )?;
    let mut catalog_scope = payload
        .get("catalog_scope")
        .map(|value| wire_from_json_value(value, "fleet catalog scope"))
        .transpose()?;
    let mut catalog_snapshot_id = optional_string_field(
        payload,
        "catalog_snapshot_id",
        "fleet catalog snapshot_id",
        MAX_IDENTIFIER_BYTES,
    )?;
    if let Some(snapshot_id) = &catalog_snapshot_id {
        validate_fleet_catalog_snapshot_id(snapshot_id)?;
    }
    let freshness = payload
        .get("freshness")
        .map(|value| {
            freshness_from_json_value(value, "fleet host payload freshness")
        })
        .transpose()?;
    let has_page = payload.get("page").is_some_and(|value| !value.is_null());
    let has_entries =
        payload.get("entries").is_some_and(|value| !value.is_null());
    if has_page && has_entries {
        return Err(FleetContractError::Validation(
            "fleet payload cannot contain both page and entries".to_string(),
        ));
    }
    let authoritative_counts = if has_entries {
        None
    } else {
        authoritative_counts_from_payload(payload, operation)?
    };
    let count_revision = optional_u64_field(payload, "count_revision")?
        .or_else(|| {
            authoritative_counts.as_ref().and_then(fleet_count_revision)
        });
    let mut summaries = Vec::new();
    let mut unresolved_logical_keys = Vec::new();
    let mut catalog = None;
    let mut partial = cursor_partial;
    if let Some(page) = payload.get("page").filter(|value| !value.is_null()) {
        let parsed = normalized_catalog_page(
            page,
            snapshot_cursor,
            alias,
            operation,
            &mut diagnostics,
        )?;
        summaries = parsed.0;
        catalog_scope = Some(parsed.1.scope);
        catalog_snapshot_id = Some(parsed.1.snapshot_id.clone());
        catalog = Some(parsed.1);
        partial |= catalog.as_ref().is_some_and(|catalog| {
            catalog.state == FleetCatalogContinuationStateWire::ResyncRequired
        });
    } else if let Some(entries) =
        payload.get("entries").filter(|value| !value.is_null())
    {
        let parsed = normalized_followed_entries(entries)?;
        summaries = parsed.0;
        unresolved_logical_keys = parsed.1;
    }
    Ok(PayloadNormalization {
        summaries,
        authoritative_counts,
        count_revision,
        catalog_scope,
        catalog_snapshot_id,
        freshness,
        catalog,
        unresolved_logical_keys,
        diagnostics,
        partial,
    })
}

fn normalized_catalog_page(
    value: &Value,
    snapshot_cursor: Option<StoreCursorWire>,
    alias: Option<&str>,
    operation: Option<&str>,
    diagnostics: &mut Vec<FleetEnvelopeDiagnosticWire>,
) -> Result<
    (Vec<ResolvedAgentSummaryWire>, FleetCatalogContinuationWire),
    FleetContractError,
> {
    let page = value.as_object().ok_or_else(|| {
        FleetContractError::Validation(
            "fleet catalog payload.page must be a JSON object".to_string(),
        )
    })?;
    validate_allowed_fields(
        page,
        "fleet catalog payload.page",
        &[
            "schema_version",
            "scope",
            "snapshot_id",
            "rows",
            "limit",
            "total_matching_rows",
            "next_cursor",
            "has_more",
            "state",
            "reset_reason",
        ],
    )?;
    validate_optional_schema(page, "fleet catalog payload.page")?;
    let scope = page
        .get("scope")
        .map(|value| wire_from_json_value(value, "fleet catalog scope"))
        .transpose()?
        .unwrap_or_default();
    let snapshot_id = optional_string_field(
        page,
        "snapshot_id",
        "fleet catalog snapshot_id",
        MAX_IDENTIFIER_BYTES,
    )?
    .ok_or_else(|| {
        FleetContractError::Validation(
            "fleet catalog snapshot_id is required".to_string(),
        )
    })?;
    validate_fleet_catalog_snapshot_id(&snapshot_id)?;
    let rows = resolved_summaries_array(
        page.get("rows"),
        "fleet catalog payload.page.rows",
    )?;
    let limit_u64 = required_u64_field(page, "limit")?;
    let limit = u32::try_from(limit_u64).map_err(|_| {
        FleetContractError::Validation(
            "fleet catalog payload.page.limit is out of range".to_string(),
        )
    })?;
    normalize_catalog_limit(Some(limit))?;
    let total_matching_rows = required_u64_field(page, "total_matching_rows")?;
    let raw_next_cursor = optional_string_field(
        page,
        "next_cursor",
        "fleet catalog next_cursor",
        MAX_IDENTIFIER_BYTES,
    )?;
    let has_more = required_bool_field(page, "has_more")?;
    let reset_reason = page
        .get("reset_reason")
        .filter(|value| !value.is_null())
        .map(|value| wire_from_json_value(value, "fleet catalog reset_reason"))
        .transpose()?;
    let state = page
        .get("state")
        .map(|value| {
            wire_from_json_value(value, "fleet catalog continuation state")
        })
        .transpose()?
        .unwrap_or(if has_more {
            FleetCatalogContinuationStateWire::Ready
        } else {
            FleetCatalogContinuationStateWire::Finished
        });
    if state == FleetCatalogContinuationStateWire::ResyncRequired {
        if has_more || raw_next_cursor.is_some() || !rows.is_empty() {
            return Err(FleetContractError::Validation(
                "fleet catalog resync page must have no rows or continuation"
                    .to_string(),
            ));
        }
        return Ok((
            rows,
            FleetCatalogContinuationWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                snapshot_cursor,
                scope,
                snapshot_id,
                limit,
                total_matching_rows,
                next_cursor: None,
                has_more,
                state,
                reset_reason: reset_reason
                    .or(Some(FleetCatalogResetReasonWire::RestartRequired)),
            },
        ));
    }
    let mut next_cursor = None;
    match (has_more, raw_next_cursor) {
        (true, Some(cursor)) => {
            let parsed = parse_catalog_cursor(&cursor)?;
            if parsed.scope != scope || parsed.snapshot_id != snapshot_id {
                return Err(FleetContractError::Validation(
                    "fleet catalog next_cursor does not match page scope and snapshot_id"
                        .to_string(),
                ));
            }
            if state != FleetCatalogContinuationStateWire::Ready {
                return Err(FleetContractError::Validation(
                    "fleet catalog page has continuation but state is not ready"
                        .to_string(),
                ));
            }
            next_cursor = Some(cursor);
        }
        (true, None) => {
            diagnostics.push(fleet_envelope_diagnostic(
                alias,
                operation,
                "fleet_cursor_missing",
                "warning",
                "fleet catalog page has more rows but no next_cursor",
            )?);
            return Err(FleetContractError::Validation(
                "fleet catalog page has more rows but no next_cursor"
                    .to_string(),
            ));
        }
        (false, Some(_)) => {
            diagnostics.push(fleet_envelope_diagnostic(
                alias,
                operation,
                "fleet_cursor_inconsistent",
                "warning",
                "fleet catalog page returned next_cursor without has_more",
            )?);
            return Err(FleetContractError::Validation(
                "fleet catalog page returned next_cursor without has_more"
                    .to_string(),
            ));
        }
        (false, None) => {}
    }
    if !has_more && state != FleetCatalogContinuationStateWire::Finished {
        return Err(FleetContractError::Validation(
            "fleet catalog page without continuation must be finished"
                .to_string(),
        ));
    }
    if reset_reason.is_some() {
        return Err(FleetContractError::Validation(
            "fleet catalog reset_reason is only valid on resync_required pages"
                .to_string(),
        ));
    }
    Ok((
        rows,
        FleetCatalogContinuationWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            snapshot_cursor,
            scope,
            snapshot_id,
            limit,
            total_matching_rows,
            next_cursor,
            has_more,
            state,
            reset_reason: None,
        },
    ))
}

fn normalized_followed_entries(
    value: &Value,
) -> Result<(Vec<ResolvedAgentSummaryWire>, Vec<String>), FleetContractError> {
    let entries = value.as_array().ok_or_else(|| {
        FleetContractError::Validation(
            "fleet followed payload.entries must be a JSON array".to_string(),
        )
    })?;
    let mut summaries = Vec::new();
    let mut unresolved = Vec::new();
    for (index, value) in entries.iter().enumerate() {
        let entry: FleetLogicalBatchEntryWire = wire_from_json_value(
            value,
            &format!("fleet followed payload.entries[{index}]"),
        )?;
        validate_schema("fleet followed payload entry", entry.schema_version)?;
        validate_key(
            "fleet followed payload requested_logical_key",
            &entry.requested_logical_key,
        )?;
        match entry.summary {
            Some(summary) => {
                summaries.push(validate_resolved_agent_summary(&summary)?);
            }
            None => unresolved.push(entry.requested_logical_key),
        }
    }
    Ok((summaries, unresolved))
}

fn authoritative_counts_from_payload(
    payload: &Map<String, Value>,
    operation: Option<&str>,
) -> Result<Option<FleetLogicalAgentCountsWire>, FleetContractError> {
    if operation == Some("followed_batch") || operation == Some("attention") {
        return Ok(None);
    }
    let Some(value) = payload.get("counts") else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let counts: FleetLogicalAgentCountsWire =
        wire_from_json_value(value, "fleet host authoritative counts")?;
    validate_fleet_logical_agent_counts(
        &counts,
        "fleet host authoritative counts",
    )
    .map(Some)
}

fn normalized_host_freshness(
    host: &Map<String, Value>,
    status: &str,
    diagnostic: Option<&FleetEnvelopeDiagnosticWire>,
) -> Result<FleetSnapshotFreshnessWire, FleetContractError> {
    if let Some(value) = host.get("freshness") {
        return freshness_from_json_value(value, "fleet host freshness");
    }
    let error = if host_status_healthy(status) {
        None
    } else {
        Some(
            diagnostic
                .map(|diagnostic| diagnostic.code.clone())
                .unwrap_or_else(|| status.to_string()),
        )
    };
    Ok(FleetSnapshotFreshnessWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        freshness: if status == "stale" {
            ObservationFreshnessWire::Stale
        } else if host_status_healthy(status) {
            ObservationFreshnessWire::Fresh
        } else {
            ObservationFreshnessWire::Unknown
        },
        partial: !host_status_healthy(status),
        refreshed_at_unix: None,
        error,
    })
}

fn freshness_from_json_value(
    value: &Value,
    label: &str,
) -> Result<FleetSnapshotFreshnessWire, FleetContractError> {
    if value.is_null() {
        return Ok(FleetSnapshotFreshnessWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            freshness: ObservationFreshnessWire::Unknown,
            partial: true,
            refreshed_at_unix: None,
            error: Some("missing_freshness".to_string()),
        });
    }
    if value.is_string() {
        let freshness: ObservationFreshnessWire =
            wire_from_json_value(value, label)?;
        return Ok(FleetSnapshotFreshnessWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            freshness,
            partial: freshness == ObservationFreshnessWire::Unknown,
            refreshed_at_unix: None,
            error: None,
        });
    }
    let freshness: FleetSnapshotFreshnessWire =
        wire_from_json_value(value, label)?;
    validate_fleet_snapshot_freshness(&freshness)
}

fn normalized_host_observed_at(
    host: &Map<String, Value>,
    freshness: &FleetSnapshotFreshnessWire,
    authoritative_counts: Option<&FleetLogicalAgentCountsWire>,
    summaries: &[ResolvedAgentSummaryWire],
) -> Result<Option<f64>, FleetContractError> {
    let host_observed = optional_f64_value(
        host.get("observed_at_unix")
            .or_else(|| host.get("observed_at")),
        "fleet host observed_at_unix",
    )?;
    let summary_observed =
        summaries.iter().try_fold(None, |observed, summary| {
            validate_resolved_agent_summary(summary)?;
            Ok::<_, FleetContractError>(max_optional_f64(
                observed,
                Some(summary.observed_at_unix),
            ))
        })?;
    Ok([
        host_observed,
        freshness.refreshed_at_unix,
        authoritative_counts
            .and_then(|counts| counts.basis.observed_at_unix_max),
        summary_observed,
    ]
    .into_iter()
    .fold(None, max_optional_f64))
}

fn optional_store_cursor_value(
    value: Option<&Value>,
    label: &str,
    alias: Option<&str>,
    operation: Option<&str>,
    diagnostics: &mut Vec<FleetEnvelopeDiagnosticWire>,
) -> Result<(Option<StoreCursorWire>, bool), FleetContractError> {
    let Some(value) = value else {
        return Ok((None, false));
    };
    if value.is_null() {
        return Ok((None, false));
    }
    let cursor: StoreCursorWire = match wire_from_json_value(value, label) {
        Ok(cursor) => cursor,
        Err(error) => {
            diagnostics.push(fleet_envelope_diagnostic(
                alias,
                operation,
                "fleet_snapshot_cursor_invalid",
                "warning",
                &error.to_string(),
            )?);
            return Ok((None, true));
        }
    };
    match cursor.validate() {
        Ok(()) => Ok((Some(cursor), false)),
        Err(error) => {
            diagnostics.push(fleet_envelope_diagnostic(
                alias,
                operation,
                "fleet_snapshot_cursor_invalid",
                "warning",
                &error.to_string(),
            )?);
            Ok((None, true))
        }
    }
}

fn nested_federation_result(response: &Value) -> &Value {
    let Some(object) = response.as_object() else {
        return response;
    };
    let Some(result) = object.get("result") else {
        return response;
    };
    if result.as_object().is_some_and(|result| {
        result.contains_key("hosts") || result.contains_key("operation")
    }) {
        result
    } else {
        response
    }
}

fn origin_from_host(
    object: &Map<String, Value>,
) -> Result<Option<OriginLocatorWire>, FleetContractError> {
    let origin = object
        .get("origin")
        .filter(|value| !value.is_null())
        .map(|value| {
            let origin: OriginLocatorWire =
                wire_from_json_value(value, "fleet federation host origin")?;
            origin.validate()?;
            Ok::<_, FleetContractError>(origin)
        })
        .transpose()?;
    let installation_id = optional_string_field(
        object,
        "installation_id",
        "fleet federation host installation_id",
        MAX_IDENTIFIER_BYTES,
    )?;
    let installation_origin = installation_id
        .map(|installation_id| {
            validate_installation_id(&installation_id)?;
            Ok::<_, FleetContractError>(OriginLocatorWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                installation_id,
            })
        })
        .transpose()?;
    if let (Some(origin), Some(installation_origin)) =
        (&origin, &installation_origin)
    {
        if origin != installation_origin {
            return Err(FleetContractError::Validation(
                "fleet federation host origin does not match installation_id"
                    .to_string(),
            ));
        }
    }
    Ok(origin.or(installation_origin))
}

fn safe_alias_from_host(object: &Map<String, Value>) -> Option<String> {
    optional_string_field(
        object,
        "alias",
        "fleet federation host alias",
        MAX_LABEL_BYTES,
    )
    .ok()
    .flatten()
}

fn diagnostic_from_error_value(
    value: Option<&Value>,
    alias: Option<&str>,
    operation: Option<&str>,
    default_code: &str,
) -> Result<Option<FleetEnvelopeDiagnosticWire>, FleetContractError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    if let Some(text) = value.as_str() {
        return fleet_envelope_diagnostic(
            alias,
            operation,
            default_code,
            "error",
            text,
        )
        .map(Some);
    }
    let object = value.as_object().ok_or_else(|| {
        FleetContractError::Validation(
            "fleet federation error must be a JSON object or string"
                .to_string(),
        )
    })?;
    validate_allowed_fields(
        object,
        "fleet federation error",
        &["schema_version", "code", "message", "target", "details"],
    )?;
    validate_optional_schema(object, "fleet federation error")?;
    let code = optional_string_field(
        object,
        "code",
        "fleet federation error code",
        MAX_IDENTIFIER_BYTES,
    )?
    .unwrap_or_else(|| default_code.to_string());
    validate_reference_id("fleet federation error code", &code)?;
    let message = optional_string_field(
        object,
        "message",
        "fleet federation error message",
        MAX_LABEL_BYTES,
    )?
    .unwrap_or_else(|| code.clone());
    fleet_envelope_diagnostic(alias, operation, &code, "error", &message)
        .map(Some)
}

fn diagnostics_array(
    value: Option<&Value>,
    alias: Option<&str>,
    operation: Option<&str>,
) -> Result<Vec<FleetEnvelopeDiagnosticWire>, FleetContractError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    if value.is_null() {
        return Ok(Vec::new());
    }
    let values = value.as_array().ok_or_else(|| {
        FleetContractError::Validation(
            "fleet federation diagnostics must be a JSON array".to_string(),
        )
    })?;
    values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let mut diagnostic: FleetEnvelopeDiagnosticWire =
                wire_from_json_value(
                    value,
                    &format!("fleet federation diagnostics[{index}]"),
                )?;
            if diagnostic.alias.is_none() {
                diagnostic.alias = alias.map(str::to_string);
            }
            if diagnostic.operation.is_none() {
                diagnostic.operation = operation.map(str::to_string);
            }
            validate_fleet_envelope_diagnostic(&diagnostic)
        })
        .collect()
}

fn validate_fleet_envelope_diagnostic(
    diagnostic: &FleetEnvelopeDiagnosticWire,
) -> Result<FleetEnvelopeDiagnosticWire, FleetContractError> {
    validate_schema("fleet envelope diagnostic", diagnostic.schema_version)?;
    if let Some(alias) = &diagnostic.alias {
        validate_label(
            "fleet envelope diagnostic alias",
            alias,
            MAX_LABEL_BYTES,
        )?;
        reject_secretish("fleet envelope diagnostic alias", alias)?;
    }
    if let Some(operation) = &diagnostic.operation {
        validate_reference_id(
            "fleet envelope diagnostic operation",
            operation,
        )?;
    }
    validate_reference_id("fleet envelope diagnostic code", &diagnostic.code)?;
    validate_reference_id(
        "fleet envelope diagnostic severity",
        &diagnostic.severity,
    )?;
    if !matches!(diagnostic.severity.as_str(), "info" | "warning" | "error") {
        return Err(FleetContractError::Validation(
            "fleet envelope diagnostic severity must be info, warning, or error"
                .to_string(),
        ));
    }
    validate_label(
        "fleet envelope diagnostic message",
        &diagnostic.message,
        MAX_LABEL_BYTES,
    )?;
    reject_secretish("fleet envelope diagnostic message", &diagnostic.message)?;
    Ok(diagnostic.clone())
}

fn fleet_envelope_diagnostic(
    alias: Option<&str>,
    operation: Option<&str>,
    code: &str,
    severity: &str,
    message: &str,
) -> Result<FleetEnvelopeDiagnosticWire, FleetContractError> {
    let message = sanitize_diagnostic_message(message);
    let diagnostic = FleetEnvelopeDiagnosticWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        alias: alias.map(str::to_string),
        operation: operation.map(str::to_string),
        code: code.to_string(),
        severity: severity.to_string(),
        message,
    };
    validate_fleet_envelope_diagnostic(&diagnostic)
}

fn sanitize_diagnostic_message(message: &str) -> String {
    let normalized = replace_control_characters(message);
    let trimmed = normalized.trim();
    let redacted = if trimmed.contains("://")
        || trimmed.contains("Authorization:")
        || trimmed.to_ascii_lowercase().contains("bearer ")
        || reject_secretish("fleet envelope diagnostic message", trimmed)
            .is_err()
    {
        "federation diagnostic redacted"
    } else if trimmed.is_empty() {
        "federation diagnostic omitted"
    } else {
        trimmed
    };
    trim_to_limit(redacted, MAX_LABEL_BYTES)
}

fn host_status_healthy(status: &str) -> bool {
    status == "ok"
}

fn resolved_summaries_array(
    value: Option<&Value>,
    label: &str,
) -> Result<Vec<ResolvedAgentSummaryWire>, FleetContractError> {
    let value = value.ok_or_else(|| {
        FleetContractError::Validation(format!("{label} is required"))
    })?;
    let values = value.as_array().ok_or_else(|| {
        FleetContractError::Validation(format!("{label} must be a JSON array"))
    })?;
    values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let summary: ResolvedAgentSummaryWire =
                wire_from_json_value(value, &format!("{label}[{index}]"))?;
            validate_resolved_agent_summary(&summary)
        })
        .collect()
}
