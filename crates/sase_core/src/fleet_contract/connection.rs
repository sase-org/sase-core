use super::error::FleetContractError;
use super::error::FLEET_CONTRACT_SCHEMA_VERSION;
use super::status::ObservationFreshnessWire;
use super::validation::validate_absolute_https_endpoint;
use super::validation::validate_installation_id;
use super::validation::validate_non_negative_seconds;
use super::validation::validate_reference_id;
use super::validation::validate_schema;
use super::validation::validate_timestamp;
use serde::{Deserialize, Serialize};

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FleetConnectionKindWire {
    Gateway,
    Tunnel,
    Direct,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum TlsTrustModeWire {
    SystemRoots,
    PinnedCa,
    PinnedServerName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsTrustSettingsWire {
    pub schema_version: u32,
    pub mode: TlsTrustModeWire,
    pub ca_ref: Option<String>,
    pub server_name_ref: Option<String>,
}

/// Serializable routing plan. Provider choice is metadata and never
/// participates in locator equality.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectionPlanWire {
    pub schema_version: u32,
    pub provider_ref: String,
    pub endpoint: String,
    pub credential_ref: String,
    pub pinned_installation_id: String,
    pub connection_kind: FleetConnectionKindWire,
    pub tls: TlsTrustSettingsWire,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeDurationRequestWire {
    pub schema_version: u32,
    pub owner_started_at_unix: f64,
    pub owner_stopped_at_unix: Option<f64>,
    pub owner_observed_at_unix: f64,
    pub max_clock_anomaly_seconds: f64,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeDurationStateWire {
    Running,
    Stopped,
    ClockAnomalyClamped,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeDurationWire {
    pub schema_version: u32,
    pub elapsed_seconds: f64,
    pub state: RuntimeDurationStateWire,
    pub clamped: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheFreshnessRequestWire {
    pub schema_version: u32,
    pub viewer_monotonic_elapsed_seconds: Option<f64>,
    pub fresh_threshold_seconds: f64,
    pub stale_threshold_seconds: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheFreshnessWire {
    pub schema_version: u32,
    pub freshness: ObservationFreshnessWire,
    pub age_seconds: Option<f64>,
}

pub fn validate_connection_plan(
    plan: &ConnectionPlanWire,
) -> Result<ConnectionPlanWire, FleetContractError> {
    validate_schema("connection plan", plan.schema_version)?;
    validate_reference_id("provider_ref", &plan.provider_ref)?;
    validate_reference_id("credential_ref", &plan.credential_ref)?;
    validate_installation_id(&plan.pinned_installation_id)?;
    validate_absolute_https_endpoint(&plan.endpoint)?;
    plan.tls.validate()?;
    Ok(plan.clone())
}

pub fn classify_runtime_duration(
    request: &RuntimeDurationRequestWire,
) -> Result<RuntimeDurationWire, FleetContractError> {
    validate_schema("runtime duration request", request.schema_version)?;
    validate_timestamp("owner_started_at_unix", request.owner_started_at_unix)?;
    validate_timestamp(
        "owner_observed_at_unix",
        request.owner_observed_at_unix,
    )?;
    if let Some(stopped) = request.owner_stopped_at_unix {
        validate_timestamp("owner_stopped_at_unix", stopped)?;
    }
    if !request.max_clock_anomaly_seconds.is_finite()
        || request.max_clock_anomaly_seconds < 0.0
    {
        return Err(FleetContractError::Validation(
            "max_clock_anomaly_seconds must be finite and non-negative"
                .to_string(),
        ));
    }
    let end = request
        .owner_stopped_at_unix
        .unwrap_or(request.owner_observed_at_unix);
    let raw = end - request.owner_started_at_unix;
    let stopped = request.owner_stopped_at_unix.is_some();
    if raw >= 0.0 {
        return Ok(RuntimeDurationWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            elapsed_seconds: raw,
            state: if stopped {
                RuntimeDurationStateWire::Stopped
            } else {
                RuntimeDurationStateWire::Running
            },
            clamped: false,
        });
    }
    if raw.abs() <= request.max_clock_anomaly_seconds {
        return Ok(RuntimeDurationWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            elapsed_seconds: 0.0,
            state: RuntimeDurationStateWire::ClockAnomalyClamped,
            clamped: true,
        });
    }
    Err(FleetContractError::Validation(
        "owner timestamps are ordered backwards beyond the allowed clock anomaly"
            .to_string(),
    ))
}

pub fn classify_cache_freshness(
    request: &CacheFreshnessRequestWire,
) -> Result<CacheFreshnessWire, FleetContractError> {
    validate_schema("cache freshness request", request.schema_version)?;
    validate_non_negative_seconds(
        "fresh_threshold_seconds",
        request.fresh_threshold_seconds,
    )?;
    validate_non_negative_seconds(
        "stale_threshold_seconds",
        request.stale_threshold_seconds,
    )?;
    if request.fresh_threshold_seconds > request.stale_threshold_seconds {
        return Err(FleetContractError::Validation(
            "fresh_threshold_seconds cannot exceed stale_threshold_seconds"
                .to_string(),
        ));
    }
    let Some(age) = request.viewer_monotonic_elapsed_seconds else {
        return Ok(CacheFreshnessWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            freshness: ObservationFreshnessWire::Unknown,
            age_seconds: None,
        });
    };
    validate_non_negative_seconds("viewer_monotonic_elapsed_seconds", age)?;
    let freshness = if age <= request.fresh_threshold_seconds {
        ObservationFreshnessWire::Fresh
    } else if age >= request.stale_threshold_seconds {
        ObservationFreshnessWire::Stale
    } else {
        ObservationFreshnessWire::Aging
    };
    Ok(CacheFreshnessWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        freshness,
        age_seconds: Some(age),
    })
}

impl TlsTrustSettingsWire {
    pub(crate) fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("tls trust settings", self.schema_version)?;
        match self.mode {
            TlsTrustModeWire::SystemRoots => {
                if self.ca_ref.is_some() || self.server_name_ref.is_some() {
                    return Err(FleetContractError::Validation(
                        "system_roots trust mode must not include CA or server-name refs"
                            .to_string(),
                    ));
                }
            }
            TlsTrustModeWire::PinnedCa => {
                let Some(ca_ref) = &self.ca_ref else {
                    return Err(FleetContractError::Validation(
                        "pinned_ca trust mode requires ca_ref".to_string(),
                    ));
                };
                validate_reference_id("ca_ref", ca_ref)?;
                if self.server_name_ref.is_some() {
                    return Err(FleetContractError::Validation(
                        "pinned_ca trust mode must not include server_name_ref"
                            .to_string(),
                    ));
                }
            }
            TlsTrustModeWire::PinnedServerName => {
                let Some(server_name_ref) = &self.server_name_ref else {
                    return Err(FleetContractError::Validation(
                        "pinned_server_name trust mode requires server_name_ref"
                            .to_string(),
                    ));
                };
                validate_reference_id("server_name_ref", server_name_ref)?;
                if self.ca_ref.is_some() {
                    return Err(FleetContractError::Validation(
                        "pinned_server_name trust mode must not include ca_ref"
                            .to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
}
