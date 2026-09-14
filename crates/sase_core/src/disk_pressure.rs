//! Shared disk-pressure classification for SASE surfaces.
//!
//! The host supplies filesystem observations and disk-owner rows; this module
//! owns the threshold math and top-owner selection so doctor, housekeeping and
//! cleanup orchestration agree about when the host is under pressure.

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const DISK_PRESSURE_WIRE_SCHEMA_VERSION: u32 = 1;

pub const PRESSURE_STATUS_OK: &str = "OK";
pub const PRESSURE_STATUS_WARN: &str = "WARN";
pub const PRESSURE_STATUS_ERROR: &str = "ERROR";

#[derive(Debug, Error)]
pub enum DiskPressureError {
    #[error("disk pressure requires schema_version {expected}, got {actual}")]
    SchemaVersion { expected: u32, actual: u32 },
    #[error("disk pressure request contains no filesystem observations")]
    MissingObservations,
    #[error("disk pressure request contains invalid thresholds")]
    InvalidThresholds,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskPressureRequestWire {
    pub schema_version: u32,
    pub observations: Vec<DiskPressureObservationWire>,
    pub owner_rows: Vec<DiskPressureOwnerRowWire>,
    pub absolute_warn_free_bytes: u64,
    pub absolute_error_free_bytes: u64,
    pub warn_free_percent: f64,
    pub error_free_percent: f64,
    pub top_owner_min_bytes: u64,
    pub top_owner_limit: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskPressureObservationWire {
    pub label: String,
    pub role: String,
    pub path: String,
    pub measurement_path: String,
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub free_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DiskPressureOwnerRowWire {
    pub section: String,
    pub name: String,
    pub path: String,
    pub size_bytes: u64,
    pub owner: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DiskPressureResultWire {
    pub schema_version: u32,
    pub status: String,
    pub trigger_level: Option<String>,
    pub pressure_active: bool,
    pub observations: Vec<DiskPressureObservationResultWire>,
    pub top_owners: Vec<DiskPressureOwnerRowWire>,
    pub effective_warn_free_bytes: u64,
    pub effective_error_free_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DiskPressureObservationResultWire {
    pub label: String,
    pub role: String,
    pub path: String,
    pub measurement_path: String,
    pub status: String,
    pub problem: Option<String>,
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub free_bytes: u64,
    pub free_percent: f64,
    pub warn_threshold_bytes_effective: u64,
    pub error_threshold_bytes_effective: u64,
}

pub fn classify_disk_pressure(
    request: &DiskPressureRequestWire,
) -> Result<DiskPressureResultWire, DiskPressureError> {
    if request.schema_version != DISK_PRESSURE_WIRE_SCHEMA_VERSION {
        return Err(DiskPressureError::SchemaVersion {
            expected: DISK_PRESSURE_WIRE_SCHEMA_VERSION,
            actual: request.schema_version,
        });
    }
    if request.observations.is_empty() {
        return Err(DiskPressureError::MissingObservations);
    }
    if !request.warn_free_percent.is_finite()
        || !request.error_free_percent.is_finite()
        || request.warn_free_percent < 0.0
        || request.error_free_percent < 0.0
    {
        return Err(DiskPressureError::InvalidThresholds);
    }

    let error_percent = request.error_free_percent.max(0.0);
    let warn_percent = request.warn_free_percent.max(error_percent);
    let mut observation_results =
        Vec::with_capacity(request.observations.len());
    let mut aggregate = PRESSURE_STATUS_OK;
    let mut effective_warn = 0_u64;
    let mut effective_error = 0_u64;

    for observation in &request.observations {
        let error_threshold = effective_threshold_bytes(
            observation.total_bytes,
            request.absolute_error_free_bytes,
            error_percent,
        );
        let warn_threshold = effective_threshold_bytes(
            observation.total_bytes,
            request.absolute_warn_free_bytes,
            warn_percent,
        )
        .max(error_threshold);
        effective_warn = effective_warn.max(warn_threshold);
        effective_error = effective_error.max(error_threshold);
        let status = if observation.free_bytes < error_threshold {
            PRESSURE_STATUS_ERROR
        } else if observation.free_bytes < warn_threshold {
            PRESSURE_STATUS_WARN
        } else {
            PRESSURE_STATUS_OK
        };
        aggregate = aggregate_status(aggregate, status);
        let free_percent =
            free_percent(observation.free_bytes, observation.total_bytes);
        observation_results.push(DiskPressureObservationResultWire {
            label: observation.label.clone(),
            role: observation.role.clone(),
            path: observation.path.clone(),
            measurement_path: observation.measurement_path.clone(),
            status: status.to_string(),
            problem: threshold_problem(ThresholdProblemInput {
                label: &observation.label,
                status,
                free_bytes: observation.free_bytes,
                total_bytes: observation.total_bytes,
                error_threshold,
                warn_threshold,
                absolute_error_bytes: request.absolute_error_free_bytes,
                absolute_warn_bytes: request.absolute_warn_free_bytes,
                error_percent,
                warn_percent,
            }),
            total_bytes: observation.total_bytes,
            used_bytes: observation.used_bytes,
            free_bytes: observation.free_bytes,
            free_percent,
            warn_threshold_bytes_effective: warn_threshold,
            error_threshold_bytes_effective: error_threshold,
        });
    }

    Ok(DiskPressureResultWire {
        schema_version: DISK_PRESSURE_WIRE_SCHEMA_VERSION,
        status: aggregate.to_string(),
        trigger_level: if aggregate == PRESSURE_STATUS_OK {
            None
        } else {
            Some(aggregate.to_string())
        },
        pressure_active: aggregate != PRESSURE_STATUS_OK,
        observations: observation_results,
        top_owners: top_owner_rows(
            &request.owner_rows,
            request.top_owner_min_bytes,
            request.top_owner_limit,
        ),
        effective_warn_free_bytes: effective_warn,
        effective_error_free_bytes: effective_error,
    })
}

fn effective_threshold_bytes(
    total_bytes: u64,
    absolute_bytes: u64,
    percent: f64,
) -> u64 {
    let proportional = ((total_bytes as f64) * (percent / 100.0)).floor();
    absolute_bytes.max(proportional.max(0.0) as u64)
}

fn free_percent(free_bytes: u64, total_bytes: u64) -> f64 {
    if total_bytes == 0 {
        return 0.0;
    }
    ((free_bytes as f64) / (total_bytes as f64)) * 100.0
}

fn aggregate_status(current: &'static str, next: &'static str) -> &'static str {
    if current == PRESSURE_STATUS_ERROR || next == PRESSURE_STATUS_ERROR {
        PRESSURE_STATUS_ERROR
    } else if current == PRESSURE_STATUS_WARN || next == PRESSURE_STATUS_WARN {
        PRESSURE_STATUS_WARN
    } else {
        PRESSURE_STATUS_OK
    }
}

#[derive(Debug, Clone, Copy)]
struct ThresholdProblemInput<'a> {
    label: &'a str,
    status: &'a str,
    free_bytes: u64,
    total_bytes: u64,
    error_threshold: u64,
    warn_threshold: u64,
    absolute_error_bytes: u64,
    absolute_warn_bytes: u64,
    error_percent: f64,
    warn_percent: f64,
}

fn threshold_problem(input: ThresholdProblemInput<'_>) -> Option<String> {
    let (threshold, absolute, percent) = match input.status {
        PRESSURE_STATUS_ERROR => (
            input.error_threshold,
            input.absolute_error_bytes,
            input.error_percent,
        ),
        PRESSURE_STATUS_WARN => (
            input.warn_threshold,
            input.absolute_warn_bytes,
            input.warn_percent,
        ),
        _ => return None,
    };
    let gib = 1024.0 * 1024.0 * 1024.0;
    let basis = if threshold > absolute {
        format_percent_basis(percent)
    } else {
        format!("{} GB", (threshold as f64 / gib).round() as u64)
    };
    Some(format!(
        "{} has less than {basis} free ({:.1} GiB available, {:.1}% of volume)",
        input.label,
        (input.free_bytes as f64) / gib,
        free_percent(input.free_bytes, input.total_bytes),
    ))
}

fn format_percent_basis(percent: f64) -> String {
    if (percent.fract()).abs() < f64::EPSILON {
        format!("{percent:.0}%")
    } else {
        format!("{percent:.2}%")
    }
}

fn top_owner_rows(
    rows: &[DiskPressureOwnerRowWire],
    min_bytes: u64,
    limit: u32,
) -> Vec<DiskPressureOwnerRowWire> {
    let mut selected = rows
        .iter()
        .filter(|row| row.size_bytes >= min_bytes)
        .cloned()
        .collect::<Vec<_>>();
    selected.sort_by(|left, right| {
        right
            .size_bytes
            .cmp(&left.size_bytes)
            .then_with(|| {
                owner_rank(&left.status).cmp(&owner_rank(&right.status))
            })
            .then_with(|| left.owner.cmp(&right.owner))
            .then_with(|| left.path.cmp(&right.path))
    });
    selected.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
    selected
}

fn owner_rank(status: &str) -> u8 {
    if status == "unowned" {
        0
    } else {
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(free_bytes: u64, total_bytes: u64) -> DiskPressureRequestWire {
        DiskPressureRequestWire {
            schema_version: DISK_PRESSURE_WIRE_SCHEMA_VERSION,
            observations: vec![DiskPressureObservationWire {
                label: "workspace_root".to_string(),
                role: "primary".to_string(),
                path: "/work".to_string(),
                measurement_path: "/work".to_string(),
                total_bytes,
                used_bytes: total_bytes.saturating_sub(free_bytes),
                free_bytes,
            }],
            owner_rows: vec![],
            absolute_warn_free_bytes: 3 * 1024 * 1024 * 1024,
            absolute_error_free_bytes: 1024 * 1024 * 1024,
            warn_free_percent: 5.0,
            error_free_percent: 1.0,
            top_owner_min_bytes: 0,
            top_owner_limit: 5,
        }
    }

    #[test]
    fn large_volume_warns_by_proportion() {
        let total = 875 * 1024 * 1024 * 1024;
        let free = 40 * 1024 * 1024 * 1024;

        let result = classify_disk_pressure(&request(free, total)).unwrap();

        assert_eq!(result.status, PRESSURE_STATUS_WARN);
        assert!(result.effective_warn_free_bytes > free);
        assert_eq!(result.observations[0].status, PRESSURE_STATUS_WARN);
    }

    #[test]
    fn small_volume_warns_by_absolute_floor() {
        let total = 20 * 1024 * 1024 * 1024;
        let free = 2 * 1024 * 1024 * 1024;

        let result = classify_disk_pressure(&request(free, total)).unwrap();

        assert_eq!(result.status, PRESSURE_STATUS_WARN);
        assert_eq!(result.effective_warn_free_bytes, 3 * 1024 * 1024 * 1024);
    }

    #[test]
    fn top_owners_are_size_sorted_and_bounded() {
        let mut req =
            request(10 * 1024 * 1024 * 1024, 100 * 1024 * 1024 * 1024);
        req.top_owner_min_bytes = 10;
        req.top_owner_limit = 2;
        req.owner_rows = vec![
            owner("owned-small", 9, "owned"),
            owner("owned-big", 100, "owned"),
            owner("unowned-big", 200, "unowned"),
            owner("owned-mid", 50, "owned"),
        ];

        let result = classify_disk_pressure(&req).unwrap();

        assert_eq!(
            result
                .top_owners
                .iter()
                .map(|row| row.name.as_str())
                .collect::<Vec<_>>(),
            vec!["unowned-big", "owned-big"],
        );
    }

    fn owner(
        name: &str,
        size_bytes: u64,
        status: &str,
    ) -> DiskPressureOwnerRowWire {
        DiskPressureOwnerRowWire {
            section: "section".to_string(),
            name: name.to_string(),
            path: format!("/{name}"),
            size_bytes,
            owner: status.to_string(),
            status: status.to_string(),
        }
    }
}
