//! Pure policy for durable artifact-link publication retries.
//!
//! Filesystem state, Git inspection, and worker execution live in Python. This
//! module owns the deterministic wire contract: identity keys, first-pending
//! preservation, due/aging classification, and bounded backoff after attempts.

use super::ArtifactLinkError;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const ARTIFACT_LINK_PUBLICATION_STATE_WIRE_SCHEMA_VERSION: u32 = 1;
pub const ARTIFACT_LINK_PUBLICATION_INITIAL_RETRY_SECONDS: f64 = 3_600.0;
pub const ARTIFACT_LINK_PUBLICATION_MAX_BACKOFF_SECONDS: f64 = 21_600.0;
pub const ARTIFACT_LINK_PUBLICATION_AGING_WARNING_SECONDS: f64 = 21_600.0;

const MAX_ID_LEN: usize = 200;
const MAX_PATH_LEN: usize = 4096;
const MAX_ERROR_LEN: usize = 2_000;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactLinkPublicationObservationWire {
    pub version: u32,
    pub project_key: String,
    pub role: String,
    pub repo_root: String,
    pub remote_url: String,
    pub upstream: String,
    pub head_revision: String,
    pub oldest_unpublished_at: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactLinkPublicationRecordWire {
    pub version: u32,
    pub key: String,
    pub project_key: String,
    pub role: String,
    pub repo_root: String,
    pub remote_url: String,
    pub upstream: String,
    pub first_pending_at: f64,
    pub pending_revision: String,
    pub pending_revision_observed_at: f64,
    pub attempt_count: u32,
    pub last_attempt_at: Option<f64>,
    pub last_error: Option<String>,
    pub last_log_path: Option<String>,
    pub next_due_at: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactLinkPublicationDueWire {
    pub version: u32,
    pub due: bool,
    pub aged: bool,
    pub age_seconds: f64,
    pub retry_after_seconds: f64,
    pub next_due_at: f64,
    pub warning: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactLinkPublicationAttemptStatusWire {
    Failed,
    Deferred,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactLinkPublicationAttemptWire {
    pub status: ArtifactLinkPublicationAttemptStatusWire,
    pub error: Option<String>,
    pub log_path: Option<String>,
}

pub fn artifact_link_publication_record_key(
    project_key: &str,
    role: &str,
    repo_root: &str,
    remote_url: &str,
    upstream: &str,
) -> Result<String, ArtifactLinkError> {
    let project_key = validate_short_text("project_key", project_key)?;
    let role = validate_short_text("role", role)?;
    let repo_root = validate_path_text("repo_root", repo_root)?;
    let remote_url = validate_path_text("remote_url", remote_url)?;
    let upstream = validate_short_text("upstream", upstream)?;

    let mut hasher = Sha256::new();
    for part in [&project_key, &role, &repo_root, &remote_url, &upstream] {
        hasher.update(part.len().to_be_bytes());
        hasher.update(part.as_bytes());
    }
    Ok(format!(
        "artifact-link-publication:v1:{}",
        hex::encode(hasher.finalize())
    ))
}

pub fn artifact_link_publication_register_pending(
    current: Option<&ArtifactLinkPublicationRecordWire>,
    observation: ArtifactLinkPublicationObservationWire,
    now: f64,
) -> Result<ArtifactLinkPublicationRecordWire, ArtifactLinkError> {
    validate_now(now)?;
    let observation = validate_observation(observation, now)?;
    let key = artifact_link_publication_record_key(
        &observation.project_key,
        &observation.role,
        &observation.repo_root,
        &observation.remote_url,
        &observation.upstream,
    )?;

    let reusable = current
        .filter(|record| record.key == key)
        .map(validate_record)
        .transpose()?;
    let observed_since = observation.oldest_unpublished_at.unwrap_or(now);
    let first_pending_at = reusable
        .as_ref()
        .map(|record| record.first_pending_at.min(observed_since))
        .unwrap_or(observed_since);
    let next_due_at = reusable
        .as_ref()
        .map(|record| record.next_due_at)
        .filter(|value| value.is_finite() && *value >= 0.0)
        .unwrap_or_else(|| next_hourly_tick_after(first_pending_at));

    Ok(ArtifactLinkPublicationRecordWire {
        version: ARTIFACT_LINK_PUBLICATION_STATE_WIRE_SCHEMA_VERSION,
        key,
        project_key: observation.project_key,
        role: observation.role,
        repo_root: observation.repo_root,
        remote_url: observation.remote_url,
        upstream: observation.upstream,
        first_pending_at,
        pending_revision: observation.head_revision,
        pending_revision_observed_at: now,
        attempt_count: reusable
            .as_ref()
            .map(|record| record.attempt_count)
            .unwrap_or(0),
        last_attempt_at: reusable
            .as_ref()
            .and_then(|record| record.last_attempt_at),
        last_error: reusable
            .as_ref()
            .and_then(|record| record.last_error.clone()),
        last_log_path: reusable
            .as_ref()
            .and_then(|record| record.last_log_path.clone()),
        next_due_at,
    })
}

pub fn artifact_link_publication_due(
    record: ArtifactLinkPublicationRecordWire,
    now: f64,
) -> Result<ArtifactLinkPublicationDueWire, ArtifactLinkError> {
    validate_now(now)?;
    let record = validate_record(&record)?;
    let age_seconds = (now - record.first_pending_at).max(0.0);
    let retry_after_seconds = (record.next_due_at - now).max(0.0);
    let aged = age_seconds >= ARTIFACT_LINK_PUBLICATION_AGING_WARNING_SECONDS;
    let warning = aged.then(|| {
        format!(
            "artifact-link publication for project {} role {} has been pending for {:.0}s",
            record.project_key, record.role, age_seconds
        )
    });

    Ok(ArtifactLinkPublicationDueWire {
        version: ARTIFACT_LINK_PUBLICATION_STATE_WIRE_SCHEMA_VERSION,
        due: now >= record.next_due_at,
        aged,
        age_seconds,
        retry_after_seconds,
        next_due_at: record.next_due_at,
        warning,
    })
}

pub fn artifact_link_publication_mark_attempt(
    record: ArtifactLinkPublicationRecordWire,
    attempt: ArtifactLinkPublicationAttemptWire,
    now: f64,
) -> Result<ArtifactLinkPublicationRecordWire, ArtifactLinkError> {
    validate_now(now)?;
    let mut record = validate_record(&record)?;
    let error = clean_optional_text("error", attempt.error, MAX_ERROR_LEN)?;
    let log_path =
        clean_optional_text("log_path", attempt.log_path, MAX_PATH_LEN)?;

    record.last_attempt_at = Some(now);
    record.last_error = error;
    record.last_log_path = log_path;
    match attempt.status {
        ArtifactLinkPublicationAttemptStatusWire::Deferred => {
            record.next_due_at = next_hourly_tick_after(now);
        }
        ArtifactLinkPublicationAttemptStatusWire::Failed => {
            record.attempt_count = record.attempt_count.saturating_add(1);
            record.next_due_at = if record.attempt_count == 1 {
                next_hourly_tick_after(now)
            } else {
                now + retry_backoff_seconds_after_attempt(record.attempt_count)
            };
        }
    }
    Ok(record)
}

pub fn retry_backoff_seconds_after_attempt(attempt_count: u32) -> f64 {
    if attempt_count <= 1 {
        return ARTIFACT_LINK_PUBLICATION_INITIAL_RETRY_SECONDS;
    }
    let exponent = attempt_count.saturating_sub(1).min(10);
    let multiplier = 2_u32.saturating_pow(exponent);
    (ARTIFACT_LINK_PUBLICATION_INITIAL_RETRY_SECONDS * f64::from(multiplier))
        .min(ARTIFACT_LINK_PUBLICATION_MAX_BACKOFF_SECONDS)
}

fn next_hourly_tick_after(now: f64) -> f64 {
    ((now / ARTIFACT_LINK_PUBLICATION_INITIAL_RETRY_SECONDS).floor() + 1.0)
        * ARTIFACT_LINK_PUBLICATION_INITIAL_RETRY_SECONDS
}

fn validate_observation(
    observation: ArtifactLinkPublicationObservationWire,
    now: f64,
) -> Result<ArtifactLinkPublicationObservationWire, ArtifactLinkError> {
    if observation.version
        != ARTIFACT_LINK_PUBLICATION_STATE_WIRE_SCHEMA_VERSION
    {
        return Err(validation(format!(
            "observation version must be {}, got {}",
            ARTIFACT_LINK_PUBLICATION_STATE_WIRE_SCHEMA_VERSION,
            observation.version
        )));
    }
    let oldest_unpublished_at = observation
        .oldest_unpublished_at
        .map(|value| validate_timestamp("oldest_unpublished_at", value, now))
        .transpose()?;
    Ok(ArtifactLinkPublicationObservationWire {
        version: observation.version,
        project_key: validate_short_text(
            "project_key",
            &observation.project_key,
        )?,
        role: validate_short_text("role", &observation.role)?,
        repo_root: validate_path_text("repo_root", &observation.repo_root)?,
        remote_url: validate_path_text("remote_url", &observation.remote_url)?,
        upstream: validate_short_text("upstream", &observation.upstream)?,
        head_revision: validate_short_text(
            "head_revision",
            &observation.head_revision,
        )?,
        oldest_unpublished_at,
    })
}

fn validate_record(
    record: &ArtifactLinkPublicationRecordWire,
) -> Result<ArtifactLinkPublicationRecordWire, ArtifactLinkError> {
    if record.version != ARTIFACT_LINK_PUBLICATION_STATE_WIRE_SCHEMA_VERSION {
        return Err(validation(format!(
            "record version must be {}, got {}",
            ARTIFACT_LINK_PUBLICATION_STATE_WIRE_SCHEMA_VERSION, record.version
        )));
    }
    validate_nonnegative_finite("first_pending_at", record.first_pending_at)?;
    validate_nonnegative_finite(
        "pending_revision_observed_at",
        record.pending_revision_observed_at,
    )?;
    validate_nonnegative_finite("next_due_at", record.next_due_at)?;
    if let Some(value) = record.last_attempt_at {
        validate_nonnegative_finite("last_attempt_at", value)?;
    }

    let key = artifact_link_publication_record_key(
        &record.project_key,
        &record.role,
        &record.repo_root,
        &record.remote_url,
        &record.upstream,
    )?;
    if key != record.key {
        return Err(validation("record key does not match record identity"));
    }

    Ok(ArtifactLinkPublicationRecordWire {
        version: record.version,
        key: record.key.clone(),
        project_key: validate_short_text("project_key", &record.project_key)?,
        role: validate_short_text("role", &record.role)?,
        repo_root: validate_path_text("repo_root", &record.repo_root)?,
        remote_url: validate_path_text("remote_url", &record.remote_url)?,
        upstream: validate_short_text("upstream", &record.upstream)?,
        first_pending_at: record.first_pending_at,
        pending_revision: validate_short_text(
            "pending_revision",
            &record.pending_revision,
        )?,
        pending_revision_observed_at: record.pending_revision_observed_at,
        attempt_count: record.attempt_count,
        last_attempt_at: record.last_attempt_at,
        last_error: clean_optional_text(
            "last_error",
            record.last_error.clone(),
            MAX_ERROR_LEN,
        )?,
        last_log_path: clean_optional_text(
            "last_log_path",
            record.last_log_path.clone(),
            MAX_PATH_LEN,
        )?,
        next_due_at: record.next_due_at,
    })
}

fn validate_timestamp(
    label: &str,
    value: f64,
    now: f64,
) -> Result<f64, ArtifactLinkError> {
    validate_nonnegative_finite(label, value)?;
    Ok(value.min(now))
}

fn validate_now(now: f64) -> Result<(), ArtifactLinkError> {
    validate_nonnegative_finite("now", now)
}

fn validate_nonnegative_finite(
    label: &str,
    value: f64,
) -> Result<(), ArtifactLinkError> {
    if value.is_finite() && value >= 0.0 {
        Ok(())
    } else {
        Err(validation(format!(
            "{label} must be a non-negative finite timestamp"
        )))
    }
}

fn validate_short_text(
    label: &str,
    value: &str,
) -> Result<String, ArtifactLinkError> {
    validate_text(label, value, MAX_ID_LEN)
}

fn validate_path_text(
    label: &str,
    value: &str,
) -> Result<String, ArtifactLinkError> {
    validate_text(label, value, MAX_PATH_LEN)
}

fn validate_text(
    label: &str,
    value: &str,
    max_chars: usize,
) -> Result<String, ArtifactLinkError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(validation(format!("{label} must not be empty")));
    }
    if trimmed.chars().count() > max_chars {
        return Err(validation(format!("{label} is too long")));
    }
    Ok(trimmed.to_string())
}

fn clean_optional_text(
    label: &str,
    value: Option<String>,
    max_chars: usize,
) -> Result<Option<String>, ArtifactLinkError> {
    value
        .map(|raw| {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else if trimmed.chars().count() > max_chars {
                Err(validation(format!("{label} is too long")))
            } else {
                Ok(Some(trimmed.to_string()))
            }
        })
        .transpose()
        .map(Option::flatten)
}

fn validation(message: impl Into<String>) -> ArtifactLinkError {
    ArtifactLinkError::validation(message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observation(now: f64) -> ArtifactLinkPublicationObservationWire {
        ArtifactLinkPublicationObservationWire {
            version: ARTIFACT_LINK_PUBLICATION_STATE_WIRE_SCHEMA_VERSION,
            project_key: "gh_acme__widget".to_string(),
            role: "research".to_string(),
            repo_root: "/tmp/research".to_string(),
            remote_url: "git@example.com:acme/widget--research.git".to_string(),
            upstream: "origin/main".to_string(),
            head_revision: "abc123".to_string(),
            oldest_unpublished_at: Some(now),
        }
    }

    #[test]
    fn pending_registration_preserves_age_across_head_changes() {
        let first = artifact_link_publication_register_pending(
            None,
            observation(1_000.0),
            1_200.0,
        )
        .unwrap();
        assert_eq!(first.first_pending_at, 1_000.0);
        assert_eq!(first.pending_revision, "abc123");
        assert_eq!(first.next_due_at, 3_600.0);

        let mut later_observation = observation(1_800.0);
        later_observation.head_revision = "def456".to_string();
        let later = artifact_link_publication_register_pending(
            Some(&first),
            later_observation,
            1_900.0,
        )
        .unwrap();
        assert_eq!(later.key, first.key);
        assert_eq!(later.first_pending_at, 1_000.0);
        assert_eq!(later.pending_revision, "def456");
        assert_eq!(later.next_due_at, 3_600.0);
    }

    #[test]
    fn due_policy_reports_retry_after_and_aging_warning() {
        let record = artifact_link_publication_register_pending(
            None,
            observation(0.0),
            10.0,
        )
        .unwrap();
        let early =
            artifact_link_publication_due(record.clone(), 3_599.0).unwrap();
        assert!(!early.due);
        assert_eq!(early.retry_after_seconds, 1.0);
        assert!(!early.aged);

        let aged = artifact_link_publication_due(
            record,
            ARTIFACT_LINK_PUBLICATION_AGING_WARNING_SECONDS,
        )
        .unwrap();
        assert!(aged.due);
        assert!(aged.aged);
        assert!(aged.warning.unwrap().contains("pending"));
    }

    #[test]
    fn failed_attempts_exponentially_back_off_at_six_hours() {
        let mut record = artifact_link_publication_register_pending(
            None,
            observation(0.0),
            10.0,
        )
        .unwrap();
        for (now, expected_due) in [
            (10.0, 3_600.0),
            (3_600.0, 10_800.0),
            (10_800.0, 25_200.0),
            (25_200.0, 46_800.0),
            (46_800.0, 68_400.0),
        ] {
            record = artifact_link_publication_mark_attempt(
                record,
                ArtifactLinkPublicationAttemptWire {
                    status: ArtifactLinkPublicationAttemptStatusWire::Failed,
                    error: Some("network".to_string()),
                    log_path: Some("/tmp/log".to_string()),
                },
                now,
            )
            .unwrap();
            assert_eq!(record.next_due_at, expected_due);
        }
        assert_eq!(record.attempt_count, 5);
    }

    #[test]
    fn deferred_attempt_does_not_consume_failure_backoff() {
        let record = artifact_link_publication_register_pending(
            None,
            observation(0.0),
            10.0,
        )
        .unwrap();
        let deferred = artifact_link_publication_mark_attempt(
            record,
            ArtifactLinkPublicationAttemptWire {
                status: ArtifactLinkPublicationAttemptStatusWire::Deferred,
                error: Some("worker already running".to_string()),
                log_path: None,
            },
            3_650.0,
        )
        .unwrap();
        assert_eq!(deferred.attempt_count, 0);
        assert_eq!(deferred.next_due_at, 7_200.0);
    }
}
