//! Deterministic policy for retired-request recovery and publication completion.
//!
//! The host owns the outbox lock, Git transaction, snapshot reads, and page
//! writes. This module selects which terminal rows to revive, classifies
//! deferred-prompt obligations, and decides whether a request is fulfilled.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::commit_sha::commit_shas_equivalent;

pub const PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION: u32 = 1;

pub const PUBLICATION_KIND_RUN: &str = "run";
pub const PUBLICATION_KIND_SESSION: &str = "session";

pub const PROMPT_STATUS_RESTORED: &str = "restored";
pub const PROMPT_STATUS_ALREADY_ARCHIVED: &str = "already_archived";
pub const PROMPT_STATUS_NOT_APPLICABLE: &str = "not_applicable";
pub const PROMPT_STATUS_UNAVAILABLE: &str = "unavailable";
pub const PROMPT_STATUS_FAILED: &str = "failed";

const MEMBER_SEPARATOR: &str = "--";

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PublicationRecoveryError {
    #[error("unsupported publication recovery wire schema_version {0}")]
    UnsupportedSchema(u32),
    #[error("invalid publication recovery request: {0}")]
    InvalidRequest(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationRetryRowWire {
    pub global_agent: String,
    pub primary_revision: String,
    pub terminal: bool,
    pub quarantined: bool,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub terminal_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationRetrySelectionRequestWire {
    pub schema_version: u32,
    pub retry_retired: bool,
    pub retry_quarantined: bool,
    #[serde(default)]
    pub rows: Vec<PublicationRetryRowWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationRetrySelectedWire {
    pub global_agent: String,
    pub primary_revision: String,
    pub prior_class: String,
    pub prior_failure: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationRetrySelectionResponseWire {
    pub schema_version: u32,
    pub selected: Vec<PublicationRetrySelectedWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationRequestIdentityWire {
    pub global_agent: String,
    pub local_agent: String,
    pub primary_revision: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationPageObservationWire {
    pub path: String,
    pub exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationRunObservationWire {
    pub global_name: String,
    pub local_name: String,
    #[serde(default)]
    pub commit_shas: Vec<String>,
    #[serde(default)]
    pub has_prompt_file: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationContainerObservationWire {
    pub kind: String,
    pub global_name: String,
    #[serde(default)]
    pub member_global_names: Vec<String>,
    #[serde(default)]
    pub commit_shas: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationCompletionRequestWire {
    pub schema_version: u32,
    pub request: PublicationRequestIdentityWire,
    #[serde(default)]
    pub pages: Vec<PublicationPageObservationWire>,
    #[serde(default)]
    pub runs: Vec<PublicationRunObservationWire>,
    #[serde(default)]
    pub containers: Vec<PublicationContainerObservationWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationCompletionResponseWire {
    pub schema_version: u32,
    pub kind: String,
    pub required_page: String,
    pub fulfilled: bool,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeferredPromptClassifyRequestWire {
    pub schema_version: u32,
    pub restore_wrote: bool,
    #[serde(default)]
    pub restore_error: Option<String>,
    pub local_source_present: bool,
    pub archive_present: bool,
    #[serde(default)]
    pub prompt_file_in_snapshot: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeferredPromptClassifyResponseWire {
    pub schema_version: u32,
    pub status: String,
    pub blocks_acknowledgment: bool,
    pub reason: String,
}

pub fn select_publication_retries(
    request: &PublicationRetrySelectionRequestWire,
) -> Result<PublicationRetrySelectionResponseWire, PublicationRecoveryError> {
    require_schema(request.schema_version)?;
    if !request.retry_retired && !request.retry_quarantined {
        return Err(PublicationRecoveryError::InvalidRequest(
            "at least one of retry_retired or retry_quarantined must be true"
                .to_string(),
        ));
    }
    let mut selected = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for row in &request.rows {
        let global_agent = require_nonempty("global_agent", &row.global_agent)?;
        let primary_revision =
            require_nonempty("primary_revision", &row.primary_revision)?;
        let key = (global_agent.clone(), primary_revision.clone());
        if !seen.insert(key) {
            continue;
        }
        let class = if row.terminal {
            "retired"
        } else if row.quarantined {
            "quarantined"
        } else {
            "active"
        };
        let eligible = (request.retry_retired && row.terminal)
            || (request.retry_quarantined && row.quarantined && !row.terminal);
        if !eligible {
            continue;
        }
        let prior_failure = row
            .terminal_reason
            .as_deref()
            .or(row.last_error.as_deref())
            .unwrap_or("unknown reason")
            .trim();
        let prior_failure = if prior_failure.is_empty() {
            "unknown reason"
        } else {
            prior_failure
        };
        selected.push(PublicationRetrySelectedWire {
            global_agent,
            primary_revision,
            prior_class: class.to_string(),
            prior_failure: prior_failure.to_string(),
        });
    }
    Ok(PublicationRetrySelectionResponseWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        selected,
    })
}

pub fn decide_publication_request_completion(
    request: &PublicationCompletionRequestWire,
) -> Result<PublicationCompletionResponseWire, PublicationRecoveryError> {
    require_schema(request.schema_version)?;
    let global_agent =
        require_nonempty("global_agent", &request.request.global_agent)?;
    let local_agent =
        require_nonempty("local_agent", &request.request.local_agent)?;
    let primary_revision = require_nonempty(
        "primary_revision",
        &request.request.primary_revision,
    )?;

    let kind =
        classify_request_kind(&local_agent, &global_agent, &request.containers);
    let required_page = match kind {
        PUBLICATION_KIND_SESSION => format!("sessions/{global_agent}.md"),
        _ => format!("agents/{global_agent}/README.md"),
    };
    let page_exists = request
        .pages
        .iter()
        .any(|page| page.path == required_page && page.exists);
    if !page_exists {
        return Ok(PublicationCompletionResponseWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            kind: kind.to_string(),
            required_page,
            fulfilled: false,
            reason: "required publication page is absent".to_string(),
        });
    }
    if revision_associated(
        kind,
        &global_agent,
        &local_agent,
        &primary_revision,
        &request.runs,
        &request.containers,
    ) {
        Ok(PublicationCompletionResponseWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            kind: kind.to_string(),
            required_page,
            fulfilled: true,
            reason:
                "required page exists and the requested revision is associated"
                    .to_string(),
        })
    } else {
        Ok(PublicationCompletionResponseWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            kind: kind.to_string(),
            required_page,
            fulfilled: false,
            reason:
                "required page exists without the requested primary revision"
                    .to_string(),
        })
    }
}

pub fn classify_deferred_prompt_obligation(
    request: &DeferredPromptClassifyRequestWire,
) -> Result<DeferredPromptClassifyResponseWire, PublicationRecoveryError> {
    require_schema(request.schema_version)?;
    if let Some(error) = request
        .restore_error
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Ok(DeferredPromptClassifyResponseWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            status: PROMPT_STATUS_FAILED.to_string(),
            blocks_acknowledgment: true,
            reason: error.to_string(),
        });
    }
    if request.restore_wrote {
        return Ok(DeferredPromptClassifyResponseWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            status: PROMPT_STATUS_RESTORED.to_string(),
            blocks_acknowledgment: false,
            reason: "deferred prompt archive was restored".to_string(),
        });
    }
    if request.archive_present {
        return Ok(DeferredPromptClassifyResponseWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            status: PROMPT_STATUS_ALREADY_ARCHIVED.to_string(),
            blocks_acknowledgment: false,
            reason: "prompt archive already exists in the sidecar".to_string(),
        });
    }
    if request.prompt_file_in_snapshot == Some(false)
        && !request.local_source_present
    {
        return Ok(DeferredPromptClassifyResponseWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            status: PROMPT_STATUS_NOT_APPLICABLE.to_string(),
            blocks_acknowledgment: false,
            reason:
                "published run has no prompt file and no local prompt source"
                    .to_string(),
        });
    }
    Ok(DeferredPromptClassifyResponseWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        status: PROMPT_STATUS_UNAVAILABLE.to_string(),
        blocks_acknowledgment: true,
        reason: "prompt archive is unresolved and a missing local source does not prove there was no prompt obligation"
            .to_string(),
    })
}

fn classify_request_kind(
    local_agent: &str,
    global_agent: &str,
    containers: &[PublicationContainerObservationWire],
) -> &'static str {
    if is_member_spelling(local_agent) || is_member_spelling(global_agent) {
        return PUBLICATION_KIND_RUN;
    }
    if containers.iter().any(|container| {
        is_session_container(&container.kind)
            && container.global_name == global_agent
    }) {
        return PUBLICATION_KIND_SESSION;
    }
    PUBLICATION_KIND_RUN
}

fn revision_associated(
    kind: &str,
    global_agent: &str,
    local_agent: &str,
    primary_revision: &str,
    runs: &[PublicationRunObservationWire],
    containers: &[PublicationContainerObservationWire],
) -> bool {
    if kind == PUBLICATION_KIND_SESSION {
        let matching: Vec<&PublicationContainerObservationWire> = containers
            .iter()
            .filter(|container| {
                is_session_container(&container.kind)
                    && container.global_name == global_agent
            })
            .collect();
        if matching
            .iter()
            .any(|container| sha_in(primary_revision, &container.commit_shas))
        {
            return true;
        }
        let member_names: std::collections::BTreeSet<&str> = matching
            .iter()
            .flat_map(|container| container.member_global_names.iter())
            .map(String::as_str)
            .collect();
        return runs.iter().any(|run| {
            (member_names.contains(run.global_name.as_str())
                || run.global_name == global_agent
                || session_name(&run.local_name) == local_agent
                || session_name(&run.global_name) == global_agent)
                && sha_in(primary_revision, &run.commit_shas)
        });
    }
    runs.iter().any(|run| {
        (run.global_name == global_agent || run.local_name == local_agent)
            && sha_in(primary_revision, &run.commit_shas)
    })
}

fn sha_in(requested: &str, observed: &[String]) -> bool {
    observed
        .iter()
        .any(|candidate| commit_shas_equivalent(requested, candidate))
}

fn is_member_spelling(name: &str) -> bool {
    name.contains(MEMBER_SEPARATOR)
}

fn is_session_container(kind: &str) -> bool {
    kind == "session" || kind == "family"
}

fn session_name(name: &str) -> &str {
    name.rsplit_once(MEMBER_SEPARATOR)
        .map(|(prefix, _)| prefix)
        .unwrap_or(name)
}

fn require_schema(schema_version: u32) -> Result<(), PublicationRecoveryError> {
    if schema_version == PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION {
        Ok(())
    } else {
        Err(PublicationRecoveryError::UnsupportedSchema(schema_version))
    }
}

fn require_nonempty(
    label: &str,
    value: &str,
) -> Result<String, PublicationRecoveryError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        Err(PublicationRecoveryError::InvalidRequest(format!(
            "{label} must not be empty"
        )))
    } else {
        Ok(trimmed.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn retry_row(
        global_agent: &str,
        revision: &str,
        terminal: bool,
        quarantined: bool,
        failure: &str,
    ) -> PublicationRetryRowWire {
        PublicationRetryRowWire {
            global_agent: global_agent.to_string(),
            primary_revision: revision.to_string(),
            terminal,
            quarantined,
            last_error: Some(failure.to_string()),
            terminal_reason: terminal.then(|| failure.to_string()),
        }
    }

    #[test]
    fn select_retries_revives_only_the_requested_terminal_classes() {
        let request = PublicationRetrySelectionRequestWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            retry_retired: true,
            retry_quarantined: true,
            rows: vec![
                retry_row(
                    "alice.athena.foo",
                    "a".repeat(40).as_str(),
                    true,
                    false,
                    "legacy mismatch",
                ),
                retry_row(
                    "alice.athena.bar",
                    "b".repeat(40).as_str(),
                    false,
                    true,
                    "push rejected",
                ),
                retry_row(
                    "alice.athena.active",
                    "c".repeat(40).as_str(),
                    false,
                    false,
                    "still working",
                ),
                retry_row(
                    "alice.athena.other",
                    "d".repeat(40).as_str(),
                    true,
                    false,
                    "legacy mismatch",
                ),
            ],
        };
        let selected = select_publication_retries(&request).unwrap().selected;
        assert_eq!(selected.len(), 3);
        assert_eq!(selected[0].prior_class, "retired");
        assert_eq!(selected[0].prior_failure, "legacy mismatch");
        assert_eq!(selected[1].prior_class, "quarantined");
        assert!(selected
            .iter()
            .all(|row| row.global_agent != "alice.athena.active"));
    }

    #[test]
    fn select_retries_retired_only_leaves_quarantined_rows() {
        let request = PublicationRetrySelectionRequestWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            retry_retired: true,
            retry_quarantined: false,
            rows: vec![
                retry_row(
                    "alice.athena.foo",
                    "a".repeat(40).as_str(),
                    true,
                    false,
                    "legacy",
                ),
                retry_row(
                    "alice.athena.bar",
                    "b".repeat(40).as_str(),
                    false,
                    true,
                    "temp",
                ),
            ],
        };
        let selected = select_publication_retries(&request).unwrap().selected;
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].global_agent, "alice.athena.foo");
    }

    #[test]
    fn completion_requires_session_page_and_revision_not_an_unrelated_readme() {
        let revision = "a".repeat(40);
        let request = PublicationCompletionRequestWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            request: PublicationRequestIdentityWire {
                global_agent: "alice.athena.foo.bar".to_string(),
                local_agent: "foo.bar".to_string(),
                primary_revision: revision.clone(),
            },
            pages: vec![
                PublicationPageObservationWire {
                    path: "agents/alice.athena.foo.bar/README.md".to_string(),
                    exists: true,
                },
                PublicationPageObservationWire {
                    path: "sessions/alice.athena.foo.bar.md".to_string(),
                    exists: true,
                },
            ],
            runs: vec![PublicationRunObservationWire {
                global_name: "alice.athena.foo.bar--code".to_string(),
                local_name: "foo.bar--code".to_string(),
                commit_shas: vec![revision],
                has_prompt_file: true,
            }],
            containers: vec![PublicationContainerObservationWire {
                kind: "family".to_string(),
                global_name: "alice.athena.foo.bar".to_string(),
                member_global_names: vec![
                    "alice.athena.foo.bar--code".to_string()
                ],
                commit_shas: vec![],
            }],
        };
        let decision = decide_publication_request_completion(&request).unwrap();
        assert_eq!(decision.kind, PUBLICATION_KIND_SESSION);
        assert_eq!(decision.required_page, "sessions/alice.athena.foo.bar.md");
        assert!(decision.fulfilled);
    }

    #[test]
    fn completion_rejects_preexisting_run_page_without_revision() {
        let request = PublicationCompletionRequestWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            request: PublicationRequestIdentityWire {
                global_agent: "alice.athena.foo".to_string(),
                local_agent: "foo".to_string(),
                primary_revision: "a".repeat(40),
            },
            pages: vec![PublicationPageObservationWire {
                path: "agents/alice.athena.foo/README.md".to_string(),
                exists: true,
            }],
            runs: vec![PublicationRunObservationWire {
                global_name: "alice.athena.foo".to_string(),
                local_name: "foo".to_string(),
                commit_shas: vec!["b".repeat(40)],
                has_prompt_file: false,
            }],
            containers: vec![],
        };
        let decision = decide_publication_request_completion(&request).unwrap();
        assert_eq!(decision.kind, PUBLICATION_KIND_RUN);
        assert!(!decision.fulfilled);
        assert!(decision.reason.contains("primary revision"));
    }

    #[test]
    fn historical_member_spelling_requires_the_run_page() {
        let revision = "c".repeat(40);
        let request = PublicationCompletionRequestWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            request: PublicationRequestIdentityWire {
                global_agent: "alice.athena.foo--code".to_string(),
                local_agent: "foo--code".to_string(),
                primary_revision: revision.clone(),
            },
            pages: vec![
                PublicationPageObservationWire {
                    path: "agents/alice.athena.foo--code/README.md".to_string(),
                    exists: true,
                },
                PublicationPageObservationWire {
                    path: "sessions/alice.athena.foo.md".to_string(),
                    exists: true,
                },
            ],
            runs: vec![PublicationRunObservationWire {
                global_name: "alice.athena.foo--code".to_string(),
                local_name: "foo--code".to_string(),
                commit_shas: vec![revision],
                has_prompt_file: true,
            }],
            containers: vec![PublicationContainerObservationWire {
                kind: "session".to_string(),
                global_name: "alice.athena.foo".to_string(),
                member_global_names: vec!["alice.athena.foo--code".to_string()],
                commit_shas: vec![],
            }],
        };
        let decision = decide_publication_request_completion(&request).unwrap();
        assert_eq!(decision.kind, PUBLICATION_KIND_RUN);
        assert_eq!(
            decision.required_page,
            "agents/alice.athena.foo--code/README.md"
        );
        assert!(decision.fulfilled);
    }

    #[test]
    fn missing_local_prompt_source_does_not_clear_an_unknown_obligation() {
        let decision = classify_deferred_prompt_obligation(
            &DeferredPromptClassifyRequestWire {
                schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
                restore_wrote: false,
                restore_error: None,
                local_source_present: false,
                archive_present: false,
                prompt_file_in_snapshot: None,
            },
        )
        .unwrap();
        assert_eq!(decision.status, PROMPT_STATUS_UNAVAILABLE);
        assert!(decision.blocks_acknowledgment);
    }

    #[test]
    fn existing_archive_or_snapshot_without_prompt_file_are_not_losses() {
        let archived = classify_deferred_prompt_obligation(
            &DeferredPromptClassifyRequestWire {
                schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
                restore_wrote: false,
                restore_error: None,
                local_source_present: false,
                archive_present: true,
                prompt_file_in_snapshot: Some(true),
            },
        )
        .unwrap();
        assert_eq!(archived.status, PROMPT_STATUS_ALREADY_ARCHIVED);
        assert!(!archived.blocks_acknowledgment);

        let not_applicable = classify_deferred_prompt_obligation(
            &DeferredPromptClassifyRequestWire {
                schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
                restore_wrote: false,
                restore_error: None,
                local_source_present: false,
                archive_present: false,
                prompt_file_in_snapshot: Some(false),
            },
        )
        .unwrap();
        assert_eq!(not_applicable.status, PROMPT_STATUS_NOT_APPLICABLE);
        assert!(!not_applicable.blocks_acknowledgment);
    }
}
