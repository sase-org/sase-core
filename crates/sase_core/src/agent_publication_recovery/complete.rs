use crate::commit_sha::commit_shas_equivalent;

use super::support::{require_nonempty, require_schema};
use super::wire::{
    DeferredPromptClassifyRequestWire, DeferredPromptClassifyResponseWire,
    PublicationCompletionRequestWire, PublicationCompletionResponseWire,
    PublicationContainerObservationWire, PublicationRecoveryError,
    PublicationRunObservationWire, PROMPT_STATUS_ALREADY_ARCHIVED,
    PROMPT_STATUS_FAILED, PROMPT_STATUS_NOT_APPLICABLE, PROMPT_STATUS_RESTORED,
    PROMPT_STATUS_UNAVAILABLE, PUBLICATION_KIND_RUN, PUBLICATION_KIND_SESSION,
    PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION, PUBLICATION_SCOPE_LINKED,
};

const MEMBER_SEPARATOR: &str = "--";

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
    if is_linked_scope(&request.request.scope) {
        return Ok(PublicationCompletionResponseWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            kind: kind.to_string(),
            required_page,
            fulfilled: true,
            reason: "required page exists for a linked-scope request"
                .to_string(),
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

fn is_linked_scope(scope: &str) -> bool {
    scope == PUBLICATION_SCOPE_LINKED
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
