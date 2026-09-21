//! Completion candidates for every editor trigger source.
//!
//! Formerly a single 7,260-line module, split by completion source:
//! `@` artifact references, catalog-backed assists, directive clauses,
//! `vcs:` references, and trigger/context detection.

mod artifact_ref;
mod assist_candidates;
mod directive_candidates;
mod trigger_context;
mod vcs_candidates;

#[cfg(test)]
mod tests;

pub use artifact_ref::{
    build_artifact_ref_kind_completion_candidates,
    build_artifact_ref_payload_completion_candidates,
    build_artifact_ref_payload_inventory,
    detect_artifact_ref_context_at_position, ARTIFACT_REF_COMMIT_ABBREV,
    ARTIFACT_REF_COMMIT_MAX_ROWS, ARTIFACT_REF_COMMIT_SCAN_LIMIT,
    ARTIFACT_REF_COMMIT_TIMEOUT_DEFAULT, ARTIFACT_REF_COMMIT_TIMEOUT_ENV,
};
pub use assist_candidates::{
    assist_entries_from_catalog, build_agent_completion_candidates,
    build_identity_target_candidates, build_snippet_completion_candidates,
    build_wait_completion_candidates,
    build_wait_completion_candidates_for_form,
    build_wait_completion_candidates_for_form_with_flags,
    build_xprompt_arg_name_candidates, build_xprompt_completion_candidates,
};
pub use directive_candidates::build_directive_clause_candidates;
pub use trigger_context::{
    classify_completion_context,
    classify_completion_context_with_artifacts_and_workflows,
    classify_completion_context_with_workflows, colon_args_skeleton,
    named_args_skeleton,
};
pub use vcs_candidates::{
    apply_vcs_project_selection, apply_vcs_ref_selection,
    apply_vcs_repo_selection, build_vcs_project_completion_candidates,
    build_vcs_ref_completion_candidates, build_vcs_repo_completion_candidates,
    detect_vcs_ref_context_at_position, detect_vcs_repo_context_at_position,
};

// Test-only surface: helpers the split test files exercise directly. These
// stay `pub(crate)` so the public API above is unchanged.
#[cfg(test)]
pub(crate) use artifact_ref::{
    append_ranked_commit_candidates, artifact_ref_commit_timeout,
    commit_age_label, commit_log_output, parse_commit_timeout,
    sort_commit_candidates, CommitCandidate, CommitLogFailure,
    CommitLogIoCause, ScratchStep, ARTIFACT_REF_MAX_SCAN_RESULTS,
    ARTIFACT_REF_REPOSITORY_KIND_SIDECAR,
};
#[cfg(test)]
pub(crate) use vcs_candidates::{
    vcs_edits_conflict, vcs_prepend_offset, vcs_project_byte_edits,
    vcs_replace_regex, VcsByteEdit, VcsProjectByteEdits,
};
