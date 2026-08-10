//! PyO3 bindings for `sase_core`.
//!
//! Phase 1D exposed `parse_project_bytes`. Phase 2D added the query
//! tokenizer/parser/evaluator. Phase 3C adds the agent-artifact snapshot
//! scanner. Currently exposed:
//!
//! - `parse_project_bytes(path: str, data: bytes) -> list[dict]`
//! - `parse_patch_project_bytes(path: str, data: bytes) -> list[dict]`
//! - `tokenize_query(query: str) -> list[dict]`
//! - `parse_query(query: str) -> dict`
//! - `canonicalize_query(query: str) -> str`
//! - `compile_corpus(specs: list[dict]) -> QueryCorpusHandle`
//! - `compile_query(query: str) -> QueryProgramHandle`
//! - `evaluate_many(program: QueryProgramHandle, corpus: QueryCorpusHandle) -> list[bool]`
//! - `evaluate_query_many(query: str, specs: list[dict]) -> list[bool]`
//! - `scan_agent_artifacts(projects_root: str, options: dict | None = None) -> dict`
//! - `scan_agent_artifact_dirs(projects_root: str, artifact_dirs: list[str], options: dict | None = None) -> dict`
//! - `aggregate_clan_runtime(members: list[dict], now_epoch_seconds: float) -> dict`
//! - `rebuild_agent_artifact_index(index_path: str, projects_root: str, options: dict | None = None) -> dict`
//! - `upsert_agent_artifact_index_row(index_path: str, projects_root: str, artifact_dir: str, options: dict | None = None) -> dict`
//! - `delete_agent_artifact_index_row(index_path: str, artifact_dir: str) -> dict`
//! - `delete_agent_artifact_index_row_bounded(index_path: str, artifact_dir: str, busy_timeout_ms: int) -> dict`
//! - `terminalize_stale_active_agent_artifact_index_rows(index_path: str, projects_root: str, stale_after_seconds: int, max_rows: int | None = None, options: dict | None = None) -> dict`
//! - `replace_agent_artifact_index_dismissed_agents(index_path: str, identities: list[dict]) -> dict`
//! - `read_agent_artifact_index_meta(index_path: str, key: str) -> str | None`
//! - `write_agent_artifact_index_meta(index_path: str, key: str, value: str) -> None`
//! - `agent_artifact_index_status(index_path: str) -> dict`
//! - `query_agent_artifact_index(index_path: str, projects_root: str, query: dict | None = None, options: dict | None = None) -> dict`
//! - `query_related_agent_artifact_dirs(index_path: str, artifact_dir: str, seed_timestamps: list[str]) -> list[str]`
//! - `query_agent_archive(root: str, request: dict) -> dict`
//! - `agent_archive_facet_counts(root: str, request: dict) -> dict`
//! - `mark_agent_archive_bundles_revived(root: str, request: dict) -> dict`
//! - `verify_agent_archive_index(root: str) -> dict`
//! - `delete_dismissed_agent_group(root: str, group_id: str) -> bool`
//! - `plan_agent_cleanup(targets: list[dict], request: dict) -> dict`
//! - `save_dismissed_agents_index(path: str, identities: list[dict]) -> None`
//! - `save_dismissed_bundle(bundle_root: str, bundle: dict) -> dict`
//! - `delete_agent_artifacts(artifacts_dir: str) -> dict`
//! - `release_workspace_from_content(content: str, workspace_num: int, workflow: str | None, cl_name: str | None) -> dict`
//! - `mark_hook_agents_as_killed(hooks: list[dict], suffixes: list[str]) -> list[dict]`
//! - `mark_mentor_agents_as_killed(mentors: list[dict], suffixes: list[str]) -> list[dict]`
//! - `mark_comment_agents_as_killed(comments: list[dict], suffixes: list[str]) -> list[dict]`
//! - `remove_workspace_suffix(status: str) -> str`
//! - `is_valid_status_transition(from_status: str, to_status: str) -> bool`
//! - `read_status_from_lines(lines: list[str], changespec_name: str) -> str | None`
//! - `apply_status_update(lines: list[str], changespec_name: str, new_status: str) -> str`
//! - `plan_status_transition(request: dict) -> dict`
//! - `parse_git_name_status_z(stdout: str) -> list[dict]`
//! - `parse_git_branch_name(stdout: str) -> str | None`
//! - `derive_git_workspace_name(remote_url: str | None, root_path: str | None) -> str | None`
//! - `parse_git_conflicted_files(stdout: str) -> list[str]`
//! - `parse_git_local_changes(stdout: str) -> str | None`
//! - `vcs_log_wire_schema_version() -> int`
//! - `parse_git_log(stdout: str) -> list[dict]`
//! - `classify_commit_presence(commits: list[dict], ahead_ids: list[str], behind_ids: list[str]) -> list[dict]`
//! - `aggregate_commit_log(repos: list[tuple[str, list[dict]]], limit: int) -> list[dict]`
//! - `parse_merge_summary(subject: str, body: str) -> dict | None`
//! - `read_project_lifecycle_from_content(content: str) -> dict`
//! - `apply_project_lifecycle_update(content: str, state: str) -> str`
//! - `apply_project_aliases_update(content: str, aliases: list[str]) -> str`
//! - `apply_project_name_update(content: str, name: str | None) -> str`
//! - `list_project_records(projects_root: str, include_states: list[str], include_home: bool = False, projects_only: bool = False) -> list[dict]`
//! - `read_notifications_snapshot(path: str, include_dismissed: bool, expire_due_snoozes: bool = False) -> dict`
//! - `read_current_notifications_snapshot(path: str, include_dismissed: bool) -> dict`
//! - `apply_notification_state_update(path: str, update: dict) -> dict`
//! - `apply_notification_state_update_counts(path: str, update: dict) -> dict`
//! - `append_notification(path: str, notification: dict) -> dict`
//! - `append_notification_counts(path: str, notification: dict) -> dict`
//! - `rewrite_notifications(path: str, notifications: list[dict]) -> dict`
//! - `rewrite_notifications_counts(path: str, notifications: list[dict]) -> dict`
//! - `classify_notification_tabs(notifications: list[dict]) -> dict`
//! - `read_prompt_stash_snapshot(path: str) -> dict`
//! - `append_prompt_stash(path: str, entry: dict) -> dict`
//! - `pop_prompt_stash(path: str, ids: list[str]) -> dict`
//! - `set_prompt_stash_pinned(path: str, ids: list[str], pinned: bool) -> dict`
//! - `rewrite_prompt_stash(path: str, entries: list[dict]) -> dict`
//! - `read_tasks_snapshot(path: str) -> dict`
//! - `append_task(path: str, task: dict, history_limit: int) -> dict`
//! - `update_task(path: str, update: dict) -> dict`
//! - `prune_tasks(path: str, history_limit: int) -> dict`
//! - `is_agent_name_template(value: str) -> bool`
//! - `parse_agent_name_template(template: str) -> dict`
//! - `agent_name_template_key(template: str) -> dict | None`
//! - `iter_agent_name_key_markers(text: str) -> list[dict]`
//! - `render_agent_name_template(template: str, token: str) -> str`
//! - `agent_name_template_namespace_template(template: str) -> str`
//! - `match_agent_name_template(template: str, concrete: str) -> str | None`
//! - `compare_agent_name_template_tokens(left: str, right: str) -> int`
//! - `agent_name_template_tokens_after(after: str | None, count: int) -> list[str]`
//! - `validate_machine_name(name: str) -> None`
//! - `qualify_machine_agent_name(name: str, machine_name: str) -> str`
//! - `strip_machine_agent_name(name: str, machine_name: str) -> str`
//! - `machine_hood_of(name: str, known_machines: list[str]) -> str | None`
//! - `validate_agent_name(name: str) -> None`
//! - `validate_agent_username(username: str) -> None`
//! - `validate_agent_owner(username: str, machine_name: str) -> None`
//! - `classify_agent_ownership(source_machine_name: str, target_username: str, target_machine_name: str, source_username: str | None = None) -> str`
//! - `classify_legacy_v1_group_ownership(group_machine_name: str, target_username: str, target_machine_name: str, v2_hood_published: bool, proven_entry_count: int, total_entry_count: int) -> str`
//! - `commit_shas_equivalent(left: str, right: str) -> bool`
//! - `normalize_agent_archive_name(name: str) -> str`
//! - `globalize_agent_name(local_name: str, username: str, machine_name: str) -> str`
//! - `globalize_legacy_agent_name(legacy_name: str, username: str, machine_name: str) -> str`
//! - `strip_global_agent_name(global_name: str, username: str, machine_name: str) -> str`
//! - `localize_agent_name(global_name: str, source_machine_name: str, target_username: str, target_machine_name: str, source_username: str | None = None) -> str`
//! - `parse_agent_family_name(name: str) -> dict`
//! - `agent_local_hood(name: str) -> str`
//! - `agent_name_in_hood(name: str, hood: str) -> bool`
//! - `agent_name_ancestors(name: str) -> list[str]`
//! - `agent_link_target(name: str, username: str, machine_name: str) -> dict`
//! - `agent_relationship_schema_version() -> int`
//! - `validate_agent_relationship_batch(batch: dict) -> dict`
//! - `rewrite_agent_relationship_batch(batch: dict, destination_ids: dict[str, str]) -> dict`
//! - `agent_launch_wire_schema_version() -> int`
//! - `prepare_agent_launch(request: dict, python_executable: str, runner_script: str, output_root: str, sase_tmpdir: str | None = None, preallocated_env: dict | None = None) -> dict`
//! - `spawn_prepared_agent_process(prepared: dict, env: dict, claim_callback: Callable[[int], bool] | None = None) -> int`
//! - `allocate_launch_timestamp_batch(count: int, base_timestamp: str, after_timestamp: str | None = None) -> list[str]`
//! - `plan_agent_launch_fanout(prompt: str, launch_kind: str | None = None) -> dict`
//! - `inline_code_ranges(text: str, masked_ranges: list[tuple[int, int]] | None = None) -> list[tuple[int, int]]`
//! - `resolve_agent_family_parent(request: dict) -> dict`
//! - `resolve_clan_summary(request: dict) -> dict`
//! - `resolve_clan_tribe(request: dict) -> dict`
//! - `list_workspace_claims_from_content(content: str) -> list[dict]`
//! - `plan_claim_workspace_from_content(content: str, request: dict) -> dict`
//! - `plan_transfer_workspace_claim_from_content(content: str, request: dict) -> dict`
//! - `allocate_and_claim_workspace_from_content(content: str, min_workspace: int, max_workspace: int, request: dict) -> dict`
//! - `config_field_model(schema: dict) -> dict`
//! - `config_inventory(request: dict) -> dict`
//! - `config_plan_edit(request: dict) -> dict`
//! - `config_validate(request: dict) -> list[dict]`
//! - `effort_override_get(sase_home: str, now: float | None = None) -> dict | None`
//! - `effort_override_set_relative(sase_home: str, effort: str, source: str, duration_seconds: float | None = None, now: float | None = None) -> dict`
//! - `effort_override_set_until(sase_home: str, effort: str, expires_at: float, source: str, now: float | None = None) -> dict`
//! - `effort_override_clear(sase_home: str) -> bool`
//! - `runner_limit_override_get(sase_home: str, now: float | None = None) -> dict | None`
//! - `runner_limit_override_set_relative(sase_home: str, limit: int, source: str, duration_seconds: float | None = None, now: float | None = None) -> dict`
//! - `runner_limit_override_set_until(sase_home: str, limit: int, expires_at: float, source: str, now: float | None = None) -> dict`
//! - `runner_limit_override_clear(sase_home: str) -> bool`
//! - `resolve_effective_effort(explicit_effort: str | None = None, alias_effort: str | None = None, temporary_effort: str | None = None, configured_effort: str | None = None) -> dict`
//! - `parse_chop_result(document: str) -> dict`
//! - `validate_chop_result(result: dict) -> dict`
//! - `validate_chop_proposal(proposal: dict, index: int, prior_ids: list[str]) -> dict`
//! - `derive_chop_agent_name(chop_name: str, target_key: str | None, proposal_index: int, run_token: str | None = None) -> str`
//! - `evaluate_chop_decision(request: dict) -> dict`
//! - `apply_chop_checkpoint_update(request: dict) -> dict`
//! - `check_and_record_chop_once_per(request: dict) -> dict`
//! - `release_chop_once_per(request: dict) -> dict`
//! - `expand_chop_targets(request: dict) -> dict`
//! - `parse_chop_duration(value: str) -> int`
//! - `split_axe_description(text: str) -> tuple[str, str]`
//! - `validate_axe_config(request: dict) -> list[dict]`
//! - `axe_status_wire_schema_version() -> int`
//! - `classify_axe_status(request: dict) -> dict`
//! - `sase_content_layout(home_root: str, project_root: str | None = None, chezmoi_root: str | None = None, project: str | None = None) -> dict`
//! - `resolve_layout_candidates(policy: str, exists: list[bool]) -> dict`
//! - `skill_reference_name(skill_name: str, project: str | None = None) -> str`
//! - `skill_placement_issue(source: str, in_skill_source: bool, declares_skill: bool, migrate_to: str | None = None) -> dict | None`
//! - `memory_reference_name(stem: str) -> str`
//! - `memory_reference_stem(name: str) -> str | None`
//! - `reserved_memory_namespace_issue(source: str, name: str) -> dict | None`
//! - `memory_note_issue(source: str, stem: str, note_type: str | None = None) -> dict | None`
//! - `plan_validate(content: str, tier: str, mode: str = "authoring") -> dict`
//! - `plan_frontmatter_schema(tier: str) -> list[dict]`
//! - `artifact_consumption_summary(log_path: str, refs: list[str] | None = None) -> dict`
//! - `artifact_consumption_wire_schema_version() -> int`
//! - `artifact_ref_parse(value: str) -> dict`
//! - `artifact_ref_render(reference: dict) -> str`
//! - `artifact_ref_canonicalize(path: str, context: dict) -> str | None`
//! - `artifact_ref_resolve(reference: str | dict, context: dict) -> dict`
//! - `artifact_ref_list_normalize(entries: list[str]) -> list[str]`
//! - `artifact_ref_list_parse(entries: list[str]) -> list[dict]`
//! - `artifact_ref_list_resolve(entries: list[str], context: dict) -> dict`
//! - `artifact_ref_list_resolution_wire_schema_version() -> int`
//! - `artifact_ref_context_wire_schema_version() -> int`
//! - `artifact_ref_path_filter_wire_schema_version() -> int`
//! - `artifact_ref_filter_path_payloads(kind: str, candidates: list[str], path_globs: list[str] | None = None) -> dict`
//! - `artifact_ref_scan_prompt(text: str) -> list[dict]`
//! - `artifact_ref_wire_schema_version() -> int`
//! - `prompt_artifact_pool_filename(sha256: str, original_name: str) -> str`
//! - `prompt_artifact_manifest_parse(data: bytes) -> list[dict]`
//! - `prompt_artifact_manifest_render_record(record: dict) -> str`
//! - `prompt_artifact_manifest_select(records: list[dict], agent_artifacts_dir: str) -> list[dict]`
//! - `prompt_artifact_rewrite_links(prompt: str, records: list[dict], resolver: Callable[[dict], str | None]) -> dict`
//! - `prompt_artifact_wire_schema_version() -> int`
//! - `artifact_files_query(index_path: str, filters: dict) -> list[dict]`
//! - `artifact_file_materialize_vcs(request: dict) -> dict`
//! - `artifact_file_query_wire_schema_version() -> int`
//! - `artifact_file_store_economics(index_path: str, options: dict) -> dict`
//! - `artifact_file_retention_plan(index_path: str, policy: dict) -> dict`
//! - `artifact_file_trash_store(request: dict) -> dict`
//! - `artifact_file_trash_list(trash_root: str) -> dict`
//! - `artifact_file_trash_restore(request: dict) -> dict`
//! - `artifact_file_trash_purge(request: dict) -> dict`
//! - `artifact_file_lifecycle_wire_schema_version() -> int`
//! - `sdd_artifact_link_parse(document: str) -> dict`
//! - `sdd_artifact_link_render(link_type: str, label: str, target: str) -> str`
//! - `sdd_artifact_link_upsert(document: str, link_type: str, label: str, target: str, remove_legacy: bool, allow_resolved_mixed: bool) -> str`
//! - `sdd_plan_header_block_wire_schema_version() -> int`
//! - `sdd_plan_header_block_parse(document: str) -> dict`
//! - `sdd_plan_header_block_render(sections: list[dict]) -> str`
//! - `sdd_plan_header_block_upsert_section(document: str, section: dict, remove_legacy: bool, allow_resolved_mixed: bool) -> str`
//! - `sdd_plan_header_block_replace(document: str, sections: list[dict], remove_legacy: bool, allow_resolved_mixed: bool) -> str`
//! - `sdd_plan_header_block_remove_section(document: str, kind: str, remove_legacy: bool, allow_resolved_mixed: bool) -> str`
//! - `at_reference_context(text: str, line: int, character: int, known_kinds:
//!   Sequence[str] | None = None) -> dict | None`
//! - `AtReferenceInventory(payloads: Sequence[dict])`
//! - `artifact_ref_payload_inventory(kind: str, context: dict) -> dict`
//! - `at_reference_menu(context: dict, inventory: dict, payload_index:
//!   AtReferenceInventory | None = None, options: dict | None = None) -> dict`
//! - `fuzzy_match(query: str, text: str) -> dict | None`
//! - `placeholder_completion(text: str, line: int, character: int, common:
//!   Sequence[str] | None = None) -> dict | None`
//! - `placeholder_spans(text: str) -> list[dict]`
//! - `raw_placeholder_fields(text: str, context_width: int) -> list[dict]`
//! - `substitute_raw_placeholders(text: str, values: dict[str, str]) -> str`
//! - `placeholder_input_names(texts: list[str]) -> list[str]`
//! - `bead_append_note(beads_dir: str, issue_id: str, entry: str, author: str | None = None, now: str | None = None) -> dict`
//! - `bead_plus_one(beads_dir: str, issue_id: str, reporter: str, note: str, refs: list[str] | None = None, now: str | None = None, observed_since: str | None = None) -> dict`
//! - `bead_snooze(beads_dir: str, issue_id: str, until: str, plus_ones: int | None = None, reason: str = "", actor: str = "", now: str | None = None) -> dict`
//! - `bead_snooze_cancel(beads_dir: str, issue_id: str, actor: str = "", now: str | None = None) -> dict`
//! - `bead_close(beads_dir: str, issue_ids: list[str], reason: str | None = None, resolution: str | None = None, force: bool = False, now: str | None = None, note: str | None = None, author: str | None = None) -> dict`
//! - `bead_update_many(beads_dir: str, issue_ids: list[str], fields: dict) -> dict`
//! - `bead_needs_size_check_relax_migration(create_table_sql: str | None) -> bool`
//! - `bead_size_check_relax_migration_sql() -> str`
//! - `bead_needs_task_ready_migration(create_table_sql: str | None) -> bool`
//! - `bead_task_ready_migration_sql() -> str`
//! - `bead_needs_snoozed_status_migration(create_table_sql: str | None) -> bool`
//! - `bead_snoozed_status_migration_sql() -> str`
//! - `telemetry_cleanup_matching_labels(store_path: str, request: dict, busy_timeout_ms: int = 250) -> dict`
//! - `telemetry_record_batch(store_path: str, batch: dict, busy_timeout_ms: int = 250) -> dict`
//! - `telemetry_query_instant(store_path: str, request: dict, busy_timeout_ms: int = 250) -> dict`
//! - `telemetry_query_range(store_path: str, request: dict, busy_timeout_ms: int = 250) -> dict`
//! - `telemetry_prune(store_path: str, request: dict, busy_timeout_ms: int = 250) -> dict`
//! - `telemetry_store_stats(store_path: str, busy_timeout_ms: int = 250) -> dict`
//! - `agent_stats_query_runs(index_path: str, request: dict) -> dict` (run,
//!   runtime, project, and Patch work rollups)
//! - `agent_stats_query_activity(index_path: str, sase_home: str, request: dict)`
//!   `-> dict` (project-filterable skills and memories plus global documents)
//! - `compose_snippet_catalog(templates: dict[str, str]) -> dict`
//!
//! Dict shapes mirror the Python wire dataclasses in
//! `sase_100/src/sase/core/query_wire.py` (rectangular, all fields always
//! present) so the Python side can rehydrate them with the existing wire
//! converters. The pure `sase_core` crate uses serde's tagged-union shape
//! for `QueryExprWire`; the converters in this file translate between the
//! two so neither side has to bend.
//!
//! `QueryErrorWire` is surfaced as a Python `ValueError` whose message is
//! the wire error's `Display` form so existing UI validation that catches
//! `ValueError` keeps working.

// `pyo3::pyfunction` macro expansion contains a `From::from` for `PyErr`
// that clippy 1.95+ reports as `useless_conversion`. The annotation has
// to live at the module scope because the macro generates wrapper code
// outside the user-written function body.
#![allow(clippy::useless_conversion)]

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs::File;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use pyo3::exceptions::{
    PyOSError, PyRuntimeError, PyTimeoutError, PyValueError,
};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDict, PyList, PyTuple};
use sase_core::agent_archive::{
    agent_archive_facet_counts as core_agent_archive_facet_counts,
    mark_agent_archive_bundles_revived as core_mark_agent_archive_bundles_revived,
    query_agent_archive as core_query_agent_archive,
    verify_agent_archive_index as core_verify_agent_archive_index,
    AgentArchiveFacetRequestWire, AgentArchiveQueryRequestWire,
    AgentArchiveReviveMarkRequestWire,
};
use sase_core::agent_clan_tribe::{
    resolve_clan_summary as core_resolve_clan_summary,
    resolve_clan_tribe as core_resolve_clan_tribe,
    ClanTribeResolutionRequestWire,
};
use sase_core::agent_cleanup::{
    cleanup_request_from_json_value,
    delete_agent_artifact_markers as core_delete_agent_artifact_markers,
    mark_comment_agents_as_killed as core_mark_comment_agents_as_killed,
    mark_hook_agents_as_killed as core_mark_hook_agents_as_killed,
    mark_mentor_agents_as_killed as core_mark_mentor_agents_as_killed,
    plan_agent_cleanup as core_plan_agent_cleanup,
    release_workspace_from_content as core_release_workspace_from_content,
    save_dismissed_agents_index as core_save_dismissed_agents_index,
    save_dismissed_bundle_json as core_save_dismissed_bundle_json,
    AgentCleanupIdentityWire, AgentCleanupRequestWire, AgentCleanupTargetWire,
};
use sase_core::agent_family::{
    resolve_agent_family_parent as core_resolve_agent_family_parent,
    AgentFamilyParentResolutionRequestWire,
};
use sase_core::agent_group_archive::{
    delete_dismissed_agent_group as core_delete_dismissed_agent_group,
    list_dismissed_agent_groups as core_list_dismissed_agent_groups,
    list_recent_dismissed_agent_groups as core_list_recent_dismissed_agent_groups,
    load_dismissed_agent_group as core_load_dismissed_agent_group,
    load_recent_dismissed_agent_group as core_load_recent_dismissed_agent_group,
    mark_dismissed_agent_group_revived as core_mark_dismissed_agent_group_revived,
    mark_recent_dismissed_agent_group_revived as core_mark_recent_dismissed_agent_group_revived,
    record_recent_dismissed_agent_group as core_record_recent_dismissed_agent_group,
    save_dismissed_agent_group as core_save_dismissed_agent_group,
    SavedAgentGroupWire,
};
use sase_core::agent_identity::{
    agent_link_target as core_agent_link_target,
    agent_local_hood as core_agent_local_hood,
    agent_name_ancestors as core_agent_name_ancestors,
    agent_name_in_hood as core_agent_name_in_hood,
    classify_agent_ownership as core_classify_agent_ownership,
    classify_legacy_v1_group_ownership as core_classify_legacy_v1_group_ownership,
    globalize_agent_name as core_globalize_agent_name,
    globalize_legacy_agent_name as core_globalize_legacy_agent_name,
    localize_agent_name as core_localize_agent_name,
    normalize_agent_archive_name as core_normalize_agent_archive_name,
    parse_agent_family_name as core_parse_agent_family_name,
    rewrite_agent_relationship_batch as core_rewrite_agent_relationship_batch,
    strip_global_agent_name as core_strip_global_agent_name,
    validate_agent_name as core_validate_agent_name,
    validate_agent_relationship_batch as core_validate_agent_relationship_batch,
    validate_agent_username as core_validate_agent_username,
    AgentOwnerIdentity, AgentRelationshipBatchWire, AgentSourceOwnerIdentity,
    LegacyV1GroupOwnershipEvidence, AGENT_RELATIONSHIP_SCHEMA_VERSION,
};
use sase_core::agent_launch::{
    allocate_and_claim_workspace_from_content as core_allocate_and_claim_workspace_from_content,
    allocate_launch_timestamp_batch as core_allocate_launch_timestamp_batch,
    list_workspace_claims_from_content as core_list_workspace_claims_from_content,
    plan_agent_launch_fanout as core_plan_agent_launch_fanout,
    plan_claim_workspace_from_content as core_plan_claim_workspace_from_content,
    plan_transfer_workspace_claim_from_content as core_plan_transfer_workspace_claim_from_content,
    prepare_agent_launch as core_prepare_agent_launch, AgentLaunchPreparedWire,
    AgentLaunchRequestWire, WorkspaceClaimRequestWire,
};
use sase_core::agent_name_template::{
    agent_name_template_key as core_agent_name_template_key,
    agent_name_template_namespace_template as core_agent_name_template_namespace_template,
    agent_name_template_tokens_after as core_agent_name_template_tokens_after,
    compare_agent_name_template_tokens as core_compare_agent_name_template_tokens,
    is_agent_name_template as core_is_agent_name_template,
    iter_agent_name_key_markers as core_iter_agent_name_key_markers,
    match_agent_name_template as core_match_agent_name_template,
    parse_agent_name_template as core_parse_agent_name_template,
    render_agent_name_template as core_render_agent_name_template,
    AgentNameTemplateKey,
};
use sase_core::agent_runtime::{
    aggregate_clan_runtime as core_aggregate_clan_runtime,
    ClanRuntimeMemberWire,
};
use sase_core::agent_scan::{
    agent_artifact_index_status as core_agent_artifact_index_status,
    canonical_agent_artifact_path as core_canonical_agent_artifact_path,
    collect_workflow_artifact_candidates as core_collect_workflow_artifact_candidates,
    delete_agent_artifact_index_row as core_delete_agent_artifact_index_row,
    delete_agent_artifact_index_row_with_busy_timeout as core_delete_agent_artifact_index_row_with_busy_timeout,
    parse_agent_artifact_path as core_parse_agent_artifact_path,
    query_agent_artifact_index as core_query_agent_artifact_index,
    query_related_agent_artifact_dirs as core_query_related_agent_artifact_dirs,
    read_agent_artifact_index_meta as core_read_agent_artifact_index_meta,
    rebuild_agent_artifact_index as core_rebuild_agent_artifact_index,
    replace_agent_artifact_index_dismissed_agents as core_replace_agent_artifact_index_dismissed_agents,
    resolve_agent_artifact_path as core_resolve_agent_artifact_path,
    resolve_agent_artifact_timestamp_path as core_resolve_agent_artifact_timestamp_path,
    scan_agent_artifact_dirs as core_scan_agent_artifact_dirs,
    scan_agent_artifacts as core_scan_agent_artifacts,
    terminalize_stale_active_agent_artifact_index_rows as core_terminalize_stale_active_agent_artifact_index_rows,
    upsert_agent_artifact_index_row as core_upsert_agent_artifact_index_row,
    write_agent_artifact_index_meta as core_write_agent_artifact_index_meta,
    AgentArtifactIndexQueryWire, AgentArtifactScanOptionsWire,
};
use sase_core::agent_stats::{
    query_activity_stats as core_query_activity_stats,
    query_run_stats as core_query_run_stats, AgentActivityStatsRequestWire,
    AgentRunStatsRequestWire,
};
use sase_core::artifact_consumption::{
    read_artifact_consumption_log as core_read_artifact_consumption_log,
    summarize_artifact_consumption as core_summarize_artifact_consumption,
    ARTIFACT_CONSUMPTION_WIRE_SCHEMA_VERSION,
};
use sase_core::artifact_file::{
    artifact_file_store_economics as core_artifact_file_store_economics,
    list_artifact_file_trash as core_list_artifact_file_trash,
    materialize_vcs_artifact_file as core_materialize_vcs_artifact_file,
    plan_artifact_file_retention as core_plan_artifact_file_retention,
    purge_artifact_file_trash as core_purge_artifact_file_trash,
    query_artifact_files as core_query_artifact_files,
    restore_artifact_file_trash as core_restore_artifact_file_trash,
    trash_artifact_file as core_trash_artifact_file,
    ArtifactFileEconomicsOptionsWire, ArtifactFileQueryError,
    ArtifactFileQueryFiltersWire, ArtifactFileRetentionPolicyWire,
    ArtifactFileTrashPurgeRequestWire, ArtifactFileTrashRequestWire,
    ArtifactFileTrashRestoreRequestWire,
    ArtifactFileVcsMaterializationRequestWire,
    ARTIFACT_FILE_LIFECYCLE_WIRE_SCHEMA_VERSION,
    ARTIFACT_FILE_QUERY_WIRE_SCHEMA_VERSION,
};
use sase_core::artifact_ref::{
    canonicalize_artifact_ref as core_canonicalize_artifact_ref,
    filter_artifact_ref_path_payloads as core_filter_artifact_ref_path_payloads,
    normalize_artifact_ref_list as core_normalize_artifact_ref_list,
    parse_artifact_ref as core_parse_artifact_ref,
    parse_artifact_ref_list as core_parse_artifact_ref_list,
    render_artifact_ref as core_render_artifact_ref,
    resolve_artifact_ref as core_resolve_artifact_ref,
    resolve_artifact_ref_list as core_resolve_artifact_ref_list,
    scan_artifact_refs as core_scan_artifact_refs, ArtifactRefContextWire,
    ArtifactRefError, ParsedArtifactRefWire,
    ARTIFACT_REF_CONTEXT_WIRE_SCHEMA_VERSION,
    ARTIFACT_REF_LIST_RESOLUTION_WIRE_SCHEMA_VERSION,
    ARTIFACT_REF_PARSE_WIRE_SCHEMA_VERSION,
    ARTIFACT_REF_PATH_FILTER_WIRE_SCHEMA_VERSION,
    ARTIFACT_REF_RESOLUTION_WIRE_SCHEMA_VERSION,
};
use sase_core::axe_chop::{
    apply_checkpoint_update as core_apply_checkpoint_update,
    check_and_record_once_per as core_check_and_record_once_per,
    derive_chop_agent_name as core_derive_chop_agent_name,
    evaluate_chop_decision as core_evaluate_chop_decision,
    expand_chop_targets as core_expand_chop_targets,
    parse_chop_duration as core_parse_chop_duration,
    parse_chop_result as core_parse_chop_result,
    release_chop_once_per as core_release_chop_once_per,
    split_axe_description as core_split_axe_description,
    validate_axe_config as core_validate_axe_config,
    validate_chop_proposal as core_validate_chop_proposal,
    validate_chop_result as core_validate_chop_result,
    AxeConfigValidationRequestWire, ChopCheckpointUpdateRequestWire,
    ChopDecisionRequestWire, ChopEngineError, ChopLaunchProposalWire,
    ChopOncePerReleaseRequestWire, ChopOncePerRequestWire,
    ChopResultDocumentWire, ChopTargetExpansionRequestWire,
    CHOP_ENGINE_SCHEMA_VERSION, CHOP_RESULT_SCHEMA_VERSION,
    CHOP_STATE_SCHEMA_VERSION,
};
use sase_core::axe_status::{
    classify_axe_status as core_classify_axe_status, AxeStatusError,
    AxeStatusRequestWire, AXE_STATUS_SCHEMA_VERSION,
};
#[cfg(test)]
use sase_core::bead::PhaseSizeWire;
use sase_core::bead::{
    add_dependency as core_bead_add_dependency,
    add_task_plus_one as core_bead_add_task_plus_one,
    append_issue_note as core_bead_append_issue_note,
    bead_history as core_bead_history, bead_lost_notes as core_bead_lost_notes,
    blocked_issues as core_bead_blocked_issues,
    build_epic_work_plan as core_bead_build_epic_work_plan,
    build_epic_work_plan_from_issues as core_bead_build_epic_work_plan_from_issues,
    cancel_task_snooze as core_bead_cancel_task_snooze,
    claim_for_agent_launch as core_bead_claim_for_agent_launch,
    claim_for_agent_wait as core_bead_claim_for_agent_wait,
    close_issues_with_note as core_bead_close_issues_with_note,
    create_issue as core_bead_create_issue, doctor as core_bead_doctor,
    doctor_report as core_bead_doctor_report,
    doctor_report_with_contexts as core_bead_doctor_report_with_contexts,
    doctor_with_contexts as core_bead_doctor_with_contexts,
    execute_bead_cli as core_execute_bead_cli,
    export_jsonl as core_bead_export_jsonl,
    get_epic_children as core_bead_get_epic_children,
    init_store as core_bead_init_store, list_issues as core_bead_list_issues,
    mark_ready_to_work as core_bead_mark_ready_to_work,
    merge_bead_event_streams as core_merge_bead_event_streams,
    merge_bead_event_streams_with_relocation as core_merge_bead_event_streams_with_relocation,
    needs_plus_one_evidence_migration as core_bead_needs_plus_one_evidence_migration,
    needs_resolution_migration as core_bead_needs_resolution_migration,
    needs_size_check_relax_migration as core_bead_needs_size_check_relax_migration,
    needs_snoozed_status_migration as core_bead_needs_snoozed_status_migration,
    needs_task_ready_migration as core_bead_needs_task_ready_migration,
    open_issue as core_bead_open_issue,
    plus_one_evidence_migration_sql as core_bead_plus_one_evidence_migration_sql,
    preclaim_epic_work_plan as core_bead_preclaim_epic_work_plan,
    read_event_store_issues as core_bead_read_event_store_issues,
    read_legacy_jsonl_issues as core_bead_read_legacy_jsonl_issues,
    read_store_issues as core_bead_read_store_issues,
    ready_issues as core_bead_ready_issues,
    reduce_event_streams as core_reduce_event_streams,
    release_agent_claim as core_bead_release_agent_claim,
    remove_dependencies as core_bead_remove_dependencies,
    remove_issue as core_bead_remove_issue,
    remove_issues as core_bead_remove_issues,
    repair_event_store_manifest as core_repair_event_store_manifest,
    resolution_migration_sql as core_bead_resolution_migration_sql,
    resolve_issue_id as core_bead_resolve_issue_id,
    search_issues as core_bead_search_issues,
    show_issue as core_bead_show_issue,
    show_issue_detail as core_bead_show_issue_detail,
    size_check_relax_migration_sql as core_bead_size_check_relax_migration_sql,
    snooze_task as core_bead_snooze_task,
    snoozed_status_migration_sql as core_bead_snoozed_status_migration_sql,
    stats as core_bead_stats, sync_is_clean as core_bead_sync_is_clean,
    task_ready_migration_sql as core_bead_task_ready_migration_sql,
    unmark_ready_to_work as core_bead_unmark_ready_to_work,
    update_issue as core_bead_update_issue,
    update_issues as core_bead_update_issues, BeadCreateRequestWire, BeadError,
    BeadEventStoreManifestWire, BeadEventStreamWire,
    BeadPreclaimAssignmentWire, BeadResolutionWire, BeadUpdateFieldsWire,
    IssueWire,
};
use sase_core::commit_footer::{
    parse_commit_footer as core_parse_commit_footer,
    update_commit_footer as core_update_commit_footer, CommitFooterUpdateWire,
    COMMIT_FOOTER_WIRE_SCHEMA_VERSION,
};
use sase_core::commit_sha::commit_shas_equivalent as core_commit_shas_equivalent;
use sase_core::commit_subject::{
    default_commit_subject_types as core_default_commit_subject_types,
    parse_commit_subject as core_parse_commit_subject,
    COMMIT_SUBJECT_WIRE_SCHEMA_VERSION,
};
use sase_core::compose_snippet_catalog as core_compose_snippet_catalog;
use sase_core::config::{
    compose_axe_config as core_compose_axe_config,
    config_field_model as core_config_field_model,
    config_inventory as core_config_inventory,
    config_plan_edit as core_config_plan_edit,
    config_validate as core_config_validate,
    plan_axe_entry_mutation as core_plan_axe_entry_mutation,
    AxeConfigComposeRequestWire, AxeEntryMutationRequestWire,
    ConfigEditRequestWire, ConfigError as ConfigDomainError,
    ConfigInventoryRequestWire, ConfigValidateRequestWire,
};
use sase_core::content_layout::{
    memory_note_issue as core_memory_note_issue,
    memory_reference_name as core_memory_reference_name,
    memory_reference_stem as core_memory_reference_stem,
    reserved_memory_namespace_issue as core_reserved_memory_namespace_issue,
    resolve_layout_candidates as core_resolve_layout_candidates,
    sase_content_layout as core_sase_content_layout,
    skill_placement_issue as core_skill_placement_issue,
    skill_reference_name as core_skill_reference_name,
    LayoutCollisionPolicyWire,
};
use sase_core::effort::resolve_effective_effort as core_resolve_effective_effort;
use sase_core::effort_override::{
    clear_effort_override as core_clear_effort_override,
    get_effort_override as core_get_effort_override,
    set_effort_override_relative as core_set_effort_override_relative,
    set_effort_override_until as core_set_effort_override_until,
    EffortOverrideError as EffortOverrideDomainError,
};
use sase_core::git_query::{
    derive_git_workspace_name as core_derive_git_workspace_name,
    parse_git_branch_name as core_parse_git_branch_name,
    parse_git_conflicted_files as core_parse_git_conflicted_files,
    parse_git_local_changes as core_parse_git_local_changes,
    parse_git_name_status_z as core_parse_git_name_status_z,
};
use sase_core::glossary::{
    build_glossary_catalog as core_build_glossary_catalog,
    compile_glossary_catalog as core_compile_glossary_catalog,
    validate_glossary_entries as core_validate_glossary_entries,
    CompiledGlossaryCatalog as CoreCompiledGlossaryCatalog, GlossaryError,
    GlossaryInputEntryWire,
};
use sase_core::inline_code_ranges as core_inline_code_ranges;
use sase_core::machine_hood::{
    machine_hood_of as core_machine_hood_of,
    qualify_machine_agent_name as core_qualify_machine_agent_name,
    strip_machine_agent_name as core_strip_machine_agent_name,
    validate_machine_name as core_validate_machine_name,
};
use sase_core::notifications::{
    append_notification as core_append_notification,
    append_notification_counts as core_append_notification_counts,
    apply_notification_state_update as core_apply_notification_state_update,
    apply_notification_state_update_counts as core_apply_notification_state_update_counts,
    classify_notification_tabs as core_classify_notification_tabs,
    read_current_notifications_snapshot as core_read_current_notifications_snapshot,
    read_notifications_snapshot_with_options as core_read_notifications_snapshot_with_options,
    rewrite_notifications as core_rewrite_notifications,
    rewrite_notifications_counts as core_rewrite_notifications_counts,
    NotificationStateUpdateWire, NotificationWire,
};
use sase_core::plan::{
    canonicalize_plan_reference as core_canonicalize_plan_reference,
    parse_plan_reference as core_parse_plan_reference,
    parse_sdd_artifact_link as core_parse_sdd_artifact_link,
    parse_sdd_plan_header_block as core_parse_sdd_plan_header_block,
    plan_frontmatter_schema as core_plan_frontmatter_schema,
    plan_validate_with_mode as core_plan_validate_with_mode,
    remove_sdd_plan_header_section as core_remove_sdd_plan_header_section,
    render_plan_reference as core_render_plan_reference,
    render_sdd_artifact_link as core_render_sdd_artifact_link,
    render_sdd_plan_header_block as core_render_sdd_plan_header_block,
    replace_sdd_plan_header_block as core_replace_sdd_plan_header_block,
    resolve_plan_reference as core_resolve_plan_reference,
    search_plans as core_plan_search,
    upsert_sdd_artifact_link as core_upsert_sdd_artifact_link,
    upsert_sdd_plan_header_section as core_upsert_sdd_plan_header_section,
    PlanError, SddPlanHeaderSectionWire, PLAN_HEADER_BLOCK_WIRE_SCHEMA_VERSION,
    PLAN_REFERENCE_RESOLUTION_WIRE_SCHEMA_VERSION,
};
use sase_core::project_spec::{
    apply_project_aliases_update as core_apply_project_aliases_update,
    apply_project_lifecycle_update as core_apply_project_lifecycle_update,
    apply_project_name_update as core_apply_project_name_update,
    list_project_records as core_list_project_records,
    read_project_lifecycle_from_content as core_read_project_lifecycle_from_content,
};
use sase_core::prompt_artifact::{
    artifact_pool_filename as core_artifact_pool_filename,
    parse_prompt_artifact_manifest as core_parse_prompt_artifact_manifest,
    render_prompt_artifact_record as core_render_prompt_artifact_record,
    rewrite_prompt_artifact_links as core_rewrite_prompt_artifact_links,
    select_manifest_records as core_select_prompt_artifact_manifest_records,
    PromptArtifactRecord, PROMPT_ARTIFACT_MANIFEST_SCHEMA_VERSION,
};
use sase_core::prompt_stash::{
    append_prompt_stash as core_append_prompt_stash,
    pop_prompt_stash as core_pop_prompt_stash,
    read_prompt_stash_snapshot as core_read_prompt_stash_snapshot,
    rewrite_prompt_stash as core_rewrite_prompt_stash,
    set_prompt_stash_pinned as core_set_prompt_stash_pinned,
    PromptStashEntryWire, PromptStashStoreError,
};
use sase_core::query::types::{QueryErrorWire, QueryExprWire};
use sase_core::query::{
    QueryCorpus as CoreQueryCorpus, QueryProgram as CoreQueryProgram,
};
use sase_core::runner_limit_override::{
    clear_runner_limit_override as core_clear_runner_limit_override,
    get_runner_limit_override as core_get_runner_limit_override,
    set_runner_limit_override_relative as core_set_runner_limit_override_relative,
    set_runner_limit_override_until as core_set_runner_limit_override_until,
    RunnerLimitOverrideError as RunnerLimitOverrideDomainError,
};
use sase_core::status::{
    apply_status_update as core_apply_status_update,
    is_valid_transition as core_is_valid_transition,
    plan_status_transition as core_plan_status_transition,
    read_status_from_lines as core_read_status_from_lines,
    remove_workspace_suffix as core_remove_workspace_suffix,
    StatusTransitionRequestWire,
};
use sase_core::tasks::{
    append_task as core_append_task, prune_tasks as core_prune_tasks,
    read_tasks_snapshot as core_read_tasks_snapshot,
    update_task as core_update_task, BackgroundTaskWire, TaskStoreError,
    TaskUpdateWire,
};
use sase_core::telemetry::{
    cleanup_matching_labels as core_telemetry_cleanup_matching_labels,
    prune as core_telemetry_prune,
    query_instant as core_telemetry_query_instant,
    query_range as core_telemetry_query_range,
    record_batch as core_telemetry_record_batch,
    store_stats as core_telemetry_store_stats, TelemetryCleanupRequestWire,
    TelemetryInstantQueryWire, TelemetryPruneRequestWire,
    TelemetryRangeQueryWire, TelemetryRecordBatchWire,
};
use sase_core::vcs_log::{
    aggregate_commit_log as core_aggregate_commit_log,
    classify_commit_presence as core_classify_commit_presence,
    parse_git_log as core_parse_git_log,
    parse_merge_summary as core_parse_merge_summary, CommitPresenceWire,
    VcsCommitWire, VCS_LOG_WIRE_SCHEMA_VERSION,
};
use sase_core::wire::ChangeSpecWire;
use sase_core::wire::{CommentWire, HookWire, MentorWire};
use serde_json::{Map as JsonMap, Value as JsonValue};

#[pyclass(name = "QueryCorpusHandle", module = "sase_core_rs")]
#[derive(Debug)]
struct PyQueryCorpusHandle {
    corpus: CoreQueryCorpus,
}

#[pymethods]
impl PyQueryCorpusHandle {
    fn __len__(&self) -> usize {
        self.corpus.len()
    }
}

#[pyclass(name = "QueryProgramHandle", module = "sase_core_rs")]
#[derive(Debug)]
struct PyQueryProgramHandle {
    program: CoreQueryProgram,
}

/// Immutable native payload rows and fuzzy-match metadata.
#[pyclass(name = "AtReferenceInventory", module = "sase_core_rs", frozen)]
#[derive(Clone, Debug)]
struct PyAtReferenceInventory {
    payloads: sase_core::AtReferencePayloadIndex,
}

/// Immutable compiled glossary matcher catalog.
#[pyclass(name = "GlossaryCatalogHandle", module = "sase_core_rs", frozen)]
#[derive(Clone, Debug)]
struct PyGlossaryCatalogHandle {
    catalog: CoreCompiledGlossaryCatalog,
}

#[pymethods]
impl PyAtReferenceInventory {
    #[new]
    #[pyo3(signature = (*, payloads))]
    fn new(payloads: &Bound<'_, PyList>) -> PyResult<Self> {
        let payloads = serde_json::from_value::<
            Vec<sase_core::AtReferencePayloadRowWire>,
        >(py_to_json_value(payloads.as_any())?)
        .map_err(|error| {
            PyValueError::new_err(format!(
                "payloads are not valid AtReferencePayloadRowWire dicts: \
                     {error}"
            ))
        })?;
        Ok(Self {
            payloads: sase_core::AtReferencePayloadIndex::new(payloads),
        })
    }

    fn __len__(&self) -> usize {
        self.payloads.len()
    }
}

#[pymethods]
impl PyGlossaryCatalogHandle {
    fn __len__(&self) -> usize {
        self.catalog.len()
    }

    fn catalog(&self, py: Python<'_>) -> PyResult<PyObject> {
        glossary_to_py(py, self.catalog.catalog())
    }

    fn scan(&self, py: Python<'_>, text: &str) -> PyResult<PyObject> {
        glossary_to_py(py, &self.catalog.scan(text))
    }

    fn lookup(
        &self,
        py: Python<'_>,
        text: &str,
        line: u32,
        character: u32,
    ) -> PyResult<PyObject> {
        let span = self
            .catalog
            .lookup(text, sase_core::EditorPosition { line, character });
        glossary_to_py(py, &span)
    }
}

#[pyfunction]
#[pyo3(name = "is_agent_name_template")]
fn py_is_agent_name_template(value: &str) -> bool {
    core_is_agent_name_template(value)
}

#[pyfunction]
#[pyo3(name = "parse_agent_name_template")]
fn py_parse_agent_name_template<'py>(
    py: Python<'py>,
    template: &str,
) -> PyResult<Bound<'py, PyDict>> {
    let parsed = core_parse_agent_name_template(template)
        .map_err(|err| PyValueError::new_err(format!("{err}")))?;
    let dict = PyDict::new_bound(py);
    dict.set_item("template", parsed.template)?;
    dict.set_item("prefix", parsed.prefix)?;
    dict.set_item("suffix", parsed.suffix)?;
    dict.set_item("marker", parsed.marker)?;
    match parsed.key {
        Some(key) => {
            dict.set_item("key", agent_name_template_key_to_py(py, &key)?)?
        }
        None => dict.set_item("key", py.None())?,
    }
    Ok(dict)
}

fn agent_name_template_key_to_py<'py>(
    py: Python<'py>,
    key: &AgentNameTemplateKey,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new_bound(py);
    dict.set_item("id", &key.id)?;
    dict.set_item("qualified", key.qualified)?;
    Ok(dict)
}

#[pyfunction]
#[pyo3(name = "agent_name_template_key")]
fn py_agent_name_template_key<'py>(
    py: Python<'py>,
    template: &str,
) -> PyResult<Option<Bound<'py, PyDict>>> {
    core_agent_name_template_key(template)
        .map_err(|err| PyValueError::new_err(format!("{err}")))?
        .as_ref()
        .map(|key| agent_name_template_key_to_py(py, key))
        .transpose()
}

#[pyfunction]
#[pyo3(name = "iter_agent_name_key_markers")]
fn py_iter_agent_name_key_markers<'py>(
    py: Python<'py>,
    text: &str,
) -> PyResult<Bound<'py, PyList>> {
    let list = PyList::empty_bound(py);
    for marker in core_iter_agent_name_key_markers(text) {
        let dict = PyDict::new_bound(py);
        dict.set_item("start", marker.start)?;
        dict.set_item("end", marker.end)?;
        dict.set_item("id", marker.id.as_deref())?;
        dict.set_item("qualified", marker.qualified)?;
        dict.set_item("braced", marker.braced)?;
        list.append(dict)?;
    }
    Ok(list)
}

#[pyfunction]
#[pyo3(name = "render_agent_name_template")]
fn py_render_agent_name_template(
    template: &str,
    token: &str,
) -> PyResult<String> {
    core_render_agent_name_template(template, token)
        .map_err(|err| PyValueError::new_err(format!("{err}")))
}

#[pyfunction]
#[pyo3(name = "agent_name_template_namespace_template")]
fn py_agent_name_template_namespace_template(
    template: &str,
) -> PyResult<String> {
    core_agent_name_template_namespace_template(template)
        .map_err(|err| PyValueError::new_err(format!("{err}")))
}

#[pyfunction]
#[pyo3(name = "match_agent_name_template")]
fn py_match_agent_name_template(
    template: &str,
    concrete: &str,
) -> PyResult<Option<String>> {
    core_match_agent_name_template(template, concrete)
        .map_err(|err| PyValueError::new_err(format!("{err}")))
}

#[pyfunction]
#[pyo3(name = "compare_agent_name_template_tokens")]
fn py_compare_agent_name_template_tokens(
    left: &str,
    right: &str,
) -> PyResult<i8> {
    let ordering = core_compare_agent_name_template_tokens(left, right)
        .map_err(|err| PyValueError::new_err(format!("{err}")))?;
    Ok(match ordering {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    })
}

#[pyfunction]
#[pyo3(name = "agent_name_template_tokens_after", signature = (after=None, count=1))]
fn py_agent_name_template_tokens_after(
    after: Option<&str>,
    count: usize,
) -> PyResult<Vec<String>> {
    core_agent_name_template_tokens_after(after, count)
        .map_err(|err| PyValueError::new_err(format!("{err}")))
}

/// Validate a machine name (non-empty, `^[a-z_]+$`); raise `ValueError`
/// otherwise.
#[pyfunction]
#[pyo3(name = "validate_machine_name")]
fn py_validate_machine_name(name: &str) -> PyResult<()> {
    core_validate_machine_name(name)
        .map_err(|err| PyValueError::new_err(format!("{err}")))
}

/// Prepend `<machine_name>.` to an agent name unless already qualified.
#[pyfunction]
#[pyo3(name = "qualify_machine_agent_name")]
fn py_qualify_machine_agent_name(name: &str, machine_name: &str) -> String {
    core_qualify_machine_agent_name(name, machine_name)
}

/// Strip a leading `<machine_name>.` from an agent name when present.
#[pyfunction]
#[pyo3(name = "strip_machine_agent_name")]
fn py_strip_machine_agent_name(name: &str, machine_name: &str) -> String {
    core_strip_machine_agent_name(name, machine_name)
}

/// Return the leading hood segment of `name` when it names a known machine.
#[pyfunction]
#[pyo3(name = "machine_hood_of")]
fn py_machine_hood_of(
    name: &str,
    known_machines: Vec<String>,
) -> Option<String> {
    core_machine_hood_of(name, &known_machines)
}

// The machine-hood bindings above are migration shims. New code should use
// the explicit owner-aware domain below.

fn explicit_owner(
    username: &str,
    machine_name: &str,
) -> PyResult<AgentOwnerIdentity> {
    AgentOwnerIdentity::new(username, machine_name)
        .map_err(|error| PyValueError::new_err(error.to_string()))
}

fn source_owner(
    source_username: Option<&str>,
    source_machine_name: &str,
) -> PyResult<AgentSourceOwnerIdentity> {
    let source = match source_username {
        Some(username) => AgentSourceOwnerIdentity::V2 {
            owner: explicit_owner(username, source_machine_name)?,
        },
        None => AgentSourceOwnerIdentity::UsernameUnknownV1 {
            machine_name: source_machine_name.to_string(),
        },
    };
    source
        .validate()
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    Ok(source)
}

fn identity_wire_to_py<'py, T: serde::Serialize>(
    py: Python<'py>,
    value: &T,
) -> PyResult<PyObject> {
    let value = serde_json::to_value(value).map_err(|error| {
        PyValueError::new_err(format!(
            "internal agent identity serialize error: {error}"
        ))
    })?;
    json_value_to_py(py, &value)
}

#[pyfunction]
#[pyo3(name = "validate_agent_username")]
fn py_validate_agent_username(username: &str) -> PyResult<()> {
    core_validate_agent_username(username)
        .map_err(|error| PyValueError::new_err(error.to_string()))
}

#[pyfunction]
#[pyo3(name = "validate_agent_name")]
fn py_validate_agent_name(name: &str) -> PyResult<()> {
    core_validate_agent_name(name)
        .map_err(|error| PyValueError::new_err(error.to_string()))
}

#[pyfunction]
#[pyo3(name = "validate_agent_owner")]
fn py_validate_agent_owner(username: &str, machine_name: &str) -> PyResult<()> {
    explicit_owner(username, machine_name).map(|_| ())
}

#[pyfunction]
#[pyo3(
    name = "classify_agent_ownership",
    signature = (
        source_machine_name,
        target_username,
        target_machine_name,
        source_username = None
    )
)]
fn py_classify_agent_ownership(
    source_machine_name: &str,
    target_username: &str,
    target_machine_name: &str,
    source_username: Option<&str>,
) -> PyResult<String> {
    let source = source_owner(source_username, source_machine_name)?;
    let target = explicit_owner(target_username, target_machine_name)?;
    core_classify_agent_ownership(&source, &target)
        .map(|classification| classification.as_str().to_string())
        .map_err(|error| PyValueError::new_err(error.to_string()))
}

#[pyfunction]
#[pyo3(name = "classify_legacy_v1_group_ownership")]
fn py_classify_legacy_v1_group_ownership(
    group_machine_name: &str,
    target_username: &str,
    target_machine_name: &str,
    v2_hood_published: bool,
    proven_entry_count: usize,
    total_entry_count: usize,
) -> PyResult<String> {
    let target = explicit_owner(target_username, target_machine_name)?;
    let evidence = LegacyV1GroupOwnershipEvidence {
        v2_hood_published,
        proven_entry_count,
        total_entry_count,
    };
    core_classify_legacy_v1_group_ownership(
        group_machine_name,
        &target,
        &evidence,
    )
    .map(|classification| classification.as_str().to_string())
    .map_err(|error| PyValueError::new_err(error.to_string()))
}

#[pyfunction]
#[pyo3(name = "commit_shas_equivalent")]
fn py_commit_shas_equivalent(left: &str, right: &str) -> bool {
    core_commit_shas_equivalent(left, right)
}

#[pyfunction]
#[pyo3(name = "normalize_agent_archive_name")]
fn py_normalize_agent_archive_name(name: &str) -> PyResult<String> {
    core_normalize_agent_archive_name(name)
        .map_err(|error| PyValueError::new_err(error.to_string()))
}

#[pyfunction]
#[pyo3(name = "globalize_agent_name")]
fn py_globalize_agent_name(
    local_name: &str,
    username: &str,
    machine_name: &str,
) -> PyResult<String> {
    core_globalize_agent_name(
        local_name,
        &explicit_owner(username, machine_name)?,
    )
    .map_err(|error| PyValueError::new_err(error.to_string()))
}

#[pyfunction]
#[pyo3(name = "globalize_legacy_agent_name")]
fn py_globalize_legacy_agent_name(
    legacy_name: &str,
    username: &str,
    machine_name: &str,
) -> PyResult<String> {
    core_globalize_legacy_agent_name(
        legacy_name,
        &explicit_owner(username, machine_name)?,
    )
    .map_err(|error| PyValueError::new_err(error.to_string()))
}

#[pyfunction]
#[pyo3(name = "strip_global_agent_name")]
fn py_strip_global_agent_name(
    global_name: &str,
    username: &str,
    machine_name: &str,
) -> PyResult<String> {
    core_strip_global_agent_name(
        global_name,
        &explicit_owner(username, machine_name)?,
    )
    .map_err(|error| PyValueError::new_err(error.to_string()))
}

#[pyfunction]
#[pyo3(
    name = "localize_agent_name",
    signature = (
        global_name,
        source_machine_name,
        target_username,
        target_machine_name,
        source_username = None
    )
)]
fn py_localize_agent_name(
    global_name: &str,
    source_machine_name: &str,
    target_username: &str,
    target_machine_name: &str,
    source_username: Option<&str>,
) -> PyResult<String> {
    core_localize_agent_name(
        global_name,
        &source_owner(source_username, source_machine_name)?,
        &explicit_owner(target_username, target_machine_name)?,
    )
    .map_err(|error| PyValueError::new_err(error.to_string()))
}

#[pyfunction]
#[pyo3(name = "parse_agent_family_name")]
fn py_parse_agent_family_name(
    py: Python<'_>,
    name: &str,
) -> PyResult<PyObject> {
    let parsed = core_parse_agent_family_name(name)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    identity_wire_to_py(py, &parsed)
}

#[pyfunction]
#[pyo3(name = "agent_local_hood")]
fn py_agent_local_hood(name: &str) -> PyResult<String> {
    core_agent_local_hood(name)
        .map_err(|error| PyValueError::new_err(error.to_string()))
}

#[pyfunction]
#[pyo3(name = "agent_name_in_hood")]
fn py_agent_name_in_hood(name: &str, hood: &str) -> PyResult<bool> {
    core_agent_name_in_hood(name, hood)
        .map_err(|error| PyValueError::new_err(error.to_string()))
}

#[pyfunction]
#[pyo3(name = "agent_name_ancestors")]
fn py_agent_name_ancestors(name: &str) -> PyResult<Vec<String>> {
    core_agent_name_ancestors(name)
        .map_err(|error| PyValueError::new_err(error.to_string()))
}

#[pyfunction]
#[pyo3(name = "agent_link_target")]
fn py_agent_link_target(
    py: Python<'_>,
    name: &str,
    username: &str,
    machine_name: &str,
) -> PyResult<PyObject> {
    let target =
        core_agent_link_target(name, &explicit_owner(username, machine_name)?)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
    identity_wire_to_py(py, &target)
}

#[pyfunction]
#[pyo3(name = "agent_relationship_schema_version")]
fn py_agent_relationship_schema_version() -> u32 {
    AGENT_RELATIONSHIP_SCHEMA_VERSION
}

fn relationship_batch_from_pydict(
    batch: &Bound<'_, PyDict>,
) -> PyResult<AgentRelationshipBatchWire> {
    let value = py_to_json_value(batch.as_any())?;
    serde_json::from_value(value).map_err(|error| {
        PyValueError::new_err(format!(
            "invalid agent relationship batch: {error}"
        ))
    })
}

#[pyfunction]
#[pyo3(name = "validate_agent_relationship_batch")]
fn py_validate_agent_relationship_batch(
    py: Python<'_>,
    batch: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let batch = relationship_batch_from_pydict(batch)?;
    let summary = core_validate_agent_relationship_batch(&batch)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    identity_wire_to_py(py, &summary)
}

#[pyfunction]
#[pyo3(name = "rewrite_agent_relationship_batch")]
fn py_rewrite_agent_relationship_batch(
    py: Python<'_>,
    batch: &Bound<'_, PyDict>,
    destination_ids: &Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let batch = relationship_batch_from_pydict(batch)?;
    let destination_ids = serde_json::from_value::<BTreeMap<String, String>>(
        py_to_json_value(destination_ids.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!("invalid destination ID map: {error}"))
    })?;
    let rewritten =
        core_rewrite_agent_relationship_batch(&batch, &destination_ids)
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
    identity_wire_to_py(py, &rewritten)
}

#[pyfunction]
#[pyo3(name = "compose_snippet_catalog")]
fn py_compose_snippet_catalog<'py>(
    py: Python<'py>,
    templates: BTreeMap<String, String>,
) -> PyResult<Bound<'py, PyDict>> {
    let composed = core_compose_snippet_catalog(&templates);
    let result = PyDict::new_bound(py);
    result.set_item("templates", composed.templates)?;
    result.set_item("alias_provenance", composed.alias_provenance)?;
    Ok(result)
}

/// Parse a project file's bytes into a `list[dict]` mirroring the
/// `ChangeSpecWire` JSON shape.
///
/// Errors raised by the Rust parser become `ValueError` on the Python
/// side. Encoding errors (non-UTF-8 input) are also surfaced as
/// `ValueError` because the Rust parser models them through
/// `ParseErrorWire { kind: "encoding", ... }`.
#[pyfunction]
#[pyo3(name = "parse_project_bytes")]
fn py_parse_project_bytes<'py>(
    py: Python<'py>,
    path: &str,
    data: &Bound<'py, PyBytes>,
) -> PyResult<Bound<'py, PyList>> {
    let bytes: &[u8] = data.as_bytes();
    let specs = sase_core::parse_project_bytes(path, bytes)
        .map_err(|err| PyValueError::new_err(format!("{err}")))?;

    let list = PyList::empty_bound(py);
    for spec in &specs {
        // Going through serde_json::Value keeps the conversion logic in one
        // place and inherits the field declaration order baked into the
        // `ChangeSpecWire` derive. Performance is fine for ChangeSpec-sized
        // documents; if it ever isn't, replace with a direct serde -> Py
        // visitor.
        let value = serde_json::to_value(spec).map_err(|e| {
            PyValueError::new_err(format!("internal serialize error: {e}"))
        })?;
        let py_obj = json_value_to_py(py, &value)?;
        list.append(py_obj)?;
    }
    Ok(list)
}

/// Parse a project file's bytes into canonical PatchWire-shape dicts.
///
/// This accepts both canonical `## Patch` / `STITCHES:` and legacy
/// `## ChangeSpec` / `COMMITS:` text, then emits `stitches` and `stitch_id`
/// keys for new Python callers.
#[pyfunction]
#[pyo3(name = "parse_patch_project_bytes")]
fn py_parse_patch_project_bytes<'py>(
    py: Python<'py>,
    path: &str,
    data: &Bound<'py, PyBytes>,
) -> PyResult<Bound<'py, PyList>> {
    let bytes: &[u8] = data.as_bytes();
    let patches = sase_core::parse_patch_project_bytes(path, bytes)
        .map_err(|err| PyValueError::new_err(format!("{err}")))?;

    let list = PyList::empty_bound(py);
    for patch in &patches {
        let value = serde_json::to_value(patch).map_err(|e| {
            PyValueError::new_err(format!("internal serialize error: {e}"))
        })?;
        let py_obj = json_value_to_py(py, &value)?;
        list.append(py_obj)?;
    }
    Ok(list)
}

/// Tokenize a query string. Returns Python `QueryTokenWire`-shape dicts.
///
/// The Rust `QueryTokenWire` already serializes to the exact field set the
/// Python wire dataclass expects (`kind`, `value`, `position`,
/// `case_sensitive`, `property_key`), so the conversion is straight serde.
#[pyfunction]
#[pyo3(name = "tokenize_query")]
fn py_tokenize_query<'py>(
    py: Python<'py>,
    query: &str,
) -> PyResult<Bound<'py, PyList>> {
    let tokens =
        sase_core::tokenize_query(query).map_err(query_error_to_pyerr)?;
    let list = PyList::empty_bound(py);
    for tok in &tokens {
        let value = serde_json::to_value(tok).map_err(|e| {
            PyValueError::new_err(format!("internal serialize error: {e}"))
        })?;
        list.append(json_value_to_py(py, &value)?)?;
    }
    Ok(list)
}

/// Parse a query string into the Python `QueryExprWire`-shape dict.
#[pyfunction]
#[pyo3(name = "parse_query")]
fn py_parse_query<'py>(py: Python<'py>, query: &str) -> PyResult<PyObject> {
    let expr = sase_core::parse_query(query).map_err(query_error_to_pyerr)?;
    let value = expr_to_python_wire(&expr);
    json_value_to_py(py, &value)
}

/// Canonicalize a query string. Mirrors Python's
/// `to_canonical_string(parse_query(...))`.
#[pyfunction]
#[pyo3(name = "canonicalize_query")]
fn py_canonicalize_query(query: &str) -> PyResult<String> {
    let expr = sase_core::parse_query(query).map_err(query_error_to_pyerr)?;
    Ok(sase_core::canonicalize_query(&expr))
}

/// Return the schema version for commit-footer binding payloads.
#[pyfunction]
#[pyo3(name = "commit_footer_wire_schema_version")]
fn py_commit_footer_wire_schema_version() -> u32 {
    COMMIT_FOOTER_WIRE_SCHEMA_VERSION
}

/// Parse a terminal SASE commit footer into its structured wire payload.
#[pyfunction]
#[pyo3(name = "parse_commit_footer")]
fn py_parse_commit_footer<'py>(
    py: Python<'py>,
    message: &str,
) -> PyResult<PyObject> {
    let footer = core_parse_commit_footer(message);
    let value = serde_json::to_value(&footer).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Update a terminal SASE commit footer from typed update payloads.
#[pyfunction]
#[pyo3(name = "update_commit_footer")]
fn py_update_commit_footer(
    message: &str,
    updates: &Bound<'_, PyList>,
    remove_keys: Vec<String>,
) -> PyResult<String> {
    let mut wire_updates = Vec::with_capacity(updates.len());
    for (index, item) in updates.iter().enumerate() {
        let value = py_to_json_value(&item)?;
        let update: CommitFooterUpdateWire =
            serde_json::from_value(value).map_err(|e| {
                PyValueError::new_err(format!(
                    "updates[{index}] is not a valid CommitFooterUpdateWire dict: {e}"
                ))
            })?;
        wire_updates.push(update);
    }
    Ok(core_update_commit_footer(
        message,
        &wire_updates,
        &remove_keys,
    ))
}

/// Return the schema version for commit-subject binding payloads.
#[pyfunction]
#[pyo3(name = "commit_subject_wire_schema_version")]
fn py_commit_subject_wire_schema_version() -> u32 {
    COMMIT_SUBJECT_WIRE_SCHEMA_VERSION
}

/// Return the default accepted Conventional Commit types.
#[pyfunction]
#[pyo3(name = "default_commit_subject_types")]
fn py_default_commit_subject_types() -> Vec<String> {
    core_default_commit_subject_types()
}

/// Parse a Conventional Commit subject into its structured wire payload.
#[pyfunction]
#[pyo3(name = "parse_commit_subject")]
fn py_parse_commit_subject<'py>(
    py: Python<'py>,
    message: &str,
    allowed_types: Vec<String>,
) -> PyResult<PyObject> {
    let subject = core_parse_commit_subject(message, &allowed_types);
    let value = serde_json::to_value(&subject).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Compile a persistent Patch corpus from Python wire dicts.
///
/// Python dicts are converted to `ChangeSpecWire` before the GIL is released;
/// the reusable corpus indexes and searchable text are then built without
/// holding the GIL.
#[pyfunction]
#[pyo3(name = "compile_corpus")]
fn py_compile_corpus<'py>(
    py: Python<'py>,
    specs: &Bound<'py, PyList>,
) -> PyResult<PyQueryCorpusHandle> {
    let wire_specs = patches_from_py_list(specs)?;
    let corpus = py.allow_threads(|| CoreQueryCorpus::new(wire_specs));
    Ok(PyQueryCorpusHandle { corpus })
}

/// Compile a query into a reusable Rust program handle.
#[pyfunction]
#[pyo3(name = "compile_query")]
fn py_compile_query(query: &str) -> PyResult<PyQueryProgramHandle> {
    let program =
        sase_core::compile_query(query).map_err(query_error_to_pyerr)?;
    Ok(PyQueryProgramHandle { program })
}

/// Evaluate a compiled query against a persistent corpus.
///
/// Evaluation releases the GIL because it only reads the owned Rust handles
/// and returns one boolean per corpus row.
#[pyfunction]
#[pyo3(name = "evaluate_many")]
fn py_evaluate_many<'py>(
    py: Python<'py>,
    program: &PyQueryProgramHandle,
    corpus: &PyQueryCorpusHandle,
) -> PyResult<Bound<'py, PyList>> {
    let results = py.allow_threads(|| {
        sase_core::evaluate_query_many_in_corpus(
            &program.program,
            &corpus.corpus,
        )
    });

    let list = PyList::empty_bound(py);
    for b in results {
        list.append(b)?;
    }
    Ok(list)
}

/// Evaluate a query against a list of `ChangeSpecWire`-shape dicts.
///
/// `specs` must be a `list[dict]` matching the JSON shape of
/// `sase_core::wire::ChangeSpecWire` (i.e. the dicts produced by
/// `sase.core.wire.to_json_dict(changespec_to_wire(cs))` legacy compatibility).
///
/// Compiles the query, builds a per-list `QueryEvaluationContext`, and
/// evaluates the program against every spec. Returns `list[bool]` of the
/// same length as `specs`.
#[pyfunction]
#[pyo3(name = "evaluate_query_many")]
fn py_evaluate_query_many<'py>(
    py: Python<'py>,
    query: &str,
    specs: &Bound<'py, PyList>,
) -> PyResult<Bound<'py, PyList>> {
    let wire_specs = patches_from_py_list(specs)?;
    let program =
        sase_core::compile_query(query).map_err(query_error_to_pyerr)?;
    let results = py.allow_threads(|| {
        sase_core::evaluate_query_many(&program, &wire_specs)
    });

    let list = PyList::empty_bound(py);
    for b in results {
        list.append(b)?;
    }
    Ok(list)
}

/// Walk an agent-artifact tree and return the snapshot dict.
///
/// Mirrors `sase.core.agent_scan_facade.scan_agent_artifacts_python`. The
/// dict shape matches what `agent_scan_wire_to_json_dict` produces on the
/// Python side, so the facade can rehydrate it into the Phase 3A
/// dataclasses without a custom JSON re-encode step.
///
/// `options` is an optional `AgentArtifactScanOptionsWire`-shape dict. Any
/// fields the dict omits fall back to the wire defaults (matching the
/// pure-Python helper's `AgentArtifactScanOptionsWire()` default). The GIL
/// is released for the duration of the filesystem walk.
#[pyfunction]
#[pyo3(name = "scan_agent_artifacts", signature = (projects_root, options = None))]
fn py_scan_agent_artifacts<'py>(
    py: Python<'py>,
    projects_root: &str,
    options: Option<&Bound<'py, PyDict>>,
) -> PyResult<PyObject> {
    let opts = match options {
        Some(dict) => agent_scan_options_from_pydict(dict)?,
        None => AgentArtifactScanOptionsWire::default(),
    };
    let root = PathBuf::from(projects_root);
    let snapshot = py.allow_threads(|| core_scan_agent_artifacts(&root, opts));
    let value = serde_json::to_value(&snapshot).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Scan exact agent artifact directories and return scanner-shaped records.
#[pyfunction]
#[pyo3(
    name = "scan_agent_artifact_dirs",
    signature = (projects_root, artifact_dirs, options = None)
)]
fn py_scan_agent_artifact_dirs<'py>(
    py: Python<'py>,
    projects_root: &str,
    artifact_dirs: Vec<String>,
    options: Option<&Bound<'py, PyDict>>,
) -> PyResult<PyObject> {
    let opts = match options {
        Some(dict) => agent_scan_options_from_pydict(dict)?,
        None => AgentArtifactScanOptionsWire::default(),
    };
    let root = PathBuf::from(projects_root);
    let artifacts = artifact_dirs
        .into_iter()
        .map(PathBuf::from)
        .collect::<Vec<_>>();
    let snapshot = py.allow_threads(|| {
        core_scan_agent_artifact_dirs(&root, &artifacts, opts)
    });
    let value = serde_json::to_value(&snapshot).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Return wall-clock union runtime for clan/family members.
#[pyfunction]
#[pyo3(name = "aggregate_clan_runtime")]
fn py_aggregate_clan_runtime<'py>(
    py: Python<'py>,
    members: &Bound<'py, PyList>,
    now_epoch_seconds: f64,
) -> PyResult<PyObject> {
    let members = clan_runtime_members_from_py_list(members)?;
    let runtime = py.allow_threads(|| {
        core_aggregate_clan_runtime(&members, now_epoch_seconds)
    });
    let value = serde_json::to_value(&runtime).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

/// Build the canonical physical path for one agent artifact timestamp.
#[pyfunction]
#[pyo3(
    name = "canonical_agent_artifact_path",
    signature = (projects_root, project_name, workflow_dir_name, timestamp)
)]
fn py_canonical_agent_artifact_path(
    projects_root: &str,
    project_name: &str,
    workflow_dir_name: &str,
    timestamp: &str,
) -> String {
    core_canonical_agent_artifact_path(
        &PathBuf::from(projects_root),
        project_name,
        workflow_dir_name,
        timestamp,
    )
    .to_string_lossy()
    .into_owned()
}

/// Resolve a legacy or sharded artifact path to the current physical path.
#[pyfunction]
#[pyo3(name = "resolve_agent_artifact_path")]
fn py_resolve_agent_artifact_path(
    projects_root: &str,
    artifact_dir: &str,
) -> String {
    core_resolve_agent_artifact_path(
        &PathBuf::from(projects_root),
        &PathBuf::from(artifact_dir),
    )
    .to_string_lossy()
    .into_owned()
}

/// Resolve a project/workflow/timestamp tuple to the current physical path.
#[pyfunction]
#[pyo3(
    name = "resolve_agent_artifact_timestamp_path",
    signature = (projects_root, project_name, workflow_dir_name, timestamp)
)]
fn py_resolve_agent_artifact_timestamp_path(
    projects_root: &str,
    project_name: &str,
    workflow_dir_name: &str,
    timestamp: &str,
) -> String {
    core_resolve_agent_artifact_timestamp_path(
        &PathBuf::from(projects_root),
        project_name,
        workflow_dir_name,
        timestamp,
    )
    .to_string_lossy()
    .into_owned()
}

/// Parse a legacy or sharded artifact path into layout metadata.
#[pyfunction]
#[pyo3(name = "parse_agent_artifact_path")]
fn py_parse_agent_artifact_path<'py>(
    py: Python<'py>,
    projects_root: &str,
    artifact_dir: &str,
) -> PyResult<Option<PyObject>> {
    let info = core_parse_agent_artifact_path(
        &PathBuf::from(projects_root),
        &PathBuf::from(artifact_dir),
    );
    match info {
        Some(info) => {
            let value = serde_json::to_value(&info).map_err(|e| {
                PyValueError::new_err(format!("internal serialize error: {e}"))
            })?;
            Ok(Some(json_value_to_py(py, &value)?))
        }
        None => Ok(None),
    }
}

/// List artifact directories for one workflow under one project.
#[pyfunction]
#[pyo3(
    name = "iter_agent_artifact_dirs",
    signature = (projects_root, project_name, workflow_dir_name, newest_first = false)
)]
fn py_iter_agent_artifact_dirs(
    projects_root: &str,
    project_name: &str,
    workflow_dir_name: &str,
    newest_first: bool,
) -> Vec<String> {
    let workflow_dir = PathBuf::from(projects_root)
        .join(project_name)
        .join("artifacts")
        .join(workflow_dir_name);
    core_collect_workflow_artifact_candidates(
        &workflow_dir,
        workflow_dir_name,
        newest_first,
    )
    .candidates
    .into_iter()
    .map(|candidate| candidate.artifact_dir.to_string_lossy().into_owned())
    .collect()
}

/// Rebuild the persistent agent artifact index from source artifacts.
#[pyfunction]
#[pyo3(
    name = "rebuild_agent_artifact_index",
    signature = (index_path, projects_root, options = None)
)]
fn py_rebuild_agent_artifact_index<'py>(
    py: Python<'py>,
    index_path: &str,
    projects_root: &str,
    options: Option<&Bound<'py, PyDict>>,
) -> PyResult<PyObject> {
    let opts = match options {
        Some(dict) => agent_scan_options_from_pydict(dict)?,
        None => AgentArtifactScanOptionsWire::default(),
    };
    let index = PathBuf::from(index_path);
    let root = PathBuf::from(projects_root);
    let update = py
        .allow_threads(|| {
            core_rebuild_agent_artifact_index(&index, &root, opts)
        })
        .map_err(PyRuntimeError::new_err)?;
    let value = serde_json::to_value(&update).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Upsert one artifact directory into the persistent artifact index.
#[pyfunction]
#[pyo3(
    name = "upsert_agent_artifact_index_row",
    signature = (index_path, projects_root, artifact_dir, options = None)
)]
fn py_upsert_agent_artifact_index_row<'py>(
    py: Python<'py>,
    index_path: &str,
    projects_root: &str,
    artifact_dir: &str,
    options: Option<&Bound<'py, PyDict>>,
) -> PyResult<PyObject> {
    let opts = match options {
        Some(dict) => agent_scan_options_from_pydict(dict)?,
        None => AgentArtifactScanOptionsWire::default(),
    };
    let index = PathBuf::from(index_path);
    let root = PathBuf::from(projects_root);
    let artifact = PathBuf::from(artifact_dir);
    let update = py
        .allow_threads(|| {
            core_upsert_agent_artifact_index_row(&index, &root, &artifact, opts)
        })
        .map_err(PyRuntimeError::new_err)?;
    let value = serde_json::to_value(&update).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Delete one artifact directory row from the persistent artifact index.
#[pyfunction]
#[pyo3(name = "delete_agent_artifact_index_row")]
fn py_delete_agent_artifact_index_row<'py>(
    py: Python<'py>,
    index_path: &str,
    artifact_dir: &str,
) -> PyResult<PyObject> {
    let index = PathBuf::from(index_path);
    let artifact = PathBuf::from(artifact_dir);
    let update = py
        .allow_threads(|| {
            core_delete_agent_artifact_index_row(&index, &artifact)
        })
        .map_err(PyRuntimeError::new_err)?;
    let value = serde_json::to_value(&update).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Delete one artifact row using a bounded SQLite busy timeout.
#[pyfunction]
#[pyo3(name = "delete_agent_artifact_index_row_bounded")]
fn py_delete_agent_artifact_index_row_bounded<'py>(
    py: Python<'py>,
    index_path: &str,
    artifact_dir: &str,
    busy_timeout_ms: u64,
) -> PyResult<PyObject> {
    let index = PathBuf::from(index_path);
    let artifact = PathBuf::from(artifact_dir);
    let update = py
        .allow_threads(|| {
            core_delete_agent_artifact_index_row_with_busy_timeout(
                &index,
                &artifact,
                Duration::from_millis(busy_timeout_ms),
            )
        })
        .map_err(PyRuntimeError::new_err)?;
    let value = serde_json::to_value(&update).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Terminalize stale, unclaimed active rows in the persistent artifact index.
#[pyfunction]
#[pyo3(
    name = "terminalize_stale_active_agent_artifact_index_rows",
    signature = (
        index_path,
        projects_root,
        stale_after_seconds,
        max_rows = None,
        options = None
    )
)]
fn py_terminalize_stale_active_agent_artifact_index_rows<'py>(
    py: Python<'py>,
    index_path: &str,
    projects_root: &str,
    stale_after_seconds: u64,
    max_rows: Option<u32>,
    options: Option<&Bound<'py, PyDict>>,
) -> PyResult<PyObject> {
    let opts = match options {
        Some(dict) => agent_scan_options_from_pydict(dict)?,
        None => AgentArtifactScanOptionsWire::default(),
    };
    let index = PathBuf::from(index_path);
    let root = PathBuf::from(projects_root);
    let update = py
        .allow_threads(|| {
            core_terminalize_stale_active_agent_artifact_index_rows(
                &index,
                &root,
                opts,
                stale_after_seconds,
                max_rows,
            )
        })
        .map_err(PyRuntimeError::new_err)?;
    let value = serde_json::to_value(&update).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Replace dismissed identities in the persistent artifact index.
#[pyfunction]
#[pyo3(name = "replace_agent_artifact_index_dismissed_agents")]
fn py_replace_agent_artifact_index_dismissed_agents<'py>(
    py: Python<'py>,
    index_path: &str,
    identities: &Bound<'_, PyList>,
) -> PyResult<PyObject> {
    let mut wire_identities: Vec<AgentCleanupIdentityWire> =
        Vec::with_capacity(identities.len());
    for (idx, item) in identities.iter().enumerate() {
        let json = py_to_json_value(&item)?;
        let identity: AgentCleanupIdentityWire =
            serde_json::from_value(json).map_err(|e| {
                PyValueError::new_err(format!(
                    "identities[{idx}] is not a valid AgentCleanupIdentityWire dict: {e}"
                ))
            })?;
        wire_identities.push(identity);
    }
    let index = PathBuf::from(index_path);
    let update = py
        .allow_threads(|| {
            core_replace_agent_artifact_index_dismissed_agents(
                &index,
                &wire_identities,
            )
        })
        .map_err(PyRuntimeError::new_err)?;
    let value = serde_json::to_value(&update).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Read one metadata value from the persistent artifact index.
#[pyfunction]
#[pyo3(name = "read_agent_artifact_index_meta")]
fn py_read_agent_artifact_index_meta<'py>(
    py: Python<'py>,
    index_path: &str,
    key: &str,
) -> PyResult<Option<String>> {
    let index = PathBuf::from(index_path);
    py.allow_threads(|| core_read_agent_artifact_index_meta(&index, key))
        .map_err(PyRuntimeError::new_err)
}

/// Write one metadata value in the persistent artifact index.
#[pyfunction]
#[pyo3(name = "write_agent_artifact_index_meta")]
fn py_write_agent_artifact_index_meta<'py>(
    py: Python<'py>,
    index_path: &str,
    key: &str,
    value: &str,
) -> PyResult<()> {
    let index = PathBuf::from(index_path);
    py.allow_threads(|| {
        core_write_agent_artifact_index_meta(&index, key, value)
    })
    .map_err(PyRuntimeError::new_err)
}

/// Return lightweight row-count status for the persistent artifact index.
#[pyfunction]
#[pyo3(name = "agent_artifact_index_status")]
fn py_agent_artifact_index_status<'py>(
    py: Python<'py>,
    index_path: &str,
) -> PyResult<PyObject> {
    let index = PathBuf::from(index_path);
    let status = py
        .allow_threads(|| core_agent_artifact_index_status(&index))
        .map_err(PyRuntimeError::new_err)?;
    let value = serde_json::to_value(&status).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Query scanner-shaped rows from the persistent artifact index.
#[pyfunction]
#[pyo3(
    name = "query_agent_artifact_index",
    signature = (index_path, projects_root, query = None, options = None)
)]
fn py_query_agent_artifact_index<'py>(
    py: Python<'py>,
    index_path: &str,
    projects_root: &str,
    query: Option<&Bound<'py, PyDict>>,
    options: Option<&Bound<'py, PyDict>>,
) -> PyResult<PyObject> {
    let query_wire = match query {
        Some(dict) => {
            let json = py_to_json_value(dict)?;
            serde_json::from_value::<AgentArtifactIndexQueryWire>(json)
                .map_err(|e| {
                    PyValueError::new_err(format!(
                        "query is not a valid AgentArtifactIndexQueryWire dict: {e}"
                    ))
                })?
        }
        None => AgentArtifactIndexQueryWire::default(),
    };
    let opts = match options {
        Some(dict) => agent_scan_options_from_pydict(dict)?,
        None => AgentArtifactScanOptionsWire::default(),
    };
    let index = PathBuf::from(index_path);
    let root = PathBuf::from(projects_root);
    let snapshot = py
        .allow_threads(|| {
            core_query_agent_artifact_index(&index, &root, query_wire, opts)
        })
        .map_err(PyRuntimeError::new_err)?;
    let value = serde_json::to_value(&snapshot).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

#[pyfunction]
#[pyo3(
    name = "query_related_agent_artifact_dirs",
    signature = (index_path, artifact_dir, seed_timestamps)
)]
fn py_query_related_agent_artifact_dirs(
    py: Python<'_>,
    index_path: &str,
    artifact_dir: &str,
    seed_timestamps: Vec<String>,
) -> PyResult<Vec<String>> {
    let index = PathBuf::from(index_path);
    let artifact = PathBuf::from(artifact_dir);
    py.allow_threads(|| {
        core_query_related_agent_artifact_dirs(
            &index,
            &artifact,
            &seed_timestamps,
        )
    })
    .map_err(PyRuntimeError::new_err)
}

/// Query dismissed-agent archive summary rows from the canonical archive index.
#[pyfunction]
#[pyo3(name = "query_agent_archive")]
fn py_query_agent_archive<'py>(
    py: Python<'py>,
    root: &str,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let request: AgentArchiveQueryRequestWire = serde_json::from_value(value)
        .map_err(|e| {
        PyValueError::new_err(format!(
            "request is not a valid AgentArchiveQueryRequestWire dict: {e}"
        ))
    })?;
    let result = py
        .allow_threads(|| {
            core_query_agent_archive(&PathBuf::from(root), request)
        })
        .map_err(PyValueError::new_err)?;
    let value = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Return grouped counts for a dismissed-agent archive facet.
#[pyfunction]
#[pyo3(name = "agent_archive_facet_counts")]
fn py_agent_archive_facet_counts<'py>(
    py: Python<'py>,
    root: &str,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let request: AgentArchiveFacetRequestWire = serde_json::from_value(value)
        .map_err(|e| {
        PyValueError::new_err(format!(
            "request is not a valid AgentArchiveFacetRequestWire dict: {e}"
        ))
    })?;
    let result = py
        .allow_threads(|| {
            core_agent_archive_facet_counts(&PathBuf::from(root), request)
        })
        .map_err(PyValueError::new_err)?;
    let value = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Mark preserved archive bundles as revived without deleting payloads.
#[pyfunction]
#[pyo3(name = "mark_agent_archive_bundles_revived")]
fn py_mark_agent_archive_bundles_revived<'py>(
    py: Python<'py>,
    root: &str,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let request: AgentArchiveReviveMarkRequestWire =
        serde_json::from_value(value).map_err(|e| {
            PyValueError::new_err(format!(
                "request is not a valid AgentArchiveReviveMarkRequestWire dict: {e}"
            ))
        })?;
    let result = py.allow_threads(|| {
        core_mark_agent_archive_bundles_revived(&PathBuf::from(root), request)
    });
    let value = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Verify the dismissed-agent archive index against bundle payload files.
#[pyfunction]
#[pyo3(name = "verify_agent_archive_index")]
fn py_verify_agent_archive_index<'py>(
    py: Python<'py>,
    root: &str,
) -> PyResult<PyObject> {
    let result = py.allow_threads(|| {
        core_verify_agent_archive_index(&PathBuf::from(root))
    });
    let value = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Save one saved dismissed-agent group metadata record.
#[pyfunction]
#[pyo3(name = "save_dismissed_agent_group")]
fn py_save_dismissed_agent_group<'py>(
    py: Python<'py>,
    root: &str,
    group: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(group.as_any())?;
    let group: SavedAgentGroupWire =
        serde_json::from_value(value).map_err(|e| {
            PyValueError::new_err(format!(
                "group is not a valid SavedAgentGroupWire dict: {e}"
            ))
        })?;
    let result = py
        .allow_threads(|| {
            core_save_dismissed_agent_group(&PathBuf::from(root), group)
        })
        .map_err(PyValueError::new_err)?;
    let value = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// List saved dismissed-agent group summaries in newest-first pages.
#[pyfunction]
#[pyo3(name = "list_dismissed_agent_groups", signature = (root, limit = 20, cursor = None))]
fn py_list_dismissed_agent_groups<'py>(
    py: Python<'py>,
    root: &str,
    limit: i64,
    cursor: Option<i64>,
) -> PyResult<PyObject> {
    let result = py.allow_threads(|| {
        core_list_dismissed_agent_groups(&PathBuf::from(root), limit, cursor)
    });
    let value = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Load one saved dismissed-agent group, returning None when absent/corrupt.
#[pyfunction]
#[pyo3(name = "load_dismissed_agent_group")]
fn py_load_dismissed_agent_group<'py>(
    py: Python<'py>,
    root: &str,
    group_id: &str,
) -> PyResult<PyObject> {
    let result = py
        .allow_threads(|| {
            core_load_dismissed_agent_group(&PathBuf::from(root), group_id)
        })
        .map_err(PyValueError::new_err)?;
    let value = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Mark one saved dismissed-agent group revived without deleting metadata.
#[pyfunction]
#[pyo3(name = "mark_dismissed_agent_group_revived")]
fn py_mark_dismissed_agent_group_revived<'py>(
    py: Python<'py>,
    root: &str,
    group_id: &str,
    revived_at: &str,
) -> PyResult<PyObject> {
    let result = py
        .allow_threads(|| {
            core_mark_dismissed_agent_group_revived(
                &PathBuf::from(root),
                group_id,
                revived_at,
            )
        })
        .map_err(PyValueError::new_err)?;
    let value = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Delete one saved dismissed-agent group metadata record.
#[pyfunction]
#[pyo3(name = "delete_dismissed_agent_group")]
fn py_delete_dismissed_agent_group(
    py: Python<'_>,
    root: &str,
    group_id: &str,
) -> PyResult<bool> {
    py.allow_threads(|| {
        core_delete_dismissed_agent_group(&PathBuf::from(root), group_id)
    })
    .map_err(PyValueError::new_err)
}

/// Record one recent dismissed-agent group and prune the capped recent store.
#[pyfunction]
#[pyo3(name = "record_recent_dismissed_agent_group", signature = (root, group, limit = 10))]
fn py_record_recent_dismissed_agent_group<'py>(
    py: Python<'py>,
    root: &str,
    group: &Bound<'py, PyDict>,
    limit: i64,
) -> PyResult<PyObject> {
    let value = py_to_json_value(group.as_any())?;
    let group: SavedAgentGroupWire =
        serde_json::from_value(value).map_err(|e| {
            PyValueError::new_err(format!(
                "group is not a valid SavedAgentGroupWire dict: {e}"
            ))
        })?;
    let result = py
        .allow_threads(|| {
            core_record_recent_dismissed_agent_group(
                &PathBuf::from(root),
                group,
                limit,
            )
        })
        .map_err(PyValueError::new_err)?;
    let value = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// List recent dismissed-agent group summaries from the capped store.
#[pyfunction]
#[pyo3(name = "list_recent_dismissed_agent_groups", signature = (root, limit = 10))]
fn py_list_recent_dismissed_agent_groups<'py>(
    py: Python<'py>,
    root: &str,
    limit: i64,
) -> PyResult<PyObject> {
    let result = py.allow_threads(|| {
        core_list_recent_dismissed_agent_groups(&PathBuf::from(root), limit)
    });
    let value = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Load one recent dismissed-agent group, returning None when absent/corrupt.
#[pyfunction]
#[pyo3(name = "load_recent_dismissed_agent_group")]
fn py_load_recent_dismissed_agent_group<'py>(
    py: Python<'py>,
    root: &str,
    group_id: &str,
) -> PyResult<PyObject> {
    let result = py
        .allow_threads(|| {
            core_load_recent_dismissed_agent_group(
                &PathBuf::from(root),
                group_id,
            )
        })
        .map_err(PyValueError::new_err)?;
    let value = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Mark one recent dismissed-agent group revived.
#[pyfunction]
#[pyo3(name = "mark_recent_dismissed_agent_group_revived")]
fn py_mark_recent_dismissed_agent_group_revived<'py>(
    py: Python<'py>,
    root: &str,
    group_id: &str,
    revived_at: &str,
) -> PyResult<PyObject> {
    let result = py
        .allow_threads(|| {
            core_mark_recent_dismissed_agent_group_revived(
                &PathBuf::from(root),
                group_id,
                revived_at,
            )
        })
        .map_err(PyValueError::new_err)?;
    let value = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Plan agent cleanup without executing side effects.
///
/// `targets` is a list of `AgentCleanupTargetWire`-shape dicts gathered by
/// the host. `request` is an `AgentCleanupRequestWire`-shape dict choosing
/// the scope and mode. The returned dict is an `AgentCleanupPlanWire` whose
/// kill/dismiss lists can be previewed or executed by Python.
#[pyfunction]
#[pyo3(name = "agent_cleanup_wire_schema_version")]
fn py_agent_cleanup_wire_schema_version() -> u32 {
    sase_core::AGENT_CLEANUP_WIRE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "plan_agent_cleanup")]
fn py_plan_agent_cleanup<'py>(
    py: Python<'py>,
    targets: &Bound<'py, PyList>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let mut wire_targets: Vec<AgentCleanupTargetWire> =
        Vec::with_capacity(targets.len());
    for (idx, item) in targets.iter().enumerate() {
        let json = py_to_json_value(&item)?;
        let target: AgentCleanupTargetWire =
            serde_json::from_value(json).map_err(|e| {
                PyValueError::new_err(format!(
                    "targets[{idx}] is not a valid AgentCleanupTargetWire dict: {e}"
                ))
            })?;
        wire_targets.push(target);
    }

    let request_value = py_to_json_value(request.as_any())?;
    let req: AgentCleanupRequestWire =
        cleanup_request_from_json_value(&request_value).map_err(|e| {
            PyValueError::new_err(format!(
                "request is not a valid AgentCleanupRequestWire dict: {e}"
            ))
        })?;
    let plan = core_plan_agent_cleanup(&wire_targets, &req)
        .map_err(PyValueError::new_err)?;
    let value = serde_json::to_value(&plan).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Save dismissed agent identities to the host-provided index file.
#[pyfunction]
#[pyo3(name = "save_dismissed_agents_index")]
fn py_save_dismissed_agents_index(
    path: &str,
    identities: &Bound<'_, PyList>,
) -> PyResult<()> {
    let mut wire_identities: Vec<AgentCleanupIdentityWire> =
        Vec::with_capacity(identities.len());
    for (idx, item) in identities.iter().enumerate() {
        let json = py_to_json_value(&item)?;
        let identity: AgentCleanupIdentityWire =
            serde_json::from_value(json).map_err(|e| {
                PyValueError::new_err(format!(
                    "identities[{idx}] is not a valid AgentCleanupIdentityWire dict: {e}"
                ))
            })?;
        wire_identities.push(identity);
    }
    core_save_dismissed_agents_index(&PathBuf::from(path), &wire_identities)
        .map_err(PyValueError::new_err)
}

/// Write one dismissed-agent bundle using the sharded bundle layout.
#[pyfunction]
#[pyo3(name = "save_dismissed_bundle")]
fn py_save_dismissed_bundle<'py>(
    py: Python<'py>,
    bundle_root: &str,
    bundle: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let json = py_to_json_value(bundle.as_any())?;
    let result =
        core_save_dismissed_bundle_json(&PathBuf::from(bundle_root), &json)
            .map_err(PyValueError::new_err)?;
    let value = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Delete loader marker files from an agent artifacts directory.
#[pyfunction]
#[pyo3(name = "delete_agent_artifacts")]
fn py_delete_agent_artifacts<'py>(
    py: Python<'py>,
    artifacts_dir: &str,
) -> PyResult<PyObject> {
    let result =
        core_delete_agent_artifact_markers(&PathBuf::from(artifacts_dir))
            .map_err(PyValueError::new_err)?;
    let value = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Return project-file text after releasing one RUNNING workspace claim.
#[pyfunction]
#[pyo3(name = "release_workspace_from_content", signature = (content, workspace_num, workflow = None, cl_name = None))]
fn py_release_workspace_from_content<'py>(
    py: Python<'py>,
    content: &str,
    workspace_num: i64,
    workflow: Option<&str>,
    cl_name: Option<&str>,
) -> PyResult<PyObject> {
    let result = core_release_workspace_from_content(
        content,
        workspace_num,
        workflow,
        cl_name,
    );
    let value = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Mark hook status lines for killed running-agent suffixes.
#[pyfunction]
#[pyo3(name = "mark_hook_agents_as_killed")]
fn py_mark_hook_agents_as_killed<'py>(
    py: Python<'py>,
    hooks: &Bound<'py, PyList>,
    suffixes: &Bound<'py, PyList>,
) -> PyResult<PyObject> {
    let wire_hooks = hooks_from_py(hooks)?;
    let suffixes = strings_from_py_list(suffixes, "suffixes")?;
    let result = core_mark_hook_agents_as_killed(&wire_hooks, &suffixes);
    json_value_to_py(
        py,
        &serde_json::to_value(result).map_err(|e| {
            PyValueError::new_err(format!("internal serialize error: {e}"))
        })?,
    )
}

/// Mark mentor status lines for killed running-agent suffixes.
#[pyfunction]
#[pyo3(name = "mark_mentor_agents_as_killed")]
fn py_mark_mentor_agents_as_killed<'py>(
    py: Python<'py>,
    mentors: &Bound<'py, PyList>,
    suffixes: &Bound<'py, PyList>,
) -> PyResult<PyObject> {
    let wire_mentors = mentors_from_py(mentors)?;
    let suffixes = strings_from_py_list(suffixes, "suffixes")?;
    let result = core_mark_mentor_agents_as_killed(&wire_mentors, &suffixes);
    json_value_to_py(
        py,
        &serde_json::to_value(result).map_err(|e| {
            PyValueError::new_err(format!("internal serialize error: {e}"))
        })?,
    )
}

/// Mark comment entries for killed running-agent suffixes.
#[pyfunction]
#[pyo3(name = "mark_comment_agents_as_killed")]
fn py_mark_comment_agents_as_killed<'py>(
    py: Python<'py>,
    comments: &Bound<'py, PyList>,
    suffixes: &Bound<'py, PyList>,
) -> PyResult<PyObject> {
    let wire_comments = comments_from_py(comments)?;
    let suffixes = strings_from_py_list(suffixes, "suffixes")?;
    let result = core_mark_comment_agents_as_killed(&wire_comments, &suffixes);
    json_value_to_py(
        py,
        &serde_json::to_value(result).map_err(|e| {
            PyValueError::new_err(format!("internal serialize error: {e}"))
        })?,
    )
}

// --- Phase 4C status state machine bindings -------------------------------

/// Strip workspace and legacy READY-TO-MAIL suffixes from a STATUS string.
///
/// Mirrors `sase.status_state_machine.constants.remove_workspace_suffix`.
/// Useful for tests and for callers that want the canonical base status
/// without going through the planner.
#[pyfunction]
#[pyo3(name = "remove_workspace_suffix")]
fn py_remove_workspace_suffix(status: &str) -> String {
    core_remove_workspace_suffix(status)
}

/// Whether a transition from *from_status* to *to_status* is allowed.
///
/// Mirrors `sase.status_state_machine.constants.is_valid_transition`.
/// Workspace suffixes on either side are stripped before validation.
#[pyfunction]
#[pyo3(name = "is_valid_status_transition")]
fn py_is_valid_status_transition(from_status: &str, to_status: &str) -> bool {
    core_is_valid_transition(from_status, to_status)
}

/// Read the STATUS for the requested Patch name from a list of project-file lines.
///
/// Mirrors `sase.status_state_machine.field_updates.read_status_from_lines_python`.
/// Returns `None` when the Patch is not present.
#[pyfunction]
#[pyo3(name = "read_status_from_lines")]
fn py_read_status_from_lines<'py>(
    py: Python<'py>,
    lines: &Bound<'py, PyList>,
    // Legacy Python keyword retained for compatibility.
    changespec_name: &str,
) -> PyResult<PyObject> {
    let mut owned: Vec<String> = Vec::with_capacity(lines.len());
    for (idx, item) in lines.iter().enumerate() {
        let s: String = item.extract().map_err(|_| {
            PyValueError::new_err(format!("lines[{idx}] must be a string"))
        })?;
        owned.push(s);
    }
    let result = core_read_status_from_lines(&owned, changespec_name);
    Ok(match result {
        Some(s) => s.into_py(py),
        None => py.None(),
    })
}

/// Apply a STATUS update to a list of project-file lines and return the
/// updated content as a single string.
///
/// Mirrors `sase.status_state_machine.field_updates.apply_status_update_python`.
#[pyfunction]
#[pyo3(name = "apply_status_update")]
fn py_apply_status_update<'py>(
    lines: &Bound<'py, PyList>,
    // Legacy Python keyword retained for compatibility.
    changespec_name: &str,
    new_status: &str,
) -> PyResult<String> {
    let mut owned: Vec<String> = Vec::with_capacity(lines.len());
    for (idx, item) in lines.iter().enumerate() {
        let s: String = item.extract().map_err(|_| {
            PyValueError::new_err(format!("lines[{idx}] must be a string"))
        })?;
        owned.push(s);
    }
    Ok(core_apply_status_update(
        &owned,
        changespec_name,
        new_status,
    ))
}

/// Plan a status transition for one Patch.
///
/// *request* must be a `StatusTransitionRequestWire`-shape dict (see
/// `sase.core.status_wire`). The result is a
/// `StatusTransitionPlanWire`-shape dict — the Python adapter rehydrates
/// it via `status_plan_from_dict`.
///
/// Schema-version mismatches and structurally invalid requests surface as
/// `ValueError` so the existing UI validation layer can catch them.
#[pyfunction]
#[pyo3(name = "plan_status_transition")]
fn py_plan_status_transition<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let req: StatusTransitionRequestWire = serde_json::from_value(value)
        .map_err(|e| {
            PyValueError::new_err(format!(
                "request is not a valid StatusTransitionRequestWire dict: {e}"
            ))
        })?;
    let plan =
        core_plan_status_transition(&req).map_err(PyValueError::new_err)?;
    let value = serde_json::to_value(&plan).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

// --- Phase 5C Git query parser bindings -----------------------------------

/// Parse the NUL-delimited output of `git diff --name-status -z` into a
/// `list[dict]` mirroring `GitNameStatusEntryWire` JSON shape.
///
/// Mirrors `sase.core.git_query_facade._parse_git_name_status_z_python`.
/// The Python facade rehydrates each dict into a
/// `GitNameStatusEntryWire` via `git_name_status_entry_from_dict` and
/// flattens to `list[tuple[str, str]]` for legacy callers. The dict
/// shape (`{"status": str, "path": str}`) is the same one
/// `git_query_wire_to_json_dict` produces, so no extra translation is
/// required.
#[pyfunction]
#[pyo3(name = "parse_git_name_status_z")]
fn py_parse_git_name_status_z<'py>(
    py: Python<'py>,
    stdout: &str,
) -> PyResult<Bound<'py, PyList>> {
    let entries = core_parse_git_name_status_z(stdout);
    let list = PyList::empty_bound(py);
    for entry in &entries {
        let dict = PyDict::new_bound(py);
        dict.set_item("status", &entry.status)?;
        dict.set_item("path", &entry.path)?;
        list.append(dict)?;
    }
    Ok(list)
}

/// Normalize `git rev-parse --abbrev-ref HEAD` stdout into a branch
/// name. Returns `None` for empty stdout or the detached-HEAD sentinel.
///
/// Mirrors `sase.core.git_query_facade.parse_git_branch_name`.
#[pyfunction]
#[pyo3(name = "parse_git_branch_name")]
fn py_parse_git_branch_name(py: Python<'_>, stdout: &str) -> PyObject {
    match core_parse_git_branch_name(stdout) {
        Some(name) => name.into_py(py),
        None => py.None(),
    }
}

/// Derive a workspace name from a remote URL (preferred) or repository
/// root path. Returns `None` when neither input produces a non-empty
/// name.
///
/// Mirrors `sase.core.git_query_facade.derive_git_workspace_name`.
#[pyfunction]
#[pyo3(name = "derive_git_workspace_name", signature = (remote_url, root_path))]
fn py_derive_git_workspace_name(
    py: Python<'_>,
    remote_url: Option<&str>,
    root_path: Option<&str>,
) -> PyObject {
    match core_derive_git_workspace_name(remote_url, root_path) {
        Some(name) => name.into_py(py),
        None => py.None(),
    }
}

/// Split `git diff --name-only --diff-filter=U` stdout into a
/// `list[str]` of conflicted paths (blank lines dropped, order
/// preserved).
///
/// Mirrors `sase.core.git_query_facade.parse_git_conflicted_files`.
#[pyfunction]
#[pyo3(name = "parse_git_conflicted_files")]
fn py_parse_git_conflicted_files<'py>(
    py: Python<'py>,
    stdout: &str,
) -> PyResult<Bound<'py, PyList>> {
    let paths = core_parse_git_conflicted_files(stdout);
    let list = PyList::empty_bound(py);
    for p in paths {
        list.append(p)?;
    }
    Ok(list)
}

/// Normalize `git status --porcelain` stdout into a clean/dirty signal.
/// Returns `None` for an empty/whitespace-only tree, the stripped text
/// otherwise.
///
/// Mirrors `sase.core.git_query_facade.parse_git_local_changes`.
#[pyfunction]
#[pyo3(name = "parse_git_local_changes")]
fn py_parse_git_local_changes(py: Python<'_>, stdout: &str) -> PyObject {
    match core_parse_git_local_changes(stdout) {
        Some(text) => text.into_py(py),
        None => py.None(),
    }
}

// --- vcs_log parser + aggregator bindings --------------------------------

/// Serialize a `VcsCommitWire` into a `PyDict` mirroring the Python
/// `VcsCommitWire` dataclass JSON shape.
fn vcs_commit_wire_to_py<'py>(
    py: Python<'py>,
    commit: &VcsCommitWire,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new_bound(py);
    dict.set_item("full_id", &commit.full_id)?;
    dict.set_item("short_id", &commit.short_id)?;
    dict.set_item("author_name", &commit.author_name)?;
    dict.set_item("author_email", &commit.author_email)?;
    dict.set_item("timestamp", commit.timestamp)?;
    dict.set_item("parent_ids", PyList::new_bound(py, &commit.parent_ids))?;
    dict.set_item("subject", &commit.subject)?;
    dict.set_item("body", &commit.body)?;
    dict.set_item("presence", commit_presence_to_str(commit.presence))?;
    Ok(dict)
}

fn commit_presence_to_str(presence: CommitPresenceWire) -> &'static str {
    match presence {
        CommitPresenceWire::Unknown => "unknown",
        CommitPresenceWire::Synced => "synced",
        CommitPresenceWire::RemoteOnly => "remote_only",
        CommitPresenceWire::LocalOnly => "local_only",
    }
}

/// Return the VCS-log wire schema version expected by this binding.
#[pyfunction]
#[pyo3(name = "vcs_log_wire_schema_version")]
fn py_vcs_log_wire_schema_version() -> u32 {
    VCS_LOG_WIRE_SCHEMA_VERSION
}

/// Parse a pinned, separator-delimited `git log --format=...` stream into
/// a `list[dict]` mirroring the `VcsCommitWire` JSON shape.
///
/// Mirrors `sase.core.vcs_log_facade._parse_git_log_python`. The Python
/// facade rehydrates each dict into a `VcsCommitWire` via
/// `vcs_commit_from_dict`.
#[pyfunction]
#[pyo3(name = "parse_git_log")]
fn py_parse_git_log<'py>(
    py: Python<'py>,
    stdout: &str,
) -> PyResult<Bound<'py, PyList>> {
    let commits = core_parse_git_log(stdout);
    let list = PyList::empty_bound(py);
    for commit in &commits {
        list.append(vcs_commit_wire_to_py(py, commit)?)?;
    }
    Ok(list)
}

/// Stamp VCS-log commits with local/remote presence.
#[pyfunction]
#[pyo3(name = "classify_commit_presence")]
fn py_classify_commit_presence<'py>(
    py: Python<'py>,
    commits: &Bound<'_, PyAny>,
    ahead_ids: Vec<String>,
    behind_ids: Vec<String>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(commits)?;
    let parsed: Vec<VcsCommitWire> =
        serde_json::from_value(value).map_err(|e| {
            PyValueError::new_err(format!(
                "commits is not a valid list[VcsCommitWire]: {e}"
            ))
        })?;
    let classified =
        core_classify_commit_presence(parsed, ahead_ids, behind_ids);
    let out = serde_json::to_value(&classified).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &out)
}

/// Interleave per-repo commit lists into a single newest-first timeline.
///
/// `repos` is a `list[tuple[str, list[dict]]]` of `(repo_label, commits)`
/// where each commit dict has the `VcsCommitWire` shape. Returns a
/// `list[dict]` of `AggregatedCommitWire`-shape dicts (the commit fields
/// flattened with a leading `repo` key), sorted by `timestamp` desc with a
/// stable `(repo, full_id)` tie-break and truncated to `limit`.
///
/// Mirrors `sase.core.vcs_log_facade._aggregate_commit_log_python`.
#[pyfunction]
#[pyo3(name = "aggregate_commit_log")]
fn py_aggregate_commit_log<'py>(
    py: Python<'py>,
    repos: &Bound<'_, PyAny>,
    limit: usize,
) -> PyResult<PyObject> {
    let value = py_to_json_value(repos)?;
    let parsed: Vec<(String, Vec<VcsCommitWire>)> =
        serde_json::from_value(value).map_err(|e| {
            PyValueError::new_err(format!(
            "repos is not a valid list[tuple[str, list[VcsCommitWire]]]: {e}"
        ))
        })?;
    let aggregated = core_aggregate_commit_log(parsed, limit);
    let out = serde_json::to_value(&aggregated).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &out)
}

/// Strictly summarize a recognized merge commit subject.
#[pyfunction]
#[pyo3(name = "parse_merge_summary")]
fn py_parse_merge_summary<'py>(
    py: Python<'py>,
    subject: &str,
    body: &str,
) -> PyResult<PyObject> {
    match core_parse_merge_summary(subject, body) {
        Some(summary) => {
            let value = serde_json::to_value(&summary).map_err(|e| {
                PyValueError::new_err(format!("internal serialize error: {e}"))
            })?;
            json_value_to_py(py, &value)
        }
        None => Ok(py.None()),
    }
}

// --- Project lifecycle bindings ------------------------------------------

/// Read effective project lifecycle metadata from ProjectSpec content.
#[pyfunction]
#[pyo3(name = "read_project_lifecycle_from_content")]
fn py_read_project_lifecycle_from_content<'py>(
    py: Python<'py>,
    content: &str,
) -> PyResult<PyObject> {
    let lifecycle = core_read_project_lifecycle_from_content(content);
    let value = serde_json::to_value(&lifecycle).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Return ProjectSpec content with PROJECT_STATE updated in the metadata header.
#[pyfunction]
#[pyo3(name = "apply_project_lifecycle_update")]
fn py_apply_project_lifecycle_update(
    content: &str,
    state: &str,
) -> PyResult<String> {
    core_apply_project_lifecycle_update(content, state)
        .map_err(|err| PyValueError::new_err(err.to_string()))
}

/// Return ProjectSpec content with PROJECT_ALIASES updated in the metadata header.
#[pyfunction]
#[pyo3(name = "apply_project_aliases_update")]
fn py_apply_project_aliases_update<'py>(
    content: &str,
    aliases: &Bound<'py, PyList>,
) -> PyResult<String> {
    let aliases = strings_from_py_list(aliases, "aliases")?;
    core_apply_project_aliases_update(content, &aliases)
        .map_err(|err| PyValueError::new_err(err.to_string()))
}

/// Return ProjectSpec content with PROJECT_NAME updated in the metadata header.
#[pyfunction]
#[pyo3(name = "apply_project_name_update")]
#[pyo3(signature = (content, name=None))]
fn py_apply_project_name_update(
    content: &str,
    name: Option<String>,
) -> PyResult<String> {
    core_apply_project_name_update(content, name.as_deref())
        .map_err(|err| PyValueError::new_err(err.to_string()))
}

/// List lifecycle records for project directories under *projects_root*.
#[pyfunction]
#[pyo3(name = "list_project_records", signature = (projects_root, include_states, include_home = false, projects_only = false))]
fn py_list_project_records<'py>(
    py: Python<'py>,
    projects_root: &str,
    include_states: &Bound<'py, PyList>,
    include_home: bool,
    projects_only: bool,
) -> PyResult<PyObject> {
    let states = strings_from_py_list(include_states, "include_states")?;
    let root = PathBuf::from(projects_root);
    let records = py
        .allow_threads(|| {
            core_list_project_records(
                &root,
                &states,
                include_home,
                projects_only,
            )
        })
        .map_err(|err| PyValueError::new_err(err.to_string()))?;
    let value = serde_json::to_value(&records).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

// --- Bead read bindings ---------------------------------------------------

#[pyfunction]
#[pyo3(
    name = "bead_needs_size_check_relax_migration",
    signature = (create_table_sql=None)
)]
fn py_bead_needs_size_check_relax_migration(
    create_table_sql: Option<&str>,
) -> bool {
    core_bead_needs_size_check_relax_migration(create_table_sql)
}

#[pyfunction]
#[pyo3(name = "bead_size_check_relax_migration_sql")]
fn py_bead_size_check_relax_migration_sql() -> &'static str {
    core_bead_size_check_relax_migration_sql()
}

#[pyfunction]
#[pyo3(
    name = "bead_needs_task_ready_migration",
    signature = (create_table_sql=None)
)]
fn py_bead_needs_task_ready_migration(create_table_sql: Option<&str>) -> bool {
    core_bead_needs_task_ready_migration(create_table_sql)
}

#[pyfunction]
#[pyo3(name = "bead_task_ready_migration_sql")]
fn py_bead_task_ready_migration_sql() -> &'static str {
    core_bead_task_ready_migration_sql()
}

#[pyfunction]
#[pyo3(
    name = "bead_needs_snoozed_status_migration",
    signature = (create_table_sql=None)
)]
fn py_bead_needs_snoozed_status_migration(
    create_table_sql: Option<&str>,
) -> bool {
    core_bead_needs_snoozed_status_migration(create_table_sql)
}

#[pyfunction]
#[pyo3(name = "bead_snoozed_status_migration_sql")]
fn py_bead_snoozed_status_migration_sql() -> &'static str {
    core_bead_snoozed_status_migration_sql()
}

#[pyfunction]
#[pyo3(
    name = "bead_needs_resolution_migration",
    signature = (create_table_sql=None)
)]
fn py_bead_needs_resolution_migration(create_table_sql: Option<&str>) -> bool {
    core_bead_needs_resolution_migration(create_table_sql)
}

#[pyfunction]
#[pyo3(name = "bead_resolution_migration_sql")]
fn py_bead_resolution_migration_sql() -> &'static str {
    core_bead_resolution_migration_sql()
}

#[pyfunction]
#[pyo3(
    name = "bead_needs_plus_one_evidence_migration",
    signature = (create_table_sql=None)
)]
fn py_bead_needs_plus_one_evidence_migration(
    create_table_sql: Option<&str>,
) -> bool {
    core_bead_needs_plus_one_evidence_migration(create_table_sql)
}

#[pyfunction]
#[pyo3(name = "bead_plus_one_evidence_migration_sql")]
fn py_bead_plus_one_evidence_migration_sql() -> &'static str {
    core_bead_plus_one_evidence_migration_sql()
}

#[pyfunction]
#[pyo3(name = "bead_read_store")]
fn py_bead_read_store<'py>(
    py: Python<'py>,
    beads_dir: &str,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| core_bead_read_store_issues(&beads_dir)),
    )
}

#[pyfunction]
#[pyo3(name = "bead_read_event_store")]
fn py_bead_read_event_store<'py>(
    py: Python<'py>,
    beads_dir: &str,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| core_bead_read_event_store_issues(&beads_dir)),
    )
}

#[pyfunction]
#[pyo3(name = "bead_read_legacy_jsonl")]
fn py_bead_read_legacy_jsonl<'py>(
    py: Python<'py>,
    beads_dir: &str,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| core_bead_read_legacy_jsonl_issues(&beads_dir)),
    )
}

#[pyfunction]
#[pyo3(name = "bead_resolve_id")]
fn py_bead_resolve_id(
    py: Python<'_>,
    beads_dir: &str,
    issue_id: &str,
) -> PyResult<String> {
    let beads_dir = PathBuf::from(beads_dir);
    py.allow_threads(|| core_bead_resolve_issue_id(&beads_dir, issue_id))
        .map_err(bead_error_to_pyerr)
}

#[pyfunction]
#[pyo3(name = "bead_show")]
fn py_bead_show<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_id: &str,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| core_bead_show_issue(&beads_dir, issue_id)),
    )
}

#[pyfunction]
#[pyo3(name = "bead_show_issue_detail")]
fn py_bead_show_issue_detail<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_id: &str,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| core_bead_show_issue_detail(&beads_dir, issue_id)),
    )
}

#[pyfunction]
#[pyo3(name = "bead_history")]
fn py_bead_history<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_id: &str,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| core_bead_history(&beads_dir, issue_id)),
    )
}

#[pyfunction]
#[pyo3(name = "bead_lost_notes")]
#[pyo3(signature = (beads_dir, issue_id=None))]
fn py_bead_lost_notes<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_id: Option<&str>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| core_bead_lost_notes(&beads_dir, issue_id)),
    )
}

#[pyfunction]
#[pyo3(name = "bead_list")]
#[pyo3(signature = (beads_dir, statuses=None, issue_types=None, tiers=None))]
fn py_bead_list<'py>(
    py: Python<'py>,
    beads_dir: &str,
    statuses: Option<Vec<String>>,
    issue_types: Option<Vec<String>>,
    tiers: Option<Vec<String>>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_list_issues(
                &beads_dir,
                statuses.as_deref(),
                issue_types.as_deref(),
                tiers.as_deref(),
            )
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_search")]
#[pyo3(signature = (beads_dir, query, statuses=None, issue_types=None, tiers=None, limit=None, regex=false))]
#[allow(clippy::too_many_arguments)]
fn py_bead_search<'py>(
    py: Python<'py>,
    beads_dir: &str,
    query: &str,
    statuses: Option<Vec<String>>,
    issue_types: Option<Vec<String>>,
    tiers: Option<Vec<String>>,
    limit: Option<usize>,
    regex: bool,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_search_issues(
                &beads_dir,
                query,
                statuses.as_deref(),
                issue_types.as_deref(),
                tiers.as_deref(),
                limit,
                regex,
            )
        }),
    )
}

/// Search and rank markdown plan artifacts under a repo `sdd/` tree and/or the
/// machine-local archive.
///
/// `repo_sdd_root`/`local_plans_dir` are passed through as `Option`s so callers
/// scope by `--source` (pass `None` to skip a corpus). The remaining arguments
/// mirror [`core_plan_search`]: optional `query` (browse when omitted), repo
/// `kinds`, frontmatter `statuses`, a `sources` filter, a `[since, until]` date
/// range, `sort` mode, and `limit` (`0`/`None` = unlimited).
/// `document_corpora`, when supplied, replaces the legacy repo-root scan with
/// explicit `(root, kind)` pairs. Returns a list of
/// `{plan, matched_fields, score}` dicts, following `bead_search`'s JSON shape.
#[pyfunction]
#[pyo3(name = "plan_search")]
#[pyo3(signature = (repo_sdd_root=None, local_plans_dir=None, query=None, kinds=None, statuses=None, sources=None, since=None, until=None, sort=None, limit=None, document_corpora=None))]
#[allow(clippy::too_many_arguments)]
fn py_plan_search<'py>(
    py: Python<'py>,
    repo_sdd_root: Option<String>,
    local_plans_dir: Option<String>,
    query: Option<String>,
    kinds: Option<Vec<String>>,
    statuses: Option<Vec<String>>,
    sources: Option<Vec<String>>,
    since: Option<String>,
    until: Option<String>,
    sort: Option<String>,
    limit: Option<usize>,
    document_corpora: Option<Vec<(String, String)>>,
) -> PyResult<PyObject> {
    let repo_sdd_root = repo_sdd_root.map(PathBuf::from);
    let local_plans_dir = local_plans_dir.map(PathBuf::from);
    let document_corpora = document_corpora.map(|corpora| {
        corpora
            .into_iter()
            .map(|(root, kind)| (PathBuf::from(root), kind))
            .collect::<Vec<_>>()
    });
    plan_result_to_py(
        py,
        py.allow_threads(|| {
            core_plan_search(
                repo_sdd_root.as_deref(),
                local_plans_dir.as_deref(),
                query.as_deref(),
                kinds.as_deref(),
                statuses.as_deref(),
                sources.as_deref(),
                since.as_deref(),
                until.as_deref(),
                sort.as_deref(),
                limit,
                document_corpora.as_deref(),
            )
        }),
    )
}

/// Strictly validate one complete markdown plan against an explicit tier.
#[pyfunction]
#[pyo3(name = "plan_validate")]
#[pyo3(signature = (content, tier, mode = "authoring"))]
fn py_plan_validate<'py>(
    py: Python<'py>,
    content: &str,
    tier: &str,
    mode: &str,
) -> PyResult<PyObject> {
    plan_result_to_py(
        py,
        py.allow_threads(|| core_plan_validate_with_mode(content, tier, mode)),
    )
}

/// Return ordered authoritative frontmatter field metadata for a plan tier.
#[pyfunction]
#[pyo3(name = "plan_frontmatter_schema")]
fn py_plan_frontmatter_schema<'py>(
    py: Python<'py>,
    tier: &str,
) -> PyResult<PyObject> {
    plan_result_to_py(
        py,
        py.allow_threads(|| core_plan_frontmatter_schema(tier)),
    )
}

/// Parse a canonical plan reference or preserve a legacy path.
#[pyfunction]
#[pyo3(name = "plan_reference_parse")]
fn py_plan_reference_parse<'py>(
    py: Python<'py>,
    value: &str,
) -> PyResult<PyObject> {
    plan_result_to_py(py, core_parse_plan_reference(value))
}

/// Render one validated canonical plan reference.
#[pyfunction]
#[pyo3(name = "plan_reference_render")]
fn py_plan_reference_render(kind: &str, path: &str) -> PyResult<String> {
    core_render_plan_reference(kind, path).map_err(plan_error_to_pyerr)
}

/// Canonicalize a plan path against ordered plan roots.
#[pyfunction]
#[pyo3(name = "plan_reference_canonicalize")]
fn py_plan_reference_canonicalize(
    path: &str,
    roots: Vec<String>,
) -> PyResult<Option<String>> {
    let path = PathBuf::from(path);
    let roots = roots.into_iter().map(PathBuf::from).collect::<Vec<_>>();
    core_canonicalize_plan_reference(&path, &roots).map_err(plan_error_to_pyerr)
}

/// Resolve a canonical or legacy plan reference against ordered roots.
#[pyfunction]
#[pyo3(name = "plan_reference_resolve")]
fn py_plan_reference_resolve<'py>(
    py: Python<'py>,
    value: &str,
    roots: Vec<String>,
) -> PyResult<PyObject> {
    let roots = roots.into_iter().map(PathBuf::from).collect::<Vec<_>>();
    plan_result_to_py(
        py,
        py.allow_threads(|| core_resolve_plan_reference(value, &roots)),
    )
}

/// Return the plan-reference resolution wire schema version.
#[pyfunction]
#[pyo3(name = "plan_reference_resolution_wire_schema_version")]
fn py_plan_reference_resolution_wire_schema_version() -> u64 {
    PLAN_REFERENCE_RESOLUTION_WIRE_SCHEMA_VERSION
}

/// Parse one canonical kind-tagged artifact reference.
#[pyfunction]
#[pyo3(name = "artifact_ref_parse")]
fn py_artifact_ref_parse<'py>(
    py: Python<'py>,
    value: &str,
) -> PyResult<PyObject> {
    artifact_ref_result_to_py(py, core_parse_artifact_ref(value))
}

/// Render a parsed artifact-reference dictionary.
#[pyfunction]
#[pyo3(name = "artifact_ref_render")]
fn py_artifact_ref_render(reference: &Bound<'_, PyAny>) -> PyResult<String> {
    let reference = artifact_ref_from_py(reference)?;
    core_render_artifact_ref(&reference).map_err(artifact_ref_error_to_pyerr)
}

/// Canonicalize an absolute path against caller-supplied local context.
#[pyfunction]
#[pyo3(name = "artifact_ref_canonicalize")]
fn py_artifact_ref_canonicalize(
    path: &str,
    context: &Bound<'_, PyDict>,
) -> PyResult<Option<String>> {
    let context = artifact_ref_context_from_pydict(context)?;
    let path = PathBuf::from(path);
    core_canonicalize_artifact_ref(&path, &context)
        .map_err(artifact_ref_error_to_pyerr)
}

/// Resolve a string or parsed reference against caller-supplied context.
#[pyfunction]
#[pyo3(name = "artifact_ref_resolve")]
fn py_artifact_ref_resolve<'py>(
    py: Python<'py>,
    reference: &Bound<'py, PyAny>,
    context: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let reference = artifact_ref_from_py(reference)?;
    let context = artifact_ref_context_from_pydict(context)?;
    artifact_ref_result_to_py(
        py,
        py.allow_threads(|| core_resolve_artifact_ref(&reference, &context)),
    )
}

/// Normalize a stored artifact-reference list.
#[pyfunction]
#[pyo3(name = "artifact_ref_list_normalize")]
fn py_artifact_ref_list_normalize(
    entries: Vec<String>,
) -> PyResult<Vec<String>> {
    core_normalize_artifact_ref_list(&entries)
        .map_err(artifact_ref_error_to_pyerr)
}

/// Parse every entry in a stored artifact-reference list.
#[pyfunction]
#[pyo3(name = "artifact_ref_list_parse")]
fn py_artifact_ref_list_parse<'py>(
    py: Python<'py>,
    entries: Vec<String>,
) -> PyResult<PyObject> {
    artifact_ref_result_to_py(py, core_parse_artifact_ref_list(&entries))
}

/// Resolve a stored artifact-reference list using one shared context.
#[pyfunction]
#[pyo3(name = "artifact_ref_list_resolve")]
fn py_artifact_ref_list_resolve<'py>(
    py: Python<'py>,
    entries: Vec<String>,
    context: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let context = artifact_ref_context_from_pydict(context)?;
    artifact_ref_result_to_py(
        py,
        py.allow_threads(|| core_resolve_artifact_ref_list(&entries, &context)),
    )
}

/// Return the artifact-reference list-resolution wire schema version.
#[pyfunction]
#[pyo3(name = "artifact_ref_list_resolution_wire_schema_version")]
fn py_artifact_ref_list_resolution_wire_schema_version() -> u64 {
    ARTIFACT_REF_LIST_RESOLUTION_WIRE_SCHEMA_VERSION
}

/// Return the artifact-reference context wire schema version.
#[pyfunction]
#[pyo3(name = "artifact_ref_context_wire_schema_version")]
fn py_artifact_ref_context_wire_schema_version() -> u64 {
    ARTIFACT_REF_CONTEXT_WIRE_SCHEMA_VERSION
}

/// Return the artifact-reference path-filter batch wire schema version.
#[pyfunction]
#[pyo3(name = "artifact_ref_path_filter_wire_schema_version")]
fn py_artifact_ref_path_filter_wire_schema_version() -> u64 {
    ARTIFACT_REF_PATH_FILTER_WIRE_SCHEMA_VERSION
}

/// Filter caller-owned repo-relative path payloads with the shared POSIX matcher.
#[pyfunction]
#[pyo3(name = "artifact_ref_filter_path_payloads")]
#[pyo3(signature = (kind, candidates, path_globs = None))]
fn py_artifact_ref_filter_path_payloads<'py>(
    py: Python<'py>,
    kind: &str,
    candidates: Vec<String>,
    path_globs: Option<Vec<String>>,
) -> PyResult<PyObject> {
    artifact_ref_result_to_py(
        py,
        core_filter_artifact_ref_path_payloads(
            kind,
            path_globs.as_deref(),
            &candidates,
        ),
    )
}

/// Scan prompt text for kind-tagged artifact-reference candidates.
#[pyfunction]
#[pyo3(name = "artifact_ref_scan_prompt")]
fn py_artifact_ref_scan_prompt<'py>(
    py: Python<'py>,
    text: &str,
) -> PyResult<PyObject> {
    let value = serde_json::to_value(core_scan_artifact_refs(text)).map_err(
        |error| {
            PyValueError::new_err(format!(
                "internal artifact reference serialize error: {error}"
            ))
        },
    )?;
    json_value_to_py(py, &value)
}

/// Return the shared parse/resolution artifact-reference wire version.
#[pyfunction]
#[pyo3(name = "artifact_ref_wire_schema_version")]
fn py_artifact_ref_wire_schema_version() -> u64 {
    debug_assert_eq!(
        ARTIFACT_REF_PARSE_WIRE_SCHEMA_VERSION,
        ARTIFACT_REF_RESOLUTION_WIRE_SCHEMA_VERSION
    );
    ARTIFACT_REF_PARSE_WIRE_SCHEMA_VERSION
}

/// Build one content-addressed prompt-artifact pool filename.
#[pyfunction]
#[pyo3(name = "prompt_artifact_pool_filename")]
fn py_prompt_artifact_pool_filename(
    sha256: &str,
    original_name: &str,
) -> String {
    core_artifact_pool_filename(sha256, original_name)
}

/// Parse valid rows from a tolerant prompt-artifact JSONL manifest.
#[pyfunction]
#[pyo3(name = "prompt_artifact_manifest_parse")]
fn py_prompt_artifact_manifest_parse<'py>(
    py: Python<'py>,
    data: &Bound<'py, PyBytes>,
) -> PyResult<PyObject> {
    prompt_artifact_result_to_py(
        py,
        &core_parse_prompt_artifact_manifest(data.as_bytes()),
    )
}

/// Render one prompt-artifact manifest row as compact JSON.
#[pyfunction]
#[pyo3(name = "prompt_artifact_manifest_render_record")]
fn py_prompt_artifact_manifest_render_record(
    record: &Bound<'_, PyDict>,
) -> PyResult<String> {
    let record = prompt_artifact_record_from_pydict(record)?;
    core_render_prompt_artifact_record(&record).map_err(|error| {
        PyValueError::new_err(format!(
            "invalid prompt artifact manifest record: {error}"
        ))
    })
}

/// Select the newest rows belonging to one agent artifact directory.
#[pyfunction]
#[pyo3(name = "prompt_artifact_manifest_select")]
fn py_prompt_artifact_manifest_select<'py>(
    py: Python<'py>,
    records: &Bound<'py, PyList>,
    agent_artifacts_dir: &str,
) -> PyResult<PyObject> {
    let records = prompt_artifact_records_from_py_list(records)?;
    let selected = core_select_prompt_artifact_manifest_records(
        &records,
        agent_artifacts_dir,
    );
    prompt_artifact_result_to_py(py, &selected)
}

/// Rewrite live artifact tokens using a Python target resolver.
#[pyfunction]
#[pyo3(name = "prompt_artifact_rewrite_links")]
fn py_prompt_artifact_rewrite_links<'py>(
    py: Python<'py>,
    prompt: &str,
    records: &Bound<'py, PyList>,
    resolver: &Bound<'py, PyAny>,
) -> PyResult<PyObject> {
    if !resolver.is_callable() {
        return Err(PyValueError::new_err(
            "prompt artifact resolver must be callable",
        ));
    }
    let records = prompt_artifact_records_from_py_list(records)?;
    let mut targets = Vec::with_capacity(records.len());
    for record in &records {
        let argument = serde_json::to_value(record)
            .map_err(|error| {
                PyValueError::new_err(format!(
                    "internal prompt artifact serialize error: {error}"
                ))
            })
            .and_then(|value| json_value_to_py(py, &value))?;
        let target = resolver.call1((argument,))?;
        targets.push(if target.is_none() {
            None
        } else {
            Some(target.extract::<String>().map_err(|_| {
                PyValueError::new_err(
                    "prompt artifact resolver must return str or None",
                )
            })?)
        });
    }
    let mut target_index = 0;
    let rewritten =
        core_rewrite_prompt_artifact_links(prompt, &records, |_| {
            let target = targets[target_index].clone();
            target_index += 1;
            target
        });
    prompt_artifact_result_to_py(py, &rewritten)
}

/// Return the prompt-artifact manifest wire schema version.
#[pyfunction]
#[pyo3(name = "prompt_artifact_wire_schema_version")]
fn py_prompt_artifact_wire_schema_version() -> u64 {
    PROMPT_ARTIFACT_MANIFEST_SCHEMA_VERSION
}

fn prompt_artifact_record_from_pydict(
    record: &Bound<'_, PyDict>,
) -> PyResult<PromptArtifactRecord> {
    serde_json::from_value(py_to_json_value(record.as_any())?).map_err(
        |error| {
            PyValueError::new_err(format!(
                "record is not a valid PromptArtifactRecord dict: {error}"
            ))
        },
    )
}

fn prompt_artifact_records_from_py_list(
    records: &Bound<'_, PyList>,
) -> PyResult<Vec<PromptArtifactRecord>> {
    records
        .iter()
        .enumerate()
        .map(|(index, record)| {
            serde_json::from_value(py_to_json_value(&record)?).map_err(
                |error| {
                    PyValueError::new_err(format!(
                        "records[{index}] is not a valid PromptArtifactRecord dict: {error}"
                    ))
                },
            )
        })
        .collect()
}

fn prompt_artifact_result_to_py<T>(
    py: Python<'_>,
    result: &T,
) -> PyResult<PyObject>
where
    T: serde::Serialize,
{
    let value = serde_json::to_value(result).map_err(|error| {
        PyValueError::new_err(format!(
            "internal prompt artifact serialize error: {error}"
        ))
    })?;
    json_value_to_py(py, &value)
}

/// Summarize the tolerant artifact-consumption ledger by canonical reference.
#[pyfunction]
#[pyo3(
    name = "artifact_consumption_summary",
    signature = (log_path, refs = None)
)]
fn py_artifact_consumption_summary<'py>(
    py: Python<'py>,
    log_path: &str,
    refs: Option<Vec<String>>,
) -> PyResult<PyObject> {
    let log_path = PathBuf::from(log_path);
    let events = py
        .allow_threads(|| core_read_artifact_consumption_log(&log_path))
        .map_err(|error| PyOSError::new_err(error.to_string()))?;
    let summary = core_summarize_artifact_consumption(&events, refs.as_deref());
    let value = serde_json::to_value(summary).map_err(|error| {
        PyValueError::new_err(format!(
            "internal artifact-consumption summary serialize error: {error}"
        ))
    })?;
    json_value_to_py(py, &value)
}

/// Return the artifact-consumption summary wire version.
#[pyfunction]
#[pyo3(name = "artifact_consumption_wire_schema_version")]
fn py_artifact_consumption_wire_schema_version() -> u64 {
    ARTIFACT_CONSUMPTION_WIRE_SCHEMA_VERSION
}

/// Query the tolerant artifact-file index using a frontend-neutral filter dict.
#[pyfunction]
#[pyo3(name = "artifact_files_query")]
fn py_artifact_files_query<'py>(
    py: Python<'py>,
    index_path: &str,
    filters: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let filters = serde_json::from_value::<ArtifactFileQueryFiltersWire>(
        py_to_json_value(filters.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "filters is not a valid ArtifactFileQueryFiltersWire dict: \
                 {error}"
        ))
    })?;
    let index_path = PathBuf::from(index_path);
    let rows = py
        .allow_threads(|| core_query_artifact_files(&index_path, &filters))
        .map_err(artifact_file_query_error_to_pyerr)?;
    let value = serde_json::to_value(rows).map_err(|error| {
        PyValueError::new_err(format!(
            "internal artifact-file query serialize error: {error}"
        ))
    })?;
    json_value_to_py(py, &value)
}

/// Return the artifact-file query result wire version.
#[pyfunction]
#[pyo3(name = "artifact_file_query_wire_schema_version")]
fn py_artifact_file_query_wire_schema_version() -> u64 {
    ARTIFACT_FILE_QUERY_WIRE_SCHEMA_VERSION
}

/// Materialize one VCS-backed artifact into its content-addressed cache.
#[pyfunction]
#[pyo3(name = "artifact_file_materialize_vcs")]
fn py_artifact_file_materialize_vcs<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request = serde_json::from_value::<
        ArtifactFileVcsMaterializationRequestWire,
    >(py_to_json_value(request.as_any())?)
    .map_err(|error| {
        PyValueError::new_err(format!(
            "request is not a valid \
                 ArtifactFileVcsMaterializationRequestWire dict: {error}"
        ))
    })?;
    let result =
        py.allow_threads(|| core_materialize_vcs_artifact_file(&request));
    let value = serde_json::to_value(result).map_err(|error| {
        PyValueError::new_err(format!(
            "internal artifact-file materialization serialize error: {error}"
        ))
    })?;
    json_value_to_py(py, &value)
}

/// Aggregate artifact-file store economics without mutating the index.
#[pyfunction]
#[pyo3(name = "artifact_file_store_economics")]
fn py_artifact_file_store_economics<'py>(
    py: Python<'py>,
    index_path: &str,
    options: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let options = serde_json::from_value::<ArtifactFileEconomicsOptionsWire>(
        py_to_json_value(options.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "options is not a valid ArtifactFileEconomicsOptionsWire dict: \
             {error}"
        ))
    })?;
    let index_path = PathBuf::from(index_path);
    let result = py
        .allow_threads(|| {
            core_artifact_file_store_economics(&index_path, &options)
        })
        .map_err(artifact_file_query_error_to_pyerr)?;
    artifact_file_lifecycle_value_to_py(py, &result, "economics")
}

/// Plan deterministic artifact-file retention without mutating the index.
#[pyfunction]
#[pyo3(name = "artifact_file_retention_plan")]
fn py_artifact_file_retention_plan<'py>(
    py: Python<'py>,
    index_path: &str,
    policy: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let policy = serde_json::from_value::<ArtifactFileRetentionPolicyWire>(
        py_to_json_value(policy.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "policy is not a valid ArtifactFileRetentionPolicyWire dict: \
             {error}"
        ))
    })?;
    let index_path = PathBuf::from(index_path);
    let result = py
        .allow_threads(|| {
            core_plan_artifact_file_retention(&index_path, &policy)
        })
        .map_err(artifact_file_query_error_to_pyerr)?;
    artifact_file_lifecycle_value_to_py(py, &result, "retention plan")
}

/// Move one artifact payload and its complete record into restorable trash.
#[pyfunction]
#[pyo3(name = "artifact_file_trash_store")]
fn py_artifact_file_trash_store<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request = serde_json::from_value::<ArtifactFileTrashRequestWire>(
        py_to_json_value(request.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "request is not a valid ArtifactFileTrashRequestWire dict: \
             {error}"
        ))
    })?;
    let result = py
        .allow_threads(|| core_trash_artifact_file(&request))
        .map_err(PyRuntimeError::new_err)?;
    artifact_file_lifecycle_value_to_py(py, &result, "trash store")
}

/// List restorable trash entries newest first.
#[pyfunction]
#[pyo3(name = "artifact_file_trash_list")]
fn py_artifact_file_trash_list<'py>(
    py: Python<'py>,
    trash_root: &str,
) -> PyResult<PyObject> {
    let trash_root = PathBuf::from(trash_root);
    let result = py
        .allow_threads(|| core_list_artifact_file_trash(&trash_root))
        .map_err(PyRuntimeError::new_err)?;
    artifact_file_lifecycle_value_to_py(py, &result, "trash list")
}

/// Restore one trash entry's payload and return its complete original record.
#[pyfunction]
#[pyo3(name = "artifact_file_trash_restore")]
fn py_artifact_file_trash_restore<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request =
        serde_json::from_value::<ArtifactFileTrashRestoreRequestWire>(
            py_to_json_value(request.as_any())?,
        )
        .map_err(|error| {
            PyValueError::new_err(format!(
                "request is not a valid \
                 ArtifactFileTrashRestoreRequestWire dict: {error}"
            ))
        })?;
    let result = py
        .allow_threads(|| core_restore_artifact_file_trash(&request))
        .map_err(PyRuntimeError::new_err)?;
    artifact_file_lifecycle_value_to_py(py, &result, "trash restore")
}

/// Permanently remove trash entries at or before an explicit cutoff.
#[pyfunction]
#[pyo3(name = "artifact_file_trash_purge")]
fn py_artifact_file_trash_purge<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request = serde_json::from_value::<ArtifactFileTrashPurgeRequestWire>(
        py_to_json_value(request.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "request is not a valid ArtifactFileTrashPurgeRequestWire \
                 dict: {error}"
        ))
    })?;
    let result = py
        .allow_threads(|| core_purge_artifact_file_trash(&request))
        .map_err(PyRuntimeError::new_err)?;
    artifact_file_lifecycle_value_to_py(py, &result, "trash purge")
}

/// Return the shared artifact-file lifecycle request/result wire version.
#[pyfunction]
#[pyo3(name = "artifact_file_lifecycle_wire_schema_version")]
fn py_artifact_file_lifecycle_wire_schema_version() -> u64 {
    ARTIFACT_FILE_LIFECYCLE_WIRE_SCHEMA_VERSION
}

/// Parse canonical and historical artifact links from one SDD document.
#[pyfunction]
#[pyo3(name = "sdd_artifact_link_parse")]
fn py_sdd_artifact_link_parse<'py>(
    py: Python<'py>,
    document: &str,
) -> PyResult<PyObject> {
    plan_result_to_py(py, Ok(core_parse_sdd_artifact_link(document)))
}

/// Render one canonical typed SDD artifact-link bullet.
#[pyfunction]
#[pyo3(name = "sdd_artifact_link_render")]
fn py_sdd_artifact_link_render(
    link_type: &str,
    label: &str,
    target: &str,
) -> PyResult<String> {
    core_render_sdd_artifact_link(link_type, label, target)
        .map_err(plan_error_to_pyerr)
}

/// Install a canonical artifact link and optionally remove its legacy field.
#[pyfunction]
#[pyo3(name = "sdd_artifact_link_upsert")]
fn py_sdd_artifact_link_upsert(
    document: &str,
    link_type: &str,
    label: &str,
    target: &str,
    remove_legacy: bool,
    allow_resolved_mixed: bool,
) -> PyResult<String> {
    core_upsert_sdd_artifact_link(
        document,
        link_type,
        label,
        target,
        remove_legacy,
        allow_resolved_mixed,
    )
    .map_err(plan_error_to_pyerr)
}

/// Return the plan-header block wire schema version.
#[pyfunction]
#[pyo3(name = "sdd_plan_header_block_wire_schema_version")]
fn py_sdd_plan_header_block_wire_schema_version() -> u64 {
    PLAN_HEADER_BLOCK_WIRE_SCHEMA_VERSION
}

/// Parse a complete SDD document's provenance header block.
#[pyfunction]
#[pyo3(name = "sdd_plan_header_block_parse")]
fn py_sdd_plan_header_block_parse<'py>(
    py: Python<'py>,
    document: &str,
) -> PyResult<PyObject> {
    plan_result_to_py(py, Ok(core_parse_sdd_plan_header_block(document)))
}

/// Render a complete canonical provenance header block.
#[pyfunction]
#[pyo3(name = "sdd_plan_header_block_render")]
fn py_sdd_plan_header_block_render(
    sections: &Bound<'_, PyList>,
) -> PyResult<String> {
    let sections = sdd_plan_header_sections_from_py_list(sections)?;
    core_render_sdd_plan_header_block(&sections).map_err(plan_error_to_pyerr)
}

/// Install or replace one provenance header section.
#[pyfunction]
#[pyo3(name = "sdd_plan_header_block_upsert_section")]
fn py_sdd_plan_header_block_upsert_section(
    document: &str,
    section: &Bound<'_, PyDict>,
    remove_legacy: bool,
    allow_resolved_mixed: bool,
) -> PyResult<String> {
    let section = sdd_plan_header_section_from_pydict(section)?;
    core_upsert_sdd_plan_header_section(
        document,
        section,
        remove_legacy,
        allow_resolved_mixed,
    )
    .map_err(plan_error_to_pyerr)
}

/// Replace the complete provenance header block.
#[pyfunction]
#[pyo3(name = "sdd_plan_header_block_replace")]
fn py_sdd_plan_header_block_replace(
    document: &str,
    sections: &Bound<'_, PyList>,
    remove_legacy: bool,
    allow_resolved_mixed: bool,
) -> PyResult<String> {
    let sections = sdd_plan_header_sections_from_py_list(sections)?;
    core_replace_sdd_plan_header_block(
        document,
        &sections,
        remove_legacy,
        allow_resolved_mixed,
    )
    .map_err(plan_error_to_pyerr)
}

/// Remove one provenance header section.
#[pyfunction]
#[pyo3(name = "sdd_plan_header_block_remove_section")]
fn py_sdd_plan_header_block_remove_section(
    document: &str,
    kind: &str,
    remove_legacy: bool,
    allow_resolved_mixed: bool,
) -> PyResult<String> {
    core_remove_sdd_plan_header_section(
        document,
        kind,
        remove_legacy,
        allow_resolved_mixed,
    )
    .map_err(plan_error_to_pyerr)
}

fn sdd_plan_header_section_from_pydict(
    section: &Bound<'_, PyDict>,
) -> PyResult<SddPlanHeaderSectionWire> {
    let value = py_to_json_value(section.as_any())?;
    serde_json::from_value(value).map_err(|error| {
        PyValueError::new_err(format!(
            "invalid plan header section payload: {error}"
        ))
    })
}

fn sdd_plan_header_sections_from_py_list(
    sections: &Bound<'_, PyList>,
) -> PyResult<Vec<SddPlanHeaderSectionWire>> {
    sections
        .iter()
        .map(|section| {
            let value = py_to_json_value(&section)?;
            serde_json::from_value(value).map_err(|error| {
                PyValueError::new_err(format!(
                    "invalid plan header section payload: {error}"
                ))
            })
        })
        .collect()
}

#[pyfunction]
#[pyo3(name = "bead_ready")]
fn py_bead_ready<'py>(py: Python<'py>, beads_dir: &str) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| core_bead_ready_issues(&beads_dir)),
    )
}

#[pyfunction]
#[pyo3(name = "bead_blocked")]
fn py_bead_blocked<'py>(
    py: Python<'py>,
    beads_dir: &str,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| core_bead_blocked_issues(&beads_dir)),
    )
}

#[pyfunction]
#[pyo3(name = "bead_stats")]
fn py_bead_stats<'py>(py: Python<'py>, beads_dir: &str) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(py, py.allow_threads(|| core_bead_stats(&beads_dir)))
}

#[pyfunction]
#[pyo3(signature = (beads_dir, plan_roots=None, reference_context=None))]
#[pyo3(name = "bead_doctor")]
fn py_bead_doctor<'py>(
    beads_dir: &str,
    plan_roots: Option<Vec<String>>,
    reference_context: Option<&Bound<'py, PyDict>>,
) -> PyResult<Vec<String>> {
    let beads_dir = PathBuf::from(beads_dir);
    if plan_roots.is_none() && reference_context.is_none() {
        return core_bead_doctor(&beads_dir).map_err(bead_error_to_pyerr);
    }
    let roots = plan_roots.and_then(|roots| {
        (!roots.is_empty())
            .then(|| roots.into_iter().map(PathBuf::from).collect::<Vec<_>>())
    });
    let reference_context = reference_context
        .map(artifact_ref_context_from_pydict)
        .transpose()?;
    let result = core_bead_doctor_with_contexts(
        &beads_dir,
        roots.as_deref(),
        reference_context.as_ref(),
    );
    result.map_err(bead_error_to_pyerr)
}

#[pyfunction]
#[pyo3(signature = (beads_dir, plan_roots=None, reference_context=None))]
#[pyo3(name = "bead_doctor_report")]
fn py_bead_doctor_report<'py>(
    py: Python<'py>,
    beads_dir: &str,
    plan_roots: Option<Vec<String>>,
    reference_context: Option<&Bound<'py, PyDict>>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    if plan_roots.is_none() && reference_context.is_none() {
        return bead_result_to_py(
            py,
            py.allow_threads(|| core_bead_doctor_report(&beads_dir)),
        );
    }
    let roots = plan_roots.and_then(|roots| {
        (!roots.is_empty())
            .then(|| roots.into_iter().map(PathBuf::from).collect::<Vec<_>>())
    });
    let reference_context = reference_context
        .map(artifact_ref_context_from_pydict)
        .transpose()?;
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_doctor_report_with_contexts(
                &beads_dir,
                roots.as_deref(),
                reference_context.as_ref(),
            )
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_get_epic_children")]
fn py_bead_get_epic_children<'py>(
    py: Python<'py>,
    beads_dir: &str,
    epic_id: &str,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| core_bead_get_epic_children(&beads_dir, epic_id)),
    )
}

#[pyfunction]
#[pyo3(name = "bead_init_store")]
fn py_bead_init_store<'py>(
    py: Python<'py>,
    root_dir: &str,
    beads_dirname: &str,
    issue_prefix: &str,
    owner: &str,
) -> PyResult<PyObject> {
    let root_dir = PathBuf::from(root_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_init_store(&root_dir, beads_dirname, issue_prefix, owner)
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_create")]
fn py_bead_create<'py>(
    py: Python<'py>,
    beads_dir: &str,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    let request = bead_create_request_from_pydict(request)?;
    bead_result_to_py(
        py,
        py.allow_threads(|| core_bead_create_issue(&beads_dir, request)),
    )
}

#[pyfunction]
#[pyo3(name = "bead_update")]
fn py_bead_update<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_id: &str,
    fields: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    let fields = bead_update_fields_from_pydict(fields)?;
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_update_issue(&beads_dir, issue_id, fields)
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_update_many")]
fn py_bead_update_many<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_ids: Vec<String>,
    fields: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    let fields = bead_update_fields_from_pydict(fields)?;
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_update_issues(&beads_dir, &issue_ids, fields)
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_append_note")]
#[pyo3(signature = (beads_dir, issue_id, entry, author=None, now=None))]
fn py_bead_append_note<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_id: &str,
    entry: &str,
    author: Option<String>,
    now: Option<String>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_append_issue_note(
                &beads_dir, issue_id, entry, author, now,
            )
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_plus_one", signature = (beads_dir, issue_id, reporter, note, refs=None, now=None, observed_since=None))]
// The argument list mirrors the exported Python binding signature; grouping it
// locally would add a wrapper type the caller could not use directly.
#[allow(clippy::too_many_arguments)]
fn py_bead_plus_one<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_id: &str,
    reporter: &str,
    note: &str,
    refs: Option<Vec<String>>,
    now: Option<String>,
    observed_since: Option<String>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    let refs = refs.unwrap_or_default();
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_add_task_plus_one(
                &beads_dir,
                issue_id,
                reporter,
                note,
                &refs,
                now,
                observed_since,
            )
        }),
    )
}

#[pyfunction]
#[pyo3(
    name = "bead_snooze",
    signature = (beads_dir, issue_id, until, plus_ones=None, reason="", actor="", now=None)
)]
// The argument list mirrors `snooze_task`'s signature exactly; bundling it
// into a struct here would only move the same fields behind a wire type the
// Python caller would have to build anyway.
#[allow(clippy::too_many_arguments)]
fn py_bead_snooze<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_id: &str,
    until: &str,
    plus_ones: Option<u32>,
    reason: &str,
    actor: &str,
    now: Option<String>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_snooze_task(
                &beads_dir, issue_id, until, plus_ones, reason, actor, now,
            )
        }),
    )
}

#[pyfunction]
#[pyo3(
    name = "bead_snooze_cancel",
    signature = (beads_dir, issue_id, actor="", now=None)
)]
fn py_bead_snooze_cancel<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_id: &str,
    actor: &str,
    now: Option<String>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_cancel_task_snooze(&beads_dir, issue_id, actor, now)
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_claim_for_agent_launch", signature = (beads_dir, bead_id, agent_name, now=None))]
fn py_bead_claim_for_agent_launch<'py>(
    py: Python<'py>,
    beads_dir: &str,
    bead_id: &str,
    agent_name: &str,
    now: Option<String>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_claim_for_agent_launch(
                &beads_dir, bead_id, agent_name, now,
            )
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_claim_for_agent_wait", signature = (beads_dir, bead_id, agent_name, now=None))]
fn py_bead_claim_for_agent_wait<'py>(
    py: Python<'py>,
    beads_dir: &str,
    bead_id: &str,
    agent_name: &str,
    now: Option<String>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_claim_for_agent_wait(&beads_dir, bead_id, agent_name, now)
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_release_agent_claim", signature = (beads_dir, bead_id, agent_name, now=None))]
fn py_bead_release_agent_claim<'py>(
    py: Python<'py>,
    beads_dir: &str,
    bead_id: &str,
    agent_name: &str,
    now: Option<String>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_release_agent_claim(&beads_dir, bead_id, agent_name, now)
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_preclaim_epic_work", signature = (beads_dir, epic_id, assignments, epic_agent_name=None, now=None))]
fn py_bead_preclaim_epic_work<'py>(
    py: Python<'py>,
    beads_dir: &str,
    epic_id: &str,
    assignments: &Bound<'py, PyList>,
    epic_agent_name: Option<String>,
    now: Option<String>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    let assignments = bead_preclaim_assignments_from_py_list(assignments)?;
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_preclaim_epic_work_plan(
                &beads_dir,
                epic_id,
                &assignments,
                epic_agent_name,
                now,
            )
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_open")]
#[pyo3(signature = (beads_dir, issue_id, now=None))]
fn py_bead_open<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_id: &str,
    now: Option<String>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| core_bead_open_issue(&beads_dir, issue_id, now)),
    )
}

#[pyfunction]
#[pyo3(name = "bead_close")]
#[pyo3(signature = (
    beads_dir,
    issue_ids,
    reason=None,
    resolution=None,
    force=false,
    now=None,
    note=None,
    author=None,
))]
#[allow(clippy::too_many_arguments)]
fn py_bead_close<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_ids: Vec<String>,
    reason: Option<String>,
    resolution: Option<String>,
    force: bool,
    now: Option<String>,
    note: Option<String>,
    author: Option<String>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    let resolution = resolution
        .as_deref()
        .map(parse_bead_resolution)
        .transpose()?;
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_close_issues_with_note(
                &beads_dir, &issue_ids, reason, resolution, force, note,
                author, now,
            )
        }),
    )
}

fn parse_bead_resolution(value: &str) -> PyResult<BeadResolutionWire> {
    match value {
        "done" => Ok(BeadResolutionWire::Done),
        "canceled" => Ok(BeadResolutionWire::Canceled),
        "superseded" => Ok(BeadResolutionWire::Superseded),
        _ => Err(PyValueError::new_err(format!(
            "invalid bead resolution: {value}"
        ))),
    }
}

#[pyfunction]
#[pyo3(name = "bead_merge_event_streams")]
fn py_bead_merge_event_streams<'py>(
    py: Python<'py>,
    base: &Bound<'py, PyDict>,
    ours: &Bound<'py, PyDict>,
    theirs: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let base = bead_event_stream_from_pydict(base, "base")?;
    let ours = bead_event_stream_from_pydict(ours, "ours")?;
    let theirs = bead_event_stream_from_pydict(theirs, "theirs")?;
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_merge_bead_event_streams(&base, &ours, &theirs)
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_merge_event_streams_with_relocation")]
#[pyo3(signature = (base, ours, theirs, relocation_issue_id=None))]
fn py_bead_merge_event_streams_with_relocation<'py>(
    py: Python<'py>,
    base: &Bound<'py, PyDict>,
    ours: &Bound<'py, PyDict>,
    theirs: &Bound<'py, PyDict>,
    relocation_issue_id: Option<String>,
) -> PyResult<PyObject> {
    let base = bead_event_stream_from_pydict(base, "base")?;
    let ours = bead_event_stream_from_pydict(ours, "ours")?;
    let theirs = bead_event_stream_from_pydict(theirs, "theirs")?;
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_merge_bead_event_streams_with_relocation(
                &base,
                &ours,
                &theirs,
                relocation_issue_id.as_deref(),
            )
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_reduce_event_streams")]
fn py_bead_reduce_event_streams<'py>(
    py: Python<'py>,
    streams: &Bound<'py, PyList>,
) -> PyResult<PyObject> {
    let streams = bead_event_streams_from_py_list(streams)?;
    bead_result_to_py(
        py,
        py.allow_threads(|| core_reduce_event_streams(&streams)),
    )
}

#[pyfunction]
#[pyo3(name = "bead_event_store_manifest")]
fn py_bead_event_store_manifest<'py>(
    py: Python<'py>,
    streams: &Bound<'py, PyList>,
) -> PyResult<PyObject> {
    let streams = bead_event_streams_from_py_list(streams)?;
    let manifest = BeadEventStoreManifestWire::from_streams(&streams);
    bead_result_to_py(py, Ok(manifest))
}

#[pyfunction]
#[pyo3(name = "bead_repair_event_store_manifest")]
fn py_bead_repair_event_store_manifest<'py>(
    py: Python<'py>,
    beads_dir: &str,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| core_repair_event_store_manifest(&beads_dir)),
    )
}

#[pyfunction]
#[pyo3(name = "bead_remove")]
fn py_bead_remove<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_id: &str,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| core_bead_remove_issue(&beads_dir, issue_id)),
    )
}

#[pyfunction]
#[pyo3(name = "bead_remove_many")]
fn py_bead_remove_many<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_ids: Vec<String>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| core_bead_remove_issues(&beads_dir, &issue_ids)),
    )
}

#[pyfunction]
#[pyo3(name = "bead_dep_add")]
#[pyo3(signature = (beads_dir, issue_id, depends_on_id, now=None))]
fn py_bead_dep_add<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_id: &str,
    depends_on_id: &str,
    now: Option<String>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_add_dependency(&beads_dir, issue_id, depends_on_id, now)
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_dep_remove")]
#[pyo3(signature = (beads_dir, issue_id, depends_on_ids, now=None))]
fn py_bead_dep_remove<'py>(
    py: Python<'py>,
    beads_dir: &str,
    issue_id: &str,
    depends_on_ids: Vec<String>,
    now: Option<String>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_remove_dependencies(
                &beads_dir,
                issue_id,
                &depends_on_ids,
                now,
            )
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_mark_ready_to_work")]
#[pyo3(signature = (beads_dir, epic_id, now=None))]
fn py_bead_mark_ready_to_work<'py>(
    py: Python<'py>,
    beads_dir: &str,
    epic_id: &str,
    now: Option<String>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_mark_ready_to_work(&beads_dir, epic_id, now)
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_unmark_ready_to_work")]
#[pyo3(signature = (beads_dir, epic_id, now=None))]
fn py_bead_unmark_ready_to_work<'py>(
    py: Python<'py>,
    beads_dir: &str,
    epic_id: &str,
    now: Option<String>,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_unmark_ready_to_work(&beads_dir, epic_id, now)
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_export_jsonl")]
fn py_bead_export_jsonl<'py>(
    py: Python<'py>,
    beads_dir: &str,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| core_bead_export_jsonl(&beads_dir)),
    )
}

#[pyfunction]
#[pyo3(name = "bead_sync_is_clean")]
fn py_bead_sync_is_clean(beads_dir: &str) -> PyResult<bool> {
    let beads_dir = PathBuf::from(beads_dir);
    core_bead_sync_is_clean(&beads_dir).map_err(bead_error_to_pyerr)
}

#[pyfunction]
#[pyo3(name = "bead_build_epic_work_plan")]
fn py_bead_build_epic_work_plan<'py>(
    py: Python<'py>,
    beads_dir: &str,
    epic_id: &str,
) -> PyResult<PyObject> {
    let beads_dir = PathBuf::from(beads_dir);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_build_epic_work_plan(&beads_dir, epic_id)
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_build_epic_work_plan_from_issues")]
fn py_bead_build_epic_work_plan_from_issues<'py>(
    py: Python<'py>,
    issues: &Bound<'py, PyList>,
    epic_id: &str,
) -> PyResult<PyObject> {
    let issues = issues_from_py_list(issues)?;
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_bead_build_epic_work_plan_from_issues(issues, epic_id)
        }),
    )
}

#[pyfunction]
#[pyo3(name = "bead_cli_execute")]
#[pyo3(signature = (
    argv,
    read_beads_dirs,
    write_beads_dir,
    cwd,
    relativize_design_paths,
    plan_roots = Vec::new(),
))]
fn py_bead_cli_execute<'py>(
    py: Python<'py>,
    argv: Vec<String>,
    read_beads_dirs: Vec<String>,
    write_beads_dir: &str,
    cwd: &str,
    relativize_design_paths: bool,
    plan_roots: Vec<String>,
) -> PyResult<PyObject> {
    let read_beads_dirs = strings_to_paths(read_beads_dirs);
    let write_beads_dir = PathBuf::from(write_beads_dir);
    let cwd = PathBuf::from(cwd);
    let plan_roots = strings_to_paths(plan_roots);
    bead_result_to_py(
        py,
        py.allow_threads(|| {
            core_execute_bead_cli(
                &argv,
                &read_beads_dirs,
                &write_beads_dir,
                &cwd,
                relativize_design_paths,
                &plan_roots,
            )
        }),
    )
}

fn strings_to_paths(paths: Vec<String>) -> Vec<PathBuf> {
    paths.into_iter().map(PathBuf::from).collect()
}

#[pyfunction]
#[pyo3(name = "resolve_agent_family_parent")]
fn py_resolve_agent_family_parent<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let request: AgentFamilyParentResolutionRequestWire =
        serde_json::from_value(value).map_err(|e| {
            PyValueError::new_err(format!(
                "request is not a valid AgentFamilyParentResolutionRequestWire dict: {e}"
            ))
        })?;
    let result = py
        .allow_threads(|| core_resolve_agent_family_parent(request))
        .map_err(PyValueError::new_err)?;
    let value = serde_json::to_value(result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

#[pyfunction]
#[pyo3(name = "resolve_clan_summary")]
fn py_resolve_clan_summary<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let request: ClanTribeResolutionRequestWire =
        serde_json::from_value(value).map_err(|e| {
            PyValueError::new_err(format!(
                "request is not a valid ClanTribeResolutionRequestWire dict: {e}"
            ))
        })?;
    let result = py.allow_threads(|| core_resolve_clan_summary(request));
    let value = serde_json::to_value(result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

#[pyfunction]
#[pyo3(name = "resolve_clan_tribe")]
fn py_resolve_clan_tribe<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let request: ClanTribeResolutionRequestWire =
        serde_json::from_value(value).map_err(|e| {
            PyValueError::new_err(format!(
                "request is not a valid ClanTribeResolutionRequestWire dict: {e}"
            ))
        })?;
    let result = py.allow_threads(|| core_resolve_clan_tribe(request));
    let value = serde_json::to_value(result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

fn bead_result_to_py<'py, T>(
    py: Python<'py>,
    result: Result<T, BeadError>,
) -> PyResult<PyObject>
where
    T: serde::Serialize,
{
    let value = serde_json::to_value(result.map_err(bead_error_to_pyerr)?)
        .map_err(|e| {
            PyValueError::new_err(format!("internal serialize error: {e}"))
        })?;
    json_value_to_py(py, &value)
}

fn plan_result_to_py<'py, T>(
    py: Python<'py>,
    result: Result<T, PlanError>,
) -> PyResult<PyObject>
where
    T: serde::Serialize,
{
    let value = serde_json::to_value(result.map_err(plan_error_to_pyerr)?)
        .map_err(|e| {
            PyValueError::new_err(format!("internal serialize error: {e}"))
        })?;
    json_value_to_py(py, &value)
}

fn plan_error_to_pyerr(err: PlanError) -> PyErr {
    PyValueError::new_err(format!("{err}"))
}

fn artifact_ref_result_to_py<'py, T>(
    py: Python<'py>,
    result: Result<T, ArtifactRefError>,
) -> PyResult<PyObject>
where
    T: serde::Serialize,
{
    let value =
        serde_json::to_value(result.map_err(artifact_ref_error_to_pyerr)?)
            .map_err(|error| {
                PyValueError::new_err(format!(
                    "internal artifact reference serialize error: {error}"
                ))
            })?;
    json_value_to_py(py, &value)
}

fn artifact_ref_error_to_pyerr(error: ArtifactRefError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

fn artifact_file_query_error_to_pyerr(error: ArtifactFileQueryError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

fn artifact_file_lifecycle_value_to_py<'py, T: serde::Serialize>(
    py: Python<'py>,
    value: &T,
    operation: &str,
) -> PyResult<PyObject> {
    let value = serde_json::to_value(value).map_err(|error| {
        PyValueError::new_err(format!(
            "internal artifact-file {operation} serialize error: {error}"
        ))
    })?;
    json_value_to_py(py, &value)
}

fn artifact_ref_from_py(
    value: &Bound<'_, PyAny>,
) -> PyResult<ParsedArtifactRefWire> {
    if let Ok(reference) = value.extract::<String>() {
        return core_parse_artifact_ref(&reference)
            .map_err(artifact_ref_error_to_pyerr);
    }
    serde_json::from_value(py_to_json_value(value)?).map_err(|error| {
        PyValueError::new_err(format!(
            "reference is not a valid ParsedArtifactRefWire dict: {error}"
        ))
    })
}

fn artifact_ref_context_from_pydict(
    context: &Bound<'_, PyDict>,
) -> PyResult<ArtifactRefContextWire> {
    serde_json::from_value(py_to_json_value(context.as_any())?).map_err(
        |error| {
            PyValueError::new_err(format!(
                "context is not a valid ArtifactRefContextWire dict: {error}"
            ))
        },
    )
}

fn bead_create_request_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<BeadCreateRequestWire> {
    let value = py_to_json_value(dict.as_any())?;
    serde_json::from_value(value).map_err(|e| {
        PyValueError::new_err(format!(
            "request is not a valid BeadCreateRequestWire dict: {e}"
        ))
    })
}

fn bead_update_fields_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<BeadUpdateFieldsWire> {
    let value = py_to_json_value(dict.as_any())?;
    serde_json::from_value(value).map_err(|e| {
        PyValueError::new_err(format!(
            "fields is not a valid BeadUpdateFieldsWire dict: {e}"
        ))
    })
}

fn bead_preclaim_assignments_from_py_list(
    list: &Bound<'_, PyList>,
) -> PyResult<Vec<BeadPreclaimAssignmentWire>> {
    let mut values = Vec::with_capacity(list.len());
    for (idx, item) in list.iter().enumerate() {
        let value = py_to_json_value(&item)?;
        let assignment: BeadPreclaimAssignmentWire =
            serde_json::from_value(value).map_err(|e| {
                PyValueError::new_err(format!(
                    "assignments[{idx}] is not a valid BeadPreclaimAssignmentWire dict: {e}"
                ))
            })?;
        values.push(assignment);
    }
    Ok(values)
}

fn issues_from_py_list(list: &Bound<'_, PyList>) -> PyResult<Vec<IssueWire>> {
    let mut values = Vec::with_capacity(list.len());
    for (idx, item) in list.iter().enumerate() {
        let value = py_to_json_value(&item)?;
        let issue: IssueWire = serde_json::from_value(value).map_err(|e| {
            PyValueError::new_err(format!(
                "issues[{idx}] is not a valid IssueWire dict: {e}"
            ))
        })?;
        issue.validate().map_err(|e| {
            PyValueError::new_err(format!("issues[{idx}] is invalid: {e}"))
        })?;
        values.push(issue);
    }
    Ok(values)
}

fn bead_event_stream_from_pydict(
    dict: &Bound<'_, PyDict>,
    label: &str,
) -> PyResult<BeadEventStreamWire> {
    let value = py_to_json_value(dict.as_any())?;
    let stream: BeadEventStreamWire =
        serde_json::from_value(value).map_err(|e| {
            PyValueError::new_err(format!(
                "{label} is not a valid BeadEventStreamWire dict: {e}"
            ))
        })?;
    stream.validate().map_err(bead_error_to_pyerr)?;
    Ok(stream)
}

fn bead_event_streams_from_py_list(
    list: &Bound<'_, PyList>,
) -> PyResult<Vec<BeadEventStreamWire>> {
    let mut values = Vec::with_capacity(list.len());
    for (idx, item) in list.iter().enumerate() {
        let value = py_to_json_value(&item)?;
        let stream: BeadEventStreamWire =
            serde_json::from_value(value).map_err(|e| {
                PyValueError::new_err(format!(
                    "streams[{idx}] is not a valid BeadEventStreamWire dict: {e}"
                ))
            })?;
        stream.validate().map_err(bead_error_to_pyerr)?;
        values.push(stream);
    }
    Ok(values)
}

// --- Notification store bindings -----------------------------------------

/// Read the notification JSONL store and return a snapshot dict.
///
/// The GIL is released while Rust performs filesystem work. When
/// ``expire_due_snoozes`` is true, due snoozes are expired under the same
/// store lock before the returned snapshot is built.
#[pyfunction]
#[pyo3(name = "read_notifications_snapshot", signature = (path, include_dismissed, expire_due_snoozes = false))]
fn py_read_notifications_snapshot<'py>(
    py: Python<'py>,
    path: &str,
    include_dismissed: bool,
    expire_due_snoozes: bool,
) -> PyResult<PyObject> {
    let path = PathBuf::from(path);
    let snapshot = py.allow_threads(|| {
        core_read_notifications_snapshot_with_options(
            &path,
            include_dismissed,
            expire_due_snoozes,
        )
    });
    let value = serde_json::to_value(snapshot.map_err(PyValueError::new_err)?)
        .map_err(|e| {
            PyValueError::new_err(format!("internal serialize error: {e}"))
        })?;
    json_value_to_py(py, &value)
}

/// Read and reconcile the user-facing current notification state.
#[pyfunction]
#[pyo3(name = "read_current_notifications_snapshot")]
fn py_read_current_notifications_snapshot<'py>(
    py: Python<'py>,
    path: &str,
    include_dismissed: bool,
) -> PyResult<PyObject> {
    let path = PathBuf::from(path);
    let snapshot = py.allow_threads(|| {
        core_read_current_notifications_snapshot(&path, include_dismissed)
    });
    let value = serde_json::to_value(snapshot.map_err(PyValueError::new_err)?)
        .map_err(|e| {
            PyValueError::new_err(format!("internal serialize error: {e}"))
        })?;
    json_value_to_py(py, &value)
}

/// Apply one notification state update and return the outcome dict.
#[pyfunction]
#[pyo3(name = "apply_notification_state_update")]
fn py_apply_notification_state_update<'py>(
    py: Python<'py>,
    path: &str,
    update: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let update = notification_update_from_pydict(update)?;
    let path = PathBuf::from(path);
    let outcome = py
        .allow_threads(|| core_apply_notification_state_update(&path, &update));
    let value = serde_json::to_value(outcome.map_err(PyValueError::new_err)?)
        .map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Apply one notification state update and return only mutation metadata.
#[pyfunction]
#[pyo3(name = "apply_notification_state_update_counts")]
fn py_apply_notification_state_update_counts<'py>(
    py: Python<'py>,
    path: &str,
    update: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let update = notification_update_from_pydict(update)?;
    let path = PathBuf::from(path);
    let outcome = py.allow_threads(|| {
        core_apply_notification_state_update_counts(&path, &update)
    });
    let value = serde_json::to_value(outcome.map_err(PyValueError::new_err)?)
        .map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Append one notification dict and return the outcome dict.
#[pyfunction]
#[pyo3(name = "append_notification")]
fn py_append_notification<'py>(
    py: Python<'py>,
    path: &str,
    notification: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let notification = notification_from_pydict(notification)?;
    let path = PathBuf::from(path);
    let outcome =
        py.allow_threads(|| core_append_notification(&path, &notification));
    let value = serde_json::to_value(outcome.map_err(PyValueError::new_err)?)
        .map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Append one notification dict and return only mutation metadata.
#[pyfunction]
#[pyo3(name = "append_notification_counts")]
fn py_append_notification_counts<'py>(
    py: Python<'py>,
    path: &str,
    notification: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let notification = notification_from_pydict(notification)?;
    let path = PathBuf::from(path);
    let outcome = py.allow_threads(|| {
        core_append_notification_counts(&path, &notification)
    });
    let value = serde_json::to_value(outcome.map_err(PyValueError::new_err)?)
        .map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Rewrite the notification JSONL store from notification dicts.
#[pyfunction]
#[pyo3(name = "rewrite_notifications")]
fn py_rewrite_notifications<'py>(
    py: Python<'py>,
    path: &str,
    notifications: &Bound<'py, PyList>,
) -> PyResult<PyObject> {
    let notifications = notifications_from_py_list(notifications)?;
    let path = PathBuf::from(path);
    let outcome =
        py.allow_threads(|| core_rewrite_notifications(&path, &notifications));
    let value = serde_json::to_value(outcome.map_err(PyValueError::new_err)?)
        .map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Rewrite the notification JSONL store and return only mutation metadata.
#[pyfunction]
#[pyo3(name = "rewrite_notifications_counts")]
fn py_rewrite_notifications_counts<'py>(
    py: Python<'py>,
    path: &str,
    notifications: &Bound<'py, PyList>,
) -> PyResult<PyObject> {
    let notifications = notifications_from_py_list(notifications)?;
    let path = PathBuf::from(path);
    let outcome = py.allow_threads(|| {
        core_rewrite_notifications_counts(&path, &notifications)
    });
    let value = serde_json::to_value(outcome.map_err(PyValueError::new_err)?)
        .map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Classify notification dicts into ordered tabs and per-row tab keys.
///
/// One call classifies a whole page, so callers never pay one FFI hop per row.
#[pyfunction]
#[pyo3(name = "classify_notification_tabs")]
fn py_classify_notification_tabs<'py>(
    py: Python<'py>,
    notifications: &Bound<'py, PyList>,
) -> PyResult<PyObject> {
    let notifications = notifications_from_py_list(notifications)?;
    let classification =
        py.allow_threads(|| core_classify_notification_tabs(&notifications));
    let value = serde_json::to_value(classification).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

// --- Prompt stash store bindings -----------------------------------------

fn prompt_stash_error_to_pyerr(error: PromptStashStoreError) -> PyErr {
    match error {
        error @ PromptStashStoreError::LockTimeout { .. } => {
            PyTimeoutError::new_err(error.to_string())
        }
        error => PyValueError::new_err(error.to_string()),
    }
}

/// Read the prompt-stash JSONL store and return a snapshot dict.
///
/// The GIL is released while Rust performs filesystem work.
#[pyfunction]
#[pyo3(name = "read_prompt_stash_snapshot")]
fn py_read_prompt_stash_snapshot(
    py: Python<'_>,
    path: &str,
) -> PyResult<PyObject> {
    let path = PathBuf::from(path);
    let snapshot = py.allow_threads(|| core_read_prompt_stash_snapshot(&path));
    let value =
        serde_json::to_value(snapshot.map_err(prompt_stash_error_to_pyerr)?)
            .map_err(|e| {
                PyValueError::new_err(format!("internal serialize error: {e}"))
            })?;
    json_value_to_py(py, &value)
}

/// Append one prompt-stash entry dict and return the updated snapshot dict.
#[pyfunction]
#[pyo3(name = "append_prompt_stash")]
fn py_append_prompt_stash<'py>(
    py: Python<'py>,
    path: &str,
    entry: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let entry = prompt_stash_entry_from_pydict(entry)?;
    let path = PathBuf::from(path);
    let snapshot = py.allow_threads(|| core_append_prompt_stash(&path, &entry));
    let value =
        serde_json::to_value(snapshot.map_err(prompt_stash_error_to_pyerr)?)
            .map_err(|e| {
                PyValueError::new_err(format!("internal serialize error: {e}"))
            })?;
    json_value_to_py(py, &value)
}

/// Remove entries whose ids appear in `ids`; return removed rows + snapshot.
#[pyfunction]
#[pyo3(name = "pop_prompt_stash")]
fn py_pop_prompt_stash(
    py: Python<'_>,
    path: &str,
    ids: Vec<String>,
) -> PyResult<PyObject> {
    let path = PathBuf::from(path);
    let outcome = py.allow_threads(|| core_pop_prompt_stash(&path, &ids));
    let value =
        serde_json::to_value(outcome.map_err(prompt_stash_error_to_pyerr)?)
            .map_err(|e| {
                PyValueError::new_err(format!("internal serialize error: {e}"))
            })?;
    json_value_to_py(py, &value)
}

/// Set the persisted pin flag for entries whose ids appear in `ids`.
#[pyfunction]
#[pyo3(name = "set_prompt_stash_pinned")]
fn py_set_prompt_stash_pinned(
    py: Python<'_>,
    path: &str,
    ids: Vec<String>,
    pinned: bool,
) -> PyResult<PyObject> {
    let path = PathBuf::from(path);
    let snapshot =
        py.allow_threads(|| core_set_prompt_stash_pinned(&path, &ids, pinned));
    let value =
        serde_json::to_value(snapshot.map_err(prompt_stash_error_to_pyerr)?)
            .map_err(|e| {
                PyValueError::new_err(format!("internal serialize error: {e}"))
            })?;
    json_value_to_py(py, &value)
}

/// Rewrite the prompt-stash store from entry dicts (merge semantics).
#[pyfunction]
#[pyo3(name = "rewrite_prompt_stash")]
fn py_rewrite_prompt_stash<'py>(
    py: Python<'py>,
    path: &str,
    entries: &Bound<'py, PyList>,
) -> PyResult<PyObject> {
    let entries = prompt_stash_entries_from_py_list(entries)?;
    let path = PathBuf::from(path);
    let snapshot =
        py.allow_threads(|| core_rewrite_prompt_stash(&path, &entries));
    let value =
        serde_json::to_value(snapshot.map_err(prompt_stash_error_to_pyerr)?)
            .map_err(|e| {
                PyValueError::new_err(format!("internal serialize error: {e}"))
            })?;
    json_value_to_py(py, &value)
}

fn prompt_stash_entry_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<PromptStashEntryWire> {
    let value = py_to_json_value(dict.as_any())?;
    serde_json::from_value(value).map_err(|e| {
        PyValueError::new_err(format!(
            "entry is not a valid PromptStashEntryWire dict: {e}"
        ))
    })
}

fn prompt_stash_entries_from_py_list(
    list: &Bound<'_, PyList>,
) -> PyResult<Vec<PromptStashEntryWire>> {
    let mut values = Vec::with_capacity(list.len());
    for (idx, item) in list.iter().enumerate() {
        let value = py_to_json_value(&item)?;
        let entry: PromptStashEntryWire =
            serde_json::from_value(value).map_err(|e| {
                PyValueError::new_err(format!(
                    "entries[{idx}] is not a valid PromptStashEntryWire dict: {e}"
                ))
            })?;
        values.push(entry);
    }
    Ok(values)
}

// --- Background-task store bindings -------------------------------------

fn task_store_error_to_pyerr(error: TaskStoreError) -> PyErr {
    match error {
        error @ TaskStoreError::LockTimeout { .. } => {
            PyTimeoutError::new_err(error.to_string())
        }
        error => PyValueError::new_err(error.to_string()),
    }
}

/// Read the background-task JSONL store and return a snapshot dict.
#[pyfunction]
#[pyo3(name = "read_tasks_snapshot")]
fn py_read_tasks_snapshot(py: Python<'_>, path: &str) -> PyResult<PyObject> {
    let path = PathBuf::from(path);
    let snapshot = py.allow_threads(|| core_read_tasks_snapshot(&path));
    task_store_result_to_py(py, &snapshot.map_err(task_store_error_to_pyerr)?)
}

/// Append one task dict, enforce retention, and return the outcome dict.
#[pyfunction]
#[pyo3(name = "append_task")]
fn py_append_task<'py>(
    py: Python<'py>,
    path: &str,
    task: &Bound<'py, PyDict>,
    history_limit: i64,
) -> PyResult<PyObject> {
    let task = background_task_from_pydict(task)?;
    let path = PathBuf::from(path);
    let outcome =
        py.allow_threads(|| core_append_task(&path, &task, history_limit));
    task_store_result_to_py(py, &outcome.map_err(task_store_error_to_pyerr)?)
}

/// Apply a partial task update and return its matched/task outcome dict.
#[pyfunction]
#[pyo3(name = "update_task")]
fn py_update_task<'py>(
    py: Python<'py>,
    path: &str,
    update: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let update = task_update_from_pydict(update)?;
    let path = PathBuf::from(path);
    let outcome = py.allow_threads(|| core_update_task(&path, &update));
    task_store_result_to_py(py, &outcome.map_err(task_store_error_to_pyerr)?)
}

/// Enforce terminal-task retention and return the fresh snapshot + pruned ids.
#[pyfunction]
#[pyo3(name = "prune_tasks")]
fn py_prune_tasks(
    py: Python<'_>,
    path: &str,
    history_limit: i64,
) -> PyResult<PyObject> {
    let path = PathBuf::from(path);
    let outcome = py.allow_threads(|| core_prune_tasks(&path, history_limit));
    task_store_result_to_py(py, &outcome.map_err(task_store_error_to_pyerr)?)
}

fn background_task_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<BackgroundTaskWire> {
    let value = py_to_json_value(dict.as_any())?;
    serde_json::from_value(value).map_err(|error| {
        PyValueError::new_err(format!(
            "task is not a valid BackgroundTaskWire dict: {error}"
        ))
    })
}

fn task_update_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<TaskUpdateWire> {
    let value = py_to_json_value(dict.as_any())?;
    serde_json::from_value(value).map_err(|error| {
        PyValueError::new_err(format!(
            "update is not a valid TaskUpdateWire dict: {error}"
        ))
    })
}

fn task_store_result_to_py<T>(py: Python<'_>, result: &T) -> PyResult<PyObject>
where
    T: serde::Serialize,
{
    let value = serde_json::to_value(result).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

/// Deserialize a `AgentArtifactScanOptionsWire` from a Python dict.
///
/// Translates the dict to `serde_json::Value` first so missing fields use
/// the Rust struct's serde defaults — this matches the Python facade's
/// "absent → default" behavior for callers who pass partial dicts.
fn agent_scan_options_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<AgentArtifactScanOptionsWire> {
    let value = py_to_json_value(dict.as_any())?;
    serde_json::from_value(value).map_err(|e| {
        PyValueError::new_err(format!(
            "options is not a valid AgentArtifactScanOptionsWire dict: {e}"
        ))
    })
}

fn clan_runtime_members_from_py_list(
    list: &Bound<'_, PyList>,
) -> PyResult<Vec<ClanRuntimeMemberWire>> {
    let mut members = Vec::with_capacity(list.len());
    for (index, item) in list.iter().enumerate() {
        let value = py_to_json_value(&item)?;
        let member = serde_json::from_value(value).map_err(|error| {
            PyValueError::new_err(format!(
                "members[{index}] is not a valid ClanRuntimeMemberWire dict: {error}"
            ))
        })?;
        members.push(member);
    }
    Ok(members)
}

fn notification_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<NotificationWire> {
    let value = py_to_json_value(dict.as_any())?;
    serde_json::from_value(value).map_err(|e| {
        PyValueError::new_err(format!(
            "notification is not a valid NotificationWire dict: {e}"
        ))
    })
}

fn notifications_from_py_list(
    list: &Bound<'_, PyList>,
) -> PyResult<Vec<NotificationWire>> {
    let mut values = Vec::with_capacity(list.len());
    for (idx, item) in list.iter().enumerate() {
        let value = py_to_json_value(&item)?;
        let notification: NotificationWire =
            serde_json::from_value(value).map_err(|e| {
                PyValueError::new_err(format!(
                    "notifications[{idx}] is not a valid NotificationWire dict: {e}"
                ))
            })?;
        values.push(notification);
    }
    Ok(values)
}

fn notification_update_from_pydict(
    dict: &Bound<'_, PyDict>,
) -> PyResult<NotificationStateUpdateWire> {
    let value = py_to_json_value(dict.as_any())?;
    serde_json::from_value(value).map_err(|e| {
        PyValueError::new_err(format!(
            "update is not a valid NotificationStateUpdateWire dict: {e}"
        ))
    })
}

fn query_error_to_pyerr(err: QueryErrorWire) -> PyErr {
    PyValueError::new_err(format!("{err}"))
}

fn bead_error_to_pyerr(err: BeadError) -> PyErr {
    PyValueError::new_err(format!("{err}"))
}

fn patches_from_py_list(
    specs: &Bound<'_, PyList>,
) -> PyResult<Vec<ChangeSpecWire>> {
    let mut wire_specs: Vec<ChangeSpecWire> = Vec::with_capacity(specs.len());
    for (idx, item) in specs.iter().enumerate() {
        let json = py_to_json_value(&item)?;
        let spec: ChangeSpecWire =
            serde_json::from_value(json).map_err(|e| {
                PyValueError::new_err(format!(
                    "specs[{idx}] is not a valid ChangeSpecWire/PatchWire-compatible dict: {e}"
                ))
            })?;
        wire_specs.push(spec);
    }
    Ok(wire_specs)
}

/// Convert a `QueryExprWire` into the Python rectangular wire shape.
///
/// Python's `QueryExprWire` always carries the same flat field set
/// regardless of `kind`; serde's tagged-union shape only emits the fields
/// relevant to a given variant. Translating here keeps the Python side
/// unchanged: it can `QueryExprWire(**dict)` directly.
fn expr_to_python_wire(expr: &QueryExprWire) -> JsonValue {
    let (kind, value, case_sensitive, is_es, is_ra, is_rp, prop_key, operands) =
        match expr {
            QueryExprWire::StringMatch {
                value,
                case_sensitive,
                is_error_suffix,
                is_running_agent,
                is_running_process,
            } => (
                "string",
                value.clone(),
                *case_sensitive,
                *is_error_suffix,
                *is_running_agent,
                *is_running_process,
                JsonValue::Null,
                JsonValue::Array(vec![]),
            ),
            QueryExprWire::PropertyMatch { key, value } => (
                "property",
                value.clone(),
                false,
                false,
                false,
                false,
                JsonValue::String(key.clone()),
                JsonValue::Array(vec![]),
            ),
            QueryExprWire::Not { operand } => (
                "not",
                String::new(),
                false,
                false,
                false,
                false,
                JsonValue::Null,
                JsonValue::Array(vec![expr_to_python_wire(operand)]),
            ),
            QueryExprWire::And { operands } => (
                "and",
                String::new(),
                false,
                false,
                false,
                false,
                JsonValue::Null,
                JsonValue::Array(
                    operands.iter().map(expr_to_python_wire).collect(),
                ),
            ),
            QueryExprWire::Or { operands } => (
                "or",
                String::new(),
                false,
                false,
                false,
                false,
                JsonValue::Null,
                JsonValue::Array(
                    operands.iter().map(expr_to_python_wire).collect(),
                ),
            ),
        };

    let mut obj = JsonMap::new();
    obj.insert("kind".into(), JsonValue::String(kind.into()));
    obj.insert("value".into(), JsonValue::String(value));
    obj.insert("case_sensitive".into(), JsonValue::Bool(case_sensitive));
    obj.insert("is_error_suffix".into(), JsonValue::Bool(is_es));
    obj.insert("is_running_agent".into(), JsonValue::Bool(is_ra));
    obj.insert("is_running_process".into(), JsonValue::Bool(is_rp));
    obj.insert("property_key".into(), prop_key);
    obj.insert("operands".into(), operands);
    JsonValue::Object(obj)
}

/// Convert a Python value (dict / list / str / number / bool / None) into
/// a `serde_json::Value`. Used to deserialize ChangeSpecWire dicts coming
/// in from the Python side of `evaluate_query_many`.
fn py_to_json_value(value: &Bound<'_, PyAny>) -> PyResult<JsonValue> {
    if value.is_none() {
        return Ok(JsonValue::Null);
    }
    if let Ok(b) = value.extract::<bool>() {
        return Ok(JsonValue::Bool(b));
    }
    if let Ok(i) = value.extract::<i64>() {
        return Ok(JsonValue::Number(i.into()));
    }
    if let Ok(u) = value.extract::<u64>() {
        return Ok(JsonValue::Number(u.into()));
    }
    if let Ok(f) = value.extract::<f64>() {
        return serde_json::Number::from_f64(f)
            .map(JsonValue::Number)
            .ok_or_else(|| {
                PyValueError::new_err(format!("non-finite float: {f}"))
            });
    }
    if let Ok(s) = value.extract::<String>() {
        return Ok(JsonValue::String(s));
    }
    if let Ok(list) = value.downcast::<PyList>() {
        let mut arr = Vec::with_capacity(list.len());
        for item in list.iter() {
            arr.push(py_to_json_value(&item)?);
        }
        return Ok(JsonValue::Array(arr));
    }
    if let Ok(tuple) = value.downcast::<PyTuple>() {
        let mut arr = Vec::with_capacity(tuple.len());
        for item in tuple.iter() {
            arr.push(py_to_json_value(&item)?);
        }
        return Ok(JsonValue::Array(arr));
    }
    if let Ok(dict) = value.downcast::<PyDict>() {
        let mut obj = JsonMap::with_capacity(dict.len());
        for (k, v) in dict.iter() {
            let key: String = k.extract().map_err(|_| {
                PyValueError::new_err("dict keys must be strings")
            })?;
            obj.insert(key, py_to_json_value(&v)?);
        }
        return Ok(JsonValue::Object(obj));
    }
    Err(PyValueError::new_err(format!(
        "unsupported value of type {}",
        value.get_type().name()?
    )))
}

fn strings_from_py_list(
    list: &Bound<'_, PyList>,
    label: &str,
) -> PyResult<Vec<String>> {
    let mut values = Vec::with_capacity(list.len());
    for (idx, item) in list.iter().enumerate() {
        values.push(item.extract::<String>().map_err(|_| {
            PyValueError::new_err(format!("{label}[{idx}] must be a string"))
        })?);
    }
    Ok(values)
}

fn hooks_from_py(list: &Bound<'_, PyList>) -> PyResult<Vec<HookWire>> {
    let mut values = Vec::with_capacity(list.len());
    for (idx, item) in list.iter().enumerate() {
        values.push(serde_json::from_value(py_to_json_value(&item)?).map_err(
            |e| {
                PyValueError::new_err(format!(
                    "hooks[{idx}] is not a valid HookWire dict: {e}"
                ))
            },
        )?);
    }
    Ok(values)
}

fn mentors_from_py(list: &Bound<'_, PyList>) -> PyResult<Vec<MentorWire>> {
    let mut values = Vec::with_capacity(list.len());
    for (idx, item) in list.iter().enumerate() {
        values.push(serde_json::from_value(py_to_json_value(&item)?).map_err(
            |e| {
                PyValueError::new_err(format!(
                    "mentors[{idx}] is not a valid MentorWire dict: {e}"
                ))
            },
        )?);
    }
    Ok(values)
}

fn comments_from_py(list: &Bound<'_, PyList>) -> PyResult<Vec<CommentWire>> {
    let mut values = Vec::with_capacity(list.len());
    for (idx, item) in list.iter().enumerate() {
        values.push(serde_json::from_value(py_to_json_value(&item)?).map_err(
            |e| {
                PyValueError::new_err(format!(
                    "comments[{idx}] is not a valid CommentWire dict: {e}"
                ))
            },
        )?);
    }
    Ok(values)
}

fn json_value_to_py<'py>(
    py: Python<'py>,
    value: &JsonValue,
) -> PyResult<PyObject> {
    match value {
        JsonValue::Null => Ok(py.None()),
        JsonValue::Bool(b) => Ok(b.into_py(py)),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_py(py))
            } else if let Some(u) = n.as_u64() {
                Ok(u.into_py(py))
            } else if let Some(f) = n.as_f64() {
                Ok(f.into_py(py))
            } else {
                // Should be unreachable for serde_json numbers.
                Err(PyValueError::new_err(format!(
                    "unrepresentable JSON number: {n}"
                )))
            }
        }
        JsonValue::String(s) => Ok(s.into_py(py)),
        JsonValue::Array(arr) => json_array_to_py(py, arr),
        JsonValue::Object(obj) => json_object_to_py(py, obj),
    }
}

fn json_array_to_py<'py>(
    py: Python<'py>,
    arr: &[JsonValue],
) -> PyResult<PyObject> {
    let list = PyList::empty_bound(py);
    for v in arr {
        list.append(json_value_to_py(py, v)?)?;
    }
    Ok(list.into())
}

fn json_object_to_py<'py>(
    py: Python<'py>,
    obj: &JsonMap<String, JsonValue>,
) -> PyResult<PyObject> {
    let dict = PyDict::new_bound(py);
    for (k, v) in obj {
        dict.set_item(k, json_value_to_py(py, v)?)?;
    }
    Ok(dict.into())
}

// --- Prompt frontmatter panel: schema & validation surface ---
//
// These four bindings expose the panel-oriented frontmatter API from
// `sase_core::editor`. They are the single source of truth the TUI prompt
// frontmatter panel shares with the xprompt LSP, so panel guidance and editor
// diagnostics never drift. Results are serialized through `serde_json` and
// rehydrated as plain dicts/lists on the Python side (see
// `sase.xprompt.frontmatter_schema`).

/// Return the ordered panel frontmatter field schema as a list of dicts.
#[pyfunction]
#[pyo3(name = "frontmatter_field_schema")]
fn py_frontmatter_field_schema(py: Python<'_>) -> PyResult<PyObject> {
    let schema = sase_core::editor_frontmatter_field_schema();
    let value = serde_json::to_value(&schema).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Return the supported `input` type catalog as a list of dicts.
#[pyfunction]
#[pyo3(name = "frontmatter_input_type_schema")]
fn py_frontmatter_input_type_schema(py: Python<'_>) -> PyResult<PyObject> {
    let schema = sase_core::editor_frontmatter_input_type_schema();
    let value = serde_json::to_value(&schema).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Validate a whole frontmatter block. Returns LSP-shape diagnostic dicts.
#[pyfunction]
#[pyo3(name = "validate_frontmatter")]
fn py_validate_frontmatter(py: Python<'_>, text: &str) -> PyResult<PyObject> {
    let diagnostics = sase_core::editor_validate_frontmatter(text);
    let value = serde_json::to_value(&diagnostics).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Validate a single frontmatter field value. Returns LSP-shape diagnostic
/// dicts. `value` is the YAML text that would follow `field:`.
#[pyfunction]
#[pyo3(name = "validate_frontmatter_field")]
fn py_validate_frontmatter_field(
    py: Python<'_>,
    field: &str,
    value: &str,
) -> PyResult<PyObject> {
    let diagnostics =
        sase_core::editor_validate_frontmatter_field(field, value);
    let json = serde_json::to_value(&diagnostics).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &json)
}

// --- At-reference menu surface ------------------------------------------
//
// These bindings expose the core `@` reference context detector and grouped
// menu builder through plain JSON-shaped Python dict/list values.

/// Return `@` reference context at the cursor, or `None` when the cursor is
/// not inside a valid reference candidate.
#[pyfunction]
#[pyo3(name = "at_reference_context")]
#[pyo3(signature = (text, line, character, known_kinds = None))]
fn py_at_reference_context(
    py: Python<'_>,
    text: &str,
    line: u32,
    character: u32,
    known_kinds: Option<Vec<String>>,
) -> PyResult<PyObject> {
    let known_kinds = known_kinds.unwrap_or_default();
    let document = sase_core::DocumentSnapshot::new(text);
    let context = sase_core::editor_detect_at_reference_context(
        &document,
        sase_core::EditorPosition { line, character },
        &known_kinds,
    );
    let value = serde_json::to_value(&context).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Return grouped `@` reference menu rows for a detected context and caller
/// supplied inventory.
///
/// `payload_index`, when supplied, replaces `inventory["payloads"]` without
/// converting those rows through Python objects on each call.
#[pyfunction]
#[pyo3(name = "at_reference_menu")]
#[pyo3(signature = (context, inventory, payload_index = None, options = None))]
fn py_at_reference_menu(
    py: Python<'_>,
    context: Bound<'_, PyDict>,
    inventory: Bound<'_, PyDict>,
    payload_index: Option<PyRef<'_, PyAtReferenceInventory>>,
    options: Option<Bound<'_, PyDict>>,
) -> PyResult<PyObject> {
    let context = serde_json::from_value::<sase_core::AtReferenceContextWire>(
        py_to_json_value(context.as_any())?,
    )
    .map_err(|error| {
        PyValueError::new_err(format!(
            "context is not a valid AtReferenceContextWire dict: {error}"
        ))
    })?;
    let inventory =
        serde_json::from_value::<sase_core::AtReferenceInventoryWire>(
            py_to_json_value(inventory.as_any())?,
        )
        .map_err(|error| {
            PyValueError::new_err(format!(
                "inventory is not a valid AtReferenceInventoryWire dict: {error}"
            ))
        })?;
    let options = options
        .map(|options| {
            serde_json::from_value::<sase_core::AtReferenceMenuOptionsWire>(
                py_to_json_value(options.as_any())?,
            )
            .map_err(|error| {
                PyValueError::new_err(format!(
                    "options is not a valid AtReferenceMenuOptionsWire dict: {error}"
                ))
            })
        })
        .transpose()?
        .unwrap_or_default();
    let menu = sase_core::editor_build_at_reference_menu_with_options(
        &context,
        &inventory,
        payload_index
            .as_ref()
            .map(|payload_index| &payload_index.payloads),
        options,
    );
    let value = serde_json::to_value(&menu).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Return the shared artifact-reference payload inventory for one kind.
#[pyfunction]
#[pyo3(name = "artifact_ref_payload_inventory")]
fn py_artifact_ref_payload_inventory(
    py: Python<'_>,
    kind: &str,
    context: Bound<'_, PyDict>,
) -> PyResult<PyObject> {
    let context = artifact_ref_context_from_pydict(&context)?;
    let inventory =
        sase_core::editor_build_artifact_ref_payload_inventory(kind, &context)
            .map_err(artifact_ref_error_to_pyerr)?;
    let value = serde_json::to_value(&inventory).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Fuzzy-match a query against text using the shared editor matcher.
#[pyfunction]
#[pyo3(name = "fuzzy_match")]
fn py_fuzzy_match(
    py: Python<'_>,
    query: &str,
    text: &str,
) -> PyResult<PyObject> {
    let Some(match_result) = sase_core::editor_fuzzy_match(query, text) else {
        return Ok(py.None());
    };
    json_value_to_py(
        py,
        &serde_json::json!({
            "tier": match_result.tier,
            "score": match_result.score,
            "runs": match_result.runs,
        }),
    )
}

// --- Placeholder completion and highlighting surface ---------------------
//
// These bindings expose the same Rust placeholder engine consumed directly
// by the xprompt LSP. They are the single source of truth shared by the TUI
// and LSP for extraction, completion filtering, and replacement ranges.

/// Return placeholder completion context and candidates, or `None` when the
/// cursor is outside a placeholder or no reusable candidates exist.
///
/// `common` carries caller-ranked placeholders from a durable store. They are
/// emitted after the document's own candidates and tagged `"common"`.
#[pyfunction]
#[pyo3(name = "placeholder_completion")]
#[pyo3(signature = (text, line, character, common = None))]
fn py_placeholder_completion(
    py: Python<'_>,
    text: &str,
    line: u32,
    character: u32,
    common: Option<Vec<String>>,
) -> PyResult<PyObject> {
    let common = common.unwrap_or_default();
    let document = sase_core::DocumentSnapshot::new(text);
    let completion = sase_core::editor_build_placeholder_completion_candidates(
        &document,
        sase_core::EditorPosition { line, character },
        &common,
    )
    .filter(|completion| !completion.candidates.is_empty());
    let value = serde_json::to_value(&completion).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Return all complete placeholder spans for prompt highlighting.
#[pyfunction]
#[pyo3(name = "placeholder_spans")]
fn py_placeholder_spans(py: Python<'_>, text: &str) -> PyResult<PyObject> {
    let document = sase_core::DocumentSnapshot::new(text);
    let spans = sase_core::editor_extract_placeholder_spans(&document);
    let value = serde_json::to_value(&spans).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Return ordered summaries for the prompt's unique raw placeholders.
#[pyfunction]
#[pyo3(name = "raw_placeholder_fields")]
fn py_raw_placeholder_fields(
    py: Python<'_>,
    text: &str,
    context_width: usize,
) -> PyResult<PyObject> {
    let fields = sase_core::editor_raw_placeholder_fields(text, context_width);
    let value = serde_json::to_value(&fields).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Replace mapped raw placeholders without touching literal spans.
#[pyfunction]
#[pyo3(name = "substitute_raw_placeholders")]
fn py_substitute_raw_placeholders(
    text: &str,
    values: BTreeMap<String, String>,
) -> String {
    sase_core::editor_substitute_raw_placeholders(text, &values)
}

/// Convert placeholder labels into stable xprompt input names.
#[pyfunction]
#[pyo3(name = "placeholder_input_names")]
fn py_placeholder_input_names(texts: Vec<String>) -> Vec<String> {
    sase_core::editor_placeholder_input_names(texts)
}

// --- Portable AXE runtime status -----------------------------------------

fn axe_status_error_to_pyerr(error: AxeStatusError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

/// Return the supported AXE runtime status wire schema version.
#[pyfunction]
#[pyo3(name = "axe_status_wire_schema_version")]
fn py_axe_status_wire_schema_version() -> u32 {
    AXE_STATUS_SCHEMA_VERSION
}

/// Classify already-collected AXE runtime observations without host I/O.
#[pyfunction]
#[pyo3(name = "classify_axe_status")]
fn py_classify_axe_status<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let request: AxeStatusRequestWire =
        serde_json::from_value(value).map_err(|error| {
            PyValueError::new_err(format!(
                "request is not a valid AxeStatusRequestWire dict: {error}"
            ))
        })?;
    let snapshot = py
        .allow_threads(|| core_classify_axe_status(&request))
        .map_err(axe_status_error_to_pyerr)?;
    let value = serde_json::to_value(snapshot).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

// --- Axe chop engine bindings --------------------------------------------

fn chop_error_to_pyerr(error: ChopEngineError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

fn chop_request_from_pydict<T>(
    request: &Bound<'_, PyDict>,
    label: &str,
) -> PyResult<T>
where
    T: serde::de::DeserializeOwned,
{
    let value = py_to_json_value(request.as_any())?;
    serde_json::from_value(value).map_err(|error| {
        PyValueError::new_err(format!(
            "request is not a valid {label} dict: {error}"
        ))
    })
}

fn chop_result_to_py<T>(py: Python<'_>, result: &T) -> PyResult<PyObject>
where
    T: serde::Serialize,
{
    let value = serde_json::to_value(result).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

#[pyfunction]
#[pyo3(name = "chop_engine_schema_version")]
fn py_chop_engine_schema_version() -> u32 {
    CHOP_ENGINE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "chop_result_schema_version")]
fn py_chop_result_schema_version() -> u32 {
    CHOP_RESULT_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "chop_state_schema_version")]
fn py_chop_state_schema_version() -> u32 {
    CHOP_STATE_SCHEMA_VERSION
}

/// Parse and validate a script-written chop result JSON document.
#[pyfunction]
#[pyo3(name = "parse_chop_result")]
fn py_parse_chop_result<'py>(
    py: Python<'py>,
    document: &str,
) -> PyResult<PyObject> {
    let result =
        core_parse_chop_result(document).map_err(chop_error_to_pyerr)?;
    chop_result_to_py(py, &result)
}

/// Validate and normalize an already-decoded chop result dict.
#[pyfunction]
#[pyo3(name = "validate_chop_result")]
fn py_validate_chop_result<'py>(
    py: Python<'py>,
    result: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let result: ChopResultDocumentWire =
        chop_request_from_pydict(result, "chop result")?;
    core_validate_chop_result(&result).map_err(chop_error_to_pyerr)?;
    chop_result_to_py(py, &result)
}

/// Validate and normalize one launch proposal.
#[pyfunction]
#[pyo3(name = "validate_chop_proposal")]
#[pyo3(signature = (proposal, index = 0, prior_ids = None))]
fn py_validate_chop_proposal<'py>(
    py: Python<'py>,
    proposal: &Bound<'py, PyDict>,
    index: usize,
    prior_ids: Option<&Bound<'py, PyList>>,
) -> PyResult<PyObject> {
    let proposal: ChopLaunchProposalWire =
        chop_request_from_pydict(proposal, "chop launch proposal")?;
    let prior_ids = match prior_ids {
        Some(items) => strings_from_py_list(items, "prior_ids")?,
        None => Vec::new(),
    };
    core_validate_chop_proposal(&proposal, index, &prior_ids)
        .map_err(chop_error_to_pyerr)?;
    chop_result_to_py(py, &proposal)
}

/// Derive the default agent name scaffold for one proposal.
#[pyfunction]
#[pyo3(name = "derive_chop_agent_name")]
#[pyo3(signature = (chop_name, target_key = None, proposal_index = 0, run_token = None))]
fn py_derive_chop_agent_name(
    chop_name: &str,
    target_key: Option<&str>,
    proposal_index: usize,
    run_token: Option<&str>,
) -> PyResult<String> {
    core_derive_chop_agent_name(
        chop_name,
        target_key,
        proposal_index,
        run_token,
    )
    .map_err(chop_error_to_pyerr)
}

/// Evaluate inhibit guards followed by the configured trigger.
#[pyfunction]
#[pyo3(name = "evaluate_chop_decision")]
fn py_evaluate_chop_decision<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: ChopDecisionRequestWire =
        chop_request_from_pydict(request, "chop decision request")?;
    let result =
        core_evaluate_chop_decision(&request).map_err(chop_error_to_pyerr)?;
    chop_result_to_py(py, &result)
}

/// Transform a runner-owned checkpoint document.
#[pyfunction]
#[pyo3(name = "apply_chop_checkpoint_update")]
fn py_apply_chop_checkpoint_update<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: ChopCheckpointUpdateRequestWire =
        chop_request_from_pydict(request, "chop checkpoint update request")?;
    let result =
        core_apply_checkpoint_update(&request).map_err(chop_error_to_pyerr)?;
    chop_result_to_py(py, &result)
}

/// Test and record one key in a bounded runner-owned seen store.
#[pyfunction]
#[pyo3(name = "check_and_record_chop_once_per")]
fn py_check_and_record_chop_once_per<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: ChopOncePerRequestWire =
        chop_request_from_pydict(request, "chop once-per request")?;
    let result = core_check_and_record_once_per(&request)
        .map_err(chop_error_to_pyerr)?;
    chop_result_to_py(py, &result)
}

/// Release exact keys from a bounded runner-owned seen store.
#[pyfunction]
#[pyo3(name = "release_chop_once_per")]
fn py_release_chop_once_per<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: ChopOncePerReleaseRequestWire =
        chop_request_from_pydict(request, "chop once-per release request")?;
    let result =
        core_release_chop_once_per(&request).map_err(chop_error_to_pyerr)?;
    chop_result_to_py(py, &result)
}

/// Expand literal or host-provided source targets into stable instances.
#[pyfunction]
#[pyo3(name = "expand_chop_targets")]
fn py_expand_chop_targets<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: ChopTargetExpansionRequestWire =
        chop_request_from_pydict(request, "chop target expansion request")?;
    let result =
        core_expand_chop_targets(&request).map_err(chop_error_to_pyerr)?;
    chop_result_to_py(py, &result)
}

/// Parse one strict positive compound duration into seconds.
#[pyfunction]
#[pyo3(name = "parse_chop_duration")]
fn py_parse_chop_duration(value: &str) -> PyResult<u64> {
    core_parse_chop_duration(value).map_err(chop_error_to_pyerr)
}

/// Normalize and split one AXE description into its summary and body.
#[pyfunction]
#[pyo3(name = "split_axe_description")]
fn py_split_axe_description(text: &str) -> (String, String) {
    core_split_axe_description(text)
}

/// Return provenance-aware diagnostics for the new axe config shape.
#[pyfunction]
#[pyo3(name = "validate_axe_config")]
fn py_validate_axe_config<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: AxeConfigValidationRequestWire =
        chop_request_from_pydict(request, "axe config validation request")?;
    let result =
        core_validate_axe_config(&request).map_err(chop_error_to_pyerr)?;
    chop_result_to_py(py, &result)
}

// --- Glossary catalog bindings -------------------------------------------

fn glossary_error_to_pyerr(error: GlossaryError) -> PyErr {
    PyValueError::new_err(error.to_string())
}

fn glossary_entries_from_pylist(
    entries: &Bound<'_, PyList>,
) -> PyResult<Vec<GlossaryInputEntryWire>> {
    serde_json::from_value(py_to_json_value(entries.as_any())?).map_err(
        |error| {
            PyValueError::new_err(format!(
                "entries are not valid GlossaryInputEntryWire dicts: {error}"
            ))
        },
    )
}

fn glossary_to_py<T>(py: Python<'_>, value: &T) -> PyResult<PyObject>
where
    T: serde::Serialize,
{
    let value = serde_json::to_value(value).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

#[pyfunction]
#[pyo3(name = "glossary_validate")]
fn py_glossary_validate(
    py: Python<'_>,
    entries: &Bound<'_, PyList>,
) -> PyResult<PyObject> {
    let entries = glossary_entries_from_pylist(entries)?;
    let diagnostics = core_validate_glossary_entries(&entries);
    glossary_to_py(py, &diagnostics)
}

#[pyfunction]
#[pyo3(name = "glossary_catalog")]
fn py_glossary_catalog(
    py: Python<'_>,
    entries: &Bound<'_, PyList>,
) -> PyResult<PyObject> {
    let entries = glossary_entries_from_pylist(entries)?;
    let catalog = core_build_glossary_catalog(entries)
        .map_err(glossary_error_to_pyerr)?;
    glossary_to_py(py, &catalog)
}

#[pyfunction]
#[pyo3(name = "compile_glossary_catalog")]
fn py_compile_glossary_catalog(
    entries: &Bound<'_, PyList>,
) -> PyResult<PyGlossaryCatalogHandle> {
    let entries = glossary_entries_from_pylist(entries)?;
    let catalog = core_compile_glossary_catalog(entries)
        .map_err(glossary_error_to_pyerr)?;
    Ok(PyGlossaryCatalogHandle { catalog })
}

// --- Config Center backend bindings ---------------------------------------
//
// JSON-in / JSON-out wrappers over `sase_core::config`. Python supplies the
// already-discovered layer stack and JSON Schema; these return plain
// dicts/lists the Python adapter rehydrates into its dataclass mirrors. Domain
// errors (e.g. an unknown target layer) surface as `ValueError`.

fn config_error_to_pyerr(err: ConfigDomainError) -> PyErr {
    PyValueError::new_err(format!("{err}"))
}

/// Flatten a JSON Schema dict into the ordered config field model dict.
#[pyfunction]
#[pyo3(name = "config_field_model")]
fn py_config_field_model<'py>(
    py: Python<'py>,
    schema: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let schema_value = py_to_json_value(schema.as_any())?;
    let model = core_config_field_model(&schema_value)
        .map_err(config_error_to_pyerr)?;
    let json = serde_json::to_value(&model).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &json)
}

/// Build the config inventory (sources + per-field provenance + diagnostics).
///
/// *request* is a `ConfigInventoryRequestWire`-shape dict: `schema`, ordered
/// `layers`, and the optional `deprecations`/`unsupported` policy.
#[pyfunction]
#[pyo3(name = "config_inventory")]
fn py_config_inventory<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let req: ConfigInventoryRequestWire = serde_json::from_value(value)
        .map_err(|e| {
            PyValueError::new_err(format!(
                "request is not a valid config inventory request: {e}"
            ))
        })?;
    let inventory =
        core_config_inventory(&req).map_err(config_error_to_pyerr)?;
    let json = serde_json::to_value(&inventory).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &json)
}

/// Plan a single set/unset edit, returning the write-plan dict.
///
/// *request* is a `ConfigEditRequestWire`-shape dict: `schema`, `layers`,
/// `target_layer`, `path`, and `op` (`{"kind": "set", "value": ...}` or
/// `{"kind": "unset"}`).
#[pyfunction]
#[pyo3(name = "config_plan_edit")]
fn py_config_plan_edit<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let req: ConfigEditRequestWire =
        serde_json::from_value(value).map_err(|e| {
            PyValueError::new_err(format!(
                "request is not a valid config edit request: {e}"
            ))
        })?;
    let plan = core_config_plan_edit(&req).map_err(config_error_to_pyerr)?;
    let json = serde_json::to_value(&plan).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &json)
}

/// Compose the ordered AXE layer stack and return exact-key provenance and
/// entity inventory alongside the effective config.
#[pyfunction]
#[pyo3(name = "axe_config_compose")]
fn py_axe_config_compose<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let req: AxeConfigComposeRequestWire = serde_json::from_value(value)
        .map_err(|e| {
            PyValueError::new_err(format!(
                "request is not a valid AXE composition request: {e}"
            ))
        })?;
    let result =
        core_compose_axe_config(&req).map_err(config_error_to_pyerr)?;
    let json = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &json)
}

/// Plan an exact-key sparse AXE lumberjack/chop contribution mutation.
#[pyfunction]
#[pyo3(name = "axe_config_plan_entry")]
fn py_axe_config_plan_entry<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let req: AxeEntryMutationRequestWire = serde_json::from_value(value)
        .map_err(|e| {
            PyValueError::new_err(format!(
                "request is not a valid AXE entry mutation request: {e}"
            ))
        })?;
    let result =
        core_plan_axe_entry_mutation(&req).map_err(config_error_to_pyerr)?;
    let json = serde_json::to_value(&result).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &json)
}

/// Schema-validate a candidate merged config, returning diagnostic dicts.
///
/// *request* is a `ConfigValidateRequestWire`-shape dict: `schema` + `config`.
#[pyfunction]
#[pyo3(name = "config_validate")]
fn py_config_validate<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let value = py_to_json_value(request.as_any())?;
    let req: ConfigValidateRequestWire = serde_json::from_value(value)
        .map_err(|e| {
            PyValueError::new_err(format!(
                "request is not a valid config validate request: {e}"
            ))
        })?;
    let diagnostics = core_config_validate(&req);
    let json = serde_json::to_value(&diagnostics).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &json)
}

// --- Temporary default reasoning-effort override -------------------------

fn effort_override_error_to_pyerr(err: EffortOverrideDomainError) -> PyErr {
    match err {
        EffortOverrideDomainError::Validation(message) => {
            PyValueError::new_err(message)
        }
        EffortOverrideDomainError::LockTimeout => {
            PyTimeoutError::new_err(err.to_string())
        }
        EffortOverrideDomainError::Io(_)
        | EffortOverrideDomainError::Json(_) => {
            PyRuntimeError::new_err(err.to_string())
        }
    }
}

fn effort_override_now(now: Option<f64>) -> PyResult<f64> {
    if let Some(value) = now {
        return Ok(value);
    }
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs_f64())
        .map_err(|error| {
            PyRuntimeError::new_err(format!(
                "could not read the current Unix timestamp: {error}"
            ))
        })
}

fn effort_wire_to_py<'py, T: serde::Serialize>(
    py: Python<'py>,
    value: &T,
) -> PyResult<PyObject> {
    let json = serde_json::to_value(value).map_err(|error| {
        PyRuntimeError::new_err(format!(
            "internal effort serialize error: {error}"
        ))
    })?;
    json_value_to_py(py, &json)
}

#[pyfunction]
#[pyo3(name = "effort_override_wire_schema_version")]
fn py_effort_override_wire_schema_version() -> u32 {
    sase_core::EFFORT_OVERRIDE_WIRE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(name = "effort_override_get", signature = (sase_home, now = None))]
fn py_effort_override_get<'py>(
    py: Python<'py>,
    sase_home: &str,
    now: Option<f64>,
) -> PyResult<PyObject> {
    let record = core_get_effort_override(
        &PathBuf::from(sase_home),
        effort_override_now(now)?,
    )
    .map_err(effort_override_error_to_pyerr)?;
    effort_wire_to_py(py, &record)
}

#[pyfunction]
#[pyo3(
    name = "effort_override_set_relative",
    signature = (
        sase_home,
        effort,
        source,
        duration_seconds = None,
        now = None
    )
)]
fn py_effort_override_set_relative<'py>(
    py: Python<'py>,
    sase_home: &str,
    effort: &str,
    source: &str,
    duration_seconds: Option<f64>,
    now: Option<f64>,
) -> PyResult<PyObject> {
    let record = core_set_effort_override_relative(
        &PathBuf::from(sase_home),
        effort,
        duration_seconds,
        source,
        effort_override_now(now)?,
    )
    .map_err(effort_override_error_to_pyerr)?;
    effort_wire_to_py(py, &record)
}

#[pyfunction]
#[pyo3(
    name = "effort_override_set_until",
    signature = (sase_home, effort, expires_at, source, now = None)
)]
fn py_effort_override_set_until<'py>(
    py: Python<'py>,
    sase_home: &str,
    effort: &str,
    expires_at: f64,
    source: &str,
    now: Option<f64>,
) -> PyResult<PyObject> {
    let record = core_set_effort_override_until(
        &PathBuf::from(sase_home),
        effort,
        expires_at,
        source,
        effort_override_now(now)?,
    )
    .map_err(effort_override_error_to_pyerr)?;
    effort_wire_to_py(py, &record)
}

#[pyfunction]
#[pyo3(name = "effort_override_clear")]
fn py_effort_override_clear(sase_home: &str) -> PyResult<bool> {
    core_clear_effort_override(&PathBuf::from(sase_home))
        .map_err(effort_override_error_to_pyerr)
}

// --- Temporary maximum-running-agents override -----------------------

fn runner_limit_override_error_to_pyerr(
    err: RunnerLimitOverrideDomainError,
) -> PyErr {
    match err {
        RunnerLimitOverrideDomainError::Validation(message) => {
            PyValueError::new_err(message)
        }
        RunnerLimitOverrideDomainError::LockTimeout => {
            PyTimeoutError::new_err(err.to_string())
        }
        RunnerLimitOverrideDomainError::Io(_)
        | RunnerLimitOverrideDomainError::Json(_) => {
            PyRuntimeError::new_err(err.to_string())
        }
    }
}

#[pyfunction]
#[pyo3(name = "runner_limit_override_wire_schema_version")]
fn py_runner_limit_override_wire_schema_version() -> u32 {
    sase_core::RUNNER_LIMIT_OVERRIDE_WIRE_SCHEMA_VERSION
}

#[pyfunction]
#[pyo3(
    name = "runner_limit_override_get",
    signature = (sase_home, now = None)
)]
fn py_runner_limit_override_get<'py>(
    py: Python<'py>,
    sase_home: &str,
    now: Option<f64>,
) -> PyResult<PyObject> {
    let record = core_get_runner_limit_override(
        &PathBuf::from(sase_home),
        effort_override_now(now)?,
    )
    .map_err(runner_limit_override_error_to_pyerr)?;
    effort_wire_to_py(py, &record)
}

#[pyfunction]
#[pyo3(
    name = "runner_limit_override_set_relative",
    signature = (
        sase_home,
        limit,
        source,
        duration_seconds = None,
        now = None
    )
)]
fn py_runner_limit_override_set_relative<'py>(
    py: Python<'py>,
    sase_home: &str,
    limit: u64,
    source: &str,
    duration_seconds: Option<f64>,
    now: Option<f64>,
) -> PyResult<PyObject> {
    let record = core_set_runner_limit_override_relative(
        &PathBuf::from(sase_home),
        limit,
        duration_seconds,
        source,
        effort_override_now(now)?,
    )
    .map_err(runner_limit_override_error_to_pyerr)?;
    effort_wire_to_py(py, &record)
}

#[pyfunction]
#[pyo3(
    name = "runner_limit_override_set_until",
    signature = (sase_home, limit, expires_at, source, now = None)
)]
fn py_runner_limit_override_set_until<'py>(
    py: Python<'py>,
    sase_home: &str,
    limit: u64,
    expires_at: f64,
    source: &str,
    now: Option<f64>,
) -> PyResult<PyObject> {
    let record = core_set_runner_limit_override_until(
        &PathBuf::from(sase_home),
        limit,
        expires_at,
        source,
        effort_override_now(now)?,
    )
    .map_err(runner_limit_override_error_to_pyerr)?;
    effort_wire_to_py(py, &record)
}

#[pyfunction]
#[pyo3(name = "runner_limit_override_clear")]
fn py_runner_limit_override_clear(sase_home: &str) -> PyResult<bool> {
    core_clear_runner_limit_override(&PathBuf::from(sase_home))
        .map_err(runner_limit_override_error_to_pyerr)
}

#[pyfunction]
#[pyo3(
    name = "resolve_effective_effort",
    signature = (
        explicit_effort = None,
        alias_effort = None,
        temporary_effort = None,
        configured_effort = None
    )
)]
fn py_resolve_effective_effort<'py>(
    py: Python<'py>,
    explicit_effort: Option<&str>,
    alias_effort: Option<&str>,
    temporary_effort: Option<&str>,
    configured_effort: Option<&str>,
) -> PyResult<PyObject> {
    effort_wire_to_py(
        py,
        &core_resolve_effective_effort(
            explicit_effort,
            alias_effort,
            temporary_effort,
            configured_effort,
        ),
    )
}

// --- Canonical project/home content layout -------------------------------

/// Return the shared canonical/legacy SASE content layout and xprompt order.
#[pyfunction]
#[pyo3(name = "sase_content_layout")]
#[pyo3(signature = (
    home_root,
    project_root = None,
    chezmoi_root = None,
    project = None
))]
fn py_sase_content_layout(
    py: Python<'_>,
    home_root: &str,
    project_root: Option<&str>,
    chezmoi_root: Option<&str>,
    project: Option<&str>,
) -> PyResult<PyObject> {
    let project_root = project_root.map(PathBuf::from);
    let chezmoi_root = chezmoi_root.map(PathBuf::from);
    let layout = core_sase_content_layout(
        project_root.as_deref(),
        &PathBuf::from(home_root),
        chezmoi_root.as_deref(),
        project,
    );
    let json = serde_json::to_value(layout).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &json)
}

/// Resolve ordered candidate presence with the core collision policy.
#[pyfunction]
#[pyo3(name = "resolve_layout_candidates")]
fn py_resolve_layout_candidates(
    py: Python<'_>,
    policy: &str,
    exists: Vec<bool>,
) -> PyResult<PyObject> {
    let policy = LayoutCollisionPolicyWire::parse(policy).ok_or_else(|| {
        PyValueError::new_err(format!(
            "unsupported layout collision policy {policy:?}; expected 'error' or 'first_wins'"
        ))
    })?;
    let resolution = core_resolve_layout_candidates(policy, &exists);
    let json = serde_json::to_value(resolution).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &json)
}

/// Return the canonical `skill/<name>` xprompt reference for a skill source.
#[pyfunction]
#[pyo3(name = "skill_reference_name")]
#[pyo3(signature = (skill_name, project = None))]
fn py_skill_reference_name(skill_name: &str, project: Option<&str>) -> String {
    core_skill_reference_name(project, skill_name)
}

/// Serialize an optional wire record to Python, or `None`.
fn optional_wire_to_py<T: serde::Serialize>(
    py: Python<'_>,
    value: Option<T>,
) -> PyResult<PyObject> {
    let Some(value) = value else {
        return Ok(py.None());
    };
    let json = serde_json::to_value(value).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &json)
}

/// Return the canonical `memory/<stem>` xprompt reference for a memory note.
#[pyfunction]
#[pyo3(name = "memory_reference_name")]
fn py_memory_reference_name(stem: &str) -> String {
    core_memory_reference_name(stem)
}

/// Split a canonical `memory/<stem>` reference back into its note stem.
#[pyfunction]
#[pyo3(name = "memory_reference_stem")]
fn py_memory_reference_stem(name: &str) -> Option<String> {
    core_memory_reference_stem(name).map(str::to_string)
}

/// Reject a non-memory definition that claims a reserved `memory/` reference.
#[pyfunction]
#[pyo3(name = "reserved_memory_namespace_issue")]
fn py_reserved_memory_namespace_issue(
    py: Python<'_>,
    source: &str,
    name: &str,
) -> PyResult<PyObject> {
    optional_wire_to_py(py, core_reserved_memory_namespace_issue(source, name))
}

/// Apply the shared xprompt-memory note rules to one file in a memory root.
#[pyfunction]
#[pyo3(name = "memory_note_issue")]
#[pyo3(signature = (source, stem, note_type = None))]
fn py_memory_note_issue(
    py: Python<'_>,
    source: &str,
    stem: &str,
    note_type: Option<&str>,
) -> PyResult<PyObject> {
    optional_wire_to_py(py, core_memory_note_issue(source, stem, note_type))
}

/// Apply the shared two-way skill placement rules to one loaded definition.
#[pyfunction]
#[pyo3(name = "skill_placement_issue")]
#[pyo3(signature = (
    source,
    in_skill_source,
    declares_skill,
    migrate_to = None
))]
fn py_skill_placement_issue(
    py: Python<'_>,
    source: &str,
    in_skill_source: bool,
    declares_skill: bool,
    migrate_to: Option<&str>,
) -> PyResult<PyObject> {
    let Some(issue) = core_skill_placement_issue(
        source,
        in_skill_source,
        declares_skill,
        migrate_to,
    ) else {
        return Ok(py.None());
    };
    let json = serde_json::to_value(issue).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &json)
}

/// Return the launch wire schema version pinned by the Rust skeleton structs.
#[pyfunction]
#[pyo3(name = "agent_launch_wire_schema_version")]
fn py_agent_launch_wire_schema_version() -> u32 {
    sase_core::AGENT_LAUNCH_WIRE_SCHEMA_VERSION
}

/// Write the launch prompt temp file and return prepared process data.
#[pyfunction]
#[pyo3(name = "prepare_agent_launch")]
#[pyo3(signature = (
    request,
    python_executable,
    runner_script,
    output_root,
    sase_tmpdir = None,
    preallocated_env = None
))]
fn py_prepare_agent_launch<'py>(
    py: Python<'py>,
    request: &Bound<'py, PyDict>,
    python_executable: &str,
    runner_script: &str,
    output_root: &str,
    sase_tmpdir: Option<&str>,
    preallocated_env: Option<&Bound<'py, PyDict>>,
) -> PyResult<PyObject> {
    let req = agent_launch_request_from_pydict(request)?;
    let preallocated = match preallocated_env {
        Some(env) => env_dict_from_pydict(env)?,
        None => std::collections::BTreeMap::new(),
    };
    let prepared = core_prepare_agent_launch(
        &req,
        python_executable,
        runner_script,
        sase_tmpdir,
        output_root,
        &preallocated,
    )
    .map_err(|err| PyValueError::new_err(format!("{err}")))?;
    let value = serde_json::to_value(&prepared).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Spawn a prepared detached agent process and run optional claim callback.
#[pyfunction]
#[pyo3(name = "spawn_prepared_agent_process")]
#[pyo3(signature = (prepared, env, claim_callback = None))]
fn py_spawn_prepared_agent_process(
    py: Python<'_>,
    prepared: &Bound<'_, PyDict>,
    env: &Bound<'_, PyDict>,
    claim_callback: Option<&Bound<'_, PyAny>>,
) -> PyResult<u32> {
    let prepared = agent_launch_prepared_from_pydict(prepared)?;
    let env = env_dict_from_pydict(env)?;
    let mut child = py
        .allow_threads(move || spawn_prepared_detached_process(prepared, env))
        .map_err(PyRuntimeError::new_err)?;
    let pid = child.id();

    if let Some(callback) = claim_callback {
        match callback.call1((pid,)) {
            Ok(value) => {
                let success = value.extract::<bool>().map_err(|err| {
                    PyValueError::new_err(format!(
                        "claim_callback must return bool, got invalid value: {err}"
                    ))
                })?;
                if !success {
                    terminate_child_after_claim_failure(&mut child);
                    return Err(PyRuntimeError::new_err(
                        "agent launch claim callback reported failure",
                    ));
                }
            }
            Err(err) => {
                terminate_child_after_claim_failure(&mut child);
                return Err(err);
            }
        }
    }

    Ok(pid)
}

/// Allocate unique launch timestamps from a base YYmmdd_HHMMSS timestamp.
#[pyfunction]
#[pyo3(name = "allocate_launch_timestamp_batch")]
#[pyo3(signature = (count, base_timestamp, after_timestamp = None))]
fn py_allocate_launch_timestamp_batch<'py>(
    py: Python<'py>,
    count: usize,
    base_timestamp: &str,
    after_timestamp: Option<&str>,
) -> PyResult<Bound<'py, PyList>> {
    let timestamps = core_allocate_launch_timestamp_batch(
        count,
        base_timestamp,
        after_timestamp,
    )
    .map_err(|err| PyValueError::new_err(format!("{err}")))?;
    let list = PyList::empty_bound(py);
    for timestamp in timestamps {
        list.append(timestamp)?;
    }
    Ok(list)
}

/// Plan deterministic prompt fan-out without launching child agents.
#[pyfunction]
#[pyo3(name = "plan_agent_launch_fanout")]
#[pyo3(signature = (prompt, launch_kind = None))]
fn py_plan_agent_launch_fanout<'py>(
    py: Python<'py>,
    prompt: &str,
    launch_kind: Option<&str>,
) -> PyResult<PyObject> {
    let plan = core_plan_agent_launch_fanout(prompt, launch_kind)
        .map_err(|err| PyValueError::new_err(format!("{err}")))?;
    let value = serde_json::to_value(&plan).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Return single-line inline-code ranges as UTF-8 byte offsets.
#[pyfunction]
#[pyo3(name = "inline_code_ranges")]
#[pyo3(signature = (text, masked_ranges = None))]
fn py_inline_code_ranges(
    text: &str,
    masked_ranges: Option<Vec<(usize, usize)>>,
) -> Vec<(usize, usize)> {
    core_inline_code_ranges(text, masked_ranges.as_deref().unwrap_or(&[]))
}

/// Return parsed RUNNING workspace claims from project-file content.
#[pyfunction]
#[pyo3(name = "list_workspace_claims_from_content")]
fn py_list_workspace_claims_from_content<'py>(
    py: Python<'py>,
    content: &str,
) -> PyResult<PyObject> {
    let claims = core_list_workspace_claims_from_content(content);
    let value = serde_json::to_value(&claims).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Plan insertion of one RUNNING workspace claim.
#[pyfunction]
#[pyo3(name = "plan_claim_workspace_from_content")]
fn py_plan_claim_workspace_from_content<'py>(
    py: Python<'py>,
    content: &str,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let req = workspace_claim_request_from_pydict(request)?;
    let plan = core_plan_claim_workspace_from_content(content, &req);
    let value = serde_json::to_value(&plan).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Plan transfer of one RUNNING workspace claim to a new PID.
#[pyfunction]
#[pyo3(name = "plan_transfer_workspace_claim_from_content")]
fn py_plan_transfer_workspace_claim_from_content<'py>(
    py: Python<'py>,
    content: &str,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let req = workspace_claim_request_from_pydict(request)?;
    let plan = core_plan_transfer_workspace_claim_from_content(content, &req);
    let value = serde_json::to_value(&plan).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

/// Plan first-free workspace allocation and RUNNING claim insertion together.
#[pyfunction]
#[pyo3(name = "allocate_and_claim_workspace_from_content")]
fn py_allocate_and_claim_workspace_from_content<'py>(
    py: Python<'py>,
    content: &str,
    min_workspace: u32,
    max_workspace: u32,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let req = workspace_claim_request_from_pydict(request)?;
    let plan = core_allocate_and_claim_workspace_from_content(
        content,
        min_workspace,
        max_workspace,
        &req,
    );
    let value = serde_json::to_value(&plan).map_err(|e| {
        PyValueError::new_err(format!("internal serialize error: {e}"))
    })?;
    json_value_to_py(py, &value)
}

fn spawn_prepared_detached_process(
    prepared: AgentLaunchPreparedWire,
    env: BTreeMap<String, String>,
) -> Result<Child, String> {
    let Some((program, args)) = prepared.argv.split_first() else {
        return Err("prepared launch argv must not be empty".to_string());
    };

    let stdout_file = File::create(&prepared.output_path).map_err(|err| {
        format!(
            "failed to open launch output file {}: {err}",
            prepared.output_path
        )
    })?;
    let stderr_file = stdout_file.try_clone().map_err(|err| {
        format!(
            "failed to clone launch output file {} for stderr: {err}",
            prepared.output_path
        )
    })?;

    let mut command = Command::new(program);
    command
        .args(args)
        .current_dir(&prepared.cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout_file))
        .stderr(Stdio::from(stderr_file))
        .env_clear()
        .envs(env);

    configure_detached_process(&mut command);

    command.spawn().map_err(|err| {
        format!(
            "failed to spawn prepared agent process in cwd {}: {err}",
            prepared.cwd
        )
    })
}

#[cfg(unix)]
fn configure_detached_process(command: &mut Command) {
    use std::os::unix::process::CommandExt;

    // Match Python's subprocess.Popen(start_new_session=True) behavior.
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(windows)]
fn configure_detached_process(command: &mut Command) {
    use std::os::windows::process::CommandExt;

    const DETACHED_PROCESS: u32 = 0x0000_0008;
    const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
    command.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP);
}

#[cfg(not(any(unix, windows)))]
fn configure_detached_process(_command: &mut Command) {}

fn terminate_child_after_claim_failure(child: &mut Child) {
    if child.try_wait().ok().flatten().is_some() {
        return;
    }

    terminate_child_gracefully(child);
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if child.try_wait().ok().flatten().is_some() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    let _ = child.kill();
    let _ = child.wait();
}

#[cfg(unix)]
fn terminate_child_gracefully(child: &Child) {
    let _ = unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGTERM) };
}

#[cfg(not(unix))]
fn terminate_child_gracefully(child: &mut Child) {
    let _ = child.kill();
}

/// Persist one telemetry accumulator flush in a single SQLite transaction.
#[pyfunction]
#[pyo3(
    name = "telemetry_record_batch",
    signature = (store_path, batch, busy_timeout_ms=250)
)]
fn py_telemetry_record_batch<'py>(
    py: Python<'py>,
    store_path: &str,
    batch: &Bound<'py, PyDict>,
    busy_timeout_ms: u64,
) -> PyResult<PyObject> {
    let batch: TelemetryRecordBatchWire =
        telemetry_request_from_pydict(batch, "TelemetryRecordBatchWire")?;
    let path = PathBuf::from(store_path);
    let result = py
        .allow_threads(|| {
            core_telemetry_record_batch(
                &path,
                batch,
                Duration::from_millis(busy_timeout_ms),
            )
        })
        .map_err(PyRuntimeError::new_err)?;
    telemetry_result_to_py(py, &result)
}

/// Preview or delete telemetry rows matching exact label values.
#[pyfunction]
#[pyo3(
    name = "telemetry_cleanup_matching_labels",
    signature = (store_path, request, busy_timeout_ms=250)
)]
fn py_telemetry_cleanup_matching_labels<'py>(
    py: Python<'py>,
    store_path: &str,
    request: &Bound<'py, PyDict>,
    busy_timeout_ms: u64,
) -> PyResult<PyObject> {
    let request: TelemetryCleanupRequestWire =
        telemetry_request_from_pydict(request, "TelemetryCleanupRequestWire")?;
    let path = PathBuf::from(store_path);
    let result = py
        .allow_threads(|| {
            core_telemetry_cleanup_matching_labels(
                &path,
                request,
                Duration::from_millis(busy_timeout_ms),
            )
        })
        .map_err(PyRuntimeError::new_err)?;
    telemetry_result_to_py(py, &result)
}

/// Query current telemetry values with source-aware gauge staleness.
#[pyfunction]
#[pyo3(
    name = "telemetry_query_instant",
    signature = (store_path, request, busy_timeout_ms=250)
)]
fn py_telemetry_query_instant<'py>(
    py: Python<'py>,
    store_path: &str,
    request: &Bound<'py, PyDict>,
    busy_timeout_ms: u64,
) -> PyResult<PyObject> {
    let request: TelemetryInstantQueryWire =
        telemetry_request_from_pydict(request, "TelemetryInstantQueryWire")?;
    let path = PathBuf::from(store_path);
    let result = py
        .allow_threads(|| {
            core_telemetry_query_instant(
                &path,
                request,
                Duration::from_millis(busy_timeout_ms),
            )
        })
        .map_err(PyRuntimeError::new_err)?;
    telemetry_result_to_py(py, &result)
}

/// Query grouped telemetry series across raw and rollup resolutions.
#[pyfunction]
#[pyo3(
    name = "telemetry_query_range",
    signature = (store_path, request, busy_timeout_ms=250)
)]
fn py_telemetry_query_range<'py>(
    py: Python<'py>,
    store_path: &str,
    request: &Bound<'py, PyDict>,
    busy_timeout_ms: u64,
) -> PyResult<PyObject> {
    let request: TelemetryRangeQueryWire =
        telemetry_request_from_pydict(request, "TelemetryRangeQueryWire")?;
    let path = PathBuf::from(store_path);
    let result = py
        .allow_threads(|| {
            core_telemetry_query_range(
                &path,
                request,
                Duration::from_millis(busy_timeout_ms),
            )
        })
        .map_err(PyRuntimeError::new_err)?;
    telemetry_result_to_py(py, &result)
}

/// Fold and prune telemetry rows using a caller-supplied retention policy.
#[pyfunction]
#[pyo3(
    name = "telemetry_prune",
    signature = (store_path, request, busy_timeout_ms=250)
)]
fn py_telemetry_prune<'py>(
    py: Python<'py>,
    store_path: &str,
    request: &Bound<'py, PyDict>,
    busy_timeout_ms: u64,
) -> PyResult<PyObject> {
    let request: TelemetryPruneRequestWire =
        telemetry_request_from_pydict(request, "TelemetryPruneRequestWire")?;
    let path = PathBuf::from(store_path);
    let result = py
        .allow_threads(|| {
            core_telemetry_prune(
                &path,
                request,
                Duration::from_millis(busy_timeout_ms),
            )
        })
        .map_err(PyRuntimeError::new_err)?;
    telemetry_result_to_py(py, &result)
}

/// Return telemetry database size, tier counts, and write freshness.
#[pyfunction]
#[pyo3(
    name = "telemetry_store_stats",
    signature = (store_path, busy_timeout_ms=250)
)]
fn py_telemetry_store_stats<'py>(
    py: Python<'py>,
    store_path: &str,
    busy_timeout_ms: u64,
) -> PyResult<PyObject> {
    let path = PathBuf::from(store_path);
    let result = py
        .allow_threads(|| {
            core_telemetry_store_stats(
                &path,
                Duration::from_millis(busy_timeout_ms),
            )
        })
        .map_err(PyRuntimeError::new_err)?;
    telemetry_result_to_py(py, &result)
}

/// Aggregate run-backed Statistics views over a caller-supplied time range.
#[pyfunction]
#[pyo3(name = "agent_stats_query_runs")]
fn py_agent_stats_query_runs<'py>(
    py: Python<'py>,
    index_path: &str,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: AgentRunStatsRequestWire =
        telemetry_request_from_pydict(request, "AgentRunStatsRequestWire")?;
    let path = PathBuf::from(index_path);
    let result = py
        .allow_threads(|| core_query_run_stats(&path, request))
        .map_err(PyRuntimeError::new_err)?;
    telemetry_result_to_py(py, &result)
}

/// Aggregate durable skill, memory, question, and plan activity.
#[pyfunction]
#[pyo3(name = "agent_stats_query_activity")]
fn py_agent_stats_query_activity<'py>(
    py: Python<'py>,
    index_path: &str,
    sase_home: &str,
    request: &Bound<'py, PyDict>,
) -> PyResult<PyObject> {
    let request: AgentActivityStatsRequestWire = telemetry_request_from_pydict(
        request,
        "AgentActivityStatsRequestWire",
    )?;
    let index_path = PathBuf::from(index_path);
    let sase_home = PathBuf::from(sase_home);
    let result = py
        .allow_threads(|| {
            core_query_activity_stats(&index_path, &sase_home, request)
        })
        .map_err(PyRuntimeError::new_err)?;
    telemetry_result_to_py(py, &result)
}

fn telemetry_request_from_pydict<T>(
    request: &Bound<'_, PyDict>,
    wire_name: &str,
) -> PyResult<T>
where
    T: serde::de::DeserializeOwned,
{
    let value = py_to_json_value(request.as_any())?;
    serde_json::from_value(value).map_err(|error| {
        PyValueError::new_err(format!(
            "request is not a valid {wire_name} dict: {error}"
        ))
    })
}

fn telemetry_result_to_py<T>(py: Python<'_>, result: &T) -> PyResult<PyObject>
where
    T: serde::Serialize,
{
    let value = serde_json::to_value(result).map_err(|error| {
        PyValueError::new_err(format!("internal serialize error: {error}"))
    })?;
    json_value_to_py(py, &value)
}

fn workspace_claim_request_from_pydict(
    request: &Bound<'_, PyDict>,
) -> PyResult<WorkspaceClaimRequestWire> {
    let value = py_to_json_value(request.as_any())?;
    serde_json::from_value(value).map_err(|e| {
        PyValueError::new_err(format!(
            "request is not a valid WorkspaceClaimRequestWire dict: {e}"
        ))
    })
}

fn agent_launch_request_from_pydict(
    request: &Bound<'_, PyDict>,
) -> PyResult<AgentLaunchRequestWire> {
    let value = py_to_json_value(request.as_any())?;
    serde_json::from_value(value).map_err(|e| {
        PyValueError::new_err(format!(
            "request is not a valid AgentLaunchRequestWire dict: {e}"
        ))
    })
}

fn agent_launch_prepared_from_pydict(
    prepared: &Bound<'_, PyDict>,
) -> PyResult<AgentLaunchPreparedWire> {
    let value = py_to_json_value(prepared.as_any())?;
    serde_json::from_value(value).map_err(|e| {
        PyValueError::new_err(format!(
            "prepared is not a valid AgentLaunchPreparedWire dict: {e}"
        ))
    })
}

fn env_dict_from_pydict(
    env: &Bound<'_, PyDict>,
) -> PyResult<std::collections::BTreeMap<String, String>> {
    let value = py_to_json_value(env.as_any())?;
    serde_json::from_value(value).map_err(|e| {
        PyValueError::new_err(format!(
            "preallocated_env is not a valid string dict: {e}"
        ))
    })
}

#[pymodule]
#[pyo3(name = "sase_core_rs")]
fn sase_core_rs(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyQueryCorpusHandle>()?;
    m.add_class::<PyQueryProgramHandle>()?;
    m.add_function(wrap_pyfunction!(py_is_agent_name_template, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_agent_name_template, m)?)?;
    m.add_function(wrap_pyfunction!(py_agent_name_template_key, m)?)?;
    m.add_function(wrap_pyfunction!(py_iter_agent_name_key_markers, m)?)?;
    m.add_function(wrap_pyfunction!(py_render_agent_name_template, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_agent_name_template_namespace_template,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_match_agent_name_template, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_compare_agent_name_template_tokens,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_agent_name_template_tokens_after, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_machine_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_qualify_machine_agent_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_strip_machine_agent_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_machine_hood_of, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_agent_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_agent_username, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_agent_owner, m)?)?;
    m.add_function(wrap_pyfunction!(py_classify_agent_ownership, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_classify_legacy_v1_group_ownership,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_commit_shas_equivalent, m)?)?;
    m.add_function(wrap_pyfunction!(py_normalize_agent_archive_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_globalize_agent_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_globalize_legacy_agent_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_strip_global_agent_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_localize_agent_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_agent_family_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_agent_local_hood, m)?)?;
    m.add_function(wrap_pyfunction!(py_agent_name_in_hood, m)?)?;
    m.add_function(wrap_pyfunction!(py_agent_name_ancestors, m)?)?;
    m.add_function(wrap_pyfunction!(py_agent_link_target, m)?)?;
    m.add_function(wrap_pyfunction!(py_agent_relationship_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_agent_relationship_batch, m)?)?;
    m.add_function(wrap_pyfunction!(py_rewrite_agent_relationship_batch, m)?)?;
    m.add_function(wrap_pyfunction!(py_compose_snippet_catalog, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_project_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_patch_project_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(py_tokenize_query, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_query, m)?)?;
    m.add_function(wrap_pyfunction!(py_canonicalize_query, m)?)?;
    m.add_function(wrap_pyfunction!(py_commit_footer_wire_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_commit_footer, m)?)?;
    m.add_function(wrap_pyfunction!(py_update_commit_footer, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_commit_subject_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_default_commit_subject_types, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_commit_subject, m)?)?;
    m.add_function(wrap_pyfunction!(py_compile_corpus, m)?)?;
    m.add_function(wrap_pyfunction!(py_compile_query, m)?)?;
    m.add_function(wrap_pyfunction!(py_evaluate_many, m)?)?;
    m.add_function(wrap_pyfunction!(py_evaluate_query_many, m)?)?;
    m.add_function(wrap_pyfunction!(py_scan_agent_artifacts, m)?)?;
    m.add_function(wrap_pyfunction!(py_scan_agent_artifact_dirs, m)?)?;
    m.add_function(wrap_pyfunction!(py_aggregate_clan_runtime, m)?)?;
    m.add_function(wrap_pyfunction!(py_canonical_agent_artifact_path, m)?)?;
    m.add_function(wrap_pyfunction!(py_resolve_agent_artifact_path, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_resolve_agent_artifact_timestamp_path,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_parse_agent_artifact_path, m)?)?;
    m.add_function(wrap_pyfunction!(py_iter_agent_artifact_dirs, m)?)?;
    m.add_function(wrap_pyfunction!(py_rebuild_agent_artifact_index, m)?)?;
    m.add_function(wrap_pyfunction!(py_upsert_agent_artifact_index_row, m)?)?;
    m.add_function(wrap_pyfunction!(py_delete_agent_artifact_index_row, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_delete_agent_artifact_index_row_bounded,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_terminalize_stale_active_agent_artifact_index_rows,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_replace_agent_artifact_index_dismissed_agents,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_read_agent_artifact_index_meta, m)?)?;
    m.add_function(wrap_pyfunction!(py_write_agent_artifact_index_meta, m)?)?;
    m.add_function(wrap_pyfunction!(py_agent_artifact_index_status, m)?)?;
    m.add_function(wrap_pyfunction!(py_query_agent_artifact_index, m)?)?;
    m.add_function(wrap_pyfunction!(py_query_related_agent_artifact_dirs, m)?)?;
    m.add_function(wrap_pyfunction!(py_query_agent_archive, m)?)?;
    m.add_function(wrap_pyfunction!(py_agent_archive_facet_counts, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_mark_agent_archive_bundles_revived,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_verify_agent_archive_index, m)?)?;
    m.add_function(wrap_pyfunction!(py_save_dismissed_agent_group, m)?)?;
    m.add_function(wrap_pyfunction!(py_list_dismissed_agent_groups, m)?)?;
    m.add_function(wrap_pyfunction!(py_load_dismissed_agent_group, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_mark_dismissed_agent_group_revived,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_delete_dismissed_agent_group, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_record_recent_dismissed_agent_group,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_list_recent_dismissed_agent_groups,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_load_recent_dismissed_agent_group, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_mark_recent_dismissed_agent_group_revived,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_agent_cleanup_wire_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_plan_agent_cleanup, m)?)?;
    m.add_function(wrap_pyfunction!(py_save_dismissed_agents_index, m)?)?;
    m.add_function(wrap_pyfunction!(py_save_dismissed_bundle, m)?)?;
    m.add_function(wrap_pyfunction!(py_delete_agent_artifacts, m)?)?;
    m.add_function(wrap_pyfunction!(py_release_workspace_from_content, m)?)?;
    m.add_function(wrap_pyfunction!(py_mark_hook_agents_as_killed, m)?)?;
    m.add_function(wrap_pyfunction!(py_mark_mentor_agents_as_killed, m)?)?;
    m.add_function(wrap_pyfunction!(py_mark_comment_agents_as_killed, m)?)?;
    m.add_function(wrap_pyfunction!(py_remove_workspace_suffix, m)?)?;
    m.add_function(wrap_pyfunction!(py_is_valid_status_transition, m)?)?;
    m.add_function(wrap_pyfunction!(py_read_status_from_lines, m)?)?;
    m.add_function(wrap_pyfunction!(py_apply_status_update, m)?)?;
    m.add_function(wrap_pyfunction!(py_plan_status_transition, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_git_name_status_z, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_git_branch_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_derive_git_workspace_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_git_conflicted_files, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_git_local_changes, m)?)?;
    m.add_function(wrap_pyfunction!(py_vcs_log_wire_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_git_log, m)?)?;
    m.add_function(wrap_pyfunction!(py_classify_commit_presence, m)?)?;
    m.add_function(wrap_pyfunction!(py_aggregate_commit_log, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_merge_summary, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_read_project_lifecycle_from_content,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_apply_project_lifecycle_update, m)?)?;
    m.add_function(wrap_pyfunction!(py_apply_project_aliases_update, m)?)?;
    m.add_function(wrap_pyfunction!(py_apply_project_name_update, m)?)?;
    m.add_function(wrap_pyfunction!(py_list_project_records, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_bead_needs_size_check_relax_migration,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_bead_size_check_relax_migration_sql,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_bead_needs_task_ready_migration, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_task_ready_migration_sql, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_bead_needs_snoozed_status_migration,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_bead_snoozed_status_migration_sql, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_needs_resolution_migration, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_resolution_migration_sql, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_bead_needs_plus_one_evidence_migration,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_bead_plus_one_evidence_migration_sql,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_bead_read_store, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_read_event_store, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_read_legacy_jsonl, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_show, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_history, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_lost_notes, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_list, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_search, m)?)?;
    m.add_function(wrap_pyfunction!(py_plan_search, m)?)?;
    m.add_function(wrap_pyfunction!(py_plan_validate, m)?)?;
    m.add_function(wrap_pyfunction!(py_plan_frontmatter_schema, m)?)?;
    m.add_function(wrap_pyfunction!(py_plan_reference_parse, m)?)?;
    m.add_function(wrap_pyfunction!(py_plan_reference_render, m)?)?;
    m.add_function(wrap_pyfunction!(py_plan_reference_canonicalize, m)?)?;
    m.add_function(wrap_pyfunction!(py_plan_reference_resolve, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_plan_reference_resolution_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_artifact_ref_parse, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_ref_render, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_ref_canonicalize, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_ref_resolve, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_ref_list_normalize, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_ref_list_parse, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_ref_list_resolve, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_ref_list_resolution_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_ref_context_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_ref_path_filter_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_artifact_ref_filter_path_payloads, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_ref_scan_prompt, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_ref_wire_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_prompt_artifact_pool_filename, m)?)?;
    m.add_function(wrap_pyfunction!(py_prompt_artifact_manifest_parse, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_prompt_artifact_manifest_render_record,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_prompt_artifact_manifest_select, m)?)?;
    m.add_function(wrap_pyfunction!(py_prompt_artifact_rewrite_links, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_prompt_artifact_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_artifact_consumption_summary, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_consumption_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_artifact_files_query, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_file_materialize_vcs, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_file_store_economics, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_file_retention_plan, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_file_trash_store, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_file_trash_list, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_file_trash_restore, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_file_trash_purge, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_file_lifecycle_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_artifact_file_query_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_sdd_artifact_link_parse, m)?)?;
    m.add_function(wrap_pyfunction!(py_sdd_artifact_link_render, m)?)?;
    m.add_function(wrap_pyfunction!(py_sdd_artifact_link_upsert, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_sdd_plan_header_block_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_sdd_plan_header_block_parse, m)?)?;
    m.add_function(wrap_pyfunction!(py_sdd_plan_header_block_render, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_sdd_plan_header_block_upsert_section,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_sdd_plan_header_block_replace, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_sdd_plan_header_block_remove_section,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_bead_ready, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_blocked, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_stats, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_doctor, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_doctor_report, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_get_epic_children, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_show_issue_detail, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_resolve_id, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_init_store, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_create, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_update, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_update_many, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_append_note, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_plus_one, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_snooze, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_snooze_cancel, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_claim_for_agent_launch, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_claim_for_agent_wait, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_release_agent_claim, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_preclaim_epic_work, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_open, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_close, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_merge_event_streams, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_bead_merge_event_streams_with_relocation,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_bead_reduce_event_streams, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_event_store_manifest, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_repair_event_store_manifest, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_remove, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_remove_many, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_dep_add, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_dep_remove, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_mark_ready_to_work, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_unmark_ready_to_work, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_export_jsonl, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_sync_is_clean, m)?)?;
    m.add_function(wrap_pyfunction!(py_bead_build_epic_work_plan, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_bead_build_epic_work_plan_from_issues,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_bead_cli_execute, m)?)?;
    m.add_function(wrap_pyfunction!(py_read_notifications_snapshot, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_read_current_notifications_snapshot,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_apply_notification_state_update, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_apply_notification_state_update_counts,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_append_notification, m)?)?;
    m.add_function(wrap_pyfunction!(py_append_notification_counts, m)?)?;
    m.add_function(wrap_pyfunction!(py_rewrite_notifications, m)?)?;
    m.add_function(wrap_pyfunction!(py_rewrite_notifications_counts, m)?)?;
    m.add_function(wrap_pyfunction!(py_classify_notification_tabs, m)?)?;
    m.add_function(wrap_pyfunction!(py_read_prompt_stash_snapshot, m)?)?;
    m.add_function(wrap_pyfunction!(py_append_prompt_stash, m)?)?;
    m.add_function(wrap_pyfunction!(py_pop_prompt_stash, m)?)?;
    m.add_function(wrap_pyfunction!(py_set_prompt_stash_pinned, m)?)?;
    m.add_function(wrap_pyfunction!(py_rewrite_prompt_stash, m)?)?;
    m.add_function(wrap_pyfunction!(py_read_tasks_snapshot, m)?)?;
    m.add_function(wrap_pyfunction!(py_append_task, m)?)?;
    m.add_function(wrap_pyfunction!(py_update_task, m)?)?;
    m.add_function(wrap_pyfunction!(py_prune_tasks, m)?)?;
    m.add_function(wrap_pyfunction!(py_frontmatter_field_schema, m)?)?;
    m.add_function(wrap_pyfunction!(py_frontmatter_input_type_schema, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_frontmatter, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_frontmatter_field, m)?)?;
    m.add_class::<PyAtReferenceInventory>()?;
    m.add_class::<PyGlossaryCatalogHandle>()?;
    m.add_function(wrap_pyfunction!(py_at_reference_context, m)?)?;
    m.add_function(wrap_pyfunction!(py_artifact_ref_payload_inventory, m)?)?;
    m.add_function(wrap_pyfunction!(py_at_reference_menu, m)?)?;
    m.add_function(wrap_pyfunction!(py_fuzzy_match, m)?)?;
    m.add_function(wrap_pyfunction!(py_placeholder_completion, m)?)?;
    m.add_function(wrap_pyfunction!(py_placeholder_spans, m)?)?;
    m.add_function(wrap_pyfunction!(py_raw_placeholder_fields, m)?)?;
    m.add_function(wrap_pyfunction!(py_substitute_raw_placeholders, m)?)?;
    m.add_function(wrap_pyfunction!(py_placeholder_input_names, m)?)?;
    m.add_function(wrap_pyfunction!(py_axe_status_wire_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_classify_axe_status, m)?)?;
    m.add_function(wrap_pyfunction!(py_chop_engine_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_chop_result_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_chop_state_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_chop_result, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_chop_result, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_chop_proposal, m)?)?;
    m.add_function(wrap_pyfunction!(py_derive_chop_agent_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_evaluate_chop_decision, m)?)?;
    m.add_function(wrap_pyfunction!(py_apply_chop_checkpoint_update, m)?)?;
    m.add_function(wrap_pyfunction!(py_check_and_record_chop_once_per, m)?)?;
    m.add_function(wrap_pyfunction!(py_release_chop_once_per, m)?)?;
    m.add_function(wrap_pyfunction!(py_expand_chop_targets, m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_chop_duration, m)?)?;
    m.add_function(wrap_pyfunction!(py_split_axe_description, m)?)?;
    m.add_function(wrap_pyfunction!(py_validate_axe_config, m)?)?;
    m.add_function(wrap_pyfunction!(py_glossary_validate, m)?)?;
    m.add_function(wrap_pyfunction!(py_glossary_catalog, m)?)?;
    m.add_function(wrap_pyfunction!(py_compile_glossary_catalog, m)?)?;
    m.add_function(wrap_pyfunction!(py_config_field_model, m)?)?;
    m.add_function(wrap_pyfunction!(py_config_inventory, m)?)?;
    m.add_function(wrap_pyfunction!(py_config_plan_edit, m)?)?;
    m.add_function(wrap_pyfunction!(py_config_validate, m)?)?;
    m.add_function(wrap_pyfunction!(py_axe_config_compose, m)?)?;
    m.add_function(wrap_pyfunction!(py_axe_config_plan_entry, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_effort_override_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_effort_override_get, m)?)?;
    m.add_function(wrap_pyfunction!(py_effort_override_set_relative, m)?)?;
    m.add_function(wrap_pyfunction!(py_effort_override_set_until, m)?)?;
    m.add_function(wrap_pyfunction!(py_effort_override_clear, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_runner_limit_override_wire_schema_version,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_runner_limit_override_get, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_runner_limit_override_set_relative,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_runner_limit_override_set_until, m)?)?;
    m.add_function(wrap_pyfunction!(py_runner_limit_override_clear, m)?)?;
    m.add_function(wrap_pyfunction!(py_resolve_effective_effort, m)?)?;
    m.add_function(wrap_pyfunction!(py_sase_content_layout, m)?)?;
    m.add_function(wrap_pyfunction!(py_skill_reference_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_memory_reference_name, m)?)?;
    m.add_function(wrap_pyfunction!(py_memory_reference_stem, m)?)?;
    m.add_function(wrap_pyfunction!(py_reserved_memory_namespace_issue, m)?)?;
    m.add_function(wrap_pyfunction!(py_memory_note_issue, m)?)?;
    m.add_function(wrap_pyfunction!(py_skill_placement_issue, m)?)?;
    m.add_function(wrap_pyfunction!(py_resolve_layout_candidates, m)?)?;
    m.add_function(wrap_pyfunction!(py_agent_launch_wire_schema_version, m)?)?;
    m.add_function(wrap_pyfunction!(py_prepare_agent_launch, m)?)?;
    m.add_function(wrap_pyfunction!(py_spawn_prepared_agent_process, m)?)?;
    m.add_function(wrap_pyfunction!(py_allocate_launch_timestamp_batch, m)?)?;
    m.add_function(wrap_pyfunction!(py_plan_agent_launch_fanout, m)?)?;
    m.add_function(wrap_pyfunction!(py_inline_code_ranges, m)?)?;
    m.add_function(wrap_pyfunction!(py_resolve_agent_family_parent, m)?)?;
    m.add_function(wrap_pyfunction!(py_resolve_clan_summary, m)?)?;
    m.add_function(wrap_pyfunction!(py_resolve_clan_tribe, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_list_workspace_claims_from_content,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_plan_claim_workspace_from_content, m)?)?;
    m.add_function(wrap_pyfunction!(
        py_plan_transfer_workspace_claim_from_content,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        py_allocate_and_claim_workspace_from_content,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(py_telemetry_cleanup_matching_labels, m)?)?;
    m.add_function(wrap_pyfunction!(py_telemetry_record_batch, m)?)?;
    m.add_function(wrap_pyfunction!(py_telemetry_query_instant, m)?)?;
    m.add_function(wrap_pyfunction!(py_telemetry_query_range, m)?)?;
    m.add_function(wrap_pyfunction!(py_telemetry_prune, m)?)?;
    m.add_function(wrap_pyfunction!(py_telemetry_store_stats, m)?)?;
    m.add_function(wrap_pyfunction!(py_agent_stats_query_runs, m)?)?;
    m.add_function(wrap_pyfunction!(py_agent_stats_query_activity, m)?)?;
    Ok(())
}

pub use sase_core as core;

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::Python;
    use sase_core::bead::IssueTypeWire;
    use serde_json::json;
    use std::fs;
    use std::path::Path;
    use std::process::Command;

    fn append_json<'py>(
        py: Python<'py>,
        list: &Bound<'py, PyList>,
        value: JsonValue,
    ) {
        list.append(json_value_to_py(py, &value).unwrap()).unwrap();
    }

    fn git(repo: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_string()
    }

    fn init_git_repo(repo: &Path) {
        fs::create_dir_all(repo).unwrap();
        git(repo, &["init", "--quiet"]);
        git(repo, &["config", "user.name", "Binding Test"]);
        git(repo, &["config", "user.email", "binding@example.com"]);
        git(repo, &["config", "core.abbrev", "7"]);
    }

    #[test]
    fn vcs_log_binding_exposes_schema_and_parent_ids() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            use sase_core::vcs_log::parsers::{RECORD_SEP, UNIT_SEP};

            assert_eq!(py_vcs_log_wire_schema_version(), 3);

            let stdout = format!(
                "full{US}short{US}A{US}a@example.com{US}42{US}p1 p2{US}subject{US}body{RS}",
                US = UNIT_SEP,
                RS = RECORD_SEP,
            );
            let parsed = py_parse_git_log(py, &stdout).unwrap();
            let value = py_to_json_value(parsed.as_any()).unwrap();
            assert_eq!(
                value,
                json!([{
                    "full_id": "full",
                    "short_id": "short",
                    "author_name": "A",
                    "author_email": "a@example.com",
                    "timestamp": 42,
                    "parent_ids": ["p1", "p2"],
                    "subject": "subject",
                    "body": "body",
                    "presence": "unknown",
                }])
            );
        });
    }

    #[test]
    fn parse_merge_summary_binding_returns_dict_or_none() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let summary = py_parse_merge_summary(
                py,
                "Merge pull request #123 from org/feature",
                "\nFeature title\n\nDetails",
            )
            .unwrap();
            let value = py_to_json_value(summary.bind(py)).unwrap();
            assert_eq!(
                value,
                json!({
                    "kind": "pull_request",
                    "reference": "123",
                    "source": "org/feature",
                    "target": null,
                    "headline": "Feature title",
                })
            );

            assert!(py_parse_merge_summary(py, "Merge unknown shape", "")
                .unwrap()
                .is_none(py));
        });
    }

    fn commit_at(
        repo: &Path,
        timestamp: i64,
        subject: &str,
        body: &str,
    ) -> String {
        let date = format!("{timestamp} +0000");
        let mut command = Command::new("git");
        command.arg("-C").arg(repo).args([
            "commit",
            "--quiet",
            "--allow-empty",
            "-m",
            subject,
        ]);
        if !body.is_empty() {
            command.args(["-m", body]);
        }
        let output = command
            .env("GIT_AUTHOR_DATE", &date)
            .env("GIT_COMMITTER_DATE", &date)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git commit failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        git(repo, &["rev-parse", "HEAD"])
    }

    #[test]
    fn bead_doctor_binding_keeps_contexts_optional_and_marks_unavailable() {
        pyo3::prepare_freethreaded_python();
        let temp = tempfile::tempdir().unwrap();
        let beads_dir = temp.path().join("beads");
        fs::create_dir_all(&beads_dir).unwrap();
        fs::write(beads_dir.join("config.json"), "{}\n").unwrap();
        fs::write(beads_dir.join("beads.db"), "").unwrap();
        fs::write(beads_dir.join("issues.jsonl"), "").unwrap();

        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
            sase_core_rs(py, &module).unwrap();
            let doctor = module.getattr("bead_doctor").unwrap();
            let path = beads_dir.to_str().unwrap();

            let compatibility: Vec<String> =
                doctor.call1((path,)).unwrap().extract().unwrap();
            assert_eq!(compatibility, vec!["OK: no issues found"]);

            let unavailable: Vec<String> = doctor
                .call1((path, Vec::<String>::new()))
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(
                unavailable,
                vec![
                    "NOTE: bead design reference validation skipped: plan roots unavailable",
                    "NOTE: bead artifact reference validation skipped: reference context unavailable"
                ]
            );

            let available: Vec<String> = doctor
                .call1((path, Vec::<String>::new(), PyDict::new_bound(py)))
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(
                available,
                vec![
                    "NOTE: bead design reference validation skipped: plan roots unavailable"
                ]
            );
        });
    }

    #[test]
    fn bead_mutation_bindings_preserve_changed_and_epic_preclaim() {
        pyo3::prepare_freethreaded_python();
        let temp = tempfile::tempdir().unwrap();
        core_bead_init_store(temp.path(), "beads", "sase", "owner").unwrap();
        let beads_dir = temp.path().join("beads");
        let epic = core_bead_create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Epic".to_string(),
                issue_type: IssueTypeWire::Plan,
                now: Some("2026-01-01T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        let phase = core_bead_create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Phase".to_string(),
                issue_type: IssueTypeWire::Phase,
                parent_id: Some(epic.id.clone()),
                now: Some("2026-01-01T00:01:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();

        Python::with_gil(|py| {
            let path = beads_dir.to_str().unwrap();
            let first = py_bead_claim_for_agent_launch(
                py,
                path,
                &phase.id,
                "worker",
                Some("2026-01-01T00:02:00Z".to_string()),
            )
            .unwrap();
            assert!(py_to_json_value(first.bind(py)).unwrap()["changed"]
                .as_bool()
                .unwrap());

            let repeated = py_bead_claim_for_agent_launch(
                py,
                path,
                &phase.id,
                "worker",
                Some("2026-01-01T00:03:00Z".to_string()),
            )
            .unwrap();
            let repeated = py_to_json_value(repeated.bind(py)).unwrap();
            assert!(!repeated["changed"].as_bool().unwrap());
            assert_eq!(repeated["issue"]["updated_at"], "2026-01-01T00:02:00Z");

            let retained = py_bead_claim_for_agent_wait(
                py,
                path,
                &phase.id,
                "worker",
                Some("2026-01-01T00:04:00Z".to_string()),
            )
            .unwrap();
            let retained = py_to_json_value(retained.bind(py)).unwrap();
            assert!(!retained["changed"].as_bool().unwrap());
            assert_eq!(retained["message"], "");

            let fields = json_value_to_py(
                py,
                &json!({
                    "title": "Phase",
                    "now": "2026-01-01T00:05:00Z"
                }),
            )
            .unwrap();
            let fields = fields.bind(py).downcast::<PyDict>().unwrap();
            let unchanged =
                py_bead_update(py, path, &phase.id, fields).unwrap();
            assert!(!py_to_json_value(unchanged.bind(py)).unwrap()["changed"]
                .as_bool()
                .unwrap());

            let assignments = PyList::empty_bound(py);
            append_json(
                py,
                &assignments,
                json!({
                    "bead_id": phase.id,
                    "agent_name": "worker-2"
                }),
            );
            let preclaimed = py_bead_preclaim_epic_work(
                py,
                path,
                &epic.id,
                &assignments,
                Some("land".to_string()),
                Some("2026-01-01T00:06:00Z".to_string()),
            )
            .unwrap();
            let preclaimed = py_to_json_value(preclaimed.bind(py)).unwrap();
            assert!(preclaimed["changed"].as_bool().unwrap());
            assert_eq!(preclaimed["issue_ids"], json!([phase.id, epic.id]));
            assert_eq!(preclaimed["rollback_preclaims"][1]["bead_id"], epic.id);
        });
    }

    #[test]
    fn bead_update_many_binding_applies_batch_and_reports_unchanged() {
        pyo3::prepare_freethreaded_python();
        let temp = tempfile::tempdir().unwrap();
        core_bead_init_store(temp.path(), "beads", "sase", "owner").unwrap();
        let beads_dir = temp.path().join("beads");
        let first = core_bead_create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "First task".to_string(),
                issue_type: IssueTypeWire::Task,
                size: Some(PhaseSizeWire::Small),
                now: Some("2026-01-01T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        let second = core_bead_create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Second task".to_string(),
                issue_type: IssueTypeWire::Task,
                size: Some(PhaseSizeWire::Small),
                now: Some("2026-01-01T00:01:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();

        Python::with_gil(|py| {
            let path = beads_dir.to_str().unwrap();
            let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
            sase_core_rs(py, &module).unwrap();
            assert!(module.getattr("bead_update_many").is_ok());

            let fields = json_value_to_py(
                py,
                &json!({
                    "status": "in_progress",
                    "now": "2026-01-01T00:02:00Z"
                }),
            )
            .unwrap();
            let fields = fields.bind(py).downcast::<PyDict>().unwrap();

            let first_call = py_bead_update_many(
                py,
                path,
                vec![first.id.clone(), second.id.clone()],
                fields,
            )
            .unwrap();
            let first_call = py_to_json_value(first_call.bind(py)).unwrap();
            assert!(first_call["changed"].as_bool().unwrap());
            assert_eq!(
                first_call["issue_ids"],
                json!([first.id.clone(), second.id.clone()])
            );
            assert_eq!(first_call["unchanged_ids"], json!([]));
            assert_eq!(first_call["issues"][0]["status"], "in_progress");
            assert_eq!(first_call["issues"][1]["status"], "in_progress");

            let repeat_call = py_bead_update_many(
                py,
                path,
                vec![first.id.clone(), second.id.clone()],
                fields,
            )
            .unwrap();
            let repeat_call = py_to_json_value(repeat_call.bind(py)).unwrap();
            assert!(!repeat_call["changed"].as_bool().unwrap());
            assert_eq!(repeat_call["issue_ids"], json!([]));
            assert_eq!(
                repeat_call["unchanged_ids"],
                json!([first.id, second.id])
            );
        });
    }

    #[test]
    fn bead_plus_one_binding_exports_structured_atomic_result() {
        pyo3::prepare_freethreaded_python();
        let temp = tempfile::tempdir().unwrap();
        core_bead_init_store(temp.path(), "beads", "sase", "owner").unwrap();
        let beads_dir = temp.path().join("beads");
        let task = core_bead_create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Task".to_string(),
                issue_type: IssueTypeWire::Task,
                size: Some(PhaseSizeWire::Small),
                created_by: Some("creator-agent".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();

        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
            sase_core_rs(py, &module).unwrap();
            assert!(module.getattr("bead_plus_one").is_ok());
            let result = py_bead_plus_one(
                py,
                beads_dir.to_str().unwrap(),
                &task.id,
                "reporter-agent",
                "reproduced",
                Some(vec!["research:202608/repro.md".to_string()]),
                Some("2026-01-02T00:00:00Z".to_string()),
                None,
            )
            .unwrap();
            let result = py_to_json_value(result.bind(py)).unwrap();
            assert_eq!(result["issue"]["status"], "ready");
            assert_eq!(
                result["issue"]["plus_one_evidence"][0]["reporter"],
                "reporter-agent"
            );
        });
    }

    #[test]
    fn bead_snooze_bindings_round_trip_the_whole_lifecycle() {
        pyo3::prepare_freethreaded_python();
        let temp = tempfile::tempdir().unwrap();
        core_bead_init_store(temp.path(), "beads", "sase", "owner").unwrap();
        let beads_dir = temp.path().join("beads");
        let task = core_bead_create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Task".to_string(),
                issue_type: IssueTypeWire::Task,
                size: Some(PhaseSizeWire::Small),
                created_by: Some("creator-agent".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        let beads_dir = beads_dir.to_str().unwrap();

        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
            sase_core_rs(py, &module).unwrap();
            for name in [
                "bead_snooze",
                "bead_snooze_cancel",
                "bead_needs_snoozed_status_migration",
                "bead_snoozed_status_migration_sql",
            ] {
                assert!(module.getattr(name).is_ok(), "missing {name}");
            }

            let snoozed = py_bead_snooze(
                py,
                beads_dir,
                &task.id,
                "2026-01-04T00:00:00Z",
                Some(2),
                "waiting on upstream",
                "owner",
                Some("2026-01-01T00:02:00Z".to_string()),
            )
            .unwrap();
            let snoozed = py_to_json_value(snoozed.bind(py)).unwrap();
            assert_eq!(snoozed["issue"]["status"], "snoozed");
            assert_eq!(
                snoozed["issue"]["snooze"]["until"],
                "2026-01-04T00:00:00Z"
            );
            assert_eq!(snoozed["issue"]["snooze"]["plus_one_target"], 2);

            let canceled = py_bead_snooze_cancel(
                py,
                beads_dir,
                &task.id,
                "owner",
                Some("2026-01-02T00:00:00Z".to_string()),
            )
            .unwrap();
            let canceled = py_to_json_value(canceled.bind(py)).unwrap();
            assert_eq!(canceled["issue"]["status"], "ready");
            assert!(canceled["issue"].get("snooze").is_none());
        });
    }

    #[test]
    fn background_task_store_bindings_round_trip_python_dicts() {
        pyo3::prepare_freethreaded_python();
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("tasks.jsonl");
        let path = path.to_str().unwrap();
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
            sase_core_rs(py, &module).unwrap();
            for name in [
                "read_tasks_snapshot",
                "append_task",
                "update_task",
                "prune_tasks",
            ] {
                assert!(module.getattr(name).is_ok(), "missing {name}");
            }

            let task = json_value_to_py(
                py,
                &json!({
                    "task_id": "task-one",
                    "label": "Binding task",
                    "kind": "command",
                    "status": "pending",
                    "command": ["true"],
                    "cwd": "/tmp",
                    "project": "sase",
                    "workspace_num": 16,
                    "session_id": "session",
                    "session_label": "ace",
                    "origin": "test",
                    "cl_name": null,
                    "tags": ["binding", "binding"],
                    "pid": null,
                    "pgid": null,
                    "exit_code": null,
                    "phase": "queued",
                    "message": null,
                    "created_at": "2026-07-25T12:00:00Z",
                    "started_at": null,
                    "finished_at": null,
                    "log_path": "/tmp/task-one.log"
                }),
            )
            .unwrap();
            let task = task.bind(py).downcast::<PyDict>().unwrap();
            let appended = py_append_task(py, path, task, 10).unwrap();
            let appended = py_to_json_value(appended.bind(py)).unwrap();
            assert_eq!(appended["snapshot"]["tasks"][0]["task_id"], "task-one");
            assert_eq!(
                appended["snapshot"]["tasks"][0]["tags"],
                json!(["binding"])
            );

            let update = json_value_to_py(
                py,
                &json!({
                    "task_id": "task-one",
                    "status": "running",
                    "session_id": null,
                    "phase": null,
                    "pid": 42
                }),
            )
            .unwrap();
            let update = update.bind(py).downcast::<PyDict>().unwrap();
            let updated = py_update_task(py, path, update).unwrap();
            let updated = py_to_json_value(updated.bind(py)).unwrap();
            assert_eq!(updated["task"]["status"], "running");
            assert_eq!(updated["task"]["session_id"], JsonValue::Null);
            assert_eq!(updated["task"]["phase"], JsonValue::Null);
            assert_eq!(updated["task"]["pid"], 42);

            let snapshot = py_read_tasks_snapshot(py, path).unwrap();
            let snapshot = py_to_json_value(snapshot.bind(py)).unwrap();
            assert_eq!(snapshot["tasks"][0]["task_id"], "task-one");
            let pruned = py_prune_tasks(py, path, 0).unwrap();
            let pruned = py_to_json_value(pruned.bind(py)).unwrap();
            assert!(pruned["pruned_task_ids"].as_array().unwrap().is_empty());
        });
    }

    #[test]
    fn commit_footer_bindings_convert_linked_payloads() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            assert_eq!(
                py_commit_footer_wire_schema_version(),
                COMMIT_FOOTER_WIRE_SCHEMA_VERSION
            );
            let updates = PyList::empty_bound(py);
            append_json(
                py,
                &updates,
                json!({
                    "key": "PLAN",
                    "label": "202607/p.md",
                    "destination": "https://github.com/o/r/blob/main/202607/p.md",
                    "reference_id": null
                }),
            );
            let message =
                py_update_commit_footer("Subject", &updates, vec![]).unwrap();
            assert!(message.contains("SASE_PLAN=[202607/p.md][1]"));

            let parsed = py_parse_commit_footer(py, &message).unwrap();
            let value = py_to_json_value(parsed.bind(py)).unwrap();
            assert_eq!(value["schema_version"], json!(1));
            assert_eq!(value["tags"][0]["label"], json!("202607/p.md"));
            assert_eq!(
                value["tags"][0]["destination"],
                json!("https://github.com/o/r/blob/main/202607/p.md")
            );
        });
    }

    #[test]
    fn commit_subject_bindings_round_trip_wire_payload() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            assert_eq!(
                py_commit_subject_wire_schema_version(),
                COMMIT_SUBJECT_WIRE_SCHEMA_VERSION
            );
            let allowed_types = py_default_commit_subject_types();
            assert_eq!(
                allowed_types.first().map(String::as_str),
                Some("build")
            );

            let parsed = py_parse_commit_subject(
                py,
                "feat(binding)!: expose subject parser\n\nBody",
                allowed_types,
            )
            .unwrap();
            let value = py_to_json_value(parsed.bind(py)).unwrap();
            assert_eq!(value["schema_version"], json!(1));
            assert_eq!(
                value["subject"],
                json!("feat(binding)!: expose subject parser")
            );
            assert_eq!(value["valid"], json!(true));
            assert_eq!(value["exempt"], json!(false));
            assert_eq!(value["commit_type"], json!("feat"));
            assert_eq!(value["scope"], json!("binding"));
            assert_eq!(value["breaking"], json!(true));
            assert_eq!(value["description"], json!("expose subject parser"));
            assert_eq!(value["violation"], JsonValue::Null);
            assert_eq!(value["found_type"], JsonValue::Null);
        });
    }

    #[test]
    fn machine_hood_bindings_qualify_strip_and_classify() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|_py| {
            py_validate_machine_name("athena").unwrap();
            assert!(py_validate_machine_name("Athena").is_err());
            assert!(py_validate_machine_name("").is_err());

            assert_eq!(
                py_qualify_machine_agent_name("foo--code", "athena"),
                "athena.foo--code"
            );
            assert_eq!(
                py_qualify_machine_agent_name("athena.foo", "athena"),
                "athena.foo"
            );
            assert_eq!(
                py_strip_machine_agent_name("athena.foo", "athena"),
                "foo"
            );
            assert_eq!(
                py_strip_machine_agent_name("zeus.bar", "athena"),
                "zeus.bar"
            );

            let known = vec!["athena".to_string(), "zeus".to_string()];
            assert_eq!(
                py_machine_hood_of("zeus.bar", known.clone()),
                Some("zeus".to_string())
            );
            assert_eq!(py_machine_hood_of("foo", known), None);
        });
    }

    #[test]
    fn agent_identity_bindings_are_exported_and_preserve_shapes() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
            sase_core_rs(py, &module).unwrap();
            for name in [
                "validate_agent_name",
                "validate_agent_username",
                "validate_agent_owner",
                "classify_agent_ownership",
                "classify_legacy_v1_group_ownership",
                "commit_shas_equivalent",
                "normalize_agent_archive_name",
                "globalize_agent_name",
                "globalize_legacy_agent_name",
                "strip_global_agent_name",
                "localize_agent_name",
                "parse_agent_family_name",
                "agent_local_hood",
                "agent_name_in_hood",
                "agent_name_ancestors",
                "agent_link_target",
                "agent_relationship_schema_version",
                "validate_agent_relationship_batch",
                "rewrite_agent_relationship_batch",
            ] {
                assert!(module.getattr(name).is_ok(), "missing {name}");
            }

            py_validate_agent_username("alice").unwrap();
            assert!(py_validate_agent_username("Alice").is_err());
            py_validate_agent_name("foo.bar--code").unwrap();
            assert!(py_validate_agent_name("foo--code.bar").is_err());
            py_validate_agent_owner("alice", "athena").unwrap();
            assert!(py_validate_agent_owner("alice", "athena1").is_err());
            assert_eq!(
                py_classify_agent_ownership(
                    "zeus",
                    "alice",
                    "athena",
                    Some("alice"),
                )
                .unwrap(),
                "same_user_other_machine"
            );
            assert_eq!(
                py_classify_legacy_v1_group_ownership(
                    "athena", "alice", "athena", false, 1, 2,
                )
                .unwrap(),
                "owner_observed"
            );
            assert_eq!(
                py_classify_legacy_v1_group_ownership(
                    "zeus", "alice", "athena", true, 2, 2,
                )
                .unwrap(),
                "foreign"
            );
            assert!(py_classify_legacy_v1_group_ownership(
                "athena", "alice", "athena", false, 2, 1,
            )
            .is_err());
            assert!(py_commit_shas_equivalent(
                "d7e06b77b",
                "d7e06b77b42d89ecf4bb1538c6f89c6fe700124e",
            ));
            assert_eq!(
                py_globalize_agent_name(
                    "260722.foo.bar--code",
                    "alice",
                    "athena"
                )
                .unwrap(),
                "alice.athena.foo.bar--code"
            );
            assert_eq!(
                py_localize_agent_name(
                    "bob.athena.foo",
                    "athena",
                    "alice",
                    "athena",
                    Some("bob"),
                )
                .unwrap(),
                "bob.athena.foo"
            );

            let family =
                py_parse_agent_family_name(py, "foo.bar--code").unwrap();
            assert_eq!(
                py_to_json_value(family.bind(py)).unwrap(),
                json!({
                    "kind": "member",
                    "family_name": "foo.bar",
                    "member_role": "code"
                })
            );
            let historical =
                py_parse_agent_family_name(py, "fi--code.f0--plan").unwrap();
            assert_eq!(
                py_to_json_value(historical.bind(py)).unwrap(),
                json!({
                    "kind": "member",
                    "family_name": "fi--code.f0",
                    "member_role": "plan"
                })
            );
            assert_eq!(py_agent_local_hood("4x--epic.f-0").unwrap(), "4x");
            assert!(py_agent_name_in_hood("fi--code.f0--code", "fi").unwrap());
            assert_eq!(
                py_agent_name_ancestors("fi--code.f0--code").unwrap(),
                ["fi", "fi--code.f0"]
            );
            let link =
                py_agent_link_target(py, "foo.bar--code", "alice", "athena")
                    .unwrap();
            assert_eq!(
                py_to_json_value(link.bind(py)).unwrap(),
                json!({
                    "kind": "family",
                    "path": "families/alice.athena.foo.bar.md",
                    "anchor": "member-code"
                })
            );
        });
    }

    #[test]
    fn relationship_bindings_validate_and_rewrite_plain_dicts() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            assert_eq!(py_agent_relationship_schema_version(), 2);
            let batch_obj = json_value_to_py(
                py,
                &json!({
                    "schema_version": 2,
                    "owner": {
                        "username": "alice",
                        "machine_name": "athena"
                    },
                    "runs": [
                        {
                            "source_run_id": "run-1",
                            "global_name": "alice.athena.foo",
                            "owner": {
                                "username": "alice",
                                "machine_name": "athena"
                            }
                        },
                        {
                            "source_run_id": "run-2",
                            "global_name": "alice.athena.foo--code",
                            "owner": {
                                "username": "alice",
                                "machine_name": "athena"
                            }
                        }
                    ],
                    "containers": [{
                        "kind": "family",
                        "global_name": "alice.athena.foo",
                        "owner": {
                            "username": "alice",
                            "machine_name": "athena"
                        },
                        "member_source_run_ids": ["run-1", "run-2"]
                    }],
                    "relationships": [{
                        "kind": "parent",
                        "source_run_id": "run-2",
                        "target": {
                            "kind": "source_run_id",
                            "source_run_id": "run-1"
                        },
                        "required": true
                    }]
                }),
            )
            .unwrap();
            let batch = batch_obj.bind(py).downcast::<PyDict>().unwrap();
            let summary =
                py_validate_agent_relationship_batch(py, batch).unwrap();
            let summary = py_to_json_value(summary.bind(py)).unwrap();
            assert_eq!(summary["run_count"], json!(2));
            assert_eq!(summary["run_order"], json!(["run-1", "run-2"]));

            let mapping_obj = json_value_to_py(
                py,
                &json!({"run-1": "dest-1", "run-2": "dest-2"}),
            )
            .unwrap();
            let mapping = mapping_obj.bind(py).downcast::<PyDict>().unwrap();
            let rewritten =
                py_rewrite_agent_relationship_batch(py, batch, mapping)
                    .unwrap();
            let rewritten = py_to_json_value(rewritten.bind(py)).unwrap();
            assert_eq!(
                rewritten["runs"][0]["destination_run_id"],
                json!("dest-1")
            );
            assert_eq!(
                rewritten["relationships"][0]["source_destination_run_id"],
                json!("dest-2")
            );
            assert_eq!(
                rewritten["relationships"][0]["target"]["destination_run_id"],
                json!("dest-1")
            );

            let malformed_obj = json_value_to_py(
                py,
                &json!({
                    "schema_version": 2,
                    "owner": {
                        "username": "Alice",
                        "machine_name": "athena"
                    },
                    "runs": [],
                    "containers": [],
                    "relationships": []
                }),
            )
            .unwrap();
            let malformed =
                malformed_obj.bind(py).downcast::<PyDict>().unwrap();
            assert!(
                py_validate_agent_relationship_batch(py, malformed).is_err()
            );
        });
    }

    #[test]
    fn compose_snippet_catalog_binding_returns_plain_dict_shape() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
            module
                .add_function(
                    wrap_pyfunction!(py_compose_snippet_catalog, &module)
                        .unwrap(),
                )
                .unwrap();
            let templates = PyDict::new_bound(py);
            templates.set_item("foo", "foo #[helper] $1$0").unwrap();
            templates.set_item("helper", "helper $1$0").unwrap();

            let result = module
                .getattr("compose_snippet_catalog")
                .unwrap()
                .call1((templates,))
                .unwrap();
            let value = py_to_json_value(&result).unwrap();

            assert_eq!(
                value,
                json!({
                    "templates": {
                        "Foo": "Foo helper $1 $2$0",
                        "Helper": "Helper $1$0",
                        "foo": "foo helper $1 $2$0",
                        "helper": "helper $1$0"
                    },
                    "alias_provenance": {
                        "Foo": "foo",
                        "Helper": "helper"
                    }
                })
            );
        });
    }

    #[test]
    fn effort_override_bindings_round_trip_and_resolve() {
        pyo3::prepare_freethreaded_python();
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().to_string_lossy();
        let now = 1_800_000_000.0;
        Python::with_gil(|py| {
            assert_eq!(
                py_effort_override_wire_schema_version(),
                sase_core::EFFORT_OVERRIDE_WIRE_SCHEMA_VERSION
            );
            let written = py_effort_override_set_relative(
                py,
                &home,
                "high",
                "binding-test",
                Some(900.0),
                Some(now),
            )
            .unwrap();
            let written_value = py_to_json_value(written.bind(py)).unwrap();
            assert_eq!(written_value["effort"], json!("high"));
            assert_eq!(written_value["expires_at"], json!(now + 900.0));

            let loaded = py_effort_override_get(py, &home, Some(now)).unwrap();
            assert_eq!(
                py_to_json_value(loaded.bind(py)).unwrap(),
                written_value
            );

            let resolved = py_resolve_effective_effort(
                py,
                None,
                None,
                Some("high"),
                Some("low"),
            )
            .unwrap();
            let resolved_value = py_to_json_value(resolved.bind(py)).unwrap();
            assert_eq!(resolved_value["level"], json!("high"));
            assert_eq!(resolved_value["source"], json!("temporary_override"));
            assert_eq!(resolved_value["explicit"], json!(false));

            assert!(py_effort_override_clear(&home).unwrap());
            assert!(!py_effort_override_clear(&home).unwrap());
        });
    }

    #[test]
    fn effort_override_binding_rejects_invalid_values() {
        pyo3::prepare_freethreaded_python();
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().to_string_lossy();
        Python::with_gil(|py| {
            let error = py_effort_override_set_until(
                py,
                &home,
                "turbo",
                2.0,
                "test",
                Some(1.0),
            )
            .unwrap_err();
            assert!(error.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn runner_limit_override_bindings_round_trip_and_replace() {
        pyo3::prepare_freethreaded_python();
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().to_string_lossy();
        let now = 1_800_000_000.0;
        Python::with_gil(|py| {
            assert_eq!(
                py_runner_limit_override_wire_schema_version(),
                sase_core::RUNNER_LIMIT_OVERRIDE_WIRE_SCHEMA_VERSION
            );
            let first = py_runner_limit_override_set_relative(
                py,
                &home,
                1,
                "binding-test",
                Some(900.0),
                Some(now),
            )
            .unwrap();
            let first_value = py_to_json_value(first.bind(py)).unwrap();
            assert_eq!(first_value["limit"], json!(1));
            assert_eq!(first_value["expires_at"], json!(now + 900.0));

            let replacement = py_runner_limit_override_set_until(
                py,
                &home,
                12,
                now + 60.0,
                "binding-test",
                Some(now),
            )
            .unwrap();
            let replacement_value =
                py_to_json_value(replacement.bind(py)).unwrap();
            assert_eq!(replacement_value["limit"], json!(12));

            let loaded =
                py_runner_limit_override_get(py, &home, Some(now)).unwrap();
            assert_eq!(
                py_to_json_value(loaded.bind(py)).unwrap(),
                replacement_value
            );
            assert!(py_runner_limit_override_clear(&home).unwrap());
            assert!(!py_runner_limit_override_clear(&home).unwrap());
        });
    }

    #[test]
    fn runner_limit_override_binding_rejects_invalid_values() {
        pyo3::prepare_freethreaded_python();
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().to_string_lossy();
        Python::with_gil(|py| {
            let error = py_runner_limit_override_set_relative(
                py,
                &home,
                0,
                "test",
                None,
                Some(1.0),
            )
            .unwrap_err();
            assert!(error.is_instance_of::<PyValueError>(py));

            let error = py_runner_limit_override_set_until(
                py,
                &home,
                1,
                1.0,
                "test",
                Some(1.0),
            )
            .unwrap_err();
            assert!(error.is_instance_of::<PyValueError>(py));
        });
    }

    fn healthy_axe_status_request_json() -> JsonValue {
        json!({
            "schema_version": 1,
            "generated_at": "2026-07-23T12:00:00-04:00",
            "desired_state": {
                "state": "running",
                "source": "binding-test",
                "timestamp": "2026-07-23T11:55:00-04:00"
            },
            "orchestrator": {
                "lifecycle_lock_held": true,
                "lock_holder": {"pid": 100, "live": true},
                "orchestrator_pid_file": {"pid": 100, "live": true},
                "legacy_pid_file": {"pid": null, "live": null}
            },
            "maintenance": null,
            "hook_runners": {"current": 1, "maximum": 3},
            "agent_runners": {"current": 2, "maximum": 4},
            "lumberjacks": [{
                "name": "hooks",
                "configured": true,
                "interval_seconds": 60,
                "configured_chops": ["zeta", "alpha", "zeta"],
                "recorded_pid": 200,
                "reported_state": "running",
                "process_live": true,
                "started_at": "2026-07-23T11:50:00-04:00",
                "start_age_seconds": 600,
                "heartbeat_at": "2026-07-23T11:59:30-04:00",
                "heartbeat_age_seconds": 30,
                "cycles_run": 10,
                "errors_encountered": 2,
                "uptime_seconds": 600
            }],
            "latest_lifecycle_event": {
                "event": "start",
                "timestamp": "2026-07-23T11:50:00-04:00",
                "source": "binding-test",
                "outcome": "started",
                "success": true,
                "reason": null,
                "orchestrator_pid": 100,
                "age_seconds": 600
            },
            "collection_error": null
        })
    }

    #[test]
    fn axe_status_binding_returns_exact_plain_python_shape() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            assert_eq!(
                py_axe_status_wire_schema_version(),
                AXE_STATUS_SCHEMA_VERSION
            );
            let request_obj =
                json_value_to_py(py, &healthy_axe_status_request_json())
                    .unwrap();
            let request = request_obj.bind(py).downcast::<PyDict>().unwrap();
            let result = py_classify_axe_status(py, request).unwrap();
            let value = py_to_json_value(result.bind(py)).unwrap();
            assert_eq!(
                value,
                json!({
                    "schema_version": 1,
                    "generated_at": "2026-07-23T12:00:00-04:00",
                    "state": "running",
                    "health": "healthy",
                    "summary": "AXE is running and healthy.",
                    "exit_code": 0,
                    "desired_state": {
                        "state": "running",
                        "source": "binding-test",
                        "timestamp": "2026-07-23T11:55:00-04:00"
                    },
                    "orchestrator": {
                        "state": "running",
                        "coherence": "coherent",
                        "live_pids": [100],
                        "lifecycle_lock_held": true,
                        "lock_holder": {"pid": 100, "live": true},
                        "orchestrator_pid_file": {"pid": 100, "live": true},
                        "legacy_pid_file": {"pid": null, "live": null}
                    },
                    "maintenance": null,
                    "hook_runners": {"current": 1, "maximum": 3},
                    "agent_runners": {"current": 2, "maximum": 4},
                    "lumberjacks": [{
                        "name": "hooks",
                        "state": "running",
                        "stale_threshold_seconds": 180,
                        "configured": true,
                        "interval_seconds": 60,
                        "configured_chops": ["alpha", "zeta"],
                        "recorded_pid": 200,
                        "reported_state": "running",
                        "process_live": true,
                        "started_at": "2026-07-23T11:50:00-04:00",
                        "start_age_seconds": 600,
                        "heartbeat_at": "2026-07-23T11:59:30-04:00",
                        "heartbeat_age_seconds": 30,
                        "cycles_run": 10,
                        "errors_encountered": 2,
                        "uptime_seconds": 600
                    }],
                    "latest_lifecycle_event": {
                        "event": "start",
                        "timestamp": "2026-07-23T11:50:00-04:00",
                        "source": "binding-test",
                        "outcome": "started",
                        "success": true,
                        "reason": null,
                        "orchestrator_pid": 100,
                        "age_seconds": 600
                    },
                    "issues": [],
                    "collection_error": null
                })
            );
            assert!(result.bind(py).downcast::<PyDict>().is_ok());
            let keys = value
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>();
            assert_eq!(
                keys,
                vec![
                    "schema_version",
                    "generated_at",
                    "state",
                    "health",
                    "summary",
                    "exit_code",
                    "desired_state",
                    "orchestrator",
                    "maintenance",
                    "hook_runners",
                    "agent_runners",
                    "lumberjacks",
                    "latest_lifecycle_event",
                    "issues",
                    "collection_error",
                ]
            );
        });
    }

    #[test]
    fn axe_status_binding_maps_schema_structural_and_unknown_errors_to_value_error(
    ) {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let mut schema = healthy_axe_status_request_json();
            schema["schema_version"] = json!(2);

            let mut structural = healthy_axe_status_request_json();
            structural["lumberjacks"][0]["interval_seconds"] = JsonValue::Null;

            let mut unknown = healthy_axe_status_request_json();
            unknown
                .as_object_mut()
                .unwrap()
                .insert("surprise".to_string(), json!(true));

            for (value, expected) in [
                (schema, "schema_version_mismatch"),
                (structural, "missing_interval"),
                (unknown, "unknown field `surprise`"),
            ] {
                let request_obj = json_value_to_py(py, &value).unwrap();
                let request =
                    request_obj.bind(py).downcast::<PyDict>().unwrap();
                let error = py_classify_axe_status(py, request).unwrap_err();
                assert!(error.is_instance_of::<PyValueError>(py));
                assert!(error.to_string().contains(expected), "{}", error);
            }
        });
    }

    #[test]
    fn chop_clan_contracts_round_trip_through_python_bindings() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let proposal_obj = json_value_to_py(
                py,
                &json!({
                    "prompt": "Split the file.",
                    "workspace": "git:sase",
                    "agent_name": "split_file.src_lib.a1b2",
                    "clan": "toobig-@",
                    "clan_summary": "[bold]Large module[/bold]"
                }),
            )
            .unwrap();
            let proposal = proposal_obj.bind(py).downcast::<PyDict>().unwrap();
            let normalized =
                py_validate_chop_proposal(py, proposal, 0, None).unwrap();
            let normalized = py_to_json_value(normalized.bind(py)).unwrap();
            assert_eq!(normalized["clan"], json!("toobig-@"));
            assert_eq!(
                normalized["clan_summary"],
                json!("[bold]Large module[/bold]")
            );
            assert_eq!(
                normalized["agent_name"],
                json!("split_file.src_lib.a1b2")
            );

            let decision_obj = json_value_to_py(
                py,
                &json!({
                    "schema_version": 1,
                    "inhibit_if": [{
                        "provider": "agent_clan",
                        "name_prefix": "toobig-"
                    }],
                    "agents": [{
                        "name": "toobig-0.split_file.src_lib.a1b2",
                        "agent_clan": "toobig-0",
                        "active": true
                    }],
                    "now": "2026-07-19T12:00:00Z"
                }),
            )
            .unwrap();
            let decision = decision_obj.bind(py).downcast::<PyDict>().unwrap();
            let evaluated = py_evaluate_chop_decision(py, decision).unwrap();
            let evaluated = py_to_json_value(evaluated.bind(py)).unwrap();
            assert_eq!(evaluated["outcome"], json!("skip"));
            assert_eq!(evaluated["provider"], json!("agent_clan"));

            let config_obj = json_value_to_py(
                py,
                &json!({
                    "schema_version": 1,
                    "config": {"lumberjacks": {"guard": {"chops": {
                        "split": {"inhibit_if": {"agent_clan": {
                            "name_prefix": "toobig-"
                        }}}
                    }}}}
                }),
            )
            .unwrap();
            let config = config_obj.bind(py).downcast::<PyDict>().unwrap();
            let diagnostics = py_validate_axe_config(py, config).unwrap();
            assert_eq!(
                py_to_json_value(diagnostics.bind(py)).unwrap(),
                json!([])
            );
        });
    }

    #[test]
    fn required_axe_descriptions_round_trip_through_python_binding() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let config_obj = json_value_to_py(
                py,
                &json!({
                    "schema_version": 1,
                    "require_descriptions": true,
                    "config": {"axe": {"lumberjacks": {"checks": {
                        "chops": {"hooks": {}}
                    }}}}
                }),
            )
            .unwrap();
            let config = config_obj.bind(py).downcast::<PyDict>().unwrap();

            let diagnostics = py_validate_axe_config(py, config).unwrap();
            let diagnostics = py_to_json_value(diagnostics.bind(py)).unwrap();

            assert_eq!(diagnostics.as_array().unwrap().len(), 2);
            assert!(diagnostics.as_array().unwrap().iter().any(|item| {
                item["code"] == "required_missing"
                    && item["path"] == "axe.lumberjacks.checks.description"
            }));
            assert!(diagnostics.as_array().unwrap().iter().any(|item| {
                item["code"] == "required_missing"
                    && item["path"]
                        == "axe.lumberjacks.checks.chops.hooks.description"
            }));
        });
    }

    #[test]
    fn axe_description_split_round_trips_through_python_binding() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
            sase_core_rs(py, &module).unwrap();

            let result = module
                .getattr("split_axe_description")
                .unwrap()
                .call1(("  Run checks  \r\n\r\nBody line  \r\n",))
                .unwrap()
                .extract::<(String, String)>()
                .unwrap();

            assert_eq!(
                result,
                ("Run checks".to_string(), "Body line".to_string())
            );
        });
    }

    fn spec_json(name: &str, status: &str, parent: Option<&str>) -> JsonValue {
        json!({
            "schema_version": 3,
            "name": name,
            "project_basename": "proj",
            "file_path": "proj.sase",
            "source_span": {
                "file_path": "proj.sase",
                "start_line": 1,
                "end_line": 10
            },
            "status": status,
            "parent": parent,
            "pr_url": null,
            "bug": null,
            "description": format!("description for {name}"),
            "commits": [],
            "hooks": [],
            "comments": [],
            "mentors": [],
            "timestamps": [],
            "deltas": []
        })
    }

    fn spec_list<'py>(
        py: Python<'py>,
        specs: &[JsonValue],
    ) -> Bound<'py, PyList> {
        let list = PyList::empty_bound(py);
        for spec in specs {
            append_json(py, &list, spec.clone());
        }
        list
    }

    fn bools_from_py_list(list: &Bound<'_, PyList>) -> Vec<bool> {
        list.iter()
            .map(|item| item.extract::<bool>().unwrap())
            .collect()
    }

    #[test]
    fn parse_patch_project_bytes_binding_emits_canonical_shape_and_query_accepts_it(
    ) {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
            sase_core_rs(py, &module).unwrap();

            let src = "\
## Patch
NAME: alpha
STATUS: WIP
STITCHES:
  (2a) Proposed stitch
HOOKS:
  just test
      | (2a) [260101_120000] PASSED (3s)
MENTORS:
  (2a) profileA[1/1]
";
            let bytes = PyBytes::new_bound(py, src.as_bytes());
            let result = module
                .getattr("parse_patch_project_bytes")
                .unwrap()
                .call1(("proj.sase", bytes))
                .unwrap();
            let value = py_to_json_value(&result).unwrap();
            let patch = &value.as_array().unwrap()[0];

            assert!(patch.get("stitches").is_some());
            assert!(patch.get("commits").is_none());
            assert_eq!(patch["stitches"][0]["proposal_letter"], json!("a"));
            assert_eq!(
                patch["hooks"][0]["status_lines"][0]["stitch_id"],
                json!("2a")
            );
            assert!(patch["hooks"][0]["status_lines"][0]
                .get("commit_entry_num")
                .is_none());
            assert_eq!(patch["mentors"][0]["stitch_id"], json!("2a"));
            assert!(patch["mentors"][0].get("entry_id").is_none());

            let specs = PyList::empty_bound(py);
            append_json(py, &specs, patch.clone());
            let results =
                py_evaluate_query_many(py, "name:alpha", &specs).unwrap();
            assert_eq!(bools_from_py_list(&results), vec![true]);
        });
    }

    #[test]
    fn memory_xprompt_bindings_expose_the_shared_contract() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            assert_eq!(py_memory_reference_name("glossary"), "memory/glossary");
            assert_eq!(
                py_memory_reference_stem("memory/glossary").as_deref(),
                Some("glossary")
            );
            assert_eq!(py_memory_reference_stem("glossary"), None);

            let layout = py_to_json_value(
                py_sase_content_layout(
                    py,
                    "/home/alice",
                    Some("/repo"),
                    None,
                    Some("demo"),
                )
                .unwrap()
                .bind(py),
            )
            .unwrap();
            assert_eq!(layout["schema_version"], json!(5));
            assert_eq!(
                layout["memory_sources"][0]["paths"]["canonical"]["path"],
                json!("/repo/sase/memory")
            );
            assert_eq!(
                layout["memory_sources"][0]["paths"]["read_policy"],
                json!("error")
            );
            assert_eq!(layout["memory_sources"][1]["id"], json!("home_memory"));
            assert_eq!(py_skill_reference_name("plan", None), "skill/plan");
            assert_eq!(
                py_skill_reference_name("plan", Some("demo")),
                "demo/skill/plan"
            );

            let reserved = py_to_json_value(
                py_reserved_memory_namespace_issue(
                    py,
                    "config xprompt",
                    "memory/glossary",
                )
                .unwrap()
                .bind(py),
            )
            .unwrap();
            assert_eq!(reserved["rule"], json!("reserved_namespace"));
            assert!(py_reserved_memory_namespace_issue(py, "src", "foo")
                .unwrap()
                .is_none(py));

            let bad_type = py_to_json_value(
                py_memory_note_issue(
                    py,
                    "sase/memory/notes.md",
                    "notes",
                    Some("dynamic"),
                )
                .unwrap()
                .bind(py),
            )
            .unwrap();
            assert_eq!(bad_type["rule"], json!("invalid_note_type"));
            assert!(py_memory_note_issue(
                py,
                "sase/memory/glossary.md",
                "glossary",
                Some("short")
            )
            .unwrap()
            .is_none(py));
        });
    }

    #[test]
    fn plan_validation_bindings_round_trip_json_shapes() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let content = "---\ntier: epic\ntitle: Binding parity\ngoal: The binding returns normalized data\nparent_bead: sase-7z.1\nbead: sase-88.1\nparent: sase/repos/plans/202607/parent.md\nphases:\n  - id: core\n    title: Core work\n    depends_on: []\n    description: Core work section exercises binding parity.\n    size: medium\n---\n# Plan\nImplement it.\n";
            let result =
                py_plan_validate(py, content, "epic", "authoring").unwrap();
            let value = py_to_json_value(result.bind(py)).unwrap();
            assert_eq!(value["schema_version"], json!(3));
            assert_eq!(value["ok"], json!(true));
            assert_eq!(
                value["diagnostics"][0]["code"],
                json!("parent-frontmatter-deprecated")
            );
            assert_eq!(value["plan"]["title"], json!("Binding parity"));
            assert_eq!(value["plan"]["parent_bead"], json!("sase-7z.1"));
            assert_eq!(value["plan"]["size"], json!(null));
            assert_eq!(value["plan"]["bead"], json!("sase-88.1"));
            assert_eq!(
                value["plan"]["parent"],
                json!("sase/repos/plans/202607/parent.md")
            );
            assert_eq!(value["plan"]["phases"][0]["depends_on"], json!([]));
            assert_eq!(value["plan"]["phases"][0]["size"], json!("medium"));

            let tale = "---\ntier: tale\ntitle: Tale binding parity\ngoal: The binding returns normalized data\nsize: medium\nbead: sase-88.1\nparent: sase/repos/plans/202607/parent.md\n---\n# Plan\nImplement it.\n";
            let tale_result =
                py_plan_validate(py, tale, "tale", "authoring").unwrap();
            let tale_value = py_to_json_value(tale_result.bind(py)).unwrap();
            assert_eq!(tale_value["ok"], json!(true));
            assert_eq!(
                tale_value["diagnostics"][0]["code"],
                json!("parent-frontmatter-deprecated")
            );
            assert_eq!(
                tale_value["plan"]["title"],
                json!("Tale binding parity")
            );
            assert_eq!(tale_value["plan"]["size"], json!("medium"));
            assert_eq!(tale_value["plan"]["bead"], json!("sase-88.1"));
            assert_eq!(
                tale_value["plan"]["parent"],
                json!("sase/repos/plans/202607/parent.md")
            );

            for (tier, extra) in [
                ("tale", "size: small\n"),
                (
                    "epic",
                    "phases:\n  - id: core\n    title: Core\n    depends_on: []\n    description: Core section exercises title validation.\n    size: small\n",
                ),
            ] {
                for title_line in ["", "title: ''\n", "title: 42\n"] {
                    let invalid = format!(
                        "---\ntier: {tier}\n{title_line}goal: outcome\n{extra}---\nbody\n"
                    );
                    let invalid_result =
                        py_plan_validate(py, &invalid, tier, "authoring")
                            .unwrap();
                    let invalid_value =
                        py_to_json_value(invalid_result.bind(py)).unwrap();
                    assert_eq!(invalid_value["ok"], json!(false));
                    assert!(invalid_value["diagnostics"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|diagnostic| diagnostic["field_path"] == "title"));
                }
            }

            let schema = py_plan_frontmatter_schema(py, "epic").unwrap();
            let schema_value = py_to_json_value(schema.bind(py)).unwrap();
            assert_eq!(schema_value[0]["name"], json!("tier"));
            assert_eq!(schema_value[0]["type"], json!("tale | epic"));
            assert!(schema_value
                .as_array()
                .unwrap()
                .iter()
                .any(|field| { field["name"] == json!("phases[].model") }));
            assert!(schema_value
                .as_array()
                .unwrap()
                .iter()
                .any(|field| { field["name"] == json!("phases[].size") }));
            assert!(schema_value
                .as_array()
                .unwrap()
                .iter()
                .any(|field| { field["name"] == json!("parent_bead") }));
            for field_name in ["bead", "parent"] {
                assert!(schema_value
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|field| field["name"] == json!(field_name)));
            }
            let tale_schema = py_plan_frontmatter_schema(py, "tale").unwrap();
            let tale_schema_value =
                py_to_json_value(tale_schema.bind(py)).unwrap();
            assert_eq!(tale_schema_value[1]["name"], json!("title"));
            assert_eq!(tale_schema_value[1]["required"], json!(true));
            assert!(tale_schema_value.as_array().unwrap().iter().any(
                |field| {
                    field["name"] == json!("size")
                        && field["required"] == json!(true)
                }
            ));
            for field_name in ["bead", "parent"] {
                assert!(tale_schema_value
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|field| field["name"] == json!(field_name)));
            }

            let legacy = content.replace("    size: medium\n", "");
            let legacy_result =
                py_plan_validate(py, &legacy, "epic", "launch").unwrap();
            let legacy_value =
                py_to_json_value(legacy_result.bind(py)).unwrap();
            assert_eq!(legacy_value["ok"], json!(true));
            assert!(legacy_value["diagnostics"]
                .as_array()
                .unwrap()
                .iter()
                .any(|diagnostic| {
                    diagnostic["code"] == json!("phase-size-missing")
                }));
            assert_eq!(
                legacy_value["plan"]["phases"][0]["size"],
                json!("small")
            );

            let error = py_plan_validate(py, content, "story", "authoring")
                .unwrap_err();
            assert!(error.to_string().contains("unsupported plan tier"));
        });
    }

    #[test]
    fn plan_reference_bindings_round_trip_json_shapes() {
        pyo3::prepare_freethreaded_python();
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("plans");
        let target = root.join("202608/plan.md");
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::write(&target, "# Plan\n").unwrap();

        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
            sase_core_rs(py, &module).unwrap();
            for name in [
                "plan_reference_parse",
                "plan_reference_render",
                "plan_reference_canonicalize",
                "plan_reference_resolve",
                "plan_reference_resolution_wire_schema_version",
            ] {
                assert!(module.getattr(name).is_ok(), "missing {name}");
            }

            let parsed =
                py_plan_reference_parse(py, "plans:202607/plan.md").unwrap();
            let parsed = py_to_json_value(parsed.bind(py)).unwrap();
            assert_eq!(parsed["kind"], json!("plans"));
            assert_eq!(parsed["legacy"], json!(false));

            assert_eq!(
                py_plan_reference_render("plans", "202607/plan.md").unwrap(),
                "plans:202607/plan.md"
            );
            assert_eq!(
                py_plan_reference_canonicalize(
                    target.to_str().unwrap(),
                    vec![root.to_string_lossy().into_owned()],
                )
                .unwrap()
                .as_deref(),
                Some("plans:202608/plan.md")
            );

            let resolved = py_plan_reference_resolve(
                py,
                "plans:202607/plan.md",
                vec![root.to_string_lossy().into_owned()],
            )
            .unwrap();
            let resolved = py_to_json_value(resolved.bind(py)).unwrap();
            assert_eq!(resolved["schema_version"], json!(1));
            assert_eq!(resolved["status"], json!("drifted"));
            assert_eq!(
                resolved["resolved_path"],
                json!(target.to_string_lossy())
            );
            assert_eq!(py_plan_reference_resolution_wire_schema_version(), 1);
        });
    }

    #[test]
    fn artifact_ref_bindings_round_trip_json_shapes() {
        pyo3::prepare_freethreaded_python();
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("plans");
        let target = root.join("202607/plan.md");
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::write(&target, "# Plan\n").unwrap();

        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
            sase_core_rs(py, &module).unwrap();
            for name in [
                "artifact_ref_parse",
                "artifact_ref_render",
                "artifact_ref_canonicalize",
                "artifact_ref_resolve",
                "artifact_ref_list_normalize",
                "artifact_ref_list_parse",
                "artifact_ref_list_resolve",
                "artifact_ref_list_resolution_wire_schema_version",
                "artifact_ref_context_wire_schema_version",
                "artifact_ref_path_filter_wire_schema_version",
                "artifact_ref_filter_path_payloads",
                "artifact_ref_scan_prompt",
                "artifact_ref_wire_schema_version",
            ] {
                assert!(module.getattr(name).is_ok(), "missing {name}");
            }

            let parsed =
                py_artifact_ref_parse(py, "plans:202607/plan.md#L2").unwrap();
            let parsed_value = py_to_json_value(parsed.bind(py)).unwrap();
            assert_eq!(parsed_value["schema_version"], json!(4));
            assert_eq!(parsed_value["kind"]["type"], json!("document"));
            assert_eq!(parsed_value["fragment"]["type"], json!("lines"));
            assert_eq!(
                py_artifact_ref_render(parsed.bind(py)).unwrap(),
                "plans:202607/plan.md#L2"
            );
            let mut fragment_free_value = parsed_value.clone();
            fragment_free_value["fragment"] = serde_json::Value::Null;
            let fragment_free =
                json_value_to_py(py, &fragment_free_value).unwrap();
            assert_eq!(
                py_artifact_ref_render(fragment_free.bind(py)).unwrap(),
                "plans:202607/plan.md"
            );
            let bug = py_artifact_ref_parse(py, "bug:sase#123").unwrap();
            assert_eq!(
                py_artifact_ref_render(bug.bind(py)).unwrap(),
                "bug:sase#123"
            );

            let context_value = json!({
                "schema_version": 1,
                "document_roots": [{
                    "kind": "plans",
                    "root": root.to_string_lossy()
                }]
            });
            let context_object = json_value_to_py(py, &context_value).unwrap();
            let context = context_object.bind(py).downcast::<PyDict>().unwrap();

            assert_eq!(
                py_artifact_ref_canonicalize(target.to_str().unwrap(), context)
                    .unwrap()
                    .as_deref(),
                Some("plans:202607/plan.md")
            );
            let resolved =
                py_artifact_ref_resolve(py, parsed.bind(py), context).unwrap();
            let resolved = py_to_json_value(resolved.bind(py)).unwrap();
            assert_eq!(resolved["schema_version"], json!(4));
            assert_eq!(resolved["status"], json!("exact"));
            assert_eq!(
                resolved["resolved_path"],
                json!(target.to_string_lossy())
            );

            let scanned =
                py_artifact_ref_scan_prompt(py, "é @plans:x.md.").unwrap();
            let scanned = py_to_json_value(scanned.bind(py)).unwrap();
            assert_eq!(scanned[0]["candidate_span"]["start"], json!(3));
            assert_eq!(scanned[0]["text"], json!("@plans:x.md"));
            assert_eq!(py_artifact_ref_wire_schema_version(), 4);
            assert!(py_artifact_ref_parse(py, "commit:sase@BAD").is_err());
            assert_eq!(
                py_artifact_ref_list_normalize(vec![
                    "plans:202607/plan.md".to_string(),
                    "bead:sase-bb".to_string(),
                    "plans:202607/plan.md".to_string(),
                ])
                .unwrap(),
                ["plans:202607/plan.md", "bead:sase-bb"]
            );
            let list_parsed = py_artifact_ref_list_parse(
                py,
                vec!["bead:sase-bb".to_string()],
            )
            .unwrap();
            assert_eq!(
                py_to_json_value(list_parsed.bind(py)).unwrap()[0]["rendered"],
                json!("bead:sase-bb")
            );
            let list_resolved = py_artifact_ref_list_resolve(
                py,
                vec!["plans:202607/plan.md".to_string(), "broken".to_string()],
                context,
            )
            .unwrap();
            let list_resolved =
                py_to_json_value(list_resolved.bind(py)).unwrap();
            assert_eq!(list_resolved["schema_version"], json!(2));
            assert_eq!(
                list_resolved["entries"][0]["resolution"]["status"],
                json!("exact")
            );
            assert_eq!(
                list_resolved["entries"][1]["resolution"]["status"],
                json!("unknown_kind")
            );
            assert_eq!(
                py_artifact_ref_list_resolution_wire_schema_version(),
                2
            );
            assert_eq!(py_artifact_ref_context_wire_schema_version(), 1);
            assert_eq!(py_artifact_ref_path_filter_wire_schema_version(), 1);
        });
    }

    #[test]
    fn prompt_artifact_bindings_round_trip_manifest_shapes() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
            sase_core_rs(py, &module).unwrap();
            for name in [
                "prompt_artifact_pool_filename",
                "prompt_artifact_manifest_parse",
                "prompt_artifact_manifest_render_record",
                "prompt_artifact_manifest_select",
                "prompt_artifact_rewrite_links",
                "prompt_artifact_wire_schema_version",
            ] {
                assert!(module.getattr(name).is_ok(), "missing {name}");
            }

            assert_eq!(py_prompt_artifact_wire_schema_version(), 1);
            assert_eq!(
                py_prompt_artifact_pool_filename(
                    &"a".repeat(64),
                    "../../diagram.png"
                ),
                "aaaaaaaaaaaa-diagram.png"
            );
            let record_value = json!({
                "schema_version": 1,
                "recorded_at": "2026-08-01T14:22:03Z",
                "agent_artifacts_dir": "/artifacts/run",
                "raw_ref": "@~/diagram.png",
                "expanded_ref": "@.sase/artifacts/home/diagram.png",
                "ref_kind": "file",
                "label": "diagram.png",
                "source_path": null,
                "sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "size_bytes": 42,
                "mime_type": "image/png",
                "pool_relpath": "pool/aaaaaaaaaaaa-diagram.png",
                "vcs_repo": null,
                "vcs_relpath": null,
                "locator": null,
                "skipped_reason": null
            });
            let record_object = json_value_to_py(py, &record_value).unwrap();
            let record = record_object.bind(py).downcast::<PyDict>().unwrap();
            let rendered =
                py_prompt_artifact_manifest_render_record(record).unwrap();
            let parsed = py_prompt_artifact_manifest_parse(
                py,
                &PyBytes::new_bound(py, rendered.as_bytes()),
            )
            .unwrap();
            assert_eq!(
                py_to_json_value(parsed.bind(py)).unwrap(),
                json!([record_value.clone()])
            );

            let records_object =
                json_value_to_py(py, &json!([record_value])).unwrap();
            let records = records_object.bind(py).downcast::<PyList>().unwrap();
            let selected = py_prompt_artifact_manifest_select(
                py,
                records,
                "/artifacts/run",
            )
            .unwrap();
            assert_eq!(
                py_to_json_value(selected.bind(py))
                    .unwrap()
                    .as_array()
                    .unwrap()
                    .len(),
                1
            );
            let resolver_module = PyModule::from_code_bound(
                py,
                "def resolve(record):\n    return 'archive.png'\n",
                "resolver.py",
                "resolver",
            )
            .unwrap();
            let resolver = resolver_module.getattr("resolve").unwrap();
            let rewritten = py_prompt_artifact_rewrite_links(
                py,
                "Open @~/diagram.png.",
                records,
                &resolver,
            )
            .unwrap();
            let rewritten = py_to_json_value(rewritten.bind(py)).unwrap();
            assert_eq!(
                rewritten["prompt"],
                json!("Open [@~/diagram.png](archive.png).")
            );
            assert_eq!(
                rewritten["linked_records"].as_array().unwrap().len(),
                1
            );
        });
    }

    #[test]
    fn artifact_consumption_binding_returns_summary_and_handshake() {
        pyo3::prepare_freethreaded_python();
        let temp = tempfile::tempdir().unwrap();
        let log = temp.path().join("consumption.jsonl");
        fs::write(
            &log,
            concat!(
                "{\"schema_version\":1,\"consumption\":{",
                "\"id\":\"one\",\"timestamp\":\"2026-07-30T10:00:00Z\",",
                "\"ref\":\"file:default:abc\",\"ref_kind\":\"file\",",
                "\"fragment\":null,\"role\":\"image\",",
                "\"artifact_id\":\"default:abc\",\"resolved_path\":\"/one\",",
                "\"resolution_status\":\"exact\",\"agent_name\":\"agent.two\",",
                "\"agent_source\":\"SASE_AGENT_NAME\",",
                "\"artifacts_dir\":null,\"project\":\"sase\"}}\n",
                "{\"schema_version\":1,\"consumption\":{",
                "\"id\":\"two\",\"timestamp\":\"2026-07-30T11:00:00Z\",",
                "\"ref\":\"file:default:abc\",\"ref_kind\":\"file\",",
                "\"fragment\":null,\"role\":\"report\",",
                "\"artifact_id\":\"default:abc\",\"resolved_path\":\"/two\",",
                "\"resolution_status\":\"exact\",\"agent_name\":\"agent.one\",",
                "\"agent_source\":\"SASE_AGENT_NAME\",",
                "\"artifacts_dir\":null,\"project\":\"sase\"}}\n"
            ),
        )
        .unwrap();

        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
            sase_core_rs(py, &module).unwrap();
            assert!(module.getattr("artifact_consumption_summary").is_ok());
            assert!(module
                .getattr("artifact_consumption_wire_schema_version")
                .is_ok());

            let result = py_artifact_consumption_summary(
                py,
                log.to_str().unwrap(),
                Some(vec![
                    "file:default:abc".to_string(),
                    "file:default:never".to_string(),
                ]),
            )
            .unwrap();
            let value = py_to_json_value(result.bind(py)).unwrap();
            let summary = &value["file:default:abc"];
            assert_eq!(summary["consumption_count"], json!(2));
            assert_eq!(summary["distinct_agent_count"], json!(2));
            assert_eq!(
                summary["agent_names"],
                json!(["agent.one", "agent.two"])
            );
            assert_eq!(summary["roles"], json!(["image", "report"]));
            assert!(value.get("file:default:never").is_none());
            assert_eq!(py_artifact_consumption_wire_schema_version(), 1);
        });
    }

    #[test]
    fn artifact_file_query_binding_returns_full_rows_and_handshake() {
        pyo3::prepare_freethreaded_python();
        let temp = tempfile::tempdir().unwrap();
        let index = temp.path().join("index.jsonl");
        fs::write(
            &index,
            concat!(
                "{\"schema_version\":1,\"artifact\":{\"id\":\"old\",",
                "\"label\":\"Old\",\"kind\":\"image\",\"path\":\"/old\",",
                "\"created_at\":\"2026-07-01T00:00:00Z\"}}\n",
                "{\"schema_version\":2,\"artifact\":{\"id\":\"new\",",
                "\"label\":\"New\",\"kind\":\"image\",\"path\":\"/new\",",
                "\"created_at\":\"2026-07-02T00:00:00Z\",",
                "\"sha256\":\"abc\",\"size_bytes\":3,",
                "\"mime_type\":\"image/png\"}}\n"
            ),
        )
        .unwrap();

        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
            sase_core_rs(py, &module).unwrap();
            assert!(module.getattr("artifact_files_query").is_ok());
            assert!(module
                .getattr("artifact_file_query_wire_schema_version")
                .is_ok());
            assert!(module.getattr("artifact_file_materialize_vcs").is_ok());

            let filters = PyDict::new_bound(py);
            filters.set_item("kinds", ["image"]).unwrap();
            filters.set_item("limit", 1).unwrap();
            let result =
                py_artifact_files_query(py, index.to_str().unwrap(), &filters)
                    .unwrap();
            let value = py_to_json_value(result.bind(py)).unwrap();
            assert_eq!(value[0]["id"], json!("new"));
            assert_eq!(value[0]["schema_version"], json!(2));
            assert_eq!(value[0]["sha256"], json!("abc"));
            assert_eq!(value[0]["size_bytes"], json!(3));
            assert_eq!(value[0]["mime_type"], json!("image/png"));
            assert_eq!(py_artifact_file_query_wire_schema_version(), 3);

            let request = PyDict::new_bound(py);
            request
                .set_item("cache_root", temp.path().join("cache"))
                .unwrap();
            request
                .set_item("checkout_paths", Vec::<String>::new())
                .unwrap();
            request
                .set_item("vcs_sha", "0123456789abcdef0123456789abcdef01234567")
                .unwrap();
            request.set_item("vcs_relpath", "docs/missing.png").unwrap();
            request
                .set_item(
                    "sha256",
                    "0000000000000000000000000000000000000000000000000000000000000000",
                )
                .unwrap();
            request.set_item("suffix", ".png").unwrap();
            request.set_item("max_history_scan", 20).unwrap();
            let materialized =
                py_artifact_file_materialize_vcs(py, &request).unwrap();
            let materialized = py_to_json_value(materialized.bind(py)).unwrap();
            assert_eq!(materialized["status"], json!("missing"));
        });
    }

    #[test]
    fn artifact_file_lifecycle_bindings_round_trip_plain_python_shapes() {
        pyo3::prepare_freethreaded_python();
        let temp = tempfile::tempdir().unwrap();
        let index = temp.path().join("index.jsonl");
        let stored = temp.path().join("store/payload.bin");
        fs::create_dir_all(stored.parent().unwrap()).unwrap();
        fs::write(&stored, b"payload").unwrap();
        fs::write(
            &index,
            format!(
                "{{\"schema_version\":2,\"artifact\":{{\"id\":\"old\",\
                 \"label\":\"x\",\"kind\":\"file\",\"path\":{},\
                 \"project\":\"p\",\"created_at\":\"2026-07-01T00:00:00Z\",\
                 \"size_bytes\":7}}}}\n",
                serde_json::to_string(&stored.to_string_lossy()).unwrap()
            ),
        )
        .unwrap();

        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
            sase_core_rs(py, &module).unwrap();
            for name in [
                "artifact_file_store_economics",
                "artifact_file_retention_plan",
                "artifact_file_trash_store",
                "artifact_file_trash_list",
                "artifact_file_trash_restore",
                "artifact_file_trash_purge",
                "artifact_file_lifecycle_wire_schema_version",
            ] {
                assert!(module.getattr(name).is_ok(), "{name}");
            }
            assert_eq!(
                py_artifact_file_lifecycle_wire_schema_version(),
                ARTIFACT_FILE_LIFECYCLE_WIRE_SCHEMA_VERSION
            );

            let options_obj = json_value_to_py(
                py,
                &json!({
                    "schema_version": 1,
                    "project": null,
                    "top_n": 10,
                    "generation_projections": [1]
                }),
            )
            .unwrap();
            let options = options_obj.bind(py).downcast::<PyDict>().unwrap();
            let economics = py_artifact_file_store_economics(
                py,
                index.to_str().unwrap(),
                options,
            )
            .unwrap();
            let economics = py_to_json_value(economics.bind(py)).unwrap();
            assert_eq!(economics["schema_version"], json!(1));
            assert_eq!(economics["total_rows"], json!(1));
            assert_eq!(economics["total_bytes"], json!(7));

            let policy_obj = json_value_to_py(
                py,
                &json!({
                    "schema_version": 1,
                    "now": "2026-07-30T00:00:00Z",
                    "keep_per_label": 0,
                    "before": null,
                    "kinds": null,
                    "project": null,
                    "min_size_bytes": null,
                    "protected_ids": [],
                    "limit": null
                }),
            )
            .unwrap();
            let policy = policy_obj.bind(py).downcast::<PyDict>().unwrap();
            let plan = py_artifact_file_retention_plan(
                py,
                index.to_str().unwrap(),
                policy,
            )
            .unwrap();
            let plan = py_to_json_value(plan.bind(py)).unwrap();
            assert_eq!(plan["schema_version"], json!(1));
            assert_eq!(plan["counts"]["selected"], json!(0));

            let trash_root = temp.path().join("trash");
            let store_obj = json_value_to_py(
                py,
                &json!({
                    "schema_version": 1,
                    "trash_root": trash_root,
                    "record": {
                        "id": "default:abcdef0123456789abcdef01",
                        "path": stored,
                        "size_bytes": 7
                    },
                    "stored_path": stored,
                    "reason": "binding test",
                    "trashed_at": "2026-07-30T12:00:00Z"
                }),
            )
            .unwrap();
            let store_request =
                store_obj.bind(py).downcast::<PyDict>().unwrap();
            let entry =
                py_artifact_file_trash_store(py, store_request).unwrap();
            let entry = py_to_json_value(entry.bind(py)).unwrap();
            assert_eq!(entry["schema_version"], json!(1));
            assert!(!stored.exists());

            let listing =
                py_artifact_file_trash_list(py, trash_root.to_str().unwrap())
                    .unwrap();
            let listing = py_to_json_value(listing.bind(py)).unwrap();
            assert_eq!(listing["entries"].as_array().unwrap().len(), 1);

            let restore_obj = json_value_to_py(
                py,
                &json!({
                    "schema_version": 1,
                    "trash_root": trash_root,
                    "entry_id": entry["entry_id"]
                }),
            )
            .unwrap();
            let restore_request =
                restore_obj.bind(py).downcast::<PyDict>().unwrap();
            let restored =
                py_artifact_file_trash_restore(py, restore_request).unwrap();
            let restored = py_to_json_value(restored.bind(py)).unwrap();
            assert_eq!(
                restored["record"]["id"],
                json!("default:abcdef0123456789abcdef01")
            );
            assert_eq!(fs::read(&stored).unwrap(), b"payload");

            let invalid_options_obj = json_value_to_py(
                py,
                &json!({
                    "schema_version": 2,
                    "project": null,
                    "top_n": 10,
                    "generation_projections": []
                }),
            )
            .unwrap();
            let invalid_options =
                invalid_options_obj.bind(py).downcast::<PyDict>().unwrap();
            assert!(py_artifact_file_store_economics(
                py,
                index.to_str().unwrap(),
                invalid_options,
            )
            .is_err());
        });
    }

    #[test]
    fn sdd_artifact_link_bindings_match_core_contract() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let rendered = py_sdd_artifact_link_render(
                "PROMPT",
                "202607/prompts/example.md",
                "prompts/example.md",
            )
            .unwrap();
            assert_eq!(
                rendered,
                "- **PROMPT:** [202607/prompts/example.md](prompts/example.md)"
            );

            let document = format!("{rendered}\n\n# Plan\n");
            let parsed = py_sdd_artifact_link_parse(py, &document).unwrap();
            let parsed_value = py_to_json_value(parsed.bind(py)).unwrap();
            assert_eq!(parsed_value["kind"], json!("canonical"));
            assert_eq!(
                parsed_value["label"],
                json!("202607/prompts/example.md")
            );
            assert_eq!(parsed_value["target"], json!("prompts/example.md"));

            assert_eq!(parsed_value["body"], json!("# Plan\n"));

            let legacy = py_sdd_artifact_link_parse(
                py,
                "---\nprompt: 202607/prompts/example.md\n---\n# Plan\n",
            )
            .unwrap();
            let legacy_value = py_to_json_value(legacy.bind(py)).unwrap();
            assert_eq!(legacy_value["kind"], json!("legacy"));
            assert_eq!(
                legacy_value["legacy"]["reference"],
                json!("202607/prompts/example.md")
            );

            let updated = py_sdd_artifact_link_upsert(
                "# Plan\n",
                "PROMPT",
                "202607/prompts/example.md",
                "prompts/example.md",
                true,
                false,
            )
            .unwrap();
            assert_eq!(updated, format!("{rendered}\n\n# Plan\n"));

            assert!(py_sdd_artifact_link_render(
                "PROMPT",
                "202607/prompts/example.md",
                "https://example.com/prompts/example.md",
            )
            .is_ok());
        });
    }

    #[test]
    fn sdd_plan_header_block_bindings_match_core_contract() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            assert_eq!(
                py_sdd_plan_header_block_wire_schema_version(),
                PLAN_HEADER_BLOCK_WIRE_SCHEMA_VERSION
            );
            let sections_obj = json_value_to_py(
                py,
                &json!([
                    {
                        "kind": "PROMPT",
                        "label": "202607/prompts/example.md",
                        "target": "prompts/example.md"
                    },
                    {
                        "kind": "BEAD",
                        "label": "sase-ai.8",
                        "target": "https://github.com/sase-org/sase--beads/blob/main/pages/sase-ai/sase-ai.8.md"
                    },
                    {
                        "kind": "COMMITS",
                        "entries": [{
                            "label": "699456a",
                            "target": "https://github.com/sase-org/sase/commit/699456a",
                            "trailing_text": "fix(parser): wrap safely"
                        }]
                    }
                ]),
            )
            .unwrap();
            let sections = sections_obj.bind(py).downcast::<PyList>().unwrap();
            let rendered = py_sdd_plan_header_block_render(sections).unwrap();
            assert!(rendered.contains("- **PROMPT:**"));
            assert!(rendered.contains("- **COMMITS:**"));

            let document = format!("{rendered}\n\n# Plan\n");
            let parsed = py_sdd_plan_header_block_parse(py, &document).unwrap();
            let parsed = py_to_json_value(parsed.bind(py)).unwrap();
            assert_eq!(parsed["schema_version"], json!(3));
            assert_eq!(parsed["sections"][1]["kind"], json!("BEAD"));
            assert_eq!(parsed["sections"][2]["kind"], json!("COMMITS"));

            let section_obj = json_value_to_py(
                py,
                &json!({
                    "kind": "PARENT",
                    "label": "202607/epic.md",
                    "target": "https://github.com/sase-org/sase--plans/blob/main/202607/epic.md"
                }),
            )
            .unwrap();
            let section = section_obj.bind(py).downcast::<PyDict>().unwrap();
            let updated = py_sdd_plan_header_block_upsert_section(
                &document, section, false, false,
            )
            .unwrap();
            assert!(updated.contains("- **PARENT:**"));

            let removed = py_sdd_plan_header_block_remove_section(
                &updated, "PARENT", false, false,
            )
            .unwrap();
            assert_eq!(removed, document);
        });
    }

    #[test]
    fn placeholder_bindings_return_plain_json_shapes() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let text = "<Alpha> use <a>";
            let completion =
                py_placeholder_completion(py, text, 0, 14, None).unwrap();
            let value = py_to_json_value(completion.bind(py)).unwrap();
            assert_eq!(value["prefix"], json!("a"));
            assert_eq!(
                value["candidates"],
                json!([{"text": "Alpha", "source": "prompt"}])
            );
            assert_eq!(value["append_closing_bracket"], json!(false));
            assert_eq!(
                value["replacement_range"]["start"]["character"],
                json!(13)
            );

            let with_common = py_placeholder_completion(
                py,
                text,
                0,
                14,
                Some(vec!["Alpha".to_string(), "anchor".to_string()]),
            )
            .unwrap();
            assert_eq!(
                py_to_json_value(with_common.bind(py)).unwrap()["candidates"],
                json!([
                    {"text": "Alpha", "source": "prompt"},
                    {"text": "anchor", "source": "common"},
                ])
            );

            let common_only = py_placeholder_completion(
                py,
                "<only>",
                0,
                5,
                Some(vec!["only tag".to_string()]),
            )
            .unwrap();
            assert_eq!(
                py_to_json_value(common_only.bind(py)).unwrap()["candidates"],
                json!([{"text": "only tag", "source": "common"}])
            );

            let empty =
                py_placeholder_completion(py, "<only>", 0, 5, None).unwrap();
            assert_eq!(
                py_to_json_value(empty.bind(py)).unwrap(),
                JsonValue::Null
            );

            let spans = py_placeholder_spans(py, "`<inline>` <live>").unwrap();
            let spans = py_to_json_value(spans.bind(py)).unwrap();
            assert_eq!(spans.as_array().unwrap().len(), 2);
            assert_eq!(spans[0]["text"], json!("inline"));
            assert_eq!(spans[0]["raw"], json!(false));
            assert_eq!(spans[0]["range"]["start"]["character"], json!(1));
            assert_eq!(spans[1]["text"], json!("live"));
            assert_eq!(spans[1]["raw"], json!(true));

            let fields =
                py_raw_placeholder_fields(py, "<live> and <live>", 60).unwrap();
            assert_eq!(
                py_to_json_value(fields.bind(py)).unwrap(),
                json!([{
                    "text": "live",
                    "occurrences": 2,
                    "context": "<live> and <live>",
                }])
            );
            assert_eq!(
                py_substitute_raw_placeholders(
                    "<live> and `<live>`",
                    BTreeMap::from([("live".to_string(), "ready".to_string())]),
                ),
                "ready and `<live>`"
            );
            assert_eq!(
                py_placeholder_input_names(vec![
                    "the plan".to_string(),
                    "the-plan".to_string(),
                ]),
                vec!["the_plan", "the_plan_2"]
            );
        });
    }

    #[test]
    fn at_reference_bindings_return_plain_json_shapes() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
            module
                .add_function(
                    wrap_pyfunction!(py_at_reference_context, &module).unwrap(),
                )
                .unwrap();
            module
                .add_function(
                    wrap_pyfunction!(py_at_reference_menu, &module).unwrap(),
                )
                .unwrap();
            module.add_class::<PyAtReferenceInventory>().unwrap();
            module
                .add_function(
                    wrap_pyfunction!(py_fuzzy_match, &module).unwrap(),
                )
                .unwrap();

            let context = module
                .getattr("at_reference_context")
                .unwrap()
                .call1(("open @fi", 0_u32, 8_u32))
                .unwrap();
            let context_value = py_to_json_value(&context).unwrap();
            assert_eq!(context_value["stage"], json!("kind"));
            assert_eq!(context_value["candidate_span"], json!([5, 8]));
            assert_eq!(context_value["replacement_span"], json!([6, 8]));
            assert_eq!(context_value["query_span"], json!([6, 8]));
            assert_eq!(context_value["query"], json!("fi"));
            assert_eq!(context_value["kind"], JsonValue::Null);
            assert_eq!(
                context_value["path_query"],
                json!({
                    "directory": "",
                    "partial": "fi",
                    "show_hidden": false,
                })
            );

            let inventory = json_value_to_py(
                py,
                &json!({
                    "kinds": [
                        {
                            "kind": "fixture",
                            "builtin": false,
                            "detail": "Custom references",
                        },
                        {
                            "kind": "file",
                            "builtin": true,
                            "detail": "Tracked files",
                        },
                    ],
                    "paths": [
                        {"name": "final.md", "is_dir": false},
                        {"name": "fixtures", "is_dir": true},
                        {"name": ".hidden", "is_dir": false},
                    ],
                    "payloads": [],
                }),
            )
            .unwrap();
            let menu = module
                .getattr("at_reference_menu")
                .unwrap()
                .call1((&context, inventory.bind(py)))
                .unwrap();
            let menu_value = py_to_json_value(&menu).unwrap();
            assert_eq!(menu_value["artifact_count"], json!(2));
            assert_eq!(menu_value["file_count"], json!(0));
            assert_eq!(menu_value["files_suppressed"], json!(true));
            assert_eq!(menu_value["shared_extension"], json!(""));
            assert_eq!(menu_value["rows"][0]["group"], json!("artifact"));
            assert_eq!(menu_value["rows"][0]["label"], json!("file"));
            assert_eq!(menu_value["rows"][0]["insertion"], json!("@file:"));
            assert_eq!(menu_value["rows"][1]["label"], json!("fixture"));

            let options =
                json_value_to_py(py, &json!({"include_files": true})).unwrap();
            let revealed_menu = module
                .getattr("at_reference_menu")
                .unwrap()
                .call1((
                    &context,
                    inventory.bind(py),
                    py.None(),
                    options.bind(py),
                ))
                .unwrap();
            let revealed_value = py_to_json_value(&revealed_menu).unwrap();
            assert_eq!(revealed_value["file_count"], json!(2));
            assert_eq!(revealed_value["files_suppressed"], json!(false));
            assert_eq!(revealed_value["rows"][2]["group"], json!("file"));
            assert_eq!(revealed_value["rows"][2]["label"], json!("fixtures/"));
            assert_eq!(
                revealed_value["rows"][2]["insertion"],
                json!("@fixtures/")
            );
            assert_eq!(revealed_value["rows"][3]["label"], json!("final.md"));

            let payload_context = module
                .getattr("at_reference_context")
                .unwrap()
                .call1(("see @bug:sa", 0_u32, 11_u32))
                .unwrap();
            let payload_context_value =
                py_to_json_value(&payload_context).unwrap();
            assert_eq!(payload_context_value["stage"], json!("payload"));
            assert_eq!(payload_context_value["query"], json!("sa"));
            assert_eq!(payload_context_value["kind"], json!("bug"));

            let payloads = json_value_to_py(
                py,
                &json!([{
                    "payload": "202607/sase_sites_hub_and_pages.md",
                    "label": "SASE Sites Hub and Pages",
                    "detail": "research",
                    "age": "3d",
                }]),
            )
            .unwrap();
            let kwargs = PyDict::new_bound(py);
            kwargs.set_item("payloads", payloads).unwrap();
            let payload_index = module
                .getattr("AtReferenceInventory")
                .unwrap()
                .call((), Some(&kwargs))
                .unwrap();
            assert_eq!(payload_index.len().unwrap(), 1);
            assert!(payload_index.setattr("payloads", py.None()).is_err());

            let indexed_context = module
                .getattr("at_reference_context")
                .unwrap()
                .call1(("see @research:site", 0_u32, 18_u32))
                .unwrap();
            let indexed_inventory = json_value_to_py(
                py,
                &json!({
                    "kinds": [],
                    "paths": [],
                    "payloads": [{
                        "payload": "ignored.md",
                        "label": "Ignored",
                        "detail": "",
                        "age": "",
                    }],
                    "truncated_payloads": 4,
                }),
            )
            .unwrap();
            let indexed_menu = module
                .getattr("at_reference_menu")
                .unwrap()
                .call1((
                    &indexed_context,
                    indexed_inventory.bind(py),
                    &payload_index,
                ))
                .unwrap();
            let indexed_menu = py_to_json_value(&indexed_menu).unwrap();
            assert_eq!(indexed_menu["payload_count"], json!(1));
            assert_eq!(indexed_menu["truncated_payloads"], json!(4));
            assert_eq!(
                indexed_menu["rows"][0]["label"],
                json!("202607/sase_sites_hub_and_pages.md")
            );
            assert_eq!(
                indexed_menu["rows"][0]["label_match"],
                json!([[12, 16]])
            );

            let fuzzy = module
                .getattr("fuzzy_match")
                .unwrap()
                .call1(("rés", "café/東京Résumé.md"))
                .unwrap();
            let fuzzy = py_to_json_value(&fuzzy).unwrap();
            assert_eq!(fuzzy["tier"], json!(2));
            assert_eq!(fuzzy["runs"], json!([[7, 10]]));
            let no_match = module
                .getattr("fuzzy_match")
                .unwrap()
                .call1(("missing", "text"))
                .unwrap();
            assert!(no_match.is_none());
        });
    }

    #[test]
    fn artifact_ref_payload_inventory_binding_returns_plain_json_shape() {
        pyo3::prepare_freethreaded_python();
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path().join("repo");
        init_git_repo(&repo);
        let sha = commit_at(
            &repo,
            1_700_000_000,
            "binding inventory subject",
            "body line\nsecond line",
        );

        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
            module
                .add_function(
                    wrap_pyfunction!(
                        py_artifact_ref_payload_inventory,
                        &module
                    )
                    .unwrap(),
                )
                .unwrap();

            let context = json_value_to_py(
                py,
                &json!({
                    "schema_version": 1,
                    "repositories": [{
                        "name": "sase-core",
                        "checkout_paths": [repo.to_string_lossy()],
                    }],
                }),
            )
            .unwrap();
            let inventory = module
                .getattr("artifact_ref_payload_inventory")
                .unwrap()
                .call1(("commit", context.bind(py)))
                .unwrap();
            let inventory = py_to_json_value(&inventory).unwrap();

            assert_eq!(inventory["truncated_payloads"], json!(0));
            assert_eq!(inventory["payloads"].as_array().unwrap().len(), 1);
            let row = &inventory["payloads"][0];
            assert_eq!(
                row["payload"],
                json!(format!("sase-core@{}", &sha[..12]))
            );
            assert_eq!(row["label"], json!("binding inventory subject"));
            assert_eq!(row["detail"], json!(""));
            assert!(row["age"].as_str().is_some());
            assert_eq!(row["scope"], json!("sase-core"));
            assert_eq!(row["rank"], json!(0));
            assert_eq!(row["body"], json!("body line\nsecond line"));

            let bad_context = json_value_to_py(
                py,
                &json!({"schema_version": 1, "repositories": "invalid"}),
            )
            .unwrap();
            let error = module
                .getattr("artifact_ref_payload_inventory")
                .unwrap()
                .call1(("commit", bad_context.bind(py)))
                .unwrap_err();
            assert!(error.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    fn artifact_ref_filter_path_payloads_binding_returns_batch_shape() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
            sase_core_rs(py, &module).unwrap();
            let filter =
                module.getattr("artifact_ref_filter_path_payloads").unwrap();

            let result = filter
                .call1((
                    "research",
                    vec!["README.md", "drafts/a.md", "image.png"],
                    vec!["**/*.md", "!drafts/**"],
                ))
                .unwrap();
            let result = py_to_json_value(&result).unwrap();
            assert_eq!(result["schema_version"], json!(1));
            assert_eq!(result["kind"], json!("research"));
            assert_eq!(result["allowed"], json!(["README.md"]));
            assert_eq!(result["filtered"], json!(["drafts/a.md", "image.png"]));

            let error = filter
                .call1(("research", vec!["README.md"], vec![""]))
                .unwrap_err();
            assert!(error.is_instance_of::<PyValueError>(py));
        });
    }

    #[test]
    #[cfg_attr(
        debug_assertions,
        ignore = "the 8 ms performance gate is calibrated for release builds"
    )]
    fn indexed_at_reference_binding_stays_below_eight_ms_for_5000_rows() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
            module.add_class::<PyAtReferenceInventory>().unwrap();
            module
                .add_function(
                    wrap_pyfunction!(py_at_reference_menu, &module).unwrap(),
                )
                .unwrap();

            let payloads = (0..5_000)
                .map(|index| {
                    json!({
                        "payload": format!(
                            "202607/bundle_{index:04}/artifact_{index:04}.md"
                        ),
                        "label": format!("Artifact title {index:04}"),
                        "detail": "plans",
                        "age": "now",
                    })
                })
                .collect::<Vec<_>>();
            let payloads = json_value_to_py(py, &json!(payloads)).unwrap();
            let kwargs = PyDict::new_bound(py);
            kwargs.set_item("payloads", payloads).unwrap();
            let payload_index = module
                .getattr("AtReferenceInventory")
                .unwrap()
                .call((), Some(&kwargs))
                .unwrap();
            let context = json_value_to_py(
                py,
                &json!({
                    "stage": "payload",
                    "candidate_span": [0, 12],
                    "replacement_span": [1, 12],
                    "query_span": [7, 12],
                    "query": "artifact",
                    "kind": "plans",
                    "path_query": null,
                }),
            )
            .unwrap();
            let inventory = json_value_to_py(
                py,
                &json!({
                    "kinds": [],
                    "paths": [],
                    "payloads": [],
                    "truncated_payloads": 0,
                }),
            )
            .unwrap();
            let menu = module.getattr("at_reference_menu").unwrap();
            menu.call1((&context, &inventory, &payload_index)).unwrap();

            const SAMPLES: u32 = 40;
            let started = Instant::now();
            for _ in 0..SAMPLES {
                menu.call1((&context, &inventory, &payload_index)).unwrap();
            }
            let mean = started.elapsed() / SAMPLES;
            assert!(
                mean < Duration::from_millis(8),
                "indexed 5000-row binding mean {mean:?} exceeded 8 ms"
            );
        });
    }

    #[test]
    fn inline_code_binding_returns_plain_byte_offset_tuples() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
            module
                .add_function(
                    wrap_pyfunction!(py_inline_code_ranges, &module).unwrap(),
                )
                .unwrap();
            let value = module
                .getattr("inline_code_ranges")
                .unwrap()
                .call1(("é`值`/`ß`",))
                .unwrap();
            assert_eq!(
                py_to_json_value(&value).unwrap(),
                json!([[2, 7], [8, 12]])
            );
        });
    }

    fn query_module<'py>(py: Python<'py>) -> Bound<'py, PyModule> {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        module.add_class::<PyQueryCorpusHandle>().unwrap();
        module.add_class::<PyQueryProgramHandle>().unwrap();
        module
            .add_function(wrap_pyfunction!(py_evaluate_many, &module).unwrap())
            .unwrap();
        module
    }

    fn temp_notification_path(name: &str) -> (tempfile::TempDir, PathBuf) {
        let temp = tempfile::Builder::new()
            .prefix("sase-core-py-notification-")
            .tempdir()
            .unwrap();
        let path = temp.path().join(name);
        (temp, path)
    }

    fn temp_beads_dir() -> (tempfile::TempDir, PathBuf) {
        let temp = tempfile::Builder::new()
            .prefix("sase-core-py-bead-")
            .tempdir()
            .unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        (temp, beads_dir)
    }

    fn temp_telemetry_path() -> (tempfile::TempDir, PathBuf) {
        let temp = tempfile::Builder::new()
            .prefix("sase-core-py-telemetry-")
            .tempdir()
            .unwrap();
        let path = temp.path().join("metrics.sqlite");
        (temp, path)
    }

    fn temp_agent_stats_root() -> tempfile::TempDir {
        tempfile::Builder::new()
            .prefix("sase-core-py-agent-stats-")
            .tempdir()
            .unwrap()
    }

    #[test]
    fn query_handles_evaluate_multiple_queries_against_one_corpus() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let specs = spec_list(
                py,
                &[
                    spec_json("alpha", "WIP", None),
                    spec_json("beta", "Submitted", Some("alpha")),
                    spec_json("gamma", "WIP", Some("beta")),
                ],
            );
            let corpus = py_compile_corpus(py, &specs).unwrap();

            let alpha = py_compile_query("name:alpha").unwrap();
            let alpha_results = py_evaluate_many(py, &alpha, &corpus).unwrap();
            assert_eq!(
                bools_from_py_list(&alpha_results),
                vec![true, false, false]
            );

            let ancestor = py_compile_query("ancestor:alpha").unwrap();
            let ancestor_results =
                py_evaluate_many(py, &ancestor, &corpus).unwrap();
            assert_eq!(
                bools_from_py_list(&ancestor_results),
                vec![true, true, true]
            );
            assert_eq!(corpus.__len__(), 3);
        });
    }

    #[test]
    fn query_handles_evaluate_one_query_against_multiple_corpora() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let program = py_compile_query("status:wip").unwrap();

            let first = spec_list(
                py,
                &[
                    spec_json("alpha", "WIP", None),
                    spec_json("beta", "Submitted", None),
                ],
            );
            let first_corpus = py_compile_corpus(py, &first).unwrap();
            let first_results =
                py_evaluate_many(py, &program, &first_corpus).unwrap();
            assert_eq!(bools_from_py_list(&first_results), vec![true, false]);

            let second = spec_list(
                py,
                &[
                    spec_json("gamma", "Submitted", None),
                    spec_json("delta", "WIP", None),
                ],
            );
            let second_corpus = py_compile_corpus(py, &second).unwrap();
            let second_results =
                py_evaluate_many(py, &program, &second_corpus).unwrap();
            assert_eq!(bools_from_py_list(&second_results), vec![false, true]);
        });
    }

    #[test]
    fn query_handles_match_legacy_one_shot_results() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let specs = spec_list(
                py,
                &[
                    spec_json("alpha", "WIP", None),
                    spec_json("beta", "Submitted", Some("alpha")),
                    spec_json("gamma", "WIP", Some("beta")),
                ],
            );
            let corpus = py_compile_corpus(py, &specs).unwrap();

            for query in ["alpha", "status:wip", "ancestor:alpha"] {
                let program = py_compile_query(query).unwrap();
                let handle_results =
                    py_evaluate_many(py, &program, &corpus).unwrap();
                let legacy_results =
                    py_evaluate_query_many(py, query, &specs).unwrap();
                assert_eq!(
                    bools_from_py_list(&handle_results),
                    bools_from_py_list(&legacy_results),
                    "query {query}"
                );
            }
        });
    }

    #[test]
    fn query_compile_errors_are_python_value_errors() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|_py| {
            let err = py_compile_query("").unwrap_err();
            assert!(err.is_instance_of::<PyValueError>(_py));
            assert!(err.to_string().contains("Empty query"));
        });
    }

    #[test]
    fn bead_search_binding_round_trips_json_shape() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let (_temp, beads_dir) = temp_beads_dir();
            fs::write(
                beads_dir.join("issues.jsonl"),
                serde_json::to_string(&json!({
                    "id": "beads-1.1",
                    "title": "Needle binding",
                    "status": "open",
                    "issue_type": "phase",
                    "parent_id": "beads-1",
                    "created_at": "2026-01-01T00:01:00Z",
                    "updated_at": "2026-01-01T00:01:00Z"
                }))
                .unwrap()
                    + "\n",
            )
            .unwrap();

            let result = py_bead_search(
                py,
                beads_dir.to_str().unwrap(),
                "needle",
                None,
                None,
                None,
                Some(1),
                false,
            )
            .unwrap();
            let value = py_to_json_value(result.bind(py)).unwrap();

            assert_eq!(value[0]["issue"]["id"], json!("beads-1.1"));
            assert_eq!(value[0]["matched_fields"], json!(["title"]));
        });
    }

    #[test]
    fn bead_search_binding_accepts_regex_keyword() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let (_temp, beads_dir) = temp_beads_dir();
            fs::write(
                beads_dir.join("issues.jsonl"),
                serde_json::to_string(&json!({
                    "id": "beads-1.1",
                    "title": "Auth binding",
                    "status": "open",
                    "issue_type": "phase",
                    "parent_id": "beads-1",
                    "created_at": "2026-01-01T00:01:00Z",
                    "updated_at": "2026-01-01T00:01:00Z"
                }))
                .unwrap()
                    + "\n",
            )
            .unwrap();
            let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
            module
                .add_function(
                    wrap_pyfunction!(py_bead_search, &module).unwrap(),
                )
                .unwrap();
            let kwargs = PyDict::new_bound(py);
            kwargs.set_item("regex", true).unwrap();

            let result = module
                .getattr("bead_search")
                .unwrap()
                .call(
                    (beads_dir.to_str().unwrap(), r"auth\s+binding"),
                    Some(&kwargs),
                )
                .unwrap();
            let value = py_to_json_value(&result).unwrap();

            assert_eq!(value[0]["issue"]["id"], json!("beads-1.1"));
            assert_eq!(value[0]["matched_fields"], json!(["title"]));
        });
    }

    #[test]
    fn plan_search_binding_accepts_explicit_document_corpora() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let temp = tempfile::tempdir().unwrap();
            let designs = temp.path().join("designs");
            fs::create_dir_all(designs.join("202607")).unwrap();
            fs::write(
                designs.join("202607").join("entry.md"),
                "# Binding design\n",
            )
            .unwrap();
            let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
            sase_core_rs(py, &module).unwrap();
            let kwargs = PyDict::new_bound(py);
            kwargs
                .set_item(
                    "document_corpora",
                    vec![(
                        designs.to_string_lossy().into_owned(),
                        "designs".to_string(),
                    )],
                )
                .unwrap();

            let result = module
                .getattr("plan_search")
                .unwrap()
                .call((), Some(&kwargs))
                .unwrap();
            let value = py_to_json_value(&result).unwrap();

            assert_eq!(value[0]["plan"]["kind"], json!("designs"));
            assert_eq!(value[0]["plan"]["relpath"], json!("202607/entry.md"));
        });
    }

    #[test]
    fn bead_merge_event_streams_binding_preserves_replay_stable_union() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
            sase_core_rs(py, &module).unwrap();
            assert!(module.getattr("bead_merge_event_streams").is_ok());

            let event = |event_id: &str, timestamp: &str, operation: &str| {
                json!({
                    "schema_version": 1,
                    "event_id": event_id,
                    "timestamp": timestamp,
                    "actor": "owner@example.com",
                    "operation": operation,
                    "issue_id": "gold-1",
                    "payload": {"kind": operation},
                })
            };
            let first =
                event("legacy-first", "2026-01-01T00:01:00Z", "ready_marked");
            let second = event(
                "legacy-second",
                "2026-01-01T00:02:00Z",
                "ready_unmarked",
            );
            let before =
                event("added-before", "2026-01-01T00:00:00Z", "ready_marked");
            let between = event(
                "added-between",
                "2026-01-01T00:01:30Z",
                "ready_unmarked",
            );
            let stream = |events: Vec<JsonValue>| {
                json!({
                    "stream_id": "gold-1",
                    "root_issue_id": "gold-1",
                    "events": events,
                })
            };
            let base_value = stream(vec![first.clone(), second.clone()]);
            let ours_value =
                stream(vec![before, first.clone(), second.clone()]);
            let theirs_value = stream(vec![first, between, second]);
            let base_obj = json_value_to_py(py, &base_value).unwrap();
            let ours_obj = json_value_to_py(py, &ours_value).unwrap();
            let theirs_obj = json_value_to_py(py, &theirs_value).unwrap();
            let base = base_obj.bind(py).downcast::<PyDict>().unwrap();
            let ours = ours_obj.bind(py).downcast::<PyDict>().unwrap();
            let theirs = theirs_obj.bind(py).downcast::<PyDict>().unwrap();

            let result =
                py_bead_merge_event_streams(py, base, ours, theirs).unwrap();
            let result = py_to_json_value(result.bind(py)).unwrap();
            assert_eq!(
                result["events"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|event| event["event_id"].as_str().unwrap())
                    .collect::<Vec<_>>(),
                vec![
                    "legacy-first",
                    "legacy-second",
                    "added-before",
                    "added-between",
                ]
            );
        });
    }

    #[test]
    fn bead_remove_many_binding_is_exported_and_removes_multiple_roots() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
            sase_core_rs(py, &module).unwrap();
            assert!(module.getattr("bead_remove").is_ok());
            assert!(module.getattr("bead_remove_many").is_ok());

            let (_temp, beads_dir) = temp_beads_dir();
            fs::write(
                beads_dir.join("issues.jsonl"),
                [
                    json!({
                        "id": "beads-1",
                        "title": "First",
                        "status": "open",
                        "issue_type": "plan",
                        "parent_id": null,
                        "created_at": "2026-01-01T00:00:00Z",
                        "updated_at": "2026-01-01T00:00:00Z"
                    }),
                    json!({
                        "id": "beads-2",
                        "title": "Second",
                        "status": "open",
                        "issue_type": "plan",
                        "parent_id": null,
                        "created_at": "2026-01-01T00:01:00Z",
                        "updated_at": "2026-01-01T00:01:00Z"
                    }),
                ]
                .into_iter()
                .map(|issue| serde_json::to_string(&issue).unwrap())
                .collect::<Vec<_>>()
                .join("\n")
                    + "\n",
            )
            .unwrap();

            let result = py_bead_remove_many(
                py,
                beads_dir.to_str().unwrap(),
                vec!["beads-2".to_string(), "beads-1".to_string()],
            )
            .unwrap();
            let value = py_to_json_value(result.bind(py)).unwrap();

            assert_eq!(value["issue_ids"], json!(["beads-2", "beads-1"]));
            assert_eq!(value["issues"][0]["id"], json!("beads-2"));
            assert_eq!(value["issues"][1]["id"], json!("beads-1"));
        });
    }

    #[test]
    fn bead_size_check_relax_bindings_are_exported_and_forward_core_policy() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
            sase_core_rs(py, &module).unwrap();
            for name in [
                "bead_needs_resolution_migration",
                "bead_resolution_migration_sql",
                "bead_needs_size_check_relax_migration",
                "bead_size_check_relax_migration_sql",
                "bead_needs_task_ready_migration",
                "bead_task_ready_migration_sql",
            ] {
                assert!(module.getattr(name).is_ok(), "missing {name}");
            }

            assert!(!py_bead_needs_size_check_relax_migration(None));
            assert!(py_bead_needs_size_check_relax_migration(Some(
                "size TEXT CHECK(size IN ('small','medium','large'))"
            )));
            assert!(!py_bead_needs_size_check_relax_migration(Some(
                "size TEXT CHECK(size IN \
                 ('xsmall','small','medium','large','xlarge'))"
            )));
            assert_eq!(
                py_bead_size_check_relax_migration_sql(),
                core_bead_size_check_relax_migration_sql()
            );
            assert!(py_bead_needs_task_ready_migration(Some(
                "CHECK(issue_type IN ('plan','phase'))"
            )));
            assert!(!py_bead_needs_task_ready_migration(Some(
                "CHECK(issue_type IN ('plan','phase','task')); \
                 CHECK(status IN ('open','ready','closed')); \
                 CHECK(status!='ready' OR issue_type='task')"
            )));
            assert_eq!(
                py_bead_task_ready_migration_sql(),
                core_bead_task_ready_migration_sql()
            );
            assert!(py_bead_needs_resolution_migration(Some(
                "CREATE TABLE issues(id TEXT)"
            )));
            assert_eq!(
                py_bead_resolution_migration_sql(),
                core_bead_resolution_migration_sql()
            );
        });
    }

    #[test]
    fn bead_manifest_repair_binding_round_trips_structured_outcome() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let (_temp, beads_dir) = temp_beads_dir();

            let result = py_bead_repair_event_store_manifest(
                py,
                beads_dir.to_str().unwrap(),
            )
            .unwrap();
            let value = py_to_json_value(result.bind(py)).unwrap();

            assert_eq!(value["status"], json!("noop"));
            assert_eq!(value["stream_count"], json!(0));
            assert!(value["manifest_path"]
                .as_str()
                .unwrap()
                .ends_with("events/manifest.json"));
        });
    }

    #[test]
    fn bead_work_plan_binding_exposes_additive_bead_id_fields() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let issues = PyList::empty_bound(py);
            append_json(
                py,
                &issues,
                json!({
                    "id": "beads-1",
                    "title": "Epic",
                    "status": "open",
                    "issue_type": "plan",
                    "tier": "epic",
                    "parent_id": null
                }),
            );
            append_json(
                py,
                &issues,
                json!({
                    "id": "beads-1.0",
                    "title": "Closed blocker",
                    "status": "closed",
                    "issue_type": "phase",
                    "parent_id": "beads-1"
                }),
            );
            append_json(
                py,
                &issues,
                json!({
                    "id": "beads-1.1",
                    "title": "Large phase",
                    "status": "open",
                    "issue_type": "phase",
                    "parent_id": "beads-1",
                    "size": "large",
                    "dependencies": [{
                        "issue_id": "beads-1.1",
                        "depends_on_id": "beads-1.0"
                    }]
                }),
            );

            let result = py_bead_build_epic_work_plan_from_issues(
                py, &issues, "beads-1",
            )
            .unwrap();
            let value = py_to_json_value(result.bind(py)).unwrap();

            assert_eq!(value["epic_id"], json!("beads-1"));
            assert_eq!(value["launch_tag_id"], json!("beads-1"));
            assert_eq!(value["total_phase_count"], json!(2));
            assert_eq!(
                value["phase_bead_ids"],
                json!(["beads-1.0", "beads-1.1"])
            );
            assert_eq!(value["waves"][0][0]["bead_id"], json!("beads-1.1"));
            assert_eq!(value["waves"][0][0]["agent_name"], json!("beads-1.1"));
            assert_eq!(value["waves"][0][0]["size"], json!("large"));
            assert_eq!(value["waves"][0][0]["waits_on"], json!([]));
            assert_eq!(
                value["waves"][0][0]["blocker_bead_ids"],
                json!(["beads-1.0"])
            );
            assert_eq!(value["waves"][0][0]["wave"], json!(0));
            assert_eq!(value["land_agent_name"], json!("beads-1.land"));
            assert_eq!(value["land_waits_on"], json!(["beads-1.1"]));
        });
    }

    #[test]
    fn query_handle_bindings_reject_wrong_handle_types() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let module = query_module(py);
            let specs = spec_list(py, &[spec_json("alpha", "WIP", None)]);
            let corpus =
                Py::new(py, py_compile_corpus(py, &specs).unwrap()).unwrap();
            let program =
                Py::new(py, py_compile_query("alpha").unwrap()).unwrap();
            let bad = PyDict::new_bound(py);
            let evaluate_many = module.getattr("evaluate_many").unwrap();

            let err = evaluate_many
                .call1((bad.clone(), corpus.clone_ref(py)))
                .unwrap_err();
            assert!(err.to_string().contains("QueryProgramHandle"));

            let err = evaluate_many.call1((program, bad)).unwrap_err();
            assert!(err.to_string().contains("QueryCorpusHandle"));
        });
    }

    #[test]
    fn plan_agent_cleanup_binding_round_trips_json_shape() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let targets = PyList::empty_bound(py);
            append_json(
                py,
                &targets,
                json!({
                    "identity": {"agent_type": "run", "cl_name": "done", "raw_suffix": "1"},
                    "agent_type": "run",
                    "status": "DONE",
                    "pid": null,
                    "workflow": null,
                    "parent_workflow": null,
                    "parent_timestamp": null,
                    "raw_suffix": "1",
                    "project_file": "/tmp/project.sase",
                    "artifacts_dir": "/tmp/artifacts",
                    "workspace": null,
                    "tribe": null,
                    "agent_clan": "shipping",
                    "agent_clan_generation": "current-gen",
                    "agent_name": "done",
                    "display_name": "done",
                    "start_time": null,
                    "stop_time": null,
                    "is_workflow_child": false,
                    "agent_family_parallel": false,
                    "appears_as_agent": false,
                    "step_type": null
                }),
            );
            let request_obj = json_value_to_py(
                py,
                &json!({
                    "schema_version": sase_core::AGENT_CLEANUP_WIRE_SCHEMA_VERSION,
                    "scope": "clan",
                    "mode": "dismiss_completed",
                    "focused_panel_tribe": null,
                    "tribe": null,
                    "clan_name": "shipping",
                    "clan_generation": "current-gen",
                    "identities": [],
                    "include_pidless_as_dismissable": false
                }),
            )
            .unwrap();
            let request = request_obj.bind(py).downcast::<PyDict>().unwrap();

            let result = py_plan_agent_cleanup(py, &targets, request).unwrap();
            let value = py_to_json_value(result.bind(py)).unwrap();

            assert_eq!(
                value["schema_version"],
                json!(sase_core::AGENT_CLEANUP_WIRE_SCHEMA_VERSION)
            );
            assert_eq!(
                value["dismiss_items"][0]["identity"]["cl_name"],
                json!("done")
            );
            assert_eq!(value["kill_items"], json!([]));
            assert_eq!(value["confirmation_severity"], json!("dismiss"));
        });
    }

    #[test]
    fn plan_agent_cleanup_binding_rejects_schema_mismatch() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let targets = PyList::empty_bound(py);
            let request_obj = json_value_to_py(
                py,
                &json!({
                    "schema_version": 999,
                    "scope": "all_panels",
                    "mode": "dismiss_completed"
                }),
            )
            .unwrap();
            let request = request_obj.bind(py).downcast::<PyDict>().unwrap();

            let err = py_plan_agent_cleanup(py, &targets, request).unwrap_err();
            assert!(err.to_string().contains("schema mismatch"));
        });
    }

    #[test]
    fn notification_store_binding_round_trips_json_shape() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let (_temp, path) = temp_notification_path("notifications.jsonl");
            let notification_obj = json_value_to_py(
                py,
                &json!({
                    "id": "n1",
                    "timestamp": "2026-04-30T12:00:00+00:00",
                    "sender": "axe",
                    "icon": "🚀",
                    "notes": ["hello"],
                    "files": [],
                    "action": "EpicApproval",
                    "action_data": {},
                    "read": false,
                    "dismissed": false,
                    "silent": false,
                    "muted": false,
                    "snooze_until": null
                }),
            )
            .unwrap();
            let notification =
                notification_obj.bind(py).downcast::<PyDict>().unwrap();

            let appended = py_append_notification(
                py,
                path.to_str().unwrap(),
                notification,
            )
            .unwrap();
            let appended_value = py_to_json_value(appended.bind(py)).unwrap();
            assert_eq!(appended_value["appended_count"], json!(1));

            let snapshot = py_read_notifications_snapshot(
                py,
                path.to_str().unwrap(),
                false,
                false,
            )
            .unwrap();
            let snapshot_value = py_to_json_value(snapshot.bind(py)).unwrap();
            assert_eq!(snapshot_value["schema_version"], json!(1));
            assert_eq!(snapshot_value["notifications"][0]["id"], json!("n1"));
            assert_eq!(snapshot_value["notifications"][0]["icon"], json!("🚀"));
            assert_eq!(
                snapshot_value["notifications"][0]["action"],
                json!("EpicApproval")
            );
            assert_eq!(snapshot_value["counts"]["priority"], json!(1));

            let update_obj =
                json_value_to_py(py, &json!({"kind": "mark_read", "id": "n1"}))
                    .unwrap();
            let update = update_obj.bind(py).downcast::<PyDict>().unwrap();
            let outcome = py_apply_notification_state_update(
                py,
                path.to_str().unwrap(),
                update,
            )
            .unwrap();
            let outcome_value = py_to_json_value(outcome.bind(py)).unwrap();
            assert_eq!(outcome_value["matched_count"], json!(1));
            assert_eq!(outcome_value["changed_count"], json!(1));
        });
    }

    #[test]
    fn notification_store_current_snapshot_binding_reconciles_snoozes() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let (_temp, path) = temp_notification_path("notifications.jsonl");
            let notification_obj = json_value_to_py(
                py,
                &json!({
                    "id": "due",
                    "timestamp": "2000-01-01T00:00:00+00:00",
                    "sender": "axe",
                    "read": true,
                    "muted": true,
                    "snooze_until": "2000-01-02T00:00:00+00:00"
                }),
            )
            .unwrap();
            let notification =
                notification_obj.bind(py).downcast::<PyDict>().unwrap();
            py_append_notification(py, path.to_str().unwrap(), notification)
                .unwrap();

            let snapshot = py_read_current_notifications_snapshot(
                py,
                path.to_str().unwrap(),
                false,
            )
            .unwrap();
            let value = py_to_json_value(snapshot.bind(py)).unwrap();
            assert_eq!(value["expired_ids"], json!(["due"]));
            assert_eq!(value["notifications"][0]["muted"], json!(false));
            assert_eq!(value["notifications"][0]["read"], json!(false));
            assert_eq!(value["notifications"][0]["snooze_until"], json!(null));
            assert!(value["notifications"][0]["resurfaced_at"].is_string());
            assert_eq!(value["next_snooze_deadline"], json!(null));
        });
    }

    #[test]
    fn notification_store_binding_rejects_bad_update_shape() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let bad_obj = json_value_to_py(
                py,
                &json!({"kind": "mark_snoozed", "id": "n1"}),
            )
            .unwrap();
            let bad = bad_obj.bind(py).downcast::<PyDict>().unwrap();

            let err = py_apply_notification_state_update(
                py,
                "/tmp/notifications.jsonl",
                bad,
            )
            .unwrap_err();
            assert!(err
                .to_string()
                .contains("NotificationStateUpdateWire dict"));
        });
    }

    #[test]
    fn notification_store_counts_binding_omits_rows_and_persists() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let (_temp, path) = temp_notification_path("notifications.jsonl");
            let notification_obj = json_value_to_py(
                py,
                &json!({
                    "id": "n1",
                    "timestamp": "2026-04-30T12:00:00+00:00",
                    "sender": "axe",
                    "notes": [],
                    "files": [],
                    "action": null,
                    "action_data": {},
                    "read": false,
                    "dismissed": false,
                    "silent": false,
                    "muted": false,
                    "snooze_until": null
                }),
            )
            .unwrap();
            let notification =
                notification_obj.bind(py).downcast::<PyDict>().unwrap();
            py_append_notification(py, path.to_str().unwrap(), notification)
                .unwrap();

            let update_obj =
                json_value_to_py(py, &json!({"kind": "mark_read", "id": "n1"}))
                    .unwrap();
            let update = update_obj.bind(py).downcast::<PyDict>().unwrap();
            let outcome = py_apply_notification_state_update_counts(
                py,
                path.to_str().unwrap(),
                update,
            )
            .unwrap();
            let outcome_value = py_to_json_value(outcome.bind(py)).unwrap();
            assert_eq!(outcome_value["matched_count"], json!(1));
            assert_eq!(outcome_value["changed_count"], json!(1));
            assert_eq!(outcome_value["notifications"], json!([]));
            assert_eq!(outcome_value["counts"]["priority"], json!(0));
            assert_eq!(outcome_value["stats"]["loaded_rows"], json!(0));

            let snapshot = py_read_notifications_snapshot(
                py,
                path.to_str().unwrap(),
                true,
                false,
            )
            .unwrap();
            let snapshot_value = py_to_json_value(snapshot.bind(py)).unwrap();
            assert_eq!(snapshot_value["notifications"][0]["read"], json!(true));
        });
    }

    #[test]
    fn notification_store_append_and_rewrite_counts_bindings_omit_rows() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let (_temp, path) = temp_notification_path("notifications.jsonl");
            let notification_obj = json_value_to_py(
                py,
                &json!({
                    "id": "n1",
                    "timestamp": "2026-04-30T12:00:00+00:00",
                    "sender": "axe",
                    "notes": [],
                    "files": [],
                    "action": null,
                    "action_data": {},
                    "read": false,
                    "dismissed": false,
                    "silent": false,
                    "muted": false,
                    "snooze_until": null
                }),
            )
            .unwrap();
            let notification =
                notification_obj.bind(py).downcast::<PyDict>().unwrap();

            let appended = py_append_notification_counts(
                py,
                path.to_str().unwrap(),
                notification,
            )
            .unwrap();
            let appended_value = py_to_json_value(appended.bind(py)).unwrap();
            assert_eq!(appended_value["appended_count"], json!(1));
            assert_eq!(appended_value["matched_count"], json!(0));
            assert_eq!(appended_value["changed_count"], json!(0));
            assert_eq!(appended_value["rewritten"], json!(false));
            assert_eq!(appended_value["notifications"], json!([]));
            assert_eq!(appended_value["counts"]["priority"], json!(0));
            assert_eq!(appended_value["stats"]["loaded_rows"], json!(0));

            let snapshot = py_read_notifications_snapshot(
                py,
                path.to_str().unwrap(),
                true,
                false,
            )
            .unwrap();
            let snapshot_value = py_to_json_value(snapshot.bind(py)).unwrap();
            assert_eq!(snapshot_value["notifications"][0]["id"], json!("n1"));

            let replacement_obj = json_value_to_py(
                py,
                &json!([{
                    "id": "n2",
                    "timestamp": "2026-04-30T13:00:00+00:00",
                    "sender": "axe",
                    "notes": [],
                    "files": [],
                    "action": null,
                    "action_data": {},
                    "read": false,
                    "dismissed": false,
                    "silent": false,
                    "muted": false,
                    "snooze_until": null
                }]),
            )
            .unwrap();
            let replacement =
                replacement_obj.bind(py).downcast::<PyList>().unwrap();

            let rewritten = py_rewrite_notifications_counts(
                py,
                path.to_str().unwrap(),
                replacement,
            )
            .unwrap();
            let rewritten_value = py_to_json_value(rewritten.bind(py)).unwrap();
            assert_eq!(rewritten_value["matched_count"], json!(1));
            assert_eq!(rewritten_value["changed_count"], json!(1));
            assert_eq!(rewritten_value["appended_count"], json!(0));
            assert_eq!(rewritten_value["rewritten"], json!(true));
            assert_eq!(rewritten_value["notifications"], json!([]));

            let after = py_read_notifications_snapshot(
                py,
                path.to_str().unwrap(),
                true,
                false,
            )
            .unwrap();
            let after_value = py_to_json_value(after.bind(py)).unwrap();
            assert_eq!(after_value["notifications"][0]["id"], json!("n2"));
            assert_eq!(after_value["notifications"][1]["id"], json!("n1"));
            assert_eq!(
                after_value["notifications"].as_array().unwrap().len(),
                2
            );

            let _ = fs::remove_file(&path);
            let _ = fs::remove_file(
                path.with_file_name("notifications.jsonl.lock"),
            );
            let _ = fs::remove_dir(path.parent().unwrap());
        });
    }

    #[test]
    fn telemetry_bindings_round_trip_python_dicts() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let (_temp, path) = temp_telemetry_path();
            let batch_obj = json_value_to_py(
                py,
                &json!({
                    "samples": [{
                        "ts": 100,
                        "metric": "sase_agent_runs_total",
                        "kind": "counter",
                        "labels": {"provider": "codex"},
                        "source": "runner-1",
                        "value": 3.0
                    }],
                    "now_ts": 110
                }),
            )
            .unwrap();
            let batch = batch_obj.bind(py).downcast::<PyDict>().unwrap();
            let recorded = py_telemetry_record_batch(
                py,
                path.to_str().unwrap(),
                batch,
                1_000,
            )
            .unwrap();
            let recorded = py_to_json_value(recorded.bind(py)).unwrap();
            assert_eq!(recorded["samples_recorded"], json!(1));

            let cleanup_obj = json_value_to_py(
                py,
                &json!({
                    "label_matches": {"provider": ["codex"]},
                    "dry_run": true
                }),
            )
            .unwrap();
            let cleanup_request =
                cleanup_obj.bind(py).downcast::<PyDict>().unwrap();
            let cleanup = py_telemetry_cleanup_matching_labels(
                py,
                path.to_str().unwrap(),
                cleanup_request,
                1_000,
            )
            .unwrap();
            let cleanup = py_to_json_value(cleanup.bind(py)).unwrap();
            assert_eq!(cleanup["dry_run"], json!(true));
            assert_eq!(cleanup["raw_rows"], json!(1));
            assert_eq!(cleanup["total_rows"], json!(1));

            let instant_obj = json_value_to_py(
                py,
                &json!({
                    "metric": "sase_agent_runs_total",
                    "group_by": [],
                    "now_ts": 110
                }),
            )
            .unwrap();
            let instant_request =
                instant_obj.bind(py).downcast::<PyDict>().unwrap();
            let instant = py_telemetry_query_instant(
                py,
                path.to_str().unwrap(),
                instant_request,
                1_000,
            )
            .unwrap();
            let instant = py_to_json_value(instant.bind(py)).unwrap();
            assert_eq!(instant["values"][0]["value"], json!(3.0));

            let range_obj = json_value_to_py(
                py,
                &json!({
                    "metric": "sase_agent_runs_total",
                    "start_ts": 100,
                    "end_ts": 159,
                    "step_seconds": 60,
                    "group_by": [],
                    "aggregation": "sum"
                }),
            )
            .unwrap();
            let range_request =
                range_obj.bind(py).downcast::<PyDict>().unwrap();
            let range = py_telemetry_query_range(
                py,
                path.to_str().unwrap(),
                range_request,
                1_000,
            )
            .unwrap();
            let range = py_to_json_value(range.bind(py)).unwrap();
            assert_eq!(range["series"][0]["points"][0]["value"], json!(3.0));

            let prune_obj = json_value_to_py(
                py,
                &json!({
                    "now_ts": 1_000,
                    "retention": {
                        "raw_seconds": 100,
                        "rollup_5m_seconds": 10_000,
                        "rollup_1h_seconds": 100_000
                    }
                }),
            )
            .unwrap();
            let prune_request =
                prune_obj.bind(py).downcast::<PyDict>().unwrap();
            let pruned = py_telemetry_prune(
                py,
                path.to_str().unwrap(),
                prune_request,
                1_000,
            )
            .unwrap();
            let pruned = py_to_json_value(pruned.bind(py)).unwrap();
            assert_eq!(pruned["raw_rows_folded"], json!(1));

            let stats =
                py_telemetry_store_stats(py, path.to_str().unwrap(), 1_000)
                    .unwrap();
            let stats = py_to_json_value(stats.bind(py)).unwrap();
            assert_eq!(stats["raw_sample_count"], json!(0));
            assert_eq!(stats["rollup_5m_count"], json!(1));
            assert_eq!(stats["last_write_by_subsystem"]["agent"], json!(100));
        });
    }

    #[test]
    fn agent_stats_binding_round_trips_python_dict() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let temp = temp_agent_stats_root();
            let root = temp.path();
            let projects = root.join("projects");
            let artifact =
                projects.join("proj/artifacts/ace-run/20260710010000");
            fs::create_dir_all(&artifact).unwrap();
            fs::write(
                projects.join("proj/proj.sase"),
                "NAME: binding-spec\nSTATUS: Ready\n",
            )
            .unwrap();
            fs::write(
                artifact.join("agent_meta.json"),
                serde_json::to_vec(&json!({
                    "name": "binding-agent",
                    "run_started_at": "100",
                    "llm_provider": "codex",
                    "model": "gpt-5",
                    "reasoning_effort": "high",
                    "cl_name": "binding-spec"
                }))
                .unwrap(),
            )
            .unwrap();
            fs::write(
                artifact.join("done.json"),
                serde_json::to_vec(&json!({
                    "outcome": "completed",
                    "finished_at": 160.0,
                    "step_output": {"meta_commits": [{
                        "sha": "abc",
                        "changespec_name": "binding-spec"
                    }]}
                }))
                .unwrap(),
            )
            .unwrap();
            let index = root.join("agent_artifact_index.sqlite");
            sase_core::rebuild_agent_artifact_index(
                &index,
                &projects,
                sase_core::AgentArtifactScanOptionsWire::default(),
            )
            .unwrap();

            let request_obj = json_value_to_py(
                py,
                &json!({
                    "start_ts": 0,
                    "end_ts": 200,
                    "runtime_group_by": "agent",
                    "bucket_seconds": 100,
                    "top_n": 5,
                    "project": "proj",
                    "work_top_n": 50
                }),
            )
            .unwrap();
            let request = request_obj.bind(py).downcast::<PyDict>().unwrap();
            let result =
                py_agent_stats_query_runs(py, index.to_str().unwrap(), request)
                    .unwrap();
            let result = py_to_json_value(result.bind(py)).unwrap();
            assert_eq!(result["schema_version"], json!(5));
            assert_eq!(result["totals"]["runs"], json!(1));
            assert_eq!(result["totals"]["completed"], json!(1));
            assert_eq!(result["providers"][0]["effort"], json!("high"));
            assert_eq!(
                result["runtime_groups"][0]["total_seconds"],
                json!(60.0)
            );
            assert_eq!(result["work"]["projects"][0]["project"], json!("proj"));
            // Legacy JSON key is still emitted for compatibility.
            assert_eq!(
                result["work"]["changespecs"][0]["name"], // legacy JSON key
                json!("binding-spec")
            );
            assert_eq!(result["runners"]["start_ts"], json!(100.0));
            assert_eq!(result["runners"]["end_ts"], json!(200.0));
            assert_eq!(result["runners"]["peak_runners"], json!(1));
            assert_eq!(result["runners"]["peak_seconds"], json!(60.0));
            assert_eq!(result["runners"]["average_runners"], json!(0.6));
            assert_eq!(result["runners"]["runner_seconds"], json!(60.0));
            assert_eq!(result["xprompts"]["runs_without_xprompts"], json!(1));
            assert_eq!(
                result["runners"]["distribution"][0]["seconds"],
                json!(40.0)
            );
            assert_eq!(
                result["runners"]["distribution"][1]["seconds"],
                json!(60.0)
            );
            assert_eq!(result["runners"]["trend"].as_array().unwrap().len(), 1);
        });
    }

    #[test]
    fn agent_activity_stats_binding_round_trips_python_dict() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let temp = temp_agent_stats_root();
            let root = temp.path();
            let projects = root.join("projects");
            let project = projects.join("proj");
            fs::create_dir_all(&project).unwrap();
            fs::write(
                project.join("skill_uses.jsonl"),
                concat!(
                    "{\"timestamp\":\"100\",\"skill_name\":\"review\",",
                    "\"agent_name\":\"binding-agent\"}\n"
                ),
            )
            .unwrap();
            fs::create_dir_all(
                root.join("interaction_requests/question/session"),
            )
            .unwrap();
            fs::write(
                root.join("interaction_requests/question/session/request.json"),
                serde_json::to_vec(&json!({
                    "request_id": "session",
                    "producer": {
                        "agent_name": "binding-agent",
                        "artifacts_dir": root
                            .join("projects/proj/artifacts/ace-run/one")
                    },
                    "payload": {
                        "timestamp": 120.0,
                        "questions": [{"question": "Continue?"}]
                    }
                }))
                .unwrap(),
            )
            .unwrap();
            let index = root.join("agent_artifact_index.sqlite");
            sase_core::rebuild_agent_artifact_index(
                &index,
                &projects,
                sase_core::AgentArtifactScanOptionsWire::default(),
            )
            .unwrap();

            let request_obj = json_value_to_py(
                py,
                &json!({
                    "start_ts": 0,
                    "end_ts": 200,
                    "top_n": 5,
                    "project": "proj"
                }),
            )
            .unwrap();
            let request = request_obj.bind(py).downcast::<PyDict>().unwrap();
            let result = py_agent_stats_query_activity(
                py,
                index.to_str().unwrap(),
                root.to_str().unwrap(),
                request,
            )
            .unwrap();
            let result = py_to_json_value(result.bind(py)).unwrap();
            assert_eq!(result["schema_version"], json!(5));
            assert_eq!(result["skills"][0]["name"], json!("review"));
            assert_eq!(result["skills"][0]["distinct_agents"], json!(1));
            assert_eq!(result["questions"]["sessions"], json!(1));
            assert_eq!(result["questions"]["asking_agents"], json!(1));
            assert_eq!(result["questions"]["questions"], json!(1));
            assert_eq!(result["coverage_start_ts"], json!(120.0));

            let _ = fs::remove_dir_all(root);
        });
    }
}
