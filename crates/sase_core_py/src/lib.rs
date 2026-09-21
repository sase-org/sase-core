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
//! - `tokenize_query_with_profile(query: str, profile: dict) -> list[dict]`
//! - `parse_query_with_profile(query: str, profile: dict) -> dict`
//! - `canonicalize_query_with_profile(query: str, profile: dict) -> str`
//! - `compile_query_with_profile(query: str, profile: dict) -> QueryProgramHandle`
//! - `compile_corpus_with_profile(profile: dict, rows: list[dict]) -> QueryCorpusHandle`
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
//! - `replace_agent_artifact_index_dismissed_agents(index_path: str, identities: list[dict], force: bool = False) -> dict`
//! - `read_agent_artifact_index_meta(index_path: str, key: str) -> str | None`
//! - `write_agent_artifact_index_meta(index_path: str, key: str, value: str) -> None`
//! - `agent_artifact_index_status(index_path: str) -> dict`
//! - `vacuum_agent_artifact_index(index_path: str) -> dict`
//! - `query_agent_artifact_index(index_path: str, projects_root: str, query: dict | None = None, options: dict | None = None) -> dict`
//! - `agent_output_variable_history_wire_schema_version() -> int`
//! - `query_agent_output_variable_history(index_path: str, query: dict | None = None) -> dict`
//! - `agent_alias_history_wire_schema_version() -> int`
//! - `query_agent_alias_history(index_path: str, query: dict) -> dict`
//! - `agent_output_variable_selector_wire_schema_version() -> int`
//! - `parse_output_variable_selector(selector: str) -> dict`
//! - `query_agent_output_variable_selectors(index_path: str, query: dict | None = None) -> dict`
//! - `query_related_agent_artifact_dirs(index_path: str, artifact_dir: str, seed_timestamps: list[str]) -> list[str]`
//! - `query_agent_archive(root: str, request: dict) -> dict`
//! - `agent_archive_facet_counts(root: str, request: dict) -> dict`
//! - `validate_agent_archive_key(request: dict) -> dict`
//! - `validate_agent_archive_visibility(request: dict) -> dict`
//! - `validate_agent_archive_capabilities(request: dict) -> dict`
//! - `mark_agent_archive_bundles_revived(root: str, request: dict) -> dict`
//! - `verify_agent_archive_index(root: str) -> dict`
//! - `delete_dismissed_agent_group(root: str, group_id: str) -> bool`
//! - `plan_agent_cleanup(targets: list[dict], request: dict) -> dict`
//! - `save_dismissed_agents_index(path: str, identities: list[dict]) -> None`
//! - `save_dismissed_bundle(bundle_root: str, bundle: dict) -> dict`
//! - `delete_agent_artifacts(artifacts_dir: str) -> dict`
//! - `release_workspace_from_content(content: str, workspace_num: int, workflow: str | None, cl_name: str | None) -> dict`
//! - `agent_ownership_batch_wire_schema_version() -> int`
//! - `plan_agent_ownership_batch(request: dict) -> dict`
//! - `mark_hook_agents_as_killed(hooks: list[dict], suffixes: list[str]) -> list[dict]`
//! - `mark_mentor_agents_as_killed(mentors: list[dict], suffixes: list[str]) -> list[dict]`
//! - `mark_comment_agents_as_killed(comments: list[dict], suffixes: list[str]) -> list[dict]`
//! - `remove_workspace_suffix(status: str) -> str`
//! - `is_valid_status_transition(from_status: str, to_status: str) -> bool`
//! - `read_status_from_lines(lines: list[str], changespec_name: str) -> str | None`
//! - `apply_status_update(lines: list[str], changespec_name: str, new_status: str) -> str`
//! - `plan_status_transition(request: dict) -> dict`
//! - `canonical_pull_request_url(url: str) -> dict | None`
//! - `plan_external_pr_import(request: dict) -> dict`
//! - `repository_resolution_wire_schema_version() -> int`
//! - `canonical_repository_identity(value: str) -> dict | None`
//! - `resolve_repository_reference(request: dict) -> dict`
//! - `parse_git_name_status_z(stdout: str) -> list[dict]`
//! - `parse_git_branch_name(stdout: str) -> str | None`
//! - `derive_git_workspace_name(remote_url: str | None, root_path: str | None) -> str | None`
//! - `parse_git_conflicted_files(stdout: str) -> list[str]`
//! - `parse_git_local_changes(stdout: str) -> str | None`
//! - `retryability_wire_schema_version() -> int`
//! - `classify_failure_retryability(operation_kind: str, exit_status: int | None = None, stdout: str = "", stderr: str = "") -> dict`
//! - `decide_sidecar_publication_after_push(returncode: int, stdout: str, stderr: str, attempt: int) -> dict`
//! - `plan_agent_publication_batches(records: list[dict], budget_bytes: int) -> dict`
//! - `vcs_log_wire_schema_version() -> int`
//! - `parse_git_log(stdout: str) -> list[dict]`
//! - `classify_commit_presence(commits: list[dict], ahead_ids: list[str], behind_ids: list[str]) -> list[dict]`
//! - `classify_commit_origin(message: str) -> str`
//! - `classify_commit_types(commit: dict) -> list[str]`
//! - `aggregate_commit_log(repos: list[tuple[str, list[dict]]], limit: int) -> list[dict]`
//! - `parse_merge_summary(subject: str, body: str) -> dict | None`
//! - `disk_inventory_wire_schema_version() -> int`
//! - `classify_disk_inventory(request: dict) -> dict`
//! - `disk_cleanup_outcome_wire_schema_version() -> int`
//! - `normalize_disk_cleanup_outcome(request: dict) -> dict`
//! - `read_project_lifecycle_from_content(content: str) -> dict`
//! - `apply_project_lifecycle_update(content: str, state: str) -> str`
//! - `apply_project_aliases_update(content: str, aliases: list[str]) -> str`
//! - `apply_project_name_update(content: str, name: str | None) -> str`
//! - `list_project_records(projects_root: str, include_states: list[str], include_home: bool = False, projects_only: bool = False) -> list[dict]`
//! - `compile_prompt_history_query(raw_query: str, catalog: list[dict]) -> dict`
//! - `encode_prompt_history_literal(text: str) -> str`
//! - `build_prompt_history_seed(request: dict, catalog: list[dict]) -> dict`
//! - `match_prompt_history_rows(query: dict, rows: list[dict]) -> dict`
//! - `read_notifications_snapshot(path: str, include_dismissed: bool, expire_due_snoozes: bool = False) -> dict`
//! - `read_current_notifications_snapshot(path: str, include_dismissed: bool) -> dict`
//! - `apply_notification_state_update(path: str, update: dict) -> dict`
//! - `apply_notification_state_update_counts(path: str, update: dict) -> dict`
//! - `append_notification(path: str, notification: dict) -> dict`
//! - `append_notification_counts(path: str, notification: dict) -> dict`
//! - `append_notification_plus_one(path: str, request: dict) -> dict`
//! - `upsert_notification(path: str, request: dict) -> dict`
//! - `rewrite_notifications(path: str, notifications: list[dict]) -> dict`
//! - `rewrite_notifications_counts(path: str, notifications: list[dict]) -> dict`
//! - `classify_notification_tabs(notifications: list[dict]) -> dict`
//! - `resolve_notification_deliveries(rules: list[dict], notifications: list[dict]) -> list[dict]`
//! - `pending_action_from_notification(notification: dict, now: float) -> dict | None`
//! - `register_pending_action(path: str, action: dict) -> dict`
//! - `read_pending_action_store(path: str, legacy_path: str | None = None) -> dict`
//! - `merge_pending_action_transport(path: str, identifier: str, transport: str, record: dict, now: float | None = None) -> bool`
//! - `mark_pending_action_handled(path: str, identifier: str, source: str, action: str | None = None, now: float | None = None) -> bool`
//! - `remove_pending_action(path: str, identifier: str) -> bool`
//! - `pending_action_transport(request: dict) -> object`
//! - `read_prompt_stash_snapshot(path: str) -> dict`
//! - `append_prompt_stash(path: str, entry: dict) -> dict`
//! - `pop_prompt_stash(path: str, ids: list[str]) -> dict`
//! - `set_prompt_stash_pinned(path: str, ids: list[str], pinned: bool) -> dict`
//! - `rewrite_prompt_stash(path: str, entries: list[dict]) -> dict`
//! - `read_procs_snapshot(path: str) -> dict`
//! - `append_proc(path: str, proc: dict, history_limit: int) -> dict`
//! - `reserve_proc(path: str, request: dict, history_limit: int) -> dict`
//! - `update_proc(path: str, update: dict) -> dict`
//! - `prune_procs(path: str, history_limit: int) -> dict`
//! - `proc_runtime_retention_wire_schema_version() -> int`
//! - `apply_proc_runtime_retention(request: dict) -> dict`
//! - `agent_artifact_run_retention_wire_schema_version() -> int`
//! - `apply_agent_artifact_run_retention(request: dict) -> dict`
//! - `read_tasks_snapshot(path: str) -> dict` (legacy alias)
//! - `append_task(path: str, task: dict, history_limit: int) -> dict` (legacy alias)
//! - `update_task(path: str, update: dict) -> dict` (legacy alias)
//! - `prune_tasks(path: str, history_limit: int) -> dict` (legacy alias)
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
//! - `machine_setup_wire_schema_version() -> int`
//! - `classify_tailnet_health(request: dict) -> dict`
//! - `classify_tailnet_discovery(request: dict) -> dict`
//! - `reconcile_machine_enrollments(request: dict) -> dict`
//! - `assess_machine_init_review(request: dict) -> dict`
//! - `merge_machine_init_review(request: dict) -> dict`
//! - `validate_agent_name(name: str) -> None`
//! - `validate_agent_username(username: str) -> None`
//! - `validate_owner_root(root: str) -> None`
//! - `validate_agent_owner(username: str, machine_name: str) -> None`
//! - `validate_owned_agent_name(name: str, username: str, machine_name: str, known_owner_roots: list[str] | None = None) -> None`
//! - `validate_tribe_name(tribe: str) -> str`
//! - `canonicalize_public_tribe_name(tribe: str) -> str`
//! - `public_tribe_name(tribe: str) -> str`
//! - `parse_tribe_reference(value: str) -> str | None`
//! - `is_reserved_tribe_name(tribe: str) -> bool`
//! - `reserved_tribe_target_reason(tribe: str) -> str`
//! - `canonicalize_agent_tribe_metadata(data: dict) -> dict`
//! - `agent_tribe_display_key(stored_tribe: str, configured_keys: list[str]) -> str`
//! - `resolve_agent_tribe_display_config(request: dict) -> dict`
//! - `resolve_agent_tribe_identity(request: dict) -> dict`
//! - `commit_shas_equivalent(left: str, right: str) -> bool`
//! - `normalize_agent_archive_name(name: str) -> str`
//! - `normalize_owned_agent_name(name: str, username: str, machine_name: str, known_owner_roots: list[str] | None = None) -> str`
//! - `globalize_agent_name(local_name: str, username: str, machine_name: str) -> str`
//! - `globalize_owned_agent_name(name: str, username: str, machine_name: str, known_owner_roots: list[str] | None = None) -> str`
//! - `foreign_agent_owner_root(name: str, username: str, machine_name: str, known_owner_roots: list[str] | None = None) -> str | None`
//! - `strip_global_agent_name(global_name: str, username: str, machine_name: str) -> str`
//! - `parse_agent_family_name(name: str) -> dict`
//! - `parse_owned_agent_name(name: str, known_owner_roots: list[str] | None = None) -> dict`
//! - `agent_local_hood(name: str, known_owner_roots: list[str] | None = None) -> str`
//! - `agent_name_in_hood(name: str, hood: str, known_owner_roots: list[str] | None = None) -> bool`
//! - `agent_name_ancestors(name: str, known_owner_roots: list[str] | None = None) -> list[str]`
//! - `agent_link_target(name: str, username: str, machine_name: str, known_owner_roots: list[str] | None = None) -> dict`
//! - `tail_text_by_lines_and_chars(text: str, max_lines: int, max_chars: int) -> dict`
//! - `agent_relationship_schema_version() -> int`
//! - `validate_agent_relationship_batch(batch: dict) -> dict`
//! - `rewrite_agent_relationship_batch(batch: dict, destination_ids: dict[str, str]) -> dict`
//! - `project_agent_relationship_graph(batch: dict, destination_ids: dict[str, str], source_username: str, source_machine_name: str, destination_username: str, destination_machine_name: str, known_owner_roots: list[str] | None = None) -> dict`
//! - `agent_launch_wire_schema_version() -> int`
//! - `prepare_agent_launch(request: dict, python_executable: str, runner_script: str, output_root: str, sase_tmpdir: str | None = None, preallocated_env: dict | None = None) -> dict`
//! - `spawn_prepared_agent_process(prepared: dict, env: dict, claim_callback: Callable[[int], bool] | None = None) -> int`
//! - `allocate_launch_timestamp_batch(count: int, base_timestamp: str, after_timestamp: str | None = None) -> list[str]`
//! - `plan_agent_launch_fanout(prompt: str, launch_kind: str | None = None) -> dict`
//! - `next_admission_actions(plan: dict, states: dict, wait_facts: list[dict], hold_blocks: list[dict] | None = None) -> list[dict]`
//! - `bind_batch_predecessor_waits(prompt: str, predecessor: dict) -> dict`
//! - `inline_code_ranges(text: str, masked_ranges: list[tuple[int, int]] | None = None) -> list[tuple[int, int]]`
//! - `model_shortcut_context(text: str, position: dict) -> dict | None`
//! - `model_shortcut_edit(text: str, position: dict, entries: list[dict], selected_value: str) -> dict | None`
//! - `filter_explicit_model_shortcut_entries(entries: list[dict], query: str) -> list[dict]`
//! - `model_alias_shortcut_context(text: str, position: dict) -> dict | None`
//! - `model_alias_shortcut_edit(text: str, position: dict, entries: list[dict], selected_alias: str) -> dict | None`
//! - `filter_model_alias_shortcut_entries(entries: list[dict], query: str) -> list[dict]`
//! - `fenced_block_ranges(text: str) -> list[tuple[int, int]]`
//! - `fenced_block_details(text: str) -> list[dict]`
//! - `scan_directive_owned_fences(text: str) -> dict`
//! - `code_value_wire_schema_version() -> int`
//! - `resolve_agent_family_parent(request: dict) -> dict`
//! - `resolve_clan_summary(request: dict) -> dict`
//! - `resolve_clan_tribe(request: dict) -> dict`
//! - `list_workspace_claims_from_content(content: str) -> list[dict]`
//! - `plan_claim_workspace_from_content(content: str, request: dict) -> dict`
//! - `plan_transfer_workspace_claim_from_content(content: str, request: dict) -> dict`
//! - `allocate_and_claim_workspace_from_content(content: str, min_workspace: int, max_workspace: int, request: dict) -> dict`
//! - `decide_workspace_occupant_conflict(occupant: dict | None, caller: dict, occupant_pid_alive: bool, running_claim: dict | None, running_claim_pid_alive: bool) -> dict`
//! - `config_field_model(schema: dict) -> dict`
//! - `config_inventory(request: dict) -> dict`
//! - `config_plan_edit(request: dict) -> dict`
//! - `config_validate(request: dict) -> list[dict]`
//! - `effort_override_get(sase_home: str, now: float | None = None) -> dict | None`
//! - `effort_override_set_relative(sase_home: str, effort: str, source: str, duration_seconds: float | None = None, now: float | None = None) -> dict`
//! - `effort_override_set_until(sase_home: str, effort: str, expires_at: float, source: str, now: float | None = None) -> dict`
//! - `effort_override_clear(sase_home: str) -> bool`
//! - `agent_hold_wire_schema_version() -> int`
//! - `agent_hold_arm_relative(sase_home: str, armer: dict, scope: dict, selectors: dict, duration_seconds: float, liveness: dict | None = None, now: float | None = None, capture: dict | None = None) -> dict`
//! - `agent_hold_arm_until(sase_home: str, armer: dict, scope: dict, selectors: dict, expires_at: float, liveness: dict | None = None, now: float | None = None, capture: dict | None = None) -> dict`
//! - `agent_hold_rebind(sase_home: str, old_key: str, new_armer: dict, liveness: dict | None = None, now: float | None = None) -> dict | None`
//! - `agent_hold_release(sase_home: str, armer_key: str, liveness: dict | None = None, now: float | None = None) -> bool`
//! - `agent_hold_list(sase_home: str, liveness: dict | None = None, now: float | None = None) -> dict`
//! - `agent_hold_blocks_candidate(record: dict, candidate: dict) -> dict | None`
//! - `agent_hold_summarize_capture(scope: dict, identities: list[dict], armer: dict | None = None) -> dict`
//! - `agent_hold_deadlock_reaches(start_artifact_dir: str, candidate: dict, nodes: list[dict]) -> bool`
//! - `launch_unit_hold_key(request_id: str, logical_id: str) -> str`
//! - `launch_unit_hold_armer(unit: dict, request_id: str, project: str, pid: int, done_marker_path: str) -> dict`
//! - `feature_flag_state_wire_schema_version() -> int`
//! - `feature_flag_state_get(sase_home: str) -> dict`
//! - `feature_flag_state_set(sase_home: str, flag: str, enabled: bool) -> dict`
//! - `feature_flag_state_reconcile(sase_home: str, registered_keys: list[str]) -> dict`
//! - `fleet_contract_schema_version() -> int`
//! - `fleet_installation_identity_load(sase_home: str) -> dict`
//! - `fleet_installation_identity_ensure(sase_home: str) -> dict`
//! - `fleet_installation_identity_rotate(sase_home: str, request: dict) -> dict`
//! - `fleet_installation_identity_migrate(sase_home: str, request: dict) -> dict`
//! - `fleet_logical_locator_key(logical_locator: dict) -> str`
//! - `fleet_instance_locator_key(instance_locator: dict) -> str`
//! - `fleet_associate_owner_display_name(request: dict) -> dict`
//! - `fleet_project_resolved_agent_summary(request: dict) -> dict`
//! - `fleet_project_resolved_agent_detail(request: dict) -> dict`
//! - `fleet_validate_resolved_agent_summary(summary: dict) -> dict`
//! - `fleet_count_logical_agents(request: dict) -> dict`
//! - `fleet_follow_record_key(record: dict) -> str`
//! - `fleet_reconcile_follow_records(request: dict) -> dict`
//! - `fleet_followed_batch_family_promotions(request: dict) -> dict`
//! - `fleet_count_focus_and_fleet(request: dict) -> dict`
//! - `fleet_validate_catalog_query(request: dict) -> dict`
//! - `fleet_validate_catalog_cursor(cursor: str) -> str`
//! - `fleet_catalog_snapshot_id(scope: str, summaries: list[dict]) -> str`
//! - `fleet_accumulate_catalog_page(request: dict) -> dict`
//! - `fleet_validate_snapshot_freshness(freshness: dict) -> dict`
//! - `fleet_normalize_federation_response(request: dict) -> dict`
//! - `fleet_count_focus_and_fleet_from_federation(request: dict) -> dict`
//! - `fleet_classify_cursor_replay(request: dict) -> dict`
//! - `fleet_operation_payload_fingerprint(request: dict) -> dict`
//! - `fleet_decide_operation_replay(request: dict) -> dict`
//! - `fleet_mutation_payload_fingerprint(intent: dict) -> dict`
//! - `fleet_validate_mutation_request(request: dict) -> dict`
//! - `fleet_evaluate_mutation_precondition(intent: dict, observed: dict | None) -> dict`
//! - `fleet_partition_bulk_targets(targets: list[dict]) -> dict`
//! - `fleet_project_attention(origin_installation_id: str, rows: list[dict], resolved: list[dict], observed_at_unix: float) -> dict`
//! - `fleet_project_attention_inventory(origin_installation_id: str, rows: list[dict], resolved: list[dict], request: dict, observed_at_unix: float, freshness: dict) -> dict`
//! - `fleet_attention_payload_fingerprint(intent: dict) -> dict`
//! - `fleet_validate_attention_request(request: dict) -> dict`
//! - `fleet_validate_attention_inventory_request(request: dict) -> dict`
//! - `fleet_validate_attention_inventory_response(response: dict) -> dict`
//! - `fleet_evaluate_attention_precondition(intent: dict, capabilities: dict, observed: dict | None = None) -> dict`
//! - `fleet_decide_attention_notices(current: list[dict], ledger: list[dict], retention_window_seconds: float, now_unix: float) -> dict`
//! - `fleet_validate_connection_plan(plan: dict) -> dict`
//! - `fleet_issue_bootstrap(sase_home: str, request: dict) -> dict`
//! - `gateway_main(args: list[str]) -> None`
//! - `federation_worker_main(args: list[str]) -> None`
//! - `sudo_validate_manifest(manifest: dict) -> dict`
//! - `sudo_manifest_sha256(manifest: dict) -> str`
//! - `sudo_derive_risk_badges(manifest: dict) -> list[dict]`
//! - `sudo_validate_ledger(ledger: dict, manifest: dict | None = None) -> dict`
//! - `sudo_validate_handshake(handshake: dict, manifest: dict | None = None) -> dict`
//! - `sudo_runner_main(args: list[str]) -> None` — PyO3-hosted reviewed sudo
//!   runner. Detached hops relaunch as
//!   `<sys.executable> -I -m sase_core_rs.sudo_runner`.
//! - `fleet_classify_runtime_duration(request: dict) -> dict`
//! - `fleet_classify_cache_freshness(request: dict) -> dict`
//! - `runner_limit_override_get(sase_home: str, now: float | None = None) -> dict | None`
//! - `runner_limit_override_set_relative(sase_home: str, limit: int, source: str, duration_seconds: float | None = None, now: float | None = None) -> dict`
//! - `runner_limit_override_set_until(sase_home: str, limit: int, expires_at: float, source: str, now: float | None = None) -> dict`
//! - `runner_limit_override_clear(sase_home: str) -> bool`
//! - `provider_disable_wire_schema_version() -> int`
//! - `provider_disable_get(sase_home: str, now: float | None = None) -> dict`
//! - `provider_disable_set_relative(sase_home: str, provider: str, source: str, mode: str = "hard", duration_seconds: float | None = None, now: float | None = None) -> dict`
//! - `provider_disable_set_until(sase_home: str, provider: str, expires_at: float, source: str, mode: str = "hard", now: float | None = None) -> dict`
//! - `provider_disable_try_set_relative(sase_home: str, provider: str, source: str, mode: str = "hard", duration_seconds: float | None = None, now: float | None = None) -> dict`
//! - `provider_disable_try_set_until(sase_home: str, provider: str, expires_at: float, source: str, mode: str = "hard", now: float | None = None) -> dict`
//! - `provider_disable_clear(sase_home: str, provider: str) -> bool`
//! - `provider_priority_wire_schema_version() -> int`
//! - `provider_routing_context_wire_schema_version() -> int`
//! - `provider_availability_wire_schema_version() -> int`
//! - `provider_priority_get(sase_home: str, now: float | None = None) -> dict | None`
//! - `provider_priority_peek(sase_home: str, now: float | None = None) -> dict`
//! - `provider_priority_decode(data: bytes | None, now: float | None = None) -> dict`
//! - `provider_priority_set_relative(sase_home: str, provider: str, source: str, facts: dict, expected: dict | None = None, duration_seconds: float | None = None, now: float | None = None) -> dict`
//! - `provider_priority_set_until(sase_home: str, provider: str, expires_at: float, source: str, facts: dict, expected: dict | None = None, now: float | None = None) -> dict`
//! - `provider_priority_clear(sase_home: str, expected: dict | None = None, now: float | None = None) -> dict`
//! - `provider_routing_context_get(sase_home: str, now: float | None = None) -> dict`
//! - `provider_routing_context_from_parts(disables: list[dict], priority: dict | None, captured_at: float) -> dict`
//! - `provider_availability_classify(context: dict, facts: dict) -> dict`
//! - `provider_availability_classify_many(context: dict, facts: list[dict]) -> list[dict]`
//! - `provider_pool_eligibility_mask(records: list[dict]) -> list[bool]`
//! - `provider_pool_reservation_eligible(records: list[dict], reserved_index: int) -> bool`
//! - `provider_usage_observation_schema_version() -> int`
//! - `provider_usage_public_schema_version() -> int`
//! - `provider_usage_indicator_schema_version() -> int`
//! - `provider_usage_store_schema_version() -> int`
//! - `provider_usage_collector_failing_threshold() -> int`
//! - `provider_usage_state_path(sase_home: str) -> str`
//! - `provider_usage_load(sase_home: str, now: float, cadence_seconds: float = 300, warn_percent: float = 75, critical_percent: float = 90) -> dict`
//! - `provider_usage_record_observation(sase_home: str, observation: dict, now: float) -> dict`
//! - `provider_usage_prepare_account_context(sase_home: str, provider: str, context_id: str, now: float) -> dict`
//! - `provider_usage_reserve_refresh(sase_home: str, request: dict, now: float) -> dict`
//! - `provider_usage_release_refresh(sase_home: str, provider: str, context_id: str, account_generation: int, lease_id: str, now: float) -> bool`
//! - `provider_usage_refresh_due(sase_home: str, request: dict, now: float) -> dict`
//! - `provider_usage_admit_refresh(sase_home: str, request: dict, now: float) -> dict`
//! - `provider_usage_mark_refresh_due(sase_home: str, request: dict, now: float) -> dict`
//! - `provider_usage_record_refresh_attempt(sase_home: str, request: dict, now: float) -> dict`
//! - `provider_usage_validate_observation(observation: dict, now: float) -> dict`
//! - `provider_usage_normalize_agy_usage(request: dict) -> dict`
//! - `provider_usage_normalize_grok_billing(request: dict) -> dict`
//! - `provider_usage_normalize_muse_usage(request: dict) -> dict`
//! - `provider_usage_project_snapshot(observations: list[dict], now: float, cadence_seconds: float = 300, warn_percent: float = 75, critical_percent: float = 90) -> dict`
//! - `provider_usage_validate_indicator_config(indicator: dict | None = None) -> dict`
//! - `provider_usage_project_indicator(request: dict) -> dict`
//! - `provider_usage_remaining_percent(used_percent: float) -> float`
//! - `provider_usage_format_remaining_text(used_percent: float) -> str`
//! - `provider_usage_classify_freshness(observed_at: float, now: float, cadence_seconds: float = 300) -> str`
//! - `provider_usage_window_applies(applicability: dict, model_id: str | None = None) -> str`
//! - `provider_usage_summarize_for_model(windows: list[dict], model_id: str) -> dict | None`
//! - `resolve_effective_effort(explicit_effort: str | None = None, alias_effort: str | None = None, temporary_effort: str | None = None, configured_effort: str | None = None) -> dict`
//! - `size_model_route(size: str) -> dict`
//! - `select_epic_land_model(explicit_model: str | None, phase_count: int, threshold: int, epic_lander_model: str, big_epic_lander_model: str) -> dict`
//! - `parse_chop_result(document: str) -> dict`
//! - `validate_chop_result(result: dict) -> dict`
//! - `validate_chop_proposal(proposal: dict, index: int, prior_ids: list[str]) -> dict`
//! - `derive_chop_agent_name(chop_name: str, target_key: str | None, proposal_index: int, run_token: str | None = None) -> str`
//! - `normalize_chop_subprocess_diagnostic(request: dict) -> dict`
//! - `evaluate_chop_decision(request: dict) -> dict`
//! - `apply_chop_checkpoint_update(request: dict) -> dict`
//! - `check_and_record_chop_once_per(request: dict) -> dict`
//! - `release_chop_once_per(request: dict) -> dict`
//! - `expand_chop_targets(request: dict) -> dict`
//! - `parse_chop_duration(value: str) -> int`
//! - `split_axe_description(text: str) -> tuple[str, str]`
//! - `validate_axe_config(request: dict) -> list[dict]`
//! - `service_config_compose(request: dict) -> dict`
//! - `service_restart_decide(request: dict) -> dict`
//! - `service_state_read(sase_home: str, boot_id: str | None = None) -> dict`
//! - `service_state_mutate(sase_home: str, mutation: dict, boot_id: str | None = None, now: float | None = None) -> dict`
//! - `service_enablement_resolve(entry: dict, override: dict | None) -> dict`
//! - `service_status_build(request: dict) -> dict`
//! - `service_status_write(path: str, snapshot: dict) -> None`
//! - `service_status_read(path: str) -> dict | None`
//! - `chop_overrun_wire_schema_version() -> int`
//! - `classify_chop_overrun(request: dict) -> dict`
//! - `gate_followup_wire_schema_version() -> int`
//! - `gate_followup_attempt_id(gate_id: str, fingerprint: str) -> str`
//! - `decide_gate_followup(request: dict) -> dict`
//! - `axe_status_wire_schema_version() -> int`
//! - `classify_axe_status(request: dict) -> dict`
//! - `project_axe_status_public(snapshot: dict) -> dict`
//! - `sase_content_layout(home_root: str, project_root: str | None = None, chezmoi_root: str | None = None, project: str | None = None) -> dict`
//! - `continuation_wire_schema_version() -> int`
//! - `continuation_validate_node(record: dict) -> dict`
//! - `continuation_validate_graph(records: list[dict]) -> dict`
//! - `continuation_validate_agent_delta(delta: dict) -> dict`
//! - `continuation_validate_intent(intent: dict) -> dict`
//! - `continuation_validate_monitor_result(result: dict) -> dict`
//! - `continuation_validate_diagnostic_manifest(manifest: dict) -> dict`
//! - `continuation_validate_delivery_record(record: dict) -> dict`
//! - `continuation_new_delivery_record(request: dict) -> dict`
//! - `continuation_transition_delivery(request: dict) -> dict`
//! - `continuation_decide_resume_adoption(request: dict) -> dict`
//! - `continuation_plan_replay(request: dict) -> dict`
//! - `continuation_plan_retention(request: dict) -> dict`
//! - `continuation_select_evidence(request: dict) -> dict`
//! - `continuation_resolve_policy(request: dict) -> dict`
//! - `continuation_validate_policy(policy: dict) -> dict`
//! - `continuation_freeze_policy(request: dict) -> dict`
//! - `continuation_plan_budget(request: dict) -> dict`
//! - `continuation_validate_conditional_completion(intent: dict) -> dict`
//! - `continuation_seal_conditional_completion(request: dict) -> dict`
//! - `continuation_preview_conditional_completion(intent: dict) -> dict`
//! - `continuation_bind_conditional_completion(request: dict) -> dict`
//! - `continuation_rollback_conditional_completion_binding(request: dict) -> dict`
//! - `continuation_evaluate_conditional_completion(request: dict) -> dict`
//! - `continuation_consume_conditional_completion(request: dict) -> dict`
//! - `continuation_invalidate_conditional_completion(request: dict) -> dict`
//! - `continuation_render_conditional_completion_message(request: dict) -> dict`
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
//! - `artifact_ref_scan_document(text: str, known_kinds: list[str] | None = None) -> dict`
//! - `artifact_ref_document_scan_wire_schema_version() -> int`
//! - `artifact_ref_split_link_location(target: str) -> dict`
//! - `artifact_ref_link_location_wire_schema_version() -> int`
//! - `artifact_ref_resolve_document_source_target(path: str, owner: dict, context: dict) -> dict`
//! - `artifact_ref_target_resolution_wire_schema_version() -> int`
//! - `artifact_ref_wire_schema_version() -> int`
//! - `prompt_artifact_pool_filename(sha256: str, original_name: str) -> str`
//! - `prompt_artifact_manifest_parse(data: bytes) -> list[dict]`
//! - `prompt_artifact_manifest_render_record(record: dict) -> str`
//! - `prompt_artifact_manifest_select(records: list[dict], agent_artifacts_dir: str) -> list[dict]`
//! - `prompt_artifact_rewrite_links(prompt: str, records: list[dict], resolver: Callable[[dict], str | None]) -> dict`
//! - `prompt_artifact_wire_schema_version() -> int`
//! - `artifact_files_query(index_path: str, filters: dict) -> list[dict]`
//! - `artifact_context_query(index_path: str, groups: list[dict]) -> list[dict]`
//! - `artifact_context_query_wire_schema_version() -> int`
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
//! - `prompt_archive_inventory_wire_schema_version() -> int`
//! - `prompt_archive_inventory(root: str, request: dict | None = None) -> dict`
//! - `migration_wire_schema_version() -> int`
//! - `migration_manifest_normalize(manifest: dict) -> dict`
//! - `migration_journal_record_normalize(record: dict) -> dict`
//! - `migration_plan_next_step(manifest: dict, records: list[dict], observed_source_digests: dict) -> dict`
//! - `migration_tree_digest(root: str) -> dict`
//! - `migration_fingerprint(value: Any) -> str`
//! - `migration_residue_classify(entry: dict, facts: dict) -> dict`
//! - `migration_reconcile_procs(legacy_rows: list[dict], canonical_proc_ids: list[str | dict]) -> dict`
//! - `migration_patch_records_plan(path: str, data: bytes, facts: dict) -> dict`
//! - `migration_patch_records_apply(path: str, data: bytes, facts: dict) -> dict`
//! - `migration_patch_records_verify(path: str, original: bytes, converted: bytes, facts: dict) -> dict`
//! - `migration_gate_bundles_plan(envelope: dict, facts: dict) -> dict`
//! - `migration_gate_bundles_apply(envelope: dict, facts: dict) -> dict`
//! - `migration_gate_bundles_verify(original: dict, converted: dict, facts: dict) -> dict`
//! - `migration_acquire_bounded_lock(lock_path: str, timeout_ms: int, operation: str) -> MigrationBoundedLockHandle`
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
//! - `xprompt_argument_spans(text: str, entries: list[dict] | None = None) -> list[dict]`
//! - `raw_placeholder_fields(text: str, context_width: int) -> list[dict]`
//! - `substitute_raw_placeholders(text: str, values: dict[str, str]) -> str`
//! - `placeholder_input_names(texts: list[str]) -> list[str]`
//! - `directive_contract(enabled_feature_flags: list[str] | None = None) -> list[dict]`
//! - `collect_queue_fields(occurrences: list[dict], enabled_feature_flags: list[str] | None = None) -> dict`
//! - `format_queue_directive(fields: dict) -> str | None`
//! - `parse_queue_capacity(raw: str, enabled_feature_flags: list[str] | None = None) -> int`
//! - `normalize_persisted_queue_capacity(queue_capacity: int | None, queue_capacity_explicit: bool, effective_weight: float, global_limit: float, capacity_budget: bool) -> dict`
//! - `queue_directive_flag_key() -> str`
//! - `collect_hold_fields(occurrences: list[dict], enabled_feature_flags: list[str] | None = None) -> dict`
//! - `format_hold_directive(fields: dict) -> str | None`
//! - `hold_fields_to_selectors(fields: dict, pending_artifact_dirs: list[str] | None = None, identity: dict | None = None) -> dict`
//! - `runner_capacity_policy_schema_version() -> int`
//! - `runner_capacity_snapshot(request: dict) -> dict`
//! - `code_value_wire_schema_version() -> int`
//! - `directive_completion_context(text: str, line: int, character: int) -> dict | None`
//! - `directive_completion_candidates(context: dict, inventories: dict | None = None) -> dict`
//! - `bead_add_link(beads_dir: str, issue_id: str, target_ref: str, relation: str, description: str, origin: str = "manual", direction: str = "out", uses: int = 1, now: str | None = None, operation_id: str | None = None) -> dict`
//! - `bead_set_link_projection(beads_dir: str, issue_id: str, target_ref: str, relation: str, direction: str, present: bool, operation_id: str, description: str | None = None, origin: str | None = None, uses: int = 1, now: str | None = None) -> dict`
//! - `bead_set_link_projections(beads_dir: str, requests: list[dict]) -> dict`
//! - `bead_remove_link(beads_dir: str, issue_id: str, target_ref: str, relation: str | None = None, direction: str = "out", now: str | None = None, operation_id: str | None = None) -> dict`
//! - `bead_append_note(beads_dir: str, issue_id: str, entry: str, author: str | None = None, now: str | None = None) -> dict` (`issue["notes"]` is a list of note records)
//! - `bead_note_edit(beads_dir: str, issue_id: str, note_id: str, text: str, author: str | None = None, now: str | None = None) -> dict`
//! - `bead_note_remove(beads_dir: str, issue_id: str, note_id: str, author: str | None = None, now: str | None = None) -> dict`
//! - `bead_target_routing_wire_schema_version() -> int`
//! - `bead_route_targets(request: dict) -> dict`
//! - `bead_touch_index_wire_schema_version() -> int`
//! - `bead_touch_index_refresh(beads_dir: str, index_path: str) -> dict` (incremental, signature-cached rebuild of the actor-keyed touch index under `<index_path>.lock`; `reduced_streams` names exactly the streams re-reduced)
//! - `bead_touch_index_query(index_path: str, actors: list[str] | None = None) -> dict` (read-only: loads the index file only, never scans or parses streams; a missing, truncated, or wrong-schema index returns no rows; `actors=None` returns every actor's touches)
//! - `bead_touch_index_status(beads_dir: str, index_path: str) -> dict` (stat-only staleness report: `state` is `missing`, `unreadable`, `schema_mismatch`, `stale`, or `fresh`)
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
//! - `bead_needs_flag_type_migration(create_table_sql: str | None) -> bool`
//! - `bead_flag_type_migration_sql() -> str`
//! - `bead_needs_drop_flag_type_migration(create_table_sql: str | None) -> bool`
//! - `bead_drop_flag_type_migration_sql() -> str`
//! - `bead_prune_removed_flag_event_streams(beads_dir: str) -> dict`
//! - `bead_needs_external_ref_migration(create_table_sql: str | None) -> bool`
//! - `bead_external_ref_migration_sql() -> str`
//! - `bead_needs_task_type_migration(create_table_sql: str | None) -> bool`
//! - `bead_task_type_migration_sql() -> str`
//! - `telemetry_cleanup_matching_labels(store_path: str, request: dict, busy_timeout_ms: int = 250) -> dict`
//! - `telemetry_record_batch(store_path: str, batch: dict, busy_timeout_ms: int = 250) -> dict`
//! - `telemetry_query_instant(store_path: str, request: dict, busy_timeout_ms: int = 250) -> dict`
//! - `telemetry_query_range(store_path: str, request: dict, busy_timeout_ms: int = 250) -> dict`
//! - `telemetry_prune(store_path: str, request: dict, busy_timeout_ms: int = 250) -> dict`
//! - `telemetry_store_stats(store_path: str, busy_timeout_ms: int = 250) -> dict`
//! - `tool_run_wire_schema_version() -> int`
//! - `tool_run_normalize_definition(definition: dict) -> dict`
//! - `tool_run_canonicalize_fingerprint(fingerprint: dict) -> dict`
//! - `tool_run_unknown_evidence(reason: str) -> dict`
//! - `tool_run_begin(store_path: str, request: dict, busy_timeout_ms: int = 250) -> dict`
//! - `tool_run_append_event(store_path: str, request: dict, busy_timeout_ms: int = 250) -> dict`
//! - `tool_run_finish(store_path: str, request: dict, busy_timeout_ms: int = 250) -> dict`
//! - `tool_run_reconcile(store_path: str, request: dict, busy_timeout_ms: int = 250) -> dict`
//! - `tool_run_list(store_path: str, request: dict, busy_timeout_ms: int = 250) -> dict`
//! - `tool_run_show(store_path: str, request: dict, busy_timeout_ms: int = 250) -> dict`
//! - `tool_run_summary(store_path: str, request: dict, busy_timeout_ms: int = 250) -> dict`
//! - `tool_run_retention_preview(store_path: str, request: dict, busy_timeout_ms: int = 250) -> dict`
//! - `tool_run_retention_apply(store_path: str, request: dict, busy_timeout_ms: int = 250) -> dict`
//! - `tool_run_store_stats(store_path: str, busy_timeout_ms: int = 250) -> dict`
//! - `perf_logs_query(request: dict) -> dict`
//! - `agent_stats_query_runs(index_path: str, request: dict) -> dict` (run,
//!   runtime, project, and Patch work rollups)
//! - `agent_stats_query_activity(index_path: str, sase_home: str, request: dict)`
//!   `-> dict` (project-filterable skills and memories plus global documents)
//! - `compose_snippet_catalog(templates: dict[str, str]) -> dict` (composed
//!   templates, alias provenance, explicit-trigger validation, call graph,
//!   and missing/cycle diagnostics)
//! - `validate_snippet_trigger(trigger: str) -> dict`
//! - `apply_snippet_session_event(state: dict, event: dict) -> dict` (the
//!   nested snippet session engine's single entry point: `event["kind"]`
//!   is one of `plan`, `expand`, `advance`, `retreat`, `apply_edit`, or
//!   `clear`; the result dict always has `state`, `cursor_offset`, `text`,
//!   and `tabstop_offsets`)
//! - `artifact_ref_kind_catalog() -> list[dict]`
//! - `artifact_ref_kind_canonicalize(label: str) -> dict`
//! - `artifact_ref_parse_canonical(value: str) -> dict`
//! - `artifact_ref_quote_argument(argument: str) -> str`
//! - `artifact_ref_expansion_placeholders() -> list[str]`
//! - `artifact_ref_expansion_validate(format: str) -> list[str]`
//! - `artifact_ref_expansion_render(format: str, values: dict[str, str]) -> str`
//! - `artifact_ref_provider_spec_validate(spec: dict) -> None`
//! - `artifact_ref_provider_spec_digest(spec: dict) -> str`
//! - `artifact_ref_provider_spec_wire_schema_version() -> int`
//! - `finalizer_wire_schema_version() -> int`
//! - `validate_finalizer_provider_spec(spec: dict) -> None`
//! - `finalizer_provider_spec_digest(spec: dict) -> str`
//! - `validate_finalizer_instance_spec(spec: dict) -> None`
//! - `finalizer_instance_spec_digest(spec: dict) -> str`
//! - `resolve_finalizer_plan(request: dict) -> dict`
//! - `finalizer_plan_digest(plan: dict) -> str`
//! - `validate_finalizer_plan(plan: dict) -> str`
//! - `authenticate_finalizer_plan(plan: dict, expected_digest: str) -> str`
//! - `finalizer_context_digest(context: dict) -> str`
//! - `validate_finalizer_context(plan: dict, context: dict) -> str`
//! - `validate_finalizer_submission(plan: dict, context: dict, submission: dict) -> dict`
//! - `select_remaining_commit_obligations(request: dict) -> dict`
//! - `finalizer_json_digest(value: Any) -> str`
//! - `aggregate_finalizer_outcomes(results: list[dict]) -> dict`
//! - `bead_action_wire_schema_version() -> int`
//! - `parse_bead_action_field(payload: dict) -> str | None`
//! - `decide_bead_action(request: dict) -> dict`
//! - `validate_finalizer_bead_decision(context: dict, decision: dict) -> dict`
//! - `validate_finalizer_assigned_bead_binding(context: dict, expected: dict | None) -> None`
//! - `gate_decision_wire_schema_version() -> int`
//! - `decide_gate_decision_acceptance(request: dict) -> dict`
//! - `claim_gate_decision_execution(request: dict) -> dict`
//! - `gate_lifecycle_wire_schema_version() -> int`
//! - `decide_gate_lifecycle(request: dict) -> dict`
//! - `validate_task_type_spec(spec: dict) -> None`
//! - `task_type_spec_digest(spec: dict) -> str`
//! - `validate_task_type_field_values(spec: dict, values: dict[str, str]) -> list[dict]`
//! - `render_task_type_body(spec: dict, values: dict[str, str]) -> str`
//! - `parse_task_type_snapshot(data: str) -> dict`
//! - `serialize_task_type_snapshot(snapshot: dict) -> str`
//! - `task_type_spec_wire_schema_version() -> int`
//! - `artifact_ref_entry_validate(entry: dict) -> None`
//! - `artifact_ref_entry_wire_schema_version() -> int`
//! - `artifact_ref_use_manifest_parse(data: bytes) -> list[dict]`
//! - `artifact_ref_use_record_render(record: dict) -> str`
//! - `artifact_ref_use_wire_schema_version() -> int`
//! - `markdown_link_refs_wire_schema_version() -> int`
//! - `markdown_reference_links_scan(document: str) -> dict`
//! - `markdown_reference_label_allocate(scan: dict, destination: str, assigned: dict[str, str]) -> str`
//! - `markdown_reference_definitions_append(document: str, definitions: list[dict]) -> str`
//! - `referenced_by_wire_schema_version() -> int`
//! - `referenced_by_block_parse(document: str) -> dict`
//! - `referenced_by_block_render(table: dict) -> str`
//! - `referenced_by_block_upsert(document: str, table: dict) -> str`
//! - `referenced_by_block_remove(document: str) -> str`
//! - `referenced_by_block_strip(document: str) -> str`
//! - `artifact_link_row_schema_version() -> int`
//! - `artifact_row_resolution_wire_schema_version() -> int`
//! - `artifact_link_ref_parts(value: str) -> dict | None`
//! - `artifact_row_index_keys(identities: list[dict]) -> list[list[list[str]]]`
//! - `artifact_row_ref_lookup_keys(query: dict) -> list[list[str]]`
//! - `artifact_row_resolve(query: dict, candidates: list[dict]) -> dict | None`
//! - `artifact_link_canonicalize(value: str) -> str`
//! - `artifact_link_validate_row(row: dict) -> dict`
//! - `artifact_link_upsert_row(rows: list[dict], row: dict) -> dict`
//! - `artifact_link_merge_indexes(base: dict, ours: dict, theirs: dict) -> dict`
//! - `artifact_link_eligibility_wire_schema_version() -> int`
//! - `decide_artifact_link_eligibility(request: dict) -> dict`
//! - `artifact_link_release_evidence(decision: dict, recorded_at: str) -> dict`
//! - `validate_artifact_link_release_evidence(evidence: dict, expected_run_id: str, expected_agent_id: str) -> None`
//! - `artifact_relations_builtins() -> list[dict]`
//! - `artifact_relation_lookup(slug: str) -> dict`
//! - `artifact_relation_label(slug: str, this_is_source: bool) -> str`
//! - `links_block_parse(document: str) -> dict`
//! - `links_block_render(table: dict, host_document: str | None = None) -> str`
//! - `links_block_upsert(document: str, table: dict) -> str`
//! - `links_block_remove(document: str) -> str`
//! - `links_block_strip(document: str) -> str`
//! - `artifact_md_path(request: dict) -> dict`
//! - `companion_md_path(asset_path: str) -> dict`
//! - `artifact_link_frontmatter_inlet(document: str) -> dict`
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

mod agent_custody;
mod agent_holds;
mod agent_identity;
mod agent_launch;
mod agent_scan;
mod artifact_links;
mod artifact_refs;
mod axe;
mod bead_decisions;
mod beads;
mod config;
mod continuation;
mod editor_completion;
mod editor_content;
mod fleet;
mod fleet_attention;
mod json_bridge;
mod migration;
mod notifications;
mod plans;
mod prelude;
mod procs;
mod provider_policy;
mod query;
mod sudo;
mod telemetry;
mod vcs;

#[cfg(test)]
mod test_support;

use prelude::*;

pub use sase_core as core;

#[pymodule]
#[pyo3(name = "sase_core_rs")]
fn sase_core_rs(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    query::register_query(m)?;
    agent_identity::register_agent_identity(m)?;
    vcs::register_vcs(m)?;
    config::register_config(m)?;
    editor_completion::register_editor_completion(m)?;
    editor_content::register_editor_content(m)?;
    agent_scan::register_agent_scan(m)?;
    agent_custody::register_agent_custody(m)?;
    bead_decisions::register_bead_decisions(m)?;
    beads::register_beads(m)?;
    plans::register_plans(m)?;
    artifact_refs::register_artifact_refs(m)?;
    artifact_links::register_artifact_links(m)?;
    migration::register_migration(m)?;
    notifications::register_notifications(m)?;
    procs::register_procs(m)?;
    agent_launch::register_agent_launch(m)?;
    provider_policy::register_provider_policy(m)?;
    axe::register_axe(m)?;
    agent_holds::register_agent_holds(m)?;
    fleet::register_fleet(m)?;
    fleet_attention::register_fleet_attention(m)?;
    sudo::register_sudo(m)?;
    continuation::register_continuation(m)?;
    telemetry::register_telemetry(m)?;
    Ok(())
}
