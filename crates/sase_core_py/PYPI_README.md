# sase-core-rs

Rust core for the [sase](https://github.com/sase-org/sase) TUI/CLI, packaged as a
PyO3 extension module.

The wheel installs the import module `sase_core_rs`. It is consumed by `sase` via
the dispatch facade in `sase.core` and is opt-in through `SASE_CORE_BACKEND=rust`
during the rollout. From Phase 6 onward the `sase` package depends on this
distribution so released `sase` installs receive a loadable Rust extension
without a local Rust toolchain.

## Provided functions

- `parse_project_bytes(path, data)` — ChangeSpec parser.
- `tokenize_query`, `parse_query`, `canonicalize_query`,
  `evaluate_query_many` — query language.
- `scan_agent_artifacts(projects_root, options=None)` — agent-artifact
  filesystem scanner.
- `remove_workspace_suffix`, `is_valid_status_transition`,
  `read_status_from_lines`, `apply_status_update`,
  `plan_status_transition` — status state machine helpers.
- `parse_git_name_status_z`, `parse_git_branch_name`,
  `derive_git_workspace_name`, `parse_git_conflicted_files`,
  `parse_git_local_changes` — git query parsers.
- `axe_status_wire_schema_version()` — portable AXE status contract version.
- `classify_axe_status(request)` — pure AXE lifecycle and worker-health
  classification from already-collected host observations.

`classify_axe_status` performs no filesystem, process, or clock access. Python
supplies lock/PID liveness, marker ages, runner occupancy, lumberjack reports,
and any typed collection error; Rust validates and normalizes those
observations into the stable status snapshot shared by human and JSON
frontends.

## Source

`https://github.com/sase-org/sase-core` — the Cargo workspace lives there.
This wheel is built from `crates/sase_core_py/`.

## License

Dual-licensed under MIT or Apache-2.0, at your option.
