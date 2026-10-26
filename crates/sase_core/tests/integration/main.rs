//! Consolidated integration-test binary for `sase_core`.
//!
//! Every integration test lives here as one `mod` per former top-level test
//! file, so `cargo test -p sase_core` links the `sase_core` rlib once instead
//! of once per file. The shared `tests/fixtures/` directory is untouched.
//!
//! Two files stay outside this binary on purpose, as their own test targets:
//! - `tests/hermetic_test_env.rs`: the hermetic-environment canary asserts no
//!   `SASE_*` variable is set, so sharing a process with tests that mutate
//!   process-global state would make it flaky.
//! - `tests/artifact_ref_commit_budget.rs`: it drives a process-global env
//!   budget override and deliberately holds exactly one test so no second
//!   test can race its `set_var`.

mod agent_scan_parity;
mod bead_event_parity;
mod bead_read_model_mutation_proof;
mod bead_read_model_parity;
mod bead_read_parity;
mod bead_storage_parity;
mod bead_touch_index_parity;
mod command_line_perf;
mod command_line_resolver;
mod config_parity;
mod continuation_contract;
mod git_query_parity;
mod golden_corpus_parity;
mod notification_store_parity;
mod plan_validate_parity;
mod prompt_stash_archive;
mod prompt_stash_store_parity;
mod prompt_stash_trash_lifecycle;
mod python_wire_parity;
mod query_evaluator_parity;
mod retryability_parity;
mod vcs_log_parity;
