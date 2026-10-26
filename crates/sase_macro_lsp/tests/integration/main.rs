//! Consolidated integration-test binary for `sase_macro_lsp`.
//!
//! Every integration test lives here as one `mod` per former top-level test
//! file, so `cargo test -p sase_macro_lsp` links once instead of once per
//! file.
//!
//! Two files stay outside this binary on purpose, as their own test targets:
//! `tests/jsonrpc_stdio_model_shortcut.rs` and
//! `tests/jsonrpc_stdio_model_alias_shortcut.rs` each drive a process-global
//! `SASE_MACRO_MODEL_CATALOG` override through `initialize`, and each
//! deliberately holds exactly one test so no second test can race its
//! `set_var`. Merging them here would race both files on the same variable.

mod jsonrpc_macro_lsp;
mod jsonrpc_stdio;
mod jsonrpc_stdio_alternation;
mod jsonrpc_stdio_choice_completion;
mod jsonrpc_stdio_choice_diagnostics;
mod jsonrpc_stdio_jinja;
mod jsonrpc_stdio_on_type_formatting;
mod jsonrpc_stdio_spacer_paren;
