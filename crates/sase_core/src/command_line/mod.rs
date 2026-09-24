//! Frozen command-line grammar resolver for the `:` panel.
pub mod complete;
pub mod diagnostics;
pub mod grammar;
pub mod help;
pub mod resolve;
pub mod run_policy;
pub mod signature;
pub mod tokenizer;
pub mod wire;

pub use complete::{complete_line, insert_text, shlex_quote};
pub use diagnostics::{sort_diagnostics, RawDiagnostic};
pub use grammar::{
    CommandLineGrammar, CommandLineGrammarError, CommandNode, OptionNode,
    PositionalCapacity, PositionalNode,
};
pub use help::command_help;
pub use resolve::{is_negative_number, looks_like_option, resolve_line};
pub use run_policy::{evaluate_run_policy, json_scalar_string};
pub use signature::{build_signature, usage_string, ActiveSlot};
pub use tokenizer::{lex_line, raw_to_wire, unquoted_prefix, RawToken};
pub use wire::{
    CommandHelpWire, CommandLineCompletionWire, CommandLineSpecWire,
    CommandSpecWire, CompletionItemWire, DynamicCandidateWire, HelpChildWire,
    HelpOptionWire, HelpPositionalWire, LineContextWire, LineDiagnosticWire,
    LineSignatureWire, LineSlotWire, LineTokenWire, NargsWire, NargsWireOut,
    OptionSpecWire, PositionalSpecWire, RunPolicyOutcomeWire,
    RunPolicyRuleWire, RunPolicyWhenWire, SignatureSegmentWire,
    COMMAND_LINE_WIRE_SCHEMA_VERSION,
};

impl CommandLineGrammar {
    pub fn resolve(&self, line: &str, cursor: usize) -> LineContextWire {
        resolve_line(self, line, cursor)
    }

    pub fn complete(
        &self,
        line: &str,
        cursor: usize,
        dynamic: &[DynamicCandidateWire],
        selected: &[String],
        limit: usize,
    ) -> CommandLineCompletionWire {
        complete_line(self, line, cursor, dynamic, selected, limit)
    }

    pub fn command_help_path(
        &self,
        path: &[String],
    ) -> Option<CommandHelpWire> {
        command_help(self, path)
    }
}
