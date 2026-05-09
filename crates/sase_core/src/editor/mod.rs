pub mod completion;
pub mod definition;
pub mod diagnostics;
pub mod directive;
pub mod file;
pub mod hover;
pub mod token;
pub mod wire;
mod xprompt_args;

pub use completion::{
    assist_entries_from_catalog, build_snippet_completion_candidates,
    build_xprompt_arg_name_candidates, build_xprompt_completion_candidates,
    classify_completion_context, colon_args_skeleton, named_args_skeleton,
};
pub use definition::{definition_at_position, DefinitionTarget};
pub use diagnostics::analyze_document;
pub use directive::{
    build_directive_completion_candidates, canonical_directive_name,
    directive_argument_candidates, directive_metadata, DIRECTIVES,
};
pub use file::{
    build_file_completion_candidates,
    build_file_completion_candidates_with_base,
    build_file_history_completion_candidates,
};
pub use hover::hover_at_position;
pub use token::{
    extract_token_at_position, is_path_like_token, is_slash_skill_like_token,
    is_snippet_trigger_token, is_xprompt_like_token,
    slash_skill_reference_name, xprompt_reference_name, DocumentSnapshot,
};
pub use wire::{
    CompletionCandidate, CompletionContext, CompletionContextKind,
    CompletionList, DiagnosticSeverity, DirectiveMetadata, EditorDiagnostic,
    EditorPosition, EditorRange, EditorTextEdit, HoverPayload, TokenInfo,
    XpromptAssistEntry, XpromptInputHint, EDITOR_WIRE_SCHEMA_VERSION,
};
