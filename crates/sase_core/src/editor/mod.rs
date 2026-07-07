pub mod completion;
pub mod definition;
pub mod diagnostics;
pub mod directive;
pub mod file;
mod frontmatter;
pub mod hover;
pub mod token;
pub mod wire;
mod xprompt_args;

pub use completion::{
    apply_vcs_project_selection, apply_vcs_ref_selection,
    apply_vcs_repo_selection, assist_entries_from_catalog,
    build_snippet_completion_candidates,
    build_vcs_project_completion_candidates,
    build_vcs_ref_completion_candidates, build_vcs_repo_completion_candidates,
    build_xprompt_arg_name_candidates, build_xprompt_completion_candidates,
    classify_completion_context, classify_completion_context_with_workflows,
    colon_args_skeleton, detect_vcs_ref_context_at_position,
    detect_vcs_repo_context_at_position, named_args_skeleton,
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
pub use frontmatter::{
    field_schema as frontmatter_field_schema,
    input_type_schema as frontmatter_input_type_schema,
    validate as validate_frontmatter,
    validate_field as validate_frontmatter_field,
};
pub use hover::hover_at_position;
pub use token::{
    extract_token_at_position, is_path_like_token, is_slash_skill_like_token,
    is_snippet_trigger_token, is_vcs_project_trigger_token,
    is_xprompt_like_token, slash_skill_reference_name,
    vcs_project_trigger_token, xprompt_reference_name, DocumentSnapshot,
};
pub use wire::{
    CompletionCandidate, CompletionContext, CompletionContextKind,
    CompletionList, DiagnosticSeverity, DirectiveMetadata, EditorDiagnostic,
    EditorPosition, EditorRange, EditorTextEdit, FrontmatterFieldKind,
    FrontmatterFieldSchema, FrontmatterInputType, HoverPayload, TokenInfo,
    VcsNamespaceEntry, VcsProjectEntry, VcsRefTrigger, VcsRepoCatalogRequest,
    VcsRepoCatalogResponse, VcsRepoEntry, VcsRepoTrigger, XpromptAssistEntry,
    XpromptInputHint, EDITOR_WIRE_SCHEMA_VERSION,
    VCS_REPO_CATALOG_SCHEMA_VERSION,
};
pub(crate) use xprompt_args::{
    find_matching_bracket_for_args, parse_xprompt_reference_body,
};
