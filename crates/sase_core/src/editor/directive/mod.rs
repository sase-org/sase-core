mod candidate_lists;
mod context;
mod contract;
mod metadata;
#[cfg(test)]
mod tests;

pub(in crate::editor) use candidate_lists::wait_queue_keyword_retired;
pub use candidate_lists::{
    build_bead_completion_candidates, build_directive_completion_candidates,
    build_directive_completion_candidates_with_flags,
    build_directive_keyword_candidates,
    build_directive_static_value_candidates,
    build_filtered_directive_keyword_candidates, directive_argument_candidates,
    rank_and_filter_bead_entries,
};
pub use context::{
    detect_directive_context_at_position, is_directive_like_token,
};
pub use contract::{
    canonical_directive_name, directive_allows_keywords, directive_contract,
    directive_contract_with_flags, directive_is_hidden_from_name_completion,
    directive_is_hidden_from_name_completion_with_flags, directive_metadata,
    directive_metadata_with_flags, if_directive_metadata,
    queue_directive_metadata,
};
pub(crate) use contract::{
    directive_argument_open_colon_at, directive_argument_open_double_colon_at,
};
pub use metadata::{BEAD_COMPLETION_LIMIT, DIRECTIVES};
