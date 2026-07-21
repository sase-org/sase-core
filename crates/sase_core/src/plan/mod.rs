//! Pure-Rust plan discovery contract for `sase plan search`.
//!
//! Mirrors the bead-search architecture (`bead/`): owned wire records plus a
//! read layer that scans and parses markdown plan artifacts, with no PyO3
//! types so binding/TUI/web frontends can reuse the same logic. This phase
//! covers the wire model, discovery (read layer), and the search/filter/rank
//! engine built on top of it.

pub mod artifact_link;
pub mod read;
pub mod search;
pub mod validate;
pub mod wire;

pub use artifact_link::{
    parse_sdd_artifact_link, render_sdd_artifact_link,
    upsert_sdd_artifact_link, SddArtifactDocumentWire, SddArtifactLinkKindWire,
    SddArtifactLinkTypeWire, SddLegacyArtifactLinkWire,
};
pub use read::{read_plans, PLAN_READ_WIRE_SCHEMA_VERSION};
pub use search::{
    search_plans, PLAN_SEARCH_FIELD_NAMES, PLAN_SEARCH_WIRE_SCHEMA_VERSION,
};
pub use validate::{
    plan_frontmatter_schema, plan_validate, plan_validate_with_mode,
    PlanDiagnosticWire, PlanFrontmatterFieldSpecWire, PlanPhaseWire,
    PlanValidationMode, PlanValidationResultWire, ValidatedPlanWire,
};
pub use wire::{
    PlanError, PlanSearchMatchWire, PlanWire, PLAN_WIRE_SCHEMA_VERSION,
};
