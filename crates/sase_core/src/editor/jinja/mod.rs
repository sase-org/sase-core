//! Jinja2 completion catalog, assist wire types, tag scanning, slot
//! classification, document scope analysis, and ranked assistance.
//!
//! The catalog ([`jinja_catalog`](crate::editor::jinja::jinja_catalog))
//! is the static source of truth for every name sase injects into agent
//! prompts, Jinja's own globals, and the identifier-named Jinja 3.1
//! filters, tests, and statement keywords the prompt environment
//! supports. The scan phase finds the tag at the cursor, classifies the
//! completion slot, and extracts the document scope for the assist phase.

mod assist;
mod catalog;
pub mod context;
mod docs;
mod hover;
pub mod scan;
pub mod scope;
mod scope_vars;
mod wire;

pub use assist::jinja_completion;
pub use catalog::jinja_catalog;
pub use context::{jinja_completion_slot, JinjaSlot, JinjaSlotContext};
pub use hover::jinja_hover;
pub use scan::{jinja_tag_at_cursor, JinjaTag, JinjaTagKind};
pub use scope::{
    jinja_declared_inputs, jinja_directive_presence, jinja_document_scope,
    jinja_skill_enabled, jinja_template_scope, JinjaDeclaredInput,
    JinjaDocumentScope, JinjaLocalKind, JinjaOpenBlock, JinjaTemplateLocal,
};
pub use scope_vars::jinja_scope_variables;
pub use wire::{
    JinjaAssistRequestWire, JinjaAvailabilityRule, JinjaAvailabilityState,
    JinjaAvailabilityWire, JinjaCatalogFilterWire, JinjaCatalogGlobalWire,
    JinjaCatalogStatementWire, JinjaCatalogTestWire, JinjaCatalogVariableWire,
    JinjaCatalogWire, JinjaCompletionItemKind, JinjaCompletionItemWire,
    JinjaCompletionSlotKind, JinjaCompletionSource, JinjaCompletionWire,
    JinjaFilterTier, JinjaNamespaceMemberWire, JinjaScopeKind,
    JinjaScopeRequestWire, JinjaScopeVariablesWire, JinjaUnavailableWire,
    JINJA_CATALOG_WIRE_SCHEMA_VERSION,
};
