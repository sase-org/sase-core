//! Jinja2 completion catalog and assist wire types.
//!
//! The catalog ([`jinja_catalog`](crate::editor::jinja::jinja_catalog))
//! is the static source of truth for every name sase injects into agent
//! prompts, Jinja's own globals, and the identifier-named Jinja 3.1
//! filters, tests, and statement keywords the prompt environment
//! supports. Later phases build scope analysis, ranking, and hover on top
//! of these tables.

mod catalog;
mod wire;

pub use catalog::jinja_catalog;
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
