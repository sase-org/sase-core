//! Pure-Rust config-domain backend for the SASE Config Center.
//!
//! Implements the four JSON-in / JSON-out contracts used by the TUI (and, in
//! future, a CLI / web / editor frontend):
//!
//! - [`config_field_model`] flattens a JSON Schema into an ordered field list.
//! - [`config_inventory`] merges the layer stack and reports per-field
//!   provenance + diagnostics.
//! - [`config_plan_edit`] turns a set/unset into a logical write-plan plus the
//!   candidate merged config, effective preview, and validation.
//! - [`config_validate`] schema-validates a candidate merged config.
//!
//! Python owns plugin/layer discovery, file IO, YAML parsing, and the
//! source-preserving write; this crate owns the deterministic decision logic.
//! The merge mirrors `sase.config.core._deep_merge` exactly so the two can
//! never silently diverge (see the parity tests).

pub mod axe;
pub mod merge;
pub mod plan;
pub mod provenance;
pub mod schema;
pub mod validate;
pub mod wire;

use serde_json::Value;

pub use axe::{
    compose_axe_config, plan_axe_entry_mutation, AxeConfigComposeRequestWire,
    AxeConfigCompositionWire, AxeEntryMutationPlanWire,
    AxeEntryMutationRequestWire, AxeEntryPreviewWire, AxeEntrySelectorWire,
    AxeFieldOperationWire, AxeFieldProvenanceWire, AxeInventoryEntryWire,
    AxeRawContributionWire,
};
pub use wire::{
    ConfigConstraintsWire, ConfigContributionWire, ConfigDiagnosticWire,
    ConfigEditOpWire, ConfigEditPlanWire, ConfigEditRequestWire,
    ConfigEffectivePreviewWire, ConfigError, ConfigFieldModelWire,
    ConfigFieldStateWire, ConfigFieldWire, ConfigInventoryRequestWire,
    ConfigInventoryWire, ConfigLayerInputWire, ConfigSourceWire,
    ConfigValidateRequestWire, ConfigWritePlanWire, ListStrategy,
    CONFIG_WIRE_SCHEMA_VERSION,
};

/// Flatten a JSON Schema document into the ordered config field model.
pub fn config_field_model(
    schema: &Value,
) -> Result<ConfigFieldModelWire, ConfigError> {
    schema::build_field_model(schema)
}

/// Merge the layer stack and report sources, per-field provenance, and
/// diagnostics.
pub fn config_inventory(
    request: &ConfigInventoryRequestWire,
) -> Result<ConfigInventoryWire, ConfigError> {
    provenance::build_inventory(request)
}

/// Plan a single set/unset edit, returning the write-plan, candidate merged
/// config, effective preview, and validation diagnostics.
pub fn config_plan_edit(
    request: &ConfigEditRequestWire,
) -> Result<ConfigEditPlanWire, ConfigError> {
    plan::plan_edit(request)
}

/// Schema-validate a candidate merged config, returning all diagnostics.
pub fn config_validate(
    request: &ConfigValidateRequestWire,
) -> Vec<ConfigDiagnosticWire> {
    validate::validate_config(&request.schema, &request.config)
}
