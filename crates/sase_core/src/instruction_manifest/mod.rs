//! Instruction manifest v1 wire contract for E2 shadow renders.
//!
//! The host (Python) composes bundles and assembles manifests; this module
//! owns the closed vocabulary, the validation invariants, and the
//! `common_digest` definition that E3 conformance, E4 history, E6 ledger,
//! and E7 evaluator all consume.

mod normalize;
mod wire;

pub use normalize::{
    compute_common_digest, normalize_instruction_manifest,
    InstructionManifestError,
};
pub use wire::{
    instruction_manifest_wire_schema_version, BudgetWire, BundleWire,
    CompilerWire, DeliveryWire, FactsWire, InstructionManifestWire,
    LayerBudgetWire, ObservationWire, SectionWire, SourceWire,
    INSTRUCTION_MANIFEST_WIRE_SCHEMA_VERSION,
};

#[cfg(test)]
mod tests;
