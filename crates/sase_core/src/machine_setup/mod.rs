//! Shared Tailnet discovery and enrollment reconciliation policy.
//!
//! Python retains subprocess management, HTTP transport, and CLI prompts.
//! Callers provide status/health observations and receive candidates,
//! diagnostics, and reconciliation decisions.

mod health;
mod peers;
mod reconcile;
mod wire;

#[cfg(test)]
mod tests;

use thiserror::Error;

pub use health::classify_tailnet_health;
pub use peers::classify_tailnet_discovery;
pub use reconcile::reconcile_machine_enrollments;
pub use wire::{
    DiscoveryCandidateWire, EnrolledMachineWire, MachineReconcileRequestWire,
    MachineReconcileResultWire, MachineSetupDiagnosticWire,
    ReconciledCandidateWire, TailnetDiscoveryRequestWire,
    TailnetDiscoveryResultWire, TailnetHealthObservationWire,
    TailnetHealthRequestWire, TailnetHealthResultWire, TailnetPeerWire,
    COMPATIBILITY_COMPATIBLE, COMPATIBILITY_INCOMPATIBLE,
    COMPATIBILITY_UNKNOWN, ENDPOINT_SOURCE_DNS, ENDPOINT_SOURCE_OVERRIDE,
    MACHINE_SETUP_WIRE_SCHEMA_VERSION, RECONCILE_STATUS_ENROLLED,
    RECONCILE_STATUS_NEW, RECONCILE_STATUS_REPAIR, SASE_GATEWAY_HEALTH_SERVICE,
    TAILNET_PROVIDER_REF,
};

/// Structural request failure for the machine-setup wire envelope.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum MachineSetupError {
    #[error(
        "unsupported machine setup schema_version {actual}; expected {expected}"
    )]
    UnsupportedSchema { actual: u32, expected: u32 },
    #[error("{0}")]
    Validation(String),
}

impl MachineSetupError {
    fn check_schema(actual: u32) -> Result<(), Self> {
        if actual == MACHINE_SETUP_WIRE_SCHEMA_VERSION {
            Ok(())
        } else {
            Err(Self::UnsupportedSchema {
                actual,
                expected: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
            })
        }
    }
}
