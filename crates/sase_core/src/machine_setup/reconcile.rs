//! Enrollment reconciliation: classify discovery candidates against pins.

use std::collections::HashMap;

use super::wire::{
    MachineReconcileRequestWire, MachineReconcileResultWire,
    ReconciledCandidateWire, MACHINE_SETUP_WIRE_SCHEMA_VERSION,
    RECONCILE_STATUS_ENROLLED, RECONCILE_STATUS_NEW, RECONCILE_STATUS_REPAIR,
};
use super::MachineSetupError;

/// Classify candidates without mutating enrolled pins.
///
/// Pin matches skip as already enrolled. An endpoint match whose discovery
/// pin differs from the enrolled pin is routed to deliberate repair. An
/// untrusted discovery hint never overwrites enrollment identity.
pub fn reconcile_machine_enrollments(
    request: &MachineReconcileRequestWire,
) -> Result<MachineReconcileResultWire, MachineSetupError> {
    MachineSetupError::check_schema(request.schema_version)?;

    let mut by_pin = HashMap::new();
    let mut by_endpoint = HashMap::new();
    for record in &request.enrolled {
        if !record.pinned_installation_id.is_empty() {
            by_pin.insert(record.pinned_installation_id.clone(), record);
        }
        by_endpoint.insert(
            (record.provider_ref.clone(), record.endpoint.clone()),
            record,
        );
    }

    let mut items = Vec::new();
    for candidate in &request.candidates {
        let pin_match = if candidate.installation_pin.is_empty() {
            None
        } else {
            by_pin.get(&candidate.installation_pin).copied()
        };
        let endpoint_match = by_endpoint
            .get(&(candidate.provider_ref.clone(), candidate.endpoint.clone()))
            .copied();

        if let Some(record) = pin_match {
            items.push(ReconciledCandidateWire {
                candidate: candidate.clone(),
                status: RECONCILE_STATUS_ENROLLED.to_string(),
                alias: record.alias.clone(),
                reason: "already enrolled".to_string(),
            });
        } else if let Some(record) = endpoint_match {
            if !candidate.installation_pin.is_empty()
                && candidate.installation_pin != record.pinned_installation_id
            {
                items.push(ReconciledCandidateWire {
                    candidate: candidate.clone(),
                    status: RECONCILE_STATUS_REPAIR.to_string(),
                    alias: record.alias.clone(),
                    reason: format!(
                        "installation identity changed; run `sase machine repair {}`",
                        record.alias
                    ),
                });
            } else {
                items.push(ReconciledCandidateWire {
                    candidate: candidate.clone(),
                    status: RECONCILE_STATUS_ENROLLED.to_string(),
                    alias: record.alias.clone(),
                    reason: "already enrolled".to_string(),
                });
            }
        } else {
            items.push(ReconciledCandidateWire {
                candidate: candidate.clone(),
                status: RECONCILE_STATUS_NEW.to_string(),
                alias: String::new(),
                reason: String::new(),
            });
        }
    }

    Ok(MachineReconcileResultWire {
        schema_version: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
        items,
    })
}
