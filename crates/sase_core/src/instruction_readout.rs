//! Pure classification policy for instruction-delivery readout.
//!
//! The host adapter gathers declaration, handoff, and workspace facts from
//! current staged records or historical legacy artifacts. This module owns
//! the scoring rule shared by both evidence sources.

use serde::{Deserialize, Serialize};

/// Declaration evidence available for one completed root run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeclarationEvidenceWire {
    Accepted,
    NotRequired,
    Missing,
}

/// Handoff class derived from staged host records or legacy tool-call evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HandoffClassWire {
    Plan,
    MonitorPrepared,
    MonitorPlain,
    Other,
}

/// Inputs gathered by the host-side instruction readout adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstructionReadoutRequestWire {
    pub handoff: Option<HandoffClassWire>,
    pub declaration: DeclarationEvidenceWire,
    pub has_dirty_obligations: bool,
}

/// Final readout class for one root run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstructionReadoutClassWire {
    Accepted,
    PlanHandoff,
    MonitorPrepared,
    MonitorPlain,
    OtherHandoff,
    NotRequired,
    Miss,
}

/// Classify declaration evidence consistently for staged and legacy handoffs.
///
/// An accepted or explicitly not-required declaration preserves the handoff
/// class. A handoff with missing declaration evidence is a miss when it has
/// dirty obligations; clean handoffs remain handoffs because the host can
/// complete them without a model-submitted declaration.
pub fn classify_instruction_readout(
    request: InstructionReadoutRequestWire,
) -> InstructionReadoutClassWire {
    let Some(handoff) = request.handoff else {
        return match request.declaration {
            DeclarationEvidenceWire::Accepted => {
                InstructionReadoutClassWire::Accepted
            }
            DeclarationEvidenceWire::NotRequired => {
                InstructionReadoutClassWire::NotRequired
            }
            DeclarationEvidenceWire::Missing => {
                InstructionReadoutClassWire::Miss
            }
        };
    };

    if request.declaration == DeclarationEvidenceWire::Missing
        && request.has_dirty_obligations
    {
        return InstructionReadoutClassWire::Miss;
    }

    match handoff {
        HandoffClassWire::Plan => InstructionReadoutClassWire::PlanHandoff,
        HandoffClassWire::MonitorPrepared => {
            InstructionReadoutClassWire::MonitorPrepared
        }
        HandoffClassWire::MonitorPlain => {
            InstructionReadoutClassWire::MonitorPlain
        }
        HandoffClassWire::Other => InstructionReadoutClassWire::OtherHandoff,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(
        handoff: Option<HandoffClassWire>,
        declaration: DeclarationEvidenceWire,
        has_dirty_obligations: bool,
    ) -> InstructionReadoutRequestWire {
        InstructionReadoutRequestWire {
            handoff,
            declaration,
            has_dirty_obligations,
        }
    }

    #[test]
    fn classifies_runs_without_handoffs_by_declaration() {
        assert_eq!(
            classify_instruction_readout(request(
                None,
                DeclarationEvidenceWire::Accepted,
                false,
            )),
            InstructionReadoutClassWire::Accepted,
        );
        assert_eq!(
            classify_instruction_readout(request(
                None,
                DeclarationEvidenceWire::NotRequired,
                false,
            )),
            InstructionReadoutClassWire::NotRequired,
        );
        assert_eq!(
            classify_instruction_readout(request(
                None,
                DeclarationEvidenceWire::Missing,
                false,
            )),
            InstructionReadoutClassWire::Miss,
        );
    }

    #[test]
    fn declaration_evidence_preserves_staged_and_legacy_handoff_classes() {
        for handoff in [
            HandoffClassWire::Plan,
            HandoffClassWire::MonitorPrepared,
            HandoffClassWire::MonitorPlain,
            HandoffClassWire::Other,
        ] {
            for declaration in [
                DeclarationEvidenceWire::Accepted,
                DeclarationEvidenceWire::NotRequired,
            ] {
                assert_ne!(
                    classify_instruction_readout(request(
                        Some(handoff),
                        declaration,
                        true,
                    )),
                    InstructionReadoutClassWire::Miss,
                );
            }
        }
    }

    #[test]
    fn dirty_handoff_without_declaration_is_a_miss() {
        for handoff in [
            HandoffClassWire::Plan,
            HandoffClassWire::MonitorPrepared,
            HandoffClassWire::MonitorPlain,
            HandoffClassWire::Other,
        ] {
            assert_eq!(
                classify_instruction_readout(request(
                    Some(handoff),
                    DeclarationEvidenceWire::Missing,
                    true,
                )),
                InstructionReadoutClassWire::Miss,
            );
        }
    }

    #[test]
    fn clean_handoff_without_declaration_keeps_its_class() {
        assert_eq!(
            classify_instruction_readout(request(
                Some(HandoffClassWire::Plan),
                DeclarationEvidenceWire::Missing,
                false,
            )),
            InstructionReadoutClassWire::PlanHandoff,
        );
    }
}
