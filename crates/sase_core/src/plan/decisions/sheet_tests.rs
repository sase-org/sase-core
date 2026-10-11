//! Sheet tests that would push `sheet.rs` past the 1,500-line cap.

use super::resolver::PlanDecisionDefinitionWire;
use super::resolver::PlanDecisionMemoryRecordWire;
use super::sheet::{plan_decision_sheet, plan_decisions_prompt_block};
use super::wire::PlanDecisionMemoryWire;
use serde_json::json;

fn memory_record() -> PlanDecisionMemoryRecordWire {
    PlanDecisionMemoryRecordWire {
        selector: "tui.md".to_string(),
        kind: "note".to_string(),
        scope: "project".to_string(),
        path: "sase/memory/tui.md".to_string(),
        record_type: "reference".to_string(),
        exists: true,
        strands: None,
    }
}

fn correction_definition() -> PlanDecisionDefinitionWire {
    PlanDecisionDefinitionWire {
        id: "tui_note".to_string(),
        kind: "toggle".to_string(),
        ask: "Record the overlay conventions in the tui note?".to_string(),
        why: None,
        choices: Vec::new(),
        default: json!(true),
        effective_default: json!(true),
        memory: Some(PlanDecisionMemoryWire {
            selectors: vec!["tui.md".to_string()],
        }),
        requested: None,
        corrects: Some("sase plan approve writes the durable plan".to_string()),
        provenance: Some("correction".to_string()),
        requested_verified: true,
        resolved: vec![memory_record()],
    }
}

#[test]
fn sheet_quote_falls_back_to_corrects() {
    let sheet = plan_decision_sheet(
        &[correction_definition()],
        &json!({"tui_note": true}),
        1,
    )
    .unwrap();
    let memory = sheet.rows[0].memory.as_ref().unwrap();
    assert_eq!(
        memory.quote,
        Some("sase plan approve writes the durable plan".to_string()),
    );
    assert_eq!(memory.provenance, "correction");
}

#[test]
fn sheet_quote_prefers_requested_over_corrects() {
    let mut definition = correction_definition();
    definition.requested = Some("please update the tui note".to_string());
    definition.provenance = Some("asked".to_string());
    let sheet =
        plan_decision_sheet(&[definition], &json!({"tui_note": true}), 1)
            .unwrap();
    let memory = sheet.rows[0].memory.as_ref().unwrap();
    assert_eq!(memory.quote, Some("please update the tui note".to_string()),);
}

#[test]
fn correction_on_auto_stays_quiet_when_left_off() {
    let mut definition = correction_definition();
    definition.effective_default = json!(false);
    let sheet =
        plan_decision_sheet(&[definition], &json!({"tui_note": false}), 1)
            .unwrap();
    let block =
        plan_decisions_prompt_block(&sheet, "auto", None, "tale_coder", None)
            .unwrap();
    assert!(!block.contains("/sase_new_task"));
    assert!(!block.contains("PROPOSED FOLLOW-UP:"));
}
