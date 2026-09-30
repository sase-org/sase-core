//! Attachment wire tests: payload round trips, byte-identical legacy
//! encoding, forward compatibility, validation, and reducer semantics.

use std::collections::BTreeMap;

use serde_json::{json, Value};

use crate::bead::events::{
    BeadEventOperationWire, BeadEventPayloadWire, BEAD_EVENT_SCHEMA_VERSION,
};
use crate::bead::wire::BeadNoteWire;
use crate::note_attachment::{AttachmentImageDimsWire, BeadNoteAttachmentWire};

use super::super::apply_event;
use super::support::issue_with_refs;

const DIGEST_A: &str =
    "9f2c1e0b77aa4c10d5e6f3a2b1c9d8e7f6a5b4c3d2e1f0a9b8c7d6e5f4a3b2c1";
const DIGEST_B: &str =
    "41aa07c3e9b1d2f0a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8091a2b3c4d5e";

fn login_attachment() -> BeadNoteAttachmentWire {
    BeadNoteAttachmentWire {
        name: "login.png".to_string(),
        sha256: DIGEST_A.to_string(),
        size_bytes: 188416,
        mime_type: "image/png".to_string(),
        image: Some(AttachmentImageDimsWire {
            width: 1280,
            height: 720,
        }),
        origin: Some("athena".to_string()),
        visibility: None,
    }
}

fn log_attachment() -> BeadNoteAttachmentWire {
    BeadNoteAttachmentWire {
        name: "crash.log".to_string(),
        sha256: DIGEST_B.to_string(),
        size_bytes: 2202009,
        mime_type: "text/plain".to_string(),
        image: None,
        origin: None,
        visibility: None,
    }
}

fn appended(entry: &str, attachments: Vec<BeadNoteAttachmentWire>) -> Value {
    serde_json::to_value(BeadEventPayloadWire::NoteAppended {
        entry: entry.to_string(),
        attachments,
    })
    .unwrap()
}

#[test]
fn note_payloads_round_trip_with_attachments() {
    let payload = BeadEventPayloadWire::NoteAppended {
        entry: "Crash @attachment:login.png".to_string(),
        attachments: vec![login_attachment()],
    };
    let json = serde_json::to_value(&payload).unwrap();
    assert_eq!(
        json,
        json!({
            "kind": "note_appended",
            "entry": "Crash @attachment:login.png",
            "attachments": [{
                "name": "login.png",
                "sha256": DIGEST_A,
                "size_bytes": 188416,
                "mime_type": "image/png",
                "image": {"width": 1280, "height": 720},
                "origin": "athena",
            }],
        })
    );
    let decoded: BeadEventPayloadWire = serde_json::from_value(json).unwrap();
    assert_eq!(decoded, payload);

    let edit = BeadEventPayloadWire::NoteEdited {
        note_id: "sase-1:1".to_string(),
        text: "Crash @attachment:login.png".to_string(),
        attachments: Some(vec![login_attachment()]),
    };
    let json = serde_json::to_value(&edit).unwrap();
    assert!(json.get("attachments").is_some());
    let decoded: BeadEventPayloadWire = serde_json::from_value(json).unwrap();
    assert_eq!(decoded, edit);
}

#[test]
fn note_payloads_without_attachments_stay_byte_identical() {
    let json = appended("plain note", Vec::new());
    assert_eq!(
        json,
        json!({"kind": "note_appended", "entry": "plain note"})
    );
    let edit = serde_json::to_value(BeadEventPayloadWire::NoteEdited {
        note_id: "sase-1:1".to_string(),
        text: "plain note".to_string(),
        attachments: None,
    })
    .unwrap();
    assert_eq!(
        edit,
        json!({
            "kind": "note_edited",
            "note_id": "sase-1:1",
            "text": "plain note",
        })
    );
    let note = BeadNoteWire {
        id: "sase-1:1".to_string(),
        timestamp: "2026-01-01T00:01:00Z".to_string(),
        author: "agent-1".to_string(),
        text: "plain note".to_string(),
        edited_at: None,
        edited_by: None,
        attachments: Vec::new(),
    };
    let json = serde_json::to_value(&note).unwrap();
    assert!(json.get("attachments").is_none());
    note.validate().unwrap();
}

#[test]
fn note_payloads_ignore_unknown_future_fields() {
    // An older reader must accept a note payload carrying a field it does
    // not know yet; only new operations are hard errors.
    let json = json!({
        "kind": "note_appended",
        "entry": "plain note",
        "future_field": {"nested": [1, 2, 3]},
    });
    let decoded: BeadEventPayloadWire = serde_json::from_value(json).unwrap();
    assert_eq!(
        decoded,
        BeadEventPayloadWire::NoteAppended {
            entry: "plain note".to_string(),
            attachments: Vec::new(),
        }
    );
    let edit = json!({
        "kind": "note_edited",
        "note_id": "sase-1:1",
        "text": "plain note",
        "future_field": 1,
    });
    let decoded: BeadEventPayloadWire = serde_json::from_value(edit).unwrap();
    assert_eq!(
        decoded,
        BeadEventPayloadWire::NoteEdited {
            note_id: "sase-1:1".to_string(),
            text: "plain note".to_string(),
            attachments: None,
        }
    );
}

#[test]
fn event_schema_version_stays_one() {
    assert_eq!(BEAD_EVENT_SCHEMA_VERSION, 1);
}

#[test]
fn note_payload_validation_rejects_mismatches() {
    // Missing token for a descriptor.
    let payload = BeadEventPayloadWire::NoteAppended {
        entry: "no tokens here".to_string(),
        attachments: vec![login_attachment()],
    };
    let error =
        record_error(BeadEventOperationWire::NoteAppended, &payload, "sase-1");
    assert!(error.contains("must match one-to-one"), "{error}");

    // Token without a descriptor.
    let payload = BeadEventPayloadWire::NoteAppended {
        entry: "see @attachment:login.png".to_string(),
        attachments: Vec::new(),
    };
    // An empty manifest never constrains the text: legacy notes render
    // unchanged, and tokens without descriptors are a renderer concern.
    assert!(record_error(
        BeadEventOperationWire::NoteAppended,
        &payload,
        "sase-1"
    )
    .is_empty());

    // Bad digest.
    let mut bad = login_attachment();
    bad.sha256 = "xyz".to_string();
    let payload = BeadEventPayloadWire::NoteAppended {
        entry: "see @attachment:login.png".to_string(),
        attachments: vec![bad],
    };
    let error =
        record_error(BeadEventOperationWire::NoteAppended, &payload, "sase-1");
    assert!(error.contains("SHA-256"), "{error}");

    // Duplicate names.
    let payload = BeadEventPayloadWire::NoteAppended {
        entry: "see @attachment:login.png".to_string(),
        attachments: vec![login_attachment(), login_attachment()],
    };
    let error =
        record_error(BeadEventOperationWire::NoteAppended, &payload, "sase-1");
    assert!(error.contains("duplicate name"), "{error}");
}

#[test]
fn note_payload_validation_accepts_repeated_token() {
    let payload = BeadEventPayloadWire::NoteAppended {
        entry: "shot @attachment:login.png again @attachment:login.png"
            .to_string(),
        attachments: vec![login_attachment()],
    };
    assert!(record_error(
        BeadEventOperationWire::NoteAppended,
        &payload,
        "sase-1"
    )
    .is_empty());
}

fn record_error(
    operation: BeadEventOperationWire,
    payload: &BeadEventPayloadWire,
    issue_id: &str,
) -> String {
    let record = crate::bead::events::BeadEventRecordWire {
        schema_version: BEAD_EVENT_SCHEMA_VERSION,
        event_id: "sase-1:9".to_string(),
        timestamp: "2026-01-01T00:09:00Z".to_string(),
        actor: "agent-1".to_string(),
        operation,
        issue_id: issue_id.to_string(),
        payload: payload.clone(),
    };
    record
        .validate()
        .map(|()| String::new())
        .unwrap_or_else(|error| {
            assert_eq!(error.kind, "validation");
            error.message
        })
}

fn apply_note_payload(
    issues: &mut BTreeMap<String, crate::bead::wire::IssueWire>,
    event_id: &str,
    payload: BeadEventPayloadWire,
    operation: BeadEventOperationWire,
) {
    let event = crate::bead::events::BeadEventRecordWire {
        schema_version: BEAD_EVENT_SCHEMA_VERSION,
        event_id: event_id.to_string(),
        timestamp: "2026-01-01T00:02:00Z".to_string(),
        actor: "agent-2".to_string(),
        operation,
        issue_id: "sase-1".to_string(),
        payload,
    };
    apply_event(issues, &event).unwrap();
}

#[test]
fn reducer_sets_replaces_and_keeps_manifests() {
    let mut issues =
        BTreeMap::from([("sase-1".to_string(), issue_with_refs(Vec::new()))]);
    apply_note_payload(
        &mut issues,
        "note",
        BeadEventPayloadWire::NoteAppended {
            entry: "Crash @attachment:login.png and @attachment:crash.log"
                .to_string(),
            attachments: vec![login_attachment(), log_attachment()],
        },
        BeadEventOperationWire::NoteAppended,
    );
    assert_eq!(issues["sase-1"].notes.len(), 1);
    assert_eq!(issues["sase-1"].notes[0].attachments.len(), 2);

    // `None` keeps the manifest while the text is rewritten verbatim.
    apply_note_payload(
        &mut issues,
        "edit-keep",
        BeadEventPayloadWire::NoteEdited {
            note_id: "note".to_string(),
            text:
                "Crash @attachment:login.png and @attachment:crash.log (edited)"
                    .to_string(),
            attachments: None,
        },
        BeadEventOperationWire::NoteEdited,
    );
    assert_eq!(issues["sase-1"].notes[0].attachments.len(), 2);

    // `Some` replaces, including `Some([])` which detaches everything.
    apply_note_payload(
        &mut issues,
        "edit-replace",
        BeadEventPayloadWire::NoteEdited {
            note_id: "note".to_string(),
            text: "Crash @attachment:login.png (trimmed)".to_string(),
            attachments: Some(vec![login_attachment()]),
        },
        BeadEventOperationWire::NoteEdited,
    );
    assert_eq!(
        issues["sase-1"].notes[0].attachments,
        vec![login_attachment()]
    );
    apply_note_payload(
        &mut issues,
        "edit-detach",
        BeadEventPayloadWire::NoteEdited {
            note_id: "note".to_string(),
            text: "Crash, attachments purged".to_string(),
            attachments: Some(Vec::new()),
        },
        BeadEventOperationWire::NoteEdited,
    );
    assert!(issues["sase-1"].notes[0].attachments.is_empty());
}
