//! Visibility wire tests: old shapes, unknown values, explicit public.

use crate::note_attachment::{
    AttachmentVisibilityWire, BeadNoteAttachmentWire,
};

fn descriptor(name: &str, sha: &str) -> BeadNoteAttachmentWire {
    BeadNoteAttachmentWire {
        name: name.to_string(),
        sha256: sha.to_string(),
        size_bytes: 10,
        mime_type: "text/plain".to_string(),
        image: None,
        origin: None,
        visibility: None,
    }
}

const DIGEST: &str =
    "9f2c1e0b77aa4c10d5e6f3a2b1c9d8e7f6a5b4c3d2e1f0a9b8c7d6e5f4a3b2c1";

#[test]
fn old_shaped_descriptors_reduce_to_private() {
    let old = serde_json::json!({
        "name": "crash.log",
        "sha256": DIGEST,
        "size_bytes": 10,
        "mime_type": "text/plain",
    });
    let decoded: BeadNoteAttachmentWire = serde_json::from_value(old).unwrap();
    assert_eq!(decoded.visibility, None);
    assert_eq!(
        decoded.effective_visibility(),
        AttachmentVisibilityWire::Private
    );
    // Round trip omits the field.
    let value = serde_json::to_value(&decoded).unwrap();
    assert!(value.get("visibility").is_none());
}

#[test]
fn unknown_visibility_falls_back_to_private() {
    let future = serde_json::json!({
        "name": "crash.log",
        "sha256": DIGEST,
        "size_bytes": 10,
        "mime_type": "text/plain",
        "visibility": "super_public",
    });
    let decoded: BeadNoteAttachmentWire =
        serde_json::from_value(future).unwrap();
    assert_eq!(
        decoded.effective_visibility(),
        AttachmentVisibilityWire::Private
    );
}

#[test]
fn explicit_public_survives() {
    let mut attachment = descriptor("crash.log", DIGEST);
    attachment.visibility = Some(AttachmentVisibilityWire::Public);
    assert_eq!(
        attachment.effective_visibility(),
        AttachmentVisibilityWire::Public
    );
    let value = serde_json::to_value(&attachment).unwrap();
    assert_eq!(value["visibility"], serde_json::json!("public"));
    let decoded: BeadNoteAttachmentWire =
        serde_json::from_value(value).unwrap();
    assert_eq!(decoded, attachment);
}

#[test]
fn visibility_wire_is_snake_case() {
    let value = serde_json::to_value(AttachmentVisibilityWire::Public).unwrap();
    assert_eq!(value, serde_json::json!("public"));
    let decoded: AttachmentVisibilityWire =
        serde_json::from_value(serde_json::json!("private")).unwrap();
    assert_eq!(decoded, AttachmentVisibilityWire::Private);
}
