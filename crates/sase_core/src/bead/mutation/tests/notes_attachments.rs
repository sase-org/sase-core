use super::super::*;
use super::support::dual_tempdir as tempdir;
use super::support::*;
use crate::bead::events::BeadEventOperationWire;
use crate::bead::events::BeadEventPayloadWire;
use crate::bead::jsonl::read_event_store;
use crate::bead::wire::IssueTypeWire;
use std::fs;

use crate::bead::config::default_config;
use crate::bead::config::save_config;
use crate::bead::events::reduce_event_streams;
use crate::note_attachment::BeadNoteAttachmentWire;

const ATTACHMENT_DIGEST_A: &str =
    "9f2c1e0b77aa4c10d5e6f3a2b1c9d8e7f6a5b4c3d2e1f0a9b8c7d6e5f4a3b2c1";
const ATTACHMENT_DIGEST_B: &str =
    "41aa07c3e9b1d2f0a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8091a2b3c4d5e";

fn attachment_blob(name: &str, sha256: &str) -> BeadNoteAttachmentWire {
    BeadNoteAttachmentWire {
        name: name.to_string(),
        sha256: sha256.to_string(),
        size_bytes: 188416,
        mime_type: "image/png".to_string(),
        image: None,
        origin: Some("athena".to_string()),
        visibility: None,
    }
}

fn note_store() -> (tempfile::TempDir, std::path::PathBuf, String) {
    let temp = tempdir();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "owner@example.com"))
        .unwrap();
    fs::write(beads_dir.join("issues.jsonl"), "").unwrap();
    let issue = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Notes".to_string(),
            issue_type: IssueTypeWire::Plan,
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    (temp, beads_dir, issue.id)
}

#[test]
fn append_issue_note_records_attachments_and_replays() {
    run_dual_mode_test(|_mode| {
        let (_temp, beads_dir, issue_id) = note_store();

        let noted = append_issue_note(
            &beads_dir,
            &issue_id,
            "Crash @attachment:login.png",
            Some("agent-1".to_string()),
            Some("2026-01-01T00:01:00Z".to_string()),
            Some(vec![attachment_blob("login.png", ATTACHMENT_DIGEST_A)]),
        )
        .unwrap()
        .issue
        .unwrap();

        assert_eq!(noted.notes.len(), 1);
        assert_eq!(
            noted.notes[0].attachments,
            vec![attachment_blob("login.png", ATTACHMENT_DIGEST_A)]
        );
        let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
        let payload = streams
            .iter()
            .flat_map(|stream| &stream.events)
            .find(|event| {
                event.issue_id == issue_id
                    && event.operation == BeadEventOperationWire::NoteAppended
            })
            .map(|event| event.payload.clone())
            .unwrap();
        assert!(
            matches!(&payload, BeadEventPayloadWire::NoteAppended { entry, attachments }
            if entry == "Crash @attachment:login.png" && attachments.len() == 1)
        );
        let reduced = reduce_event_streams(&streams).unwrap();
        let reduced_issue =
            reduced.iter().find(|issue| issue.id == issue_id).unwrap();
        assert_eq!(reduced_issue.notes, noted.notes);
        assert_dual_mode_parity_for_current_mode();
    });
}

#[test]
fn append_issue_note_rejects_orphan_descriptor_without_writing() {
    run_dual_mode_test(|_mode| {
        let (_temp, beads_dir, issue_id) = note_store();
        let before = persisted_claim_state(&beads_dir);

        let error = append_issue_note(
            &beads_dir,
            &issue_id,
            "Crash with no token",
            None,
            Some("2026-01-01T00:01:00Z".to_string()),
            Some(vec![attachment_blob("login.png", ATTACHMENT_DIGEST_A)]),
        )
        .unwrap_err();

        assert_eq!(error.kind, "validation");
        assert!(
            error.message.contains("must match one-to-one"),
            "{}",
            error.message
        );
        assert_eq!(persisted_claim_state(&beads_dir), before);
        assert_dual_mode_parity_for_current_mode();
    });
}

#[test]
fn edit_issue_note_replaces_keeps_or_detaches_manifest() {
    run_dual_mode_test(|_mode| {
        let (_temp, beads_dir, issue_id) = note_store();
        let noted = append_issue_note(
            &beads_dir,
            &issue_id,
            "Crash @attachment:login.png",
            None,
            Some("2026-01-01T00:01:00Z".to_string()),
            Some(vec![attachment_blob("login.png", ATTACHMENT_DIGEST_A)]),
        )
        .unwrap()
        .issue
        .unwrap();
        let note_id = noted.notes[0].id.clone();

        // `None` keeps the manifest on a text-preserving edit.
        let kept = edit_issue_note(
            &beads_dir,
            &issue_id,
            &note_id,
            "Crash @attachment:login.png!",
            None,
            Some("2026-01-01T00:02:00Z".to_string()),
            None,
        )
        .unwrap()
        .issue
        .unwrap();
        assert_eq!(
            kept.notes[0].attachments,
            vec![attachment_blob("login.png", ATTACHMENT_DIGEST_A)]
        );

        // `Some` replaces it.
        let replaced = edit_issue_note(
            &beads_dir,
            &issue_id,
            &note_id,
            "Log @attachment:crash.log",
            None,
            Some("2026-01-01T00:03:00Z".to_string()),
            Some(vec![attachment_blob("crash.log", ATTACHMENT_DIGEST_B)]),
        )
        .unwrap()
        .issue
        .unwrap();
        assert_eq!(
            replaced.notes[0].attachments,
            vec![attachment_blob("crash.log", ATTACHMENT_DIGEST_B)]
        );

        // `Some([])` detaches everything.
        let detached = edit_issue_note(
            &beads_dir,
            &issue_id,
            &note_id,
            "Log, attachment purged",
            None,
            Some("2026-01-01T00:04:00Z".to_string()),
            Some(Vec::new()),
        )
        .unwrap()
        .issue
        .unwrap();
        assert!(detached.notes[0].attachments.is_empty());

        let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
        let reduced = reduce_event_streams(&streams).unwrap();
        let reduced_issue =
            reduced.iter().find(|issue| issue.id == issue_id).unwrap();
        assert_eq!(reduced_issue.notes, detached.notes);
        assert_dual_mode_parity_for_current_mode();
    });
}
