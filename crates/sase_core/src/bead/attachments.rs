//! Attachment roster and reference queries over the bead store.
//!
//! The roster answers "what is attached to this bead right now" (current
//! notes only, latest note wins per name). The references query answers
//! "every note that ever referenced this digest" (current and historical),
//! which feeds pinning, purge preview, and doctor.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use super::events::{
    merge_stream_events, reduce_event_streams, validated_event_streams,
    BeadEventPayloadWire,
};
use super::jsonl::read_event_store;
use super::read::{read_store_issues, resolve_issue_id_in_issues};
use super::wire::BeadError;
use crate::note_attachment::{
    BeadAttachmentReferenceWire, BeadAttachmentRosterEntryWire,
};

/// Current attachment roster for one bead: current notes only, the latest
/// note wins per name, and each entry names its note id and ordinal.
pub fn bead_attachment_roster(
    beads_dir: &Path,
    issue_id: &str,
) -> Result<Vec<BeadAttachmentRosterEntryWire>, BeadError> {
    let issues = read_store_issues(beads_dir)?;
    let resolved = resolve_issue_id_in_issues(&issues, issue_id)?;
    let issue = issues
        .iter()
        .find(|issue| issue.id == resolved)
        .ok_or_else(|| {
            BeadError::validation(format!("issue not found: {issue_id}"))
        })?;
    let mut by_name: BTreeMap<&str, BeadAttachmentRosterEntryWire> =
        BTreeMap::new();
    for (index, note) in issue.notes.iter().enumerate() {
        let ordinal = index as u64 + 1;
        for attachment in &note.attachments {
            by_name.insert(
                attachment.name.as_str(),
                BeadAttachmentRosterEntryWire {
                    name: attachment.name.clone(),
                    sha256: attachment.sha256.clone(),
                    size_bytes: attachment.size_bytes,
                    mime_type: attachment.mime_type.clone(),
                    note_id: note.id.clone(),
                    ordinal,
                },
            );
        }
    }
    Ok(by_name.into_values().collect())
}

/// Every current and historical `(issue, note, name, sha256)` attachment
/// reference in the project, read from the event store.
///
/// `current` is true exactly when the reference survives in the reduced
/// (current) projection. Objects stay pinned while any row — current or
/// historical — references them.
pub fn bead_attachment_references(
    beads_dir: &Path,
) -> Result<Vec<BeadAttachmentReferenceWire>, BeadError> {
    let (_manifest, streams) = read_event_store(beads_dir)?;
    let streams = validated_event_streams(&streams)?;
    let mut seen: BTreeSet<(String, String, String, String)> = BTreeSet::new();
    for event in merge_stream_events(&streams) {
        match &event.payload {
            BeadEventPayloadWire::NoteAppended { attachments, .. } => {
                for attachment in attachments {
                    seen.insert((
                        event.issue_id.clone(),
                        event.event_id.clone(),
                        attachment.name.clone(),
                        attachment.sha256.clone(),
                    ));
                }
            }
            BeadEventPayloadWire::NoteEdited {
                note_id,
                attachments: Some(manifest),
                ..
            } => {
                for attachment in manifest {
                    seen.insert((
                        event.issue_id.clone(),
                        note_id.clone(),
                        attachment.name.clone(),
                        attachment.sha256.clone(),
                    ));
                }
            }
            _ => {}
        }
    }
    let mut current: BTreeSet<(String, String, String, String)> =
        BTreeSet::new();
    for issue in reduce_event_streams(&streams)? {
        for note in &issue.notes {
            for attachment in &note.attachments {
                current.insert((
                    issue.id.clone(),
                    note.id.clone(),
                    attachment.name.clone(),
                    attachment.sha256.clone(),
                ));
            }
        }
    }
    Ok(seen
        .into_iter()
        .map(|(issue, note, name, sha256)| {
            let is_current = current.contains(&(
                issue.clone(),
                note.clone(),
                name.clone(),
                sha256.clone(),
            ));
            BeadAttachmentReferenceWire {
                issue,
                note,
                name,
                sha256,
                current: is_current,
            }
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::super::mutation::{
        append_issue_note, create_issue, edit_issue_note, init_store,
        BeadCreateRequestWire,
    };
    use super::super::read::show_issue;
    use super::super::wire::{IssueTypeWire, PhaseSizeWire};
    use super::*;
    use crate::note_attachment::BeadNoteAttachmentWire;

    const DIGEST_A: &str =
        "9f2c1e0b77aa4c10d5e6f3a2b1c9d8e7f6a5b4c3d2e1f0a9b8c7d6e5f4a3b2c1";
    const DIGEST_B: &str =
        "41aa07c3e9b1d2f0a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8091a2b3c4d5e";

    fn blob(name: &str, sha256: &str) -> BeadNoteAttachmentWire {
        BeadNoteAttachmentWire {
            name: name.to_string(),
            sha256: sha256.to_string(),
            size_bytes: 1024,
            mime_type: "image/png".to_string(),
            image: None,
            origin: None,
        }
    }

    fn task_with_notes() -> (tempfile::TempDir, std::path::PathBuf, String) {
        let temp = tempfile::tempdir().unwrap();
        init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
        let beads_dir = temp.path().join("beads");
        let issue = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Attachment queries".to_string(),
                issue_type: IssueTypeWire::Task,
                size: Some(PhaseSizeWire::Small),
                task_type: Some("bug".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        // First note attaches login.png (digest A).
        let issue = append_issue_note(
            &beads_dir,
            &issue.id,
            "Crash @attachment:login.png",
            None,
            Some("2026-01-01T00:01:00Z".to_string()),
            Some(vec![blob("login.png", DIGEST_A)]),
        )
        .unwrap()
        .issue
        .unwrap();
        // Second note reuses the name with new bytes (digest B): the
        // latest note wins per name on the roster.
        append_issue_note(
            &beads_dir,
            &issue.id,
            "Retake @attachment:login.png",
            None,
            Some("2026-01-01T00:02:00Z".to_string()),
            Some(vec![blob("login.png", DIGEST_B)]),
        )
        .unwrap();
        (temp, beads_dir, issue.id)
    }

    #[test]
    fn roster_reports_latest_note_per_name() {
        let (_temp, beads_dir, issue_id) = task_with_notes();
        let roster = bead_attachment_roster(&beads_dir, &issue_id).unwrap();
        assert_eq!(roster.len(), 1);
        assert_eq!(roster[0].name, "login.png");
        assert_eq!(roster[0].sha256, DIGEST_B);
        assert_eq!(roster[0].ordinal, 2);
        assert!(!roster[0].note_id.is_empty());
    }

    #[test]
    fn references_cover_current_and_historical_rows() {
        let (_temp, beads_dir, issue_id) = task_with_notes();
        // Edit the first note to detach digest A: it stays referenced as
        // history while digest B stays current.
        let issue = show_issue(&beads_dir, &issue_id).unwrap();
        let first_note = issue.notes[0].id.clone();
        edit_issue_note(
            &beads_dir,
            &issue_id,
            &first_note,
            "Crash, screenshot removed",
            None,
            Some("2026-01-01T00:03:00Z".to_string()),
            Some(Vec::new()),
        )
        .unwrap();
        let references = bead_attachment_references(&beads_dir).unwrap();
        assert_eq!(references.len(), 2);
        let historical = references
            .iter()
            .find(|row| row.sha256 == DIGEST_A)
            .unwrap();
        assert!(!historical.current);
        let current = references
            .iter()
            .find(|row| row.sha256 == DIGEST_B)
            .unwrap();
        assert!(current.current);
        assert_eq!(current.issue, issue_id);
    }
}
