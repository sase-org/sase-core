//! Attachment roster and reference queries over the bead store.
//!
//! The roster answers "what is attached to this bead right now" (current
//! notes only, latest note wins per name). The references query answers
//! "every note that ever referenced this digest" (current and historical),
//! which feeds pinning, purge preview, and doctor.

use std::collections::BTreeMap;
use std::path::Path;

use super::events::{
    merge_stream_events, reduce_parsed_event_streams, BeadEventPayloadWire,
};
use super::jsonl::read_event_store;
use super::read::{read_store_issues, resolve_issue_id_in_issues};
use super::wire::BeadError;
use crate::note_attachment::{
    BeadAttachmentReferenceWire, BeadAttachmentRosterEntryWire,
    BeadAttachmentSourceWire,
};

/// Stable roster/reference identifier for `TaskPlusOneRecorded` evidence.
///
/// Evidence has no note id, so rows use `plus-one:<reporter>` (the
/// reporter is unique per bead: a second +1 from the same reporter is
/// ignored). Roster rows for evidence use ordinal `0`; note rows use
/// 1-based ordinals. This mapping is stable across reads and never
/// invents a mutable note.
pub fn plus_one_note_key(reporter: &str) -> String {
    format!("plus-one:{reporter}")
}

/// Current attachment roster for one bead: current notes plus current
/// `+1` evidence, the latest entry wins per name, and each entry names
/// its note id and ordinal.
///
/// Notes are processed in order (1-based ordinals), then `+1` evidence in
/// stored order (ordinal `0`), so a `+1` attachment shadows a same-named
/// note attachment deterministically.
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
                    visibility: attachment.effective_visibility(),
                    source: BeadAttachmentSourceWire::Note,
                },
            );
        }
    }
    for evidence in &issue.plus_one_evidence {
        let note_key = plus_one_note_key(&evidence.reporter);
        for attachment in &evidence.attachments {
            by_name.insert(
                attachment.name.as_str(),
                BeadAttachmentRosterEntryWire {
                    name: attachment.name.clone(),
                    sha256: attachment.sha256.clone(),
                    size_bytes: attachment.size_bytes,
                    mime_type: attachment.mime_type.clone(),
                    note_id: note_key.clone(),
                    ordinal: 0,
                    visibility: attachment.effective_visibility(),
                    source: BeadAttachmentSourceWire::PlusOne,
                },
            );
        }
    }
    Ok(by_name.into_values().collect())
}

/// Every current and historical `(issue, note, name, sha256)` attachment
/// reference in the project, read from the event store.
///
/// Covers initial `IssueCreated` notes, appended/edited notes, and
/// `TaskPlusOneRecorded` evidence. `+1` rows use
/// [`plus_one_note_key`] as the stable `note` identifier with
/// `source` `plus_one`. `current` is true exactly when the reference
/// survives in the reduced (current) projection. Objects stay pinned
/// while any row — current or historical — references them.
/// Rows are deterministic (sorted by key); historical-only rows report
/// the last-seen visibility, current rows report the live visibility.
pub fn bead_attachment_references(
    beads_dir: &Path,
) -> Result<Vec<BeadAttachmentReferenceWire>, BeadError> {
    use crate::note_attachment::AttachmentVisibilityWire;
    let (_manifest, streams) = read_event_store(beads_dir)?;
    // Streams arrive parse-validated and sorted from `read_event_store`.
    type Key = (String, String, String, String);
    type Meta = (AttachmentVisibilityWire, BeadAttachmentSourceWire);
    let mut seen: BTreeMap<Key, Meta> = BTreeMap::new();
    for event in merge_stream_events(&streams) {
        match &event.payload {
            BeadEventPayloadWire::IssueCreated { issue } => {
                for note in &issue.notes {
                    for attachment in &note.attachments {
                        seen.insert(
                            (
                                issue.id.clone(),
                                note.id.clone(),
                                attachment.name.clone(),
                                attachment.sha256.clone(),
                            ),
                            (
                                attachment.effective_visibility(),
                                BeadAttachmentSourceWire::Note,
                            ),
                        );
                    }
                }
            }
            BeadEventPayloadWire::NoteAppended { attachments, .. } => {
                for attachment in attachments {
                    seen.insert(
                        (
                            event.issue_id.clone(),
                            event.event_id.clone(),
                            attachment.name.clone(),
                            attachment.sha256.clone(),
                        ),
                        (
                            attachment.effective_visibility(),
                            BeadAttachmentSourceWire::Note,
                        ),
                    );
                }
            }
            BeadEventPayloadWire::NoteEdited {
                note_id,
                attachments: Some(manifest),
                ..
            } => {
                for attachment in manifest {
                    seen.insert(
                        (
                            event.issue_id.clone(),
                            note_id.clone(),
                            attachment.name.clone(),
                            attachment.sha256.clone(),
                        ),
                        (
                            attachment.effective_visibility(),
                            BeadAttachmentSourceWire::Note,
                        ),
                    );
                }
            }
            BeadEventPayloadWire::TaskPlusOneRecorded { evidence } => {
                let note_key = plus_one_note_key(&evidence.reporter);
                for attachment in &evidence.attachments {
                    seen.insert(
                        (
                            event.issue_id.clone(),
                            note_key.clone(),
                            attachment.name.clone(),
                            attachment.sha256.clone(),
                        ),
                        (
                            attachment.effective_visibility(),
                            BeadAttachmentSourceWire::PlusOne,
                        ),
                    );
                }
            }
            _ => {}
        }
    }
    let mut current: BTreeMap<Key, Meta> = BTreeMap::new();
    for issue in reduce_parsed_event_streams(&streams)? {
        for note in &issue.notes {
            for attachment in &note.attachments {
                current.insert(
                    (
                        issue.id.clone(),
                        note.id.clone(),
                        attachment.name.clone(),
                        attachment.sha256.clone(),
                    ),
                    (
                        attachment.effective_visibility(),
                        BeadAttachmentSourceWire::Note,
                    ),
                );
            }
        }
        for evidence in &issue.plus_one_evidence {
            let note_key = plus_one_note_key(&evidence.reporter);
            for attachment in &evidence.attachments {
                current.insert(
                    (
                        issue.id.clone(),
                        note_key.clone(),
                        attachment.name.clone(),
                        attachment.sha256.clone(),
                    ),
                    (
                        attachment.effective_visibility(),
                        BeadAttachmentSourceWire::PlusOne,
                    ),
                );
            }
        }
    }
    Ok(seen
        .into_iter()
        .map(|(key, (seen_visibility, seen_source))| {
            let (issue, note, name, sha256) = key;
            let live = current.get(&(
                issue.clone(),
                note.clone(),
                name.clone(),
                sha256.clone(),
            ));
            let is_current = live.is_some();
            let (visibility, source) =
                live.copied().unwrap_or((seen_visibility, seen_source));
            BeadAttachmentReferenceWire {
                issue,
                note,
                name,
                sha256,
                current: is_current,
                visibility,
                source,
            }
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::super::mutation::{
        add_task_plus_one, append_issue_note, create_issue, edit_issue_note,
        init_store, BeadCreateRequestWire,
    };
    use super::super::read::show_issue;
    use super::super::wire::{IssueTypeWire, PhaseSizeWire};
    use super::*;
    use crate::note_attachment::{
        AttachmentVisibilityWire, BeadAttachmentSourceWire,
        BeadNoteAttachmentWire,
    };

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
            visibility: None,
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

    fn public_blob(name: &str, sha256: &str) -> BeadNoteAttachmentWire {
        BeadNoteAttachmentWire {
            name: name.to_string(),
            sha256: sha256.to_string(),
            size_bytes: 512,
            mime_type: "text/plain".to_string(),
            image: None,
            origin: None,
            visibility: Some(AttachmentVisibilityWire::Public),
        }
    }

    #[test]
    fn roster_carries_visibility_and_source() {
        let (_temp, beads_dir, issue_id) = task_with_notes();
        let roster = bead_attachment_roster(&beads_dir, &issue_id).unwrap();
        assert_eq!(roster.len(), 1);
        assert_eq!(roster[0].visibility, AttachmentVisibilityWire::Private);
        assert_eq!(roster[0].source, BeadAttachmentSourceWire::Note);
    }

    #[test]
    fn roster_and_references_include_plus_one_evidence() {
        let (_temp, beads_dir, issue_id) = task_with_notes();
        add_task_plus_one(
            &beads_dir,
            &issue_id,
            "reporter@example.com",
            "Corroborated @attachment:plus.png",
            &[],
            Some("2026-01-01T00:04:00Z".to_string()),
            None,
            Some(vec![public_blob("plus.png", DIGEST_A)]),
        )
        .unwrap();
        let roster = bead_attachment_roster(&beads_dir, &issue_id).unwrap();
        let plus = roster
            .iter()
            .find(|row| row.name == "plus.png")
            .expect("plus-one attachment on roster");
        assert_eq!(plus.source, BeadAttachmentSourceWire::PlusOne);
        assert_eq!(plus.ordinal, 0);
        assert_eq!(plus.note_id, plus_one_note_key("reporter@example.com"));
        assert_eq!(plus.visibility, AttachmentVisibilityWire::Public);

        let references = bead_attachment_references(&beads_dir).unwrap();
        let plus_ref = references
            .iter()
            .find(|row| row.name == "plus.png")
            .expect("plus-one reference");
        assert!(plus_ref.current);
        assert_eq!(plus_ref.source, BeadAttachmentSourceWire::PlusOne);
        assert_eq!(plus_ref.note, plus_one_note_key("reporter@example.com"));
        assert_eq!(plus_ref.visibility, AttachmentVisibilityWire::Public);

        // Deterministic order by key.
        let mut keys: Vec<_> = references
            .iter()
            .map(|row| {
                (
                    row.issue.clone(),
                    row.note.clone(),
                    row.name.clone(),
                    row.sha256.clone(),
                )
            })
            .collect();
        let mut sorted = keys.clone();
        sorted.sort();
        sorted.dedup();
        keys.sort();
        assert_eq!(keys, sorted);
    }

    #[test]
    fn references_include_initial_issue_created_notes() {
        use super::super::events::{BeadEventPayloadWire, BeadEventRecordWire};
        use super::super::jsonl::event_streams_dir;
        use super::super::wire::BeadNoteWire;
        let (_temp, beads_dir, issue_id) = task_with_notes();
        // Streams persist as JSONL event records; inject an attachment
        // into the IssueCreated payload on disk.
        let streams_dir = event_streams_dir(&beads_dir);
        let mut stream_path = None;
        for entry in std::fs::read_dir(&streams_dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().and_then(|ext| ext.to_str()) == Some("jsonl") {
                stream_path = Some(path);
            }
        }
        let stream_path = stream_path.expect("event stream file");
        let raw = std::fs::read_to_string(&stream_path).unwrap();
        let mut injected = false;
        let mut out_lines = Vec::new();
        for line in raw.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let mut event: BeadEventRecordWire =
                serde_json::from_str(line).unwrap();
            if let BeadEventPayloadWire::IssueCreated { issue } =
                &mut event.payload
            {
                if issue.id == issue_id {
                    issue.notes.push(BeadNoteWire {
                        id: "init-note".to_string(),
                        timestamp: "2026-01-01T00:00:00Z".to_string(),
                        author: "owner@example.com".to_string(),
                        text: "Init @attachment:init.png".to_string(),
                        edited_at: None,
                        edited_by: None,
                        attachments: vec![blob("init.png", DIGEST_A)],
                    });
                    injected = true;
                }
            }
            event.validate().unwrap();
            out_lines.push(serde_json::to_string(&event).unwrap());
        }
        assert!(injected, "IssueCreated event found");
        std::fs::write(&stream_path, out_lines.join("\n") + "\n").unwrap();
        let references = bead_attachment_references(&beads_dir).unwrap();
        let init = references
            .iter()
            .find(|row| row.name == "init.png")
            .expect("initial note reference");
        assert_eq!(init.source, BeadAttachmentSourceWire::Note);
        let roster = bead_attachment_roster(&beads_dir, &issue_id).unwrap();
        assert!(roster.iter().any(|row| row.name == "init.png"));
    }
}
