use super::super::*;
use super::support::*;
use crate::bead::events::BeadEventOperationWire;
use crate::bead::events::BeadEventPayloadWire;
use crate::bead::jsonl::event_streams_dir;
use crate::bead::jsonl::read_event_store;
use crate::bead::jsonl::repair_event_store_manifest;
use crate::bead::mutation::store::store_io_stats;
use crate::bead::mutation::store::MutableStore;
use crate::bead::wire::IssueTypeWire;
use std::fs;
use tempfile::tempdir;

use crate::artifact_link::ArtifactLinkOriginWire;
use crate::artifact_link::BeadLinkDirectionWire;
use crate::bead::config::default_config;
use crate::bead::config::save_config;
use crate::bead::events::reduce_event_streams;
use crate::bead::events::BeadIssueUpdateEventFieldsWire;
#[test]
fn one_mutation_touches_only_the_mutated_stream_file() {
    let (_temp, beads_dir, ids) = multi_stream_store();
    let before = persisted_stream_files(&beads_dir);
    let mutated_file = format!("{}.jsonl", ids[1]);

    append_issue_note(
        &beads_dir,
        &ids[1],
        "probe",
        Some("owner@example.com".to_string()),
        Some("2026-01-01T00:10:00Z".to_string()),
    )
    .unwrap();

    let after = persisted_stream_files(&beads_dir);
    assert_eq!(
        before.keys().collect::<Vec<_>>(),
        after.keys().collect::<Vec<_>>()
    );
    for (name, before_file) in before {
        let after_file = after.get(&name).unwrap();
        if name == mutated_file {
            assert_ne!(after_file.bytes, before_file.bytes, "{name}");
        } else {
            assert_eq!(after_file.bytes, before_file.bytes, "{name}");
            assert_eq!(after_file.modified, before_file.modified, "{name}");
        }
    }
}

#[test]
fn sase_mk_blast_radius_regression_preserves_unrelated_stream_bytes() {
    let (_temp, beads_dir, ids) = multi_stream_store();
    let unrelated_path = beads_dir
        .join("events/streams")
        .join(format!("{}.jsonl", ids[0]));
    let canonical = fs::read_to_string(&unrelated_path).unwrap();
    let reordered = canonical
        .lines()
        .map(reordered_event_json_object)
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    assert_ne!(reordered, canonical);
    fs::write(&unrelated_path, &reordered).unwrap();

    append_issue_note(
        &beads_dir,
        &ids[2],
        "mutate a different stream",
        Some("owner@example.com".to_string()),
        Some("2026-01-01T00:10:00Z".to_string()),
    )
    .unwrap();

    assert_eq!(fs::read_to_string(&unrelated_path).unwrap(), reordered);
}

#[test]
fn legacy_jsonl_migration_first_save_writes_every_imported_stream() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "owner@example.com"))
        .unwrap();
    fs::write(
        beads_dir.join("issues.jsonl"),
        [
            issue(
                "sase-1",
                "First",
                "plan",
                None,
                "open",
                "2026-01-01T00:00:00Z",
            ),
            issue(
                "sase-2",
                "Second",
                "plan",
                None,
                "open",
                "2026-01-01T00:01:00Z",
            ),
            issue(
                "sase-3",
                "Third",
                "plan",
                None,
                "open",
                "2026-01-01T00:02:00Z",
            ),
        ]
        .join("\n")
            + "\n",
    )
    .unwrap();
    assert!(!beads_dir.join("events").exists());

    append_issue_note(
        &beads_dir,
        "sase-2",
        "migrate and mutate",
        Some("owner@example.com".to_string()),
        Some("2026-01-01T00:10:00Z".to_string()),
    )
    .unwrap();

    let (manifest, streams) = read_event_store(&beads_dir).unwrap();
    assert_eq!(manifest.stream_count, 3);
    assert_eq!(
        streams
            .iter()
            .map(|stream| stream.stream_id.as_str())
            .collect::<Vec<_>>(),
        vec!["sase-1", "sase-2", "sase-3"]
    );
}

#[test]
fn append_note_preserves_legacy_issue_created_prefix_and_projects_structured_note(
) {
    // Regression for the sase-t2.2 failure: an `issue_created` event
    // published before the structured-note rollout encodes
    // `payload.issue.notes` as a literal string, not the current
    // `Vec<BeadNoteWire>` shape. A later mutation must append its new
    // event without reserializing (and thereby rewriting) that
    // already-published line.
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "owner@example.com"))
        .unwrap();

    let streams_dir = event_streams_dir(&beads_dir);
    fs::create_dir_all(&streams_dir).unwrap();
    let stream_path = streams_dir.join("sase-1.jsonl");
    let legacy_issue = issue(
        "sase-1",
        "Legacy",
        "plan",
        None,
        "open",
        "2026-01-01T00:00:00Z",
    );
    let legacy_line = format!(
        r#"{{"schema_version":1,"event_id":"sase-1:1","timestamp":"2026-01-01T00:00:00Z","actor":"owner@example.com","operation":"issue_created","issue_id":"sase-1","payload":{{"kind":"issue_created","issue":{legacy_issue}}}}}"#
    );
    fs::write(&stream_path, format!("{legacy_line}\n")).unwrap();
    repair_event_store_manifest(&beads_dir).unwrap();

    append_issue_note(
        &beads_dir,
        "sase-1",
        "a new note",
        Some("owner@example.com".to_string()),
        Some("2026-01-02T00:00:00Z".to_string()),
    )
    .unwrap();

    let after = fs::read_to_string(&stream_path).unwrap();
    assert!(
        after.starts_with(&legacy_line),
        "the published legacy issue_created line must be byte-identical"
    );
    assert_eq!(after.lines().count(), 2);

    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
    let issues = reduce_event_streams(&streams).unwrap();
    let issue = issues.iter().find(|issue| issue.id == "sase-1").unwrap();
    assert_eq!(issue.notes.len(), 1);
    assert_eq!(issue.notes[0].text, "a new note");
}

#[test]
fn mutable_appends_mint_stable_content_hashed_event_ids() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "owner@example.com"))
        .unwrap();
    fs::write(beads_dir.join("issues.jsonl"), "").unwrap();

    let epic = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Epic".to_string(),
            issue_type: IssueTypeWire::Plan,
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    let alpha_payload = BeadEventPayloadWire::IssueUpdated {
        fields: BeadIssueUpdateEventFieldsWire {
            title: Some("Alpha".to_string()),
            ..Default::default()
        },
    };
    let beta_payload = BeadEventPayloadWire::IssueUpdated {
        fields: BeadIssueUpdateEventFieldsWire {
            title: Some("Beta".to_string()),
            ..Default::default()
        },
    };
    let mut alpha = MutableStore::load(&beads_dir).unwrap();
    let mut duplicate = MutableStore::load(&beads_dir).unwrap();
    let mut beta = MutableStore::load(&beads_dir).unwrap();
    for (store, payload) in [
        (&mut alpha, alpha_payload.clone()),
        (&mut duplicate, alpha_payload),
        (&mut beta, beta_payload),
    ] {
        store
            .append_issue_event(
                &epic.id,
                BeadEventOperationWire::IssueUpdated,
                payload,
                "2026-01-01T00:01:00Z",
                "owner@example.com",
            )
            .unwrap();
    }

    let alpha_id = &alpha.streams.all()[0].events.last().unwrap().event_id;
    let duplicate_id =
        &duplicate.streams.all()[0].events.last().unwrap().event_id;
    let beta_id = &beta.streams.all()[0].events.last().unwrap().event_id;

    assert_eq!(alpha_id, duplicate_id);
    assert_ne!(alpha_id, beta_id);
    assert_eq!(
        alpha_id.rsplit_once(':').unwrap().0,
        beta_id.rsplit_once(':').unwrap().0
    );
    assert!(alpha_id.rsplit(':').next().is_some_and(|digest| {
        digest.len() == 64
            && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
    }));
}

#[test]
fn event_backed_child_id_reuse_after_remove_matches_jsonl_semantics() {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("sdd/beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", "owner@example.com"))
        .unwrap();
    fs::write(beads_dir.join("issues.jsonl"), "").unwrap();

    let epic = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Epic".to_string(),
            issue_type: IssueTypeWire::Plan,
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    let first = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "First child".to_string(),
            issue_type: IssueTypeWire::Phase,
            parent_id: Some(epic.id.clone()),
            now: Some("2026-01-01T00:01:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();
    remove_issue(&beads_dir, &first.id).unwrap();
    let second = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "Replacement child".to_string(),
            issue_type: IssueTypeWire::Phase,
            parent_id: Some(epic.id.clone()),
            now: Some("2026-01-01T00:02:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();

    assert_eq!(second.id, first.id);
    let store = MutableStore::load(&beads_dir).unwrap();
    assert_eq!(
        store.get_issue(&second.id).unwrap().title,
        "Replacement child"
    );
}

#[test]
fn link_projection_batch_takes_one_load_and_save_cycle() {
    let temp = tempdir().unwrap();
    init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = temp.path().join("beads");
    let mut issue_ids = Vec::new();
    for index in 0..8 {
        let issue = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: format!("Issue {index}"),
                issue_type: IssueTypeWire::Plan,
                now: Some(format!("2026-01-01T00:00:{index:02}Z")),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        issue_ids.push(issue.id);
    }

    let mut requests = Vec::new();
    for (index, issue_id) in issue_ids.iter().enumerate() {
        for inner in 0..4 {
            let n = (index * 4 + inner + 1) as u64;
            requests.push(projection_request(
                issue_id,
                &format!("plan:202609/{n}.md"),
                "related",
                BeadLinkDirectionWire::Out,
                true,
                &hex_operation_id(n),
                &format!("edge {n}"),
                ArtifactLinkOriginWire::Manual,
                1,
                &format!("2026-01-01T01:{:02}:00Z", n),
            ));
        }
    }
    assert_eq!(requests.len(), 32);

    store_io_stats::reset();
    let batch = set_bead_link_projections(&beads_dir, &requests).unwrap();
    assert!(batch.changed);
    assert_eq!(store_io_stats::loads(), 1);
    assert_eq!(store_io_stats::saves(), 1);

    let singleton_root = temp.path().join("singleton-root");
    fs::create_dir_all(&singleton_root).unwrap();
    init_store(&singleton_root, "beads", "sase", "owner@example.com").unwrap();
    let singleton_dir = singleton_root.join("beads");
    let mut singleton_ids = Vec::new();
    for index in 0..8 {
        let issue = create_issue(
            &singleton_dir,
            BeadCreateRequestWire {
                title: format!("Issue {index}"),
                issue_type: IssueTypeWire::Plan,
                now: Some(format!("2026-01-01T00:00:{index:02}Z")),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        singleton_ids.push(issue.id);
    }
    let singleton_requests: Vec<_> = requests
        .iter()
        .enumerate()
        .map(|(index, request)| {
            let mut cloned = request.clone();
            cloned.issue_id = singleton_ids[index / 4].clone();
            cloned
        })
        .collect();

    store_io_stats::reset();
    for request in &singleton_requests {
        set_bead_link_projection(
            &singleton_dir,
            &request.issue_id,
            &request.target_ref,
            &request.relation,
            request.direction,
            request.present,
            request.description.clone(),
            request.origin,
            request.uses,
            request.now.clone(),
            request.operation_id.clone(),
        )
        .unwrap();
    }
    assert_eq!(store_io_stats::loads(), 32);
    assert_eq!(store_io_stats::saves(), 32);
}
