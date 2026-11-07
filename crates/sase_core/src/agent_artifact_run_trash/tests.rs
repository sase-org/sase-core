use super::*;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::json;
use tempfile::TempDir;

use crate::agent_artifact_run_retention::AgentArtifactRunCandidateWire;
use crate::continuation::ContinuationRetentionRunWire;
use crate::store_lock::LockMode;

const TIMESTAMP: &str = "20260101000000";

struct Fixture {
    _temp: TempDir,
    projects: PathBuf,
    trash: PathBuf,
    run: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let projects = temp.path().join("projects");
        let trash = temp.path().join("trash");
        let run = projects
            .join("demo/artifacts/ace-run/202601/01")
            .join(TIMESTAMP);
        fs::create_dir_all(&run).unwrap();
        fs::write(run.join("done.json"), b"{}\n").unwrap();
        fs::write(run.join("payload.txt"), b"original bytes\n").unwrap();
        Self {
            _temp: temp,
            projects,
            trash,
            run,
        }
    }

    fn quarantine_request(
        &self,
        batch_id: &str,
    ) -> AgentArtifactRunTrashQuarantineRequestWire {
        AgentArtifactRunTrashQuarantineRequestWire {
            schema_version: AGENT_ARTIFACT_RUN_TRASH_WIRE_SCHEMA_VERSION,
            projects_root: self.projects.to_string_lossy().into_owned(),
            sources_unavailable: Vec::new(),
            protected_dirs: Vec::new(),
            protected_timestamps: Vec::new(),
            recent_months: Vec::new(),
            current_timestamp: "20260102000000".to_string(),
            items: vec![candidate(&self.run, TIMESTAMP)],
            trash_root: self.trash.to_string_lossy().into_owned(),
            batch_id: batch_id.to_string(),
            created_at: "2026-02-01T00:00:00Z".to_string(),
            empty_shard_roots: Vec::new(),
            empty_shard_watched_paths: Vec::new(),
            empty_shard_removal_budget: 10,
        }
    }

    fn restore_request(
        &self,
        batch_id: &str,
    ) -> AgentArtifactRunTrashRestoreRequestWire {
        AgentArtifactRunTrashRestoreRequestWire {
            schema_version: AGENT_ARTIFACT_RUN_TRASH_WIRE_SCHEMA_VERSION,
            trash_root: self.trash.to_string_lossy().into_owned(),
            projects_root: self.projects.to_string_lossy().into_owned(),
            batch_id: batch_id.to_string(),
            items: None,
        }
    }

    fn purge_request(&self) -> AgentArtifactRunTrashPurgeRequestWire {
        AgentArtifactRunTrashPurgeRequestWire {
            schema_version: AGENT_ARTIFACT_RUN_TRASH_WIRE_SCHEMA_VERSION,
            trash_root: self.trash.to_string_lossy().into_owned(),
            projects_root: self.projects.to_string_lossy().into_owned(),
            before: None,
            purge_all: true,
            sources_unavailable: Vec::new(),
            protected_dirs: Vec::new(),
            protected_timestamps: Vec::new(),
            protected_agent_names: Vec::new(),
            non_closed_bead_ids_by_project: BTreeMap::new(),
            continuation_runs: Vec::new(),
        }
    }
}

fn candidate(path: &Path, timestamp: &str) -> AgentArtifactRunCandidateWire {
    AgentArtifactRunCandidateWire {
        artifact_dir: path.to_string_lossy().into_owned(),
        project: "demo".to_string(),
        timestamp: timestamp.to_string(),
        protected_reasons: Vec::new(),
    }
}

fn trash_run(fixture: &Fixture, batch: &str) -> PathBuf {
    fixture
        .trash
        .join(batch)
        .join("runs/demo/202601/01")
        .join(TIMESTAMP)
}

#[test]
fn quarantine_wire_requires_every_protection_evidence_field() {
    let value = json!({
        "schema_version": AGENT_ARTIFACT_RUN_TRASH_WIRE_SCHEMA_VERSION,
        "projects_root": "/tmp/projects",
        "sources_unavailable": [],
        "protected_dirs": [],
        "recent_months": [],
        "current_timestamp": "20260102000000",
        "items": [],
        "trash_root": "/tmp/trash",
        "batch_id": "batch-1",
        "created_at": "2026-02-01T00:00:00Z"
    });
    assert!(
        serde_json::from_value::<AgentArtifactRunTrashQuarantineRequestWire>(
            value
        )
        .is_err()
    );

    let purge = json!({
        "schema_version": AGENT_ARTIFACT_RUN_TRASH_WIRE_SCHEMA_VERSION,
        "trash_root": "/tmp/trash",
        "projects_root": "/tmp/projects",
        "before": null,
        "purge_all": false,
        "sources_unavailable": [],
        "protected_dirs": [],
        "protected_timestamps": []
    });
    assert!(
        serde_json::from_value::<AgentArtifactRunTrashPurgeRequestWire>(purge)
            .is_err()
    );

    let stale = json!({
        "schema_version": 1,
        "trash_root": "/tmp/trash",
        "projects_root": "/tmp/projects",
        "before": null,
        "purge_all": false,
        "sources_unavailable": [],
        "protected_dirs": [],
        "protected_timestamps": [],
        "protected_agent_names": [],
        "non_closed_bead_ids_by_project": {},
        "continuation_runs": []
    });
    let parsed =
        serde_json::from_value::<AgentArtifactRunTrashPurgeRequestWire>(stale)
            .unwrap();
    assert!(purge_agent_artifact_run_trash(&parsed).is_err());
}

#[test]
fn unavailable_protections_block_quarantine_and_purge_without_effects() {
    let fixture = Fixture::new();
    let mut request = fixture.quarantine_request("blocked");
    request
        .sources_unavailable
        .push("artifact index".to_string());
    let result = quarantine_agent_artifact_runs(&request).unwrap();
    assert_eq!(result.status, "blocked");
    assert_eq!(
        result.blocked_reason.as_deref(),
        Some("protection_unavailable")
    );
    assert!(fixture.run.exists());
    assert!(!fixture.trash.exists());

    let ready = fixture.quarantine_request("purge-blocked");
    assert_eq!(
        quarantine_agent_artifact_runs(&ready).unwrap().quarantined,
        1
    );
    let mut request = fixture.purge_request();
    request.sources_unavailable = vec!["beads".to_string()];
    let purged = purge_agent_artifact_run_trash(&request).unwrap();
    assert_eq!(purged.status, "blocked");
    assert_eq!(purged.purged, 0);
    assert!(trash_run(&fixture, "purge-blocked").exists());
}

#[test]
fn quarantine_rechecks_protection_terminal_markers_and_unsafe_paths() {
    let fixture = Fixture::new();

    let mut protected = fixture.quarantine_request("protected");
    protected.protected_timestamps.push(TIMESTAMP.to_string());
    let result = quarantine_agent_artifact_runs(&protected).unwrap();
    assert!(result.items[0]
        .reasons
        .iter()
        .any(|reason| reason == "protected:referenced_timestamp"));

    let active = fixture
        .projects
        .join("demo/artifacts/ace-run/202601/02/20260102000000");
    fs::create_dir_all(&active).unwrap();
    fs::write(active.join("done.json"), b"{}\n").unwrap();
    fs::write(active.join("running.json"), b"{}\n").unwrap();
    let mut request = fixture.quarantine_request("not-terminal");
    request.items = vec![candidate(&active, "20260102000000")];
    request.current_timestamp = "20260103000000".to_string();
    let result = quarantine_agent_artifact_runs(&request).unwrap();
    assert_eq!(result.items[0].reasons, vec!["active_marker:running.json"]);

    let nonterminal = fixture
        .projects
        .join("demo/artifacts/ace-run/202601/03/20260103000000");
    fs::create_dir_all(&nonterminal).unwrap();
    let mut request = fixture.quarantine_request("active-marker");
    request.items = vec![candidate(&nonterminal, "20260103000000")];
    request.current_timestamp = "20260104000000".to_string();
    let result = quarantine_agent_artifact_runs(&request).unwrap();
    assert_eq!(result.items[0].reasons, vec!["not_terminal"]);

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;
        let real = fixture.projects.join("real/artifacts");
        let alias = fixture.projects.join("alias");
        let aliased_run = real.join("ace-run/202601/04/20260104000000");
        fs::create_dir_all(&aliased_run).unwrap();
        fs::write(aliased_run.join("done.json"), b"{}\n").unwrap();
        fs::create_dir_all(&alias).unwrap();
        symlink(&real, alias.join("artifacts")).unwrap();
        let mut request = fixture.quarantine_request("symlink");
        let path = alias.join("artifacts/ace-run/202601/04/20260104000000");
        request.items = vec![AgentArtifactRunCandidateWire {
            artifact_dir: path.to_string_lossy().into_owned(),
            project: "alias".to_string(),
            timestamp: "20260104000000".to_string(),
            protected_reasons: Vec::new(),
        }];
        request.current_timestamp = "20260105000000".to_string();
        let result = quarantine_agent_artifact_runs(&request).unwrap();
        assert_eq!(result.items[0].reasons, vec!["unsafe_path"]);
        assert!(aliased_run.exists());
    }
}

#[test]
fn quarantine_moves_to_mirrored_layout_and_restore_is_byte_identical() {
    let fixture = Fixture::new();
    let expected = fs::read(fixture.run.join("payload.txt")).unwrap();
    let mut request = fixture.quarantine_request("success");
    request.empty_shard_roots = vec![fixture
        .projects
        .join("demo/artifacts/ace-run")
        .to_string_lossy()
        .into_owned()];
    let result = quarantine_agent_artifact_runs(&request).unwrap();
    assert_eq!(result.status, "complete");
    assert_eq!(result.quarantined, 1);
    assert!(result.removed_empty_shards > 0);
    assert!(!fixture.run.exists());
    assert!(!fixture
        .projects
        .join("demo/artifacts/ace-run/202601")
        .exists());
    let moved = trash_run(&fixture, "success");
    assert_eq!(fs::read(moved.join("payload.txt")).unwrap(), expected);
    // ensure_trash_root reports paths under the canonical trash root; on macOS the
    // tempdir's /var ancestor resolves to /private/var.
    let canonical_trash = fs::canonicalize(&fixture.trash).unwrap();
    assert_eq!(
        Path::new(&result.journal_path),
        canonical_trash.join("success/journal.jsonl")
    );

    let restored =
        restore_agent_artifact_run_trash(&fixture.restore_request("success"))
            .unwrap();
    assert_eq!(restored.restored, 1);
    assert_eq!(fs::read(fixture.run.join("payload.txt")).unwrap(), expected);
    assert!(!fixture.trash.join("success").exists());
}

#[test]
fn destination_conflict_is_kept_and_duplicate_batch_can_resume() {
    let fixture = Fixture::new();
    let batch = fixture.trash.join("resume");
    let destination = trash_run(&fixture, "resume");
    fs::create_dir_all(destination.parent().unwrap()).unwrap();
    fs::create_dir(&destination).unwrap();
    fs::write(
        batch.join("journal.jsonl"),
        format!(
            "{}\n",
            json!({
                "kind": "header",
                "schema_version": 1,
                "batch_id": "resume",
                "created_at": "2026-02-01T00:00:00Z"
            })
        ),
    )
    .unwrap();
    let result =
        quarantine_agent_artifact_runs(&fixture.quarantine_request("resume"))
            .unwrap();
    assert_eq!(result.kept, 1);
    assert_eq!(result.items[0].reasons, vec!["trash_conflict"]);
    assert!(fixture.run.exists());
}

#[test]
fn layout_discovery_finds_and_restores_a_move_missing_from_the_journal() {
    let fixture = Fixture::new();
    fs::remove_dir_all(&fixture.run).unwrap();
    let orphan = trash_run(&fixture, "crash");
    fs::create_dir_all(&orphan).unwrap();
    fs::write(orphan.join("done.json"), b"{}\n").unwrap();
    fs::write(orphan.join("payload.txt"), b"orphan\n").unwrap();
    let batch = fixture.trash.join("crash");
    fs::write(
        batch.join("journal.jsonl"),
        format!(
            "{}\n",
            json!({
                "kind": "header",
                "schema_version": 1,
                "batch_id": "crash",
                "created_at": "2026-02-01T00:00:00Z"
            })
        ),
    )
    .unwrap();
    let listed =
        list_agent_artifact_run_trash(&fixture.trash.to_string_lossy())
            .unwrap();
    assert_eq!(listed.batches[0].orphan_items.len(), 1);
    assert_eq!(listed.batches[0].orphan_items[0].timestamp, TIMESTAMP);
    assert_eq!(
        listed.batches[0].orphan_items[0].original_path,
        format!("demo/artifacts/ace-run/202601/01/{TIMESTAMP}")
    );
    let restored =
        restore_agent_artifact_run_trash(&fixture.restore_request("crash"))
            .unwrap();
    assert_eq!(restored.restored, 1);
    assert_eq!(
        fs::read(fixture.run.join("payload.txt")).unwrap(),
        b"orphan\n"
    );
}

#[test]
fn restore_reports_original_path_conflicts_and_keeps_the_trash_item() {
    let fixture = Fixture::new();
    assert_eq!(
        quarantine_agent_artifact_runs(&fixture.quarantine_request("conflict"))
            .unwrap()
            .quarantined,
        1
    );
    fs::create_dir_all(fixture.run.parent().unwrap()).unwrap();
    fs::create_dir(&fixture.run).unwrap();
    fs::write(fixture.run.join("new.txt"), b"new data\n").unwrap();
    let result =
        restore_agent_artifact_run_trash(&fixture.restore_request("conflict"))
            .unwrap();
    assert_eq!(result.conflicts, 1);
    assert!(trash_run(&fixture, "conflict").exists());
    assert_eq!(
        fs::read(fixture.run.join("new.txt")).unwrap(),
        b"new data\n"
    );
}

#[test]
fn purge_restores_newly_protected_runs_and_refuses_symlink_trash_paths() {
    let fixture = Fixture::new();
    assert_eq!(
        quarantine_agent_artifact_runs(
            &fixture.quarantine_request("protected")
        )
        .unwrap()
        .quarantined,
        1
    );
    let mut request = fixture.purge_request();
    request.protected_timestamps = vec![TIMESTAMP.to_string()];
    let result = purge_agent_artifact_run_trash(&request).unwrap();
    assert_eq!(result.restored, 1);
    assert!(fixture.run.join("payload.txt").exists());

    let symlink_fixture = Fixture::new();
    assert_eq!(
        quarantine_agent_artifact_runs(
            &symlink_fixture.quarantine_request("symlink-purge")
        )
        .unwrap()
        .quarantined,
        1
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;
        let outside = symlink_fixture._temp.path().join("outside");
        fs::create_dir_all(&outside).unwrap();
        fs::write(outside.join("keep.txt"), b"safe\n").unwrap();
        let trash_item = trash_run(&symlink_fixture, "symlink-purge");
        fs::remove_dir_all(&trash_item).unwrap();
        symlink(&outside, &trash_item).unwrap();
        let request = symlink_fixture.purge_request();
        let result = purge_agent_artifact_run_trash(&request).unwrap();
        assert_eq!(result.purged, 0);
        assert_eq!(fs::read(outside.join("keep.txt")).unwrap(), b"safe\n");
    }
}

#[test]
fn cross_device_runs_are_kept_when_a_second_filesystem_is_available() {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let Ok(project_temp) = tempfile::tempdir_in("/dev/shm") else {
            return;
        };
        let trash_temp = tempfile::tempdir().unwrap();
        if fs::metadata(project_temp.path()).unwrap().dev()
            == fs::metadata(trash_temp.path()).unwrap().dev()
        {
            return;
        }
        let projects = project_temp.path().join("projects");
        let run = projects
            .join("demo/artifacts/ace-run/202601/01")
            .join(TIMESTAMP);
        fs::create_dir_all(&run).unwrap();
        fs::write(run.join("done.json"), b"{}\n").unwrap();
        let request = AgentArtifactRunTrashQuarantineRequestWire {
            schema_version: AGENT_ARTIFACT_RUN_TRASH_WIRE_SCHEMA_VERSION,
            projects_root: projects.to_string_lossy().into_owned(),
            sources_unavailable: Vec::new(),
            protected_dirs: Vec::new(),
            protected_timestamps: Vec::new(),
            recent_months: Vec::new(),
            current_timestamp: "20260102000000".to_string(),
            items: vec![candidate(&run, TIMESTAMP)],
            trash_root: trash_temp
                .path()
                .join("trash")
                .to_string_lossy()
                .into_owned(),
            batch_id: "cross-device".to_string(),
            created_at: "2026-02-01T00:00:00Z".to_string(),
            empty_shard_roots: Vec::new(),
            empty_shard_watched_paths: Vec::new(),
            empty_shard_removal_budget: 0,
        };
        let result = quarantine_agent_artifact_runs(&request).unwrap();
        assert_eq!(result.items[0].reasons, vec!["cross_device"]);
        assert!(run.exists());
    }
}

#[test]
fn a_held_trash_lock_returns_busy() {
    let fixture = Fixture::new();
    fs::create_dir_all(&fixture.trash).unwrap();
    let _held = crate::store_lock::acquire_store_lock(
        &fixture.trash.join(".agent-artifact-run-trash.lock"),
        &fixture.trash.join(".agent-artifact-run-trash.lock.json"),
        LockMode::Exclusive,
        Duration::from_millis(100),
        "test-holder",
    )
    .unwrap();
    let result =
        quarantine_agent_artifact_runs(&fixture.quarantine_request("busy"))
            .unwrap();
    assert_eq!(result.blocked_reason.as_deref(), Some("busy"));
    assert!(fixture.run.exists());
}

const TIMESTAMP_B: &str = "20260101010000";

fn extra_run(fixture: &Fixture, timestamp: &str) -> PathBuf {
    let run = fixture
        .projects
        .join("demo/artifacts/ace-run")
        .join(&timestamp[..6])
        .join(&timestamp[6..8])
        .join(timestamp);
    fs::create_dir_all(&run).unwrap();
    fs::write(run.join("done.json"), b"{}\n").unwrap();
    fs::write(run.join("payload.txt"), format!("payload {timestamp}\n"))
        .unwrap();
    run
}

fn continuation_run(
    artifact_dir: &Path,
    timestamp: &str,
    live: bool,
) -> ContinuationRetentionRunWire {
    ContinuationRetentionRunWire {
        artifact_dir: artifact_dir.to_string_lossy().into_owned(),
        timestamp: timestamp.to_string(),
        node_id: None,
        parent_node_ids: Vec::new(),
        starter_artifact_dir: None,
        live,
        recoverable: false,
        portable_refs: Vec::new(),
        required_ref_failures: Vec::new(),
        metadata_unavailable: None,
    }
}

fn write_empty_children(run: &Path) {
    fs::create_dir(run.join("empty_child")).unwrap();
    fs::create_dir_all(run.join("nested/empty")).unwrap();
    fs::write(run.join("nested/file.txt"), b"keep nested\n").unwrap();
}

fn assert_empty_children(run: &Path) {
    assert!(run.join("empty_child").is_dir());
    assert_eq!(fs::read_dir(run.join("empty_child")).unwrap().count(), 0);
    assert!(run.join("nested/empty").is_dir());
    assert_eq!(fs::read_dir(run.join("nested/empty")).unwrap().count(), 0);
    assert_eq!(
        fs::read(run.join("nested/file.txt")).unwrap(),
        b"keep nested\n"
    );
}

#[test]
fn purge_restores_runs_protected_by_agent_bead_or_continuation() {
    let fixture = Fixture::new();
    fs::write(
        fixture.run.join("agent_meta.json"),
        json!({
            "name": "keeper",
            "bead_id": "sase-open"
        })
        .to_string(),
    )
    .unwrap();
    write_empty_children(&fixture.run);
    assert_eq!(
        quarantine_agent_artifact_runs(&fixture.quarantine_request("agent"))
            .unwrap()
            .quarantined,
        1
    );
    let mut request = fixture.purge_request();
    request.protected_agent_names = vec!["keeper".to_string()];
    let result = purge_agent_artifact_run_trash(&request).unwrap();
    assert_eq!(result.restored, 1);
    assert_eq!(result.purged, 0);
    assert!(result.items.iter().any(|item| {
        item.outcome == "restored_protected"
            && item
                .reasons
                .iter()
                .any(|reason| reason == "referenced_agent")
    }));
    assert_empty_children(&fixture.run);

    let bead = Fixture::new();
    fs::write(
        bead.run.join("agent_meta.json"),
        json!({"phase_bead_id": "sase-live"}).to_string(),
    )
    .unwrap();
    assert_eq!(
        quarantine_agent_artifact_runs(&bead.quarantine_request("bead"))
            .unwrap()
            .quarantined,
        1
    );
    let mut request = bead.purge_request();
    request.non_closed_bead_ids_by_project =
        BTreeMap::from([("demo".to_string(), vec!["sase-live".to_string()])]);
    let result = purge_agent_artifact_run_trash(&request).unwrap();
    assert_eq!(result.restored, 1);
    assert!(result.items.iter().any(|item| item
        .reasons
        .iter()
        .any(|reason| reason == "non_closed_bead")));
    assert!(bead.run.join("payload.txt").exists());

    let ancestry = Fixture::new();
    assert_eq!(
        quarantine_agent_artifact_runs(
            &ancestry.quarantine_request("continuation")
        )
        .unwrap()
        .quarantined,
        1
    );
    let original = ancestry
        .projects
        .join("demo/artifacts/ace-run/202601/01")
        .join(TIMESTAMP);
    let live = extra_run(&ancestry, "20260102000000");
    let mut parent = continuation_run(&original, TIMESTAMP, false);
    parent.node_id = Some("parent1".to_string());
    let mut child = continuation_run(&live, "20260102000000", true);
    child.node_id = Some("child1".to_string());
    child.parent_node_ids = vec!["parent1".to_string()];
    let mut request = ancestry.purge_request();
    request.continuation_runs = vec![parent, child];
    let result = purge_agent_artifact_run_trash(&request).unwrap();
    assert_eq!(result.restored, 1);
    assert!(result.items.iter().any(|item| item
        .reasons
        .iter()
        .any(|reason| reason == "continuation_ancestry")));
    assert_eq!(
        fs::read(ancestry.run.join("payload.txt")).unwrap(),
        b"original bytes\n"
    );
}

#[test]
fn purge_keeps_complete_trees_in_a_mixed_batch_and_skips_symlink_runs() {
    let fixture = Fixture::new();
    write_empty_children(&fixture.run);
    fs::write(fixture.run.join("agent_meta.json"), b"{not-json").unwrap();
    let other = extra_run(&fixture, TIMESTAMP_B);
    let mut request = fixture.quarantine_request("mixed");
    request.items = vec![
        candidate(&fixture.run, TIMESTAMP),
        candidate(&other, TIMESTAMP_B),
    ];
    assert_eq!(
        quarantine_agent_artifact_runs(&request)
            .unwrap()
            .quarantined,
        2
    );
    let result =
        purge_agent_artifact_run_trash(&fixture.purge_request()).unwrap();
    assert_eq!(result.purged, 1);
    assert_eq!(result.kept, 1);
    assert!(result.items.iter().any(|item| {
        item.outcome == "kept"
            && item
                .reasons
                .iter()
                .any(|reason| reason == "metadata_unavailable")
    }));
    let kept = trash_run(&fixture, "mixed");
    assert_empty_children(&kept);
    assert_eq!(
        fs::read(kept.join("payload.txt")).unwrap(),
        b"original bytes\n"
    );
    assert!(!fixture
        .trash
        .join("mixed/runs/demo/202601/01")
        .join(TIMESTAMP_B)
        .exists());

    let symlink_fixture = Fixture::new();
    assert_eq!(
        quarantine_agent_artifact_runs(
            &symlink_fixture.quarantine_request("runs-link")
        )
        .unwrap()
        .quarantined,
        1
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;
        let outside = symlink_fixture._temp.path().join("outside");
        fs::create_dir_all(&outside).unwrap();
        fs::write(outside.join("keep.txt"), b"safe\n").unwrap();
        let runs = symlink_fixture.trash.join("runs-link/runs");
        fs::remove_dir_all(&runs).unwrap();
        symlink(&outside, &runs).unwrap();
        let result =
            purge_agent_artifact_run_trash(&symlink_fixture.purge_request())
                .unwrap();
        assert_eq!(result.purged, 0);
        assert_eq!(fs::read(outside.join("keep.txt")).unwrap(), b"safe\n");
        assert!(outside.join("keep.txt").exists());
    }
}

#[test]
fn purge_counts_journal_and_cleanup_failures_without_dropping_effects() {
    let fixture = Fixture::new();
    assert_eq!(
        quarantine_agent_artifact_runs(&fixture.quarantine_request("journal"))
            .unwrap()
            .quarantined,
        1
    );
    let journal = fixture.trash.join("journal/journal.jsonl");
    let mut permissions = fs::metadata(&journal).unwrap().permissions();
    permissions.set_readonly(true);
    fs::set_permissions(&journal, permissions).unwrap();
    let result =
        purge_agent_artifact_run_trash(&fixture.purge_request()).unwrap();
    assert_eq!(result.purged, 1);
    assert!(result.errors >= 1);
    assert_eq!(result.status, "partial");
    assert!(!trash_run(&fixture, "journal").exists());

    let leftover = Fixture::new();
    assert_eq!(
        quarantine_agent_artifact_runs(&leftover.quarantine_request("cleanup"))
            .unwrap()
            .quarantined,
        1
    );
    let month = leftover.trash.join("cleanup/runs/demo/202601");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(&month).unwrap().permissions();
        permissions.set_mode(0o555);
        fs::set_permissions(&month, permissions.clone()).unwrap();
        let result =
            purge_agent_artifact_run_trash(&leftover.purge_request()).unwrap();
        assert_eq!(result.purged, 1);
        assert!(result.errors >= 1);
        assert_eq!(result.status, "partial");
        permissions.set_mode(0o755);
        fs::set_permissions(&month, permissions).unwrap();
    }
}
