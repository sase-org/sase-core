use super::super::storage::open_index;
use super::super::{LOOKUP_SESSION_INDEX_COLUMN, *};
use super::support::{artifact, rebuild_index, write_json};
use crate::agent_scan::layout::ACE_RUN_WORKFLOW_DIR;
use rusqlite::{params_from_iter, Connection};
use serde_json::json;
use std::fs;
use std::time::Duration;
use tempfile::tempdir;

fn names_query(names: &[&str]) -> AgentArtifactLookupQueryWire {
    AgentArtifactLookupQueryWire {
        target: AgentArtifactLookupTargetWire::Names {
            names: names.iter().map(|name| name.to_string()).collect(),
        },
        include_hidden: true,
        forced_unavailability: None,
    }
}

fn session_query(names: &[&str]) -> AgentArtifactLookupQueryWire {
    AgentArtifactLookupQueryWire {
        target: AgentArtifactLookupTargetWire::Session {
            names: names.iter().map(|name| name.to_string()).collect(),
        },
        include_hidden: true,
        forced_unavailability: None,
    }
}

fn clan_query(names: &[&str]) -> AgentArtifactLookupQueryWire {
    AgentArtifactLookupQueryWire {
        target: AgentArtifactLookupTargetWire::Clan {
            names: names.iter().map(|name| name.to_string()).collect(),
        },
        include_hidden: true,
        forced_unavailability: None,
    }
}

fn pointer_query(dirs: &[&str]) -> AgentArtifactLookupQueryWire {
    AgentArtifactLookupQueryWire {
        target: AgentArtifactLookupTargetWire::Pointers {
            artifact_dirs: dirs.iter().map(|dir| dir.to_string()).collect(),
        },
        include_hidden: true,
        forced_unavailability: None,
    }
}

fn write_named(dir: &std::path::Path, payload: serde_json::Value) {
    write_json(&dir.join("agent_meta.json"), payload);
}

fn explain_details(
    index: &std::path::Path,
    sql: &str,
    params: &[String],
) -> Vec<String> {
    let conn = Connection::open(index).unwrap();
    let mut stmt = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
    stmt.query_map(params_from_iter(params.iter()), |row| row.get(3))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

fn hold_exclusive(path: &std::path::Path) -> Connection {
    let holder = Connection::open(path).unwrap();
    holder
        .pragma_update(None, "locking_mode", "EXCLUSIVE")
        .unwrap();
    holder
        .execute(
            "UPDATE meta SET value = value WHERE key = 'schema_version'",
            [],
        )
        .unwrap();
    holder
}

#[test]
fn healthy_name_lookup_is_complete_and_does_not_decode_records() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let target = artifact(&projects, "20261116120000");
    write_named(
        &target,
        json!({
            "name": "lane-a--plan",
            "workflow_name": "lane-a",
            "agent_session": "lane-a"
        }),
    );
    for n in 0..40 {
        write_named(
            &artifact(&projects, &format!("2026111609{n:04}")),
            json!({"name": format!("unrelated-{n}")}),
        );
    }
    let index = rebuild_index(tmp.path(), &projects);
    let before = fs::metadata(&index).unwrap().modified().unwrap();

    let result = lookup_agent_artifacts(
        &index,
        &projects,
        names_query(&["lane-a--plan"]),
    );
    assert_eq!(
        result.availability,
        AgentArtifactLookupAvailabilityWire::Ready
    );
    assert!(result.complete);
    assert_eq!(result.source, "index");
    assert_eq!(result.hits.len(), 1);
    assert_eq!(result.hits[0].agent_name.as_deref(), Some("lane-a--plan"));
    assert_eq!(result.hits[0].lookup_session.as_deref(), Some("lane-a"));
    assert_eq!(result.hits[0].match_kind.as_deref(), Some("exact"));
    assert_eq!(last_lookup_records_decoded(), 0);
    assert_eq!(
        fs::metadata(&index).unwrap().modified().unwrap(),
        before,
        "healthy lookup must not migrate or rewrite the index"
    );
}

#[test]
fn metadata_aliases_match_scanner_for_indexed_and_recovery_queries() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let target = artifact(&projects, "20261116120000");
    write_named(
        &target,
        json!({
            "name": "lane--code",
            "workflow_name": "lane",
            "agent_family": "lane",
            "agent_clan": "wave",
            "patch_name": "feature",
            "changespec_name": "feature",
            "cl_name": "feature",
            "commit_patch_name": "feature",
            "commit_changespec_name": "feature"
        }),
    );
    let index = rebuild_index(tmp.path(), &projects);
    for index_path in [&index, &tmp.path().join("missing.sqlite")] {
        for query in [
            names_query(&["lane--code"]),
            session_query(&["lane"]),
            clan_query(&["wave"]),
        ] {
            let result = lookup_agent_artifacts(index_path, &projects, query);
            assert_eq!(result.hits.len(), 1, "{result:?}");
            assert_eq!(result.hits[0].lookup_session.as_deref(), Some("lane"));
            assert_eq!(result.hits[0].agent_clan.as_deref(), Some("wave"));
        }
    }
}

#[test]
fn healthy_empty_name_lookup_is_ready_and_complete() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_named(
        &artifact(&projects, "20261116120000"),
        json!({"name": "other"}),
    );
    let index = rebuild_index(tmp.path(), &projects);
    let result =
        lookup_agent_artifacts(&index, &projects, names_query(&["missing"]));
    assert_eq!(
        result.availability,
        AgentArtifactLookupAvailabilityWire::Ready
    );
    assert!(result.complete);
    assert_eq!(result.source, "index");
    assert!(result.hits.is_empty());
    assert!(!result.truncated);
}

#[test]
fn workflow_name_is_a_name_candidate() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_named(
        &artifact(&projects, "20261116120000"),
        json!({
            "name": "lane-a--code",
            "workflow_name": "lane-a"
        }),
    );
    let index = rebuild_index(tmp.path(), &projects);
    let result =
        lookup_agent_artifacts(&index, &projects, names_query(&["lane-a"]));
    assert_eq!(result.hits.len(), 1);
    assert_eq!(result.hits[0].match_kind.as_deref(), Some("workflow"));
}

#[test]
fn session_lookup_uses_effective_lookup_session() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_named(
        &artifact(&projects, "20261116120000"),
        json!({
            "name": "lane-a--plan",
            "workflow_name": "lane-a"
        }),
    );
    write_named(
        &artifact(&projects, "20261116120100"),
        json!({"name": "solo", "workflow_name": "lane-a"}),
    );
    let index = rebuild_index(tmp.path(), &projects);
    let stored: Option<String> = Connection::open(&index)
        .unwrap()
        .query_row(
            "SELECT lookup_session FROM agent_artifacts \
             WHERE agent_name = 'lane-a--plan'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(stored.as_deref(), Some("lane-a"));

    let result =
        lookup_agent_artifacts(&index, &projects, session_query(&["lane-a"]));
    assert_eq!(
        result.availability,
        AgentArtifactLookupAvailabilityWire::Ready
    );
    assert_eq!(result.hits.len(), 1);
    assert_eq!(result.hits[0].agent_name.as_deref(), Some("lane-a--plan"));
    assert_eq!(result.hits[0].match_kind.as_deref(), Some("session"));
    assert_eq!(last_lookup_records_decoded(), 0);
}

#[test]
fn clan_lookup_includes_legacy_parallel_session() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_named(
        &artifact(&projects, "20261116120000"),
        json!({
            "name": "wave",
            "agent_session": "wave",
            "agent_session_parallel": true
        }),
    );
    write_named(
        &artifact(&projects, "20261116120100"),
        json!({
            "name": "clan-member",
            "agent_clan": "clan-a",
            "agent_clan_generation": "g1"
        }),
    );
    let index = rebuild_index(tmp.path(), &projects);
    let stored: Option<String> = Connection::open(&index)
        .unwrap()
        .query_row(
            "SELECT agent_clan FROM agent_artifacts \
             WHERE agent_name = 'wave'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(stored.as_deref(), Some("wave"));

    let parallel =
        lookup_agent_artifacts(&index, &projects, clan_query(&["wave"]));
    assert_eq!(parallel.hits.len(), 1);
    assert_eq!(parallel.hits[0].agent_clan.as_deref(), Some("wave"));
    let named =
        lookup_agent_artifacts(&index, &projects, clan_query(&["clan-a"]));
    assert_eq!(named.hits.len(), 1);
    assert_eq!(named.hits[0].agent_name.as_deref(), Some("clan-member"));
}

#[test]
fn pointer_lookup_resolves_aliases_and_symlinks() {
    let tmp = tempdir().unwrap();
    // The index stores artifact dirs in the projects root's lexical form, and
    // macOS's `/var` -> `/private/var` link would otherwise make `canonical`
    // differ from the indexed path.
    let root = tmp.path().canonicalize().unwrap();
    let projects = root.join("projects");
    let dir = artifact(&projects, "20261116120000");
    write_named(&dir, json!({"name": "pointer-target"}));
    let index = rebuild_index(&root, &projects);
    let canonical = dir.canonicalize().unwrap();
    let alias = root.join("alias-pointer");
    upsert_agent_artifact_alias(
        &index,
        &alias.to_string_lossy(),
        &canonical.to_string_lossy(),
    )
    .unwrap();
    let link = root.join("symlink-pointer");
    std::os::unix::fs::symlink(&canonical, &link).unwrap();

    for path in [
        canonical.to_string_lossy().into_owned(),
        alias.to_string_lossy().into_owned(),
        link.to_string_lossy().into_owned(),
    ] {
        let result =
            lookup_agent_artifacts(&index, &projects, pointer_query(&[&path]));
        assert_eq!(result.hits.len(), 1, "path {path}");
        assert_eq!(
            result.hits[0].agent_name.as_deref(),
            Some("pointer-target")
        );
        assert_eq!(result.hits[0].match_kind.as_deref(), Some("pointer"));
    }
}

#[test]
fn name_lookup_query_plan_uses_agent_name_index() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_named(
        &artifact(&projects, "20261116120000"),
        json!({"name": "lane-a--plan"}),
    );
    let index = rebuild_index(tmp.path(), &projects);
    let sql = "SELECT artifact_dir FROM agent_artifacts \
         WHERE workflow_dir_name = ? \
           AND agent_meta_sig IS NOT NULL \
           AND (agent_name IN (?) OR workflow_name IN (?))";
    let params = vec![
        ACE_RUN_WORKFLOW_DIR.to_string(),
        "lane-a--plan".to_string(),
        "lane-a--plan".to_string(),
    ];
    let details = explain_details(&index, sql, &params);
    assert!(
        details.iter().any(|detail| {
            detail.contains("idx_agent_artifacts_agent_name")
                || detail.contains("idx_agent_artifacts_workflow_name")
        }),
        "expected a name/workflow index in {details:?}"
    );
}

#[test]
fn session_lookup_query_plan_uses_lookup_session_index() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_named(
        &artifact(&projects, "20261116120000"),
        json!({
            "name": "lane-a--plan",
            "workflow_name": "lane-a"
        }),
    );
    let index = rebuild_index(tmp.path(), &projects);
    let sql = format!(
        "SELECT artifact_dir FROM agent_artifacts \
         WHERE workflow_dir_name = ? \
           AND agent_meta_sig IS NOT NULL \
           AND {LOOKUP_SESSION_INDEX_COLUMN} IN (?)"
    );
    let params = vec![ACE_RUN_WORKFLOW_DIR.to_string(), "lane-a".to_string()];
    let details = explain_details(&index, &sql, &params);
    assert!(
        details
            .iter()
            .any(|detail| detail.contains("idx_agent_artifacts_lookup_session")),
        "expected lookup_session index in {details:?}"
    );
}

#[test]
fn exhausted_small_name_recovery_is_complete() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_named(
        &artifact(&projects, "20261116120000"),
        json!({"name": "from-source"}),
    );
    let index = tmp.path().join("missing.sqlite");
    let result = lookup_agent_artifacts(
        &index,
        &projects,
        names_query(&["from-source"]),
    );
    assert_eq!(
        result.availability,
        AgentArtifactLookupAvailabilityWire::Missing
    );
    assert!(result.complete);
    assert_eq!(result.source, "fallback");
    assert_eq!(result.hits.len(), 1);
    assert_eq!(result.hits[0].agent_name.as_deref(), Some("from-source"));
}

#[test]
fn missing_index_and_projects_root_is_a_complete_empty_result() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let index = tmp.path().join("missing.sqlite");
    let result = lookup_agent_artifacts(
        &index,
        &projects,
        session_query(&["missing-session"]),
    );
    assert_eq!(
        result.availability,
        AgentArtifactLookupAvailabilityWire::Missing
    );
    assert!(result.complete);
    assert_eq!(result.source, "none");
    assert!(result.hits.is_empty());
    assert_eq!(result.dirs_visited, 0);
}

#[test]
fn stale_schema_does_not_migrate_and_maintenance_restores_index() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_named(
        &artifact(&projects, "20261116120000"),
        json!({
            "name": "lane-a--plan",
            "workflow_name": "lane-a"
        }),
    );
    let index = rebuild_index(tmp.path(), &projects);
    {
        let conn = Connection::open(&index).unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES ('schema_version', '37')",
            [],
        )
        .unwrap();
        conn.execute("UPDATE agent_artifacts SET lookup_session = NULL", [])
            .unwrap();
    }
    let before = fs::metadata(&index).unwrap().modified().unwrap();
    std::thread::sleep(Duration::from_millis(20));
    let stale =
        lookup_agent_artifacts(&index, &projects, session_query(&["lane-a"]));
    assert_eq!(
        stale.availability,
        AgentArtifactLookupAvailabilityWire::StaleSchema
    );
    assert!(
        stale.complete,
        "the bounded source walk exhausted this fixture"
    );
    assert_eq!(stale.source, "fallback");
    assert_eq!(stale.hits.len(), 1);
    let version: String = Connection::open(&index)
        .unwrap()
        .query_row(
            "SELECT value FROM meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(version, "37");
    assert_eq!(fs::metadata(&index).unwrap().modified().unwrap(), before);

    drop(open_index(&index).unwrap());
    let restored =
        lookup_agent_artifacts(&index, &projects, session_query(&["lane-a"]));
    assert_eq!(
        restored.availability,
        AgentArtifactLookupAvailabilityWire::Ready
    );
    assert!(restored.complete);
    assert_eq!(restored.source, "index");
    assert_eq!(restored.hits.len(), 1);
}

#[test]
fn newer_schema_is_unavailable_without_writes() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_named(
        &artifact(&projects, "20261116120000"),
        json!({"name": "newer"}),
    );
    let index = rebuild_index(tmp.path(), &projects);
    {
        let conn = Connection::open(&index).unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES ('schema_version', '99')",
            [],
        )
        .unwrap();
    }
    let before = fs::metadata(&index).unwrap().modified().unwrap();
    let result =
        lookup_agent_artifacts(&index, &projects, names_query(&["newer"]));
    assert_eq!(
        result.availability,
        AgentArtifactLookupAvailabilityWire::NewerSchema
    );
    assert!(result.complete);
    assert!(!result.truncated);
    let version: String = Connection::open(&index)
        .unwrap()
        .query_row(
            "SELECT value FROM meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(version, "99");
    assert_eq!(fs::metadata(&index).unwrap().modified().unwrap(), before);
}

#[test]
fn corrupt_index_falls_back_without_quarantine() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_named(
        &artifact(&projects, "20261116120000"),
        json!({"name": "corrupt-src"}),
    );
    let index = tmp.path().join("agent_artifact_index.sqlite");
    fs::write(&index, b"this is not a sqlite database").unwrap();
    let result = lookup_agent_artifacts(
        &index,
        &projects,
        names_query(&["corrupt-src"]),
    );
    assert_eq!(
        result.availability,
        AgentArtifactLookupAvailabilityWire::Corrupt
    );
    assert!(result.complete);
    assert!(!result.truncated);
    assert_eq!(result.source, "fallback");
    assert_eq!(result.hits.len(), 1);
    let leftover: Vec<_> = fs::read_dir(tmp.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.contains(".corrupt-"))
        .collect();
    assert!(
        leftover.is_empty(),
        "lookup must not quarantine: {leftover:?}"
    );
}

#[test]
fn busy_index_falls_back_without_mutating() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_named(
        &artifact(&projects, "20261116120000"),
        json!({"name": "busy-src"}),
    );
    let index = rebuild_index(tmp.path(), &projects);
    let _holder = hold_exclusive(&index);
    let result =
        lookup_agent_artifacts(&index, &projects, names_query(&["busy-src"]));
    assert_eq!(
        result.availability,
        AgentArtifactLookupAvailabilityWire::Busy
    );
    assert!(result.complete);
    assert!(!result.truncated);
    assert_eq!(result.source, "fallback");
    assert_eq!(result.hits.len(), 1);
}

#[test]
fn fallback_budget_truncates_directory_walk() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let budget = AGENT_ARTIFACT_LOOKUP_FALLBACK_BUDGET as usize;
    for n in 0..=budget {
        write_named(
            &artifact(&projects, &format!("20261116{:06}", n)),
            json!({"name": format!("extra-{n}")}),
        );
    }
    let index = tmp.path().join("missing.sqlite");
    let result =
        lookup_agent_artifacts(&index, &projects, names_query(&["no-such"]));
    assert!(result.truncated);
    assert!(
        result.dirs_visited <= u64::from(AGENT_ARTIFACT_LOOKUP_FALLBACK_BUDGET)
    );
    assert!(!result.complete);
}

#[test]
fn hidden_rows_are_optional() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_named(
        &artifact(&projects, "20261116120000"),
        json!({"name": "hidden-agent", "hidden": true}),
    );
    let index = rebuild_index(tmp.path(), &projects);
    let included = lookup_agent_artifacts(
        &index,
        &projects,
        names_query(&["hidden-agent"]),
    );
    assert_eq!(included.hits.len(), 1);
    let mut query = names_query(&["hidden-agent"]);
    query.include_hidden = false;
    let excluded = lookup_agent_artifacts(&index, &projects, query);
    assert!(excluded.hits.is_empty());
}

#[test]
fn lookup_session_column_is_present_after_open() {
    let tmp = tempdir().unwrap();
    let index = tmp.path().join("agent_artifact_index.sqlite");
    drop(open_index(&index).unwrap());
    let conn = Connection::open(&index).unwrap();
    let mut stmt = conn.prepare("PRAGMA table_info(agent_artifacts)").unwrap();
    let columns: Vec<String> = stmt
        .query_map([], |row| row.get(1))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert!(columns
        .iter()
        .any(|column| column == LOOKUP_SESSION_INDEX_COLUMN));
    let version: String = conn
        .query_row(
            "SELECT value FROM meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(version, AGENT_ARTIFACT_INDEX_SCHEMA_VERSION.to_string());
}

#[test]
fn fallback_bounds_unrelated_directory_entries_and_reports_truncation() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    fs::create_dir_all(&projects).unwrap();
    for n in 0..300 {
        fs::write(projects.join(format!("unrelated-{n}")), "").unwrap();
    }
    let result = lookup_agent_artifacts(
        &tmp.path().join("missing.sqlite"),
        &projects,
        session_query(&["lane"]),
    );
    assert!(result.truncated);
    assert!(!result.complete);
    assert_eq!(result.dirs_visited, 200);
}

#[test]
fn bounded_recovery_preserves_legacy_non_timestamp_artifacts() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_named(
        &artifact(&projects, "member-ts"),
        json!({"name": "wave.worker", "agent_clan": "wave"}),
    );
    let result = lookup_agent_artifacts(
        &tmp.path().join("missing.sqlite"),
        &projects,
        clan_query(&["wave"]),
    );
    assert!(result.complete);
    assert_eq!(result.hits.len(), 1);
    assert_eq!(result.hits[0].agent_name.as_deref(), Some("wave.worker"));
}

#[test]
fn fully_exhausted_small_group_recovery_is_complete() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_named(
        &artifact(&projects, "20261116120000"),
        json!({"name":"lane--1", "agent_session":"lane"}),
    );
    let result = lookup_agent_artifacts(
        &tmp.path().join("missing.sqlite"),
        &projects,
        session_query(&["lane"]),
    );
    assert!(result.complete);
    assert_eq!(result.hits.len(), 1);
    assert_eq!(result.source, "fallback");
}

#[test]
fn healthy_lookup_revalidates_relation_and_markers_only_on_matching_records() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let member = artifact(&projects, "20261116120000");
    write_named(&member, json!({"name":"lane--1", "agent_session":"lane"}));
    let index = rebuild_index(tmp.path(), &projects);
    fs::write(member.join("done.json"), r#"{"outcome":"completed"}"#).unwrap();
    let result =
        lookup_agent_artifacts(&index, &projects, session_query(&["lane"]));
    assert!(result.hits[0].has_done_marker);
    write_named(
        &member,
        json!({"name":"renamed--1", "agent_session":"renamed"}),
    );
    assert!(lookup_agent_artifacts(
        &index,
        &projects,
        session_query(&["lane"])
    )
    .hits
    .is_empty());
}

#[test]
fn explicit_pointer_is_discovered_before_its_index_upsert() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    fs::create_dir_all(&projects).unwrap();
    let index = rebuild_index(tmp.path(), &projects);
    let member = artifact(&projects, "20261116120000");
    write_named(&member, json!({"name":"lane--1", "agent_session":"lane"}));
    let query = AgentArtifactLookupQueryWire {
        target: AgentArtifactLookupTargetWire::Pointers {
            artifact_dirs: vec![member.to_string_lossy().into_owned()],
        },
        include_hidden: true,
        forced_unavailability: None,
    };
    let result = lookup_agent_artifacts(&index, &projects, query);
    assert!(result.complete);
    assert_eq!(result.hits.len(), 1);
    assert_eq!(result.hits[0].lookup_session.as_deref(), Some("lane"));
}
