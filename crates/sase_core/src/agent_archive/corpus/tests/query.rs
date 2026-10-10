use std::collections::BTreeMap;

use chrono::DateTime;
use serde_json::{json, Value};
use tempfile::tempdir;

use super::{create_index, insert_row, request, TestRow};
use crate::agent_archive::corpus::compile::compile_agent_archive_corpus;
use crate::agent_archive::corpus::query::{
    count_agent_archive_corpus, lookup_agent_archive_corpus,
    rows_agent_archive_corpus, summarize_agent_archive_corpus,
};
use crate::agent_archive::corpus::wire::{
    AgentArchiveCorpusWire, AgentArchiveCountRequestWire,
    AgentArchiveIndexProbeStatusWire, AgentArchiveLightRowWire,
    AgentArchiveLinkFacetsWire, AgentArchiveLookupRequestWire,
    AgentArchiveOutcomeWire, AgentArchiveQueryError,
    AgentArchiveRowsRequestWire, AgentArchiveSummaryRequestWire,
};

fn archive_profile() -> Value {
    json!({
        "pane_id": "agents-archive",
        "boolean": true,
        "fields": [
            {"key": "name", "exact_match": true, "searchable": true},
            {"key": "session", "exact_match": true},
            {"key": "clan", "exact_match": true},
            {"key": "project", "exact_match": true},
            {"key": "role"},
            {"key": "workflow"},
            {"key": "model"},
            {
                "key": "provider",
                "value_kind": "enum",
                "static_values": ["agy", "claude", "codex", "grok", "muse", "opencode", "qwen"]
            },
            {
                "key": "kind",
                "value_kind": "enum",
                "static_values": ["agent", "member", "session", "clan", "workflow", "workflow-child", "run"]
            },
            {
                "key": "status",
                "value_kind": "enum",
                "static_values": ["STARTING", "RUNNING", "WAITING", "DONE", "FAILED", "COMPLETED", "QUESTION"]
            },
            {"key": "since", "value_kind": "date"},
            {"key": "until", "value_kind": "date"},
            {"key": "after", "value_kind": "date"},
            {"key": "before", "value_kind": "date"},
            {"key": "min", "value_kind": "int"},
            {"key": "max", "value_kind": "int"},
            {"key": "attempt", "value_kind": "int"},
            {"key": "retry", "value_kind": "bool", "static_values": ["true", "false"]},
            {
                "key": "outcome",
                "value_kind": "enum",
                "static_values": ["done", "failed", "interrupted"]
            },
            {"key": "restorable", "value_kind": "bool", "static_values": ["true", "false"]},
            {"key": "tab", "exact_match": true},
            {"key": "tribe", "exact_match": true},
            {"key": "clan_tribe"},
            {"key": "linked", "value_kind": "bool", "static_values": ["true", "false"]},
            {"key": "relation"},
            {"key": "artifact"}
        ]
    })
}

fn compile_fixture() -> AgentArchiveCorpusWire {
    let temp = tempdir().unwrap();
    let mut conn = create_index(temp.path(), 3);

    let mut alpha = TestRow::new("alpha");
    alpha.agent_name = Some("250302.alpha".to_string());
    alpha.project_name = Some("sase".to_string());
    alpha.model = Some("muse".to_string());
    alpha.stop_time = Some("2025-03-02T12:00:00Z".to_string());
    alpha.restorable = true;
    insert_row(&mut conn, temp.path(), &alpha);

    let mut child = TestRow::new("alpha");
    child.filename = "250302__child.json".to_string();
    child.bundle_path = temp
        .path()
        .join(&child.filename)
        .to_string_lossy()
        .to_string();
    child.agent_name = Some("250302.child".to_string());
    child.is_workflow_child = true;
    insert_row(&mut conn, temp.path(), &child);

    let mut beta = TestRow::new("beta");
    beta.agent_name = Some("250302.beta".to_string());
    beta.project_name = Some("bob".to_string());
    beta.model = Some("opus".to_string());
    beta.status = "FAILED".to_string();
    beta.stop_time = Some("2025-03-02T11:00:00Z".to_string());
    insert_row(&mut conn, temp.path(), &beta);

    let mut gamma = TestRow::new("gamma");
    gamma.agent_name = Some("250301.gamma".to_string());
    gamma.project_name = Some("sase".to_string());
    gamma.model = Some("muse".to_string());
    gamma.status = "RUNNING".to_string();
    gamma.stop_time = Some("2025-03-01T04:00:00Z".to_string());
    gamma.restorable = true;
    insert_row(&mut conn, temp.path(), &gamma);

    let mut clan_new = TestRow::new("clan-new");
    clan_new.agent_name = Some("250302.clan-new".to_string());
    clan_new.agent_clan = Some("review".to_string());
    clan_new.agent_clan_generation = Some("run-1".to_string());
    clan_new.project_name = Some("sase".to_string());
    clan_new.model = Some("muse".to_string());
    clan_new.stop_time = Some("2025-03-02T10:00:00Z".to_string());
    clan_new.restorable = true;
    insert_row(&mut conn, temp.path(), &clan_new);

    let mut clan_old = TestRow::new("clan-old");
    clan_old.agent_name = Some("250302.clan-old".to_string());
    clan_old.agent_clan = Some("review".to_string());
    clan_old.agent_clan_generation = Some("run-1".to_string());
    clan_old.project_name = Some("sase".to_string());
    clan_old.model = Some("muse".to_string());
    clan_old.status = "FAILED".to_string();
    clan_old.stop_time = Some("2025-03-02T09:00:00Z".to_string());
    insert_row(&mut conn, temp.path(), &clan_old);

    let mut tie_a = TestRow::new("tie-a");
    tie_a.agent_name = Some("250302.tie-a".to_string());
    tie_a.project_name = Some("sase".to_string());
    tie_a.model = Some("muse".to_string());
    tie_a.stop_time = Some("2025-03-02T08:00:00Z".to_string());
    insert_row(&mut conn, temp.path(), &tie_a);

    let mut tie_b = TestRow::new("tie-b");
    tie_b.agent_name = Some("250302.tie-b".to_string());
    tie_b.project_name = Some("sase".to_string());
    tie_b.model = Some("muse".to_string());
    tie_b.stop_time = Some("2025-03-02T08:00:00Z".to_string());
    insert_row(&mut conn, temp.path(), &tie_b);

    insert_row(&mut conn, temp.path(), &TestRow::new("no-times"));

    let mut facets = BTreeMap::new();
    facets.insert(
        "alpha".to_string(),
        AgentArchiveLinkFacetsWire {
            relations: vec!["implements".to_string()],
            artifacts: vec!["plan:archive".to_string()],
            count: 1,
        },
    );
    compile_agent_archive_corpus(request(
        temp.path(),
        AgentArchiveIndexProbeStatusWire::Ok,
        facets,
    ))
    .unwrap()
}

fn count_request(query: &str) -> AgentArchiveCountRequestWire {
    AgentArchiveCountRequestWire {
        query: query.to_string(),
        profile: archive_profile(),
    }
}

fn summary_request(
    query: &str,
    group_by: &str,
) -> AgentArchiveSummaryRequestWire {
    AgentArchiveSummaryRequestWire {
        query: query.to_string(),
        profile: archive_profile(),
        group_by: group_by.to_string(),
    }
}

fn rows_request(
    query: &str,
    group_by: &str,
    group_key: &str,
    offset: i64,
    limit: i64,
    expanded: &[&str],
) -> AgentArchiveRowsRequestWire {
    AgentArchiveRowsRequestWire {
        query: query.to_string(),
        profile: archive_profile(),
        group_by: group_by.to_string(),
        group_key: group_key.to_string(),
        offset,
        limit,
        expanded_container_keys: expanded
            .iter()
            .map(|key| (*key).to_string())
            .collect(),
    }
}

fn run_ids(rows: &[AgentArchiveLightRowWire]) -> Vec<&str> {
    rows.iter()
        .map(|row| row.archive_key.source_run_id.as_str())
        .collect()
}

#[test]
fn query_filters_outcome_restorable_project_model_date_and_link_facets() {
    let corpus = compile_fixture();

    let failed =
        count_agent_archive_corpus(&corpus, count_request("outcome:failed"))
            .unwrap();
    assert_eq!(failed.count, 2);

    let restorable =
        count_agent_archive_corpus(&corpus, count_request("restorable:true"))
            .unwrap();
    assert_eq!(restorable.count, 3);

    let project =
        count_agent_archive_corpus(&corpus, count_request("project:sase"))
            .unwrap();
    assert_eq!(project.count, 5);

    let model =
        count_agent_archive_corpus(&corpus, count_request("model:opus"))
            .unwrap();
    assert_eq!(model.count, 1);

    let bound = DateTime::parse_from_rfc3339("2025-03-02T10:30:00Z")
        .unwrap()
        .timestamp();
    let after = count_agent_archive_corpus(
        &corpus,
        count_request(&format!("after:{bound}")),
    )
    .unwrap();
    assert_eq!(after.count, 2);

    let linked =
        count_agent_archive_corpus(&corpus, count_request("linked:true"))
            .unwrap();
    assert_eq!(linked.count, 1);

    let relation = count_agent_archive_corpus(
        &corpus,
        count_request("relation:implements"),
    )
    .unwrap();
    assert_eq!(relation.count, 1);

    let artifact = count_agent_archive_corpus(
        &corpus,
        count_request(r#"artifact:"plan:archive""#),
    )
    .unwrap();
    assert_eq!(artifact.count, 1);
}

#[test]
fn query_summaries_omit_empty_groups_and_use_compile_timezone_days() {
    let corpus = compile_fixture();

    let day =
        summarize_agent_archive_corpus(&corpus, summary_request("", "day"))
            .unwrap();
    assert_eq!(day.total, 7);
    assert_eq!(
        day.groups
            .iter()
            .map(|group| group.key.as_str())
            .collect::<Vec<_>>(),
        vec!["2025-03-02", "2025-02-28", ""]
    );
    assert_eq!(day.groups[0].count, 5);
    assert_eq!(day.groups[1].count, 1);
    assert_eq!(day.groups[2].count, 1);

    let project =
        summarize_agent_archive_corpus(&corpus, summary_request("", "project"))
            .unwrap();
    assert_eq!(
        project
            .groups
            .iter()
            .map(|group| (group.key.as_str(), group.count))
            .collect::<Vec<_>>(),
        vec![("sase", 5), ("bob", 1), ("", 1)]
    );

    let outcome =
        summarize_agent_archive_corpus(&corpus, summary_request("", "outcome"))
            .unwrap();
    assert_eq!(
        outcome
            .groups
            .iter()
            .map(|group| (group.key.as_str(), group.count))
            .collect::<Vec<_>>(),
        vec![("done", 4), ("failed", 2), ("interrupted", 1)]
    );

    let model = summarize_agent_archive_corpus(
        &corpus,
        summary_request("outcome:failed", "model"),
    )
    .unwrap();
    assert_eq!(model.total, 2);
    assert_eq!(
        model
            .groups
            .iter()
            .map(|group| group.key.as_str())
            .collect::<Vec<_>>(),
        vec!["opus", "muse"]
    );
    assert!(
        !model.groups.iter().any(|group| group.count == 0),
        "empty groups must be omitted"
    );
}

#[test]
fn query_windows_page_flattened_rows_and_insert_expanded_members() {
    let corpus = compile_fixture();
    let first = rows_agent_archive_corpus(
        &corpus,
        rows_request("", "day", "2025-03-02", 0, 2, &[]),
    )
    .unwrap();
    assert_eq!(first.total, 5);
    assert_eq!(run_ids(&first.rows), vec!["alpha", "beta"]);
    assert!(!first.rows.iter().any(|row| row.container));

    let second = rows_agent_archive_corpus(
        &corpus,
        rows_request("", "day", "2025-03-02", 2, 2, &[]),
    )
    .unwrap();
    assert_eq!(run_ids(&second.rows), vec!["clan-new", "tie-a"]);
    assert!(second.rows[0].container);
    assert_eq!(second.rows[0].member_count, 2);
    assert_eq!(second.rows[0].agent_name.as_deref(), Some("review"));
    assert_eq!(second.rows[0].outcome, AgentArchiveOutcomeWire::Failed);

    let expanded = rows_agent_archive_corpus(
        &corpus,
        rows_request("", "day", "2025-03-02", 2, 3, &["clan:review:run-1"]),
    )
    .unwrap();
    assert_eq!(expanded.total, 7);
    assert_eq!(
        run_ids(&expanded.rows),
        vec!["clan-new", "clan-new", "clan-old"]
    );
    assert!(expanded.rows[0].container);
    assert!(!expanded.rows[1].container);
    assert!(!expanded.rows[2].container);
    assert_eq!(
        expanded.rows[1].agent_name.as_deref(),
        Some("250302.clan-new")
    );
    assert_eq!(
        expanded.rows[2].agent_name.as_deref(),
        Some("250302.clan-old")
    );
}

#[test]
fn query_lookup_resolves_short_canonical_global_and_workflow_child_names() {
    let corpus = compile_fixture();
    let lookup = |name: &str| {
        lookup_agent_archive_corpus(
            &corpus,
            AgentArchiveLookupRequestWire {
                name: name.to_string(),
            },
        )
        .row
        .unwrap()
        .archive_key
        .source_run_id
    };
    assert_eq!(lookup("alpha"), "alpha");
    assert_eq!(lookup("250302.alpha"), "alpha");
    assert_eq!(lookup("bryan.athena.alpha"), "alpha");
    assert_eq!(lookup("child"), "alpha");
    assert_eq!(lookup("250302.child"), "alpha");
    assert!(lookup_agent_archive_corpus(
        &corpus,
        AgentArchiveLookupRequestWire {
            name: "missing".to_string(),
        },
    )
    .row
    .is_none());
}

#[test]
fn query_unknown_field_enum_value_and_group_by_are_typed_errors() {
    let corpus = compile_fixture();

    let unknown_field =
        count_agent_archive_corpus(&corpus, count_request("nope:true"))
            .unwrap_err();
    let AgentArchiveQueryError::Query(error) = unknown_field else {
        panic!("expected query engine error, got {unknown_field:?}");
    };
    assert!(error.message.contains("Unknown property key"), "{error:?}");

    let unknown_value =
        count_agent_archive_corpus(&corpus, count_request("outcome:bananas"))
            .unwrap_err();
    let AgentArchiveQueryError::Query(error) = unknown_value else {
        panic!("expected query engine error, got {unknown_value:?}");
    };
    assert!(error.message.contains("must be one of"), "{error:?}");

    let unknown_group =
        summarize_agent_archive_corpus(&corpus, summary_request("", "color"))
            .unwrap_err();
    assert_eq!(
        unknown_group,
        AgentArchiveQueryError::UnknownGroupBy("color".to_string())
    );
}

#[test]
fn query_order_is_newest_activity_first_and_stable_on_ties() {
    let corpus = compile_fixture();
    let page = rows_agent_archive_corpus(
        &corpus,
        rows_request("", "day", "2025-03-02", 0, 10, &[]),
    )
    .unwrap();
    assert_eq!(
        run_ids(&page.rows),
        vec!["alpha", "beta", "clan-new", "tie-a", "tie-b"]
    );
    assert_eq!(page.rows[3].last_activity_at, page.rows[4].last_activity_at);
}
