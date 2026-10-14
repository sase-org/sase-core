use std::sync::Arc;
use std::thread;
use std::time::Instant;

use serde_json::{json, Value};

use crate::artifact_link::relation::builtin_artifact_relations;
use crate::artifact_link::row_resolution::{
    artifact_row_index_keys, ArtifactRowIdentityWire,
};

use super::{
    artifact_link_index_chips, artifact_link_index_project,
    artifact_link_index_rows, artifact_link_index_target,
    compile_artifact_link_index, ArtifactLinkIndexBuildRequestWire,
    ArtifactLinkIndexIdentityWire, ArtifactLinkIndexProjectRequestWire,
    ArtifactLinkIndexRowsRequestWire, PreparedArtifactLinkIndex,
    ARTIFACT_LINK_INDEX_WIRE_SCHEMA_VERSION,
};

const INPUT_JSON: &str = include_str!("fixtures/input.json");
const EXPECTED_CHIPS_JSON: &str = include_str!("fixtures/expected_chips.json");

fn fixture_request() -> ArtifactLinkIndexBuildRequestWire {
    let payload: Value = serde_json::from_str(INPUT_JSON).unwrap();
    let identity = payload
        .get("identity")
        .cloned()
        .unwrap_or_else(|| json!({}));
    ArtifactLinkIndexBuildRequestWire {
        schema_version: ARTIFACT_LINK_INDEX_WIRE_SCHEMA_VERSION,
        rows: payload
            .get("rows")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default(),
        identity: serde_json::from_value(identity).unwrap(),
        relations: Vec::new(),
    }
}

fn compile_fixture() -> PreparedArtifactLinkIndex {
    compile_artifact_link_index(fixture_request()).unwrap()
}

fn identity(pane_id: &str, parts: &[&str]) -> ArtifactRowIdentityWire {
    ArtifactRowIdentityWire {
        pane_id: pane_id.to_string(),
        parts: parts.iter().map(|part| (*part).to_string()).collect(),
    }
}

fn all_relation_slugs() -> Vec<String> {
    builtin_artifact_relations()
        .into_iter()
        .map(|relation| relation.slug)
        .collect()
}

fn chip_json(chip: &super::ArtifactLinkIndexChipWire) -> Value {
    json!({
        "created_at": chip.created_at,
        "created_by": chip.created_by,
        "directed": chip.directed,
        "label": chip.label,
        "neighbor_ref": chip.neighbor_ref,
        "neighbor_target": serde_json::to_value(&chip.neighbor_target).unwrap(),
        "origin": chip.origin,
        "relation": chip.relation,
        "this_is_source": chip.this_is_source,
        "uses": chip.uses,
        "why": chip.why,
        "writable": chip.writable,
    })
}

fn expected_chip_json(value: &Value) -> Value {
    let mut object = serde_json::Map::new();
    for key in [
        "created_at",
        "created_by",
        "directed",
        "label",
        "neighbor_ref",
        "neighbor_target",
        "origin",
        "relation",
        "this_is_source",
        "uses",
        "why",
        "writable",
    ] {
        object.insert(
            key.to_string(),
            value.get(key).cloned().unwrap_or(Value::Null),
        );
    }
    Value::Object(object)
}

#[test]
fn parity_fixture_pins_every_link_chip_and_target_field() {
    let index = compile_fixture();
    let expected: Value = serde_json::from_str(EXPECTED_CHIPS_JSON).unwrap();
    let by_ref = expected["by_ref"].as_object().unwrap();
    assert_eq!(index.chip_ref_count(), by_ref.len());
    let mut diffs = Vec::new();
    for (reference, expected_chips) in by_ref {
        let actual = artifact_link_index_chips(&index, reference);
        let expected_list = expected_chips.as_array().unwrap();
        if actual.len() != expected_list.len() {
            diffs.push(format!(
                "{reference}: chip count {} != {}",
                actual.len(),
                expected_list.len()
            ));
            continue;
        }
        for (offset, (chip, expected_chip)) in
            actual.iter().zip(expected_list).enumerate()
        {
            let actual_json = chip_json(chip);
            let expected_json = expected_chip_json(expected_chip);
            if actual_json != expected_json {
                diffs.push(format!(
                    "{reference}[{offset}]: {actual_json} != {expected_json}"
                ));
            }
        }
    }
    let targets = expected["targets_by_ref"].as_object().unwrap();
    for (reference, expected_target) in targets {
        let actual = artifact_link_index_target(&index, reference);
        assert!(actual.present, "{reference} should be indexed");
        let actual_json = serde_json::to_value(&actual.target).unwrap();
        if &actual_json != expected_target {
            diffs.push(format!(
                "target {reference}: {actual_json} != {expected_target}"
            ));
        }
    }
    assert!(diffs.is_empty(), "{}", diffs.join("\n"));
}

#[test]
fn parity_fixture_skips_empty_refs_and_keeps_malformed_endpoints() {
    let index = compile_fixture();
    assert!(artifact_link_index_chips(&index, "").is_empty());
    assert!(artifact_link_index_chips(&index, "   ").is_empty());
    let malformed = artifact_link_index_chips(&index, "not-a-ref");
    assert!(!malformed.is_empty());
    assert_eq!(malformed[0].neighbor_ref, "bead:sase-1");
    let target = artifact_link_index_target(&index, "not-a-ref");
    assert!(target.present);
    assert!(target.target.is_none());
}

#[test]
fn parity_fixture_first_wins_equal_uses_and_max_uses_unequal() {
    let index = compile_fixture();
    let cites = artifact_link_index_chips(&index, "agent:worker");
    let plan_chips: Vec<_> = cites
        .iter()
        .filter(|chip| chip.neighbor_ref == "plan:202608/x.md")
        .collect();
    assert_eq!(plan_chips.len(), 1);
    assert_eq!(plan_chips[0].uses, 5);
    assert_eq!(plan_chips[0].why, "high uses wins");
    let related: Vec<_> = artifact_link_index_chips(&index, "file:doc")
        .into_iter()
        .filter(|chip| chip.neighbor_ref == "file:other")
        .collect();
    assert_eq!(related.len(), 1);
    assert_eq!(related[0].why, "first equal uses");
}

#[test]
fn parity_fixture_alias_collision_does_not_shadow_canonical() {
    let index = compile_fixture();
    assert_eq!(
        artifact_link_index_chips(&index, "ref:plan:202608/x.md")[0].relation,
        "cites"
    );
    assert_eq!(
        artifact_link_index_chips(&index, "plan:202608/x.md")[0].relation,
        "related"
    );
    assert_eq!(
        artifact_link_index_chips(&index, "agent:athena.foo"),
        artifact_link_index_chips(&index, "agent:foo")
    );
    let full = "stitch:sase@0123456789abcdef0123456789abcdef01234567";
    assert_eq!(
        artifact_link_index_chips(&index, "stitch:sase@0123456"),
        artifact_link_index_chips(&index, full)
    );
}

#[test]
fn parity_fixture_self_edge_and_projected_writability() {
    let index = compile_fixture();
    let self_chips = artifact_link_index_chips(&index, "bead:sase-self");
    assert_eq!(self_chips.len(), 2);
    let perspectives: std::collections::BTreeSet<_> =
        self_chips.iter().map(|chip| chip.this_is_source).collect();
    assert_eq!(perspectives, [true, false].into_iter().collect());
    let awaits = artifact_link_index_chips(&index, "agent:alice.athena.9w");
    assert!(!awaits.is_empty());
    assert!(!awaits[0].writable);
    assert_eq!(awaits[0].origin, "projected");
}

#[test]
fn project_query_preserves_first_row_patch_edges() {
    let index = compile_fixture();
    let known_targets = vec![
        identity("patches", &["alpha", "feat"]),
        identity("patches", &["alpha", "feat_child"]),
        identity("patches", &["alpha", "same"]),
    ];
    let response = artifact_link_index_project(
        &index,
        ArtifactLinkIndexProjectRequestWire {
            schema_version: ARTIFACT_LINK_INDEX_WIRE_SCHEMA_VERSION,
            known_targets,
            permitted_relations: all_relation_slugs(),
            project_hint: Some("alpha".to_string()),
        },
    );
    let edges: Vec<_> = response
        .edges
        .iter()
        .map(|edge| {
            (
                edge.relation.as_str(),
                edge.source.pane_id.as_str(),
                edge.source.parts.clone(),
                edge.target.pane_id.as_str(),
                edge.target.parts.clone(),
                edge.description.as_str(),
                edge.uses,
            )
        })
        .collect();
    assert!(
        edges.contains(&(
            "implements",
            "patches",
            vec!["alpha".into(), "feat".into()],
            "beads",
            vec!["alpha".into(), "task".into(), "sase-1".into()],
            "patch implements bead",
            4,
        )),
        "{edges:?}"
    );
    assert!(
        edges.contains(&(
            "related",
            "patches",
            vec!["alpha".into(), "same".into()],
            "patches",
            vec!["alpha".into(), "feat".into()],
            "alpha same-name patch",
            1,
        )),
        "{edges:?}"
    );
    assert!(
        edges.contains(&(
            "related",
            "patches",
            vec!["alpha".into(), "same".into()],
            "ref:plan",
            vec!["beta".into(), "archive".into(), "202608/a.md".into()],
            "beta same-name patch",
            1,
        )),
        "{edges:?}"
    );
    assert!(
        edges.contains(&(
            "related",
            "patches",
            vec!["alpha".into(), "feat".into()],
            "patches",
            vec!["alpha".into(), "ghost".into()],
            "dangling neighbor",
            1,
        )),
        "{edges:?}"
    );
}

#[test]
fn rows_and_project_do_not_scan_unrelated_rows() {
    let mut rows = Vec::new();
    for index in 0..200 {
        rows.push(json!({
            "source_ref": format!("bead:sase-unrelated-{index}"),
            "relation": "related",
            "target_ref": format!("bead:sase-other-{index}"),
            "uses": 1,
            "_project": "noise",
        }));
    }
    rows.push(json!({
        "source_ref": "patch:feat",
        "relation": "implements",
        "target_ref": "bead:sase-1",
        "description": "only patch row",
        "uses": 1,
        "_project": "alpha",
    }));
    rows.push(json!({
        "source_ref": "patch:feat",
        "relation": "related",
        "target_ref": "patch:ghost",
        "description": "dangling neighbor",
        "uses": 1,
        "_project": "alpha",
    }));
    let index =
        compile_artifact_link_index(ArtifactLinkIndexBuildRequestWire {
            schema_version: ARTIFACT_LINK_INDEX_WIRE_SCHEMA_VERSION,
            rows,
            identity: ArtifactLinkIndexIdentityWire::default(),
            relations: Vec::new(),
        })
        .unwrap();
    assert_eq!(index.raw_row_count(), 202);
    let known = identity("patches", &["alpha", "feat"]);
    let lookup_keys = artifact_row_index_keys(&known);
    let rows_response = artifact_link_index_rows(
        &index,
        ArtifactLinkIndexRowsRequestWire {
            schema_version: ARTIFACT_LINK_INDEX_WIRE_SCHEMA_VERSION,
            lookup_keys,
        },
    );
    assert_eq!(rows_response.candidate_row_count, 2);
    assert_eq!(rows_response.rows.len(), 2);
    assert!(rows_response.candidate_row_count < index.raw_row_count() as u64);
    let project = artifact_link_index_project(
        &index,
        ArtifactLinkIndexProjectRequestWire {
            schema_version: ARTIFACT_LINK_INDEX_WIRE_SCHEMA_VERSION,
            known_targets: vec![known],
            permitted_relations: all_relation_slugs(),
            project_hint: Some("alpha".to_string()),
        },
    );
    assert_eq!(project.candidate_row_count, 2);
    assert_eq!(project.edges.len(), 2);
}

#[test]
fn empty_lookup_keys_yield_no_rows() {
    let index = compile_fixture();
    let response = artifact_link_index_rows(
        &index,
        ArtifactLinkIndexRowsRequestWire {
            schema_version: ARTIFACT_LINK_INDEX_WIRE_SCHEMA_VERSION,
            lookup_keys: Vec::new(),
        },
    );
    assert!(response.rows.is_empty());
    assert_eq!(response.candidate_row_count, 0);
}

#[test]
fn empty_and_malformed_rows_do_not_reject_the_snapshot() {
    let index =
        compile_artifact_link_index(ArtifactLinkIndexBuildRequestWire {
            schema_version: ARTIFACT_LINK_INDEX_WIRE_SCHEMA_VERSION,
            rows: vec![
                json!("not-an-object"),
                json!({
                    "source_ref": "",
                    "relation": "related",
                    "target_ref": "bead:sase-1",
                }),
                json!({
                    "source_ref": "bead:sase-1",
                    "relation": "related",
                    "target_ref": "bead:sase-2",
                    "uses": "nope",
                }),
            ],
            identity: ArtifactLinkIndexIdentityWire::default(),
            relations: Vec::new(),
        })
        .unwrap();
    assert_eq!(index.raw_row_count(), 3);
    let chips = artifact_link_index_chips(&index, "bead:sase-1");
    assert_eq!(chips.len(), 1);
    assert_eq!(chips[0].uses, 0);
    assert_eq!(chips[0].neighbor_ref, "bead:sase-2");
}

#[test]
fn frozen_handle_supports_concurrent_reads() {
    let index = Arc::new(compile_fixture());
    let mut joins = Vec::new();
    for _ in 0..8 {
        let handle = Arc::clone(&index);
        joins.push(thread::spawn(move || {
            let chips = artifact_link_index_chips(&handle, "bead:sase-1");
            assert!(!chips.is_empty());
            let target = artifact_link_index_target(&handle, "bead:sase-1");
            assert!(target.present);
            artifact_link_index_rows(
                &handle,
                ArtifactLinkIndexRowsRequestWire {
                    schema_version: ARTIFACT_LINK_INDEX_WIRE_SCHEMA_VERSION,
                    lookup_keys: artifact_row_index_keys(&identity(
                        "beads",
                        &["alpha", "task", "sase-1"],
                    )),
                },
            )
            .candidate_row_count
        }));
    }
    let counts: Vec<u64> =
        joins.into_iter().map(|join| join.join().unwrap()).collect();
    assert!(counts.iter().all(|count| *count == counts[0]));
}

#[test]
fn native_build_and_selected_query_are_measurable() {
    let request = fixture_request();
    let started = Instant::now();
    let index = compile_artifact_link_index(request).unwrap();
    let build = started.elapsed();
    let started = Instant::now();
    let _chips = artifact_link_index_chips(&index, "bead:sase-1");
    let chips = started.elapsed();
    let started = Instant::now();
    let _project = artifact_link_index_project(
        &index,
        ArtifactLinkIndexProjectRequestWire {
            schema_version: ARTIFACT_LINK_INDEX_WIRE_SCHEMA_VERSION,
            known_targets: vec![
                identity("patches", &["alpha", "feat"]),
                identity("patches", &["alpha", "same"]),
            ],
            permitted_relations: all_relation_slugs(),
            project_hint: Some("alpha".to_string()),
        },
    );
    let project = started.elapsed();
    assert!(build.as_secs_f64() >= 0.0);
    assert!(chips.as_secs_f64() >= 0.0);
    assert!(project.as_secs_f64() >= 0.0);
}

#[test]
fn missing_ref_is_absent_not_indexed_non_selectable() {
    let index = compile_fixture();
    let missing = artifact_link_index_target(&index, "missing:ref");
    assert!(!missing.present);
    assert!(missing.target.is_none());
    let chop =
        artifact_link_index_target(&index, "chop:refresh_docs/refresh_docs");
    assert!(chop.present);
    assert!(chop.target.is_none());
}

#[test]
fn neighbor_target_uses_row_project_not_first_indexed_target() {
    let index = compile_fixture();
    let chips = artifact_link_index_chips(&index, "file:hint");
    assert_eq!(chips.len(), 1);
    let neighbor = chips[0].neighbor_target.as_ref().unwrap();
    assert_eq!(neighbor.pane_id, "beads");
    assert_eq!(neighbor.parts, vec!["beta", "task", "sase-1"]);
    let indexed = artifact_link_index_target(&index, "bead:sase-1");
    assert_eq!(
        indexed.target.unwrap().parts,
        vec!["alpha", "task", "sase-1"]
    );
}

#[test]
fn colliding_agent_alias_resolves_to_earliest_row_in_row_order() {
    let identity = ArtifactLinkIndexIdentityWire {
        username: Some("bbugyi200".to_string()),
        machine_name: Some("athena".to_string()),
        sibling_machines: vec!["athena".to_string()],
        known_owner_roots: vec![
            "athena".to_string(),
            "bbugyi200.athena".to_string(),
        ],
    };
    let request = ArtifactLinkIndexBuildRequestWire {
        schema_version: ARTIFACT_LINK_INDEX_WIRE_SCHEMA_VERSION,
        rows: vec![
            json!({
                "source_ref": "agent:072",
                "relation": "read",
                "target_ref": "plan:202609/x.md",
                "uses": 1,
                "_project": "alpha",
            }),
            json!({
                "source_ref": "stitch:sase@0123456789abcdef0123456789abcdef01234567",
                "relation": "produced-by",
                "target_ref": "agent:bbugyi200.athena.072",
                "uses": 1,
                "_project": "alpha",
            }),
        ],
        identity,
        relations: Vec::new(),
    };
    let index = compile_artifact_link_index(request).unwrap();
    // Both canonicals claim the `agent:athena.072` spelling; the earliest
    // row's chips must win deterministically, matching the historical
    // Python first-wins alias pass on every process.
    for _ in 0..5 {
        let chips = artifact_link_index_chips(&index, "agent:athena.072");
        assert_eq!(chips.len(), 1);
        assert_eq!(chips[0].relation, "read");
        assert_eq!(chips[0].neighbor_ref, "plan:202609/x.md");
    }
    let own = artifact_link_index_chips(&index, "agent:072");
    assert_eq!(own.len(), 1);
    assert_eq!(own[0].relation, "read");
    let qualified =
        artifact_link_index_chips(&index, "agent:bbugyi200.athena.072");
    assert_eq!(qualified.len(), 1);
    assert_eq!(qualified[0].relation, "produced-by");
}

fn compile_rows(rows: Vec<Value>) -> PreparedArtifactLinkIndex {
    compile_artifact_link_index(ArtifactLinkIndexBuildRequestWire {
        schema_version: ARTIFACT_LINK_INDEX_WIRE_SCHEMA_VERSION,
        rows,
        identity: ArtifactLinkIndexIdentityWire::default(),
        relations: Vec::new(),
    })
    .unwrap()
}

fn chip_for(
    rows: Vec<Value>,
    reference: &str,
) -> super::ArtifactLinkIndexChipWire {
    let index = compile_rows(rows);
    let chips = artifact_link_index_chips(&index, reference);
    assert_eq!(chips.len(), 1, "expected one chip for {reference}");
    chips.into_iter().next().unwrap()
}

fn row_with(field: &str, value: Value) -> Value {
    let mut row = serde_json::Map::new();
    row.insert(
        "source_ref".to_string(),
        Value::String("bead:a".to_string()),
    );
    row.insert(
        "target_ref".to_string(),
        Value::String("bead:b".to_string()),
    );
    row.insert("relation".to_string(), Value::String("related".to_string()));
    row.insert(field.to_string(), value);
    Value::Object(row)
}

#[test]
fn tolerant_uses_match_legacy_truthiness_truncation_and_strings() {
    for (value, expected) in [
        (json!(true), 1),
        (json!(false), 0),
        (json!(2.9), 2),
        (json!(2.0), 2),
        (json!(-2.9), 0),
        (json!("1_000"), 1000),
        (json!("  5 "), 5),
        (json!("+7"), 7),
        (json!("-3"), 0),
        (json!("1__000"), 0),
        (json!("_5"), 0),
        (json!("5_"), 0),
        (json!("5.0"), 0),
        (json!("١_٢"), 12),
        (json!("184467440737095516160000"), u64::MAX),
        (json!(""), 0),
        (json!(null), 0),
        (json!(0), 0),
        (json!(0.0), 0),
        (json!(5), 5),
        (json!(-3), 0),
        (json!([]), 0),
        (json!({}), 0),
        (json!([1]), 0),
        (json!({"a": 1}), 0),
    ] {
        let chip = chip_for(vec![row_with("uses", value.clone())], "bead:a");
        assert_eq!(chip.uses, expected, "uses {value}");
    }
}

#[test]
fn tolerant_string_fields_match_legacy_python_str() {
    let chip = chip_for(vec![row_with("description", json!(false))], "bead:a");
    assert_eq!(chip.why, "");
    let chip = chip_for(vec![row_with("description", json!(true))], "bead:a");
    assert_eq!(chip.why, "True");
    let chip =
        chip_for(vec![row_with("description", json!({"a": 1}))], "bead:a");
    assert_eq!(chip.why, "{'a': 1}");
    let chip = chip_for(vec![row_with("description", json!([1, 2]))], "bead:a");
    assert_eq!(chip.why, "[1, 2]");
    let chip = chip_for(
        vec![row_with(
            "description",
            json!({"a": 1, "quote": "it's", "items": [true, null]}),
        )],
        "bead:a",
    );
    assert_eq!(
        chip.why,
        "{'a': 1, 'quote': \"it's\", 'items': [True, None]}"
    );
    let chip = chip_for(vec![row_with("description", json!(1e-7))], "bead:a");
    assert_eq!(chip.why, "1e-07");
    let chip = chip_for(vec![row_with("origin", json!(0))], "bead:a");
    assert_eq!(chip.origin, "");
    let chip = chip_for(vec![row_with("origin", json!(5))], "bead:a");
    assert_eq!(chip.origin, "5");
    let chip =
        chip_for(vec![row_with("description", json!("  padded  "))], "bead:a");
    assert_eq!(chip.why, "padded");
}

#[test]
fn tolerant_duplicate_winner_uses_legacy_counts_and_edges_keep_first_row() {
    let index = compile_rows(vec![
        json!({
            "source_ref": "patch:feat",
            "relation": "cites",
            "target_ref": "bead:sase-1",
            "uses": 1,
            "description": "first row",
            "origin": "first",
            "_project": "alpha",
        }),
        json!({
            "source_ref": "patch:feat",
            "relation": "cites",
            "target_ref": "bead:sase-1",
            "uses": "1_000",
            "description": "larger same-project duplicate",
            "origin": "same-project-winner",
            "_project": "alpha",
        }),
        json!({
            "source_ref": "patch:feat",
            "relation": "cites",
            "target_ref": "bead:sase-1",
            "uses": 5000,
            "description": "larger cross-project chip winner",
            "origin": "cross-project-winner",
            "_project": "beta",
        }),
    ]);
    let chips = artifact_link_index_chips(&index, "patch:feat");
    assert_eq!(chips.len(), 1);
    assert_eq!(chips[0].uses, 5000);
    assert_eq!(chips[0].why, "larger cross-project chip winner");
    assert_eq!(chips[0].origin, "cross-project-winner");
    let project = artifact_link_index_project(
        &index,
        ArtifactLinkIndexProjectRequestWire {
            schema_version: ARTIFACT_LINK_INDEX_WIRE_SCHEMA_VERSION,
            known_targets: vec![identity("patches", &["alpha", "feat"])],
            permitted_relations: all_relation_slugs(),
            project_hint: Some("alpha".to_string()),
        },
    );
    assert_eq!(project.edges.len(), 2);
    assert_eq!(project.edges[0].source.parts, ["alpha", "feat"]);
    assert_eq!(project.edges[0].description, "first row");
    assert_eq!(project.edges[0].origin, "first");
    assert_eq!(project.edges[0].uses, 1);
    assert_eq!(project.edges[1].target.parts, ["beta", "task", "sase-1"]);
    assert_eq!(
        project.edges[1].description,
        "larger cross-project chip winner"
    );
    assert_eq!(project.edges[1].origin, "cross-project-winner");
    assert_eq!(project.edges[1].uses, 5000);
}
