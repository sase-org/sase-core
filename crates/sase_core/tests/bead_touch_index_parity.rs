//! Fixture parity for the actor-keyed bead touch index: a small store of
//! event streams reduces, refreshes incrementally, and queries to the exact
//! snapshot pinned in `expected_query.json`.

use std::fs;
use std::path::Path;

use sase_core::bead::{
    bead_touch_index_status, query_bead_touches, refresh_bead_touch_index,
    BeadTouchIndexStateWire, BEAD_TOUCH_INDEX_WIRE_SCHEMA_VERSION,
};
use serde_json::Value;
use tempfile::tempdir;

const PLAN_STREAM: &str =
    include_str!("fixtures/bead/touch_index/streams/plan-1.jsonl");
const TASK_STREAM: &str =
    include_str!("fixtures/bead/touch_index/streams/task-1.jsonl");
// Only an excluded operation: contributes no touch.
const ONLY_STREAM: &str =
    include_str!("fixtures/bead/touch_index/streams/only-1.jsonl");
// Events for `task-1` filed under another name: does not belong to its file.
const COPIED_STREAM: &str =
    include_str!("fixtures/bead/touch_index/streams/copied.jsonl");
const EXPECTED_QUERY: &str =
    include_str!("fixtures/bead/touch_index/expected_query.json");

fn write_store(beads_dir: &Path) {
    let streams = beads_dir.join("events/streams");
    fs::create_dir_all(&streams).unwrap();
    for (name, contents) in [
        ("plan-1", PLAN_STREAM),
        ("task-1", TASK_STREAM),
        ("only-1", ONLY_STREAM),
        ("copied", COPIED_STREAM),
    ] {
        fs::write(streams.join(format!("{name}.jsonl")), contents).unwrap();
    }
}

#[test]
fn fixture_store_reduces_to_the_pinned_snapshot() {
    let dir = tempdir().unwrap();
    let beads_dir = dir.path().join("beads");
    let index_path = dir.path().join("agent_bead_touches.json");
    write_store(&beads_dir);

    let refresh = refresh_bead_touch_index(&beads_dir, &index_path).unwrap();
    assert!(refresh.full_rebuild && refresh.wrote);
    assert_eq!(
        refresh.reduced_streams,
        vec!["copied", "only-1", "plan-1", "task-1"]
    );
    assert_eq!((refresh.stream_count, refresh.touch_count), (4, 6));

    let query = query_bead_touches(&index_path, None);
    assert_eq!(query.schema_version, BEAD_TOUCH_INDEX_WIRE_SCHEMA_VERSION);
    assert_eq!(query.generation, refresh.generation);
    let expected: Value = serde_json::from_str(EXPECTED_QUERY).unwrap();
    assert_eq!(serde_json::to_value(&query.touches).unwrap(), expected);

    let status = bead_touch_index_status(&beads_dir, &index_path).unwrap();
    assert_eq!(status.state, BeadTouchIndexStateWire::Fresh);

    // A second refresh over the same store reuses everything and leaves the
    // file alone.
    let again = refresh_bead_touch_index(&beads_dir, &index_path).unwrap();
    assert!(!again.wrote && !again.full_rebuild);
    assert_eq!(again.reused_streams, 4);
    assert_eq!(query_bead_touches(&index_path, None), query);
}

#[test]
fn per_actor_queries_are_exact_subsets_of_the_snapshot() {
    let dir = tempdir().unwrap();
    let beads_dir = dir.path().join("beads");
    let index_path = dir.path().join("agent_bead_touches.json");
    write_store(&beads_dir);
    refresh_bead_touch_index(&beads_dir, &index_path).unwrap();
    let expected: Vec<Value> = serde_json::from_str(EXPECTED_QUERY).unwrap();

    for actor in [
        "bbugyi200.athena.0aa",
        "bbugyi200.athena.0bb",
        "013",
        "sase-zt.6.5.land",
        "owner@example.com",
        "bbugyi200.athena.0cc",
    ] {
        let got = query_bead_touches(&index_path, Some(&[actor.to_string()]));
        let want: Vec<&Value> = expected
            .iter()
            .filter(|touch| touch["actor"] == actor)
            .collect();
        assert_eq!(
            serde_json::to_value(&got.touches).unwrap(),
            serde_json::to_value(want).unwrap(),
            "{actor}"
        );
    }
}
