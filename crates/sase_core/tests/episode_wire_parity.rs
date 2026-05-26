//! Compare Rust episode wire JSON against the Python dataclass shape.

use sase_core::{
    EpisodeEdgeWire, EpisodeEventWire, EpisodeLessonWire, EpisodeNodeWire,
    EpisodeSourceRefWire, EpisodeWire, EPISODE_WIRE_SCHEMA_VERSION,
};
use serde_json::Value;
use std::collections::BTreeMap;

const PYTHON_FIXTURE: &str = r#"{
    "schema_version": 1,
    "episode_id": "ep-fixture",
    "project": "sase",
    "title": "Fixture Episode",
    "summary": "A deterministic fixture.",
    "root_source_id": "src-chat",
    "sources": [
        {
            "id": "src-chat",
            "kind": "chat",
            "path": "/tmp/chat.md",
            "label": null,
            "exists": true,
            "size_bytes": 12,
            "sha256": "aaaaaaaa"
        }
    ],
    "nodes": [
        {
            "id": "node-chat",
            "kind": "chat",
            "label": "Chat",
            "source_id": "src-chat",
            "metadata": {
                "agent": "coder"
            }
        }
    ],
    "edges": [
        {
            "id": "edge-chat-plan",
            "from_node_id": "node-chat",
            "to_node_id": "node-plan",
            "kind": "mentions",
            "evidence_ids": ["src-chat"],
            "metadata": {}
        }
    ],
    "events": [
        {
            "id": "event-start",
            "kind": "start",
            "timestamp": "2026-05-26T00:00:00Z",
            "title": "Started",
            "description": null,
            "evidence_ids": ["src-chat"]
        }
    ],
    "lessons": [
        {
            "id": "lesson-goal",
            "kind": "goal",
            "text": "Keep lessons source-grounded.",
            "evidence_ids": ["src-chat"],
            "source_confidence": "deterministic"
        }
    ],
    "metadata": {
        "changespec": "memory"
    }
}"#;

fn rust_episode() -> EpisodeWire {
    let mut node_metadata = BTreeMap::new();
    node_metadata.insert("agent".to_string(), "coder".to_string());
    let mut metadata = BTreeMap::new();
    metadata.insert("changespec".to_string(), "memory".to_string());

    EpisodeWire {
        schema_version: EPISODE_WIRE_SCHEMA_VERSION,
        episode_id: "ep-fixture".to_string(),
        project: "sase".to_string(),
        title: "Fixture Episode".to_string(),
        summary: "A deterministic fixture.".to_string(),
        root_source_id: "src-chat".to_string(),
        sources: vec![EpisodeSourceRefWire {
            id: "src-chat".to_string(),
            kind: "chat".to_string(),
            path: "/tmp/chat.md".to_string(),
            label: None,
            exists: true,
            size_bytes: Some(12),
            sha256: Some("aaaaaaaa".to_string()),
        }],
        nodes: vec![EpisodeNodeWire {
            id: "node-chat".to_string(),
            kind: "chat".to_string(),
            label: Some("Chat".to_string()),
            source_id: Some("src-chat".to_string()),
            metadata: node_metadata,
        }],
        edges: vec![EpisodeEdgeWire {
            id: "edge-chat-plan".to_string(),
            from_node_id: "node-chat".to_string(),
            to_node_id: "node-plan".to_string(),
            kind: "mentions".to_string(),
            evidence_ids: vec!["src-chat".to_string()],
            metadata: BTreeMap::new(),
        }],
        events: vec![EpisodeEventWire {
            id: "event-start".to_string(),
            kind: "start".to_string(),
            timestamp: Some("2026-05-26T00:00:00Z".to_string()),
            title: "Started".to_string(),
            description: None,
            evidence_ids: vec!["src-chat".to_string()],
        }],
        lessons: vec![EpisodeLessonWire {
            id: "lesson-goal".to_string(),
            kind: "goal".to_string(),
            text: "Keep lessons source-grounded.".to_string(),
            evidence_ids: vec!["src-chat".to_string()],
            source_confidence: "deterministic".to_string(),
        }],
        metadata,
    }
}

#[test]
fn rust_json_equals_python_episode_fixture() {
    let rust_value: Value = serde_json::to_value(rust_episode()).unwrap();
    let python_value: Value = serde_json::from_str(PYTHON_FIXTURE).unwrap();
    assert_eq!(rust_value, python_value);
}

#[test]
fn python_episode_fixture_deserializes_into_rust_type() {
    let episode: EpisodeWire = serde_json::from_str(PYTHON_FIXTURE).unwrap();
    assert_eq!(episode, rust_episode());
}
