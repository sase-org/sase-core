//! Compare Rust episode wire JSON against the Python dataclass shape.

use sase_core::{
    EpisodeEdgeWire, EpisodeEventWire, EpisodeImportanceFactorWire,
    EpisodeNodeWire, EpisodeSafetyWire, EpisodeSourceRefWire,
    EpisodeWeakRefsWire, EpisodeWire, EPISODE_WIRE_SCHEMA_VERSION,
};
use serde_json::Value;
use std::collections::BTreeMap;

const PYTHON_FIXTURE: &str = r#"{
    "schema_version": 2,
    "episode_id": "ep-fixture",
    "project": "sase",
    "title": "Fixture Episode",
    "summary": "A deterministic fixture.",
    "root_source_id": "src-chat",
    "component_key": "component/chat/src-chat",
    "component_root_kind": "chat",
    "status": "active",
    "importance_score": 83,
    "importance_band": "high",
    "importance_factors": [
        {
            "kind": "verification",
            "label": "Focused verification passed",
            "score": 25,
            "evidence_ids": ["src-chat"],
            "metadata": {
                "command": "cargo test"
            }
        }
    ],
    "safety": {
        "untrusted_transcript_text": true,
        "prompt_injection_phrase_hits": [],
        "redaction_hits": [],
        "private_or_missing_source_flags": ["private-chat"],
        "warnings": []
    },
    "weak_refs": {
        "changespec_names": ["memory"],
        "bead_ids": ["sase-48.1"],
        "agent_families": ["coder"],
        "touched_paths": ["src/sase/core/episode_wire.py"],
        "metadata": {
            "models": ["codex/gpt-5"]
        }
    },
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
    "lessons": [],
    "metadata": {
        "changespec": "memory"
    }
}"#;

fn rust_episode() -> EpisodeWire {
    let mut node_metadata = BTreeMap::new();
    node_metadata.insert("agent".to_string(), "coder".to_string());
    let mut metadata = BTreeMap::new();
    metadata.insert("changespec".to_string(), "memory".to_string());
    let mut factor_metadata = BTreeMap::new();
    factor_metadata.insert("command".to_string(), "cargo test".to_string());
    let mut weak_metadata = BTreeMap::new();
    weak_metadata.insert("models".to_string(), vec!["codex/gpt-5".to_string()]);

    EpisodeWire {
        schema_version: EPISODE_WIRE_SCHEMA_VERSION,
        episode_id: "ep-fixture".to_string(),
        project: "sase".to_string(),
        title: "Fixture Episode".to_string(),
        summary: "A deterministic fixture.".to_string(),
        root_source_id: "src-chat".to_string(),
        component_key: "component/chat/src-chat".to_string(),
        component_root_kind: "chat".to_string(),
        status: "active".to_string(),
        importance_score: 83,
        importance_band: "high".to_string(),
        importance_factors: vec![EpisodeImportanceFactorWire {
            kind: "verification".to_string(),
            label: "Focused verification passed".to_string(),
            score: 25,
            evidence_ids: vec!["src-chat".to_string()],
            metadata: factor_metadata,
        }],
        safety: EpisodeSafetyWire {
            untrusted_transcript_text: true,
            prompt_injection_phrase_hits: vec![],
            redaction_hits: vec![],
            private_or_missing_source_flags: vec!["private-chat".to_string()],
            warnings: vec![],
        },
        weak_refs: EpisodeWeakRefsWire {
            changespec_names: vec!["memory".to_string()],
            bead_ids: vec!["sase-48.1".to_string()],
            agent_families: vec!["coder".to_string()],
            touched_paths: vec!["src/sase/core/episode_wire.py".to_string()],
            metadata: weak_metadata,
        },
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
        lessons: vec![],
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

#[test]
fn v1_episode_fixture_deserializes_with_legacy_defaults() {
    let legacy_fixture = r#"{
        "schema_version": 1,
        "episode_id": "ep-legacy",
        "project": "sase",
        "title": "Legacy Episode",
        "summary": "A legacy lesson fixture.",
        "root_source_id": "src-chat",
        "sources": [],
        "nodes": [],
        "edges": [],
        "events": [],
        "lessons": [
            {
                "id": "lesson-goal",
                "kind": "goal",
                "text": "Legacy lessons stay parseable.",
                "evidence_ids": [],
                "source_confidence": "deterministic"
            }
        ],
        "metadata": {}
    }"#;

    let episode: EpisodeWire = serde_json::from_str(legacy_fixture).unwrap();

    assert_eq!(episode.schema_version, 1);
    assert_eq!(episode.status, "legacy");
    assert_eq!(episode.importance_band, "unknown");
    assert_eq!(episode.lessons.len(), 1);
}
