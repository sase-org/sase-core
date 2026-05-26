pub mod wire;

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub use wire::{
    EpisodeBuildReportWire, EpisodeBuildRequestWire, EpisodeEdgeWire,
    EpisodeEventWire, EpisodeLessonWire, EpisodeNodeWire, EpisodeSourceRefWire,
    EpisodeSourceVerifyResultWire, EpisodeStorageIndexRowWire,
    EpisodeVerifyReportWire, EpisodeWire, EPISODE_WIRE_SCHEMA_VERSION,
};

pub fn canonical_episode_json(
    episode: &EpisodeWire,
) -> Result<String, serde_json::Error> {
    let canonical = canonicalize_episode(episode);
    let mut serialized = serde_json::to_string(&canonical)?;
    serialized.push('\n');
    Ok(serialized)
}

pub fn stable_source_id(source: &EpisodeSourceRefWire) -> String {
    format!("src-{}", hash_json_value(&source_identity_value(source)))
}

pub fn stable_episode_id(
    project: &str,
    root_source_id: &str,
    sources: &[EpisodeSourceRefWire],
) -> String {
    let mut canonical_sources = sources.to_vec();
    canonical_sources.sort_by(source_sort_key);
    let source_values = canonical_sources
        .iter()
        .map(source_identity_value)
        .collect::<Vec<_>>();
    let value = json!({
        "project": project,
        "root_source_id": root_source_id,
        "sources": source_values,
    });
    format!("ep-{}", hash_json_value(&value))
}

pub fn verify_episode_sources(
    episode_id: &str,
    sources: &[EpisodeSourceRefWire],
) -> EpisodeVerifyReportWire {
    let mut sorted_sources = sources.to_vec();
    sorted_sources.sort_by(source_sort_key);

    let mut results = Vec::with_capacity(sorted_sources.len());
    let mut ok_count = 0;
    let mut missing_count = 0;
    let mut changed_count = 0;

    for source in sorted_sources {
        let result = verify_one_source(&source);
        match result.status.as_str() {
            "ok" => ok_count += 1,
            "missing" => missing_count += 1,
            "changed" => changed_count += 1,
            _ => changed_count += 1,
        }
        results.push(result);
    }

    EpisodeVerifyReportWire {
        schema_version: EPISODE_WIRE_SCHEMA_VERSION,
        episode_id: episode_id.to_string(),
        ok: missing_count == 0 && changed_count == 0,
        source_count: results.len() as u64,
        ok_count,
        missing_count,
        changed_count,
        results,
    }
}

fn canonicalize_episode(episode: &EpisodeWire) -> EpisodeWire {
    let mut canonical = episode.clone();
    canonical.sources.sort_by(source_sort_key);
    canonical.nodes.sort_by(|a, b| a.id.cmp(&b.id));
    canonical.edges.sort_by(edge_sort_key);
    canonical.events.sort_by(event_sort_key);
    canonical.lessons.sort_by(|a, b| a.id.cmp(&b.id));

    for edge in &mut canonical.edges {
        edge.evidence_ids.sort();
    }
    for event in &mut canonical.events {
        event.evidence_ids.sort();
    }
    for lesson in &mut canonical.lessons {
        lesson.evidence_ids.sort();
    }

    canonical
}

fn source_sort_key(
    a: &EpisodeSourceRefWire,
    b: &EpisodeSourceRefWire,
) -> std::cmp::Ordering {
    (&a.id, &a.kind, &a.path, &a.sha256, &a.size_bytes, &a.exists).cmp(&(
        &b.id,
        &b.kind,
        &b.path,
        &b.sha256,
        &b.size_bytes,
        &b.exists,
    ))
}

fn edge_sort_key(
    a: &EpisodeEdgeWire,
    b: &EpisodeEdgeWire,
) -> std::cmp::Ordering {
    (&a.id, &a.from_node_id, &a.to_node_id, &a.kind).cmp(&(
        &b.id,
        &b.from_node_id,
        &b.to_node_id,
        &b.kind,
    ))
}

fn event_sort_key(
    a: &EpisodeEventWire,
    b: &EpisodeEventWire,
) -> std::cmp::Ordering {
    (&a.timestamp, &a.id, &a.kind, &a.title).cmp(&(
        &b.timestamp,
        &b.id,
        &b.kind,
        &b.title,
    ))
}

fn source_identity_value(source: &EpisodeSourceRefWire) -> Value {
    let mut obj = BTreeMap::new();
    obj.insert("exists".to_string(), json!(source.exists));
    obj.insert("kind".to_string(), json!(source.kind));
    obj.insert("path".to_string(), json!(source.path));
    obj.insert("sha256".to_string(), json!(source.sha256));
    obj.insert("size_bytes".to_string(), json!(source.size_bytes));
    Value::Object(obj.into_iter().collect())
}

fn hash_json_value(value: &Value) -> String {
    let bytes = serde_json::to_vec(value).expect("serializing JSON value");
    let digest = Sha256::digest(bytes);
    hex::encode(digest)[..24].to_string()
}

fn verify_one_source(
    source: &EpisodeSourceRefWire,
) -> EpisodeSourceVerifyResultWire {
    let path = Path::new(&source.path);
    let (actual_exists, actual_size_bytes, actual_sha256) =
        actual_source_fingerprint(path);

    let status = if source.exists != actual_exists {
        if source.exists {
            "missing"
        } else {
            "changed"
        }
    } else if !source.exists {
        "ok"
    } else if expected_fingerprint_matches(
        source.size_bytes,
        source.sha256.as_deref(),
        actual_size_bytes,
        actual_sha256.as_deref(),
    ) {
        "ok"
    } else {
        "changed"
    };

    EpisodeSourceVerifyResultWire {
        source_id: if source.id.is_empty() {
            stable_source_id(source)
        } else {
            source.id.clone()
        },
        path: source.path.clone(),
        expected_exists: source.exists,
        actual_exists,
        expected_size_bytes: source.size_bytes,
        actual_size_bytes,
        expected_sha256: source.sha256.clone(),
        actual_sha256,
        status: status.to_string(),
    }
}

fn actual_source_fingerprint(
    path: &Path,
) -> (bool, Option<u64>, Option<String>) {
    let Ok(metadata) = fs::metadata(path) else {
        return (false, None, None);
    };
    if !metadata.is_file() {
        return (true, Some(metadata.len()), None);
    }
    match fs::read(path) {
        Ok(bytes) => {
            let digest = Sha256::digest(&bytes);
            (true, Some(metadata.len()), Some(hex::encode(digest)))
        }
        Err(_) => (true, Some(metadata.len()), None),
    }
}

fn expected_fingerprint_matches(
    expected_size: Option<u64>,
    expected_sha: Option<&str>,
    actual_size: Option<u64>,
    actual_sha: Option<&str>,
) -> bool {
    if let Some(expected) = expected_size {
        if Some(expected) != actual_size {
            return false;
        }
    }
    if let Some(expected) = expected_sha {
        if Some(expected) != actual_sha {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn source(
        id: &str,
        kind: &str,
        path: &str,
        size_bytes: u64,
        sha256: &str,
    ) -> EpisodeSourceRefWire {
        EpisodeSourceRefWire {
            id: id.to_string(),
            kind: kind.to_string(),
            path: path.to_string(),
            label: None,
            exists: true,
            size_bytes: Some(size_bytes),
            sha256: Some(sha256.to_string()),
        }
    }

    #[test]
    fn canonical_episode_json_sorts_collections() {
        let episode = EpisodeWire {
            schema_version: EPISODE_WIRE_SCHEMA_VERSION,
            episode_id: "ep-test".to_string(),
            project: "sase".to_string(),
            title: "Episode".to_string(),
            summary: "Summary".to_string(),
            root_source_id: "src-b".to_string(),
            sources: vec![
                source("src-b", "chat", "b.md", 2, "bbb"),
                source("src-a", "plan", "a.md", 1, "aaa"),
            ],
            nodes: vec![
                EpisodeNodeWire {
                    id: "node-b".to_string(),
                    kind: "chat".to_string(),
                    ..EpisodeNodeWire::default()
                },
                EpisodeNodeWire {
                    id: "node-a".to_string(),
                    kind: "plan".to_string(),
                    ..EpisodeNodeWire::default()
                },
            ],
            edges: vec![EpisodeEdgeWire {
                id: "edge-a".to_string(),
                from_node_id: "node-a".to_string(),
                to_node_id: "node-b".to_string(),
                kind: "links".to_string(),
                evidence_ids: vec!["src-b".to_string(), "src-a".to_string()],
                metadata: BTreeMap::new(),
            }],
            events: vec![
                EpisodeEventWire {
                    id: "event-b".to_string(),
                    kind: "finish".to_string(),
                    timestamp: Some("2026-05-02T00:00:00Z".to_string()),
                    title: "Finish".to_string(),
                    description: None,
                    evidence_ids: vec![],
                },
                EpisodeEventWire {
                    id: "event-a".to_string(),
                    kind: "start".to_string(),
                    timestamp: Some("2026-05-01T00:00:00Z".to_string()),
                    title: "Start".to_string(),
                    description: None,
                    evidence_ids: vec![],
                },
            ],
            lessons: vec![EpisodeLessonWire {
                id: "lesson-a".to_string(),
                kind: "goal".to_string(),
                text: "Goal".to_string(),
                evidence_ids: vec!["src-b".to_string(), "src-a".to_string()],
                source_confidence: "deterministic".to_string(),
            }],
            metadata: BTreeMap::new(),
        };

        let value: Value =
            serde_json::from_str(&canonical_episode_json(&episode).unwrap())
                .unwrap();
        assert_eq!(value["sources"][0]["id"], "src-a");
        assert_eq!(value["nodes"][0]["id"], "node-a");
        assert_eq!(value["events"][0]["id"], "event-a");
        assert_eq!(
            value["edges"][0]["evidence_ids"],
            json!(["src-a", "src-b"])
        );
        assert!(canonical_episode_json(&episode).unwrap().ends_with('\n'));
    }

    #[test]
    fn stable_ids_ignore_source_order() {
        let a = source("src-explicit-a", "chat", "chat.md", 4, "abc");
        let b = source("src-explicit-b", "plan", "plan.md", 7, "def");

        assert_eq!(
            stable_episode_id("sase", "src-root", &[a.clone(), b.clone()]),
            stable_episode_id("sase", "src-root", &[b.clone(), a.clone()])
        );
        assert_eq!(stable_source_id(&a), stable_source_id(&a));
        assert_ne!(stable_source_id(&a), stable_source_id(&b));
    }

    #[test]
    fn verify_episode_sources_reports_missing_and_changed_sources() {
        let dir = tempdir().unwrap();
        let ok_path = dir.path().join("ok.txt");
        fs::write(&ok_path, b"ok\n").unwrap();
        let changed_path = dir.path().join("changed.txt");
        fs::write(&changed_path, b"new\n").unwrap();

        let ok_sha = hex::encode(Sha256::digest(b"ok\n"));
        let sources = vec![
            EpisodeSourceRefWire {
                id: "src-ok".to_string(),
                kind: "artifact".to_string(),
                path: ok_path.to_string_lossy().to_string(),
                label: None,
                exists: true,
                size_bytes: Some(3),
                sha256: Some(ok_sha),
            },
            EpisodeSourceRefWire {
                id: "src-missing".to_string(),
                kind: "artifact".to_string(),
                path: dir
                    .path()
                    .join("missing.txt")
                    .to_string_lossy()
                    .to_string(),
                label: None,
                exists: true,
                size_bytes: Some(1),
                sha256: Some("bad".to_string()),
            },
            EpisodeSourceRefWire {
                id: "src-changed".to_string(),
                kind: "artifact".to_string(),
                path: changed_path.to_string_lossy().to_string(),
                label: None,
                exists: true,
                size_bytes: Some(3),
                sha256: Some("old".to_string()),
            },
        ];

        let report = verify_episode_sources("ep-test", &sources);
        assert!(!report.ok);
        assert_eq!(report.ok_count, 1);
        assert_eq!(report.missing_count, 1);
        assert_eq!(report.changed_count, 1);
    }
}
