//! Bounded graph materialization and deterministic text exports.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt::Write as _;

use petgraph::graph::{Graph, NodeIndex};
use petgraph::Directed;
use rusqlite::Connection;

use super::store::ArtifactStore;
use super::wire::{
    ArtifactGraphOptionsWire, ArtifactGraphWire, ArtifactLinkWire,
    ArtifactNodeWire, ARTIFACT_TOMBSTONE_LINK, ARTIFACT_TOMBSTONE_NODE,
    ARTIFACT_WIRE_SCHEMA_VERSION,
};

pub fn artifact_materialize_graph(
    store: &ArtifactStore,
    options: ArtifactGraphOptionsWire,
) -> Result<ArtifactGraphWire, String> {
    validate_schema_version(options.schema_version)?;
    if options.full_graph {
        materialize_full_graph(store.connection(), options)
    } else {
        materialize_bounded_graph(store.connection(), options)
    }
}

pub fn artifact_export_json(
    store: &ArtifactStore,
    options: ArtifactGraphOptionsWire,
) -> Result<String, String> {
    let graph = artifact_materialize_graph(store, options)?;
    serde_json::to_string_pretty(&graph).map_err(|e| e.to_string())
}

pub fn artifact_export_dot(
    store: &ArtifactStore,
    options: ArtifactGraphOptionsWire,
) -> Result<String, String> {
    let graph = artifact_materialize_graph(store, options)?;
    graph_to_dot(&graph)
}

pub fn artifact_export_mermaid(
    store: &ArtifactStore,
    options: ArtifactGraphOptionsWire,
) -> Result<String, String> {
    let graph = artifact_materialize_graph(store, options)?;
    graph_to_mermaid(&graph)
}

fn materialize_full_graph(
    conn: &Connection,
    options: ArtifactGraphOptionsWire,
) -> Result<ArtifactGraphWire, String> {
    let mut nodes = load_visible_nodes(conn)?;
    sort_nodes_by_id(&mut nodes);
    let visible_ids = nodes
        .iter()
        .map(|node| node.id.clone())
        .collect::<BTreeSet<_>>();
    let mut links = load_visible_links(conn, &options.link_types)?;
    links.retain(|link| {
        visible_ids.contains(&link.source_id)
            && visible_ids.contains(&link.target_id)
    });
    sort_links(&mut links);

    let total_node_count = nodes.len() as u64;
    let total_link_count = links.len() as u64;
    let mut truncated = false;
    if let Some(limit) = options.limit {
        let limit = limit as usize;
        if nodes.len() > limit {
            truncated = true;
            nodes.truncate(limit);
        }
    }
    let output_ids = nodes
        .iter()
        .map(|node| node.id.clone())
        .collect::<BTreeSet<_>>();
    links.retain(|link| {
        output_ids.contains(&link.source_id)
            && output_ids.contains(&link.target_id)
    });

    let _graph = build_petgraph(&nodes, &links);
    Ok(ArtifactGraphWire {
        schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
        root_id: options.root_id,
        nodes,
        links,
        node_count: total_node_count,
        link_count: total_link_count,
        truncated,
        limit: options.limit,
    })
}

fn materialize_bounded_graph(
    conn: &Connection,
    options: ArtifactGraphOptionsWire,
) -> Result<ArtifactGraphWire, String> {
    let Some(root_id) = options
        .root_id
        .clone()
        .filter(|root_id| !root_id.trim().is_empty())
    else {
        return Err(
            "artifact graph root_id is required unless full_graph is true"
                .to_string(),
        );
    };
    let nodes_by_id = load_visible_nodes(conn)?
        .into_iter()
        .map(|node| (node.id.clone(), node))
        .collect::<BTreeMap<_, _>>();
    if !nodes_by_id.contains_key(&root_id) {
        return Ok(ArtifactGraphWire {
            schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
            root_id: Some(root_id),
            nodes: Vec::new(),
            links: Vec::new(),
            node_count: 0,
            link_count: 0,
            truncated: false,
            limit: options.limit,
        });
    }

    let all_links = load_visible_links(conn, &options.link_types)?;
    let limit = options.limit.map(|value| value as usize);
    if limit == Some(0) {
        return Ok(ArtifactGraphWire {
            schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
            root_id: Some(root_id),
            nodes: Vec::new(),
            links: Vec::new(),
            node_count: 1,
            link_count: 0,
            truncated: true,
            limit: options.limit,
        });
    }

    let candidate_limit = limit.map(|value| value.saturating_add(1));
    let mut discovered = BTreeSet::new();
    let mut discovery_order = Vec::new();
    let mut queue = VecDeque::new();
    discovered.insert(root_id.clone());
    discovery_order.push(root_id.clone());
    queue.push_back((root_id.clone(), 0_u32));
    let mut truncated = false;

    'walk: while let Some((artifact_id, depth)) = queue.pop_front() {
        if options
            .max_depth
            .is_some_and(|max_depth| depth >= max_depth)
        {
            continue;
        }
        for link in incident_links(&all_links, &artifact_id, &options) {
            let neighbor_id = if link.source_id == artifact_id {
                &link.target_id
            } else {
                &link.source_id
            };
            if !nodes_by_id.contains_key(neighbor_id) {
                continue;
            }
            if discovered.insert(neighbor_id.clone()) {
                discovery_order.push(neighbor_id.clone());
                if candidate_limit.is_some_and(|candidate_limit| {
                    discovery_order.len() >= candidate_limit
                }) {
                    truncated = true;
                    break 'walk;
                }
                queue.push_back((neighbor_id.clone(), depth + 1));
            }
        }
    }

    let total_node_count = discovery_order.len() as u64;
    if let Some(limit) = limit {
        discovery_order.truncate(limit);
    }
    let output_ids = discovery_order.iter().cloned().collect::<BTreeSet<_>>();
    let mut nodes = discovery_order
        .iter()
        .filter_map(|id| nodes_by_id.get(id).cloned())
        .collect::<Vec<_>>();
    sort_nodes_by_id(&mut nodes);
    let mut links = all_links
        .into_iter()
        .filter(|link| {
            output_ids.contains(&link.source_id)
                && output_ids.contains(&link.target_id)
        })
        .collect::<Vec<_>>();
    sort_links(&mut links);

    let graph = build_petgraph(&nodes, &links);
    Ok(ArtifactGraphWire {
        schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
        root_id: Some(root_id),
        nodes,
        links,
        node_count: total_node_count,
        link_count: graph.edge_count() as u64,
        truncated,
        limit: options.limit,
    })
}

fn load_visible_nodes(
    conn: &Connection,
) -> Result<Vec<ArtifactNodeWire>, String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT node_json
            FROM artifacts a
            WHERE NOT EXISTS (
                SELECT 1 FROM manual_tombstones t
                WHERE t.tombstone_type = ?1 AND t.artifact_id = a.id
            )
            "#,
        )
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map([ARTIFACT_TOMBSTONE_NODE], |row| row.get::<_, String>(0))
        .map_err(|e| e.to_string())?;
    let mut nodes = Vec::new();
    for row in rows {
        let json = row.map_err(|e| e.to_string())?;
        nodes.push(serde_json::from_str(&json).map_err(|e| e.to_string())?);
    }
    Ok(nodes)
}

fn load_visible_links(
    conn: &Connection,
    link_types: &[String],
) -> Result<Vec<ArtifactLinkWire>, String> {
    let filter = link_types
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut stmt = conn
        .prepare(
            r#"
            SELECT l.link_json
            FROM artifact_links l
            WHERE NOT EXISTS (
                SELECT 1 FROM manual_tombstones t
                WHERE t.tombstone_type = ?1 AND t.link_id = l.id
            )
            "#,
        )
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map([ARTIFACT_TOMBSTONE_LINK], |row| row.get::<_, String>(0))
        .map_err(|e| e.to_string())?;
    let mut links = Vec::new();
    for row in rows {
        let json = row.map_err(|e| e.to_string())?;
        let link: ArtifactLinkWire =
            serde_json::from_str(&json).map_err(|e| e.to_string())?;
        if filter.is_empty() || filter.contains(link.link_type.as_str()) {
            links.push(link);
        }
    }
    sort_links(&mut links);
    Ok(links)
}

fn incident_links<'a>(
    links: &'a [ArtifactLinkWire],
    artifact_id: &str,
    options: &ArtifactGraphOptionsWire,
) -> Vec<&'a ArtifactLinkWire> {
    let mut incident = links
        .iter()
        .filter(|link| {
            (options.include_outbound && link.source_id == artifact_id)
                || (options.include_inbound && link.target_id == artifact_id)
        })
        .collect::<Vec<_>>();
    incident.sort_by(|a, b| {
        let a_neighbor = if a.source_id == artifact_id {
            a.target_id.as_str()
        } else {
            a.source_id.as_str()
        };
        let b_neighbor = if b.source_id == artifact_id {
            b.target_id.as_str()
        } else {
            b.source_id.as_str()
        };
        (a_neighbor, a.link_type.as_str(), a.id.as_str()).cmp(&(
            b_neighbor,
            b.link_type.as_str(),
            b.id.as_str(),
        ))
    });
    incident
}

fn build_petgraph(
    nodes: &[ArtifactNodeWire],
    links: &[ArtifactLinkWire],
) -> Graph<ArtifactNodeWire, ArtifactLinkWire, Directed> {
    let mut graph =
        Graph::<ArtifactNodeWire, ArtifactLinkWire, Directed>::new();
    let mut indices = BTreeMap::<String, NodeIndex>::new();
    for node in nodes {
        let index = graph.add_node(node.clone());
        indices.insert(node.id.clone(), index);
    }
    for link in links {
        let (Some(source), Some(target)) =
            (indices.get(&link.source_id), indices.get(&link.target_id))
        else {
            continue;
        };
        graph.add_edge(*source, *target, link.clone());
    }
    graph
}

fn graph_to_dot(graph: &ArtifactGraphWire) -> Result<String, String> {
    let node_ids = synthetic_node_ids(&graph.nodes);
    let mut output = String::from("digraph artifact_graph {\n");
    writeln!(
        output,
        "  graph [label=\"schema_version={} truncated={} node_count={} link_count={}\"];",
        graph.schema_version, graph.truncated, graph.node_count, graph.link_count
    )
    .map_err(|e| e.to_string())?;
    for node in &graph.nodes {
        let synthetic_id = node_ids.get(&node.id).ok_or_else(|| {
            format!("missing synthetic node id for {}", node.id)
        })?;
        writeln!(
            output,
            "  {synthetic_id} [label={} tooltip={} shape=box];",
            dot_string(&node.display_title)?,
            dot_string(&node.id)?,
        )
        .map_err(|e| e.to_string())?;
    }
    for link in &graph.links {
        let (Some(source), Some(target)) =
            (node_ids.get(&link.source_id), node_ids.get(&link.target_id))
        else {
            continue;
        };
        writeln!(
            output,
            "  {source} -> {target} [label={} tooltip={}];",
            dot_string(&link.link_type)?,
            dot_string(&link.id)?,
        )
        .map_err(|e| e.to_string())?;
    }
    output.push_str("}\n");
    Ok(output)
}

fn graph_to_mermaid(graph: &ArtifactGraphWire) -> Result<String, String> {
    let node_ids = synthetic_node_ids(&graph.nodes);
    let mut output = format!(
        "%% schema_version={} truncated={} node_count={} link_count={}\nflowchart TD\n",
        graph.schema_version, graph.truncated, graph.node_count, graph.link_count
    );
    for node in &graph.nodes {
        let synthetic_id = node_ids.get(&node.id).ok_or_else(|| {
            format!("missing synthetic node id for {}", node.id)
        })?;
        writeln!(
            output,
            "  {synthetic_id}[\"{}\"]",
            mermaid_label(&node.display_title)
        )
        .map_err(|e| e.to_string())?;
    }
    for link in &graph.links {
        let (Some(source), Some(target)) =
            (node_ids.get(&link.source_id), node_ids.get(&link.target_id))
        else {
            continue;
        };
        writeln!(
            output,
            "  {source} -->|\"{}\"| {target}",
            mermaid_label(&link.link_type)
        )
        .map_err(|e| e.to_string())?;
    }
    Ok(output)
}

fn synthetic_node_ids(nodes: &[ArtifactNodeWire]) -> BTreeMap<String, String> {
    nodes
        .iter()
        .enumerate()
        .map(|(idx, node)| (node.id.clone(), format!("n{idx}")))
        .collect()
}

fn dot_string(value: &str) -> Result<String, String> {
    serde_json::to_string(value).map_err(|e| e.to_string())
}

fn mermaid_label(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn sort_nodes_by_id(nodes: &mut [ArtifactNodeWire]) {
    nodes.sort_by(|a, b| a.id.cmp(&b.id));
}

fn sort_links(links: &mut [ArtifactLinkWire]) {
    links.sort_by(|a, b| {
        (
            a.link_type.as_str(),
            a.source_id.as_str(),
            a.target_id.as_str(),
            a.id.as_str(),
        )
            .cmp(&(
                b.link_type.as_str(),
                b.source_id.as_str(),
                b.target_id.as_str(),
                b.id.as_str(),
            ))
    });
}

fn validate_schema_version(schema_version: u32) -> Result<(), String> {
    if schema_version != ARTIFACT_WIRE_SCHEMA_VERSION {
        return Err(format!(
            "unsupported artifact schema_version {schema_version}; expected {ARTIFACT_WIRE_SCHEMA_VERSION}"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tempfile::tempdir;

    use crate::artifact::store::{
        deterministic_artifact_link_id, open_artifact_store,
        upsert_artifact_link, upsert_artifact_node,
    };
    use crate::artifact::wire::{
        ArtifactLinkUpsertWire, ArtifactNodeUpsertWire, ARTIFACT_KIND_AGENT,
        ARTIFACT_KIND_FILE, ARTIFACT_LINK_CREATED, ARTIFACT_LINK_PARENT,
        ARTIFACT_LINK_RELATED, ARTIFACT_LINK_WORKER,
        ARTIFACT_PROVENANCE_MANUAL, ARTIFACT_ROOT_ID,
    };

    use super::*;

    fn manual_node(id: &str, kind: &str, title: &str) -> ArtifactNodeWire {
        ArtifactNodeWire {
            id: id.to_string(),
            kind: kind.to_string(),
            display_title: title.to_string(),
            subtitle: None,
            provenance: ARTIFACT_PROVENANCE_MANUAL.to_string(),
            source_kind: None,
            source_id: None,
            source_version: None,
            search_text: format!("{title} {id}"),
            metadata: serde_json::Map::new(),
            created_at: None,
            updated_at: None,
        }
    }

    fn manual_link(
        link_type: &str,
        source_id: &str,
        target_id: &str,
    ) -> ArtifactLinkWire {
        ArtifactLinkWire {
            id: deterministic_artifact_link_id(link_type, source_id, target_id),
            link_type: link_type.to_string(),
            source_id: source_id.to_string(),
            target_id: target_id.to_string(),
            provenance: ARTIFACT_PROVENANCE_MANUAL.to_string(),
            source_kind: None,
            source_id_hint: None,
            source_version: None,
            metadata: serde_json::Map::new(),
            created_at: None,
            updated_at: None,
        }
    }

    fn upsert_node(store: &mut ArtifactStore, node: ArtifactNodeWire) {
        upsert_artifact_node(
            store,
            ArtifactNodeUpsertWire {
                schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
                node,
                replace_payloads: false,
            },
        )
        .unwrap();
    }

    fn upsert_link(store: &mut ArtifactStore, link: ArtifactLinkWire) {
        upsert_artifact_link(
            store,
            ArtifactLinkUpsertWire {
                schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
                link,
            },
        )
        .unwrap();
    }

    fn fixture_store(in_reverse_order: bool) -> ArtifactStore {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("artifacts.sqlite");
        let mut store = open_artifact_store(&db).unwrap();
        let nodes = [
            manual_node("/tmp", ARTIFACT_KIND_FILE, "tmp"),
            manual_node("/tmp/a.md", ARTIFACT_KIND_FILE, "a markdown"),
            manual_node("/tmp/b.md", ARTIFACT_KIND_FILE, "b markdown"),
            manual_node("agent-1", ARTIFACT_KIND_AGENT, "agent one"),
        ];
        if in_reverse_order {
            for node in nodes.into_iter().rev() {
                upsert_node(&mut store, node);
            }
        } else {
            for node in nodes {
                upsert_node(&mut store, node);
            }
        }
        upsert_link(
            &mut store,
            manual_link(ARTIFACT_LINK_PARENT, "/tmp", ARTIFACT_ROOT_ID),
        );
        upsert_link(
            &mut store,
            manual_link(ARTIFACT_LINK_PARENT, "/tmp/a.md", "/tmp"),
        );
        upsert_link(
            &mut store,
            manual_link(ARTIFACT_LINK_PARENT, "/tmp/b.md", "/tmp"),
        );
        upsert_link(
            &mut store,
            manual_link(ARTIFACT_LINK_CREATED, "agent-1", "/tmp/a.md"),
        );
        upsert_link(
            &mut store,
            manual_link(ARTIFACT_LINK_RELATED, "/tmp/b.md", "agent-1"),
        );
        store
    }

    #[test]
    fn depth_and_link_type_filters_bound_the_materialized_graph() {
        let store = fixture_store(false);

        let graph = artifact_materialize_graph(
            &store,
            ArtifactGraphOptionsWire {
                root_id: Some(ARTIFACT_ROOT_ID.to_string()),
                max_depth: Some(2),
                link_types: vec![ARTIFACT_LINK_PARENT.to_string()],
                include_inbound: true,
                include_outbound: false,
                limit: None,
                ..ArtifactGraphOptionsWire::default()
            },
        )
        .unwrap();

        let node_ids = graph
            .nodes
            .iter()
            .map(|node| node.id.as_str())
            .collect::<Vec<_>>();
        let link_types = graph
            .links
            .iter()
            .map(|link| link.link_type.as_str())
            .collect::<Vec<_>>();
        assert_eq!(node_ids, vec!["/", "/tmp", "/tmp/a.md", "/tmp/b.md"]);
        assert_eq!(link_types, vec!["parent", "parent", "parent"]);
    }

    #[test]
    fn bounded_graph_respects_direction_flags_and_worker_edges() {
        let mut store = fixture_store(false);
        upsert_node(
            &mut store,
            manual_node("bead-1", ARTIFACT_KIND_FILE, "bead one"),
        );
        upsert_link(
            &mut store,
            manual_link(ARTIFACT_LINK_WORKER, "bead-1", "agent-1"),
        );

        let inbound = artifact_materialize_graph(
            &store,
            ArtifactGraphOptionsWire {
                root_id: Some("agent-1".to_string()),
                max_depth: Some(1),
                link_types: vec![ARTIFACT_LINK_WORKER.to_string()],
                include_inbound: true,
                include_outbound: false,
                limit: None,
                ..ArtifactGraphOptionsWire::default()
            },
        )
        .unwrap();
        assert_eq!(
            inbound
                .nodes
                .iter()
                .map(|node| node.id.as_str())
                .collect::<Vec<_>>(),
            vec!["agent-1", "bead-1"]
        );
        assert_eq!(inbound.links[0].source_id, "bead-1");
        assert_eq!(inbound.links[0].target_id, "agent-1");

        let outbound = artifact_materialize_graph(
            &store,
            ArtifactGraphOptionsWire {
                root_id: Some("agent-1".to_string()),
                max_depth: Some(1),
                link_types: vec![ARTIFACT_LINK_WORKER.to_string()],
                include_inbound: false,
                include_outbound: true,
                limit: None,
                ..ArtifactGraphOptionsWire::default()
            },
        )
        .unwrap();
        assert_eq!(
            outbound
                .nodes
                .iter()
                .map(|node| node.id.as_str())
                .collect::<Vec<_>>(),
            vec!["agent-1"]
        );
        assert!(outbound.links.is_empty());
    }

    #[test]
    fn default_limits_truncate_deterministically() {
        let store = fixture_store(false);

        let graph = artifact_materialize_graph(
            &store,
            ArtifactGraphOptionsWire {
                root_id: Some(ARTIFACT_ROOT_ID.to_string()),
                include_inbound: true,
                include_outbound: true,
                max_depth: None,
                limit: Some(3),
                ..ArtifactGraphOptionsWire::default()
            },
        )
        .unwrap();

        assert!(graph.truncated);
        assert_eq!(graph.node_count, 4);
        assert_eq!(
            graph
                .nodes
                .iter()
                .map(|node| node.id.as_str())
                .collect::<Vec<_>>(),
            vec!["/", "/tmp", "/tmp/a.md"]
        );
    }

    #[test]
    fn full_graph_export_is_stable_across_insertion_order() {
        let first = fixture_store(false);
        let second = fixture_store(true);
        let options = ArtifactGraphOptionsWire {
            full_graph: true,
            root_id: Some(ARTIFACT_ROOT_ID.to_string()),
            limit: None,
            ..ArtifactGraphOptionsWire::default()
        };

        let first_json = artifact_export_json(&first, options.clone()).unwrap();
        let second_json = artifact_export_json(&second, options).unwrap();

        assert_eq!(first_json, second_json);
        let value: serde_json::Value =
            serde_json::from_str(&first_json).unwrap();
        assert_eq!(value["node_count"], json!(5));
        assert_eq!(value["link_count"], json!(5));
    }

    #[test]
    fn full_graph_limit_bounds_nodes_and_suppresses_external_links() {
        let store = fixture_store(false);

        let graph = artifact_materialize_graph(
            &store,
            ArtifactGraphOptionsWire {
                full_graph: true,
                limit: Some(2),
                ..ArtifactGraphOptionsWire::default()
            },
        )
        .unwrap();

        assert!(graph.truncated);
        assert_eq!(graph.node_count, 5);
        assert_eq!(
            graph
                .nodes
                .iter()
                .map(|node| node.id.as_str())
                .collect::<Vec<_>>(),
            vec!["/", "/tmp"]
        );
        assert_eq!(
            graph
                .links
                .iter()
                .map(|link| (link.source_id.as_str(), link.target_id.as_str()))
                .collect::<Vec<_>>(),
            vec![("/tmp", "/")]
        );
    }

    #[test]
    fn dot_and_mermaid_exports_are_stable() {
        let store = fixture_store(false);
        let options = ArtifactGraphOptionsWire {
            full_graph: true,
            root_id: Some(ARTIFACT_ROOT_ID.to_string()),
            link_types: vec![ARTIFACT_LINK_PARENT.to_string()],
            limit: None,
            ..ArtifactGraphOptionsWire::default()
        };

        let dot = artifact_export_dot(&store, options.clone()).unwrap();
        assert_eq!(
            dot,
            "digraph artifact_graph {\n  graph [label=\"schema_version=1 truncated=false node_count=5 link_count=3\"];\n  n0 [label=\"/\" tooltip=\"/\" shape=box];\n  n1 [label=\"tmp\" tooltip=\"/tmp\" shape=box];\n  n2 [label=\"a markdown\" tooltip=\"/tmp/a.md\" shape=box];\n  n3 [label=\"b markdown\" tooltip=\"/tmp/b.md\" shape=box];\n  n4 [label=\"agent one\" tooltip=\"agent-1\" shape=box];\n  n1 -> n0 [label=\"parent\" tooltip=\"link:6:parent:4:/tmp:1:/\"];\n  n2 -> n1 [label=\"parent\" tooltip=\"link:6:parent:9:/tmp/a.md:4:/tmp\"];\n  n3 -> n1 [label=\"parent\" tooltip=\"link:6:parent:9:/tmp/b.md:4:/tmp\"];\n}\n"
        );

        let mermaid = artifact_export_mermaid(&store, options).unwrap();
        assert_eq!(
            mermaid,
            "%% schema_version=1 truncated=false node_count=5 link_count=3\nflowchart TD\n  n0[\"/\"]\n  n1[\"tmp\"]\n  n2[\"a markdown\"]\n  n3[\"b markdown\"]\n  n4[\"agent one\"]\n  n1 -->|\"parent\"| n0\n  n2 -->|\"parent\"| n1\n  n3 -->|\"parent\"| n1\n"
        );
    }
}
