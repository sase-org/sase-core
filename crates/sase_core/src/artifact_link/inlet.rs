//! Tolerant `links:` frontmatter inlet. Only `{ref, relation, description}`
//! lists are ingested; every other shape, including mkdocs `- Label: path`,
//! is left unrecognized so callers do not touch the key.

use serde::{Deserialize, Serialize};
use serde_yaml::Value;

pub const ARTIFACT_LINK_INLET_WIRE_SCHEMA_VERSION: u64 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactLinkFrontmatterInletKindWire {
    Absent,
    Entries,
    Unrecognized,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactLinkInletEntryWire {
    #[serde(rename = "ref")]
    pub artifact_ref: String,
    pub relation: String,
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactLinkFrontmatterInletWire {
    pub schema_version: u64,
    pub kind: ArtifactLinkFrontmatterInletKindWire,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entries: Vec<ArtifactLinkInletEntryWire>,
}

fn inlet(
    kind: ArtifactLinkFrontmatterInletKindWire,
    entries: Vec<ArtifactLinkInletEntryWire>,
) -> ArtifactLinkFrontmatterInletWire {
    ArtifactLinkFrontmatterInletWire {
        schema_version: ARTIFACT_LINK_INLET_WIRE_SCHEMA_VERSION,
        kind,
        entries,
    }
}

/// Classify a document's `links:` frontmatter key without mutating it.
pub fn parse_artifact_link_frontmatter_inlet(
    document: &str,
) -> ArtifactLinkFrontmatterInletWire {
    let Some(frontmatter) = yaml_frontmatter(document) else {
        return inlet(ArtifactLinkFrontmatterInletKindWire::Absent, Vec::new());
    };
    let Ok(value) = serde_yaml::from_str::<Value>(frontmatter) else {
        return inlet(
            ArtifactLinkFrontmatterInletKindWire::Unrecognized,
            Vec::new(),
        );
    };
    let Some(mapping) = value.as_mapping() else {
        return inlet(
            ArtifactLinkFrontmatterInletKindWire::Unrecognized,
            Vec::new(),
        );
    };
    let Some(links) = mapping.get(Value::String("links".to_string())) else {
        return inlet(ArtifactLinkFrontmatterInletKindWire::Absent, Vec::new());
    };
    parse_links_value(links)
}

fn parse_links_value(value: &Value) -> ArtifactLinkFrontmatterInletWire {
    let Some(items) = value.as_sequence() else {
        return inlet(
            ArtifactLinkFrontmatterInletKindWire::Unrecognized,
            Vec::new(),
        );
    };
    let mut entries = Vec::with_capacity(items.len());
    for item in items {
        let Some(mapping) = item.as_mapping() else {
            return inlet(
                ArtifactLinkFrontmatterInletKindWire::Unrecognized,
                Vec::new(),
            );
        };
        let Some(artifact_ref) = mapping_string(mapping, "ref") else {
            return inlet(
                ArtifactLinkFrontmatterInletKindWire::Unrecognized,
                Vec::new(),
            );
        };
        let Some(relation) = mapping_string(mapping, "relation") else {
            return inlet(
                ArtifactLinkFrontmatterInletKindWire::Unrecognized,
                Vec::new(),
            );
        };
        let Some(description) = mapping_string(mapping, "description") else {
            return inlet(
                ArtifactLinkFrontmatterInletKindWire::Unrecognized,
                Vec::new(),
            );
        };
        entries.push(ArtifactLinkInletEntryWire {
            artifact_ref,
            relation,
            description,
        });
    }
    inlet(ArtifactLinkFrontmatterInletKindWire::Entries, entries)
}

fn mapping_string(mapping: &serde_yaml::Mapping, key: &str) -> Option<String> {
    mapping
        .get(Value::String(key.to_string()))
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn yaml_frontmatter(document: &str) -> Option<&str> {
    let mut lines = document.split_inclusive('\n');
    let first = lines.next()?;
    if first.trim_end_matches(['\n', '\r']) != "---" {
        return None;
    }
    let mut consumed = first.len();
    let start = consumed;
    for line in lines {
        let line_start = consumed;
        consumed += line.len();
        if line.trim_end_matches(['\n', '\r']) == "---" {
            return Some(&document[start..line_start]);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_key_is_absent() {
        let parsed = parse_artifact_link_frontmatter_inlet(
            "---\ntitle: Doc\n---\n# Body\n",
        );
        assert_eq!(parsed.kind, ArtifactLinkFrontmatterInletKindWire::Absent);
        assert!(parsed.entries.is_empty());
    }

    #[test]
    fn no_frontmatter_is_absent() {
        let parsed = parse_artifact_link_frontmatter_inlet("# Body\n");
        assert_eq!(parsed.kind, ArtifactLinkFrontmatterInletKindWire::Absent);
    }

    #[test]
    fn matching_shape_is_entries() {
        let document = "---\nlinks:\n  - ref: bead:sase-js\n    relation: implements\n    description: extends the ref contract\n---\n# Body\n";
        let parsed = parse_artifact_link_frontmatter_inlet(document);
        assert_eq!(parsed.kind, ArtifactLinkFrontmatterInletKindWire::Entries);
        assert_eq!(parsed.entries.len(), 1);
        assert_eq!(parsed.entries[0].artifact_ref, "bead:sase-js");
        assert_eq!(parsed.entries[0].relation, "implements");
        assert_eq!(parsed.entries[0].description, "extends the ref contract");
    }

    #[test]
    fn mkdocs_label_path_is_unrecognized() {
        let document = "---\nlinks:\n  - Label: path/to.md\n---\n# Body\n";
        let parsed = parse_artifact_link_frontmatter_inlet(document);
        assert_eq!(
            parsed.kind,
            ArtifactLinkFrontmatterInletKindWire::Unrecognized
        );
        assert!(parsed.entries.is_empty());
    }

    #[test]
    fn empty_list_is_entries() {
        let parsed = parse_artifact_link_frontmatter_inlet(
            "---\nlinks: []\n---\n# Body\n",
        );
        assert_eq!(parsed.kind, ArtifactLinkFrontmatterInletKindWire::Entries);
        assert!(parsed.entries.is_empty());
    }

    #[test]
    fn mapping_links_value_is_unrecognized() {
        let parsed = parse_artifact_link_frontmatter_inlet(
            "---\nlinks:\n  home: /\n---\n# Body\n",
        );
        assert_eq!(
            parsed.kind,
            ArtifactLinkFrontmatterInletKindWire::Unrecognized
        );
    }
}
