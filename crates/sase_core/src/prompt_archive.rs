//! Read-only inventory for canonical prompt archive documents.
//!
//! This is intentionally smaller than validation: it discovers and parses
//! `prompts/*/*.md` snapshots without checking linked plans, artifact payloads,
//! counterpart repositories, or unpublished manifests.

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::plan::artifact_link::{
    parse_sdd_plan_header_block, SddArtifactLinkKindWire,
    SddPlanHeaderSectionWire,
};

/// Wire schema for [`PromptArchiveInventoryWire`].
pub const PROMPT_ARCHIVE_INVENTORY_WIRE_SCHEMA_VERSION: u64 = 1;

/// Options for prompt archive inventory.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptArchiveInventoryRequestWire {
    /// Optional `YYYYMM` archive shard selector.
    #[serde(default)]
    pub month: Option<String>,
}

/// One discovered prompt archive Markdown document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptArchiveDocumentWire {
    /// Repository-relative path, e.g. `prompts/202609/example.md`.
    pub path: String,
    /// Archive month directory name.
    pub month: String,
    /// File stem used as the prompt locator.
    pub name: String,
    /// Original UTF-8 document content.
    pub content: String,
    /// Body after removing a valid leading plan-header block.
    pub body: String,
    /// Parsed header-block disposition.
    pub kind: SddArtifactLinkKindWire,
    /// Parsed header sections, empty when missing or unreadable.
    pub sections: Vec<SddPlanHeaderSectionWire>,
    /// Whether the original document had YAML frontmatter.
    pub has_frontmatter: bool,
    /// Whether the header block already used canonical placement/layout.
    pub canonical_layout: bool,
    /// Per-file read/UTF-8/header error. Inventory continues after errors.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parse_error: Option<String>,
}

/// Complete prompt archive inventory result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptArchiveInventoryWire {
    pub schema_version: u64,
    pub root: String,
    pub documents: Vec<PromptArchiveDocumentWire>,
}

/// Discover and parse prompt archive documents under `root`.
pub fn prompt_archive_inventory(
    root: impl AsRef<Path>,
    request: PromptArchiveInventoryRequestWire,
) -> PromptArchiveInventoryWire {
    let root = root.as_ref().to_path_buf();
    let documents = prompt_paths(&root, request.month.as_deref())
        .into_iter()
        .map(|path| document_from_path(&root, path))
        .collect();
    PromptArchiveInventoryWire {
        schema_version: PROMPT_ARCHIVE_INVENTORY_WIRE_SCHEMA_VERSION,
        root: root.to_string_lossy().into_owned(),
        documents,
    }
}

fn document_from_path(root: &Path, path: PathBuf) -> PromptArchiveDocumentWire {
    let relpath = relative_path(root, &path);
    let month = path
        .parent()
        .and_then(Path::file_name)
        .map(|value| value.to_string_lossy().into_owned())
        .unwrap_or_default();
    let name = path
        .file_stem()
        .map(|value| value.to_string_lossy().into_owned())
        .unwrap_or_default();

    let content = match fs::read(&path) {
        Ok(data) => match String::from_utf8(data) {
            Ok(content) => content,
            Err(error) => {
                return unreadable_document(
                    relpath,
                    month,
                    name,
                    format!("stream did not contain valid UTF-8: {error}"),
                );
            }
        },
        Err(error) => {
            return unreadable_document(
                relpath,
                month,
                name,
                error.to_string(),
            );
        }
    };

    let parsed = parse_sdd_plan_header_block(&content);
    let parse_error =
        (parsed.kind == SddArtifactLinkKindWire::Invalid).then(|| {
            parsed
                .reason
                .clone()
                .unwrap_or_else(|| "invalid prompt header".to_string())
        });
    PromptArchiveDocumentWire {
        path: relpath,
        month,
        name,
        content,
        body: parsed.body,
        kind: parsed.kind,
        sections: parsed.sections,
        has_frontmatter: parsed.has_frontmatter,
        canonical_layout: parsed.canonical_layout,
        parse_error,
    }
}

fn unreadable_document(
    relpath: String,
    month: String,
    name: String,
    parse_error: String,
) -> PromptArchiveDocumentWire {
    PromptArchiveDocumentWire {
        path: relpath,
        month,
        name,
        content: String::new(),
        body: String::new(),
        kind: SddArtifactLinkKindWire::Invalid,
        sections: Vec::new(),
        has_frontmatter: false,
        canonical_layout: false,
        parse_error: Some(parse_error),
    }
}

fn prompt_paths(root: &Path, month: Option<&str>) -> Vec<PathBuf> {
    let prompts_root = root.join("prompts");
    let mut paths = Vec::new();
    if let Some(month) = month {
        collect_month_prompt_paths(&prompts_root.join(month), &mut paths);
    } else if let Ok(entries) = fs::read_dir(&prompts_root) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() && file_name(&path) != Some("README.md") {
                collect_month_prompt_paths(&path, &mut paths);
            }
        }
    }
    paths.sort_by(|left, right| {
        relative_path(root, left).cmp(&relative_path(root, right))
    });
    paths
}

fn collect_month_prompt_paths(dir: &Path, paths: &mut Vec<PathBuf>) {
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if is_prompt_markdown_file(&path) {
                paths.push(path);
            }
        }
    }
}

fn is_prompt_markdown_file(path: &Path) -> bool {
    path.is_file()
        && file_name(path) != Some("README.md")
        && path.extension().and_then(|value| value.to_str()) == Some("md")
}

fn file_name(path: &Path) -> Option<&str> {
    path.file_name().and_then(|value| value.to_str())
}

fn relative_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(path: &Path, content: &str) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, content).unwrap();
    }

    fn prompt(body: &str) -> String {
        format!(
            "- **PLAN:** [202609/plan.md](https://example.test/plan)\n\
             - **AGENTS:**\n  - [alice.athena.worker](https://example.test/agent)\n\n\
             {body}\n"
        )
    }

    #[test]
    fn inventories_sorted_prompt_markdown_files() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        write(&root.join("prompts/202609/b.md"), &prompt("# B"));
        write(&root.join("prompts/202608/a.md"), &prompt("# A"));
        write(&root.join("prompts/README.md"), "# index");
        write(&root.join("prompts/202609/README.md"), "# month");
        write(&root.join("prompts/202609/ignored.txt"), "ignored");

        let inventory = prompt_archive_inventory(
            root,
            PromptArchiveInventoryRequestWire::default(),
        );

        assert_eq!(inventory.schema_version, 1);
        assert_eq!(
            inventory
                .documents
                .iter()
                .map(|document| document.path.as_str())
                .collect::<Vec<_>>(),
            vec!["prompts/202608/a.md", "prompts/202609/b.md"]
        );
        assert_eq!(inventory.documents[0].month, "202608");
        assert_eq!(inventory.documents[0].name, "a");
        assert_eq!(inventory.documents[0].body.trim(), "# A");
        assert!(inventory.documents[0].parse_error.is_none());
    }

    #[test]
    fn month_selector_limits_discovery_without_validating_name() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        write(&root.join("prompts/202608/a.md"), &prompt("# A"));
        write(&root.join("prompts/202609/b.md"), &prompt("# B"));

        let inventory = prompt_archive_inventory(
            root,
            PromptArchiveInventoryRequestWire {
                month: Some("202609".to_string()),
            },
        );

        assert_eq!(inventory.documents.len(), 1);
        assert_eq!(inventory.documents[0].path, "prompts/202609/b.md");
    }

    #[test]
    fn invalid_headers_are_per_file_parse_errors() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        write(
            &root.join("prompts/202609/broken.md"),
            "- **PLAN:** [plan](https://example.test/plan)\n\
             - **PROMPT:** [prompt](https://example.test/prompt)\n\n\
             Body\n",
        );

        let inventory = prompt_archive_inventory(
            root,
            PromptArchiveInventoryRequestWire::default(),
        );

        assert_eq!(inventory.documents.len(), 1);
        assert_eq!(
            inventory.documents[0].kind,
            SddArtifactLinkKindWire::Invalid
        );
        assert!(inventory.documents[0].parse_error.is_some());
    }
}
