use std::path::Path;

use sase_core::editor::jinja::JinjaScopeKind;

/// Derive the Jinja assist scope for an LSP document.
///
/// - `gitcommit` language → `None` (no Jinja completion).
/// - `sase` / `sase_prompt` language ids → `Prompt`.
/// - xprompt-directory paths and memory notes → `Xprompt`.
/// - prompt temp files and all other eligible markdown → `Prompt`.
pub fn jinja_scope_for_document(
    source_path: Option<&Path>,
    language_id: &str,
) -> Option<JinjaScopeKind> {
    if language_id == "gitcommit" {
        return None;
    }
    if matches!(language_id, "sase" | "sase_prompt") {
        return Some(JinjaScopeKind::Prompt);
    }
    let Some(path) = source_path else {
        return Some(JinjaScopeKind::Prompt);
    };
    if path.components().any(|component| {
        matches!(
            component.as_os_str().to_str(),
            Some("xprompts" | ".xprompts" | "default_xprompts")
        )
    }) {
        return Some(JinjaScopeKind::Xprompt);
    }
    if is_memory_note_path(path) {
        return Some(JinjaScopeKind::Xprompt);
    }
    Some(JinjaScopeKind::Prompt)
}

fn is_memory_note_path(path: &Path) -> bool {
    if path.extension().and_then(|ext| ext.to_str()) != Some("md") {
        return false;
    }
    path.parent()
        .and_then(Path::file_name)
        .and_then(|name| name.to_str())
        == Some(sase_core::MEMORY_NAMESPACE_SEGMENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn path(value: &str) -> PathBuf {
        PathBuf::from(value)
    }

    #[test]
    fn gitcommit_never_gets_jinja_scope() {
        assert_eq!(
            jinja_scope_for_document(
                Some(path("/repo/prompt.md").as_path()),
                "gitcommit"
            ),
            None
        );
    }

    #[test]
    fn sase_language_ids_use_prompt_scope() {
        for language in ["sase", "sase_prompt"] {
            assert_eq!(
                jinja_scope_for_document(None, language),
                Some(JinjaScopeKind::Prompt)
            );
        }
    }

    #[test]
    fn xprompt_directories_use_xprompt_scope() {
        for candidate in [
            "/repo/xprompts/review.md",
            "/repo/.xprompts/local.md",
            "/repo/default_xprompts/help.md",
        ] {
            assert_eq!(
                jinja_scope_for_document(
                    Some(path(candidate).as_path()),
                    "markdown"
                ),
                Some(JinjaScopeKind::Xprompt),
                "{candidate}"
            );
        }
    }

    #[test]
    fn memory_notes_use_xprompt_scope() {
        assert_eq!(
            jinja_scope_for_document(
                Some(path("/repo/sase/memory/note.md").as_path()),
                "markdown"
            ),
            Some(JinjaScopeKind::Xprompt)
        );
    }

    #[test]
    fn other_markdown_uses_prompt_scope() {
        assert_eq!(
            jinja_scope_for_document(
                Some(path("/tmp/sase_prompt_abc.md").as_path()),
                "markdown"
            ),
            Some(JinjaScopeKind::Prompt)
        );
        assert_eq!(
            jinja_scope_for_document(
                Some(path("/repo/notes.md").as_path()),
                "markdown"
            ),
            Some(JinjaScopeKind::Prompt)
        );
    }
}
