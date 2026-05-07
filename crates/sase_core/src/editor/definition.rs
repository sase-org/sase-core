use std::path::{Path, PathBuf};

use super::token::{
    extract_token_at_position, slash_skill_reference_name,
    xprompt_reference_name, DocumentSnapshot,
};
use super::wire::{EditorPosition, EditorRange, XpromptAssistEntry};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefinitionTarget {
    pub path: PathBuf,
    pub range: Option<EditorRange>,
}

pub fn definition_at_position(
    document: &DocumentSnapshot,
    position: EditorPosition,
    entries: &[XpromptAssistEntry],
) -> Option<DefinitionTarget> {
    let token = extract_token_at_position(document, position)?;
    let entry = entry_for_token(&token.text, entries)?;
    let path = definition_path(entry.definition_path.as_deref()?)?;
    Some(DefinitionTarget { path, range: None })
}

fn entry_for_token<'a>(
    token: &str,
    entries: &'a [XpromptAssistEntry],
) -> Option<&'a XpromptAssistEntry> {
    if let Some(name) = xprompt_reference_name(token) {
        return entries.iter().find(|entry| entry.name == name);
    }
    if let Some(name) = slash_skill_reference_name(token) {
        return entries
            .iter()
            .find(|entry| entry.is_skill && entry.name == name);
    }
    None
}

fn definition_path(raw: &str) -> Option<PathBuf> {
    let raw = raw.trim();
    if raw.is_empty()
        || raw.contains('\n')
        || raw.contains('\r')
        || looks_like_uri(raw)
    {
        return None;
    }
    let path = Path::new(raw);
    let canonical = path.canonicalize().ok()?;
    canonical.is_file().then_some(canonical)
}

fn looks_like_uri(value: &str) -> bool {
    if value.starts_with("file:") || value.contains("://") {
        return true;
    }
    let Some(colon) = value.find(':') else {
        return false;
    };
    let scheme = &value[..colon];
    if scheme.len() == 1 && cfg!(windows) {
        return false;
    }
    !scheme.is_empty()
        && scheme.chars().all(|ch| {
            ch.is_ascii_alphanumeric() || matches!(ch, '+' | '-' | '.')
        })
        && scheme
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_alphabetic())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::*;
    use crate::editor::wire::XpromptInputHint;

    fn pos(character: u32) -> EditorPosition {
        EditorPosition { line: 0, character }
    }

    fn entry(
        name: &str,
        insertion: &str,
        definition_path: Option<String>,
        is_skill: bool,
    ) -> XpromptAssistEntry {
        XpromptAssistEntry {
            name: name.to_string(),
            display_label: name.to_string(),
            insertion: insertion.to_string(),
            reference_prefix: if insertion.starts_with("#!") {
                "#!".to_string()
            } else {
                "#".to_string()
            },
            kind: None,
            source_bucket: "test".to_string(),
            project: None,
            tags: Vec::new(),
            input_signature: None,
            inputs: Vec::<XpromptInputHint>::new(),
            content_preview: None,
            description: None,
            source_path_display: Some("display-only.md".to_string()),
            definition_path,
            is_skill,
        }
    }

    #[test]
    fn resolves_inline_and_standalone_xprompt_definitions() {
        let temp = tempdir().unwrap();
        let inline_path = temp.path().join("review.md");
        let standalone_path = temp.path().join("workflow.md");
        fs::write(&inline_path, "review").unwrap();
        fs::write(&standalone_path, "workflow").unwrap();
        let entries = vec![
            entry(
                "review",
                "#review",
                Some(inline_path.to_string_lossy().into_owned()),
                false,
            ),
            entry(
                "workflow",
                "#!workflow",
                Some(standalone_path.to_string_lossy().into_owned()),
                false,
            ),
        ];

        let inline = definition_at_position(
            &DocumentSnapshot::new("run #review now"),
            pos(6),
            &entries,
        )
        .unwrap();
        let standalone = definition_at_position(
            &DocumentSnapshot::new("#!workflow"),
            pos(3),
            &entries,
        )
        .unwrap();

        assert_eq!(inline.path, inline_path.canonicalize().unwrap());
        assert_eq!(standalone.path, standalone_path.canonicalize().unwrap());
        assert_eq!(inline.range, None);
    }

    #[test]
    fn resolves_namespaced_shorthand_and_slash_skill_definitions() {
        let temp = tempdir().unwrap();
        let namespaced_path = temp.path().join("ns.md");
        let skill_path = temp.path().join("skill.md");
        fs::write(&namespaced_path, "namespaced").unwrap();
        fs::write(&skill_path, "skill").unwrap();
        let entries = vec![
            entry(
                "bd/work_phase_bead",
                "#bd/work_phase_bead",
                Some(namespaced_path.to_string_lossy().into_owned()),
                false,
            ),
            entry(
                "sase_plan",
                "#sase_plan",
                Some(skill_path.to_string_lossy().into_owned()),
                true,
            ),
        ];

        let namespaced = definition_at_position(
            &DocumentSnapshot::new("#bd__work_phase_bead"),
            pos(8),
            &entries,
        )
        .unwrap();
        let slash = definition_at_position(
            &DocumentSnapshot::new("/sase_plan"),
            pos(3),
            &entries,
        )
        .unwrap();

        assert_eq!(namespaced.path, namespaced_path.canonicalize().unwrap());
        assert_eq!(slash.path, skill_path.canonicalize().unwrap());
    }

    #[test]
    fn returns_none_for_missing_entries_or_source_targets() {
        let temp = tempdir().unwrap();
        let missing_path = temp.path().join("missing.md");
        let entries = vec![
            entry("known", "#known", None, false),
            entry(
                "gone",
                "#gone",
                Some(missing_path.to_string_lossy().into_owned()),
                false,
            ),
        ];

        assert_eq!(
            definition_at_position(
                &DocumentSnapshot::new("#unknown"),
                pos(3),
                &entries,
            ),
            None
        );
        assert_eq!(
            definition_at_position(
                &DocumentSnapshot::new("#known"),
                pos(3),
                &entries,
            ),
            None
        );
        assert_eq!(
            definition_at_position(
                &DocumentSnapshot::new("#gone"),
                pos(3),
                &entries
            ),
            None
        );
    }

    #[test]
    fn validates_local_file_definition_paths_conservatively() {
        let temp = tempdir().unwrap();
        let source_path = temp.path().join("global.md");
        let directory_path = temp.path().join("dir");
        fs::write(&source_path, "global").unwrap();
        fs::create_dir(&directory_path).unwrap();

        assert_eq!(
            definition_path(&source_path.to_string_lossy()).unwrap(),
            source_path.canonicalize().unwrap()
        );
        assert_eq!(definition_path(""), None);
        assert_eq!(definition_path("file:///tmp/source.md"), None);
        assert_eq!(definition_path("plugin:module/name"), None);
        assert_eq!(definition_path("/tmp/source.md\n"), None);
        assert_eq!(definition_path(&directory_path.to_string_lossy()), None);
    }
}
