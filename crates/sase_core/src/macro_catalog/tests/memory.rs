use super::*;

#[test]
fn memory_notes_load_as_namespaced_no_argument_xprompt_memories() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    let root = temp.path().join("workspace");
    fs::create_dir_all(&home).unwrap();
    write_memory_note(
            &root,
            "glossary.md",
            "---\ntype: short\nparent: AGENTS.md\ndescription: SASE terms\n---\nGlossary body\n",
        );
    write_memory_note(
        &root,
        "tui_perf.md",
        "---\ntype: long\ndescription: TUI performance\n---\nPerf body\n",
    );
    // Generated documentation and nested assets are not catalog entries.
    write_memory_note(&root, "README.md", "---\ntype: long\n---\nIndex\n");
    fs::create_dir_all(root.join("sase/memory/assets")).unwrap();
    fs::write(
        root.join("sase/memory/assets/nested.md"),
        "---\ntype: long\n---\nNested\n",
    )
    .unwrap();

    let loader = CatalogLoader {
        root_dir: Some(root.clone()),
        home_dir: Some(home),
        ..CatalogLoader::default()
    };
    let xprompts = loader.load_all_xprompts(None).unwrap();

    assert_eq!(
        xprompts.keys().collect::<Vec<_>>(),
        vec!["memory/glossary", "memory/tui_perf"]
    );
    let glossary = &xprompts["memory/glossary"];
    // Frontmatter is stripped, description and tier are preserved, and an
    // xprompt memory takes no arguments.
    assert_eq!(glossary.content, "Glossary body");
    assert_eq!(glossary.description.as_deref(), Some("SASE terms"));
    assert_eq!(glossary.memory_type, Some(MemoryTierWire::Core));
    assert!(glossary.inputs.is_empty());
    assert!(!glossary.is_skill && glossary.skill_name.is_none());
    assert_eq!(
        xprompts["memory/tui_perf"].memory_type,
        Some(MemoryTierWire::Reference)
    );
    // The `memory/` prefix is mandatory: there is no bare alias.
    assert!(!xprompts.contains_key("glossary"));
    assert!(loader.placement_warnings().is_empty());
}
#[test]
fn memory_entries_render_as_memory_with_a_navigable_definition() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    write_memory_note(
        root,
        "glossary.md",
        "---\ntype: long\ndescription: SASE terms\n---\nGlossary body\n",
    );

    let response = load_editor_xprompt_catalog(
        &request(),
        &XpromptCatalogLoadOptions::new(Some(root.to_path_buf())),
    )
    .unwrap();
    let entry = response
        .entries
        .iter()
        .find(|entry| entry.name == "memory/glossary")
        .unwrap();

    assert_eq!(entry.kind.as_deref(), Some("memory"));
    assert_eq!(entry.memory_type, Some(MemoryTierWire::Reference));
    assert_eq!(entry.insertion.as_deref(), Some("#memory/glossary"));
    assert_eq!(entry.input_signature, None);
    // A memory entry is never a slash skill.
    assert!(!entry.is_skill && entry.skill_name.is_none());
    // Definition navigation lands on the note itself.
    assert_eq!(
        entry.definition_path.as_deref().map(Path::new),
        Some(
            root.join("sase/memory/glossary.md")
                .canonicalize()
                .unwrap()
                .as_path()
        )
    );
    // The stats projection counts every xprompt memory in the catalog,
    // including whatever the ambient home root contributes.
    assert_eq!(
        response.stats.memory_count,
        response
            .entries
            .iter()
            .filter(|entry| entry.memory_type.is_some())
            .count() as u64
    );
    assert!(response.stats.memory_count >= 1);
}
#[test]
fn project_memory_shadows_home_memory_of_the_same_stem() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    let root = temp.path().join("workspace");
    write_memory_note(&home, "glossary.md", "---\ntype: short\n---\nHome\n");
    write_memory_note(
        &home,
        "obsidian.md",
        "---\ntype: long\n---\nHome only\n",
    );
    write_memory_note(&root, "glossary.md", "---\ntype: short\n---\nProject\n");

    let loader = CatalogLoader {
        root_dir: Some(root),
        home_dir: Some(home),
        ..CatalogLoader::default()
    };
    let xprompts = loader.load_all_xprompts(None).unwrap();

    assert_eq!(xprompts["memory/glossary"].content, "Project");
    // Home still supplies notes the project does not define.
    assert_eq!(xprompts["memory/obsidian"].content, "Home only");
}
#[test]
fn explicit_project_selection_picks_that_projects_memory_only() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    let root = temp.path().join("workspace");
    let other = temp.path().join("other");
    fs::create_dir_all(&home).unwrap();
    write_memory_note(&root, "glossary.md", "---\ntype: short\n---\nRoot\n");
    write_memory_note(&other, "glossary.md", "---\ntype: short\n---\nOther\n");
    write_memory_note(&other, "only.md", "---\ntype: long\n---\nOther only\n");

    let loader = CatalogLoader {
        root_dir: Some(root),
        home_dir: Some(home),
        known_workspaces: BTreeMap::from([(
            "other".to_string(),
            other.clone(),
        )]),
        ..CatalogLoader::default()
    };

    // Selecting a registered project changes which root supplies
    // `#memory/foo`; the reference name never gains a project prefix.
    let selected = loader.load_all_xprompts(Some("other")).unwrap();
    assert_eq!(selected["memory/glossary"].content, "Other");
    assert!(selected.contains_key("memory/only"));

    // The ambient catalog never mixes another project's memory in.
    let ambient = loader.load_all_xprompts(None).unwrap();
    assert_eq!(ambient["memory/glossary"].content, "Root");
    assert!(!ambient.contains_key("memory/only"));
}
#[test]
fn split_canonical_and_legacy_memory_state_is_a_collision_error() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("workspace");
    write_memory_note(&root, "glossary.md", "---\ntype: short\n---\nBody\n");
    fs::create_dir_all(root.join("memory")).unwrap();
    fs::write(
        root.join("memory/glossary.md"),
        "---\ntype: short\n---\nLegacy\n",
    )
    .unwrap();

    let loader = CatalogLoader {
        root_dir: Some(root),
        ..CatalogLoader::default()
    };
    let error = loader.load_all_xprompts(None).unwrap_err();

    let XpromptCatalogLoadError::LayoutCollision(message) = error else {
        panic!("expected a memory layout collision");
    };
    assert!(message.contains("project memory"), "{message}");
    assert!(
        message.contains("migrate to the canonical path"),
        "{message}"
    );
}
#[test]
fn invalid_memory_notes_become_diagnostics_instead_of_silent_gaps() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("workspace");
    write_memory_note(&root, "untyped.md", "---\nparent: AGENTS.md\n---\nBody");
    write_memory_note(&root, "keyworded.md", "---\ntype: dynamic\n---\nBody");
    write_memory_note(&root, "bad-stem.md", "---\ntype: long\n---\nBody");
    write_memory_note(&root, "ok.md", "---\ntype: long\n---\nBody");

    let loader = CatalogLoader {
        root_dir: Some(root),
        ..CatalogLoader::default()
    };
    let xprompts = loader.load_all_xprompts(None).unwrap();

    assert_eq!(xprompts.keys().collect::<Vec<_>>(), vec!["memory/ok"]);
    let warnings = loader.placement_warnings().join("\n");
    assert!(warnings.contains("untyped.md"), "{warnings}");
    assert!(warnings.contains("keyworded.md"), "{warnings}");
    assert!(warnings.contains("#memory/bad-stem"), "{warnings}");
}
#[test]
fn ordinary_definitions_cannot_claim_the_reserved_memory_namespace() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("workspace");
    fs::create_dir_all(root.join("sase/xprompts")).unwrap();
    fs::write(
        root.join("sase/xprompts/imposter.md"),
        "---\nname: memory/glossary\n---\nNot a memory note",
    )
    .unwrap();
    fs::write(
        root.join("sase/sase.yml"),
        "xprompts:\n  memory/tui_perf:\n    content: Also not a memory note\n",
    )
    .unwrap();
    write_memory_note(&root, "glossary.md", "---\ntype: short\n---\nReal\n");

    let loader = CatalogLoader {
        root_dir: Some(root),
        ..CatalogLoader::default()
    };
    let xprompts = loader.load_all_xprompts(None).unwrap();

    // Load order never decides the winner: the colliding definitions are
    // rejected outright, and only the real memory note is reachable.
    assert_eq!(xprompts.keys().collect::<Vec<_>>(), vec!["memory/glossary"]);
    assert_eq!(xprompts["memory/glossary"].content, "Real");
    let warnings = loader.placement_warnings().join("\n");
    assert!(warnings.contains("imposter.md"), "{warnings}");
    assert!(warnings.contains("memory/tui_perf"), "{warnings}");
}
