use super::super::types::MacroCatalogLoadError;
use super::*;

fn loader_with_config(
    config_path: &Path,
    home: &Path,
    accept_legacy: bool,
) -> CatalogLoader {
    // Pin home_dir to an isolated temp home so ambient ~/.config never
    // leaks into policy assertions.
    CatalogLoader {
        home_dir: Some(home.to_path_buf()),
        default_config_path: Some(config_path.to_path_buf()),
        accept_legacy_xprompt_names: accept_legacy,
        ..CatalogLoader::default()
    }
}

fn isolated_home(temp: &tempfile::TempDir) -> PathBuf {
    let home = temp.path().join("home");
    fs::create_dir_all(&home).unwrap();
    home
}

fn write_config(dir: &Path, body: &str) -> PathBuf {
    fs::create_dir_all(dir).unwrap();
    let path = dir.join("default_config.yml");
    fs::write(&path, body).unwrap();
    path
}

#[test]
fn config_macros_key_loads_like_retired_xprompts_key() {
    let temp = tempfile::tempdir().unwrap();
    let home = isolated_home(&temp);
    let new_path = write_config(
        &temp.path().join("new"),
        "macros:\n  ship:\n    content: Ship it\n",
    );
    let new_macros = loader_with_config(&new_path, &home, true)
        .load_config_macros(None)
        .unwrap();
    assert_eq!(new_macros["ship"].content, "Ship it");

    let old_path = write_config(
        &temp.path().join("old"),
        "xprompts:\n  ship:\n    content: Ship it\n",
    );
    let old_macros = loader_with_config(&old_path, &home, true)
        .load_config_macros(None)
        .unwrap();
    assert_eq!(old_macros["ship"].content, "Ship it");
}

#[test]
fn config_both_keys_is_an_error_even_when_empty() {
    let temp = tempfile::tempdir().unwrap();
    let home = isolated_home(&temp);
    for body in [
        "xprompts:\n  a:\n    content: A\nmacros:\n  b:\n    content: B\n",
        "xprompts:\nmacros:\n",
        "xprompts: null\nmacros: null\n",
    ] {
        let path = write_config(&temp.path().join("both"), body);
        let error = loader_with_config(&path, &home, true)
            .load_config_macros(None)
            .expect_err("both authored keys must conflict");
        assert!(
            matches!(error, MacroCatalogLoadError::DuplicateAuthoredKeys(_)),
            "{error:?}"
        );
        assert!(error.to_string().contains("macros"), "{error}");
    }
}

#[test]
fn config_retired_key_loads_under_both_policies() {
    let temp = tempfile::tempdir().unwrap();
    let home = isolated_home(&temp);
    let old_path = write_config(
        &temp.path().join("old"),
        "xprompts:\n  ship:\n    content: Ship it\n",
    );
    // The retired switch no longer changes policy: both values accept.
    for accept in [false, true] {
        let macros = loader_with_config(&old_path, &home, accept)
            .load_config_macros(None)
            .unwrap();
        assert_eq!(macros["ship"].content, "Ship it");
    }

    let new_path = write_config(
        &temp.path().join("new"),
        "macros:\n  ship:\n    content: Ship it\n",
    );
    let macros = loader_with_config(&new_path, &home, false)
        .load_config_macros(None)
        .unwrap();
    assert_eq!(macros["ship"].content, "Ship it");
}

#[test]
fn malformed_macros_section_is_an_error_while_legacy_stays_lenient() {
    let temp = tempfile::tempdir().unwrap();
    let home = isolated_home(&temp);
    let bad_path = write_config(&temp.path().join("bad"), "macros: oops\n");
    let error = loader_with_config(&bad_path, &home, true)
        .load_config_macros(None)
        .expect_err("malformed canonical section must error");
    assert!(
        matches!(error, MacroCatalogLoadError::MalformedAuthoredSection(_)),
        "{error:?}"
    );

    let legacy_path =
        write_config(&temp.path().join("legacy"), "xprompts: oops\n");
    let macros = loader_with_config(&legacy_path, &home, true)
        .load_config_macros(None)
        .unwrap();
    assert!(macros.is_empty());
}

#[test]
fn markdown_frontmatter_macros_section_loads_locals() {
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path().join("macros");
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join("doc.md"),
        "---\ndescription: Doc\nmacros:\n  _helper:\n    content: Helper body\n---\nBody",
    )
    .unwrap();

    let loader = CatalogLoader::default();
    let all = loader.load_macros_from_dir(&dir, None, false).unwrap();
    assert_eq!(all["doc"].local_macros.len(), 1);
    assert_eq!(all["doc"].local_macros[0].name, "_helper");
    assert_eq!(all["doc"].local_macros[0].content, "Helper body");
}

#[test]
fn markdown_frontmatter_both_keys_conflict_and_retired_loads() {
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path().join("macros");
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join("both.md"),
        "---\nxprompts:\n  a:\n    content: A\nmacros:\n  b:\n    content: B\n---\nBody",
    )
    .unwrap();
    fs::write(
        dir.join("retired.md"),
        "---\nxprompts:\n  _helper:\n    content: Helper\n---\nBody",
    )
    .unwrap();

    let loader = CatalogLoader::default();
    let error = loader
        .load_macros_from_dir(&dir, None, true)
        .expect_err("both authored keys must conflict");
    assert!(
        matches!(error, MacroCatalogLoadError::DuplicateAuthoredKeys(_)),
        "{error:?}"
    );

    let alone = temp.path().join("alone");
    fs::create_dir_all(&alone).unwrap();
    fs::write(
        alone.join("retired.md"),
        "---\nxprompts:\n  _helper:\n    content: Helper\n---\nBody",
    )
    .unwrap();
    // The retired switch no longer changes policy: a denied loader still
    // accepts the retired-only frontmatter section.
    let denied = CatalogLoader {
        accept_legacy_xprompt_names: false,
        ..CatalogLoader::default()
    };
    let all = denied.load_macros_from_dir(&alone, None, false).unwrap();
    assert_eq!(all["retired"].local_macros.len(), 1);
    assert_eq!(all["retired"].local_macros[0].name, "_helper");
}

#[test]
fn workflow_yaml_macros_section_loads_locals() {
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path().join("workflows");
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join("ship.yml"),
        "macros:\n  _helper:\n    content: Helper\nsteps:\n  - name: run\n    agent: Ship it\n",
    )
    .unwrap();

    let loader = CatalogLoader::default();
    let workflows = loader.load_workflows_from_dir(&dir, None, false).unwrap();
    assert_eq!(workflows["ship"].local_macros.len(), 1);
    assert_eq!(workflows["ship"].local_macros[0].name, "_helper");

    fs::write(
        dir.join("both.yml"),
        "xprompts:\n  a:\n    content: A\nmacros:\n  b:\n    content: B\nsteps:\n  - name: run\n    agent: Ship it\n",
    )
    .unwrap();
    let error = loader
        .load_workflows_from_dir(&dir, None, true)
        .expect_err("both authored keys must conflict");
    assert!(
        matches!(error, MacroCatalogLoadError::DuplicateAuthoredKeys(_)),
        "{error:?}"
    );
}

#[test]
fn nested_local_helpers_use_authored_key_rules() {
    let temp = tempfile::tempdir().unwrap();
    let home = isolated_home(&temp);
    let path = write_config(
        &temp.path().join("nested"),
        "macros:\n  parent:\n    content: Parent\n    macros:\n      _child:\n        content: Child\n",
    );
    let macros = loader_with_config(&path, &home, true)
        .load_config_macros(None)
        .unwrap();
    assert_eq!(macros["parent"].local_macros.len(), 1);
    assert_eq!(macros["parent"].local_macros[0].name, "_child");

    let bad_path = write_config(
        &temp.path().join("nested-bad"),
        "macros:\n  parent:\n    content: Parent\n    xprompts:\n      a:\n        content: A\n    macros:\n      b:\n        content: B\n",
    );
    let error = loader_with_config(&bad_path, &home, true)
        .load_config_macros(None)
        .expect_err("nested both-keys must conflict");
    assert!(
        matches!(error, MacroCatalogLoadError::DuplicateAuthoredKeys(_)),
        "{error:?}"
    );
}

#[test]
fn project_local_config_accepts_macros_key() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    fs::create_dir_all(workspace.join("sase")).unwrap();
    fs::write(
        workspace.join("sase/sase.yml"),
        "macros:\n  local_one:\n    content: Local body\n",
    )
    .unwrap();

    let loader = CatalogLoader {
        root_dir: Some(workspace.clone()),
        ..CatalogLoader::default()
    };
    let macros = loader
        .load_project_local_macros("demo", &workspace)
        .unwrap();
    assert_eq!(macros["demo/local_one"].content, "Local body");
}

#[test]
fn definition_range_prefers_macros_section() {
    let temp = tempfile::tempdir().unwrap();
    let home = isolated_home(&temp);
    let path = write_config(
        &temp.path().join("range"),
        "macros:\n  review:\n    content: body\n",
    );
    let loader = loader_with_config(&path, &home, true);
    let entry = StructuredSource {
        name: "review".to_string(),
        workflow: CatalogWorkflow {
            name: "review".to_string(),
            inputs: Vec::new(),
            steps: Vec::new(),
            local_macros: Vec::new(),
            source_path: Some("default_config".to_string()),
            tags: Default::default(),
            description: None,
        },
        bucket: "config".to_string(),
        project: None,
        description: None,
        is_skill: false,
        skill_name: None,
        memory_type: None,
        content: "body".to_string(),
        definition_section: DefinitionSection::Macros,
    };
    let range = loader.definition_range(&entry).expect("range resolves");
    assert_eq!(range.start.line, 1);
}
