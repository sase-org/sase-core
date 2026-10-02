use super::*;

#[test]
fn canonical_project_sources_win_with_legacy_read_compatibility() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("workspace");
    fs::create_dir_all(root.join("sase/xprompts")).unwrap();
    fs::create_dir_all(root.join(".xprompts")).unwrap();
    fs::create_dir_all(root.join("xprompts")).unwrap();

    fs::write(root.join("sase/xprompts/shared.md"), "Canonical body").unwrap();
    fs::write(root.join(".xprompts/shared.md"), "Hidden legacy body").unwrap();
    fs::write(root.join("xprompts/shared.md"), "Visible legacy body").unwrap();
    fs::write(root.join(".xprompts/hidden_only.md"), "Hidden only").unwrap();
    fs::write(root.join("xprompts/visible_only.md"), "Visible only").unwrap();
    fs::write(
        root.join("sase/xprompts/flow.yml"),
        "steps:\n  - name: main\n    prompt_part: Canonical workflow\n",
    )
    .unwrap();
    fs::write(
        root.join(".xprompts/flow.yml"),
        "steps:\n  - name: main\n    prompt_part: Legacy workflow\n",
    )
    .unwrap();
    fs::write(
        root.join("sase.yml"),
        "xprompts:\n  legacy_config:\n    content: Legacy config body\n",
    )
    .unwrap();

    let loader = CatalogLoader {
        root_dir: Some(root.clone()),
        home_dir: None,
        package_xprompts_dir: None,
        default_xprompts_dir: None,
        default_config_path: None,
        plugin_xprompt_dirs: BTreeMap::new(),
        plugin_config_paths: BTreeMap::new(),
        known_workspaces: BTreeMap::from([("app".to_string(), root.clone())]),
        canonical_project_refs: BTreeMap::from([(
            "app".to_string(),
            "app".to_string(),
        )]),
        ..CatalogLoader::default()
    };

    let entries = loader.gather_structured_sources(Some("app")).unwrap();
    let by_name = entries
        .iter()
        .map(|entry| (entry.name.as_str(), entry))
        .collect::<BTreeMap<_, _>>();

    assert_eq!(by_name["app/shared"].content, "Canonical body");
    assert_eq!(by_name["app/hidden_only"].content, "Hidden only");
    assert_eq!(by_name["app/visible_only"].content, "Visible only");
    assert_eq!(
        workflow_prompt_part(&by_name["app/flow"].workflow),
        "Canonical workflow"
    );
    assert_eq!(
        workflow_prompt_part(&by_name["app/legacy_config"].workflow),
        "Legacy config body"
    );

    let shared = structured_entry(by_name["app/shared"], &loader);
    assert_eq!(
        shared.source_path_display.as_deref(),
        Some("sase/xprompts/shared.md")
    );
    assert_eq!(
        shared.definition_path.as_deref(),
        Some(
            root.join("sase/xprompts/shared.md")
                .canonicalize()
                .unwrap()
                .to_str()
                .unwrap()
        )
    );
}
#[test]
fn project_config_collision_reports_split_state() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("workspace");
    fs::create_dir_all(root.join("sase")).unwrap();
    fs::write(root.join("sase/sase.yml"), "xprompts: {}\n").unwrap();
    fs::write(root.join("sase.yml"), "xprompts: {}\n").unwrap();

    let loader = CatalogLoader {
        root_dir: Some(root),
        home_dir: None,
        package_xprompts_dir: None,
        default_xprompts_dir: None,
        default_config_path: None,
        plugin_xprompt_dirs: BTreeMap::new(),
        plugin_config_paths: BTreeMap::new(),
        known_workspaces: BTreeMap::new(),
        canonical_project_refs: BTreeMap::new(),
        ..CatalogLoader::default()
    };

    let error = loader.gather_structured_sources(None).unwrap_err();
    assert!(matches!(
        error,
        XpromptCatalogLoadError::LayoutCollision(message)
            if message.contains("multiple canonical/legacy")
                && message.contains("sase/sase.yml")
                && message.contains("sase.yml")
    ));
}
#[test]
fn computes_known_project_local_config_definition_range() {
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    fs::create_dir_all(workspace.join("sase/xprompts")).unwrap();
    fs::write(
        workspace.join("sase/sase.yml"),
        "xprompts:\n  project_cfg:\n    content: Project body\n",
    )
    .unwrap();
    fs::write(
        workspace.join("sase/xprompts/project_file.md"),
        "Project file body",
    )
    .unwrap();

    let loader = CatalogLoader {
        root_dir: None,
        home_dir: None,
        package_xprompts_dir: None,
        default_xprompts_dir: None,
        default_config_path: None,
        plugin_xprompt_dirs: BTreeMap::new(),
        plugin_config_paths: BTreeMap::new(),
        known_workspaces: BTreeMap::from([(
            "app".to_string(),
            workspace.clone(),
        )]),
        canonical_project_refs: BTreeMap::from([(
            "app".to_string(),
            "app".to_string(),
        )]),
        ..CatalogLoader::default()
    };

    let entries = loader.gather_structured_sources(None).unwrap();
    let wire_entries = entries
        .iter()
        .map(|entry| structured_entry(entry, &loader))
        .collect::<Vec<_>>();
    let entry = wire_entries
        .iter()
        .find(|entry| entry.name == "app/project_cfg")
        .unwrap();
    let file_entry = wire_entries
        .iter()
        .find(|entry| entry.name == "app/project_file")
        .unwrap();

    assert_eq!(
        entry.definition_path.as_deref(),
        Some(
            workspace
                .join("sase/sase.yml")
                .canonicalize()
                .unwrap()
                .to_str()
                .unwrap()
        )
    );
    assert_eq!(definition_line(entry), Some(1));
    assert_eq!(
        file_entry.source_path_display.as_deref(),
        Some("sase/xprompts/project_file.md")
    );
    assert_eq!(
        file_entry.definition_path.as_deref(),
        Some(
            workspace
                .join("sase/xprompts/project_file.md")
                .canonicalize()
                .unwrap()
                .to_str()
                .unwrap()
        )
    );
}
#[test]
fn known_projects_use_display_names_aliases_and_gp_fallback() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    let projects_dir = home.join(".sase").join("projects");

    let canonical_workspace = temp.path().join("canonical_ws");
    let canonical_project_dir = projects_dir.join("gh_org__proj");
    fs::create_dir_all(&canonical_workspace).unwrap();
    fs::create_dir_all(&canonical_project_dir).unwrap();
    fs::write(
            canonical_project_dir.join("gh_org__proj.sase"),
            format!(
                "PROJECT_NAME: proj\nPROJECT_ALIASES: p, project\nWORKSPACE_DIR: {}\n",
                canonical_workspace.display()
            ),
        )
        .unwrap();
    fs::write(
        canonical_project_dir.join("gh_org__proj.gp"),
        "WORKSPACE_DIR: /tmp/should-be-ignored\n",
    )
    .unwrap();

    let legacy_workspace = temp.path().join("legacy_ws");
    let legacy_project_dir = projects_dir.join("legacy");
    fs::create_dir_all(&legacy_workspace).unwrap();
    fs::create_dir_all(&legacy_project_dir).unwrap();
    fs::write(
        legacy_project_dir.join("legacy.gp"),
        format!("WORKSPACE_DIR: {}\n", legacy_workspace.display()),
    )
    .unwrap();

    let archived_project_dir = projects_dir.join("archived");
    fs::create_dir_all(&archived_project_dir).unwrap();
    fs::write(
        archived_project_dir.join("archived-archive.sase"),
        format!("WORKSPACE_DIR: {}\n", temp.path().display()),
    )
    .unwrap();

    let known = known_projects(Some(home.as_path()));

    assert_eq!(
        known.workspaces.get("proj").map(PathBuf::as_path),
        Some(canonical_workspace.as_path()),
    );
    assert_eq!(
        known.workspaces.get("legacy").map(PathBuf::as_path),
        Some(legacy_workspace.as_path()),
    );
    assert!(!known.workspaces.contains_key("gh_org__proj"));
    assert!(!known.workspaces.contains_key("archived"));
    for project_ref in ["gh_org__proj", "proj", "p", "project"] {
        assert_eq!(
            known.canonical_refs.get(project_ref).map(String::as_str),
            Some("proj"),
        );
    }
    assert_eq!(
        known.canonical_refs.get("legacy").map(String::as_str),
        Some("legacy"),
    );
}
#[test]
fn project_catalog_uses_canonical_namespace_and_filter_refs() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    let projects_dir = home.join(".sase").join("projects");
    let project_dir = projects_dir.join("gh_org__proj");
    let workspace = temp.path().join("workspace");
    fs::create_dir_all(&project_dir).unwrap();
    fs::create_dir_all(workspace.join("sase/xprompts")).unwrap();
    fs::write(
            project_dir.join("gh_org__proj.sase"),
            format!(
                "PROJECT_NAME: proj\nPROJECT_ALIASES: shortcut\nWORKSPACE_DIR: {}\n",
                workspace.display()
            ),
        )
        .unwrap();
    fs::write(
        workspace.join("sase/xprompts/thing.md"),
        "Project prompt body",
    )
    .unwrap();

    let known = known_projects(Some(home.as_path()));
    let loader = CatalogLoader {
        root_dir: Some(workspace.clone()),
        home_dir: Some(home),
        package_xprompts_dir: None,
        default_xprompts_dir: None,
        default_config_path: None,
        plugin_xprompt_dirs: BTreeMap::new(),
        plugin_config_paths: BTreeMap::new(),
        known_workspaces: known.workspaces,
        canonical_project_refs: known.canonical_refs,
        ..CatalogLoader::default()
    };
    let entries = loader.gather_structured_sources(None).unwrap();

    assert_eq!(
        entries
            .iter()
            .filter(|entry| entry.name == "proj/thing")
            .count(),
        1,
    );
    let entry = entries
        .iter()
        .find(|entry| entry.name == "proj/thing")
        .unwrap();
    assert_eq!(entry.project.as_deref(), Some("proj"));
    assert!(entries
        .iter()
        .all(|entry| entry.name != "gh_org__proj/thing"));

    for project_ref in ["gh_org__proj", "proj", "shortcut"] {
        let mut filtered_request = request();
        filtered_request.project = Some(project_ref.to_string());
        let canonical =
            loader.canonical_project(filtered_request.project.as_deref());
        let filtered = filter_structured_sources(
            entries.clone(),
            &filtered_request,
            canonical.as_deref(),
        );
        assert_eq!(
            filtered
                .iter()
                .map(|entry| entry.name.as_str())
                .collect::<Vec<_>>(),
            vec!["proj/thing"],
            "{project_ref}",
        );
    }
}
