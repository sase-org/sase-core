use super::*;

#[test]
fn loads_markdown_and_workflow_with_canonical_insertions() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let xprompts = root.join("sase/xprompts");
    let skills = root.join("sase/skills");
    fs::create_dir_all(&xprompts).unwrap();
    fs::create_dir_all(&skills).unwrap();
    fs::write(
            skills.join("swarm.md"),
            "---\nname: swarm\ninput:\n  target: word\ntags: [mentor]\nskill: true\n---\nfirst\n---\nsecond",
        )
        .unwrap();
    fs::write(
            xprompts.join("ship.yml"),
            "input:\n  target: word\nsteps:\n  - name: run\n    agent: Ship {{ target }}\n",
        )
        .unwrap();

    let response = load_editor_xprompt_catalog(
        &request(),
        &XpromptCatalogLoadOptions::new(Some(root.to_path_buf())),
    )
    .unwrap();
    let by_name = response
        .entries
        .iter()
        .map(|entry| (entry.name.as_str(), entry))
        .collect::<BTreeMap<_, _>>();

    // A skill source keeps its `/swarm` provider name but is only
    // reachable inline through the namespaced `#skill/swarm`.
    assert!(!by_name.contains_key("swarm"));
    let swarm = by_name["skill/swarm"];
    assert_eq!(swarm.insertion.as_deref(), Some("#skill/swarm"));
    assert_eq!(swarm.reference_prefix.as_deref(), Some("#"));
    assert_eq!(swarm.kind.as_deref(), Some("xprompt"));
    assert!(swarm.is_skill);
    assert_eq!(swarm.skill_name.as_deref(), Some("swarm"));
    assert_eq!(swarm.input_signature.as_deref(), Some("(target: word)"));
    assert_eq!(by_name["ship"].insertion.as_deref(), Some("#!ship"));
    assert_eq!(by_name["ship"].kind.as_deref(), Some("standalone_workflow"));
}
#[test]
fn rejects_misplaced_skill_definitions_in_both_directions() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    let root = temp.path().join("workspace");
    fs::create_dir_all(root.join("sase/xprompts")).unwrap();
    fs::create_dir_all(root.join("sase/skills")).unwrap();
    fs::create_dir_all(&home).unwrap();
    // A skill declaration in the ordinary xprompt directory.
    fs::write(
        root.join("sase/xprompts/stale_skill.md"),
        "---\nskill: true\n---\nOld location",
    )
    .unwrap();
    // An ordinary prompt parked in the canonical skill directory.
    fs::write(
        root.join("sase/skills/not_a_skill.md"),
        "---\ndescription: Plain prompt\n---\nBody",
    )
    .unwrap();
    // A config-defined skill, which no longer exists as a concept.
    fs::write(
        root.join("sase/sase.yml"),
        "xprompts:\n  config_skill:\n    content: Body\n    skill: true\n",
    )
    .unwrap();

    let loader = CatalogLoader {
        root_dir: Some(root.clone()),
        home_dir: Some(home),
        ..CatalogLoader::default()
    };
    let xprompts = loader.load_all_xprompts(None).unwrap();
    assert!(xprompts.is_empty(), "{:?}", xprompts.keys());

    // Nothing is dropped silently: each rejection names the source and
    // the move it needs.
    let warnings = loader.placement_warnings().join("\n");
    assert!(warnings.contains("stale_skill.md"), "{warnings}");
    assert!(
        warnings.contains(root.join("sase/skills").to_string_lossy().as_ref()),
        "{warnings}"
    );
    assert!(warnings.contains("not_a_skill.md"), "{warnings}");
    assert!(
        warnings
            .contains(root.join("sase/xprompts").to_string_lossy().as_ref()),
        "{warnings}"
    );
    assert!(warnings.contains("config_skill"), "{warnings}");
}
#[test]
fn packaged_skill_frame_template_is_not_a_skill_source() {
    let temp = tempfile::tempdir().unwrap();
    let package = temp.path().join("package");
    let package_skills = package.join("xprompts/skills");
    fs::create_dir_all(&package_skills).unwrap();
    fs::write(
        package_skills.join("sase_plan.md"),
        "---\nskill: true\n---\nPlan body",
    )
    .unwrap();
    // The Jinja frame ships beside the sources and has no frontmatter, so
    // it must be skipped rather than reported as a misplaced definition.
    fs::write(
        package_skills.join(SKILL_FRAME_TEMPLATE_FILENAME),
        "{{ frontmatter }}\n\n{{ body }}\n",
    )
    .unwrap();

    let loader = CatalogLoader {
        package_skills_dir: Some(package_skills),
        ..CatalogLoader::default()
    };
    let xprompts = loader.load_all_xprompts(None).unwrap();

    assert_eq!(xprompts.keys().collect::<Vec<_>>(), vec!["skill/sase_plan"]);
    assert!(
        loader.placement_warnings().is_empty(),
        "{:?}",
        loader.placement_warnings()
    );
}
#[test]
fn packaged_skills_load_from_nested_xprompts_skills_only() {
    let temp = tempfile::tempdir().unwrap();
    let package = temp.path().join("package");
    let nested = package.join("xprompts/skills");
    let legacy = package.join("skills");
    fs::create_dir_all(&nested).unwrap();
    fs::create_dir_all(&legacy).unwrap();
    fs::write(
        nested.join("sase_plan.md"),
        "---\nskill: true\n---\nPlan body",
    )
    .unwrap();
    fs::write(
        legacy.join("legacy_plan.md"),
        "---\nskill: true\n---\nLegacy body",
    )
    .unwrap();

    let loader = CatalogLoader {
        package_skills_dir: Some(package.join("xprompts/skills")),
        ..CatalogLoader::default()
    };
    let xprompts = loader.load_all_xprompts(None).unwrap();

    assert!(xprompts.contains_key("skill/sase_plan"));
    assert!(!xprompts.contains_key("skill/legacy_plan"));
    assert!(!xprompts.contains_key("skills/sase_plan"));
}
#[test]
fn home_skills_use_the_skill_namespace_and_project_qualified_form() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    let root = temp.path().join("workspace");
    fs::create_dir_all(home.join("sase/skills/app")).unwrap();
    fs::create_dir_all(&root).unwrap();
    fs::write(
        home.join("sase/skills/bob_query.md"),
        "---\nskill: true\n---\nQuery body",
    )
    .unwrap();
    fs::write(
        home.join("sase/skills/app/scoped.md"),
        "---\nskill: true\n---\nScoped body",
    )
    .unwrap();

    let loader = CatalogLoader {
        root_dir: Some(root),
        home_dir: Some(home),
        ..CatalogLoader::default()
    };
    let xprompts = loader.load_all_xprompts(Some("app")).unwrap();
    let names = xprompts.keys().cloned().collect::<Vec<_>>();
    assert_eq!(names, vec!["app/skill/scoped", "skill/bob_query"]);
    assert_eq!(
        xprompts["skill/bob_query"].skill_name.as_deref(),
        Some("bob_query")
    );
    assert_eq!(
        xprompts["app/skill/scoped"].skill_name.as_deref(),
        Some("scoped")
    );
    // The bare names never resolve after the cutover.
    assert!(!xprompts.contains_key("bob_query"));
    assert!(!xprompts.contains_key("app/scoped"));
}
#[test]
fn parity_fixture_covers_supported_catalog_sources() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("workspace");
    let home = temp.path().join("home");
    let package = temp.path().join("package");
    fs::create_dir_all(root.join("sase/xprompts")).unwrap();
    fs::create_dir_all(root.join("sase/skills")).unwrap();
    fs::create_dir_all(home.join("sase/xprompts/app")).unwrap();
    fs::create_dir_all(package.join("xprompts")).unwrap();
    fs::create_dir_all(package.join("xprompts/skills")).unwrap();
    fs::create_dir_all(package.join("default_xprompts")).unwrap();

    fs::write(
        package.join("xprompts/builtin.md"),
        "---\ntags: [mentor]\n---\nBuilt in",
    )
    .unwrap();
    fs::write(
        package.join("xprompts/skills/sase_plan.md"),
        "---\nskill: true\n---\nPlan skill",
    )
    .unwrap();
    fs::write(
        root.join("sase/skills/local_skill.md"),
        "---\nskill: [claude]\n---\nProject skill",
    )
    .unwrap();
    fs::write(
        package.join("default_xprompts/defaulted.md"),
        "---\ndescription: Default prompt\n---\nDefault body",
    )
    .unwrap();
    fs::write(
            package.join("default_config.yml"),
            "xprompts:\n  cfg:\n    content: Config body\n    input:\n      count:\n        type: int\n        default: 2\n",
        )
        .unwrap();
    fs::write(
        root.join("sase/xprompts/local.md"),
        "---\ninput: {target: word}\n---\nLocal body",
    )
    .unwrap();
    fs::write(root.join("sase/xprompts/swarm.md"), "one\n---\ntwo").unwrap();
    fs::write(
            root.join("sase/xprompts/flow.yml"),
            "input: {target: word}\nsteps:\n  - name: run\n    agent: Run {{ target }}\n",
        )
        .unwrap();
    fs::write(
        home.join("sase/xprompts/app/project.md"),
        "---\ndescription: Project prompt\n---\nProject body",
    )
    .unwrap();

    let loader = CatalogLoader {
        root_dir: Some(root.clone()),
        home_dir: Some(home.clone()),
        package_xprompts_dir: Some(package.join("xprompts")),
        package_skills_dir: Some(package.join("xprompts/skills")),
        default_xprompts_dir: Some(package.join("default_xprompts")),
        default_config_path: Some(package.join("default_config.yml")),
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

    assert_eq!(by_name["builtin"].bucket, "built-in");
    assert!(!by_name["builtin"].is_skill);
    assert_eq!(by_name["skill/sase_plan"].bucket, "built-in");
    assert!(by_name["skill/sase_plan"].is_skill);
    assert_eq!(
        by_name["skill/sase_plan"].skill_name.as_deref(),
        Some("sase_plan")
    );
    // A project skill is namespaced inside its existing project
    // namespace, and a provider list is just as truthy as `true`.
    let project_skill = by_name["app/skill/local_skill"];
    assert!(project_skill.is_skill);
    assert_eq!(project_skill.skill_name.as_deref(), Some("local_skill"));
    assert_eq!(project_skill.project.as_deref(), Some("app"));
    assert_eq!(by_name["defaulted"].bucket, "built-in");
    assert_eq!(by_name["cfg"].bucket, "config");
    assert_eq!(by_name["app/local"].project.as_deref(), Some("app"));
    assert_eq!(by_name["app/project"].bucket, "config");

    let wire_entries = entries
        .iter()
        .map(|entry| structured_entry(entry, &loader))
        .collect::<Vec<_>>();
    let wire_by_name = wire_entries
        .iter()
        .map(|entry| (entry.name.as_str(), entry))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        wire_by_name["app/flow"].kind.as_deref(),
        Some("standalone_workflow")
    );
    assert_eq!(
        wire_by_name["app/flow"].insertion.as_deref(),
        Some("#!app/flow")
    );
    assert_eq!(
        wire_by_name["app/swarm"].insertion.as_deref(),
        Some("#app/swarm")
    );
    assert_eq!(
        wire_by_name["cfg"].input_signature.as_deref(),
        Some("(count?: int)")
    );
    assert_eq!(
        wire_by_name["builtin"].definition_path.as_deref(),
        Some(
            package
                .join("xprompts/builtin.md")
                .canonicalize()
                .unwrap()
                .to_str()
                .unwrap()
        )
    );
    assert_eq!(
        wire_by_name["skill/sase_plan"].insertion.as_deref(),
        Some("#skill/sase_plan")
    );
    assert_eq!(
        wire_by_name["app/skill/local_skill"].insertion.as_deref(),
        Some("#app/skill/local_skill")
    );
    assert_eq!(
        wire_by_name["skill/sase_plan"].definition_path.as_deref(),
        Some(
            package
                .join("xprompts/skills/sase_plan.md")
                .canonicalize()
                .unwrap()
                .to_str()
                .unwrap()
        )
    );
    assert_eq!(
        wire_by_name["defaulted"].definition_path.as_deref(),
        Some(
            package
                .join("default_xprompts/defaulted.md")
                .canonicalize()
                .unwrap()
                .to_str()
                .unwrap()
        )
    );
    assert_eq!(
        wire_by_name["cfg"].definition_path.as_deref(),
        Some(
            package
                .join("default_config.yml")
                .canonicalize()
                .unwrap()
                .to_str()
                .unwrap()
        )
    );
    assert_eq!(definition_line(wire_by_name["cfg"]), Some(1));
    assert_eq!(
        wire_by_name["app/local"].definition_path.as_deref(),
        Some(
            root.join("sase/xprompts/local.md")
                .canonicalize()
                .unwrap()
                .to_str()
                .unwrap()
        )
    );
    assert_eq!(
        wire_by_name["app/local"].source_path_display.as_deref(),
        Some("sase/xprompts/local.md")
    );
    assert_eq!(
        wire_by_name["app/project"].source_path_display.as_deref(),
        Some("~/sase/xprompts/app/project.md")
    );
}
#[test]
fn loads_plugin_file_and_config_catalog_sources() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("workspace");
    let home = temp.path().join("home");
    let package = temp.path().join("package");
    let plugin_prompts = temp.path().join("plugin").join("xprompts");
    let plugin_config = temp.path().join("plugin_config");
    fs::create_dir_all(root.join("sase")).unwrap();
    fs::create_dir_all(home.join(".config/sase")).unwrap();
    fs::create_dir_all(package.join("xprompts")).unwrap();
    fs::create_dir_all(package.join("default_xprompts")).unwrap();
    fs::create_dir_all(&plugin_prompts).unwrap();
    fs::create_dir_all(&plugin_config).unwrap();

    fs::write(package.join("default_config.yml"), "xprompts: {}\n").unwrap();
    fs::write(
        plugin_prompts.join("plug.md"),
        "---\nname: plug\ndescription: Plugin prompt\n---\nPlugin prompt body",
    )
    .unwrap();
    fs::write(
        plugin_prompts.join("gh.yml"),
        "steps:\n  - name: main\n    prompt_part: GitHub workflow body\n",
    )
    .unwrap();
    fs::write(
            plugin_config.join("default_config.yml"),
            "xprompts:\n  plug_cfg:\n    content: Plugin config body\nworkflows:\n  plug_flow:\n    steps:\n      - name: run\n        prompt_part: Plugin config workflow\n",
        )
        .unwrap();
    fs::write(
        root.join("sase/sase.yml"),
        "xprompts:\n  plug_cfg:\n    content: Local override body\n",
    )
    .unwrap();

    let loader = CatalogLoader {
        root_dir: Some(root.clone()),
        home_dir: Some(home),
        package_xprompts_dir: Some(package.join("xprompts")),
        default_xprompts_dir: Some(package.join("default_xprompts")),
        default_config_path: Some(package.join("default_config.yml")),
        plugin_xprompt_dirs: BTreeMap::from([(
            "fake_plugin.prompts".to_string(),
            plugin_prompts.clone(),
        )]),
        plugin_config_paths: BTreeMap::from([(
            "fake_plugin.config".to_string(),
            plugin_config.join("default_config.yml"),
        )]),
        known_workspaces: BTreeMap::new(),
        canonical_project_refs: BTreeMap::new(),
        ..CatalogLoader::default()
    };

    let entries = loader.gather_structured_sources(None).unwrap();
    let by_name = entries
        .iter()
        .map(|entry| (entry.name.as_str(), entry))
        .collect::<BTreeMap<_, _>>();

    assert_eq!(by_name["plug"].bucket, "plugin");
    assert_eq!(by_name["gh"].bucket, "plugin");
    assert!(
        !by_name.contains_key("plug_flow"),
        "config-defined workflows must not appear in the catalog"
    );
    assert_eq!(by_name["plug_cfg"].bucket, "config");
    assert_eq!(
        workflow_prompt_part(&by_name["plug_cfg"].workflow),
        "Local override body"
    );

    let wire_entries = entries
        .iter()
        .map(|entry| structured_entry(entry, &loader))
        .collect::<Vec<_>>();
    let wire_by_name = wire_entries
        .iter()
        .map(|entry| (entry.name.as_str(), entry))
        .collect::<BTreeMap<_, _>>();

    assert_eq!(
        wire_by_name["plug"].source_path_display.as_deref(),
        Some("plugin:fake_plugin.prompts/plug.md")
    );
    assert_eq!(
        wire_by_name["plug"].definition_path.as_deref(),
        Some(
            plugin_prompts
                .join("plug.md")
                .canonicalize()
                .unwrap()
                .to_str()
                .unwrap()
        )
    );
    assert_eq!(
        wire_by_name["plug_cfg"].definition_path.as_deref(),
        Some(
            root.join("sase/sase.yml")
                .canonicalize()
                .unwrap()
                .to_str()
                .unwrap()
        )
    );
    assert_eq!(definition_line(wire_by_name["plug_cfg"]), Some(1));
}
#[test]
fn config_workflows_are_ignored_but_file_backed_project_workflows_load() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("workspace");
    let home = temp.path().join("home");
    let project_workspace = temp.path().join("project");
    fs::create_dir_all(root.join("sase")).unwrap();
    fs::create_dir_all(home.join(".config/sase")).unwrap();
    fs::create_dir_all(project_workspace.join("sase/xprompts")).unwrap();

    fs::write(
            root.join("sase/sase.yml"),
            "xprompts:\n  local_xp:\n    content: Local config xprompt body\nworkflows:\n  local_flow:\n    steps:\n      - name: run\n        prompt_part: Local config workflow body\n",
        )
        .unwrap();
    fs::write(
            home.join(".config/sase/sase.yml"),
            "xprompts:\n  user_xp:\n    content: User config xprompt body\nworkflows:\n  user_flow:\n    steps:\n      - name: run\n        prompt_part: User config workflow body\n",
        )
        .unwrap();
    fs::write(
        project_workspace.join("sase/xprompts/file_flow.yml"),
        "steps:\n  - name: run\n    prompt_part: File-backed workflow body\n",
    )
    .unwrap();

    let loader = CatalogLoader {
        root_dir: Some(root.clone()),
        home_dir: Some(home),
        package_xprompts_dir: None,
        default_xprompts_dir: None,
        default_config_path: None,
        plugin_xprompt_dirs: BTreeMap::new(),
        plugin_config_paths: BTreeMap::new(),
        known_workspaces: BTreeMap::from([(
            "app".to_string(),
            project_workspace.clone(),
        )]),
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

    assert!(by_name.contains_key("user_xp"));
    assert_eq!(by_name["user_xp"].bucket, "config");
    assert!(
        by_name.contains_key("app/local_xp"),
        "local_config xprompts still namespace under the active project"
    );
    assert!(
        !by_name.contains_key("user_flow"),
        "user config workflows must not appear"
    );
    assert!(
        !by_name.contains_key("local_flow"),
        "local config workflows must not appear"
    );
    assert!(
        !by_name.contains_key("app/local_flow"),
        "namespaced local config workflows must not appear"
    );
    assert!(
        by_name.contains_key("app/file_flow"),
        "file-backed workflows in known project workspaces still load"
    );
    assert_eq!(by_name["app/file_flow"].bucket, "project");
}
