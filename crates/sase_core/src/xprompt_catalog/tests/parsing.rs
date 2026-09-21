use super::*;

#[test]
fn yaml_child_key_range_finds_immediate_quoted_children() {
    let text = "xprompts:\n  parent:\n    child: nested\n  \"quoted/key\": body\nworkflows:\n  flow:\n    steps: []\n";

    let range = yaml_child_key_range(text, "xprompts", "quoted/key").unwrap();

    assert_eq!(range.start.line, 3);
    assert_eq!(range.start.character, 2);
    assert_eq!(yaml_child_key_range(text, "xprompts", "child"), None);
    assert_eq!(
        yaml_child_key_range(text, "workflows", "flow")
            .unwrap()
            .start
            .line,
        5
    );
}
#[test]
fn projects_repeatable_agent_input_metadata() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let xprompts = root.join("sase/xprompts");
    fs::create_dir_all(&xprompts).unwrap();
    fs::write(
            xprompts.join("merge.yml"),
            "input:\n  names:\n    type: agent\n    default:\n    repeatable: true\nsteps:\n  - name: main\n    prompt_part: '{{ names }}'\n",
        )
        .unwrap();

    let response = load_editor_xprompt_catalog(
        &request(),
        &XpromptCatalogLoadOptions::new(Some(root.to_path_buf())),
    )
    .unwrap();
    let entry = response
        .entries
        .iter()
        .find(|entry| entry.name == "merge")
        .unwrap();

    assert_eq!(entry.input_signature.as_deref(), Some("(names…?: agent)"));
    assert_eq!(entry.inputs[0].r#type, "agent");
    assert!(entry.inputs[0].repeatable);
}
#[test]
fn filters_step_inputs_and_formats_defaults() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let xprompts = root.join("sase/xprompts");
    fs::create_dir_all(&xprompts).unwrap();
    fs::write(
            xprompts.join("typed.yml"),
            "input:\n  required_word: word\n  string_default:\n    type: line\n    default: secret\n  null_default:\n    type: text\n    default:\n  count:\n    type: int\n    default: 3\n  enabled:\n    type: bool\n    default: false\nsteps:\n  - name: setup\n    bash: echo hi\n    output: {value: line}\n  - name: main\n    prompt_part: body\n",
        )
        .unwrap();

    let response = load_editor_xprompt_catalog(
        &request(),
        &XpromptCatalogLoadOptions::new(Some(root.to_path_buf())),
    )
    .unwrap();
    let entry = response
        .entries
        .iter()
        .find(|entry| entry.name == "typed")
        .unwrap();

    assert_eq!(
            entry.input_signature.as_deref(),
            Some(
                "(required_word: word, string_default?: line, null_default?: text, count?: int, enabled?: bool)"
            )
        );
    assert_eq!(
        entry
            .inputs
            .iter()
            .map(|input| (
                input.name.as_str(),
                input.r#type.as_str(),
                input.required,
                input.default_display.as_deref(),
                input.position,
            ))
            .collect::<Vec<_>>(),
        vec![
            ("required_word", "word", true, None, 0),
            ("string_default", "line", false, None, 1),
            ("null_default", "text", false, None, 2),
            ("count", "int", false, Some("3"), 3),
            ("enabled", "bool", false, Some("false"), 4),
        ]
    );
}
#[test]
fn parses_xprompt_workflow_and_input_descriptions() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let xprompts = root.join("sase/xprompts");
    fs::create_dir_all(&xprompts).unwrap();
    fs::write(
            xprompts.join("long.md"),
            "---\ndescription: Long prompt\ninput:\n  - name: prompt\n    type: text\n    description: User request for the prompt.\n---\nBody {{ prompt }}",
        )
        .unwrap();
    fs::write(
            xprompts.join("nested.md"),
            "---\ninput:\n  target:\n    type: word\n    description: Target name to inspect.\n---\nTarget {{ target }}",
        )
        .unwrap();
    fs::write(
            xprompts.join("ship.yml"),
            "description: Ship workflow\ninput:\n  path:\n    type: path\n    description: Source path for workflow.\nxprompts:\n  _helper:\n    description: Local helper summary.\n    input:\n      topic:\n        type: word\n        description: Local topic description.\n    content: Helper {{ topic }}\nsteps:\n  - name: main\n    prompt_part: Ship {{ path }}\n",
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

    assert_eq!(by_name["long"].description.as_deref(), Some("Long prompt"));
    assert_eq!(
        by_name["long"].inputs[0].description.as_deref(),
        Some("User request for the prompt.")
    );
    assert_eq!(
        by_name["nested"].inputs[0].description.as_deref(),
        Some("Target name to inspect.")
    );
    assert_eq!(
        by_name["ship"].description.as_deref(),
        Some("Ship workflow")
    );
    assert_eq!(
        by_name["ship"].inputs[0].description.as_deref(),
        Some("Source path for workflow.")
    );
    let mut filtered_request = request();
    filtered_request.query = Some("local helper summary".to_string());
    let filtered = load_editor_xprompt_catalog(
        &filtered_request,
        &XpromptCatalogLoadOptions::new(Some(root.to_path_buf())),
    )
    .unwrap();
    assert_eq!(
        filtered
            .entries
            .iter()
            .map(|entry| entry.name.as_str())
            .collect::<Vec<_>>(),
        vec!["ship"]
    );
    filtered_request.query = Some("local topic description".to_string());
    let filtered = load_editor_xprompt_catalog(
        &filtered_request,
        &XpromptCatalogLoadOptions::new(Some(root.to_path_buf())),
    )
    .unwrap();
    assert_eq!(
        filtered
            .entries
            .iter()
            .map(|entry| entry.name.as_str())
            .collect::<Vec<_>>(),
        vec!["ship"]
    );
}
#[test]
fn parses_markdown_frontmatter_local_xprompts_without_global_entry() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let xprompts = root.join("sase/xprompts");
    fs::create_dir_all(&xprompts).unwrap();
    fs::write(
            xprompts.join("reads.md"),
            "---\ndescription: Read articles\nxprompts:\n  _article_search_agent:\n    description: Local article helper summary.\n    input:\n      topic:\n        type: word\n        description: Search topic description.\n    content: Search {{ topic }}\n---\n#_article_search_agent(news)\n",
        )
        .unwrap();

    let loader = CatalogLoader::new(&XpromptCatalogLoadOptions::new(Some(
        root.to_path_buf(),
    )));
    let loaded = loader
        .load_xprompts_from_dir(&xprompts, None, false)
        .unwrap();
    assert!(loaded.contains_key("reads"));
    assert!(!loaded.contains_key("_article_search_agent"));

    let workflow = xprompt_to_workflow(loaded.get("reads").unwrap());
    assert_eq!(workflow.local_xprompts.len(), 1);
    let helper = &workflow.local_xprompts[0];
    assert_eq!(helper.name, "_article_search_agent");
    assert_eq!(
        helper.description.as_deref(),
        Some("Local article helper summary.")
    );
    assert_eq!(helper.inputs[0].name, "topic");
    assert_eq!(
        helper.inputs[0].description.as_deref(),
        Some("Search topic description.")
    );

    let mut filtered_request = request();
    filtered_request.query = Some("local article helper summary".to_string());
    let filtered = load_editor_xprompt_catalog(
        &filtered_request,
        &XpromptCatalogLoadOptions::new(Some(root.to_path_buf())),
    )
    .unwrap();
    assert_eq!(
        filtered
            .entries
            .iter()
            .map(|entry| entry.name.as_str())
            .collect::<Vec<_>>(),
        vec!["reads"]
    );
}
