use super::*;

#[test]
fn resolves_xprompt_skill_definition_sources() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let skills = root.join("sase/skills");
    fs::create_dir_all(&skills).unwrap();
    let source = skills.join("sase_plan.md");
    fs::write(
        &source,
        "---\nname: sase_plan\nskill: true\n---\nPlan body\n",
    )
    .unwrap();
    let options = MacroCatalogLoadOptions::new(Some(root.to_path_buf()));

    let explicit = resolve_macro_skill_definition(
        &MacroSkillDefinitionRequestWire {
            schema_version: MACRO_SKILL_DEFINITION_WIRE_SCHEMA_VERSION,
            reference: "#skill/sase_plan".to_string(),
            project: None,
        },
        &options,
    );
    assert_eq!(explicit.status, "success");
    assert_eq!(
        explicit.canonical_reference.as_deref(),
        Some("skill/sase_plan")
    );
    assert_eq!(
        explicit.definition_path.as_deref(),
        Some(source.canonicalize().unwrap().to_str().unwrap())
    );

    let shorthand = resolve_macro_skill_definition(
        &MacroSkillDefinitionRequestWire {
            schema_version: MACRO_SKILL_DEFINITION_WIRE_SCHEMA_VERSION,
            reference: "#skill__sase_plan".to_string(),
            project: None,
        },
        &options,
    );
    assert_eq!(shorthand.status, "success");
    assert_eq!(
        shorthand.canonical_reference.as_deref(),
        Some("skill/sase_plan")
    );

    let slash = resolve_macro_skill_definition(
        &MacroSkillDefinitionRequestWire {
            schema_version: MACRO_SKILL_DEFINITION_WIRE_SCHEMA_VERSION,
            reference: "/sase_plan".to_string(),
            project: None,
        },
        &options,
    );
    assert_eq!(slash.status, "success");
    assert_eq!(slash.skill_name.as_deref(), Some("sase_plan"));

    let missing = resolve_macro_skill_definition(
        &MacroSkillDefinitionRequestWire {
            schema_version: MACRO_SKILL_DEFINITION_WIRE_SCHEMA_VERSION,
            reference: "#skill/missing".to_string(),
            project: None,
        },
        &options,
    );
    assert_eq!(missing.status, "missing_skill");
    assert_eq!(
        missing.canonical_reference.as_deref(),
        Some("skill/missing")
    );
}
#[test]
fn catalog_payloads_without_memory_fields_still_deserialize() {
    // Helper payloads written before xprompt memories existed omit the
    // additive fields entirely.
    let entry: MobileMacroCatalogEntryWire = serde_json::from_str(
            r##"{"name":"foo","display_label":"foo","insertion":"#foo","reference_prefix":"#","kind":"xprompt","description":null,"source_bucket":"config","project":null,"tags":[],"input_signature":null,"is_skill":false,"content_preview":null,"source_path_display":null}"##,
        )
        .unwrap();
    assert_eq!(entry.memory_type, None);
    let stats: MobileMacroCatalogStatsWire = serde_json::from_str(
            r#"{"total_count":1,"project_count":0,"skill_count":0,"pdf_requested":false}"#,
        )
        .unwrap();
    assert_eq!(stats.memory_count, 0);
    // A memory entry serializes its tier as the current canonical value.
    let rendered = serde_json::to_value(MobileMacroCatalogEntryWire {
        memory_type: Some(MemoryTierWire::Reference),
        ..entry
    })
    .unwrap();
    assert_eq!(rendered["memory_type"], "reference");
}
#[test]
fn pseudo_sources_do_not_get_definition_paths() {
    let temp = tempfile::tempdir().unwrap();
    let loader = CatalogLoader {
        root_dir: Some(temp.path().to_path_buf()),
        home_dir: Some(temp.path().join("home")),
        package_xprompts_dir: None,
        default_xprompts_dir: None,
        default_config_path: None,
        plugin_xprompt_dirs: BTreeMap::new(),
        plugin_config_paths: BTreeMap::new(),
        known_workspaces: BTreeMap::new(),
        canonical_project_refs: BTreeMap::new(),
        ..CatalogLoader::default()
    };
    let entry = StructuredSource {
        name: "plugin".to_string(),
        workflow: CatalogWorkflow {
            name: "plugin".to_string(),
            inputs: Vec::new(),
            steps: vec![CatalogStep {
                name: "prompt".to_string(),
                kind: StepKind::PromptPart,
                prompt_part: Some("body".to_string()),
                has_output: false,
            }],
            local_macros: Vec::new(),
            source_path: Some("plugin:module/plugin.md".to_string()),
            tags: BTreeSet::new(),
            description: None,
        },
        bucket: "plugin".to_string(),
        project: None,
        description: None,
        is_skill: false,
        skill_name: None,
        memory_type: None,
        content: "body".to_string(),
        definition_section: DefinitionSection::Macros,
    };

    assert_eq!(structured_entry(&entry, &loader).definition_path, None);
}

#[test]
fn macro_skill_wires_pin_legacy_schema_and_shape() {
    assert_eq!(
        MACRO_SKILL_DEFINITION_WIRE_SCHEMA_VERSION, 1,
        "wire schema version is a protected contract value"
    );
    let request = MacroSkillDefinitionRequestWire {
        schema_version: MACRO_SKILL_DEFINITION_WIRE_SCHEMA_VERSION,
        reference: "#skill/foo".to_string(),
        project: None,
    };
    let json = serde_json::to_value(&request).unwrap();
    assert_eq!(
        json,
        serde_json::json!({
            "schema_version": 1,
            "reference": "#skill/foo",
        })
    );
    let round_tripped: MacroSkillDefinitionRequestWire =
        serde_json::from_value(json).unwrap();
    assert_eq!(round_tripped, request);
}
