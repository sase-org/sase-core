use super::*;

#[test]
fn loads_native_snippet_catalog_with_user_overrides() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let xprompts = root.join("sase/xprompts");
    fs::create_dir_all(&xprompts).unwrap();
    fs::write(
            xprompts.join("review.md"),
            "---\nsnippet: true\ndescription: Review code\ninput:\n  language: word\n  focus:\n    type: line\n    default: correctness\n---\nReview this {{ language }} code for {{ focus }}.\nLegacy {2:done} {3}",
        )
        .unwrap();
    fs::write(
        xprompts.join("skip.md"),
        "---\nsnippet: bad-trigger!\n---\nBody",
    )
    .unwrap();
    fs::write(
            xprompts.join("capital.md"),
            "---\nsnippet: Review\ndescription: Explicit capitalized review\n---\nAuthored capital review",
        )
        .unwrap();
    fs::write(
        root.join("sase/sase.yml"),
        "ace:\n  snippets:\n    review: User review $0\n    plan: Plan $1$0\n",
    )
    .unwrap();

    let response = load_editor_snippet_catalog(
        &EditorSnippetCatalogRequestWire {
            schema_version: 1,
            project: None,
        },
        &XpromptCatalogLoadOptions::new(Some(root.to_path_buf())),
    )
    .unwrap();
    let by_trigger = response
        .entries
        .iter()
        .map(|entry| (entry.trigger.as_str(), entry))
        .collect::<BTreeMap<_, _>>();

    assert!(response.stats.total_count >= 2);
    assert_eq!(by_trigger["review"].source, "user_config");
    assert_eq!(by_trigger["review"].template, "User review $0");
    assert_eq!(by_trigger["Review"].source, "xprompt");
    assert_eq!(by_trigger["Review"].template, "Authored capital review$0");
    assert_eq!(
        by_trigger["Review"].description.as_deref(),
        Some("Explicit capitalized review")
    );
    assert_eq!(by_trigger["plan"].template, "Plan $1$0");
    assert_eq!(by_trigger["Plan"].template, "Plan $1$0");
    assert_eq!(by_trigger["Plan"].source, "user_config");
    assert!(!by_trigger.contains_key("bad-trigger!"));
}
#[test]
fn converts_native_xprompt_snippet_templates() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let xprompts = root.join("sase/xprompts");
    fs::create_dir_all(&xprompts).unwrap();
    fs::write(
            xprompts.join("fix.md"),
            "---\nsnippet: fixit\ndescription: Fix a bug\ninput:\n  bug: word\n  area:\n    type: line\n    default: parser\n  empty:\n    type: line\n    default:\n---\nfix {{ bug }} in {{ area }}{{ empty }}. Then {2} or {3:done}.",
        )
        .unwrap();
    fs::write(
        xprompts.join("complex.md"),
        "---\nsnippet: true\n---\n{% if enabled %}skip{% endif %}",
    )
    .unwrap();

    let response = load_editor_snippet_catalog(
        &EditorSnippetCatalogRequestWire {
            schema_version: 1,
            project: None,
        },
        &XpromptCatalogLoadOptions::new(Some(root.to_path_buf())),
    )
    .unwrap();
    let by_trigger = response
        .entries
        .iter()
        .map(|entry| (entry.trigger.as_str(), entry))
        .collect::<BTreeMap<_, _>>();

    assert_eq!(
        by_trigger["fixit"].template,
        "fix $1 in parser. Then $2 or done.$0"
    );
    assert_eq!(by_trigger["fixit"].xprompt_name.as_deref(), Some("fix"));
    assert_eq!(
        by_trigger["Fixit"].template,
        "Fix $1 in parser. Then $2 or done.$0"
    );
    assert_eq!(by_trigger["Fixit"].source, by_trigger["fixit"].source);
    assert_eq!(
        by_trigger["Fixit"].xprompt_name,
        by_trigger["fixit"].xprompt_name
    );
    assert_eq!(
        by_trigger["Fixit"].description,
        by_trigger["fixit"].description
    );
    assert_eq!(
        by_trigger["Fixit"].source_path_display,
        by_trigger["fixit"].source_path_display
    );
    assert_eq!(response.stats.total_count, response.entries.len() as u64);
    assert!(!by_trigger.contains_key("complex"));
}
#[test]
fn native_snippet_catalog_resolves_references_after_user_merge() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let xprompts = root.join("sase/xprompts");
    fs::create_dir_all(&xprompts).unwrap();
    fs::write(
        xprompts.join("helper.md"),
        "---\nsnippet: true\ninput:\n  topic: word\n---\nHelp {{ topic }}",
    )
    .unwrap();
    fs::write(
            xprompts.join("outer.md"),
            "---\nsnippet: true\ninput:\n  topic: word\n---\n#[user_snip] {{ topic }}",
        )
        .unwrap();
    fs::write(
            root.join("sase/sase.yml"),
            "ace:\n  snippets:\n    user_snip: User $1$0\n    wrap: \"#[helper(World)] $1$0\"\n",
        )
        .unwrap();

    let response = load_editor_snippet_catalog(
        &EditorSnippetCatalogRequestWire {
            schema_version: 1,
            project: None,
        },
        &XpromptCatalogLoadOptions::new(Some(root.to_path_buf())),
    )
    .unwrap();
    let by_trigger = response
        .entries
        .iter()
        .map(|entry| (entry.trigger.as_str(), entry))
        .collect::<BTreeMap<_, _>>();

    assert_eq!(by_trigger["outer"].template, "User $1 $2$0");
    assert_eq!(by_trigger["wrap"].template, "Help World $1$0");
}
