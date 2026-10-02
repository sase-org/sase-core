use super::*;
use std::env;

fn write_macro(dir: &Path, name: &str, body: &str) {
    fs::create_dir_all(dir).unwrap();
    fs::write(dir.join(name), body).unwrap();
}

#[test]
fn catalog_options_default_to_accepting_legacy() {
    assert!(MacroCatalogLoadOptions::new(None).accept_legacy_xprompt_names);
    assert!(MacroCatalogLoadOptions::default().accept_legacy_xprompt_names);
    assert!(CatalogLoader::default().accepts_legacy());
    let options = MacroCatalogLoadOptions::new(None).with_legacy_policy(false);
    assert!(!options.accept_legacy_xprompt_names);
    assert!(!CatalogLoader::new(&options).accepts_legacy());
}

#[test]
fn canonical_macros_precede_retired_xprompts_with_conflict() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("workspace");
    let home = temp.path().join("home");
    fs::create_dir_all(&home).unwrap();
    write_macro(
        &root.join("sase/macros"),
        "foo.md",
        "---\ndescription: Canonical\n---\nCanonical body",
    );
    write_macro(
        &root.join("sase/xprompts"),
        "foo.md",
        "---\ndescription: Retired\n---\nRetired body",
    );

    let loader = CatalogLoader {
        root_dir: Some(root.clone()),
        home_dir: Some(home),
        ..CatalogLoader::default()
    };
    let all = loader.load_all_macros(None).unwrap();
    assert_eq!(all["foo"].description.as_deref(), Some("Canonical"));
    assert!(all["foo"].content.contains("Canonical body"));
}

#[test]
fn canonical_only_and_old_only_installations_both_load() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    fs::create_dir_all(&home).unwrap();

    let canonical_root = temp.path().join("canonical");
    write_macro(
        &canonical_root.join("sase/macros"),
        "newbie.md",
        "---\ndescription: New\n---\nNew body",
    );
    let loader = CatalogLoader {
        root_dir: Some(canonical_root),
        home_dir: Some(home.clone()),
        ..CatalogLoader::default()
    };
    assert!(loader.load_all_macros(None).unwrap().contains_key("newbie"));

    let legacy_root = temp.path().join("legacy");
    write_macro(
        &legacy_root.join("sase/xprompts"),
        "oldie.md",
        "---\ndescription: Old\n---\nOld body",
    );
    let loader = CatalogLoader {
        root_dir: Some(legacy_root),
        home_dir: Some(home),
        ..CatalogLoader::default()
    };
    assert!(loader.load_all_macros(None).unwrap().contains_key("oldie"));
}

#[test]
fn explicit_package_macros_win_over_xprompts() {
    let temp = tempfile::tempdir().unwrap();
    let macros_dir = temp.path().join("pkg_macros");
    let xprompts_dir = temp.path().join("pkg_xprompts");
    write_macro(&macros_dir, "plug.md", "Canonical body");
    write_macro(&xprompts_dir, "plug.md", "Retired body");

    let options = MacroCatalogLoadOptions::new(None).with_resource_paths(
        MacroCatalogResourcePaths {
            package_macros_dir: Some(macros_dir),
            package_xprompts_dir: Some(xprompts_dir),
            ..MacroCatalogResourcePaths::default()
        },
    );
    let loader = CatalogLoader::new(&options);
    let all = loader.load_all_macros(None).unwrap();
    assert!(all["plug"].content.contains("Canonical"));
}

#[test]
fn explicit_resources_beat_inferred_environment_paths() {
    let temp = tempfile::tempdir().unwrap();
    let explicit = temp.path().join("explicit_macros");
    let inferred = temp.path().join("inferred_macros");
    write_macro(&explicit, "winner.md", "Explicit body");
    write_macro(&inferred, "winner.md", "Inferred body");

    env::set_var(
        "SASE_MACRO_BUILTIN_DIR",
        inferred.to_string_lossy().to_string(),
    );
    let options = MacroCatalogLoadOptions::new(None).with_resource_paths(
        MacroCatalogResourcePaths {
            package_macros_dir: Some(explicit),
            ..MacroCatalogResourcePaths::default()
        },
    );
    let all = CatalogLoader::new(&options).load_all_macros(None).unwrap();
    env::remove_var("SASE_MACRO_BUILTIN_DIR");
    assert!(all["winner"].content.contains("Explicit"));
}

#[test]
fn macro_env_precedence_is_new_first() {
    let temp = tempfile::tempdir().unwrap();
    let new_dir = temp.path().join("new_builtin");
    let old_dir = temp.path().join("old_builtin");
    write_macro(&new_dir, "envy.md", "New env body");
    write_macro(&old_dir, "envy.md", "Old env body");

    env::set_var(
        "SASE_MACRO_BUILTIN_DIR",
        new_dir.to_string_lossy().to_string(),
    );
    env::set_var(
        "SASE_XPROMPT_BUILTIN_DIR",
        old_dir.to_string_lossy().to_string(),
    );
    let all = CatalogLoader::new(&MacroCatalogLoadOptions::new(None))
        .load_all_macros(None)
        .unwrap();
    env::remove_var("SASE_MACRO_BUILTIN_DIR");
    env::remove_var("SASE_XPROMPT_BUILTIN_DIR");
    assert!(all["envy"].content.contains("New env"));

    let new_default = temp.path().join("new_default");
    let old_default = temp.path().join("old_default");
    write_macro(&new_default, "def.md", "New default");
    write_macro(&old_default, "def.md", "Old default");
    env::set_var(
        "SASE_MACRO_DEFAULT_DIR",
        new_default.to_string_lossy().to_string(),
    );
    env::set_var(
        "SASE_XPROMPT_DEFAULT_DIR",
        old_default.to_string_lossy().to_string(),
    );
    let all = CatalogLoader::new(&MacroCatalogLoadOptions::new(None))
        .load_all_macros(None)
        .unwrap();
    env::remove_var("SASE_MACRO_DEFAULT_DIR");
    env::remove_var("SASE_XPROMPT_DEFAULT_DIR");
    assert!(all["def"].content.contains("New default"));

    let new_plugin = temp.path().join("new_plugin");
    let old_plugin = temp.path().join("old_plugin");
    write_macro(&new_plugin, "plug.md", "New plugin");
    write_macro(&old_plugin, "plug.md", "Old plugin");
    let new_json = serde_json::to_string(&vec![serde_json::json!({
        "module": "demo",
        "path": new_plugin.to_string_lossy(),
    })])
    .unwrap();
    let old_json = serde_json::to_string(&vec![serde_json::json!({
        "module": "demo",
        "path": old_plugin.to_string_lossy(),
    })])
    .unwrap();
    env::set_var("SASE_MACRO_PLUGIN_DIRS_JSON", &new_json);
    env::set_var("SASE_XPROMPT_PLUGIN_DIRS_JSON", &old_json);
    let all = CatalogLoader::new(&MacroCatalogLoadOptions::new(None))
        .load_all_macros(None)
        .unwrap();
    env::remove_var("SASE_MACRO_PLUGIN_DIRS_JSON");
    env::remove_var("SASE_XPROMPT_PLUGIN_DIRS_JSON");
    assert!(all["plug"].content.contains("New plugin"));
}

#[test]
fn policy_false_skips_retired_but_keeps_skills_memory_and_config() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("workspace");
    let home = temp.path().join("home");
    fs::create_dir_all(&home).unwrap();
    write_macro(
        &root.join("sase/macros"),
        "keep.md",
        "---\ndescription: Keep\n---\nKeep body",
    );
    write_macro(
        &root.join("sase/xprompts"),
        "drop.md",
        "---\ndescription: Drop\n---\nDrop body",
    );
    write_macro(
        &root.join("sase/skills"),
        "stay.md",
        "---\nskill: true\n---\nStay body",
    );
    write_memory_note(&root, "glossary.md", "---\ntype: core\n---\nGloss");
    fs::write(
        root.join("sase/sase.yml"),
        "macros:\n  cfg_keep:\n    content: Config body\n",
    )
    .unwrap();

    let options = MacroCatalogLoadOptions::new(Some(root.clone()))
        .with_legacy_policy(false);
    let loader = CatalogLoader {
        root_dir: Some(root),
        home_dir: Some(home),
        accept_legacy_xprompt_names: false,
        ..CatalogLoader::default()
    };
    assert!(!loader.accepts_legacy());
    let _ = &options;
    let all = loader.load_all_macros(None).unwrap();
    assert!(all.contains_key("keep"));
    assert!(!all.contains_key("drop"));
    assert!(all.contains_key("skill/stay"));
    assert!(all.contains_key("memory/glossary"));
    assert!(all.contains_key("cfg_keep"));
}

#[test]
fn policy_false_skips_explicit_retired_and_plugin_retired() {
    let temp = tempfile::tempdir().unwrap();
    let retired = temp.path().join("retired");
    let canonical = temp.path().join("canonical");
    write_macro(&retired, "old.md", "Retired body");
    write_macro(&canonical, "new.md", "Canonical body");
    let plugin_old = temp.path().join("plugin_old");
    write_macro(&plugin_old, "pold.md", "Plugin retired");

    let options = MacroCatalogLoadOptions::new(None)
        .with_resource_paths(MacroCatalogResourcePaths {
            package_xprompts_dir: Some(retired),
            package_macros_dir: Some(canonical),
            plugin_xprompt_dirs: std::collections::BTreeMap::from([(
                "demo".to_string(),
                plugin_old,
            )]),
            ..MacroCatalogResourcePaths::default()
        })
        .with_legacy_policy(false);
    // Pin home to an isolated temp dir: ambient ~/.config would otherwise
    // leak a retired authored key into this false-policy load and fail it.
    let home = temp.path().join("home");
    fs::create_dir_all(&home).unwrap();
    let mut loader = CatalogLoader::new(&options);
    loader.home_dir = Some(home);
    let all = loader.load_all_macros(None).unwrap();
    assert!(!all.contains_key("old"));
    assert!(all.contains_key("new"));
    assert!(!all.contains_key("pold"));
}

#[test]
fn package_macro_skills_precede_xprompt_skills() {
    let temp = tempfile::tempdir().unwrap();
    let package = temp.path().join("package");
    let macros_skills = package.join("macros/skills");
    let xprompts_skills = package.join("xprompts/skills");
    write_macro(
        &macros_skills,
        "plan.md",
        "---\nskill: true\n---\nMacro skill",
    );
    write_macro(
        &xprompts_skills,
        "plan.md",
        "---\nskill: true\n---\nXprompt skill",
    );

    let loader = CatalogLoader {
        package_macros_dir: Some(package.join("macros")),
        package_skills_dir: Some(xprompts_skills),
        ..CatalogLoader::default()
    };
    let all = loader.load_all_macros(None).unwrap();
    let entry = &all["skill/plan"];
    assert!(entry.content.contains("Macro skill"));
}

#[test]
fn config_collision_rules_are_preserved() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("workspace");
    let home = temp.path().join("home");
    fs::create_dir_all(&home).unwrap();
    fs::create_dir_all(root.join("sase")).unwrap();
    fs::write(
        root.join("sase/sase.yml"),
        "xprompts:\n  a:\n    content: A\n",
    )
    .unwrap();
    fs::write(root.join("sase.yml"), "xprompts:\n  b:\n    content: B\n")
        .unwrap();

    let loader = CatalogLoader {
        root_dir: Some(root),
        home_dir: Some(home),
        ..CatalogLoader::default()
    };
    let result = loader.load_all_macros(None);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("multiple"));
}
