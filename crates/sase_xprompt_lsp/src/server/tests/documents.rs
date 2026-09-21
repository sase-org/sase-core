use super::support::*;

use super::super::actions::{document_eligible, should_invalidate_for_uri};
use super::super::state::ServerConfig;

#[test]
fn document_eligibility_narrows_plain_markdown() {
    let temp = std::env::temp_dir();
    let config = ServerConfig::default();
    let canonical_xprompts_uri = file_uri(
        temp.join("project")
            .join("sase")
            .join("xprompts")
            .join("foo.md"),
    );
    let legacy_xprompts_uri =
        file_uri(temp.join("project").join("xprompts").join("foo.md"));
    let dot_xprompts_uri =
        file_uri(temp.join("project").join(".xprompts").join("foo.md"));
    let default_xprompts_uri = file_uri(
        temp.join("project")
            .join("src")
            .join("sase")
            .join("default_xprompts")
            .join("research_swarm.md"),
    );
    let ace_prompt_uri = file_uri(temp.join("sase_ace_prompt_abc.md"));
    let cli_prompt_uri = file_uri(temp.join("sase_prompt_abc.md"));
    let prose_uri = file_uri(
        temp.join("project")
            .join("sdd")
            .join("research")
            .join("202605")
            .join("memory_system_prior_art.md"),
    );

    assert!(document_eligible(
        &canonical_xprompts_uri,
        "markdown",
        &config
    ));
    assert!(document_eligible(&legacy_xprompts_uri, "markdown", &config));
    assert!(document_eligible(&dot_xprompts_uri, "markdown", &config));
    assert!(document_eligible(
        &default_xprompts_uri,
        "markdown",
        &config
    ));
    assert!(document_eligible(&ace_prompt_uri, "markdown", &config));
    assert!(document_eligible(&cli_prompt_uri, "markdown", &config));
    assert!(!document_eligible(&prose_uri, "markdown", &config));

    let all_markdown = ServerConfig {
        allow_all_markdown: true,
        ..ServerConfig::default()
    };
    assert!(document_eligible(&prose_uri, "markdown", &all_markdown));
    assert!(document_eligible(&prose_uri, "gitcommit", &config));
    assert!(document_eligible(&prose_uri, "sase", &config));
    assert!(document_eligible(&prose_uri, "sase_prompt", &config));

    // Memory notes are xprompt memories, so a flat note in a canonical or
    // legacy memory root gets prompt assistance too.
    let canonical_memory_uri = file_uri(
        temp.join("project")
            .join("sase")
            .join("memory")
            .join("glossary.md"),
    );
    let legacy_memory_uri =
        file_uri(temp.join("project").join("memory").join("glossary.md"));
    let nested_memory_asset_uri = file_uri(
        temp.join("project")
            .join("sase")
            .join("memory")
            .join("assets")
            .join("diagram.md"),
    );
    assert!(document_eligible(
        &canonical_memory_uri,
        "markdown",
        &config
    ));
    assert!(document_eligible(&legacy_memory_uri, "markdown", &config));
    assert!(!document_eligible(
        &nested_memory_asset_uri,
        "markdown",
        &config
    ));
}

#[test]
fn catalog_invalidation_tracks_xprompt_source_dirs() {
    let temp = std::env::temp_dir();
    let canonical_xprompts_uri = file_uri(
        temp.join("project")
            .join("sase")
            .join("xprompts")
            .join("foo.md"),
    );
    let legacy_xprompts_uri =
        file_uri(temp.join("project").join("xprompts").join("foo.md"));
    let dot_xprompts_uri =
        file_uri(temp.join("project").join(".xprompts").join("foo.md"));
    let default_xprompts_uri = file_uri(
        temp.join("project")
            .join("src")
            .join("sase")
            .join("default_xprompts")
            .join("research_swarm.md"),
    );
    let canonical_refs_uri = file_uri(
        temp.join("project")
            .join("sase")
            .join("refs")
            .join("research.md"),
    );
    let home_refs_uri = file_uri(
        temp.join("home")
            .join(".config")
            .join("sase")
            .join("refs")
            .join("plans.md"),
    );
    let plugin_refs_uri = file_uri(
        temp.join("plugin")
            .join("sase_xprompts")
            .join("refs")
            .join("designs.md"),
    );
    let prose_uri = file_uri(
        temp.join("project")
            .join("sdd")
            .join("research")
            .join("202605")
            .join("memory_system_prior_art.md"),
    );

    assert!(should_invalidate_for_uri(&canonical_xprompts_uri));
    assert!(should_invalidate_for_uri(&legacy_xprompts_uri));
    assert!(should_invalidate_for_uri(&dot_xprompts_uri));
    assert!(should_invalidate_for_uri(&default_xprompts_uri));
    assert!(should_invalidate_for_uri(&canonical_refs_uri));
    assert!(should_invalidate_for_uri(&home_refs_uri));
    assert!(should_invalidate_for_uri(&plugin_refs_uri));
    assert!(should_invalidate_for_uri(&file_uri(
        temp.join("plugin").join("default_config.yml"),
    )));
    assert!(!should_invalidate_for_uri(&prose_uri));

    // Creating, editing, renaming, or deleting a memory note changes the
    // xprompt-memory catalog, so it must invalidate too.
    assert!(should_invalidate_for_uri(&file_uri(
        temp.join("project")
            .join("sase")
            .join("memory")
            .join("glossary.md"),
    )));
    assert!(should_invalidate_for_uri(&file_uri(
        temp.join("home").join("memory").join("glossary.md"),
    )));
    assert!(!should_invalidate_for_uri(&file_uri(
        temp.join("project")
            .join("sase")
            .join("memory")
            .join("assets")
            .join("diagram.md"),
    )));
}
