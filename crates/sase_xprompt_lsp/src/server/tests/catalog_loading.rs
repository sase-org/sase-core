use std::{path::Path, process::Command, sync::Arc};

use lsp_types::{CompletionItemKind, CompletionTextEdit, Position};

use super::super::*;

use super::support::*;

use super::super::catalogs::{
    load_artifact_ref_catalog, load_vcs_project_catalog,
};
use super::super::state::ArtifactRefCatalog;

#[test]
fn loads_v1_vcs_project_catalog_with_project_defaults() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("vcs_project_catalog.json");
    fs::write(
        &catalog_path,
        r##"{
            "schema_version": 1,
            "workflow_names": ["gh"],
            "entries": [
                {
                    "name": "sase",
                    "vcs_prefix": "gh",
                    "display_tag": "#gh:sase",
                    "provider_display": "GitHub",
                    "description": "",
                    "aliases": []
                }
            ]
        }"##,
    )
    .unwrap();

    let catalog = load_vcs_project_catalog(Some(&catalog_path));

    assert_eq!(catalog.workflow_names, vec!["gh"]);
    assert_eq!(catalog.entries.len(), 1);
    assert!(catalog.entries[0].kind.is_empty());
    assert!(catalog.entries[0].entry_kind.is_empty());
    assert_eq!(catalog.entries[0].project, "");
    assert_eq!(catalog.entries[0].status, "");
    assert!(catalog.namespaces.is_empty());
}

#[test]
fn loads_v3_vcs_project_catalog_namespaces() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("vcs_project_catalog.json");
    fs::write(
        &catalog_path,
        r##"{
            "schema_version": 3,
            "workflow_names": ["gh", "git"],
            "entries": [],
            "namespaces": {
                "gh": [
                    {
                        "name": "sase-org",
                        "description": "2 enabled projects",
                        "kind_label": "org"
                    },
                    {
                        "name": "bbugyi200"
                    }
                ]
            }
        }"##,
    )
    .unwrap();

    let catalog = load_vcs_project_catalog(Some(&catalog_path));

    assert_eq!(catalog.workflow_names, vec!["gh", "git"]);
    assert!(catalog.entries.is_empty());
    let namespaces = catalog.namespaces.get("gh").unwrap();
    assert_eq!(namespaces.len(), 2);
    assert_eq!(namespaces[0].name, "sase-org");
    assert_eq!(namespaces[0].description, "2 enabled projects");
    assert_eq!(namespaces[0].kind_label, "org");
    assert_eq!(namespaces[1].name, "bbugyi200");
    assert_eq!(namespaces[1].kind_label, "org");
}

#[test]
fn loads_v4_vcs_project_catalog_with_patch_entry_kind() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("vcs_project_catalog.json");
    fs::write(
        &catalog_path,
        r##"{
            "schema_version": 4,
            "workflow_names": ["gh"],
            "entries": [
                {
                    "name": "ship-completion",
                    "vcs_prefix": "gh",
                    "display_tag": "#gh:ship-completion",
                    "provider_display": "GitHub",
                    "description": "",
                    "aliases": [],
                    "entry_kind": "patch",
                    "kind": "changespec",
                    "project": "sase",
                    "status": "Ready"
                }
            ]
        }"##,
    )
    .unwrap();

    let catalog = load_vcs_project_catalog(Some(&catalog_path));

    assert_eq!(catalog.workflow_names, vec!["gh"]);
    assert_eq!(catalog.entries.len(), 1);
    assert_eq!(catalog.entries[0].entry_kind, "patch");
    assert_eq!(catalog.entries[0].kind, "changespec");
    assert_eq!(catalog.entries[0].project, "sase");
    assert_eq!(catalog.entries[0].status, "Ready");
}

#[test]
fn loads_v4_vcs_project_catalog_with_entry_kind_and_no_legacy_kind() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("vcs_project_catalog.json");
    fs::write(
        &catalog_path,
        r##"{
            "schema_version": 4,
            "workflow_names": ["gh"],
            "entries": [
                {
                    "name": "ship-completion",
                    "vcs_prefix": "gh",
                    "display_tag": "#gh:ship-completion",
                    "provider_display": "GitHub",
                    "description": "",
                    "aliases": [],
                    "entry_kind": "patch",
                    "project": "sase",
                    "status": "Ready"
                }
            ]
        }"##,
    )
    .unwrap();

    let catalog = load_vcs_project_catalog(Some(&catalog_path));

    assert_eq!(catalog.entries.len(), 1);
    assert_eq!(catalog.entries[0].entry_kind, "patch");
    assert!(catalog.entries[0].kind.is_empty());
    assert_eq!(catalog.entries[0].name, "ship-completion");
}

#[test]
fn loads_v5_vcs_project_catalog_with_palette_and_tags() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("vcs_project_catalog.json");
    fs::write(
        &catalog_path,
        r##"{
            "schema_version": 5,
            "workflow_names": ["gh", "git"],
            "entries": [
                {
                    "name": "sase",
                    "vcs_prefix": "gh",
                    "display_tag": "#gh:gh_sase-org__sase",
                    "provider_display": "GitHub",
                    "description": "SASE repo",
                    "aliases": ["sa"],
                    "entry_kind": "project",
                    "kind": "project",
                    "project": "sase",
                    "status": "",
                    "key": "gh_sase-org__sase",
                    "tag": "+sase",
                    "accent_index": 2,
                    "current": true
                }
            ],
            "namespaces": {},
            "accent_palette": ["#ff0000", "#00ff00", "#0000ff"],
            "project_tags": [
                {
                    "key": "gh_sase-org__sase",
                    "name": "sase",
                    "aliases": ["sa"],
                    "workflow_type": "gh"
                },
                {
                    "key": "home",
                    "name": "home"
                }
            ]
        }"##,
    )
    .unwrap();

    let catalog = load_vcs_project_catalog(Some(&catalog_path));

    assert_eq!(catalog.entries.len(), 1);
    assert_eq!(catalog.entries[0].key, "gh_sase-org__sase");
    assert_eq!(catalog.entries[0].tag, "+sase");
    assert_eq!(catalog.entries[0].accent_index, Some(2));
    assert_eq!(catalog.entries[0].current, Some(true));
    assert_eq!(catalog.accent_palette.len(), 3);
    assert_eq!(catalog.project_tags.len(), 2);
    assert_eq!(catalog.project_tags[0].workflow_type.as_deref(), Some("gh"));
    assert_eq!(catalog.project_tags[1].workflow_type, None);
}

#[test]
fn pre_v5_vcs_project_catalogs_default_palette_and_tags() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("vcs_project_catalog.json");
    fs::write(
        &catalog_path,
        r#"{"schema_version": 4, "workflow_names": ["gh"], "entries": []}"#,
    )
    .unwrap();

    let catalog = load_vcs_project_catalog(Some(&catalog_path));

    assert!(catalog.accent_palette.is_empty());
    assert!(catalog.project_tags.is_empty());
}

#[test]
fn load_vcs_project_catalog_ignores_malformed_namespaces() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("vcs_project_catalog.json");
    fs::write(
        &catalog_path,
        r##"{
            "schema_version": 3,
            "workflow_names": ["gh"],
            "entries": [
                {
                    "name": "sase",
                    "vcs_prefix": "gh",
                    "display_tag": "#gh:sase",
                    "provider_display": "GitHub"
                }
            ],
            "namespaces": ["not", "a", "map"]
        }"##,
    )
    .unwrap();

    let catalog = load_vcs_project_catalog(Some(&catalog_path));

    assert_eq!(catalog.workflow_names, vec!["gh"]);
    assert_eq!(catalog.entries.len(), 1);
    assert!(catalog.namespaces.is_empty());
}

#[test]
fn load_vcs_project_catalog_rejects_unknown_schema() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("vcs_project_catalog.json");
    fs::write(
        &catalog_path,
        r#"{"schema_version": 99, "workflow_names": ["gh"], "entries": []}"#,
    )
    .unwrap();

    let catalog = load_vcs_project_catalog(Some(&catalog_path));

    assert!(catalog.entries.is_empty());
    assert!(catalog.workflow_names.is_empty());
    assert!(catalog.namespaces.is_empty());
}

#[test]
fn artifact_catalog_loader_is_tolerant_and_schema_gated() {
    let temp = tempfile::tempdir().unwrap();
    let catalog_path = temp.path().join("artifact_ref_catalog.json");

    assert_eq!(
        load_artifact_ref_catalog(None),
        ArtifactRefCatalog::default()
    );
    fs::write(&catalog_path, "{not json").unwrap();
    assert_eq!(
        load_artifact_ref_catalog(Some(&catalog_path)),
        ArtifactRefCatalog::default()
    );
    fs::write(&catalog_path, r#"{"schema_version":99,"projects":[]}"#).unwrap();
    assert_eq!(
        load_artifact_ref_catalog(Some(&catalog_path)),
        ArtifactRefCatalog::default()
    );
    fs::write(
        &catalog_path,
        r#"{
            "schema_version": 1,
            "default_project": "sase",
            "projects": [
                {"name": "broken"},
                {
                    "name": "sase",
                    "key": "key_sase",
                    "context": {"schema_version": 1, "document_roots": []}
                }
            ]
        }"#,
    )
    .unwrap();

    let catalog = load_artifact_ref_catalog(Some(&catalog_path));

    assert_eq!(catalog.default_project.as_deref(), Some("sase"));
    assert_eq!(catalog.projects.len(), 1);
    assert_eq!(catalog.projects[0].key, "key_sase");
}

#[tokio::test]
async fn completes_artifact_kinds_and_local_payloads_per_active_project() {
    let temp = tempfile::tempdir().unwrap();
    let artifact_path = temp.path().join("artifact_ref_catalog.json");
    let vcs_path = temp.path().join("vcs_project_catalog.json");
    write_artifact_ref_catalog(&artifact_path, temp.path(), Some("local"));
    write_vcs_ref_catalog(&vcs_path);
    for project in ["sase", "local"] {
        let project_root = temp.path().join(project);
        fs::create_dir_all(project_root.join("designs")).unwrap();
        fs::create_dir_all(project_root.join("chats/202607")).unwrap();
        fs::write(
            project_root.join("designs").join(format!("{project}.md")),
            project,
        )
        .unwrap();
        fs::write(project_root.join("chats/202607/agent.md"), project).unwrap();
        fs::write(
            project_root.join("artifact-index.jsonl"),
            format!(
                "{{\"schema_version\":1,\"artifact\":{{\"id\":\"default:52895d68931185056fd0e49f\",\"path\":\"/{project}/image.png\"}}}}\n"
            ),
        )
        .unwrap();
    }
    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();
    {
        let mut config = server.config.write().unwrap();
        config.artifact_ref_catalog = Some(artifact_path.clone());
        config.vcs_project_catalog = Some(vcs_path);
        config.project = Some("sase_18".to_string());
    }

    for (text, expected) in [
        ("#gh:ship-completion @designs:sa", "@designs:sase.md"),
        ("#git:local @designs:lo", "@designs:local.md"),
        ("@designs:lo", "@designs:local.md"),
        ("@chat:202607/a", "@chat:202607/agent.md"),
        ("@file:default:", "@file:default:52895d68931185056fd0e49f"),
    ] {
        let items = completion_items(
            server
                .completion_for_text(
                    text.to_string(),
                    Position::new(0, text.len() as u32),
                )
                .await
                .unwrap(),
        );
        assert_eq!(items.len(), 1, "{text}: {items:?}");
        assert_eq!(items[0].label, expected, "{text}");
    }

    let kind_items = completion_items(
        server
            .completion_for_text("@de".to_string(), Position::new(0, 3))
            .await
            .unwrap(),
    );
    assert_eq!(kind_items.len(), 1);
    assert_eq!(kind_items[0].label, "@designs:");

    let payload_text = "@designs:lo";
    let payload_items = completion_items(
        server
            .completion_for_text(
                payload_text.to_string(),
                Position::new(0, payload_text.len() as u32),
            )
            .await
            .unwrap(),
    );
    let Some(CompletionTextEdit::Edit(edit)) =
        payload_items[0].text_edit.as_ref()
    else {
        panic!("expected artifact payload text edit");
    };
    assert_eq!(edit.range.start, Position::new(0, 0));
    assert_eq!(edit.range.end, Position::new(0, 11));

    for text in ["@commit:sase@0123456", "@bug:sase#1"] {
        let items = completion_items(
            server
                .completion_for_text(
                    text.to_string(),
                    Position::new(0, text.len() as u32),
                )
                .await
                .unwrap(),
        );
        assert!(items.is_empty(), "{text}: {items:?}");
    }

    write_artifact_ref_catalog(&artifact_path, temp.path(), None);
    let items = completion_items(
        server
            .completion_for_text(
                "@designs:sa".to_string(),
                Position::new(0, 11),
            )
            .await
            .unwrap(),
    );
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].label, "@designs:sase.md");
}

fn git(repo: &Path, args: &[&str]) {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn init_commit_git_repo(repo: &Path) {
    fs::create_dir_all(repo).unwrap();
    git(repo, &["init", "--quiet"]);
    git(repo, &["config", "user.name", "Commit Test"]);
    git(repo, &["config", "user.email", "commit@example.com"]);
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args([
            "commit",
            "--quiet",
            "--allow-empty",
            "-m",
            "fix(stats): expose occupancy",
        ])
        .env("GIT_AUTHOR_DATE", "1700000000 +0000")
        .env("GIT_COMMITTER_DATE", "1700000000 +0000")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git commit failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn completes_commit_payloads_from_a_real_git_checkout() {
    let temp = tempfile::tempdir().unwrap();
    let checkout = temp.path().join("sase-core-checkout");
    init_commit_git_repo(&checkout);

    let artifact_path = temp.path().join("artifact_ref_catalog.json");
    fs::write(
        &artifact_path,
        serde_json::to_vec(&serde_json::json!({
            "schema_version": 1,
            "default_project": "sase",
            "projects": [{
                "name": "sase",
                "key": "key_sase",
                "context": {
                    "schema_version": 1,
                    "repositories": [{
                        "name": "sase-core",
                        "checkout_paths": [checkout.to_string_lossy()],
                    }],
                },
            }],
        }))
        .unwrap(),
    )
    .unwrap();

    let (service, _) = LspService::new(|client| {
        XpromptLspServer::with_bridge(
            client,
            Arc::new(bridge_with_catalog_entries(Vec::new())),
        )
    });
    let server = service.inner();
    server.config.write().unwrap().artifact_ref_catalog = Some(artifact_path);

    // The regression this phase fixes: `commit` used to short-circuit to
    // an empty inventory before the payload cache was ever consulted.
    let text = "@commit:sase-core@fix";
    let items = completion_items(
        server
            .completion_for_text(
                text.to_string(),
                Position::new(0, text.len() as u32),
            )
            .await
            .unwrap(),
    );
    assert!(!items.is_empty(), "{text}: expected ranked commit items");
    assert!(items[0].label.starts_with("@commit:sase-core@"));
    assert_eq!(items[0].kind, Some(CompletionItemKind::REFERENCE));
    assert_eq!(
        items[0]
            .label_details
            .as_ref()
            .and_then(|details| details.description.as_deref()),
        Some("commit")
    );
}
