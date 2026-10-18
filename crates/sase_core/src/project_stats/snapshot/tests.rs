use std::fs;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

use super::{cache, measure_code_snapshot, CodeSnapshot};
use crate::project_stats::classify::MAX_CLASSIFIED_BLOB_BYTES;
use crate::project_stats::git::resolve_repo;
use crate::project_stats::test_support::{
    commit, commit_all, git, init_repo, spec, stage_blob_at_raw_path,
    write_bytes, write_file, Fixture,
};
use crate::project_stats::wire::{
    ProjectStatsBudgetWire, ProjectStatsCodeCounterWire,
    ProjectStatsExclusionReasonWire, ProjectStatsFileExclusionWire,
    ProjectStatsFileRoleWire, ProjectStatsSnapshotWire,
};

fn budget() -> ProjectStatsBudgetWire {
    ProjectStatsBudgetWire::default()
}

fn cache_dir() -> tempfile::TempDir {
    tempfile::tempdir().expect("cache directory")
}

fn measure_at(
    repo: &Path,
    cache: &Path,
) -> Result<CodeSnapshot, crate::project_stats::wire::ProjectStatsIssueWire> {
    cache::clear_memo();
    let spec = spec(repo);
    let resolved = resolve_repo(&spec, &budget()).expect("resolve");
    measure_code_snapshot(
        &resolved,
        &spec,
        &budget(),
        &cache.to_string_lossy(),
        ProjectStatsCodeCounterWire::Nonblank,
    )
}

fn measure(repo: &Path, cache: &Path) -> CodeSnapshot {
    measure_at(repo, cache).expect("snapshot")
}

fn role_files(
    snapshot: &ProjectStatsSnapshotWire,
    role: ProjectStatsFileRoleWire,
) -> u64 {
    snapshot
        .role_counts
        .as_ref()
        .expect("role counts")
        .iter()
        .find(|row| row.role == role)
        .map(|row| row.files)
        .unwrap_or(0)
}

fn role_lines(
    snapshot: &ProjectStatsSnapshotWire,
    role: ProjectStatsFileRoleWire,
) -> u64 {
    snapshot
        .role_counts
        .as_ref()
        .expect("role counts")
        .iter()
        .find(|row| row.role == role)
        .map(|row| row.lines)
        .unwrap_or(0)
}

fn exclusion_files(
    snapshot: &ProjectStatsSnapshotWire,
    exclusion: ProjectStatsFileExclusionWire,
) -> u64 {
    snapshot
        .exclusion_counts
        .as_ref()
        .expect("exclusion counts")
        .iter()
        .find(|row| row.exclusion == exclusion)
        .map(|row| row.files)
        .unwrap_or(0)
}

fn assert_partition(snapshot: &ProjectStatsSnapshotWire) {
    let included: u64 = snapshot
        .role_counts
        .as_ref()
        .expect("role counts")
        .iter()
        .map(|row| row.files)
        .sum();
    let excluded: u64 = snapshot
        .exclusion_counts
        .as_ref()
        .expect("exclusion counts")
        .iter()
        .map(|row| row.files)
        .sum();
    assert_eq!(
        included + excluded + snapshot.skipped_symlinks.unwrap_or(0),
        snapshot.tracked_files
    );
}

fn seed_repo() -> Fixture {
    let fixture = init_repo();
    commit(
        &fixture.repo,
        "src/lib.rs",
        "fn main() {\n    println!(\"hi\");\n}\n",
        "source",
    );
    write_file(&fixture.repo, "src/lib_test.rs", "fn test_it() {}\n");
    write_file(&fixture.repo, "README.md", "hello docs\n");
    write_file(&fixture.repo, "AGENTS.md", "agent notes\n");
    write_file(&fixture.repo, "Cargo.toml", "[package]\nname = \"x\"\n");
    write_file(&fixture.repo, "notes.unknown", "plain other text\n");
    write_file(&fixture.repo, "generated/out.rs", "fn generated() {}\n");
    write_file(&fixture.repo, "vendor/dep.rs", "fn vendored() {}\n");
    commit_all(&fixture.repo, "roles");
    fixture
}

#[test]
fn empty_repository_is_a_valid_empty_snapshot() {
    let fixture = init_repo();
    let cache = cache_dir();
    let snapshot = measure(&fixture.repo, cache.path());
    assert!(snapshot.tree_oid.is_empty());
    assert_eq!(snapshot.snapshot.tracked_files, 0);
    assert_eq!(snapshot.snapshot.tracked_lines, Some(0));
    assert_eq!(snapshot.snapshot.languages.as_ref().map(Vec::len), Some(0));
    assert_eq!(snapshot.blob_oids_requested.len(), 0);
    assert_partition(&snapshot.snapshot);
}

#[test]
fn measures_every_role_and_skips_gitlinks_and_symlinks() {
    let fixture = seed_repo();
    std::os::unix::fs::symlink("src/lib.rs", fixture.repo.join("alias.rs"))
        .expect("symlink");
    commit_all(&fixture.repo, "symlink");
    let head = git(&fixture.repo, &["rev-parse", "HEAD"]);
    git(
        &fixture.repo,
        &[
            "update-index",
            "--add",
            "--cacheinfo",
            &format!("160000,{head},vendor/sub"),
        ],
    );
    git(&fixture.repo, &["commit", "-m", "gitlink"]);

    let cache = cache_dir();
    let snapshot = measure(&fixture.repo, cache.path());
    assert_partition(&snapshot.snapshot);
    assert_eq!(
        role_files(&snapshot.snapshot, ProjectStatsFileRoleWire::Source),
        1
    );
    assert_eq!(
        role_files(&snapshot.snapshot, ProjectStatsFileRoleWire::Tests),
        1
    );
    assert_eq!(
        role_files(&snapshot.snapshot, ProjectStatsFileRoleWire::Docs),
        1
    );
    assert_eq!(
        role_files(&snapshot.snapshot, ProjectStatsFileRoleWire::AgentContext),
        1
    );
    assert_eq!(
        role_files(&snapshot.snapshot, ProjectStatsFileRoleWire::ConfigData),
        1
    );
    assert_eq!(
        role_files(&snapshot.snapshot, ProjectStatsFileRoleWire::Other),
        1
    );
    assert_eq!(
        exclusion_files(
            &snapshot.snapshot,
            ProjectStatsFileExclusionWire::Generated
        ),
        1
    );
    assert_eq!(
        exclusion_files(
            &snapshot.snapshot,
            ProjectStatsFileExclusionWire::Vendored
        ),
        1
    );
    assert_eq!(snapshot.snapshot.skipped_symlinks, Some(1));
    assert_eq!(snapshot.snapshot.skipped_gitlinks, Some(1));
    assert_eq!(
        snapshot.snapshot.tracked_files,
        8 + 1 // roles+exclusions plus symlink; gitlink is omitted
    );
    assert!(snapshot
        .snapshot
        .languages
        .as_ref()
        .expect("languages")
        .iter()
        .any(|row| row.name == "rust" && row.files == 2));
}

#[test]
fn odd_filenames_and_non_utf8_paths_are_preserved() {
    let fixture = init_repo();
    write_file(&fixture.repo, "src/odd name.rs", "fn spaced() {}\n");
    // The non-UTF-8 name goes straight into the index: macOS rejects it on
    // disk (EILSEQ), and the measured snapshot reads only the committed tree.
    git(&fixture.repo, &["add", "src/odd name.rs"]);
    stage_blob_at_raw_path(
        &fixture.repo,
        std::ffi::OsStr::from_bytes(b"src/\xff.rs"),
        b"fn bytes() {}\n",
    );
    git(&fixture.repo, &["commit", "-m", "odd names"]);
    let cache = cache_dir();
    let snapshot = measure(&fixture.repo, cache.path());
    assert_eq!(
        role_files(&snapshot.snapshot, ProjectStatsFileRoleWire::Source),
        2
    );
    assert_partition(&snapshot.snapshot);
}

#[test]
fn committed_attributes_win_over_dirty_worktree_and_info_attributes() {
    let fixture = init_repo();
    write_file(
        &fixture.repo,
        ".gitattributes",
        "src/keep.rs linguist-generated=false\n",
    );
    write_file(&fixture.repo, "src/keep.rs", "fn keep() {}\n");
    commit_all(&fixture.repo, "attrs");
    write_file(
        &fixture.repo,
        ".gitattributes",
        "src/keep.rs linguist-generated\n",
    );
    write_file(
        &fixture.repo,
        ".git/info/attributes",
        "src/keep.rs linguist-generated\n",
    );
    write_file(&fixture.repo, "src/keep.rs", "fn dirty() {}\n");

    let cache = cache_dir();
    let snapshot = measure(&fixture.repo, cache.path());
    assert_eq!(
        role_files(&snapshot.snapshot, ProjectStatsFileRoleWire::Source),
        1
    );
    assert_eq!(
        exclusion_files(
            &snapshot.snapshot,
            ProjectStatsFileExclusionWire::Generated
        ),
        0
    );
    assert_eq!(
        role_lines(&snapshot.snapshot, ProjectStatsFileRoleWire::Source),
        1
    );
}

#[test]
fn explicit_false_overrides_suppress_path_heuristics() {
    let fixture = init_repo();
    write_file(
        &fixture.repo,
        ".gitattributes",
        "vendor/keep.rs linguist-vendored=false\n",
    );
    write_file(&fixture.repo, "vendor/keep.rs", "fn keep() {}\n");
    commit_all(&fixture.repo, "override");
    let cache = cache_dir();
    let snapshot = measure(&fixture.repo, cache.path());
    assert_eq!(
        role_files(&snapshot.snapshot, ProjectStatsFileRoleWire::Source),
        1
    );
    assert_eq!(
        exclusion_files(
            &snapshot.snapshot,
            ProjectStatsFileExclusionWire::Vendored
        ),
        0
    );
}

#[test]
fn binary_lfs_and_oversize_blobs_are_excluded_without_being_zero() {
    let fixture = init_repo();
    write_bytes(&fixture.repo, "src/data.bin", b"a\0b\n");
    write_file(
        &fixture.repo,
        "src/large.rs",
        &format!(
            "fn x() {{ {} }}\n",
            "a".repeat((MAX_CLASSIFIED_BLOB_BYTES as usize) + 8)
        ),
    );
    write_file(
        &fixture.repo,
        "src/ptr.bin",
        "version https://git-lfs.github.com/spec/v1\noid sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\nsize 4\n",
    );
    commit_all(&fixture.repo, "excluded");
    let cache = cache_dir();
    let snapshot = measure(&fixture.repo, cache.path());
    assert_eq!(
        exclusion_files(
            &snapshot.snapshot,
            ProjectStatsFileExclusionWire::Binary
        ),
        2
    );
    assert_eq!(
        exclusion_files(
            &snapshot.snapshot,
            ProjectStatsFileExclusionWire::Generated
        ),
        1
    );
    let generated = snapshot
        .snapshot
        .exclusion_counts
        .as_ref()
        .expect("exclusions")
        .iter()
        .find(|row| row.exclusion == ProjectStatsFileExclusionWire::Generated)
        .expect("generated");
    assert_eq!(
        generated.reasons[0].reason,
        ProjectStatsExclusionReasonWire::Oversize
    );
    assert_eq!(snapshot.snapshot.tracked_lines, Some(0));
    assert_partition(&snapshot.snapshot);
}

#[test]
fn zero_code_repository_keeps_empty_language_list() {
    let fixture = init_repo();
    commit(&fixture.repo, "README.md", "docs only\n", "docs");
    let cache = cache_dir();
    let snapshot = measure(&fixture.repo, cache.path());
    assert_eq!(
        role_files(&snapshot.snapshot, ProjectStatsFileRoleWire::Source),
        0
    );
    assert_eq!(
        role_files(&snapshot.snapshot, ProjectStatsFileRoleWire::Docs),
        1
    );
    assert_eq!(snapshot.snapshot.languages.as_ref().map(Vec::len), Some(0));
}

#[test]
fn truncated_tree_listing_is_unavailable_not_a_zero_snapshot() {
    let fixture = seed_repo();
    let spec = spec(&fixture.repo);
    let resolved = resolve_repo(&spec, &budget()).expect("resolve");
    let tight = ProjectStatsBudgetWire {
        max_output_bytes: 16,
        ..ProjectStatsBudgetWire::default()
    };
    let error = measure_code_snapshot(
        &resolved,
        &spec,
        &tight,
        "",
        ProjectStatsCodeCounterWire::Nonblank,
    )
    .expect_err("tiny budget cannot list the tree");
    assert_eq!(error.code, "tree_unavailable");
}

#[test]
fn identical_tree_reuses_the_complete_cache_without_rereading_blobs() {
    let fixture = seed_repo();
    let cache = cache_dir();
    let cold = measure(&fixture.repo, cache.path());
    assert!(!cold.blob_oids_requested.is_empty());
    let warm = measure(&fixture.repo, cache.path());
    assert_eq!(warm.tree_oid, cold.tree_oid);
    assert_eq!(warm.snapshot, cold.snapshot);
    assert!(warm.blob_oids_requested.is_empty());
}

#[test]
fn content_edit_rereads_only_the_changed_blob() {
    let fixture = seed_repo();
    let cache = cache_dir();
    let cold = measure(&fixture.repo, cache.path());
    commit(
        &fixture.repo,
        "src/lib.rs",
        "fn main() {\n    println!(\"changed\");\n}\n",
        "edit",
    );
    let warm = measure(&fixture.repo, cache.path());
    assert_eq!(warm.blob_oids_requested.len(), 1);
    assert_ne!(warm.tree_oid, cold.tree_oid);
    assert_eq!(
        warm.snapshot,
        measure(&fixture.repo, cache_dir().path()).snapshot
    );
}

#[test]
fn deletion_and_rename_reuse_unchanged_blob_oids() {
    let fixture = seed_repo();
    let cache = cache_dir();
    let _cold = measure(&fixture.repo, cache.path());
    git(&fixture.repo, &["rm", "notes.unknown"]);
    git(&fixture.repo, &["mv", "src/lib.rs", "src/main.rs"]);
    commit_all(&fixture.repo, "rename and delete");
    let warm = measure(&fixture.repo, cache.path());
    assert!(
        warm.blob_oids_requested.is_empty(),
        "rename of an already counted blob must not reread it: {:?}",
        warm.blob_oids_requested
    );
    assert_eq!(
        role_files(&warm.snapshot, ProjectStatsFileRoleWire::Source),
        1
    );
    assert_eq!(
        role_files(&warm.snapshot, ProjectStatsFileRoleWire::Other),
        0
    );
    let cold = measure(&fixture.repo, cache_dir().path());
    assert_eq!(warm.snapshot, cold.snapshot);
}

#[test]
fn attribute_only_commit_reclassifies_without_rereading_unchanged_blobs() {
    let fixture = init_repo();
    commit(&fixture.repo, "src/lib.rs", "fn main() {}\n", "source");
    let cache = cache_dir();
    let _cold = measure(&fixture.repo, cache.path());
    let source_oid = git(&fixture.repo, &["rev-parse", "HEAD:src/lib.rs"]);
    write_file(
        &fixture.repo,
        ".gitattributes",
        "src/lib.rs linguist-generated\n",
    );
    commit_all(&fixture.repo, "attrs only");
    let warm = measure(&fixture.repo, cache.path());
    assert!(
        !warm
            .blob_oids_requested
            .iter()
            .any(|oid| oid.eq_ignore_ascii_case(&source_oid)),
        "unchanged source blob must not be reread: {:?}",
        warm.blob_oids_requested
    );
    assert_eq!(
        exclusion_files(
            &warm.snapshot,
            ProjectStatsFileExclusionWire::Generated
        ),
        1
    );
    assert_eq!(
        role_files(&warm.snapshot, ProjectStatsFileRoleWire::Source),
        0
    );
}

#[test]
fn newly_included_blob_that_was_previously_skipped_is_read() {
    let fixture = init_repo();
    commit(
        &fixture.repo,
        "generated/keep.rs",
        "fn keep() {}\n",
        "generated",
    );
    let cache = cache_dir();
    let cold = measure(&fixture.repo, cache.path());
    assert!(cold.blob_oids_requested.is_empty());
    fs::create_dir_all(fixture.repo.join("src")).expect("src directory");
    git(&fixture.repo, &["mv", "generated/keep.rs", "src/keep.rs"]);
    commit_all(&fixture.repo, "include");
    let warm = measure(&fixture.repo, cache.path());
    assert_eq!(warm.blob_oids_requested.len(), 1);
    assert_eq!(
        role_files(&warm.snapshot, ProjectStatsFileRoleWire::Source),
        1
    );
}

#[test]
fn corrupt_cache_and_wrong_classifier_version_rebuild() {
    let fixture = seed_repo();
    let cache = cache_dir();
    let cold = measure(&fixture.repo, cache.path());
    let key_files: Vec<PathBuf> = fs::read_dir(cache.path().join("v1"))
        .expect("cache dir")
        .map(|entry| entry.expect("entry").path())
        .filter(|path| {
            path.extension().and_then(|ext| ext.to_str()) == Some("json")
        })
        .collect();
    assert_eq!(key_files.len(), 1);
    fs::write(&key_files[0], "{not json").expect("corrupt");
    cache::clear_memo();
    let recovered = measure(&fixture.repo, cache.path());
    assert_eq!(recovered.snapshot, cold.snapshot);
    assert!(!recovered.blob_oids_requested.is_empty());

    let mut entry: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&key_files[0]).expect("read"))
            .expect("json");
    entry["classifier_version"] = serde_json::json!(0);
    fs::write(&key_files[0], serde_json::to_vec(&entry).expect("write"))
        .expect("version");
    cache::clear_memo();
    let rebuilt = measure(&fixture.repo, cache.path());
    assert!(!rebuilt.blob_oids_requested.is_empty());
    assert_eq!(rebuilt.snapshot, cold.snapshot);
}

#[test]
fn local_user_attributes_file_does_not_contaminate_committed_policy() {
    let fixture = init_repo();
    commit(&fixture.repo, "src/lib.rs", "fn main() {}\n", "source");
    let attr_file = fixture.repo.join("user.attributes");
    fs::write(&attr_file, "src/lib.rs linguist-generated\n")
        .expect("user attrs");
    git(
        &fixture.repo,
        &[
            "config",
            "core.attributesFile",
            &attr_file.to_string_lossy(),
        ],
    );
    let cache = cache_dir();
    let snapshot = measure(&fixture.repo, cache.path());
    assert_eq!(
        role_files(&snapshot.snapshot, ProjectStatsFileRoleWire::Source),
        1
    );
    assert_eq!(
        exclusion_files(
            &snapshot.snapshot,
            ProjectStatsFileExclusionWire::Generated
        ),
        0
    );
}
