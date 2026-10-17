//! Temporary Git repositories shared by project-stats phase tests.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use super::wire::{ProjectStatsRepoKindWire, ProjectStatsRepoSpecWire};

pub(crate) struct Fixture {
    _temp: tempfile::TempDir,
    pub(crate) repo: PathBuf,
}

pub(crate) fn init_repo() -> Fixture {
    let temp = tempfile::tempdir().expect("temporary directory");
    let repo = temp.path().join("repo");
    fs::create_dir_all(&repo).expect("create repository directory");
    git(&repo, &["init", "--initial-branch=main"]);
    git(&repo, &["config", "user.name", "SASE Test"]);
    git(&repo, &["config", "user.email", "sase@example.test"]);
    git(&repo, &["config", "commit.gpgsign", "false"]);
    Fixture { _temp: temp, repo }
}

pub(crate) fn git(repo: &Path, args: &[&str]) -> String {
    let output = git_output(repo, args);
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

pub(crate) fn git_output(repo: &Path, args: &[&str]) -> Output {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .env_remove("GIT_OBJECT_DIRECTORY")
        .env_remove("GIT_ALTERNATE_OBJECT_DIRECTORIES")
        .env_remove("GIT_COMMON_DIR")
        .env_remove("GIT_NAMESPACE")
        .output()
        .expect("run git");
    output
}

pub(crate) fn write_file(repo: &Path, relative: &str, text: &str) {
    let path = repo.join(relative);
    fs::create_dir_all(path.parent().expect("file parent"))
        .expect("create parent directory");
    fs::write(path, text).expect("write fixture file");
}

pub(crate) fn commit(repo: &Path, relative: &str, text: &str, message: &str) {
    write_file(repo, relative, text);
    git(repo, &["add", "-A"]);
    git(repo, &["commit", "-m", message]);
}

pub(crate) fn spec(path: &Path) -> ProjectStatsRepoSpecWire {
    ProjectStatsRepoSpecWire {
        id: "fixture".to_owned(),
        name: "fixture repository".to_owned(),
        kind: ProjectStatsRepoKindWire::Primary,
        path: path.to_string_lossy().into_owned(),
        identity: "https://example.test/project/fixture.git".to_owned(),
        ref_candidates: vec!["main".to_owned(), "HEAD".to_owned()],
    }
}
