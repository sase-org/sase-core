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

pub(crate) fn commit_at(
    repo: &Path,
    relative: &str,
    text: &str,
    message: &str,
    author_ts: i64,
    committer_ts: i64,
) {
    write_file(repo, relative, text);
    git(repo, &["add", "-A"]);
    let author = format!("@{author_ts}");
    let committer = format!("@{committer_ts}");
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(["commit", "-m", message])
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .env_remove("GIT_OBJECT_DIRECTORY")
        .env_remove("GIT_ALTERNATE_OBJECT_DIRECTORIES")
        .env_remove("GIT_COMMON_DIR")
        .env_remove("GIT_NAMESPACE")
        .env("GIT_AUTHOR_DATE", &author)
        .env("GIT_COMMITTER_DATE", &committer)
        .output()
        .expect("run git commit");
    assert!(
        output.status.success(),
        "git commit failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

pub(crate) fn clone_repo(source: &Path) -> Fixture {
    let temp = tempfile::tempdir().expect("temporary directory");
    let repo = temp.path().join("repo");
    let output = Command::new("git")
        .args(["clone", "--quiet"])
        .arg(source)
        .arg(&repo)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .env_remove("GIT_OBJECT_DIRECTORY")
        .env_remove("GIT_ALTERNATE_OBJECT_DIRECTORIES")
        .env_remove("GIT_COMMON_DIR")
        .env_remove("GIT_NAMESPACE")
        .output()
        .expect("clone repository");
    assert!(
        output.status.success(),
        "git clone failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    git(&repo, &["config", "user.name", "SASE Test"]);
    git(&repo, &["config", "user.email", "sase@example.test"]);
    git(&repo, &["config", "commit.gpgsign", "false"]);
    Fixture { _temp: temp, repo }
}

pub(crate) fn agent_message(subject: &str, agent: &str, model: &str) -> String {
    format!("{subject}\n\nSASE_AGENT={agent}\nSASE_MODEL={model}\n")
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
