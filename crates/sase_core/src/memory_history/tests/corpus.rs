//! Shared fixture corpus for the memory-history phases.
//!
//! One project repo with a fixed 23-commit history (committer and
//! author dates pinned so order never depends on the clock) plus a
//! smaller home repo whose single note commit lands between two
//! project commits for the feed-merge test. Later phases reuse these
//! builders; the commit labels stay stable.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use super::super::wire::{
    MemoryHistoryInstructionFileWire, MemoryHistoryScopeKindWire,
    MemoryHistoryScopeWire,
};

/// Scope key of the project corpus.
pub const PROJECT_SCOPE_KEY: &str = "project:fixture";
/// Scope key of the home corpus. Built by later phases.
#[allow(dead_code)]
pub const HOME_SCOPE_KEY: &str = "home:fixture";
/// Epoch seconds of the first project commit.
pub const CORPUS_BASE_TIME: i64 = 1_700_000_000;
/// Seconds between project commits.
pub const CORPUS_STEP_SECS: i64 = 300;

/// Epoch seconds of project commit *index* (0-based, oldest first).
pub fn corpus_time(index: u32) -> i64 {
    CORPUS_BASE_TIME + index as i64 * CORPUS_STEP_SECS
}

/// A built corpus repo: the checkout, its scope, and commit SHAs by
/// label, oldest first in label order of construction.
pub struct Corpus {
    /// Temp dir owning the repo (kept alive by ownership).
    #[allow(dead_code)]
    pub tmp: tempfile::TempDir,
    /// The fixture repo checkout.
    pub repo: PathBuf,
    /// The scope later phases pass to core.
    pub scope: MemoryHistoryScopeWire,
    /// Commit SHAs by label.
    pub commits: BTreeMap<String, String>,
}

/// Scope for a project corpus checkout.
pub fn project_scope(repo: &Path, cache_dir: &Path) -> MemoryHistoryScopeWire {
    MemoryHistoryScopeWire {
        scope_key: PROJECT_SCOPE_KEY.to_string(),
        scope_kind: MemoryHistoryScopeKindWire::Project,
        repo_root: repo.to_string_lossy().into_owned(),
        memory_roots: vec!["sase/memory".to_string(), "memory".to_string()],
        instruction_files: vec![MemoryHistoryInstructionFileWire {
            dir: ".".to_string(),
            agents_path: "AGENTS.md".to_string(),
            shim_paths: vec!["CLAUDE.md".to_string()],
            template: false,
            managed: true,
        }],
        generated_notes: vec!["sase/memory/roster.md".to_string()],
        renderer_prefixes: vec!["src/sase/amd/".to_string()],
        config_paths: vec!["sase/sase.yml".to_string()],
        cache_dir: cache_dir.to_string_lossy().into_owned(),
    }
}

/// Scope for a home corpus checkout. Used by later phases.
#[allow(dead_code)]
fn home_scope(repo: &Path, cache_dir: &Path) -> MemoryHistoryScopeWire {
    MemoryHistoryScopeWire {
        scope_key: HOME_SCOPE_KEY.to_string(),
        scope_kind: MemoryHistoryScopeKindWire::Home,
        repo_root: repo.to_string_lossy().into_owned(),
        memory_roots: vec!["sase/memory".to_string(), "memory".to_string()],
        instruction_files: Vec::new(),
        generated_notes: Vec::new(),
        renderer_prefixes: Vec::new(),
        config_paths: Vec::new(),
        cache_dir: cache_dir.to_string_lossy().into_owned(),
    }
}

fn git(repo: &Path, args: &[&str]) -> String {
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
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

fn git_env(repo: &Path, args: &[&str], time: i64) -> String {
    let stamp = format!("{time} +0000");
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .env("GIT_AUTHOR_DATE", &stamp)
        .env("GIT_COMMITTER_DATE", &stamp)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

fn init_repo() -> (tempfile::TempDir, PathBuf) {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().join("repo");
    fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--initial-branch=master"]);
    git(&repo, &["config", "user.name", "SASE Test"]);
    git(&repo, &["config", "user.email", "sase@example.com"]);
    git(&repo, &["config", "commit.gpgsign", "false"]);
    (tmp, repo)
}

fn write_file(repo: &Path, rel: &str, contents: &[u8]) {
    let path = repo.join(rel);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, contents).unwrap();
}

fn commit(
    corpus: &mut Corpus,
    label: &str,
    message: &str,
    footers: &[&str],
    time: i64,
) {
    git(&corpus.repo, &["add", "-A"]);
    let mut args = vec!["commit", "-m", message];
    for footer in footers {
        args.push("-m");
        args.push(footer);
    }
    git_env(&corpus.repo, &args, time);
    let sha = git(&corpus.repo, &["rev-parse", "HEAD"]);
    corpus.commits.insert(label.to_string(), sha);
}

/// Sixty-line note body; *changed* indexes are replaced with
/// unrelated text (22 of 60 yields rename similarity R063, the same
/// shape as `file_history/tests.rs`).
fn note_body(changed: &[usize]) -> String {
    (0..60)
        .map(|index| {
            if changed.contains(&index) {
                format!(
                    "CHANGED {index:02} completely different words here now yes"
                )
            } else {
                format!(
                    "line {index:02} the quick brown fox jumps over the lazy dog"
                )
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n"
}

fn changed_22() -> Vec<usize> {
    vec![
        1, 4, 6, 9, 12, 15, 18, 21, 24, 27, 30, 33, 36, 39, 42, 45, 48, 51, 54,
        56, 58, 59,
    ]
}

fn policies_body(type_value: &str, changed_word: &str) -> String {
    format!(
        "---\ntype: {type_value}\n---\npolicies line 00 alpha beta gamma\npolicies line 01 {changed_word}\npolicies line 02 delta epsilon zeta\npolicies line 03 eta theta iota\npolicies line 04 kappa lambda mu\n"
    )
}

fn legacy_body(type_value: &str) -> String {
    format!(
        "---\ntype: {type_value}\n---\nlegacy line 00 alpha beta gamma\nlegacy line 01 delta epsilon zeta\nlegacy line 02 eta theta iota\n"
    )
}

fn scratch_body(title: &str, body: &str) -> String {
    format!("---\ntitle: {title}\n---\n{body}")
}

/// Build the project corpus: 23 commits, oldest first.
pub fn build_project_corpus() -> Corpus {
    let (tmp, repo) = init_repo();
    let cache_dir = tmp.path().join("cache");
    let scope = project_scope(&repo, &cache_dir);
    let mut corpus = Corpus {
        tmp,
        repo,
        scope,
        commits: BTreeMap::new(),
    };
    let mut clock: u32 = 0;
    let mut tick = || {
        let time = corpus_time(clock);
        clock += 1;
        time
    };

    // 1. Boilerplate init.
    write_file(
        &corpus.repo,
        "memory/build_and_run.md",
        note_body(&[]).as_bytes(),
    );
    write_file(
        &corpus.repo,
        "memory/policies.md",
        policies_body("reference", "bodily").as_bytes(),
    );
    write_file(
        &corpus.repo,
        "memory/legacy.md",
        legacy_body("core").as_bytes(),
    );
    write_file(
        &corpus.repo,
        "memory/scratch.md",
        scratch_body("Old", "scratch body v1\n").as_bytes(),
    );
    write_file(&corpus.repo, "AGENTS.md", b"project agents v1\n");
    write_file(&corpus.repo, "CLAUDE.md", b"claude era\n");
    commit(
        &mut corpus,
        "init",
        "chore: initialize sase memory",
        &[],
        tick(),
    );

    // 2. Promotion plus a body word change.
    write_file(
        &corpus.repo,
        "memory/policies.md",
        policies_body("core", "CHANGED").as_bytes(),
    );
    commit(
        &mut corpus,
        "promotion",
        "promote policies to core",
        &["SASE_AGENT=athena.sase-1au.5\nSASE_BEAD=sase-1au.5"],
        tick(),
    );

    // 3. Demotion with no body change.
    write_file(
        &corpus.repo,
        "memory/legacy.md",
        legacy_body("reference").as_bytes(),
    );
    commit(
        &mut corpus,
        "demotion",
        "demote legacy to reference",
        &[],
        tick(),
    );

    // 4. Reflow only: same words, rewrapped lines 10-11.
    let mut lines: Vec<String> =
        note_body(&[]).lines().map(ToString::to_string).collect();
    let words: Vec<String> = lines[10]
        .split_whitespace()
        .chain(lines[11].split_whitespace())
        .map(ToString::to_string)
        .collect();
    lines[10] = words[..6].join(" ");
    lines[11] = words[6..12].join(" ");
    lines.insert(12, words[12..].join(" "));
    write_file(
        &corpus.repo,
        "memory/build_and_run.md",
        (lines.join("\n") + "\n").as_bytes(),
    );
    commit(&mut corpus, "reflow", "rewrap build note", &[], tick());

    // 5. Whitespace only on scratch.
    write_file(
        &corpus.repo,
        "memory/scratch.md",
        scratch_body("Old", "scratch body v1   \n").as_bytes(),
    );
    commit(
        &mut corpus,
        "whitespace",
        "touch scratch whitespace",
        &[],
        tick(),
    );

    // 6. Frontmatter only on scratch.
    write_file(
        &corpus.repo,
        "memory/scratch.md",
        scratch_body("New", "scratch body v1   \n").as_bytes(),
    );
    commit(&mut corpus, "frontmatter", "retitle scratch", &[], tick());

    // 7. Pure directory rename.
    fs::create_dir_all(corpus.repo.join("sase")).unwrap();
    git(&corpus.repo, &["mv", "memory", "sase/memory"]);
    commit(
        &mut corpus,
        "dirmove",
        "move memory under sase",
        &[],
        tick(),
    );

    // 8. Content rename with 22 of 60 lines changed.
    git(
        &corpus.repo,
        &[
            "mv",
            "sase/memory/build_and_run.md",
            "sase/memory/lint_and_test.md",
        ],
    );
    write_file(
        &corpus.repo,
        "sase/memory/lint_and_test.md",
        note_body(&changed_22()).as_bytes(),
    );
    commit(
        &mut corpus,
        "content_rename",
        "rename build note to lint note",
        &[],
        tick(),
    );

    // 9. Delete scratch.
    git(&corpus.repo, &["rm", "-q", "sase/memory/scratch.md"]);
    git_env(&corpus.repo, &["commit", "-qm", "delete scratch"], tick());
    corpus.commits.insert(
        "delete_scratch".to_string(),
        git(&corpus.repo, &["rev-parse", "HEAD"]),
    );

    // 10. Recreate scratch with a new body.
    write_file(
        &corpus.repo,
        "sase/memory/scratch.md",
        scratch_body("New", "scratch body v2 totally new words here\n")
            .as_bytes(),
    );
    commit(
        &mut corpus,
        "recreate_scratch",
        "recreate scratch",
        &[],
        tick(),
    );

    // 11. Web: add descriptor plus strand, then move the strand.
    write_file(
        &corpus.repo,
        "sase/memory/glossary.md",
        b"---\ntype: reference\n---\nglossary terms live here\n",
    );
    write_file(
        &corpus.repo,
        "sase/memory/glossary/stitch.md",
        b"---\ntype: reference\n---\na stitch in time\n",
    );
    commit(
        &mut corpus,
        "web_add",
        "add glossary web and stitch strand",
        &[],
        tick(),
    );
    git(
        &corpus.repo,
        &[
            "mv",
            "sase/memory/glossary/stitch.md",
            "sase/memory/glossary/bead.md",
        ],
    );
    commit(
        &mut corpus,
        "strand_mv",
        "rename stitch strand to bead",
        &[],
        tick(),
    );

    // 12. Co-rendered note: one word of the lint note plus AGENTS.md.
    let mut lint: Vec<String> = note_body(&changed_22())
        .lines()
        .map(ToString::to_string)
        .collect();
    lint[0] =
        "line 00 the quick brown fox jumps over the sleepy dog".to_string();
    write_file(
        &corpus.repo,
        "sase/memory/lint_and_test.md",
        (lint.join("\n") + "\n").as_bytes(),
    );
    write_file(&corpus.repo, "AGENTS.md", b"project agents v2\n");
    commit(
        &mut corpus,
        "co_render",
        "feat(memory): edit lint note",
        &["SASE_AGENT=athena.sase-1bc.12\nSASE_BEAD=sase-1bc.12"],
        tick(),
    );

    // 13. Config: sase.yml plus AGENTS.md only.
    write_file(&corpus.repo, "sase/sase.yml", b"key: value\n");
    write_file(&corpus.repo, "AGENTS.md", b"project agents v3\n");
    commit(
        &mut corpus,
        "config",
        "tweak config and rerender",
        &[],
        tick(),
    );

    // 14. Renderer: render.rs plus AGENTS.md only.
    write_file(&corpus.repo, "src/sase/amd/render.rs", b"fn render() {}\n");
    write_file(&corpus.repo, "AGENTS.md", b"project agents v4\n");
    commit(
        &mut corpus,
        "renderer",
        "tweak renderer and rerender",
        &[],
        tick(),
    );

    // 15. Regen-only: AGENTS.md only.
    write_file(&corpus.repo, "AGENTS.md", b"project agents v5\n");
    commit(&mut corpus, "regen_only", "rerender agents", &[], tick());

    // 16. Generated consequence: add roster plus a lint edit, then a
    // roster-only edit.
    write_file(&corpus.repo, "sase/memory/roster.md", b"roster v1\n");
    lint[1] =
        "CHANGED 01 entirely rewritten words for the roster commit".to_string();
    write_file(
        &corpus.repo,
        "sase/memory/lint_and_test.md",
        (lint.join("\n") + "\n").as_bytes(),
    );
    commit(
        &mut corpus,
        "roster_add",
        "add roster and touch lint note",
        &[],
        tick(),
    );
    write_file(&corpus.repo, "sase/memory/roster.md", b"roster v2\n");
    commit(&mut corpus, "roster_edit", "update roster", &[], tick());

    // 17. Shim convergence, then divergence.
    let agents = fs::read(corpus.repo.join("AGENTS.md")).unwrap();
    write_file(&corpus.repo, "CLAUDE.md", &agents);
    commit(
        &mut corpus,
        "shim_converge",
        "align claude shim",
        &[],
        tick(),
    );
    write_file(&corpus.repo, "CLAUDE.md", b"claude diverged again\n");
    commit(
        &mut corpus,
        "shim_diverge",
        "hand edit claude shim",
        &[],
        tick(),
    );

    // 18. Asset.
    write_file(
        &corpus.repo,
        "sase/memory/diagram.png",
        &[0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x01],
    );
    commit(&mut corpus, "asset_add", "add diagram asset", &[], tick());

    // 19. Stays deleted: add gone.md, delete it next commit.
    write_file(&corpus.repo, "sase/memory/gone.md", b"gone\n");
    commit(&mut corpus, "gone_add", "add gone note", &[], tick());
    git(&corpus.repo, &["rm", "-q", "sase/memory/gone.md"]);
    git_env(&corpus.repo, &["commit", "-qm", "delete gone note"], tick());
    corpus.commits.insert(
        "gone_delete".to_string(),
        git(&corpus.repo, &["rev-parse", "HEAD"]),
    );

    corpus
}

/// Build the home corpus: one note commit at *commit_time*, chosen by
/// the caller between two project commits for the feed-merge test.
/// Used by later phases.
#[allow(dead_code)]
pub fn build_home_corpus(commit_time: i64) -> Corpus {
    let (tmp, repo) = init_repo();
    let cache_dir = tmp.path().join("cache");
    let scope = home_scope(&repo, &cache_dir);
    let mut corpus = Corpus {
        tmp,
        repo,
        scope,
        commits: BTreeMap::new(),
    };
    write_file(
        &corpus.repo,
        "sase/memory/home_note.md",
        b"home words here\n",
    );
    commit(&mut corpus, "note", "home note", &[], commit_time);
    corpus
}
