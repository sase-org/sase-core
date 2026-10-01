//! Classification tests over the shared fixture corpus.
//!
//! Each committed version gets one primary class from the §3
//! priority list, plus summary, volume, provenance, and the
//! boilerplate flag.

use super::super::classify::classify_subjects;
use super::super::subjects::{derive_subjects, memory_history_pathspecs};
use super::super::wire::{
    MemoryHistoryClassWire, MemoryHistorySubjectWire, MemoryHistoryVersionWire,
};
use super::corpus::{build_project_corpus, Corpus, PROJECT_SCOPE_KEY};
use crate::file_history::{build_index, FileHistoryBudgetWire};

fn classified(corpus: &Corpus) -> Vec<MemoryHistorySubjectWire> {
    let specs = memory_history_pathspecs(&corpus.scope).unwrap();
    let index =
        build_index(&corpus.repo, &specs, &FileHistoryBudgetWire::default())
            .unwrap();
    let mut subjects = derive_subjects(&index, &corpus.scope).unwrap();
    classify_subjects(&corpus.repo, &index, &mut subjects, &corpus.scope)
        .unwrap();
    subjects
}

fn subject<'a>(
    subjects: &'a [MemoryHistorySubjectWire],
    id: &str,
) -> &'a MemoryHistorySubjectWire {
    subjects
        .iter()
        .find(|entry| entry.id == id)
        .unwrap_or_else(|| panic!("missing subject {id}"))
}

fn at_commit<'a>(
    found: &'a MemoryHistorySubjectWire,
    commit: &str,
) -> &'a MemoryHistoryVersionWire {
    found
        .versions
        .iter()
        .find(|version| version.commit == commit)
        .unwrap_or_else(|| panic!("{} has no version at {commit}", found.id))
}

fn lint_id() -> String {
    format!("note:{}/lint_and_test", PROJECT_SCOPE_KEY)
}

fn key_id(kind: &str, name: &str) -> String {
    format!("{kind}:{PROJECT_SCOPE_KEY}/{name}")
}

#[test]
fn committed_versions_classify_per_priority_table() {
    let corpus = build_project_corpus();
    let subjects = classified(&corpus);
    let commits = &corpus.commits;

    let lint = subject(&subjects, &lint_id());
    let oldest = lint.versions.last().unwrap();
    assert_eq!(oldest.commit, commits["init"]);
    assert_eq!(oldest.class, MemoryHistoryClassWire::Created);
    assert!(!oldest.hidden_by_default);

    let instructions = subject(&subjects, &key_id("instructions", "."));
    assert_eq!(
        at_commit(instructions, &commits["init"]).class,
        MemoryHistoryClassWire::Created
    );

    let policies = subject(&subjects, &key_id("note", "policies"));
    let promoted = at_commit(policies, &commits["promotion"]);
    assert_eq!(promoted.class, MemoryHistoryClassWire::Promoted);
    assert!(!promoted.hidden_by_default);

    let legacy = subject(&subjects, &key_id("note", "legacy"));
    let demoted = at_commit(legacy, &commits["demotion"]);
    assert_eq!(demoted.class, MemoryHistoryClassWire::Demoted);
    assert!(!demoted.hidden_by_default);

    let reflow = at_commit(lint, &commits["reflow"]);
    assert_eq!(reflow.class, MemoryHistoryClassWire::Reflow);
    assert!(reflow.hidden_by_default);

    let scratch = subject(&subjects, &key_id("note", "scratch"));
    let whitespace = at_commit(scratch, &commits["whitespace"]);
    assert_eq!(whitespace.class, MemoryHistoryClassWire::Whitespace);
    assert!(whitespace.hidden_by_default);

    let title = at_commit(scratch, &commits["frontmatter"]);
    assert_eq!(title.class, MemoryHistoryClassWire::Frontmatter);
    assert!(!title.hidden_by_default);

    let dirmove = at_commit(lint, &commits["dirmove"]);
    assert_eq!(dirmove.class, MemoryHistoryClassWire::Moved);
    assert!(dirmove.hidden_by_default);

    let renamed = at_commit(lint, &commits["content_rename"]);
    assert_eq!(renamed.class, MemoryHistoryClassWire::Authored);
    assert!(!renamed.hidden_by_default);

    let deleted = at_commit(scratch, &commits["delete_scratch"]);
    assert_eq!(deleted.class, MemoryHistoryClassWire::Deleted);
    assert!(!deleted.hidden_by_default);
    let gone = subject(&subjects, &key_id("note", "gone"));
    assert_eq!(
        at_commit(gone, &commits["gone_delete"]).class,
        MemoryHistoryClassWire::Deleted
    );

    let recreated = at_commit(scratch, &commits["recreate_scratch"]);
    assert_eq!(recreated.class, MemoryHistoryClassWire::Created);
    assert!(!recreated.hidden_by_default);

    let bead = subject(&subjects, &key_id("strand", "glossary/bead"));
    let strand_mv = at_commit(bead, &commits["strand_mv"]);
    assert_eq!(strand_mv.class, MemoryHistoryClassWire::Moved);
    assert!(strand_mv.hidden_by_default);

    let co_rendered = at_commit(lint, &commits["co_render"]);
    assert_eq!(co_rendered.class, MemoryHistoryClassWire::Authored);

    let roster = subject(&subjects, &key_id("note", "roster"));
    assert_eq!(
        at_commit(roster, &commits["roster_edit"]).class,
        MemoryHistoryClassWire::Regenerated
    );

    let asset = subject(&subjects, &key_id("asset", "sase/memory/diagram.png"));
    let asset_add = at_commit(asset, &commits["asset_add"]);
    assert_eq!(asset_add.class, MemoryHistoryClassWire::Created);
    assert!(!asset_add.hidden_by_default);
}

#[test]
fn promotion_summary_phrase_and_word_counts() {
    let corpus = build_project_corpus();
    let subjects = classified(&corpus);
    let commits = &corpus.commits;

    let policies = subject(&subjects, &key_id("note", "policies"));
    let promoted = at_commit(policies, &commits["promotion"]);
    assert_eq!(
        promoted.summary.frontmatter_phrase.as_deref(),
        Some("promoted reference → core")
    );
    assert!(promoted.summary.words_added > 0);
    assert!(promoted.summary.words_removed > 0);
    assert_eq!(
        promoted.summary.volume,
        promoted.summary.words_added + promoted.summary.words_removed
    );

    let legacy = subject(&subjects, &key_id("note", "legacy"));
    let demoted = at_commit(legacy, &commits["demotion"]);
    assert_eq!(
        demoted.summary.frontmatter_phrase.as_deref(),
        Some("demoted core → reference")
    );
}

#[test]
fn reflow_has_zero_volume_and_empty_word_counts() {
    let corpus = build_project_corpus();
    let subjects = classified(&corpus);
    let lint = subject(&subjects, &lint_id());
    let reflow = at_commit(lint, &corpus.commits["reflow"]);
    assert_eq!(reflow.summary.volume, 0);
    assert_eq!(reflow.summary.words_added, 0);
    assert_eq!(reflow.summary.words_removed, 0);
}

#[test]
fn created_words_match_document_word_count() {
    let corpus = build_project_corpus();
    let subjects = classified(&corpus);
    let lint = subject(&subjects, &lint_id());
    let oldest = lint.versions.last().unwrap();
    assert_eq!(oldest.class, MemoryHistoryClassWire::Created);
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(&corpus.repo)
        .arg("show")
        .arg(format!(
            "{}:memory/build_and_run.md",
            corpus.commits["init"]
        ))
        .output()
        .unwrap();
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).unwrap();
    let expected = text.split_whitespace().count() as u64;
    assert!(expected > 0);
    assert_eq!(oldest.summary.created_words, Some(expected));
    assert_eq!(oldest.summary.volume, expected);
}

#[test]
fn provenance_agent_bead_types_and_boilerplate() {
    let corpus = build_project_corpus();
    let subjects = classified(&corpus);
    let commits = &corpus.commits;

    let policies = subject(&subjects, &key_id("note", "policies"));
    let promoted = at_commit(policies, &commits["promotion"]);
    assert_eq!(
        promoted.provenance.agent.as_deref(),
        Some("athena.sase-1au.5")
    );
    assert_eq!(promoted.provenance.bead.as_deref(), Some("sase-1au.5"));
    assert!(promoted
        .provenance
        .commit_types
        .contains(&"stitch".to_string()));
    assert_eq!(promoted.provenance.subject, "promote policies to core");

    let lint = subject(&subjects, &lint_id());
    let co_rendered = at_commit(lint, &commits["co_render"]);
    assert_eq!(
        co_rendered.provenance.conventional_type.as_deref(),
        Some("feat")
    );

    let oldest = lint.versions.last().unwrap();
    assert!(oldest.boilerplate);
    assert!(!co_rendered.boilerplate);
}

#[test]
fn content_rename_is_visible_and_pure_move_is_not_authored() {
    let corpus = build_project_corpus();
    let subjects = classified(&corpus);
    let lint = subject(&subjects, &lint_id());
    let renamed = at_commit(lint, &corpus.commits["content_rename"]);
    assert!(!renamed.hidden_by_default);
    assert_ne!(renamed.class, MemoryHistoryClassWire::Moved);

    let dirmove = at_commit(lint, &corpus.commits["dirmove"]);
    assert_ne!(dirmove.class, MemoryHistoryClassWire::Authored);
}

#[test]
fn frontmatter_only_phrase_names_changed_key() {
    let corpus = build_project_corpus();
    let subjects = classified(&corpus);
    let scratch = subject(&subjects, &key_id("note", "scratch"));
    let title = at_commit(scratch, &corpus.commits["frontmatter"]);
    assert_eq!(
        title.summary.frontmatter_phrase.as_deref(),
        Some("title: Old → New")
    );
}
