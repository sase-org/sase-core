//! Cause attribution tests over the shared fixture corpus.
//!
//! Classification leaves instruction versions without a memory source
//! as `regen_only`; attribution refines them into `config` when a
//! config or renderer path co-changed, and fills the cause lists from
//! one batched diff-tree.

use super::super::causes::attribute_instruction_causes;
use super::super::classify::classify_subjects;
use super::super::subjects::{derive_subjects, memory_history_pathspecs};
use super::super::wire::{
    MemoryHistoryCauseWire, MemoryHistoryClassWire, MemoryHistorySubjectWire,
    MemoryHistoryVersionWire,
};
use super::corpus::{build_project_corpus, Corpus, PROJECT_SCOPE_KEY};
use crate::file_history::{build_index, FileHistoryBudgetWire};

fn attributed(corpus: &Corpus) -> Vec<MemoryHistorySubjectWire> {
    let specs = memory_history_pathspecs(&corpus.scope).unwrap();
    let index =
        build_index(&corpus.repo, &specs, &FileHistoryBudgetWire::default())
            .unwrap();
    let mut subjects = derive_subjects(&index, &corpus.scope).unwrap();
    classify_subjects(&corpus.repo, &index, &mut subjects, &corpus.scope)
        .unwrap();
    attribute_instruction_causes(&corpus.repo, &mut subjects, &corpus.scope)
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

fn key_id(kind: &str, name: &str) -> String {
    format!("{kind}:{PROJECT_SCOPE_KEY}/{name}")
}

fn lint_id() -> String {
    key_id("note", "lint_and_test")
}

fn instructions_id() -> String {
    key_id("instructions", ".")
}

#[test]
fn co_rendered_instruction_names_lint_source() {
    let corpus = build_project_corpus();
    let subjects = attributed(&corpus);
    let commits = &corpus.commits;

    let lint = subject(&subjects, &lint_id());
    let lint_rendered = at_commit(lint, &commits["co_render"]);
    assert_eq!(lint_rendered.class, MemoryHistoryClassWire::Authored);

    let instructions = subject(&subjects, &instructions_id());
    let rendered = at_commit(instructions, &commits["co_render"]);
    assert_eq!(rendered.class, MemoryHistoryClassWire::Rendered);
    assert_eq!(rendered.cause.sources.len(), 1);
    assert_eq!(rendered.cause.sources[0].subject_id, lint_id());
    assert_eq!(rendered.cause.sources[0].ordinal, lint_rendered.ordinal);
    assert!(rendered.cause.config_paths.is_empty());
    assert!(rendered.cause.renderer_paths.is_empty());
    assert!(!rendered.cause.regen_only);
}

#[test]
fn config_commit_marks_config_cause() {
    let corpus = build_project_corpus();
    let subjects = attributed(&corpus);

    let instructions = subject(&subjects, &instructions_id());
    let version = at_commit(instructions, &corpus.commits["config"]);
    assert_eq!(version.class, MemoryHistoryClassWire::Config);
    assert_eq!(version.cause.config_paths, vec!["sase/sase.yml"]);
    assert!(version.cause.sources.is_empty());
    assert!(version.cause.renderer_paths.is_empty());
    assert!(!version.cause.regen_only);
}

#[test]
fn renderer_commit_marks_renderer_cause() {
    let corpus = build_project_corpus();
    let subjects = attributed(&corpus);

    let instructions = subject(&subjects, &instructions_id());
    let version = at_commit(instructions, &corpus.commits["renderer"]);
    assert_eq!(version.class, MemoryHistoryClassWire::Config);
    assert_eq!(version.cause.renderer_paths, vec!["src/sase/amd/render.rs"]);
    assert!(version.cause.sources.is_empty());
    assert!(version.cause.config_paths.is_empty());
    assert!(!version.cause.regen_only);
}

#[test]
fn regen_only_commit_has_empty_cause() {
    let corpus = build_project_corpus();
    let subjects = attributed(&corpus);

    let instructions = subject(&subjects, &instructions_id());
    let version = at_commit(instructions, &corpus.commits["regen_only"]);
    assert_eq!(version.class, MemoryHistoryClassWire::RegenOnly);
    assert_eq!(
        version.cause,
        MemoryHistoryCauseWire {
            sources: Vec::new(),
            config_paths: Vec::new(),
            renderer_paths: Vec::new(),
            regen_only: true,
        }
    );
}

#[test]
fn non_instruction_versions_keep_an_empty_cause() {
    let corpus = build_project_corpus();
    let subjects = attributed(&corpus);

    let lint = subject(&subjects, &lint_id());
    let version = at_commit(lint, &corpus.commits["co_render"]);
    assert_eq!(version.cause, MemoryHistoryCauseWire::default());
}
