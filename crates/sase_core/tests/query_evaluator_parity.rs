//! Phase 2C parity gate: parse the golden corpus and run the Phase 2A
//! query matrix through the Rust evaluator. The expected match lists are
//! pinned in `sase_100/tests/test_core_query_golden.py::
//! test_evaluation_matrix_snapshot` — every change here must update both
//! sides in lockstep.
//!
//! The fixture under `tests/fixtures/myproj.sase` is a byte-for-byte copy of
//! `sase_100/tests/core_golden/myproj.sase`. We pass the corpus's relative
//! path (`tests/core_golden/myproj.sase`) into `parse_project_bytes` so the
//! `project:` matcher's parent-directory logic produces the same answers as
//! Python (`project_name == "core_golden"`).

use sase_core::{
    compile_query, evaluate_query_many, evaluate_query_many_in_corpus,
    parse_project_bytes, QueryCorpus,
};

const MYPROJ_SASE: &[u8] = include_bytes!("fixtures/myproj.sase");
const MYPROJ_PATH: &str = "tests/core_golden/myproj.sase";

fn matches(query: &str) -> Vec<String> {
    let specs =
        parse_project_bytes(MYPROJ_PATH, MYPROJ_SASE).expect("parse corpus");
    let program = compile_query(query).expect("compile");
    let results = evaluate_query_many(&program, &specs);
    specs
        .iter()
        .zip(results.iter())
        .filter_map(|(cs, m)| if *m { Some(cs.name.clone()) } else { None })
        .collect()
}

fn corpus_matches(query: &str, corpus: &QueryCorpus) -> Vec<String> {
    let program = compile_query(query).expect("compile");
    let results = evaluate_query_many_in_corpus(&program, corpus);
    corpus
        .specs
        .iter()
        .zip(results.iter())
        .filter_map(|(cs, m)| if *m { Some(cs.name.clone()) } else { None })
        .collect()
}

/// One row from the Python `test_evaluation_matrix_snapshot` golden matrix.
fn case(query: &str, expected: &[&str]) {
    let got = matches(query);
    let expected: Vec<String> =
        expected.iter().map(|s| s.to_string()).collect();
    assert_eq!(got, expected, "query {query}");
}

fn corpus_case(query: &str, expected: &[&str], corpus: &QueryCorpus) {
    let got = corpus_matches(query, corpus);
    let expected: Vec<String> =
        expected.iter().map(|s| s.to_string()).collect();
    assert_eq!(got, expected, "query {query}");
}

#[test]
fn evaluation_matrix_quoted_strings() {
    case("\"alpha\"", &["alpha", "beta", "beta__260102_010101"]);
    case("c\"Alpha\"", &[]);
    case("\"feature\"", &["alpha", "beta", "gamma"]);
    case("alpha", &["alpha", "beta", "beta__260102_010101"]);
    case("\"alpha\" \"beta\"", &["beta", "beta__260102_010101"]);
}

#[test]
fn evaluation_matrix_boolean_ops() {
    case("\"alpha\" AND \"beta\"", &["beta", "beta__260102_010101"]);
    case(
        "\"alpha\" OR \"beta\"",
        &["alpha", "beta", "beta__260102_010101"],
    );
    case("NOT \"beta\"", &["alpha", "gamma"]);
    case(
        "(\"alpha\" OR \"beta\") AND \"feature\"",
        &["alpha", "beta"],
    );
}

#[test]
fn evaluation_matrix_escapes_are_literal() {
    case(r#""foo\\bar""#, &[]);
    case(r#""line\nbreak""#, &[]);
}

#[test]
fn evaluation_matrix_error_running_shorthands() {
    case("!!!", &["alpha"]);
    case("!", &["alpha"]);
    case("@@@", &["gamma"]);
    case("@", &["gamma"]);
    case("$$$", &[]);
    case("$", &[]);
    case("*", &["alpha", "gamma"]);
    case("!!", &["beta", "beta__260102_010101", "gamma"]);
    case("!@", &["alpha", "beta", "beta__260102_010101"]);
    case("!$", &["alpha", "beta", "beta__260102_010101", "gamma"]);
}

#[test]
fn evaluation_matrix_property_filters() {
    case("status:Ready", &["gamma"]);
    case(
        "status:Reverted OR status:Submitted",
        &["alpha", "beta__260102_010101"],
    );
    // Python project_name = parent dir = "core_golden", not "myproj".
    case("project:myproj", &[]);
    case("ancestor:alpha", &["alpha", "beta", "beta__260102_010101"]);
    case("name:beta", &["beta"]);
    case("sibling:beta", &["beta"]);
    case("ancestor:alpha AND NOT \"beta\"", &["alpha"]);
}

#[test]
fn evaluation_matrix_status_shorthands() {
    case("%d", &[]);
    case("%m", &[]);
    case("%r", &["beta__260102_010101"]);
    case("%s", &["alpha"]);
    case("%w", &["beta"]);
    case("%y", &["gamma"]);
}

#[test]
fn evaluation_matrix_property_shorthands() {
    case("+myproj", &[]);
    case("^alpha", &["alpha", "beta", "beta__260102_010101"]);
    case("~beta", &["beta"]);
    case("&beta", &["beta"]);
}

#[test]
fn persistent_corpus_matches_golden_matrix_samples() {
    let specs =
        parse_project_bytes(MYPROJ_PATH, MYPROJ_SASE).expect("parse corpus");
    let corpus = QueryCorpus::new(specs);

    corpus_case(
        "\"alpha\" OR \"beta\"",
        &["alpha", "beta", "beta__260102_010101"],
        &corpus,
    );
    corpus_case("status:Ready", &["gamma"], &corpus);
    corpus_case(
        "ancestor:alpha",
        &["alpha", "beta", "beta__260102_010101"],
        &corpus,
    );
    corpus_case("sibling:beta", &["beta"], &corpus);
    corpus_case("@@@", &["gamma"], &corpus);
}

#[test]
fn substring_semantics_not_regex() {
    // ".*" is not a wildcard — the literal ".*" appears nowhere in the
    // corpus, so this query matches nothing.
    case("\".*\"", &[]);
}

#[test]
fn batch_and_oneshot_agree() {
    let specs =
        parse_project_bytes(MYPROJ_PATH, MYPROJ_SASE).expect("parse corpus");
    let program = compile_query("ancestor:alpha AND NOT \"beta\"").unwrap();
    let batch = evaluate_query_many(&program, &specs);
    for (i, cs) in specs.iter().enumerate() {
        let one = sase_core::evaluate_query_one(&program, cs, &specs);
        assert_eq!(batch[i], one, "spec {}", cs.name);
    }
}

#[test]
fn batch_evaluation_is_idempotent() {
    let specs =
        parse_project_bytes(MYPROJ_PATH, MYPROJ_SASE).expect("parse corpus");
    let program = compile_query("ancestor:alpha").unwrap();
    let first = evaluate_query_many(&program, &specs);
    let second = evaluate_query_many(&program, &specs);
    assert_eq!(first, second);
}

#[test]
fn persistent_corpus_reuses_derived_data_across_repeated_evaluations() {
    let specs =
        parse_project_bytes(MYPROJ_PATH, MYPROJ_SASE).expect("parse corpus");
    let corpus = QueryCorpus::new(specs);
    let program = compile_query("ancestor:alpha").unwrap();

    let first = evaluate_query_many_in_corpus(&program, &corpus);
    let second = evaluate_query_many_in_corpus(&program, &corpus);

    assert_eq!(first, second);
}

#[test]
fn persistent_corpus_keeps_ancestor_memo_query_specific() {
    let specs =
        parse_project_bytes(MYPROJ_PATH, MYPROJ_SASE).expect("parse corpus");
    let corpus = QueryCorpus::new(specs);

    let missing = compile_query("ancestor:missing").unwrap();
    let alpha = compile_query("ancestor:alpha").unwrap();

    assert_eq!(
        evaluate_query_many_in_corpus(&missing, &corpus),
        vec![false, false, false, false]
    );
    assert_eq!(
        evaluate_query_many_in_corpus(&alpha, &corpus),
        vec![true, true, true, false]
    );
}

#[test]
fn ancestor_walk_avoids_cycles() {
    // Construct a synthetic 2-spec list where parents form a cycle. The
    // evaluator must not recurse forever — the cycle guard breaks the walk.
    use sase_core::{ChangeSpecWire, SourceSpanWire};
    let span = SourceSpanWire {
        file_path: "p.sase".into(),
        start_line: 1,
        end_line: 1,
    };
    let mk = |name: &str, parent: &str| ChangeSpecWire {
        schema_version: 1,
        name: name.into(),
        project_basename: "p".into(),
        file_path: "core_golden/p.sase".into(),
        source_span: span.clone(),
        status: "WIP".into(),
        parent: Some(parent.into()),
        cl_or_pr: None,
        bug: None,
        description: String::new(),
        test_targets: vec![],
        commits: vec![],
        hooks: vec![],
        comments: vec![],
        mentors: vec![],
        timestamps: vec![],
        deltas: vec![],
    };
    let specs = vec![mk("a", "b"), mk("b", "a")];
    // ancestor:c is unreachable; the cycle must not loop forever.
    let program = compile_query("ancestor:c").unwrap();
    let results = evaluate_query_many(&program, &specs);
    assert_eq!(results, vec![false, false]);
    // ancestor:a hits the self-name match for "a" and walks b -> a for "b".
    let program = compile_query("ancestor:a").unwrap();
    let results = evaluate_query_many(&program, &specs);
    assert_eq!(results, vec![true, true]);
}
