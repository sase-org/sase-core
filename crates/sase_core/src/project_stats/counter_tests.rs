use super::counter::{count_blob, count_nonblank_lines, CounterError};
use super::wire::ProjectStatsCodeCounterWire;

#[test]
fn nonblank_counter_handles_empty_whitespace_crlf_and_final_line() {
    assert_eq!(count_nonblank_lines(b""), 0);
    assert_eq!(count_nonblank_lines(b" \t\r\n\r\n\t "), 0);
    assert_eq!(count_nonblank_lines("\u{2003}\r\nvalue\n".as_bytes()), 1);
    assert_eq!(count_nonblank_lines(b"one\r\n \t \r\nthree\nfour"), 3);
}

#[test]
fn comment_only_text_counts_as_nonblank_lines_under_baseline_policy() {
    let result = count_blob(
        ProjectStatsCodeCounterWire::Nonblank,
        Some("rust"),
        b"// comment\n/* another comment */\n",
    )
    .unwrap();
    assert_eq!(result.selected_lines, 2);
    assert_eq!(result.nonblank_lines, 2);
}

#[test]
fn counter_policy_and_language_are_explicit_inputs() {
    let result = count_blob(
        ProjectStatsCodeCounterWire::Nonblank,
        Some("python"),
        b"print('hello')",
    )
    .unwrap();
    assert_eq!(result.selected_lines, 1);
    assert_eq!(result.nonblank_lines, 1);
    assert_eq!(
        count_blob(ProjectStatsCodeCounterWire::Sloc, Some("python"), b"x"),
        Err(CounterError::SlocPolicyNotAvailable)
    );
}
