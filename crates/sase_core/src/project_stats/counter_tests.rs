use super::counter::{count_blob, count_nonblank_lines};
use super::wire::ProjectStatsCodeCounterWire;

#[test]
fn nonblank_counter_handles_empty_whitespace_crlf_and_final_line() {
    assert_eq!(count_nonblank_lines(b""), 0);
    assert_eq!(count_nonblank_lines(b" \t\r\n\r\n\t "), 0);
    assert_eq!(count_nonblank_lines("\u{2003}\r\nvalue\n".as_bytes()), 1);
    assert_eq!(count_nonblank_lines(b"one\r\n \t \r\nthree\nfour"), 3);
}

#[test]
fn sloc_excludes_comments_and_multiline_docstrings_but_retains_nonblank() {
    for (path, language, bytes, expected) in [
        (
            b"lib.rs".as_slice(),
            "rust",
            b"// comment\r\n/* block */\r\nfn main() {}".as_slice(),
            1,
        ),
        (
            b"main.py",
            "python",
            b"# comment\n\"\"\"\ndocstring\n\"\"\"\nprint(1)\n",
            1,
        ),
        (b"file.rs", "rust", b"/* only\n comment */\n", 0),
        (
            b"run",
            "python",
            b"#!/usr/bin/env python3\n# comment\nprint(1)\n",
            1,
        ),
        (
            b"run",
            "bash",
            b"#!/usr/bin/env bash\n# comment\necho ok\n",
            1,
        ),
        (b"empty.rs", "rust", b" \r\n\t", 0),
    ] {
        let count = count_blob(
            ProjectStatsCodeCounterWire::Sloc,
            path,
            Some(language),
            bytes,
        );
        assert_eq!(count.selected_lines, expected, "{language}: {bytes:?}");
        assert_eq!(count.nonblank_lines, count_nonblank_lines(bytes));
    }
}

#[test]
fn pinned_tokei_counts_single_line_triple_quoted_strings_as_code() {
    let count = count_blob(
        ProjectStatsCodeCounterWire::Sloc,
        b"main.py",
        Some("python"),
        b"# comment\n\"\"\"docstring\"\"\"\nprint(1)\n",
    );
    assert_eq!(count.selected_lines, 2);
    assert_eq!(count.nonblank_lines, 3);
}

#[test]
fn mixed_language_counts_include_embedded_code_and_exclude_its_comments() {
    let count = count_blob(ProjectStatsCodeCounterWire::Sloc, b"view.html", Some("html"),
        b"<!-- prose comment -->\n<script>\n// js comment\nlet x = 1;\n</script>\n<style>\n/* css comment */\na { color: red; }\n</style>\n");
    assert_eq!(count.selected_lines, 6);
    assert_eq!(count.nonblank_lines, 9);
}

#[test]
fn extension_selects_parser_even_when_classifier_labels_are_equal() {
    let bytes = b"# comment\nvalue\n";
    let py = count_blob(
        ProjectStatsCodeCounterWire::Sloc,
        b"same.py",
        Some("python"),
        bytes,
    );
    let rs = count_blob(
        ProjectStatsCodeCounterWire::Sloc,
        b"same.rs",
        Some("python"),
        bytes,
    );
    assert_eq!(py.selected_lines, 1);
    assert_eq!(rs.selected_lines, 2);
}
