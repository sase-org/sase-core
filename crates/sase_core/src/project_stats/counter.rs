//! Shared blob-line counter interface for the project statistics snapshot.

use crate::project_stats::wire::ProjectStatsCodeCounterWire;

// tokei 15.0.0 passed the isolated release-wheel gate on 2026-10-18:
// +159,459 bytes (0.152 MiB), 2,068.595 / 1,867.069 seconds (1.108x).
// Both target and build directories were empty and independent; this replaces
// the invalid earlier timing that inherited a shared Cargo build directory.
// Bump this identity when parser version, language mapping or Config changes.
pub(crate) const COUNTER_VERSION: &str = "tokei-15.0.0-docstrings-comments-v2";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct BlobLineCounts {
    pub selected_lines: u64,
    pub nonblank_lines: u64,
}

/// Resolve only the supplied extension and SASE's bounded language/shebang hint.
/// Never use tokei's filesystem detection or Config::from_config_files.
pub(crate) fn code_language(
    path: &[u8],
    language: Option<&str>,
) -> tokei::LanguageType {
    let extension = path
        .rsplit(|byte| *byte == b'/')
        .next()
        .and_then(|name| {
            name.iter()
                .rposition(|byte| *byte == b'.')
                .map(|dot| &name[dot + 1..])
        })
        .and_then(|suffix| std::str::from_utf8(suffix).ok());
    extension
        .and_then(|ext| {
            tokei::LanguageType::from_file_extension(&ext.to_ascii_lowercase())
        })
        .or_else(|| {
            language.and_then(|name| {
                // from_name is case-sensitive; SASE hints are lowercase.
                match name {
                    "cpp" => "C++",
                    "jinja" => "Jinja2",
                    "make" => "Makefile",
                    "docker" => "Dockerfile",
                    "sh" | "zsh" | "bash" => "BASH",
                    other => other,
                }
                .parse::<tokei::LanguageType>()
                .ok()
            })
        })
        .unwrap_or(tokei::LanguageType::Text)
}

/// Count source/tests with an explicit policy. Other roles retain nonblank.
/// Embedded languages contribute through summarise().
pub(crate) fn count_blob(
    policy: ProjectStatsCodeCounterWire,
    path: &[u8],
    language: Option<&str>,
    bytes: &[u8],
) -> BlobLineCounts {
    let nonblank_lines = count_nonblank_lines(bytes);
    let selected_lines = match policy {
        ProjectStatsCodeCounterWire::Nonblank => nonblank_lines,
        ProjectStatsCodeCounterWire::Sloc => {
            // Keep the pinned parser's semantics: multiline Python docstrings
            // are comments, but a single-line triple-quoted string counts as
            // code in tokei 15.0.0, even with this option enabled.
            let config = tokei::Config {
                treat_doc_strings_as_comments: Some(true),
                ..tokei::Config::default()
            };
            code_language(path, language)
                .parse_from_slice(bytes, &config)
                .summarise()
                .code as u64
        }
    };
    BlobLineCounts {
        selected_lines,
        nonblank_lines,
    }
}

/// Count lines containing a non-whitespace character. LF terminates lines,
/// CRLF is treated as one line ending, and an unterminated final line counts.
pub(crate) fn count_nonblank_lines(bytes: &[u8]) -> u64 {
    bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| {
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            match std::str::from_utf8(line) {
                Ok(text) => !text.trim().is_empty(),
                Err(_) => line.iter().any(|byte| !byte.is_ascii_whitespace()),
            }
        })
        .count() as u64
}
