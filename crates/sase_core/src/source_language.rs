//! Shared source-language identity for pager and other frontends.
//!
//! This module owns filename mapping, trusted-category precedence, shebang
//! inspection, and bounded stdin diff recognition. It performs no file I/O
//! and has no Pygments or Rich dependency. Explicit engine aliases are
//! validated by the frontend and are not part of automatic detection.

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// Wire schema for source-language request, result, and filename-hint
/// payloads.
pub const SOURCE_LANGUAGE_WIRE_SCHEMA_VERSION: u32 = 1;

/// Maximum UTF-8 byte length inspected for shebang and diff sniffing.
pub const SOURCE_LANGUAGE_PREFIX_BUDGET_BYTES: usize = 8 * 1024;

const REASON_INELIGIBLE: &str = "ineligible";
const REASON_TRUSTED_MARKDOWN: &str = "trusted_markdown";
const REASON_TRUSTED_DIFF: &str = "trusted_diff";
const REASON_FILENAME: &str = "filename";
const REASON_PLAIN_FILENAME: &str = "plain_filename";
const REASON_SHEBANG: &str = "shebang";
const REASON_DIFF_PREFIX: &str = "diff_prefix";
const REASON_UNKNOWN: &str = "unknown";
const REASON_UNTYPED: &str = "untyped";

/// How a source section reached the resolver.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceCategory {
    RawFile,
    Stdin,
    MarkdownDocument,
    Diff,
    Formatted,
}

/// Additive request for automatic language selection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceLanguageRequestWire {
    pub schema_version: u32,
    pub category: SourceCategory,
    #[serde(default)]
    pub logical_filename: Option<String>,
    #[serde(default)]
    pub prefix: Option<String>,
}

/// Automatic language selection result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceLanguageResultWire {
    pub schema_version: u32,
    pub language: Option<String>,
    pub reason: String,
    pub supported_text: bool,
}

/// Filename provenance hints for later adapters.
///
/// Precedence is `source_path`, then `vcs_relpath`, then `resolved_path`.
/// Callers use the selected value only as a filename hint and must read
/// content from the already resolved artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceFilenameHintsWire {
    pub schema_version: u32,
    #[serde(default)]
    pub source_path: Option<String>,
    #[serde(default)]
    pub vcs_relpath: Option<String>,
    #[serde(default)]
    pub resolved_path: Option<String>,
}

/// Decode a [`SourceLanguageRequestWire`], validating the schema version.
pub fn source_language_request_from_json_value(
    value: &JsonValue,
) -> Result<SourceLanguageRequestWire, String> {
    validate_schema_version(value, "source language request")?;
    serde_json::from_value(value.clone()).map_err(|error| error.to_string())
}

/// Decode a [`SourceFilenameHintsWire`], validating the schema version.
pub fn source_filename_hints_from_json_value(
    value: &JsonValue,
) -> Result<SourceFilenameHintsWire, String> {
    validate_schema_version(value, "source filename hints")?;
    serde_json::from_value(value.clone()).map_err(|error| error.to_string())
}

/// Select the logical filename hint from provenance fields.
pub fn logical_filename_from_hints(
    hints: &SourceFilenameHintsWire,
) -> Option<String> {
    first_nonempty([
        hints.source_path.as_deref(),
        hints.vcs_relpath.as_deref(),
        hints.resolved_path.as_deref(),
    ])
    .map(str::to_string)
}

/// Resolve a canonical language identity for an eligible source.
pub fn resolve_source_language(
    request: &SourceLanguageRequestWire,
) -> SourceLanguageResultWire {
    match request.category {
        SourceCategory::Formatted => {
            return result(None, REASON_INELIGIBLE, false);
        }
        SourceCategory::MarkdownDocument => {
            return result(Some("markdown"), REASON_TRUSTED_MARKDOWN, true);
        }
        SourceCategory::Diff => {
            return result(Some("diff"), REASON_TRUSTED_DIFF, true);
        }
        SourceCategory::RawFile | SourceCategory::Stdin => {}
    }

    let filename = request.logical_filename.as_deref().and_then(nonempty_str);
    if let Some(mapped) = filename.and_then(language_from_filename) {
        return filename_result(mapped);
    }

    let prefix = bounded_prefix(request.prefix.as_deref().unwrap_or(""));
    let extensionless = match filename {
        None => true,
        Some(value) => suffix(basename(value)).is_none(),
    };
    if extensionless {
        if let Some(language) = language_from_shebang(prefix) {
            return result(Some(language), REASON_SHEBANG, false);
        }
    }

    if request.category == SourceCategory::Stdin && looks_like_diff(prefix) {
        return result(Some("diff"), REASON_DIFF_PREFIX, false);
    }

    if request.category == SourceCategory::Stdin {
        result(None, REASON_UNTYPED, false)
    } else {
        result(None, REASON_UNKNOWN, false)
    }
}

fn validate_schema_version(
    value: &JsonValue,
    label: &str,
) -> Result<(), String> {
    let schema = value
        .get("schema_version")
        .and_then(JsonValue::as_u64)
        .ok_or_else(|| {
            format!("{label} missing or non-integer schema_version")
        })?;
    if schema != u64::from(SOURCE_LANGUAGE_WIRE_SCHEMA_VERSION) {
        return Err(format!(
            "{label} schema mismatch: got {schema}, expected {}",
            SOURCE_LANGUAGE_WIRE_SCHEMA_VERSION
        ));
    }
    Ok(())
}

fn result(
    language: Option<&str>,
    reason: &str,
    supported_text: bool,
) -> SourceLanguageResultWire {
    SourceLanguageResultWire {
        schema_version: SOURCE_LANGUAGE_WIRE_SCHEMA_VERSION,
        language: language.map(str::to_string),
        reason: reason.to_string(),
        supported_text,
    }
}

fn filename_result(mapped: FilenameLanguage) -> SourceLanguageResultWire {
    match mapped {
        FilenameLanguage::Language(language) => {
            result(Some(language), REASON_FILENAME, true)
        }
        FilenameLanguage::Plain => result(None, REASON_PLAIN_FILENAME, true),
    }
}

#[derive(Clone, Copy)]
enum FilenameLanguage {
    Language(&'static str),
    Plain,
}

fn language_from_filename(path: &str) -> Option<FilenameLanguage> {
    let name = basename(path);
    if name.is_empty() {
        return None;
    }
    if let Some(ext) = suffix(name) {
        if let Some(mapped) = suffix_language(&ext.to_ascii_lowercase()) {
            return Some(mapped);
        }
    }
    if let Some(mapped) = basename_language(name) {
        return Some(mapped);
    }
    if name.starts_with(".env.") && name.len() > 5 {
        return Some(FilenameLanguage::Language("bash"));
    }
    None
}

fn suffix_language(ext: &str) -> Option<FilenameLanguage> {
    for (suffix, language) in SUFFIX_MAP {
        if *suffix == ext {
            return Some(match language {
                Some(name) => FilenameLanguage::Language(name),
                None => FilenameLanguage::Plain,
            });
        }
    }
    None
}

fn basename_language(name: &str) -> Option<FilenameLanguage> {
    for (basename, language) in BASENAME_MAP {
        if *basename == name {
            return Some(match language {
                Some(lang) => FilenameLanguage::Language(lang),
                None => FilenameLanguage::Plain,
            });
        }
    }
    None
}

fn language_from_shebang(prefix: &str) -> Option<&'static str> {
    let line = first_line(prefix);
    let interpreter = interpreter_from_shebang(line)?;
    interpreter_language(interpreter)
}

fn interpreter_from_shebang(line: &str) -> Option<&str> {
    let rest = line.strip_prefix("#!")?.trim();
    if rest.is_empty() {
        return None;
    }
    let mut tokens = rest.split_whitespace();
    let program = basename(tokens.next()?);
    if program != "env" {
        return Some(program);
    }
    for token in tokens {
        if token == "--" || token.starts_with('-') || token.contains('=') {
            continue;
        }
        return Some(basename(token));
    }
    None
}

fn interpreter_language(name: &str) -> Option<&'static str> {
    if is_python_interpreter(name) {
        return Some("python");
    }
    match name {
        "bash" => Some("bash"),
        "sh" | "dash" => Some("sh"),
        "zsh" => Some("zsh"),
        _ => None,
    }
}

fn is_python_interpreter(name: &str) -> bool {
    let Some(rest) = name.strip_prefix("python") else {
        return false;
    };
    rest.is_empty() || rest.chars().all(|ch| ch.is_ascii_digit() || ch == '.')
}

fn looks_like_diff(prefix: &str) -> bool {
    let lines: Vec<&str> = prefix
        .split('\n')
        .map(|line| line.strip_suffix('\r').unwrap_or(line))
        .collect();
    let mut index = 0;
    while index < lines.len() {
        let line = lines[index];
        if line.starts_with("diff --git ") {
            return true;
        }
        if line.starts_with("--- ")
            && lines
                .get(index + 1)
                .is_some_and(|next| next.starts_with("+++ "))
        {
            let mut cursor = index + 2;
            while cursor < lines.len() && lines[cursor].is_empty() {
                cursor += 1;
            }
            if lines
                .get(cursor)
                .is_some_and(|candidate| is_unified_hunk_header(candidate))
            {
                return true;
            }
        }
        index += 1;
    }
    false
}

fn is_unified_hunk_header(line: &str) -> bool {
    let Some(rest) = line.strip_prefix("@@") else {
        return false;
    };
    let rest = rest.trim_start();
    let Some(rest) = rest.strip_prefix('-') else {
        return false;
    };
    let Some(rest) = take_hunk_range(rest) else {
        return false;
    };
    let rest = rest.trim_start();
    let Some(rest) = rest.strip_prefix('+') else {
        return false;
    };
    let Some(rest) = take_hunk_range(rest) else {
        return false;
    };
    rest.trim_start().starts_with("@@")
}

fn take_hunk_range(input: &str) -> Option<&str> {
    let rest = take_digits(input)?;
    if let Some(rest) = rest.strip_prefix(',') {
        take_digits(rest)
    } else {
        Some(rest)
    }
}

fn take_digits(input: &str) -> Option<&str> {
    let digits = input.bytes().take_while(u8::is_ascii_digit).count();
    if digits == 0 {
        return None;
    }
    Some(&input[digits..])
}

fn bounded_prefix(prefix: &str) -> &str {
    if prefix.len() <= SOURCE_LANGUAGE_PREFIX_BUDGET_BYTES {
        return prefix;
    }
    let mut end = SOURCE_LANGUAGE_PREFIX_BUDGET_BYTES;
    while end > 0 && !prefix.is_char_boundary(end) {
        end -= 1;
    }
    &prefix[..end]
}

fn first_line(text: &str) -> &str {
    match text.find(['\n', '\r']) {
        Some(index) => &text[..index],
        None => text,
    }
}

fn basename(path: &str) -> &str {
    path.rsplit(['/', '\\']).next().unwrap_or(path)
}

fn suffix(name: &str) -> Option<&str> {
    let dot = name.rfind('.')?;
    if dot == 0 {
        return None;
    }
    Some(&name[dot + 1..])
}

fn nonempty_str(value: &str) -> Option<&str> {
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

fn first_nonempty(values: [Option<&str>; 3]) -> Option<&str> {
    values.into_iter().flatten().find(|value| !value.is_empty())
}

/// Suffix → language. `None` is deliberately plain supported text.
const SUFFIX_MAP: &[(&str, Option<&str>)] = &[
    ("bash", Some("bash")),
    ("c", Some("c")),
    ("cc", Some("cpp")),
    ("cjs", Some("javascript")),
    ("cpp", Some("cpp")),
    ("css", Some("css")),
    ("diff", Some("diff")),
    ("go", Some("go")),
    ("h", Some("c")),
    ("hpp", Some("cpp")),
    ("htm", Some("html")),
    ("html", Some("html")),
    ("j2", Some("jinja")),
    ("java", Some("java")),
    ("jinja", Some("jinja")),
    ("jinja2", Some("jinja")),
    ("js", Some("javascript")),
    ("json", Some("json")),
    ("jsonl", Some("json")),
    ("jsx", Some("jsx")),
    ("log", None),
    ("lua", Some("lua")),
    ("markdown", Some("markdown")),
    ("md", Some("markdown")),
    ("mdown", Some("markdown")),
    ("mjs", Some("javascript")),
    ("mkd", Some("markdown")),
    ("patch", Some("diff")),
    ("py", Some("python")),
    ("pyi", Some("python")),
    ("rb", Some("ruby")),
    ("rs", Some("rust")),
    ("rst", Some("rst")),
    ("sase", None),
    ("sh", Some("sh")),
    ("sql", Some("sql")),
    ("tcss", Some("css")),
    ("toml", Some("toml")),
    ("ts", Some("typescript")),
    ("tsx", Some("tsx")),
    ("txt", None),
    ("xml", Some("xml")),
    ("xprompt", Some("markdown")),
    ("yaml", Some("yaml")),
    ("yml", Some("yaml")),
    ("zsh", Some("zsh")),
];

/// Exact special basenames. `None` is deliberately plain supported text.
const BASENAME_MAP: &[(&str, Option<&str>)] = &[
    (".env", Some("bash")),
    (".gitignore", None),
    ("Dockerfile", Some("docker")),
    ("GNUmakefile", Some("make")),
    ("Justfile", None),
    ("Makefile", Some("make")),
    ("README", Some("markdown")),
    ("justfile", None),
    ("makefile", Some("make")),
    ("uv.lock", Some("toml")),
];

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn resolve(
        category: SourceCategory,
        filename: Option<&str>,
        prefix: Option<&str>,
    ) -> SourceLanguageResultWire {
        resolve_source_language(&SourceLanguageRequestWire {
            schema_version: SOURCE_LANGUAGE_WIRE_SCHEMA_VERSION,
            category,
            logical_filename: filename.map(str::to_string),
            prefix: prefix.map(str::to_string),
        })
    }

    fn assert_lang(
        category: SourceCategory,
        filename: Option<&str>,
        prefix: Option<&str>,
        language: Option<&str>,
        reason: &str,
        supported_text: bool,
    ) {
        let got = resolve(category, filename, prefix);
        assert_eq!(got.schema_version, SOURCE_LANGUAGE_WIRE_SCHEMA_VERSION);
        assert_eq!(
            got.language.as_deref(),
            language,
            "language for {filename:?}"
        );
        assert_eq!(got.reason, reason, "reason for {filename:?}");
        assert_eq!(
            got.supported_text, supported_text,
            "supported_text for {filename:?}"
        );
    }

    #[test]
    fn mapping_table_covers_declared_families() {
        let cases: &[(&str, Option<&str>)] = &[
            ("app.py", Some("python")),
            ("types.pyi", Some("python")),
            ("main.rs", Some("rust")),
            ("main.go", Some("go")),
            ("file.c", Some("c")),
            ("file.h", Some("c")),
            ("file.cc", Some("cpp")),
            ("file.cpp", Some("cpp")),
            ("file.hpp", Some("cpp")),
            ("Main.java", Some("java")),
            ("script.rb", Some("ruby")),
            ("mod.lua", Some("lua")),
            ("app.js", Some("javascript")),
            ("app.mjs", Some("javascript")),
            ("app.cjs", Some("javascript")),
            ("app.jsx", Some("jsx")),
            ("app.ts", Some("typescript")),
            ("app.tsx", Some("tsx")),
            ("run.sh", Some("sh")),
            ("run.bash", Some("bash")),
            ("run.zsh", Some("zsh")),
            ("data.json", Some("json")),
            ("events.jsonl", Some("json")),
            ("config.yaml", Some("yaml")),
            ("config.yml", Some("yaml")),
            ("Cargo.toml", Some("toml")),
            ("uv.lock", Some("toml")),
            ("note.md", Some("markdown")),
            ("note.markdown", Some("markdown")),
            ("note.mdown", Some("markdown")),
            ("note.mkd", Some("markdown")),
            ("prompt.xprompt", Some("markdown")),
            ("README", Some("markdown")),
            ("README.rst", Some("rst")),
            ("guide.rst", Some("rst")),
            ("index.html", Some("html")),
            ("index.htm", Some("html")),
            ("data.xml", Some("xml")),
            ("theme.css", Some("css")),
            ("app.tcss", Some("css")),
            ("query.sql", Some("sql")),
            ("changes.diff", Some("diff")),
            ("changes.patch", Some("diff")),
            ("Makefile", Some("make")),
            ("makefile", Some("make")),
            ("GNUmakefile", Some("make")),
            ("Dockerfile", Some("docker")),
            ("page.j2", Some("jinja")),
            ("page.jinja", Some("jinja")),
            ("page.jinja2", Some("jinja")),
            (".env", Some("bash")),
            (".env.local", Some("bash")),
            (".env.production", Some("bash")),
            ("notes.txt", None),
            ("server.log", None),
            ("project.sase", None),
            (".gitignore", None),
            ("Justfile", None),
            ("justfile", None),
        ];
        for (filename, language) in cases {
            let reason = if language.is_some() {
                REASON_FILENAME
            } else {
                REASON_PLAIN_FILENAME
            };
            assert_lang(
                SourceCategory::RawFile,
                Some(filename),
                None,
                *language,
                reason,
                true,
            );
        }
    }

    #[test]
    fn suffix_matching_is_case_insensitive_and_basenames_are_exact() {
        assert_lang(
            SourceCategory::RawFile,
            Some("FOO.PY"),
            None,
            Some("python"),
            REASON_FILENAME,
            true,
        );
        assert_lang(
            SourceCategory::RawFile,
            Some("App.TSX"),
            None,
            Some("tsx"),
            REASON_FILENAME,
            true,
        );
        assert_lang(
            SourceCategory::RawFile,
            Some("MAKEFILE"),
            None,
            None,
            REASON_UNKNOWN,
            false,
        );
        assert_lang(
            SourceCategory::RawFile,
            Some("dockerfile"),
            None,
            None,
            REASON_UNKNOWN,
            false,
        );
        assert_lang(
            SourceCategory::RawFile,
            Some("readme"),
            None,
            None,
            REASON_UNKNOWN,
            false,
        );
        assert_lang(
            SourceCategory::RawFile,
            Some("README.txt"),
            None,
            None,
            REASON_PLAIN_FILENAME,
            true,
        );
    }

    #[test]
    fn justfile_is_plain_and_not_make() {
        assert_lang(
            SourceCategory::RawFile,
            Some("Justfile"),
            None,
            None,
            REASON_PLAIN_FILENAME,
            true,
        );
        assert_lang(
            SourceCategory::RawFile,
            Some("Makefile"),
            None,
            Some("make"),
            REASON_FILENAME,
            true,
        );
    }

    #[test]
    fn trusted_categories_override_filename_and_skip_sniffing() {
        let python_shebang = "#!/usr/bin/env python3\nprint(1)\n";
        assert_lang(
            SourceCategory::MarkdownDocument,
            Some("note.py"),
            Some(python_shebang),
            Some("markdown"),
            REASON_TRUSTED_MARKDOWN,
            true,
        );
        assert_lang(
            SourceCategory::Diff,
            Some("note.md"),
            Some(python_shebang),
            Some("diff"),
            REASON_TRUSTED_DIFF,
            true,
        );
        assert_lang(
            SourceCategory::Formatted,
            Some("note.py"),
            Some(python_shebang),
            None,
            REASON_INELIGIBLE,
            false,
        );
    }

    #[test]
    fn filename_wins_over_shebang_and_diff_prefix() {
        let diff = "diff --git a/a b/b\n";
        assert_lang(
            SourceCategory::RawFile,
            Some("app.py"),
            Some("#!/bin/bash\n"),
            Some("python"),
            REASON_FILENAME,
            true,
        );
        assert_lang(
            SourceCategory::Stdin,
            Some("app.py"),
            Some(diff),
            Some("python"),
            REASON_FILENAME,
            true,
        );
    }

    #[test]
    fn extensionless_shebangs_cover_python_and_shell_dialects() {
        let cases: &[(&str, &str)] = &[
            ("#!/usr/bin/env python\n", "python"),
            ("#!/usr/bin/env python3\n", "python"),
            ("#!/usr/bin/python3.12\n", "python"),
            ("#!/usr/bin/env -S python3 -u\n", "python"),
            ("#!/usr/bin/env -S PYTHONSTARTUP= python3\n", "python"),
            ("#!/bin/bash\n", "bash"),
            ("#!/usr/bin/env bash\n", "bash"),
            ("#!/bin/sh\n", "sh"),
            ("#!/usr/bin/env sh\n", "sh"),
            ("#!/bin/zsh\n", "zsh"),
            ("#!/usr/bin/env zsh\n", "zsh"),
            ("#!/usr/bin/env dash\n", "sh"),
        ];
        for (prefix, language) in cases {
            assert_lang(
                SourceCategory::RawFile,
                Some("myscript"),
                Some(prefix),
                Some(language),
                REASON_SHEBANG,
                false,
            );
        }
    }

    #[test]
    fn shebang_is_skipped_for_suffixed_unknown_files() {
        assert_lang(
            SourceCategory::RawFile,
            Some("myscript.foo"),
            Some("#!/usr/bin/env python3\n"),
            None,
            REASON_UNKNOWN,
            false,
        );
    }

    #[test]
    fn stdin_diff_sniff_positive_and_negative_samples() {
        let git = "diff --git a/old b/new\nindex 111..222 100644\n";
        let unified = "--- a/old\n+++ b/new\n@@ -1,2 +1,2 @@\n-a\n+b\n";
        let new_file = "--- /dev/null\n+++ b/created\n@@ -0,0 +1,2 @@\n+hi\n";
        let deleted = "--- a/gone\n+++ /dev/null\n@@ -1,2 +0,0 @@\n-hi\n";
        assert_lang(
            SourceCategory::Stdin,
            None,
            Some(git),
            Some("diff"),
            REASON_DIFF_PREFIX,
            false,
        );
        assert_lang(
            SourceCategory::Stdin,
            None,
            Some(unified),
            Some("diff"),
            REASON_DIFF_PREFIX,
            false,
        );
        assert_lang(
            SourceCategory::Stdin,
            None,
            Some(new_file),
            Some("diff"),
            REASON_DIFF_PREFIX,
            false,
        );
        assert_lang(
            SourceCategory::Stdin,
            None,
            Some(deleted),
            Some("diff"),
            REASON_DIFF_PREFIX,
            false,
        );

        for prefix in [
            "@@ -1,1 +1,1 @@\n",
            "--- a/old\n",
            "This document mentions --- and @@ hunks.\n",
            "{\n  \"name\": \"--- a/foo\",\n  \"hunk\": \"@@ -1 +1 @@\"\n}\n",
            "--- a/old\n+++ b/new\nnot a hunk\n@@ -1,1 +1,1 @@\n",
        ] {
            assert_lang(
                SourceCategory::Stdin,
                None,
                Some(prefix),
                None,
                REASON_UNTYPED,
                false,
            );
        }
    }

    #[test]
    fn diff_sniff_is_stdin_only() {
        let git = "diff --git a/old b/new\n";
        assert_lang(
            SourceCategory::RawFile,
            Some("notes"),
            Some(git),
            None,
            REASON_UNKNOWN,
            false,
        );
    }

    #[test]
    fn prefix_inspection_is_bounded_to_8_kib() {
        let mut oversized = "x".repeat(SOURCE_LANGUAGE_PREFIX_BUDGET_BYTES);
        oversized.push_str("\ndiff --git a/old b/new\n");
        assert_lang(
            SourceCategory::Stdin,
            None,
            Some(&oversized),
            None,
            REASON_UNTYPED,
            false,
        );

        let mut shebang_after_bound =
            "x".repeat(SOURCE_LANGUAGE_PREFIX_BUDGET_BYTES);
        shebang_after_bound.push_str("\n#!/usr/bin/env python3\n");
        assert_lang(
            SourceCategory::RawFile,
            Some("myscript"),
            Some(&shebang_after_bound),
            None,
            REASON_UNKNOWN,
            false,
        );
    }

    #[test]
    fn windows_and_unix_paths_use_basename() {
        assert_lang(
            SourceCategory::RawFile,
            Some(r"C:\Users\me\app.py"),
            None,
            Some("python"),
            REASON_FILENAME,
            true,
        );
        assert_lang(
            SourceCategory::RawFile,
            Some("/home/me/src/app.py"),
            None,
            Some("python"),
            REASON_FILENAME,
            true,
        );
    }

    #[test]
    fn unknown_and_empty_names_are_not_supported_text() {
        assert_lang(
            SourceCategory::RawFile,
            Some("mystery.xyz"),
            None,
            None,
            REASON_UNKNOWN,
            false,
        );
        assert_lang(
            SourceCategory::RawFile,
            None,
            None,
            None,
            REASON_UNKNOWN,
            false,
        );
        assert_lang(
            SourceCategory::Stdin,
            None,
            Some("hello\n"),
            None,
            REASON_UNTYPED,
            false,
        );
    }

    #[test]
    fn logical_filename_uses_source_then_vcs_then_resolved() {
        let hints = SourceFilenameHintsWire {
            schema_version: SOURCE_LANGUAGE_WIRE_SCHEMA_VERSION,
            source_path: Some("/orig/app.py".to_string()),
            vcs_relpath: Some("docs/app.py".to_string()),
            resolved_path: Some("/objects/abc".to_string()),
        };
        assert_eq!(
            logical_filename_from_hints(&hints).as_deref(),
            Some("/orig/app.py")
        );

        let without_source = SourceFilenameHintsWire {
            source_path: Some(String::new()),
            vcs_relpath: Some("docs/app.py".to_string()),
            ..hints.clone()
        };
        assert_eq!(
            logical_filename_from_hints(&without_source).as_deref(),
            Some("docs/app.py")
        );

        let resolved_only = SourceFilenameHintsWire {
            source_path: None,
            vcs_relpath: Some(String::new()),
            resolved_path: Some("/objects/abc".to_string()),
            schema_version: SOURCE_LANGUAGE_WIRE_SCHEMA_VERSION,
        };
        assert_eq!(
            logical_filename_from_hints(&resolved_only).as_deref(),
            Some("/objects/abc")
        );
    }

    #[test]
    fn request_and_result_json_roundtrip() {
        let request = SourceLanguageRequestWire {
            schema_version: SOURCE_LANGUAGE_WIRE_SCHEMA_VERSION,
            category: SourceCategory::RawFile,
            logical_filename: Some("app.py".to_string()),
            prefix: Some("print(1)\n".to_string()),
        };
        let encoded = serde_json::to_value(&request).unwrap();
        let decoded =
            source_language_request_from_json_value(&encoded).unwrap();
        assert_eq!(decoded, request);

        let selected = resolve_source_language(&request);
        let result_json = serde_json::to_value(&selected).unwrap();
        let rebuilt: SourceLanguageResultWire =
            serde_json::from_value(result_json.clone()).unwrap();
        assert_eq!(rebuilt, selected);
        assert_eq!(result_json["language"], json!("python"));
        assert_eq!(result_json["reason"], json!(REASON_FILENAME));
        assert_eq!(result_json["supported_text"], json!(true));
    }

    #[test]
    fn hints_json_roundtrip_and_schema_errors() {
        let hints = SourceFilenameHintsWire {
            schema_version: SOURCE_LANGUAGE_WIRE_SCHEMA_VERSION,
            source_path: Some("/orig/app.py".to_string()),
            vcs_relpath: None,
            resolved_path: Some("/objects/abc".to_string()),
        };
        let encoded = serde_json::to_value(&hints).unwrap();
        let decoded = source_filename_hints_from_json_value(&encoded).unwrap();
        assert_eq!(decoded, hints);

        let err = source_language_request_from_json_value(&json!({
            "category": "raw_file"
        }))
        .unwrap_err();
        assert!(err.contains("schema_version"), "{err}");

        let mismatch = source_filename_hints_from_json_value(&json!({
            "schema_version": 99,
            "source_path": "a"
        }))
        .unwrap_err();
        assert!(mismatch.contains("schema mismatch"), "{mismatch}");
    }
}
