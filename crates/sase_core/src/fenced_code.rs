//! Shared fenced-code contract: CommonMark fence scan, `CodeValue`, and
//! directive-owned `%if::` / `%proc::` spans.
//!
//! The scanner matches the historical Python `fenced_block_details` rules so
//! launch, ACE, and the xprompt LSP share one ownership/indent/closure model.
//! Directive-owned fences are identified before ordinary literal-zone
//! protection so `%if`/`%proc` bodies stay opaque.

use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Wire schema for [`CodeValueWire`] and [`CodeDirectiveScanWire`].
pub const CODE_VALUE_WIRE_SCHEMA_VERSION: u32 = 1;

const PREVIEW_MAX_CHARS: usize = 80;
const TYPED_LAUNCH_UNITS_FLAG: &str = "typed_launch_units";

/// Supported interpreters for directive-owned code and `type: code` values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CodeLanguage {
    Bash,
    Python,
}

impl CodeLanguage {
    /// Canonical lowercase wire spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Bash => "bash",
            Self::Python => "python",
        }
    }
}

/// Structured source plus language. Bodies are opaque: later xprompt, Jinja,
/// and directive scans must not reinterpret them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeValue {
    pub source: String,
    pub language: CodeLanguage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub info_string: Option<String>,
}

impl CodeValue {
    /// SHA-256 hex digest of newline-normalized source.
    pub fn digest(&self) -> String {
        hex::encode(Sha256::digest(normalize_newlines(&self.source).as_bytes()))
    }

    /// Single-line safe preview of the source.
    pub fn preview(&self) -> String {
        safe_preview(&self.source)
    }

    /// Additive versioned wire copy.
    pub fn to_wire(&self) -> CodeValueWire {
        CodeValueWire {
            schema_version: CODE_VALUE_WIRE_SCHEMA_VERSION,
            source: self.source.clone(),
            language: self.language.as_str().to_string(),
            info_string: self.info_string.clone(),
            digest: self.digest(),
            preview: self.preview(),
        }
    }
}

/// JSON-shaped `CodeValue` consumed by Python and editor surfaces.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeValueWire {
    pub schema_version: u32,
    pub source: String,
    pub language: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub info_string: Option<String>,
    pub digest: String,
    pub preview: String,
}

/// One CommonMark fenced block, closed or live-unclosed, as UTF-8 byte ranges.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FencedBlock {
    pub block_range: (usize, usize),
    pub opening_fence: (usize, usize),
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub info_string: Option<(usize, usize)>,
    pub content_range: (usize, usize),
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closing_fence: Option<(usize, usize)>,
}

/// JSON-shaped fence details for the Python adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FencedBlockWire {
    pub block_range: [usize; 2],
    pub opening_fence: [usize; 2],
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub info_string: Option<[usize; 2]>,
    pub content_range: [usize; 2],
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closing_fence: Option<[usize; 2]>,
}

impl From<&FencedBlock> for FencedBlockWire {
    fn from(block: &FencedBlock) -> Self {
        Self {
            block_range: [block.block_range.0, block.block_range.1],
            opening_fence: [block.opening_fence.0, block.opening_fence.1],
            info_string: block.info_string.map(|(start, end)| [start, end]),
            content_range: [block.content_range.0, block.content_range.1],
            closing_fence: block.closing_fence.map(|(start, end)| [start, end]),
        }
    }
}

/// One `%if` / `%proc` span captured by the directive-owned fence scanner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeDirectiveSpanWire {
    pub name: String,
    pub span: [usize; 2],
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<CodeValueWire>,
}

/// Actionable diagnostic for an incomplete, illegal, or multiply-owned fence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeDirectiveDiagnosticWire {
    pub code: String,
    pub message: String,
    pub span: [usize; 2],
}

/// Versioned scan result shared by launch parsing and editor surfaces.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodeDirectiveScanWire {
    pub schema_version: u32,
    pub directives: Vec<CodeDirectiveSpanWire>,
    pub diagnostics: Vec<CodeDirectiveDiagnosticWire>,
}

#[derive(Debug, Clone, Copy)]
struct OpeningFence {
    char: u8,
    length: usize,
    run_start: usize,
    run_end: usize,
    info_string: Option<(usize, usize)>,
}

/// Return structured ranges for every closed or live unclosed fence.
pub fn fenced_block_details(text: &str) -> Vec<FencedBlock> {
    let mut details = Vec::new();
    let mut block_start: Option<usize> = None;
    let mut content_start = 0;
    let mut opening_span = (0, 0);
    let mut info_string = None;
    let mut fence_char = b'`';
    let mut fence_length = 0;

    for (line_start, line_end, line) in line_ranges(text) {
        if block_start.is_none() {
            let Some(opening) = opening_fence(line) else {
                continue;
            };
            fence_char = opening.char;
            fence_length = opening.length;
            block_start = Some(line_start);
            content_start = line_end;
            opening_span =
                (line_start + opening.run_start, line_start + opening.run_end);
            info_string = opening
                .info_string
                .map(|(start, end)| (line_start + start, line_start + end));
            continue;
        }

        let Some(closing) = closing_fence_span(line, fence_char, fence_length)
        else {
            continue;
        };
        let closing_fence = (line_start + closing.0, line_start + closing.1);
        details.push(FencedBlock {
            block_range: (block_start.unwrap(), closing_fence.1),
            opening_fence: opening_span,
            info_string,
            content_range: (content_start, line_start),
            closing_fence: Some(closing_fence),
        });
        block_start = None;
    }

    if let Some(block_start) = block_start {
        details.push(FencedBlock {
            block_range: (block_start, text.len()),
            opening_fence: opening_span,
            info_string,
            content_range: (content_start, text.len()),
            closing_fence: None,
        });
    }
    details
}

/// Return `(start, end)` byte ranges for every fenced code block.
pub fn fenced_block_ranges(text: &str) -> Vec<(usize, usize)> {
    fenced_block_details(text)
        .into_iter()
        .map(|block| block.block_range)
        .collect()
}

/// JSON-shaped fence details for the Python adapter.
pub fn fenced_block_details_wire(text: &str) -> Vec<FencedBlockWire> {
    fenced_block_details(text)
        .iter()
        .map(FencedBlockWire::from)
        .collect()
}

/// Capture `%if::` / `%proc::` fences that are not already inside an ordinary
/// fence. Intervening blank lines are allowed; unknown languages, missing or
/// unclosed fences, empty source, and multiply-owned fences are diagnostics.
pub fn scan_directive_owned_fences(text: &str) -> CodeDirectiveScanWire {
    let ordinary = fenced_block_details(text);
    let mut directives = Vec::new();
    let mut diagnostics = Vec::new();
    let mut claimed: BTreeMap<usize, String> = BTreeMap::new();

    for (line_start, line_end, line) in line_ranges(text) {
        if position_in_ranges(line_start, &ordinary) {
            continue;
        }
        let Some(name) = code_directive_double_colon_name(line) else {
            continue;
        };
        match take_owned_fence(text, line_end, &ordinary) {
            Ok(fence) => {
                if let Some(owner) = claimed.get(&fence.block_range.0) {
                    diagnostics.push(CodeDirectiveDiagnosticWire {
                        code: "multiply_owned_fence".to_string(),
                        message: format!(
                            "%{name}:: and %{owner}:: both claim the same fenced block"
                        ),
                        span: [line_start, fence.block_range.1],
                    });
                    continue;
                }
                let source = text[fence.content_range.0..fence.content_range.1]
                    .to_string();
                if source.trim().is_empty() {
                    diagnostics.push(CodeDirectiveDiagnosticWire {
                        code: "empty_code".to_string(),
                        message: format!(
                            "%{name}:: requires a non-empty fenced code block"
                        ),
                        span: [line_start, fence.block_range.1],
                    });
                    continue;
                }
                if fence.closing_fence.is_none() {
                    diagnostics.push(CodeDirectiveDiagnosticWire {
                        code: "unclosed_fence".to_string(),
                        message: format!(
                            "%{name}:: requires exactly one closed fenced code block"
                        ),
                        span: [line_start, text.len()],
                    });
                    continue;
                }
                let info = fence
                    .info_string
                    .map(|(start, end)| text[start..end].to_string());
                match language_from_info_string(info.as_deref()) {
                    Ok(language) => {
                        let span_end = closing_line_end(text, &fence);
                        claimed.insert(fence.block_range.0, name.to_string());
                        directives.push(CodeDirectiveSpanWire {
                            name: name.to_string(),
                            span: [line_start, span_end],
                            code: Some(
                                CodeValue {
                                    source,
                                    language,
                                    info_string: info,
                                }
                                .to_wire(),
                            ),
                        });
                    }
                    Err(message) => {
                        diagnostics.push(CodeDirectiveDiagnosticWire {
                            code: "unknown_language".to_string(),
                            message,
                            span: [line_start, fence.block_range.1],
                        })
                    }
                }
            }
            Err(diagnostic) => {
                diagnostics.push(CodeDirectiveDiagnosticWire {
                    span: [line_start, line_end],
                    ..diagnostic
                });
            }
        }
    }

    CodeDirectiveScanWire {
        schema_version: CODE_VALUE_WIRE_SCHEMA_VERSION,
        directives,
        diagnostics,
    }
}

/// Feature-flag key that gates `%if` and `%proc`.
pub fn typed_launch_units_flag_key() -> &'static str {
    TYPED_LAUNCH_UNITS_FLAG
}

/// Resolve a fence info-string (or unlabelled fence) to a supported language.
pub fn language_from_info_string(
    info: Option<&str>,
) -> Result<CodeLanguage, String> {
    let first = info.unwrap_or("").split_whitespace().next().unwrap_or("");
    match first.to_ascii_lowercase().as_str() {
        "" | "bash" => Ok(CodeLanguage::Bash),
        "python" => Ok(CodeLanguage::Python),
        other => Err(format!(
            "unsupported code language {other:?}; v1 accepts unlabelled, bash, or python"
        )),
    }
}

fn take_owned_fence(
    text: &str,
    after_directive: usize,
    ordinary: &[FencedBlock],
) -> Result<FencedBlock, CodeDirectiveDiagnosticWire> {
    for (line_start, line_end, line) in line_ranges(&text[after_directive..]) {
        let abs_start = after_directive + line_start;
        let abs_end = after_directive + line_end;
        let content = line_content(line);
        if content.trim().is_empty() {
            continue;
        }
        let Some(fence) = ordinary
            .iter()
            .find(|block| block.block_range.0 == abs_start)
            .cloned()
        else {
            return Err(CodeDirectiveDiagnosticWire {
                code: "missing_fence".to_string(),
                message:
                    "expected exactly one closed fenced code block after %if:: or %proc::"
                        .to_string(),
                span: [abs_start, abs_end],
            });
        };
        return Ok(fence);
    }
    Err(CodeDirectiveDiagnosticWire {
        code: "missing_fence".to_string(),
        message: "expected exactly one closed fenced code block after %if:: or %proc::"
            .to_string(),
        span: [after_directive, text.len()],
    })
}

fn closing_line_end(text: &str, fence: &FencedBlock) -> usize {
    let Some((_, close_end)) = fence.closing_fence else {
        return text.len();
    };
    for (line_start, line_end, _) in line_ranges(text) {
        if line_start <= close_end && close_end <= line_end {
            return line_end;
        }
    }
    fence.block_range.1
}

fn code_directive_double_colon_name(line: &str) -> Option<&'static str> {
    let content = line_content(line);
    let trimmed = content.trim_start_matches([' ', '\t']);
    let rest = trimmed.strip_prefix('%')?;
    let (name, suffix) = rest.split_once("::")?;
    if !suffix.trim().is_empty() {
        return None;
    }
    match name {
        "if" => Some("if"),
        "proc" => Some("proc"),
        other if proc_option_header(other) => Some("proc"),
        _ => None,
    }
}

fn proc_option_header(value: &str) -> bool {
    value.starts_with("proc(") && value.ends_with(')')
}

fn position_in_ranges(position: usize, fences: &[FencedBlock]) -> bool {
    fences.iter().any(|block| {
        let (start, end) = block.block_range;
        start <= position && position < end
    })
}

fn line_ranges(text: &str) -> impl Iterator<Item = (usize, usize, &str)> {
    let bytes = text.as_bytes();
    let mut start = 0;
    std::iter::from_fn(move || {
        if start >= bytes.len() {
            return None;
        }
        let mut end = start;
        while end < bytes.len() && bytes[end] != b'\n' {
            end += 1;
        }
        if end < bytes.len() {
            end += 1;
        }
        let line = &text[start..end];
        let range = (start, end, line);
        start = end;
        Some(range)
    })
}

fn line_content(line: &str) -> &str {
    let without_lf = line.strip_suffix('\n').unwrap_or(line);
    without_lf.strip_suffix('\r').unwrap_or(without_lf)
}

fn leading_spaces(line: &str) -> Option<usize> {
    let mut spaces = 0;
    for byte in line.as_bytes() {
        if *byte != b' ' {
            break;
        }
        spaces += 1;
        if spaces > 3 {
            return None;
        }
    }
    Some(spaces)
}

fn opening_fence(line: &str) -> Option<OpeningFence> {
    let content = line_content(line);
    let spaces = leading_spaces(content)?;
    if spaces == content.len() {
        return None;
    }
    let fence_char = content.as_bytes()[spaces];
    if fence_char != b'`' && fence_char != b'~' {
        return None;
    }
    let mut fence_end = spaces;
    while fence_end < content.len()
        && content.as_bytes()[fence_end] == fence_char
    {
        fence_end += 1;
    }
    let fence_length = fence_end - spaces;
    if fence_length < 3 {
        return None;
    }
    let mut info_start = fence_end;
    while info_start < content.len()
        && content.as_bytes()[info_start].is_ascii_whitespace()
    {
        info_start += 1;
    }
    let info_end = content.trim_end().len();
    let info_string = (info_start < info_end).then_some((info_start, info_end));
    Some(OpeningFence {
        char: fence_char,
        length: fence_length,
        run_start: spaces,
        run_end: fence_end,
        info_string,
    })
}

fn closing_fence_span(
    line: &str,
    fence_char: u8,
    fence_length: usize,
) -> Option<(usize, usize)> {
    let content = line_content(line);
    let spaces = leading_spaces(content)?;
    if spaces == content.len() {
        return None;
    }
    let fence_start = spaces;
    if content.as_bytes()[fence_start] != fence_char {
        return None;
    }
    let mut fence_end = fence_start;
    while fence_end < content.len()
        && content.as_bytes()[fence_end] == fence_char
    {
        fence_end += 1;
    }
    if fence_end - fence_start < fence_length {
        return None;
    }
    if !content[fence_end..].trim().is_empty() {
        return None;
    }
    Some((fence_start, fence_end))
}

fn normalize_newlines(source: &str) -> String {
    source.replace("\r\n", "\n").replace('\r', "\n")
}

fn safe_preview(source: &str) -> String {
    let line = source
        .lines()
        .find(|candidate| !candidate.trim().is_empty())
        .unwrap_or("")
        .trim();
    let cleaned: String = line
        .chars()
        .map(|ch| if ch.is_control() { ' ' } else { ch })
        .collect();
    let count = cleaned.chars().count();
    if count > PREVIEW_MAX_CHARS {
        let mut truncated: String =
            cleaned.chars().take(PREVIEW_MAX_CHARS - 1).collect();
        truncated.push('…');
        truncated
    } else {
        cleaned
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BOXED: &str = "before\n```\nouter snapshot\n| prompt displays an inner fence:\n|  ```\n|  @a9q.cdx\nstill inside the outer snapshot\n```\nafter\n";

    #[test]
    fn boxed_displayed_inner_fence_is_one_block() {
        let details = fenced_block_details(BOXED);
        assert_eq!(details.len(), 1);
        let start = "before\n".len();
        let block = &BOXED[details[0].block_range.0..details[0].block_range.1];
        assert!(block.starts_with("```\nouter snapshot"));
        assert_eq!(details[0].block_range.0, start);
        assert!(BOXED[details[0].block_range.1..].starts_with("\nafter\n"));
    }

    #[test]
    fn tilde_and_indented_info_string_offsets_match_python() {
        let prompt =
            "before\n  ~~~~ python title=demo\nprint('hi')\n ~~~~~  \nafter";
        let [details] = fenced_block_details(prompt).try_into().unwrap();
        assert_eq!(
            &prompt[details.opening_fence.0..details.opening_fence.1],
            "~~~~"
        );
        assert_eq!(
            &prompt[details.info_string.unwrap().0
                ..details.info_string.unwrap().1],
            "python title=demo"
        );
        assert_eq!(
            &prompt[details.content_range.0..details.content_range.1],
            "print('hi')\n"
        );
        assert_eq!(
            &prompt[details.closing_fence.unwrap().0
                ..details.closing_fence.unwrap().1],
            "~~~~~"
        );
        assert_eq!(fenced_block_ranges(prompt), vec![details.block_range]);
    }

    #[test]
    fn unclosed_fence_runs_to_eof() {
        let prompt = "```py\nvalue = 1";
        let [details] = fenced_block_details(prompt).try_into().unwrap();
        assert_eq!(details.closing_fence, None);
        assert_eq!(details.content_range, ("```py\n".len(), prompt.len()));
        assert_eq!(details.block_range, (0, prompt.len()));
    }

    #[test]
    fn crlf_opening_and_closing_fences_scan() {
        let prompt = "```bash\r\necho hi\r\n```\r\n";
        let [details] = fenced_block_details(prompt).try_into().unwrap();
        assert_eq!(
            &prompt[details.info_string.unwrap().0
                ..details.info_string.unwrap().1],
            "bash"
        );
        assert!(details.closing_fence.is_some());
    }

    #[test]
    fn owned_if_fence_is_opaque_and_captures_code_value() {
        let prompt = "%if::\n\n```bash\ntest -f pyproject.toml\n# not a heading\n```\nReview";
        let scan = scan_directive_owned_fences(prompt);
        assert!(scan.diagnostics.is_empty(), "{:?}", scan.diagnostics);
        assert_eq!(scan.directives.len(), 1);
        let directive = &scan.directives[0];
        assert_eq!(directive.name, "if");
        let code = directive.code.as_ref().unwrap();
        assert_eq!(code.language, "bash");
        assert!(code.source.contains("test -f pyproject.toml"));
        assert!(code.source.contains("# not a heading"));
        assert_eq!(code.digest.len(), 64);
        assert!(!code.preview.is_empty());
        assert_eq!(&prompt[directive.span[1]..], "Review");
    }

    #[test]
    fn if_inside_ordinary_fence_is_not_owned() {
        let prompt = "```\n%if::\n```bash\ntrue\n```\n```\n";
        let scan = scan_directive_owned_fences(prompt);
        assert!(scan.directives.is_empty());
        assert!(scan.diagnostics.is_empty());
    }

    #[test]
    fn unknown_language_and_missing_fence_are_diagnostics() {
        let missing = scan_directive_owned_fences("%if::\n\nReview");
        assert_eq!(missing.diagnostics[0].code, "missing_fence");

        let unknown =
            scan_directive_owned_fences("%proc::\n```ruby\nputs 1\n```\n");
        assert_eq!(unknown.diagnostics[0].code, "unknown_language");
    }

    #[test]
    fn unlabelled_fence_is_bash() {
        let prompt = "%proc::\n```\njust check\n```\n";
        let scan = scan_directive_owned_fences(prompt);
        assert!(scan.diagnostics.is_empty(), "{:?}", scan.diagnostics);
        assert_eq!(scan.directives[0].code.as_ref().unwrap().language, "bash");
    }

    #[test]
    fn python_info_string_and_empty_source_and_unclosed() {
        let python = scan_directive_owned_fences(
            "%if::\n```python\nraise SystemExit(0)\n```\n",
        );
        assert_eq!(
            python.directives[0].code.as_ref().unwrap().language,
            "python"
        );

        let empty = scan_directive_owned_fences("%if::\n```bash\n\n```\n");
        assert_eq!(empty.diagnostics[0].code, "empty_code");

        let unclosed = scan_directive_owned_fences("%if::\n```bash\ntrue\n");
        assert_eq!(unclosed.diagnostics[0].code, "unclosed_fence");
    }

    #[test]
    fn digest_normalizes_crlf() {
        let unix = CodeValue {
            source: "echo hi\n".to_string(),
            language: CodeLanguage::Bash,
            info_string: None,
        };
        let dos = CodeValue {
            source: "echo hi\r\n".to_string(),
            language: CodeLanguage::Bash,
            info_string: None,
        };
        assert_eq!(unix.digest(), dos.digest());
    }
}
