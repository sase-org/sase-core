//! AXE subprocess failure diagnostics.

use std::sync::OnceLock;

use regex::Regex;
use serde::{Deserialize, Serialize};

use super::wire::{ChopEngineError, CHOP_ENGINE_SCHEMA_VERSION};

pub const CHOP_SUBPROCESS_DIAGNOSTIC_MAX_LINES: usize = 200;
pub const CHOP_SUBPROCESS_DIAGNOSTIC_MAX_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChopSubprocessOutputStatusWire {
    Captured,
    Absent,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChopSubprocessDiagnosticRequestWire {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub run_id: Option<String>,
    #[serde(default)]
    pub exit_code: Option<i64>,
    #[serde(default)]
    pub source_log_path: Option<String>,
    #[serde(default)]
    pub output: Option<String>,
    #[serde(default)]
    pub output_status: Option<ChopSubprocessOutputStatusWire>,
    #[serde(default)]
    pub unavailable_reason: Option<String>,
    #[serde(default)]
    pub input_omitted_bytes: u64,
    #[serde(default)]
    pub had_decode_errors: bool,
    #[serde(default = "default_max_lines")]
    pub max_lines: usize,
    #[serde(default = "default_max_bytes")]
    pub max_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChopSubprocessDiagnosticWire {
    pub schema_version: u32,
    pub run_id: Option<String>,
    pub exit_code: Option<i64>,
    pub source_log_path: Option<String>,
    pub output_status: ChopSubprocessOutputStatusWire,
    pub unavailable_reason: Option<String>,
    pub output_excerpt: String,
    pub truncated: bool,
    pub omitted_lines: u64,
    pub omitted_bytes: u64,
    pub had_decode_errors: bool,
}

pub fn normalize_chop_subprocess_diagnostic(
    request: &ChopSubprocessDiagnosticRequestWire,
) -> Result<ChopSubprocessDiagnosticWire, ChopEngineError> {
    if request.schema_version != CHOP_ENGINE_SCHEMA_VERSION {
        return Err(ChopEngineError::new(
            "invalid_schema_version",
            "$.schema_version",
            format!(
                "expected schema_version {CHOP_ENGINE_SCHEMA_VERSION}, got {}",
                request.schema_version
            ),
        ));
    }

    let raw_output = request.output.as_deref().unwrap_or("");
    let requested_status = request.output_status.unwrap_or({
        if raw_output.is_empty() {
            ChopSubprocessOutputStatusWire::Absent
        } else {
            ChopSubprocessOutputStatusWire::Captured
        }
    });
    let status = match requested_status {
        ChopSubprocessOutputStatusWire::Unavailable => {
            ChopSubprocessOutputStatusWire::Unavailable
        }
        ChopSubprocessOutputStatusWire::Absent if raw_output.is_empty() => {
            ChopSubprocessOutputStatusWire::Absent
        }
        ChopSubprocessOutputStatusWire::Absent => {
            ChopSubprocessOutputStatusWire::Captured
        }
        ChopSubprocessOutputStatusWire::Captured if raw_output.is_empty() => {
            ChopSubprocessOutputStatusWire::Absent
        }
        ChopSubprocessOutputStatusWire::Captured => {
            ChopSubprocessOutputStatusWire::Captured
        }
    };

    let normalized = if status == ChopSubprocessOutputStatusWire::Captured {
        redact_telegram_credentials(&strip_control_sequences(raw_output))
    } else {
        String::new()
    };
    let (output_excerpt, omitted_lines, excerpt_omitted_bytes) =
        tail_text_by_lines_and_bytes(
            &normalized,
            request.max_lines.min(CHOP_SUBPROCESS_DIAGNOSTIC_MAX_LINES),
            request.max_bytes.min(CHOP_SUBPROCESS_DIAGNOSTIC_MAX_BYTES),
        );
    let omitted_bytes = request.input_omitted_bytes + excerpt_omitted_bytes;
    let truncated = request.input_omitted_bytes > 0
        || omitted_lines > 0
        || omitted_bytes > 0;

    Ok(ChopSubprocessDiagnosticWire {
        schema_version: CHOP_ENGINE_SCHEMA_VERSION,
        run_id: request.run_id.clone(),
        exit_code: request.exit_code,
        source_log_path: request.source_log_path.clone(),
        output_status: status,
        unavailable_reason: match status {
            ChopSubprocessOutputStatusWire::Unavailable => {
                request.unavailable_reason.clone()
            }
            _ => None,
        },
        output_excerpt,
        truncated,
        omitted_lines,
        omitted_bytes,
        had_decode_errors: request.had_decode_errors,
    })
}

fn default_schema_version() -> u32 {
    CHOP_ENGINE_SCHEMA_VERSION
}

fn default_max_lines() -> usize {
    CHOP_SUBPROCESS_DIAGNOSTIC_MAX_LINES
}

fn default_max_bytes() -> usize {
    CHOP_SUBPROCESS_DIAGNOSTIC_MAX_BYTES
}

fn strip_control_sequences(text: &str) -> String {
    let normalized = text.replace("\r\n", "\n").replace('\r', "\n");
    let without_osc = ansi_osc_re().replace_all(&normalized, "");
    let without_csi = ansi_csi_re().replace_all(&without_osc, "");
    let without_single = ansi_single_re().replace_all(&without_csi, "");
    without_single
        .chars()
        .filter(|c| *c == '\n' || *c == '\t' || !c.is_control())
        .collect()
}

fn redact_telegram_credentials(text: &str) -> String {
    let redacted_urls = telegram_url_token_re()
        .replace_all(text, "${1}<redacted>")
        .into_owned();
    let redacted_assignments = telegram_assignment_token_re()
        .replace_all(&redacted_urls, "$1$2<redacted>$4")
        .into_owned();
    bare_bot_token_re()
        .replace_all(&redacted_assignments, "bot<redacted>")
        .into_owned()
}

fn tail_text_by_lines_and_bytes(
    text: &str,
    max_lines: usize,
    max_bytes: usize,
) -> (String, u64, u64) {
    let lines: Vec<&str> = text.lines().collect();
    let omitted_lines = lines.len().saturating_sub(max_lines);
    let selected_lines = if max_lines == 0 {
        &lines[lines.len()..]
    } else {
        &lines[omitted_lines..]
    };

    let selected = selected_lines.join("\n");
    if selected.len() <= max_bytes {
        return (selected, omitted_lines as u64, 0);
    }
    if max_bytes == 0 {
        return (String::new(), omitted_lines as u64, selected.len() as u64);
    }

    let mut start = selected.len() - max_bytes;
    while !selected.is_char_boundary(start) {
        start += 1;
    }
    let tail = selected[start..].to_string();
    let omitted_bytes = selected.len() - tail.len();
    (tail, omitted_lines as u64, omitted_bytes as u64)
}

fn ansi_csi_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\x1B\[[0-?]*[ -/]*[@-~]").unwrap())
}

fn ansi_osc_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\x1B\][^\x07]*(?:\x07|\x1B\\)").unwrap())
}

fn ansi_single_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\x1B[@-Z\\-_]").unwrap())
}

fn telegram_url_token_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"(?i)(https?://api\.telegram\.org/bot)([0-9]{5,}:[A-Za-z0-9_-]{20,})",
        )
        .unwrap()
    })
}

fn telegram_assignment_token_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r#"(?i)\b(telegram(?:_bot)?_token|bot_token|token)(\s*[:=]\s*["']?)([0-9]{5,}:[A-Za-z0-9_-]{20,})(["']?)"#,
        )
        .unwrap()
    })
}

fn bare_bot_token_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(?i)\bbot([0-9]{5,}:[A-Za-z0-9_-]{20,})").unwrap()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(output: &str) -> ChopSubprocessDiagnosticRequestWire {
        ChopSubprocessDiagnosticRequestWire {
            schema_version: CHOP_ENGINE_SCHEMA_VERSION,
            run_id: Some("20260906T211142_996558".to_string()),
            exit_code: Some(1),
            source_log_path: Some("/tmp/run.log".to_string()),
            output: Some(output.to_string()),
            output_status: Some(ChopSubprocessOutputStatusWire::Captured),
            unavailable_reason: None,
            input_omitted_bytes: 0,
            had_decode_errors: false,
            max_lines: CHOP_SUBPROCESS_DIAGNOSTIC_MAX_LINES,
            max_bytes: CHOP_SUBPROCESS_DIAGNOSTIC_MAX_BYTES,
        }
    }

    #[test]
    fn normalizes_redacts_and_tails_subprocess_output() {
        let mut req = request(
            "line 1\n\x1b[31mline 2\x1b[0m\nhttps://api.telegram.org/bot123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi/getUpdates\ntelegram.error.TimedOut: Timed out",
        );
        req.max_lines = 2;

        let diagnostic = normalize_chop_subprocess_diagnostic(&req).unwrap();

        assert_eq!(
            diagnostic.output_status,
            ChopSubprocessOutputStatusWire::Captured
        );
        assert!(diagnostic
            .output_excerpt
            .contains("telegram.error.TimedOut"));
        assert!(diagnostic
            .output_excerpt
            .contains("https://api.telegram.org/bot<redacted>/getUpdates"));
        assert!(!diagnostic.output_excerpt.contains("123456789:"));
        assert!(!diagnostic.output_excerpt.contains("\x1b"));
        assert_eq!(diagnostic.omitted_lines, 2);
        assert!(diagnostic.truncated);
    }

    #[test]
    fn byte_bound_keeps_utf8_boundary_and_marks_truncation() {
        let mut req = request("alpha\nbeta\ngamma");
        req.max_lines = 10;
        req.max_bytes = 7;

        let diagnostic = normalize_chop_subprocess_diagnostic(&req).unwrap();

        assert_eq!(diagnostic.output_excerpt, "a\ngamma");
        assert_eq!(diagnostic.omitted_lines, 0);
        assert_eq!(diagnostic.omitted_bytes, 9);
        assert!(diagnostic.truncated);
    }

    #[test]
    fn reports_absent_and_unavailable_output() {
        let absent =
            normalize_chop_subprocess_diagnostic(&request("")).unwrap();
        assert_eq!(
            absent.output_status,
            ChopSubprocessOutputStatusWire::Absent
        );
        assert_eq!(absent.output_excerpt, "");

        let unavailable = normalize_chop_subprocess_diagnostic(
            &ChopSubprocessDiagnosticRequestWire {
                schema_version: CHOP_ENGINE_SCHEMA_VERSION,
                run_id: Some("run".to_string()),
                exit_code: Some(-9),
                source_log_path: Some("/tmp/missing.log".to_string()),
                output: None,
                output_status: Some(
                    ChopSubprocessOutputStatusWire::Unavailable,
                ),
                unavailable_reason: Some("missing_log".to_string()),
                input_omitted_bytes: 0,
                had_decode_errors: false,
                max_lines: CHOP_SUBPROCESS_DIAGNOSTIC_MAX_LINES,
                max_bytes: CHOP_SUBPROCESS_DIAGNOSTIC_MAX_BYTES,
            },
        )
        .unwrap();
        assert_eq!(
            unavailable.output_status,
            ChopSubprocessOutputStatusWire::Unavailable
        );
        assert_eq!(unavailable.exit_code, Some(-9));
        assert_eq!(
            unavailable.unavailable_reason.as_deref(),
            Some("missing_log")
        );
    }
}
