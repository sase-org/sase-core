//! Volatile-token masking for scheduler error text.
//!
//! Masking runs before lowercasing so case-insensitive classes (hex SHAs,
//! `PID` prefixes) match regardless of the original casing. Rules apply in
//! an order that keeps composite tokens intact: `agent@sha` forms and
//! absolute paths first (they contain digits and hex runs that later rules
//! would otherwise carve up), then timestamps, dates, versions, durations,
//! pids, and run/request ids, and only last the bare hex and integer runs.

use std::sync::OnceLock;

use regex::Regex;

use super::wire::AXE_ERROR_MAX_SIGNATURE_CHARS;

fn agent_at_sha_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"[A-Za-z0-9_.-]*[.-][A-Za-z0-9_.-]*@[0-9a-fA-F]{7,}")
            .expect("agent@sha pattern is static")
    })
}

fn path_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"/([^/\s,:]+/)*[^/\s,:]+").expect("path pattern is static")
    })
}

fn uuid_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b",
        )
        .expect("uuid pattern is static")
    })
}

fn iso_timestamp_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})?\b",
        )
        .expect("iso timestamp pattern is static")
    })
}

fn bare_date_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"\b\d{4}-\d{2}-\d{2}\b").expect("date pattern is static")
    })
}

fn version_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"\b\d+\.\d+(\.\d+)*\b").expect("version pattern is static")
    })
}

fn duration_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"(?i)\b\d+(\.\d+)?\s?(milliseconds?|seconds?|secs?|minutes?|mins?|hours?|hrs?|days?|ms|s|m|h|d)\b",
        )
        .expect("duration pattern is static")
    })
}

fn pid_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(?i)\bpid\s*[=:]?\s*\d+\b").expect("pid pattern is static")
    })
}

fn named_id_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"(?i)\b((run|request|job|task|attempt)[-_ ]?id)\s*[=:]?\s*[\w.~-]+",
        )
        .expect("named id pattern is static")
    })
}

fn dashed_id_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(?i)\b(run|request|job|task)[-_][A-Za-z0-9_.-]+")
            .expect("dashed id pattern is static")
    })
}

fn hex_sha_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        // The pre-token guard is inline (no look-around support): capture it
        // and re-emit it so only the hex run itself is masked.
        Regex::new(r"(^|[^A-Za-z0-9_@])([0-9a-fA-F]{7,})\b")
            .expect("hex sha pattern is static")
    })
}

fn bare_int_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        // Colons and dots are excluded from the pre-token guard so clock
        // times and version tails the earlier rules left alone keep their
        // separators instead of masking piecemeal.
        Regex::new(r"(^|[^A-Za-z0-9_:.])(\d+)")
            .expect("bare integer pattern is static")
    })
}

/// Turn error text into a stable signature.
///
/// Volatile tokens are masked, then the text is lowercased, whitespace is
/// collapsed, and the result truncates to
/// [`AXE_ERROR_MAX_SIGNATURE_CHARS`] characters.
pub fn normalize_error_signature(text: &str) -> String {
    let masked = agent_at_sha_re().replace_all(text, "<agent>");
    let masked = path_re().replace_all(&masked, "<path>");
    let masked = uuid_re().replace_all(&masked, "<id>");
    let masked = iso_timestamp_re().replace_all(&masked, "<time>");
    let masked = bare_date_re().replace_all(&masked, "<time>");
    let masked = version_re().replace_all(&masked, "<v>");
    let masked = duration_re().replace_all(&masked, "<dur>");
    let masked = pid_re().replace_all(&masked, "pid <pid>");
    let masked = named_id_re().replace_all(&masked, "$1 <id>");
    let masked = dashed_id_re().replace_all(&masked, "$1 <id>");
    let masked = hex_sha_re().replace_all(&masked, |caps: &regex::Captures| {
        let hex = &caps[2];
        if hex.bytes().any(|byte| byte.is_ascii_digit()) {
            format!("{}<sha>", &caps[1])
        } else {
            caps[0].to_string()
        }
    });
    let masked = bare_int_re().replace_all(&masked, "${1}<n>");
    let lowered = masked.to_lowercase();
    let collapsed = lowered.split_whitespace().collect::<Vec<_>>().join(" ");
    truncate_chars(&collapsed, AXE_ERROR_MAX_SIGNATURE_CHARS)
}

fn truncate_chars(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    text.chars().take(max_chars).collect()
}
