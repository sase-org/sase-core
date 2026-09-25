//! Line normalization, path-root stripping, display redaction and bound.

use regex::Regex;
use std::sync::OnceLock;

use super::wire::TOOL_RUN_TRIAGE_DISPLAY_MAX_CHARS;

fn csi_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new("\x1b\\[[0-9;?]*[ -/]*[@-~]").unwrap())
}

fn osc_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new("\x1b\\][^\x07\x1b]*(?:\x07|\x1b\\\\)").unwrap()
    })
}

fn esc_pair_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new("\x1b.").unwrap())
}

fn linked_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\S*/sase/repos/linked/[^/\s]+/").unwrap())
}

fn workspaces_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"\S*/workspaces/[^/\s]+/[^/\s]+/[^/\s]+_\d+/").unwrap()
    })
}

fn sase_num_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\S*/sase_\d+/").unwrap())
}

fn cargo_target_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"\S*?/(?:cargo-targets/[^/\s]+|target)/(?:[^/\s]+/)*?(debug|release)/",
        )
        .unwrap()
    })
}

fn tmp_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"/tmp/\S*").unwrap())
}

fn iso_ts_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?",
        )
        .unwrap()
    })
}

fn clock_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\b\d{2}:\d{2}:\d{2}(?:\.\d+)?\b").unwrap())
}

fn duration_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"\b\d+(?:\.\d+)?(?:ns|µs|us|ms|s|m|h)\b").unwrap()
    })
}

fn pytest_dur_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\(\d+:\d{2}:\d{2}\)").unwrap())
}

fn thread_pid_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"thread '(.*)' \(\d+\)").unwrap())
}

fn pid_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\bpid[ =:]\d+").unwrap())
}

fn hex_addr_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\b0x[0-9a-fA-F]+\b").unwrap())
}

fn long_hex_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\b[0-9a-fA-F]{12,}\b").unwrap())
}

fn secret_kv_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"(?i)(token|secret|password|passwd|api[_-]?key|authorization|bearer)\s*[:=]\s*\S+",
        )
        .unwrap()
    })
}

fn gh_token_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"ghp_[A-Za-z0-9_]{20,}|gho_[A-Za-z0-9_]{20,}|ghs_[A-Za-z0-9_]{20,}|github_pat_[A-Za-z0-9_]{20,}").unwrap()
    })
}

fn sk_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"sk-[A-Za-z0-9_-]{20,}").unwrap())
}

fn xox_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"xox[abprs]-\S+").unwrap())
}

fn akia_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"AKIA[0-9A-Z]{16}").unwrap())
}

fn env_assign_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\b[A-Z][A-Z0-9_]{2,}=\S+").unwrap())
}

fn abs_path_token_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(^|\s)(/(?:[^/\s]+/)+[^/\s]*)").unwrap())
}

fn whitespace_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\s+").unwrap())
}

fn with_trailing_slash(root: &str) -> String {
    if root.ends_with('/') {
        root.to_string()
    } else {
        format!("{root}/")
    }
}

fn contains_digit(text: &str) -> bool {
    text.bytes().any(|byte| byte.is_ascii_digit())
}

/// Normalize one output line before any extractor sees it.
pub fn normalize_line(line: &str, roots: &[String]) -> String {
    // 1. Strip ANSI/terminal control; keep only text after last \r.
    let mut text = line.split('\r').next_back().unwrap_or("").to_string();
    text = csi_re().replace_all(&text, "").into_owned();
    text = osc_re().replace_all(&text, "").into_owned();
    text = esc_pair_re().replace_all(&text, "").into_owned();
    text = text
        .chars()
        .filter(|ch| *ch == '\t' || !ch.is_control())
        .collect();
    // 2. Strip roots (longest first).
    let mut sorted_roots: Vec<String> =
        roots.iter().map(|root| with_trailing_slash(root)).collect();
    sorted_roots.sort_by_key(|left| std::cmp::Reverse(left.len()));
    for root in &sorted_roots {
        text = text.replace(root.as_str(), "");
    }
    text = linked_re().replace_all(&text, "").into_owned();
    text = workspaces_re().replace_all(&text, "").into_owned();
    text = sase_num_re().replace_all(&text, "").into_owned();
    // 3. Cargo target dirs.
    text = cargo_target_re()
        .replace_all(&text, "<target>/$1/")
        .into_owned();
    // 4. /tmp.
    text = tmp_re().replace_all(&text, "<tmp>").into_owned();
    // 5. Timestamps.
    text = iso_ts_re().replace_all(&text, "<ts>").into_owned();
    text = clock_re().replace_all(&text, "<time>").into_owned();
    // 6. Durations.
    text = duration_re().replace_all(&text, "<dur>").into_owned();
    text = pytest_dur_re().replace_all(&text, "(<dur>)").into_owned();
    // 7. PIDs.
    text = thread_pid_re()
        .replace_all(&text, "thread '$1' (<pid>)")
        .into_owned();
    text = pid_re().replace_all(&text, "pid=<pid>").into_owned();
    // 8. Hex ids.
    text = hex_addr_re().replace_all(&text, "<hex>").into_owned();
    let result = long_hex_re().replace_all(&text, |caps: &regex::Captures| {
        let hit = &caps[0];
        if contains_digit(hit) {
            "<hex>".to_string()
        } else {
            hit.to_string()
        }
    });
    result.into_owned()
}

/// Normalize, then redact and bound to 512 chars on a char boundary.
pub fn display_text(raw: &str) -> String {
    let spaced = raw.replace(['\r', '\n'], " ");
    let normalized = normalize_line(&spaced, &[]);
    let mut text = secret_kv_re()
        .replace_all(&normalized, "$1=<redacted>")
        .into_owned();
    // Secret-shaped tokens become a bare <redacted>.
    for replacer in [gh_token_re(), sk_re(), xox_re(), akia_re()] {
        text = replacer.replace_all(&text, "<redacted>").into_owned();
    }
    text = env_assign_re()
        .replace_all(&text, |caps: &regex::Captures| {
            let hit = &caps[0];
            match hit.find('=') {
                Some(index) => format!("{}=<redacted>", &hit[..index]),
                None => hit.to_string(),
            }
        })
        .into_owned();
    text = abs_path_token_re()
        .replace_all(&text, |caps: &regex::Captures| {
            let prefix = caps.get(1).map(|hit| hit.as_str()).unwrap_or("");
            let hit = caps.get(2).map(|hit| hit.as_str()).unwrap_or("");
            let last = hit.rsplit('/').next().unwrap_or("");
            if last.is_empty() {
                format!("{prefix}<path>")
            } else {
                format!("{prefix}<path>/{last}")
            }
        })
        .into_owned();
    text = whitespace_re().replace_all(text.trim(), " ").into_owned();
    bound_chars(&text, TOOL_RUN_TRIAGE_DISPLAY_MAX_CHARS)
}

fn bound_chars(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let mut out: String = text.chars().take(max_chars - 1).collect();
    out.push('…');
    out
}

/// Strip a leading `./`; return None when the path is still absolute.
pub fn clean_locator_path(raw: &str) -> Option<String> {
    let stripped = raw.strip_prefix("./").unwrap_or(raw);
    if stripped.starts_with('/') {
        return None;
    }
    if stripped.is_empty() {
        return None;
    }
    Some(stripped.to_string())
}
