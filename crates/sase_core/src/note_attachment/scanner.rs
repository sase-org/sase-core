//! Streaming secret scanner for attachment bytes.
//!
//! `attachment_scan_file` runs on the ingested CAS object for text classes
//! (including SVG), up to `max_bytes`. Returns `clean`, `hit`, or
//! `skipped`. A hit carries only kind, rule id, and line — never a matched
//! value or surrounding text.

use std::collections::HashMap;
use std::sync::OnceLock;

use aho_corasick::AhoCorasick;
use regex::Regex;
use serde::{Deserialize, Serialize};

use super::zones::sase_secret_file_paths;

/// Scanner rules version, pinned by tests and exposed to Python.
pub const ATTACHMENT_SCANNER_RULES_VERSION: u32 = 1;

const MAX_KNOWN_VALUE_CHARS: usize = 12;
const MAX_LINE_SCAN_BYTES: usize = 16 * 1024;
const LINE_WINDOW_OVERLAP: usize = 256;
const HIGH_ENTROPY_MIN_LEN: usize = 20;
const HIGH_ENTROPY_THRESHOLD: f64 = 4.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttachmentScanOutcomeWire {
    Clean,
    Hit,
    Skipped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttachmentScanHitKindWire {
    KnownValue,
    CredentialPattern,
    EnvDump,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttachmentScanHitWire {
    pub kind: AttachmentScanHitKindWire,
    pub rule_id: String,
    pub line: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttachmentScanWire {
    pub outcome: AttachmentScanOutcomeWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hit: Option<AttachmentScanHitWire>,
    pub bytes_scanned: u64,
    pub rules_version: u32,
}

impl AttachmentScanWire {
    fn clean(bytes_scanned: u64) -> Self {
        Self {
            outcome: AttachmentScanOutcomeWire::Clean,
            hit: None,
            bytes_scanned,
            rules_version: ATTACHMENT_SCANNER_RULES_VERSION,
        }
    }

    fn skipped(bytes_scanned: u64) -> Self {
        Self {
            outcome: AttachmentScanOutcomeWire::Skipped,
            hit: None,
            bytes_scanned,
            rules_version: ATTACHMENT_SCANNER_RULES_VERSION,
        }
    }

    fn hit(
        kind: AttachmentScanHitKindWire,
        rule_id: &str,
        line: u64,
        bytes_scanned: u64,
    ) -> Self {
        Self {
            outcome: AttachmentScanOutcomeWire::Hit,
            hit: Some(AttachmentScanHitWire {
                kind,
                rule_id: rule_id.to_string(),
                line,
            }),
            bytes_scanned,
            rules_version: ATTACHMENT_SCANNER_RULES_VERSION,
        }
    }
}

pub fn attachment_scanner_rules_version() -> u32 {
    ATTACHMENT_SCANNER_RULES_VERSION
}

fn secret_name_match(name: &str) -> bool {
    let upper = name.to_ascii_uppercase();
    if upper.contains("TOKEN")
        || upper.contains("SECRET")
        || upper.contains("PASSWORD")
        || upper.contains("CREDENTIAL")
    {
        return true;
    }
    if upper == "KEY" || upper.ends_with("_KEY") {
        return true;
    }
    if let Some(api) = upper.find("API") {
        if upper[api..].contains("KEY") {
            return true;
        }
    }
    false
}

fn keep_known_value(value: &str) -> bool {
    let trimmed = value.trim();
    if trimmed.chars().count() < MAX_KNOWN_VALUE_CHARS {
        return false;
    }
    let lower = trimmed.to_ascii_lowercase();
    if lower == "true" || lower == "false" {
        return false;
    }
    if trimmed.starts_with('/') {
        return false;
    }
    if !trimmed.is_empty() && trimmed.bytes().all(|byte| byte.is_ascii_digit())
    {
        return false;
    }
    true
}

fn collect_known_values(
    env: &HashMap<String, String>,
    home: &str,
    sase_home: &str,
) -> Vec<String> {
    let _ = home;
    let mut values = Vec::new();
    for (name, value) in env {
        if !secret_name_match(name) {
            continue;
        }
        if keep_known_value(value) {
            values.push(value.trim().to_string());
        }
    }
    for path in sase_secret_file_paths(sase_home) {
        let Ok(content) = std::fs::read_to_string(&path) else {
            continue;
        };
        let trimmed = content.trim().to_string();
        if keep_known_value(&trimmed) {
            values.push(trimmed);
        }
    }
    values.sort();
    values.dedup();
    values
}

fn github_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(ghp_|gho_|ghu_|ghs_|ghr_|github_pat_)[A-Za-z0-9_]{20,}")
            .unwrap()
    })
}

fn openai_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"sk-[A-Za-z0-9_-]{20,}").unwrap())
}

fn anthropic_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"sk-ant-[A-Za-z0-9_-]{20,}").unwrap())
}

fn google_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"AIza[A-Za-z0-9_-]{35}").unwrap())
}

fn aws_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"AKIA[0-9A-Z]{16}|ASIA[0-9A-Z]{16}").unwrap())
}

fn slack_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"xox[abprs]-[A-Za-z0-9-]{10,}").unwrap())
}

fn telegram_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"\b[0-9]{5,12}:[A-Za-z0-9_-]{30,}\b").unwrap()
    })
}

fn stripe_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(sk_live_|rk_live_)[A-Za-z0-9_]{15,}").unwrap()
    })
}

fn pem_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----")
            .unwrap()
    })
}

fn jwt_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b")
            .unwrap()
    })
}

fn kv_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r#"(?i)(key|secret|token|password)\s*[:=]\s*['"]?([A-Za-z0-9_\-+/=]{20,})['"]?"#,
        )
        .unwrap()
    })
}

fn env_assign_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^[A-Z][A-Z0-9_]{2,}\s*=\S+").unwrap())
}

type CredentialRule = (&'static str, fn() -> &'static Regex);

fn credential_rules() -> &'static [CredentialRule] {
    static RULES: &[CredentialRule] = &[
        ("github-token", github_re),
        ("openai-key", openai_re),
        ("anthropic-key", anthropic_re),
        ("google-api-key", google_re),
        ("aws-access-key", aws_re),
        ("slack-token", slack_re),
        ("telegram-bot-token", telegram_re),
        ("stripe-key", stripe_re),
        ("pem-private-key", pem_re),
        ("jwt", jwt_re),
    ];
    RULES
}

const WELL_KNOWN_ENV_KEYS: &[&str] = &[
    "PATH", "HOME", "USER", "SHELL", "PWD", "LANG", "TERM", "EDITOR", "VISUAL",
    "TMPDIR", "TEMP", "HOSTNAME", "LOGNAME", "UID", "TZ", "LC_ALL", "LANGUAGE",
    "SHELL",
];

fn shannon_entropy(value: &str) -> f64 {
    if value.is_empty() {
        return 0.0;
    }
    let mut counts = HashMap::new();
    for byte in value.bytes() {
        *counts.entry(byte).or_insert(0usize) += 1;
    }
    let len = value.len() as f64;
    counts
        .values()
        .map(|count| {
            let probability = *count as f64 / len;
            -probability * probability.log2()
        })
        .sum()
}

fn scan_windows(line: &str) -> Vec<&str> {
    if line.len() <= MAX_LINE_SCAN_BYTES {
        return vec![line];
    }
    let bytes = line.as_bytes();
    let mut windows = Vec::new();
    let mut start = 0;
    while start < bytes.len() {
        let mut end = (start + MAX_LINE_SCAN_BYTES).min(bytes.len());
        while end < bytes.len() && !line.is_char_boundary(end) {
            end += 1;
        }
        let mut window_start = start;
        while window_start > 0 && !line.is_char_boundary(window_start) {
            window_start += 1;
        }
        windows.push(&line[window_start..end]);
        if end >= bytes.len() {
            break;
        }
        start = end.saturating_sub(LINE_WINDOW_OVERLAP);
        if start >= end {
            break;
        }
    }
    windows
}

fn kv_hit(window: &str) -> bool {
    for captures in kv_re().captures_iter(window) {
        let value = captures.get(2).map(|hit| hit.as_str()).unwrap_or_default();
        if value.len() >= HIGH_ENTROPY_MIN_LEN
            && shannon_entropy(value) >= HIGH_ENTROPY_THRESHOLD
        {
            return true;
        }
    }
    false
}

fn well_known_key_count(line: &str) -> usize {
    WELL_KNOWN_ENV_KEYS
        .iter()
        .filter(|key| {
            line.split(|character: char| {
                !(character.is_ascii_alphanumeric() || character == '_')
            })
            .any(|token| token == **key)
        })
        .count()
}

/// Scan one file for secrets. `env` maps variable names to values.
///
/// Reads at most `max_bytes` content bytes in bounded chunks, detects
/// values split across chunk boundaries, and bounds memory for long
/// lines. Never returns a matched value or line content.
pub fn attachment_scan_file(
    path: &str,
    max_bytes: u64,
    env: &HashMap<String, String>,
    home: &str,
    sase_home: &str,
) -> AttachmentScanWire {
    let known = collect_known_values(env, home, sase_home);
    let automaton = if known.is_empty() {
        None
    } else {
        AhoCorasick::new(&known).ok()
    };

    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(_) => return AttachmentScanWire::skipped(0),
    };
    let file_len = file
        .metadata()
        .map(|metadata| metadata.len())
        .unwrap_or(u64::MAX);
    if file_len > max_bytes {
        return AttachmentScanWire::skipped(max_bytes);
    }

    use std::io::Read;
    let mut content = Vec::new();
    let mut file = file;
    let read_result =
        file.by_ref().take(max_bytes + 1).read_to_end(&mut content);
    if read_result.is_err() {
        return AttachmentScanWire::skipped(0);
    }
    if content.len() as u64 > max_bytes {
        return AttachmentScanWire::skipped(max_bytes);
    }
    let bytes_scanned = content.len() as u64;
    if content.contains(&0) {
        return AttachmentScanWire::skipped(bytes_scanned);
    }
    let Ok(text) = std::str::from_utf8(&content) else {
        return AttachmentScanWire::skipped(bytes_scanned);
    };

    // Detector 1: known values over the full text (chunk-straddling safe:
    // the automaton sees the whole buffer, so a value split across any
    // read-chunk boundary still matches). Line numbers come from the
    // match offset.
    if let Some(automaton) = automaton.as_ref() {
        if let Some(hit) = automaton.find(text) {
            let line =
                text[..hit.start()].bytes().filter(|b| *b == b'\n').count()
                    as u64
                    + 1;
            return AttachmentScanWire::hit(
                AttachmentScanHitKindWire::KnownValue,
                "known-value",
                line,
                bytes_scanned,
            );
        }
    }

    // Detectors 2-3: per-line credential patterns and env-dump windows.
    // Long lines are scanned in bounded overlapping windows.
    let mut assign_window: Vec<bool> = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let line_no = index as u64 + 1;
        for window in scan_windows(line) {
            for (rule_id, rule) in credential_rules() {
                if rule().is_match(window) {
                    return AttachmentScanWire::hit(
                        AttachmentScanHitKindWire::CredentialPattern,
                        rule_id,
                        line_no,
                        bytes_scanned,
                    );
                }
            }
            if kv_hit(window) {
                return AttachmentScanWire::hit(
                    AttachmentScanHitKindWire::CredentialPattern,
                    "high-entropy-assignment",
                    line_no,
                    bytes_scanned,
                );
            }
        }
        if well_known_key_count(line) >= 5
            && (line.contains('{') || line.contains('['))
        {
            return AttachmentScanWire::hit(
                AttachmentScanHitKindWire::EnvDump,
                "structured-env-dump",
                line_no,
                bytes_scanned,
            );
        }
        let is_assign = env_assign_re().is_match(line.trim_start());
        assign_window.push(is_assign);
        if assign_window.len() > 10 {
            assign_window.remove(0);
        }
        if assign_window.len() == 10
            && assign_window.iter().filter(|hit| **hit).count() >= 5
        {
            return AttachmentScanWire::hit(
                AttachmentScanHitKindWire::EnvDump,
                "env-dump-window",
                line_no,
                bytes_scanned,
            );
        }
    }
    // Trailing short windows (fewer than ten lines total).
    if assign_window.len() < 10
        && assign_window.iter().filter(|hit| **hit).count() >= 5
    {
        return AttachmentScanWire::hit(
            AttachmentScanHitKindWire::EnvDump,
            "env-dump-window",
            text.lines().count() as u64,
            bytes_scanned,
        );
    }

    AttachmentScanWire::clean(bytes_scanned)
}
