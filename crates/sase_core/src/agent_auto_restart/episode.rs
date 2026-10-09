//! Update-episode identity.
//!
//! The episode id is the culprit commit from W3 when known (grouping
//! every death one update caused, including agents that woke after a
//! later install). Otherwise it is a stable digest of the target
//! revision set.

use super::wire::{
    AutoRestartEpisodeWire, AutoRestartWitnessesWire,
    AGENT_AUTO_RESTART_WIRE_SCHEMA_VERSION,
};

/// Derive the episode identity for one witness bundle.
pub fn derive_auto_restart_episode(
    witnesses: &AutoRestartWitnessesWire,
) -> AutoRestartEpisodeWire {
    if let Some(proof) = witnesses.file_proof.as_ref() {
        if let Some(culprit) = proof.culprit_commit.as_deref() {
            let culprit = culprit.trim();
            if !culprit.is_empty() {
                let short = short_sha(culprit);
                let (from_rev, to_rev) = revision_range(witnesses);
                let label = match proof.culprit_subject.as_deref() {
                    Some(subject) if !subject.trim().is_empty() => {
                        format!("sase update {short} (\"{}\")", subject.trim())
                    }
                    _ => format!("sase update {short}"),
                };
                return AutoRestartEpisodeWire {
                    schema_version: AGENT_AUTO_RESTART_WIRE_SCHEMA_VERSION,
                    id: format!("sase@{short}"),
                    slug: format!("sase-{short}"),
                    culprit_short: Some(short),
                    from_rev,
                    to_rev,
                    label,
                };
            }
        }
    }
    let (from_rev, to_rev) = revision_range(witnesses);
    match (&from_rev, &to_rev) {
        (Some(from), Some(to)) => {
            let digest = stable_digest(&[from.as_str(), to.as_str()]);
            AutoRestartEpisodeWire {
                schema_version: AGENT_AUTO_RESTART_WIRE_SCHEMA_VERSION,
                id: format!("sase@{from}-{to}"),
                slug: format!("sase-{digest}"),
                culprit_short: None,
                from_rev: Some(from.clone()),
                to_rev: Some(to.clone()),
                label: format!("sase update {from} → {to}"),
            }
        }
        _ => AutoRestartEpisodeWire {
            schema_version: AGENT_AUTO_RESTART_WIRE_SCHEMA_VERSION,
            id: String::new(),
            slug: "sase-unknown".to_string(),
            culprit_short: None,
            from_rev,
            to_rev,
            label: "sase update (unknown revision)".to_string(),
        },
    }
}

fn revision_range(
    witnesses: &AutoRestartWitnessesWire,
) -> (Option<String>, Option<String>) {
    match witnesses.refresh_log_line.as_ref() {
        Some(line) if !line.from.is_empty() || !line.to.is_empty() => {
            (short_opt(&line.from), short_opt(&line.to))
        }
        _ => (None, None),
    }
}

fn short_opt(rev: &str) -> Option<String> {
    let rev = rev.trim();
    if rev.is_empty() {
        None
    } else {
        Some(short_sha(rev))
    }
}

fn short_sha(rev: &str) -> String {
    rev.chars().take(7).collect()
}

/// Small stable hex digest over short strings (FNV-1a, 64-bit).
fn stable_digest(parts: &[&str]) -> String {
    let mut hash: u64 = 0xcbf29ce484222325;
    for part in parts {
        for byte in part.bytes() {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}
