//! Measurable sealed-archive triggers for `sase bead doctor`.
//!
//! The physical sealed archive is not built: it waits behind measured
//! triggers (`seal-watch`). This module computes the three measurable
//! triggers — hot stream file count, full stat sweep cost, and store
//! working-tree size — against core threshold constants, so `bead doctor`
//! can report OK or WARN with a pointer to the gated sealed-segment
//! design in `docs/beads.md`. Computation never fails: an unusable store
//! is a report, not an error.

use std::fs;
use std::path::Path;
use std::time::Instant;

use serde::{Deserialize, Serialize};

use super::jsonl::event_store_present;
use super::read_model::sweep_store_signatures;

/// Wire schema version for [`BeadSealWatchWire`].
pub const BEAD_SEAL_WATCH_WIRE_SCHEMA_VERSION: u32 = 1;

/// Warn when hot (`events/streams/`) stream files exceed this count.
pub const SEAL_WATCH_HOT_STREAM_FILES_WARN: u64 = 10_000;
/// Warn when a full stat sweep takes longer than this many milliseconds.
///
/// The probe times the same [`sweep_store_signatures`] pass the read model
/// runs whenever its O(1) freshness token changes.
pub const SEAL_WATCH_SWEEP_MS_WARN: u64 = 50;
/// Warn when the store working tree exceeds this many bytes (~250 MiB).
pub const SEAL_WATCH_TREE_BYTES_WARN: u64 = 250 * 1024 * 1024;

/// One measured sealed-archive trigger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadSealWatchTriggerWire {
    /// Trigger key: `hot_stream_files`, `stat_sweep_ms`, or `store_tree_bytes`.
    pub name: String,
    /// Measured value, in [`BeadSealWatchTriggerWire::unit`] units.
    pub value: u64,
    /// Warn threshold in the same units (a core `SEAL_WATCH_*` constant).
    pub threshold: u64,
    /// Unit label for rendering: `files`, `ms`, or `bytes`.
    pub unit: String,
    /// True when `value` exceeds `threshold`.
    pub warn: bool,
    /// Human-readable measurement, e.g. `"2,108 hot stream files"`.
    pub detail: String,
}

/// Sealed-archive watch report for `sase bead doctor`. Never fails.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadSealWatchWire {
    /// Schema version of this wire type.
    pub schema_version: u32,
    /// False when the store has no event store to watch (legacy or empty).
    pub available: bool,
    /// Why the watch is or is not reporting measurements.
    pub reason: String,
    /// One entry per measurable trigger; empty when unavailable.
    pub triggers: Vec<BeadSealWatchTriggerWire>,
    /// True when any trigger warns.
    pub warn: bool,
}

/// Classify raw measurements against the `SEAL_WATCH_*` thresholds.
///
/// Returns `(hot_stream_files_warn, stat_sweep_warn, store_tree_warn)`.
/// Each warns only when its value strictly exceeds its threshold, so a
/// measurement exactly at the threshold still reports OK.
pub fn classify_seal_watch_triggers(
    stream_files: u64,
    sweep_ms: u64,
    tree_bytes: u64,
) -> (bool, bool, bool) {
    (
        stream_files > SEAL_WATCH_HOT_STREAM_FILES_WARN,
        sweep_ms > SEAL_WATCH_SWEEP_MS_WARN,
        tree_bytes > SEAL_WATCH_TREE_BYTES_WARN,
    )
}

/// Compute the sealed-archive trigger report for the store at `beads_dir`.
///
/// The sweep measurement times the same full stat pass the read model
/// runs on a changed freshness token. Never fails: a missing event store
/// or an unreadable sweep/tree reports unavailable with a reason.
pub fn bead_seal_watch_triggers(beads_dir: &Path) -> BeadSealWatchWire {
    if !event_store_present(beads_dir) {
        return BeadSealWatchWire {
            schema_version: BEAD_SEAL_WATCH_WIRE_SCHEMA_VERSION,
            available: false,
            reason:
                "no event store: plain replay serves the read; nothing to seal"
                    .to_string(),
            triggers: vec![],
            warn: false,
        };
    }
    let started = Instant::now();
    let sweep = match sweep_store_signatures(beads_dir) {
        Ok(sweep) => sweep,
        Err(error) => {
            return BeadSealWatchWire {
                schema_version: BEAD_SEAL_WATCH_WIRE_SCHEMA_VERSION,
                available: false,
                reason: format!("full stat sweep unreadable: {error}"),
                triggers: vec![],
                warn: false,
            };
        }
    };
    let sweep_ms =
        u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    let stream_files = sweep.streams.len() as u64;
    let tree_bytes = match store_tree_bytes(beads_dir) {
        Ok(bytes) => bytes,
        Err(error) => {
            return BeadSealWatchWire {
                schema_version: BEAD_SEAL_WATCH_WIRE_SCHEMA_VERSION,
                available: false,
                reason: format!("store tree unreadable: {error}"),
                triggers: vec![],
                warn: false,
            };
        }
    };
    let (hot_warn, sweep_warn, tree_warn) =
        classify_seal_watch_triggers(stream_files, sweep_ms, tree_bytes);
    let triggers = vec![
        BeadSealWatchTriggerWire {
            name: "hot_stream_files".to_string(),
            value: stream_files,
            threshold: SEAL_WATCH_HOT_STREAM_FILES_WARN,
            unit: "files".to_string(),
            warn: hot_warn,
            detail: format!(
                "{} hot stream files",
                format_thousands(stream_files)
            ),
        },
        BeadSealWatchTriggerWire {
            name: "stat_sweep_ms".to_string(),
            value: sweep_ms,
            threshold: SEAL_WATCH_SWEEP_MS_WARN,
            unit: "ms".to_string(),
            warn: sweep_warn,
            detail: format!("full stat sweep {sweep_ms} ms"),
        },
        BeadSealWatchTriggerWire {
            name: "store_tree_bytes".to_string(),
            value: tree_bytes,
            threshold: SEAL_WATCH_TREE_BYTES_WARN,
            unit: "bytes".to_string(),
            warn: tree_warn,
            detail: format!("store working tree {}", format_mib(tree_bytes)),
        },
    ];
    let warn = hot_warn || sweep_warn || tree_warn;
    BeadSealWatchWire {
        schema_version: BEAD_SEAL_WATCH_WIRE_SCHEMA_VERSION,
        available: true,
        reason: if warn {
            "at least one sealed-archive trigger fires".to_string()
        } else {
            "no sealed-archive trigger fires".to_string()
        },
        triggers,
        warn,
    }
}

/// Sum the working-tree bytes under `beads_dir`, following no symlinks.
///
/// Only regular files count; symlinks, sockets, and other specials are
/// skipped so a linked checkout cannot double-count or cycle. An
/// unreadable entry is an error so the caller reports unavailable rather
/// than a partial size.
fn store_tree_bytes(beads_dir: &Path) -> Result<u64, String> {
    let mut total: u64 = 0;
    let mut stack = vec![beads_dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let read = fs::read_dir(&dir)
            .map_err(|error| format!("{}: {error}", dir.display()))?;
        for entry in read {
            let entry =
                entry.map_err(|error| format!("{}: {error}", dir.display()))?;
            let file_type = entry.file_type().map_err(|error| {
                format!("{}: {error}", entry.path().display())
            })?;
            if file_type.is_dir() {
                stack.push(entry.path());
            } else if file_type.is_file() {
                let size = entry
                    .metadata()
                    .map_err(|error| {
                        format!("{}: {error}", entry.path().display())
                    })?
                    .len();
                total = total.saturating_add(size);
            }
        }
    }
    Ok(total)
}

/// Format a count with thousands separators (`2108` -> `"2,108"`).
fn format_thousands(value: u64) -> String {
    let digits: Vec<char> = value.to_string().chars().collect();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, digit) in digits.iter().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            out.push(',');
        }
        out.push(*digit);
    }
    out
}

/// Format a byte count as whole MiB (`36700160` -> `"35 MiB"`).
fn format_mib(bytes: u64) -> String {
    format!("{} MiB", bytes / (1024 * 1024))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn thresholds_warn_only_above_the_line() {
        assert_eq!(
            classify_seal_watch_triggers(
                SEAL_WATCH_HOT_STREAM_FILES_WARN - 1,
                SEAL_WATCH_SWEEP_MS_WARN - 1,
                SEAL_WATCH_TREE_BYTES_WARN - 1,
            ),
            (false, false, false)
        );
        assert_eq!(
            classify_seal_watch_triggers(
                SEAL_WATCH_HOT_STREAM_FILES_WARN,
                SEAL_WATCH_SWEEP_MS_WARN,
                SEAL_WATCH_TREE_BYTES_WARN,
            ),
            (false, false, false)
        );
        assert_eq!(
            classify_seal_watch_triggers(
                SEAL_WATCH_HOT_STREAM_FILES_WARN + 1,
                SEAL_WATCH_SWEEP_MS_WARN,
                SEAL_WATCH_TREE_BYTES_WARN,
            ),
            (true, false, false)
        );
        assert_eq!(
            classify_seal_watch_triggers(
                SEAL_WATCH_HOT_STREAM_FILES_WARN,
                SEAL_WATCH_SWEEP_MS_WARN + 1,
                SEAL_WATCH_TREE_BYTES_WARN,
            ),
            (false, true, false)
        );
        assert_eq!(
            classify_seal_watch_triggers(
                SEAL_WATCH_HOT_STREAM_FILES_WARN,
                SEAL_WATCH_SWEEP_MS_WARN,
                SEAL_WATCH_TREE_BYTES_WARN + 1,
            ),
            (false, false, true)
        );
    }

    #[test]
    fn threshold_constants_match_the_plan() {
        assert_eq!(SEAL_WATCH_HOT_STREAM_FILES_WARN, 10_000);
        assert_eq!(SEAL_WATCH_SWEEP_MS_WARN, 50);
        assert_eq!(SEAL_WATCH_TREE_BYTES_WARN, 250 * 1024 * 1024);
    }

    fn seed_event_store(beads_dir: &Path, streams: usize) {
        use crate::bead::events::import_issues_to_event_streams;
        use crate::bead::jsonl::{parse_issues_jsonl, write_event_store};
        fs::write(beads_dir.join("config.json"), "{}\n").unwrap();
        let jsonl = (0..streams.max(1))
            .map(|index| {
                format!(
                    "{{\"id\":\"beads-{index}\",\"title\":\"Bead {index}\",\"status\":\"open\",\"issue_type\":\"task\",\"created_at\":\"2026-01-01T00:00:00Z\"}}\n"
                )
            })
            .collect::<String>();
        let outcome = parse_issues_jsonl(&jsonl);
        assert_eq!(outcome.loaded_rows, streams.max(1));
        let event_streams =
            import_issues_to_event_streams(&outcome.issues).unwrap();
        assert_eq!(event_streams.len(), streams.max(1));
        write_event_store(beads_dir, &event_streams).unwrap();
    }

    #[test]
    fn probe_measures_a_small_store_as_ok() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("beads");
        fs::create_dir_all(&beads_dir).unwrap();
        seed_event_store(&beads_dir, 3);

        let report = bead_seal_watch_triggers(&beads_dir);
        assert!(report.available);
        assert!(!report.warn);
        assert_eq!(report.triggers.len(), 3);
        let on_disk = fs::read_dir(beads_dir.join("events").join("streams"))
            .unwrap()
            .count() as u64;
        let hot = &report.triggers[0];
        assert_eq!(hot.name, "hot_stream_files");
        assert_eq!(hot.value, on_disk);
        assert_eq!(hot.threshold, SEAL_WATCH_HOT_STREAM_FILES_WARN);
        assert!(!hot.warn);
        let tree = &report.triggers[2];
        assert_eq!(tree.name, "store_tree_bytes");
        assert!(tree.value > 0);
        assert!(tree.value < SEAL_WATCH_TREE_BYTES_WARN);
    }

    #[test]
    fn probe_counts_nested_tree_bytes() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("beads");
        fs::create_dir_all(&beads_dir).unwrap();
        seed_event_store(&beads_dir, 1);
        let nested = beads_dir.join("events").join("sealed");
        fs::create_dir_all(&nested).unwrap();
        fs::write(nested.join("payload.bin"), vec![7u8; 4096]).unwrap();

        let report = bead_seal_watch_triggers(&beads_dir);
        assert!(report.available);
        let tree = &report.triggers[2];
        assert!(tree.value >= 4096);
    }

    #[test]
    fn probe_is_unavailable_without_an_event_store() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("beads");
        fs::create_dir_all(&beads_dir).unwrap();
        fs::write(beads_dir.join("issues.jsonl"), "{}\n").unwrap();

        let report = bead_seal_watch_triggers(&beads_dir);
        assert!(!report.available);
        assert!(!report.warn);
        assert!(report.triggers.is_empty());
    }

    #[test]
    fn probe_is_unavailable_for_a_missing_store() {
        let temp = tempdir().unwrap();
        let report = bead_seal_watch_triggers(&temp.path().join("missing"));
        assert!(!report.available);
        assert!(report.triggers.is_empty());
    }
}
