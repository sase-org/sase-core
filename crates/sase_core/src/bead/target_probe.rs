//! Filesystem-only bead-target ownership probe.
//!
//! Routing a CLI target to its owning store must not cost a full store read:
//! the mutation's own locked resolution stays the authority for existence
//! and ambiguity. This probe answers only ownership — which store's stream
//! files could hold the target — with a handful of stat calls:
//!
//! * a root ID owns ``events/streams/<id>.jsonl``;
//! * a phase lives in its parent plan's stream, so dotted IDs walk their
//!   lineage prefixes (``a.b.c`` probes ``a.b.c``, ``a.b``, then ``a``);
//! * the target's top-level prefix must match the store's configured
//!   ``issue_prefix``;
//! * a stream file left behind by ``sase bead rm`` (a tombstoned stem) or by
//!   an event-level relocation still routes to its store: the file exists,
//!   and the later operation reports the authoritative answer.
//!
//! Stores without an event layout (legacy ``issues.jsonl`` stores) report
//! ``unknown`` so callers fall back to the ID-list path for them.

use std::path::Path;

use serde::{Deserialize, Serialize};

use super::config::load_config;
use super::jsonl::{event_store_present, event_streams_dir};
use super::wire::BeadError;

/// Ownership verdict for one probe call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BeadTargetProbeStatusWire {
    /// A lineage stem file exists in this store.
    Hit,
    /// No lineage stem file exists here, or the prefix does not match.
    Miss,
    /// The store has no event layout to probe; fall back to ID lists.
    Unknown,
}

/// Ownership probe result for one ``(store, target)`` pair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadTargetProbeOutcomeWire {
    pub status: BeadTargetProbeStatusWire,
    /// The stem that decided a hit, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stem: Option<String>,
}

/// Probe whether *beads_dir* could own *target* without reading the store.
///
/// Never fails for a missing or unreadable store: those report
/// [`BeadTargetProbeStatusWire::Unknown`] (missing layout) or
/// [`BeadTargetProbeStatusWire::Miss`] (no stem), never an error, so
/// routing treats an unreadable candidate exactly as before.
pub fn probe_bead_target_owner(
    beads_dir: &Path,
    target: &str,
) -> Result<BeadTargetProbeOutcomeWire, BeadError> {
    if !beads_dir.is_dir() {
        return Ok(BeadTargetProbeOutcomeWire {
            status: BeadTargetProbeStatusWire::Unknown,
            stem: None,
        });
    }
    if !event_store_present(beads_dir) {
        return Ok(BeadTargetProbeOutcomeWire {
            status: BeadTargetProbeStatusWire::Unknown,
            stem: None,
        });
    }
    let Some(top_level) =
        target.split('.').next().filter(|top| !top.is_empty())
    else {
        return Ok(BeadTargetProbeOutcomeWire {
            status: BeadTargetProbeStatusWire::Miss,
            stem: None,
        });
    };
    // Shorthand carries no store prefix and no lineage stem: it never
    // routes away from the local store, so it never probes a hit here.
    if !target.contains('-') {
        return Ok(BeadTargetProbeOutcomeWire {
            status: BeadTargetProbeStatusWire::Miss,
            stem: None,
        });
    }
    let prefix = top_level
        .rsplit_once('-')
        .map(|(head, _)| head)
        .unwrap_or("");
    let config = load_config(beads_dir, super::config::default_config("", ""))?;
    if !prefix.is_empty()
        && !config.issue_prefix.is_empty()
        && prefix != config.issue_prefix
    {
        return Ok(BeadTargetProbeOutcomeWire {
            status: BeadTargetProbeStatusWire::Miss,
            stem: None,
        });
    }
    let streams_dir = event_streams_dir(beads_dir);
    let mut stem = target.to_string();
    loop {
        if streams_dir.join(format!("{stem}.jsonl")).is_file() {
            return Ok(BeadTargetProbeOutcomeWire {
                status: BeadTargetProbeStatusWire::Hit,
                stem: Some(stem),
            });
        }
        let Some(parent) = stem.rsplit_once('.').map(|(head, _)| head) else {
            break;
        };
        stem = parent.to_string();
    }
    Ok(BeadTargetProbeOutcomeWire {
        status: BeadTargetProbeStatusWire::Miss,
        stem: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    fn probe_store(prefix: &str, stems: &[&str]) -> tempfile::TempDir {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd").join("beads");
        fs::create_dir_all(beads_dir.join("events").join("streams")).unwrap();
        fs::write(
            beads_dir.join("config.json"),
            format!(
                "{{\"issue_prefix\":\"{prefix}\",\"next_counter\":42,\
                 \"owner\":\"owner@example.com\"}}"
            ),
        )
        .unwrap();
        for stem in stems {
            fs::write(
                beads_dir
                    .join("events")
                    .join("streams")
                    .join(format!("{stem}.jsonl")),
                "{\"operation\":\"issue_created\"}\n",
            )
            .unwrap();
        }
        temp
    }

    fn beads_dir(temp: &tempfile::TempDir) -> std::path::PathBuf {
        temp.path().join("sdd").join("beads")
    }

    #[test]
    fn root_id_hits_its_stream_file() {
        let temp = probe_store("beads", &["beads-1"]);
        let outcome =
            probe_bead_target_owner(&beads_dir(&temp), "beads-1").unwrap();
        assert_eq!(outcome.status, BeadTargetProbeStatusWire::Hit);
        assert_eq!(outcome.stem.as_deref(), Some("beads-1"));
    }

    #[test]
    fn phase_id_hits_through_its_parent_stem() {
        let temp = probe_store("beads", &["beads-1"]);
        let outcome =
            probe_bead_target_owner(&beads_dir(&temp), "beads-1.2").unwrap();
        assert_eq!(outcome.status, BeadTargetProbeStatusWire::Hit);
        assert_eq!(outcome.stem.as_deref(), Some("beads-1"));
    }

    #[test]
    fn missing_stem_misses() {
        let temp = probe_store("beads", &["beads-1"]);
        let outcome =
            probe_bead_target_owner(&beads_dir(&temp), "beads-9").unwrap();
        assert_eq!(outcome.status, BeadTargetProbeStatusWire::Miss);
        assert_eq!(outcome.stem, None);
    }

    #[test]
    fn foreign_prefix_misses_without_reading_streams() {
        let temp = probe_store("beads", &["other-1"]);
        let outcome =
            probe_bead_target_owner(&beads_dir(&temp), "other-1").unwrap();
        assert_eq!(outcome.status, BeadTargetProbeStatusWire::Miss);
    }

    #[test]
    fn shorthand_never_probes_a_hit() {
        let temp = probe_store("beads", &["beads-1"]);
        let outcome = probe_bead_target_owner(&beads_dir(&temp), "1").unwrap();
        assert_eq!(outcome.status, BeadTargetProbeStatusWire::Miss);
    }

    #[test]
    fn tombstoned_stem_still_routes_to_its_store() {
        // A removed bead's stream file lingers until the next locked
        // prune; the probe reports ownership and the later operation
        // reports the authoritative not-found.
        let temp = probe_store("beads", &["beads-7"]);
        let outcome =
            probe_bead_target_owner(&beads_dir(&temp), "beads-7").unwrap();
        assert_eq!(outcome.status, BeadTargetProbeStatusWire::Hit);
    }

    #[test]
    fn legacy_store_is_unknown() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("beads");
        fs::create_dir_all(&beads_dir).unwrap();
        fs::write(beads_dir.join("issues.jsonl"), "[]\n").unwrap();
        let outcome = probe_bead_target_owner(&beads_dir, "beads-1").unwrap();
        assert_eq!(outcome.status, BeadTargetProbeStatusWire::Unknown);
    }

    #[test]
    fn missing_store_is_unknown() {
        let temp = tempdir().unwrap();
        let outcome =
            probe_bead_target_owner(&temp.path().join("nope"), "beads-1")
                .unwrap();
        assert_eq!(outcome.status, BeadTargetProbeStatusWire::Unknown);
    }
}
