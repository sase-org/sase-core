//! First-seen-order grouping of error entries by cause.
//!
//! Entries arrive keyed by `(routine, job, signature)`. Each group carries
//! the exact entry count, the observed first and last timestamps, and one
//! bounded sample (the first-seen entry's sample text).

use std::collections::HashMap;

use super::wire::{
    AxeErrorGroupRequestWire, AxeErrorGroupResponseWire, AxeErrorGroupWire,
    AxeErrorWireError, AXE_ERROR_MAX_SAMPLE_CHARS,
    AXE_ERROR_WIRE_SCHEMA_VERSION,
};

struct GroupAccumulator {
    routine: String,
    job: String,
    signature: String,
    count: u64,
    first_ts: String,
    last_ts: String,
    sample: String,
}

/// Fold entries into per-cause groups in first-seen group order.
///
/// This operation performs no filesystem, clock, or process I/O.
pub fn group_axe_errors(
    request: &AxeErrorGroupRequestWire,
) -> Result<AxeErrorGroupResponseWire, AxeErrorWireError> {
    if request.schema_version != AXE_ERROR_WIRE_SCHEMA_VERSION {
        return Err(AxeErrorWireError::new(
            "schema_version_mismatch",
            "$.schema_version",
            format!(
                "got {}, expected {AXE_ERROR_WIRE_SCHEMA_VERSION}",
                request.schema_version
            ),
        ));
    }
    let mut order: Vec<(String, String, String)> = Vec::new();
    let mut groups: HashMap<(String, String, String), GroupAccumulator> =
        HashMap::new();
    for entry in &request.entries {
        let key = (
            entry.routine.clone(),
            entry.job.clone(),
            entry.signature.clone(),
        );
        if let Some(group) = groups.get_mut(&key) {
            group.count += 1;
            if entry.timestamp < group.first_ts {
                group.first_ts.clone_from(&entry.timestamp);
            }
            if entry.timestamp > group.last_ts {
                group.last_ts.clone_from(&entry.timestamp);
            }
        } else {
            order.push(key.clone());
            groups.insert(
                key,
                GroupAccumulator {
                    routine: entry.routine.clone(),
                    job: entry.job.clone(),
                    signature: entry.signature.clone(),
                    count: 1,
                    first_ts: entry.timestamp.clone(),
                    last_ts: entry.timestamp.clone(),
                    sample: truncate_chars(
                        &entry.sample,
                        AXE_ERROR_MAX_SAMPLE_CHARS,
                    ),
                },
            );
        }
    }
    let groups = order
        .into_iter()
        .map(|key| {
            let group = groups
                .remove(&key)
                .expect("group order only holds inserted keys");
            AxeErrorGroupWire {
                routine: group.routine,
                job: group.job,
                signature: group.signature,
                count: group.count,
                first_ts: group.first_ts,
                last_ts: group.last_ts,
                sample: group.sample,
            }
        })
        .collect();
    Ok(AxeErrorGroupResponseWire {
        schema_version: AXE_ERROR_WIRE_SCHEMA_VERSION,
        groups,
    })
}

fn truncate_chars(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    text.chars().take(max_chars).collect()
}
