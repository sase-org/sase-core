//! Agent-completion ack and lean unread completion index.
//!
//! Both calls run under the store lock with the file lock held across the
//! row read and the generation sample, so an observed generation always
//! describes the returned rows: no ack can be resurrected by an older
//! observation.

use std::path::Path;

use fs2::FileExt;

use super::generation::{generation_for_missing_store, read_generation};
use super::store::{
    compact_notifications_for_index_unlocked,
    matches_agent_completion_notification,
    matches_agent_completion_notification_for_agents,
    matches_agent_settlement_notification,
    matches_agent_settlement_notification_for_agents,
    merge_and_rewrite_notifications_unlocked, open_lock_file,
    read_rows_unlocked, unlock,
};
use super::wire::{
    NotificationAckOutcomeWire, NotificationAckRequestWire,
    NotificationAgentKeyWire, UnreadCompletionIndexRowWire,
    UnreadCompletionIndexWire, NOTIFICATION_STORE_WIRE_SCHEMA_VERSION,
};

/// Dismiss completion and settlement rows for `request.agents`.
///
/// Mirrors the `DismissAgentCompletionsMatchingAgents` state-update arm:
/// every live row that is not already dismissed and matches either the
/// completion or the settlement agent matcher is stored with
/// `dismissed = true` and `snooze_until = None`. Already-dismissed rows
/// are unchanged and are not returned. Ids are collected in file order.
///
/// An empty id list (including an empty `agents` request) writes nothing
/// and returns the current generation. Otherwise the rows persist through
/// the merge/compaction rewrite, whose internal bump is the only bump,
/// and the returned generation is sampled after that write.
pub fn ack_agent_completions(
    path: &Path,
    request: &NotificationAckRequestWire,
) -> Result<NotificationAckOutcomeWire, String> {
    let lock = open_lock_file(path)?;
    lock.lock_exclusive().map_err(|e| e.to_string())?;
    let result = ack_agent_completions_unlocked(path, request);
    unlock(lock)?;
    result
}

fn ack_agent_completions_unlocked(
    path: &Path,
    request: &NotificationAckRequestWire,
) -> Result<NotificationAckOutcomeWire, String> {
    let store_exists = path.exists();
    let (mut rows, _) = read_rows_unlocked(path, true)?;
    let mut dismissed_ids = Vec::new();
    for row in &mut rows {
        if row.dismissed {
            continue;
        }
        if matches_agent_completion_notification_for_agents(
            row,
            &request.agents,
        ) || matches_agent_settlement_notification_for_agents(
            row,
            &request.agents,
        ) {
            row.dismissed = true;
            row.snooze_until = None;
            dismissed_ids.push(row.id.clone());
        }
    }
    if dismissed_ids.is_empty() {
        return Ok(NotificationAckOutcomeWire {
            schema_version: NOTIFICATION_STORE_WIRE_SCHEMA_VERSION,
            dismissed_ids,
            generation: current_generation(path, store_exists)?,
        });
    }
    merge_and_rewrite_notifications_unlocked(path, &rows)?;
    Ok(NotificationAckOutcomeWire {
        schema_version: NOTIFICATION_STORE_WIRE_SCHEMA_VERSION,
        dismissed_ids,
        generation: read_generation(path)?,
    })
}

/// Read the lean unread completion index and its store generation.
///
/// One row per live completion row and one per live settlement row, in
/// file order, dismissed rows included. Pending-gate rows and every other
/// sender stay out. Silent rows are not dropped, the archive file is not
/// read, and snoozes are not expired on this read. Compaction runs under
/// the same conditions the snapshot reader uses, and the returned
/// generation is the post-compaction value.
pub fn read_unread_completion_index(
    path: &Path,
) -> Result<UnreadCompletionIndexWire, String> {
    if !path.exists() {
        return Ok(UnreadCompletionIndexWire {
            schema_version: NOTIFICATION_STORE_WIRE_SCHEMA_VERSION,
            generation: generation_for_missing_store(path),
            rows: Vec::new(),
        });
    }
    let lock = open_lock_file(path)?;
    lock.lock_exclusive().map_err(|e| e.to_string())?;
    let result = read_unread_completion_index_unlocked(path);
    unlock(lock)?;
    result
}

fn read_unread_completion_index_unlocked(
    path: &Path,
) -> Result<UnreadCompletionIndexWire, String> {
    // Runs the snapshot reader's compaction when it would run there, so a
    // large store compacts on this read too. The rewrite bumps; the
    // generation sampled below is the post-compaction value.
    let rows = compact_notifications_for_index_unlocked(path)?;
    let generation = read_generation(path)?;
    Ok(UnreadCompletionIndexWire {
        schema_version: NOTIFICATION_STORE_WIRE_SCHEMA_VERSION,
        generation,
        rows: rows
            .iter()
            .filter_map(unread_index_row_for_notification)
            .collect(),
    })
}

fn unread_index_row_for_notification(
    notification: &super::wire::NotificationWire,
) -> Option<UnreadCompletionIndexRowWire> {
    if matches_agent_completion_notification(notification) {
        let cl_name = notification.action_data.get("cl_name")?;
        if cl_name.is_empty() {
            return None;
        }
        let raw_suffix = notification
            .action_data
            .get("raw_suffix")
            .filter(|suffix| !suffix.is_empty())
            .cloned();
        return Some(UnreadCompletionIndexRowWire {
            id: notification.id.clone(),
            agent: NotificationAgentKeyWire {
                cl_name: cl_name.clone(),
                raw_suffix,
            },
            read: notification.read,
            dismissed: notification.dismissed,
        });
    }
    if matches_agent_settlement_notification(notification) {
        let cl_name = notification.action_data.get("cl_name")?.clone();
        let raw_suffix = notification.action_data.get("raw_suffix")?.clone();
        if cl_name.is_empty() || raw_suffix.is_empty() {
            return None;
        }
        return Some(UnreadCompletionIndexRowWire {
            id: notification.id.clone(),
            agent: NotificationAgentKeyWire {
                cl_name,
                raw_suffix: Some(raw_suffix),
            },
            read: notification.read,
            dismissed: notification.dismissed,
        });
    }
    None
}

/// Current generation for a no-write ack: strict when the store exists
/// (a corrupt sibling is an error), lenient when it does not.
fn current_generation(path: &Path, store_exists: bool) -> Result<u64, String> {
    if store_exists {
        read_generation(path)
    } else {
        Ok(generation_for_missing_store(path))
    }
}
