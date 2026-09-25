//! Actor-keyed touch index over bead event streams.
//!
//! A **touch** is one `(actor, bead)` pair with the verbs the actor performed
//! on that bead. The per-bead event streams already record every mutation
//! with its actor, so the index is a pure reduction of those streams, with
//! one write-time exception: every `issue_closed` event carries the acting
//! closer in its `closed_by` payload field alongside the envelope actor. The
//! index is a derived cache that is always safe to delete and rebuild.
//!
//! Three entry points, all deliberately separate:
//!
//! - [`reduce_stream_touches`] is the pure function from one stream's lines
//!   to that stream's touch rows.
//! - [`refresh_bead_touch_index`] stats every `events/streams/*.jsonl`,
//!   re-reduces only the streams whose `(mtime_ns, size)` signature changed,
//!   and rewrites the index atomically under an exclusive lock.
//! - [`query_bead_touches`] loads the index and filters it by actor. It never
//!   scans the streams directory and never parses a stream, so a stale index
//!   returns stale answers instead of charging the caller for freshness.
//!
//! [`bead_touch_index_status`] is the stat-only companion that classifies an
//! index as missing, unreadable, schema-mismatched, stale, or fresh.
//!
//! Resilience: malformed lines, unknown operations, mismatched payload
//! shapes, and streams that do not belong to their filename are skipped, never
//! fatal, and a missing, truncated, or wrong-schema index is a cache miss.
//! [`BeadError`] is reserved for genuine failures (an unreadable stream, a
//! lock timeout, a failed write).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tempfile::NamedTempFile;

use crate::agent_identity::validate_agent_name;
use crate::store_lock::{
    acquire_store_lock, holder_path_for, timeout_from_env, LockMode,
    StoreLockError,
};

use super::events::BeadEventOperationWire;
use super::jsonl::event_streams_dir;
use super::wire::{BeadError, BeadResolutionWire, IssueTypeWire, StatusWire};

/// Schema version of both the index file and every wire type in this module.
///
/// Bumping it makes every existing index a cache miss: the next refresh
/// rebuilds from the streams and the query returns nothing until then.
pub const BEAD_TOUCH_INDEX_WIRE_SCHEMA_VERSION: u32 = 3;

/// Bound on the note text stored in a [`BeadNotePreviewWire`], in Unicode
/// scalar values (`char`s). One pathological note cannot grow the shared
/// index without limit; longer text is cut at this prefix and marked with
/// `truncated`. The TUI applies its separate three-display-line limit.
pub const NOTE_PREVIEW_TEXT_LIMIT: usize = 1024;

const LOCK_TIMEOUT_ENV: &str = "SASE_BEAD_TOUCH_INDEX_LOCK_TIMEOUT";
const LOCK_TIMEOUT_DEFAULT: Duration = Duration::from_secs(2);

/// Preview of the newest surviving structured note one agent authored on
/// one bead.
///
/// Derived only from `note_appended` / `note_edited` / `note_removed`
/// payloads replayed by stable note ID. The note keeps its original author
/// and append timestamp even when another actor edits it later; `text` is
/// the current text cut to [`NOTE_PREVIEW_TEXT_LIMIT`]. Legacy free-text
/// note blobs without per-note attribution never produce a preview.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadNotePreviewWire {
    /// Stable note ID: the `event_id` of the `note_appended` event.
    pub id: String,
    /// Original author (trimmed event actor at append time).
    pub author: String,
    /// Original append timestamp (RFC 3339 Z display form).
    pub timestamp: String,
    /// Current text, cut to [`NOTE_PREVIEW_TEXT_LIMIT`] scalar values.
    pub text: String,
    /// Edit instant (RFC 3339 Z display form), when edited.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edited_at: Option<String>,
    /// Trimmed actor of the last edit, when edited.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edited_by: Option<String>,
    /// True when `text` was shortened for the cache.
    #[serde(default)]
    pub truncated: bool,
}

/// One `(actor, bead)` pair with aggregated verbs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadTouchWire {
    /// Trimmed actor string as the stream recorded it (globalized or bare).
    pub actor: String,
    pub bead_id: String,
    /// Bead title at the newest stream event that carried one, else `""`.
    pub title: String,
    /// `plan`, `phase`, or `task`, else `""`.
    pub issue_type: String,
    /// Last-known status from the reduced events, else `""`.
    pub status: String,
    /// Verb -> positive count. See [`verb_for_operation`] for the vocabulary.
    pub verbs: BTreeMap<String, u64>,
    /// Earliest contributing event timestamp (RFC 3339 Z).
    pub first_at: String,
    /// Newest contributing event timestamp (RFC 3339 Z).
    pub last_at: String,
    /// Number of surviving structured notes this actor authored on this
    /// bead. Retracted notes vanish; edits keep the count.
    #[serde(default)]
    pub current_note_count: u64,
    /// Newest surviving structured note this actor authored on this bead,
    /// by append timestamp with deterministic ID tie-break. `None` when the
    /// actor has no surviving notes here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note_preview: Option<BeadNotePreviewWire>,
    /// This actor's latest credited close for this bead, in stream order.
    /// `None` when the actor has no credited close here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub close: Option<BeadTouchCloseWire>,
    /// Stream file that produced this row. Cache bookkeeping: a refresh
    /// reuses the rows of an unchanged stream verbatim. Consumers ignore it.
    pub stream_id: String,
}

/// One agent's close of one bead: when it happened, how it resolved, why,
/// and whether it still stands.
///
/// Known limit: a `task_plus_one_recorded` reopen is not replayed, so such
/// a bead reads as standing until its next status event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadTouchCloseWire {
    /// The credited close event's timestamp (RFC 3339 Z display form).
    pub closed_at: String,
    /// Payload resolution in snake_case, `done` when absent.
    pub resolution: String,
    /// Trimmed `close_reason`, `None` when blank.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// True iff the bead's final reduced status is `closed` and this event
    /// is the bead's last `issue_closed` event in stream order, credited or
    /// not. Re-closing an already-closed bead writes no event, so any later
    /// close implies a reopen in between.
    pub standing: bool,
}

/// `(mtime_ns, size)` of one stream file, serialized as a two-element array.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadStreamSignatureWire(pub i64, pub u64);

/// The persisted index file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadTouchIndexWire {
    pub schema_version: u32,
    /// RFC 3339 Z instant of the refresh that last wrote this file.
    pub generation: String,
    pub streams: BTreeMap<String, BeadStreamSignatureWire>,
    pub touches: Vec<BeadTouchWire>,
}

/// Result of [`query_bead_touches`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadTouchQueryWire {
    pub schema_version: u32,
    /// Generation of the index the rows came from, `""` on a cache miss.
    pub generation: String,
    /// Newest touch first, then by bead id and actor.
    pub touches: Vec<BeadTouchWire>,
}

/// Result of [`refresh_bead_touch_index`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadTouchRefreshWire {
    pub schema_version: u32,
    pub generation: String,
    /// True when no usable index existed (missing, unreadable, or written
    /// under another schema version), so every stream was reduced.
    pub full_rebuild: bool,
    /// False when nothing changed and the existing file was left untouched.
    pub wrote: bool,
    /// Streams indexed after this refresh.
    pub stream_count: usize,
    /// Streams re-reduced because they were new or their signature changed.
    pub reduced_streams: Vec<String>,
    /// Streams whose cached rows were reused verbatim.
    pub reused_streams: usize,
    /// Streams dropped because their file vanished or stopped being valid.
    pub removed_streams: Vec<String>,
    pub touch_count: usize,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum BeadTouchIndexStateWire {
    Missing,
    Unreadable,
    SchemaMismatch,
    Stale,
    Fresh,
}

/// Result of [`bead_touch_index_status`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadTouchIndexStatusWire {
    pub schema_version: u32,
    pub state: BeadTouchIndexStateWire,
    /// `schema_version` recorded in the index file, when it could be read.
    pub index_schema_version: Option<u32>,
    pub generation: String,
    pub indexed_streams: usize,
    pub current_streams: usize,
    /// Streams that are new or whose signature differs from the index.
    pub changed_streams: Vec<String>,
    /// Indexed streams whose file no longer exists.
    pub vanished_streams: Vec<String>,
}

/// Map a bead event operation onto the verb it contributes, if any.
///
/// The match is exhaustive on purpose: adding a bead operation is a compile
/// error here until someone decides which verb, if any, it earns.
/// `EpicWorkPreclaimed` earns none: it is the runner reserving a phase bead at
/// epic launch, not an agent editing a bead.
pub fn verb_for_operation(
    operation: BeadEventOperationWire,
) -> Option<&'static str> {
    match operation {
        BeadEventOperationWire::IssueCreated => Some("created"),
        BeadEventOperationWire::IssueUpdated => Some("updated"),
        BeadEventOperationWire::NoteAppended
        | BeadEventOperationWire::NoteEdited
        | BeadEventOperationWire::NoteRemoved => Some("noted"),
        BeadEventOperationWire::IssueClosed => Some("closed"),
        BeadEventOperationWire::IssueOpened => Some("reopened"),
        BeadEventOperationWire::TaskPlusOneRecorded => Some("+1"),
        BeadEventOperationWire::ReadyMarked
        | BeadEventOperationWire::ReadyUnmarked => Some("ready"),
        BeadEventOperationWire::TaskSnoozed
        | BeadEventOperationWire::TaskSnoozeWoken
        | BeadEventOperationWire::TaskSnoozeCanceled => Some("snoozed"),
        BeadEventOperationWire::DependencyAdded
        | BeadEventOperationWire::DependencyRemoved => Some("dep"),
        BeadEventOperationWire::LinkAdded
        | BeadEventOperationWire::LinkRemoved => Some("linked"),
        BeadEventOperationWire::ReferenceAdded
        | BeadEventOperationWire::ReferenceRemoved => Some("ref"),
        BeadEventOperationWire::IssueRemoved => Some("removed"),
        BeadEventOperationWire::EpicWorkPreclaimed => None,
    }
}

/// Reduce one stream's JSONL text to that stream's touch rows.
///
/// Pure: no filesystem access. `stream_id` is the stream's filename stem; a
/// stream none of whose events concern its own root bead does not belong to
/// that filename (a renamed or copied file) and reduces to nothing.
///
/// Bead metadata (title, type, status) follows every event in stream order,
/// whoever recorded it, because the store owner's `issue_created` is often the
/// only event carrying a title. Touch rows come only from events recorded by
/// an agent actor: an email address, or a name `validate_agent_name` rejects,
/// is not an agent and contributes nothing. Close events are the exception:
/// the credited closer is [`credited_close_actor`], never the envelope actor
/// alone, and an uncredited close contributes no verb and no timestamp to any
/// touch.
///
/// Note previews replay the structured `note_appended` / `note_edited` /
/// `note_removed` payloads by stable note ID (the append event's `event_id`,
/// mirroring the bead store's note rules: blank append/edit text is ignored,
/// edits retarget text in place, removals delete the note, and an edit or
/// remove naming an unknown ID leaves every other row alone). Only appends
/// by a valid agent enter the projection; an edit by another actor updates
/// the text without moving authorship. Embedded legacy note blobs on
/// `issue_created` never produce previews. Verb counts and touch timestamps
/// are unaffected by the preview replay, including edit/remove actions.
pub fn reduce_stream_touches(
    stream_id: &str,
    contents: &str,
) -> Vec<BeadTouchWire> {
    let events: Vec<ParsedEvent> =
        contents.lines().filter_map(ParsedEvent::parse).collect();
    if !events.iter().any(|event| event.issue_id == stream_id) {
        return Vec::new();
    }

    // Legacy close recovery: the valid agent authors of `note_appended`
    // events per exact instant across the whole stream. A legacy close (no
    // `closed_by`) is credited only to the unique author at its instant.
    let mut note_authors_by_instant: BTreeMap<DateTime<Utc>, BTreeSet<String>> =
        BTreeMap::new();
    for event in &events {
        if event.operation != BeadEventOperationWire::NoteAppended {
            continue;
        }
        let NoteDetail::Append { entry } = &event.note_detail else {
            continue;
        };
        if entry.trim().is_empty() {
            continue;
        }
        let Some(author) = agent_actor(&event.actor) else {
            continue;
        };
        note_authors_by_instant
            .entry(event.at)
            .or_default()
            .insert(author.to_string());
    }

    // The bead's last `issue_closed` event in stream order, credited or not.
    // It decides every close record's `standing` for that bead.
    let mut last_close_idx_by_bead: HashMap<&str, usize> = HashMap::new();
    for (idx, event) in events.iter().enumerate() {
        if event.operation == BeadEventOperationWire::IssueClosed {
            last_close_idx_by_bead.insert(event.issue_id.as_str(), idx);
        }
    }

    let mut metas: HashMap<&str, BeadMeta> = HashMap::new();
    // Owned keys: a credited closer is a new string (the `closed_by`
    // payload or the legacy same-instant note author), not a borrow of the
    // event, so it cannot live in a borrowed-key map.
    let mut accs: BTreeMap<(String, String), TouchAcc> = BTreeMap::new();
    let mut notes: HashMap<String, BTreeMap<String, LiveNote>> = HashMap::new();
    // The actor's latest credited close for that bead, in stream order:
    // `((bead_id, actor), (stream index, close detail, display timestamp))`.
    let mut latest_close: HashMap<
        (String, String),
        (usize, CloseDetail, String),
    > = HashMap::new();
    for (idx, event) in events.iter().enumerate() {
        metas
            .entry(event.issue_id.as_str())
            .or_default()
            .apply(&event.meta_update);
        apply_note_event(&mut notes, event);
        if event.operation == BeadEventOperationWire::IssueClosed {
            let Some(credited) =
                credited_close_actor(event, &note_authors_by_instant)
            else {
                continue;
            };
            let Some(verb) = verb_for_operation(event.operation) else {
                continue;
            };
            accs.entry((event.issue_id.clone(), credited.clone()))
                .or_default()
                .record(verb, event);
            latest_close.insert(
                (event.issue_id.clone(), credited),
                (idx, event.close_detail.clone(), event.at_text.clone()),
            );
            continue;
        }
        let Some(verb) = verb_for_operation(event.operation) else {
            continue;
        };
        let Some(actor) = agent_actor(&event.actor) else {
            continue;
        };
        accs.entry((event.issue_id.clone(), actor.to_string()))
            .or_default()
            .record(verb, event);
    }

    accs.into_iter()
        .filter_map(|((bead_id, actor), acc)| {
            let (first, last) = (acc.first?, acc.last?);
            let meta = metas.get(bead_id.as_str()).cloned().unwrap_or_default();
            let (current_note_count, note_preview) =
                note_preview_for(&bead_id, &actor, &notes);
            let close = latest_close
                .get(&(bead_id.clone(), actor.clone()))
                .map(|(idx, detail, at_text)| {
                    let standing = meta.status == "closed"
                        && last_close_idx_by_bead.get(bead_id.as_str())
                            == Some(idx);
                    BeadTouchCloseWire {
                        closed_at: at_text.clone(),
                        resolution: detail
                            .resolution
                            .as_ref()
                            .map(BeadResolutionWire::as_str)
                            .unwrap_or("done")
                            .to_string(),
                        reason: detail
                            .close_reason
                            .as_deref()
                            .map(str::trim)
                            .filter(|reason| !reason.is_empty())
                            .map(str::to_string),
                        standing,
                    }
                });
            Some(BeadTouchWire {
                actor,
                bead_id,
                title: meta.title,
                issue_type: meta.issue_type,
                status: meta.status,
                verbs: acc.verbs,
                first_at: first.text,
                last_at: last.text,
                current_note_count,
                note_preview,
                close,
                stream_id: stream_id.to_string(),
            })
        })
        .collect()
}

/// The actor credited with an `issue_closed` event, if any.
///
/// A `closed_by` payload marks the new format: the trimmed value must itself
/// be a valid agent, and legacy recovery is never applied. Humans and the
/// owner get no credit. Otherwise the legacy rule applies: the unique valid
/// agent author of a `note_appended` event at the identical instant across
/// the whole stream. Zero or several such authors means no credit; the
/// envelope actor (the creator by construction on legacy events) is never a
/// fallback.
fn credited_close_actor(
    event: &ParsedEvent,
    note_authors_by_instant: &BTreeMap<DateTime<Utc>, BTreeSet<String>>,
) -> Option<String> {
    if let Some(closed_by) = event.close_detail.closed_by.as_deref() {
        let trimmed = closed_by.trim();
        if trimmed.is_empty() {
            return legacy_close_actor(event, note_authors_by_instant);
        }
        return agent_actor(trimmed).map(str::to_string);
    }
    legacy_close_actor(event, note_authors_by_instant)
}

/// Legacy close recovery: the unique valid agent that appended a note at the
/// identical instant, or `None` when there is no such agent or more than one.
fn legacy_close_actor(
    event: &ParsedEvent,
    note_authors_by_instant: &BTreeMap<DateTime<Utc>, BTreeSet<String>>,
) -> Option<String> {
    let authors = note_authors_by_instant.get(&event.at)?;
    if authors.len() == 1 {
        authors.iter().next().cloned()
    } else {
        None
    }
}

/// One surviving structured note before it is projected onto a touch row.
struct LiveNote {
    author: String,
    at: DateTime<Utc>,
    at_text: String,
    text: String,
    edited_at: Option<String>,
    edited_by: Option<String>,
}

impl LiveNote {
    fn preview(&self, id: &str) -> BeadNotePreviewWire {
        let (text, truncated) = truncate_preview_text(&self.text);
        BeadNotePreviewWire {
            id: id.to_string(),
            author: self.author.clone(),
            timestamp: self.at_text.clone(),
            text,
            edited_at: self.edited_at.clone(),
            edited_by: self.edited_by.clone(),
            truncated,
        }
    }
}

/// Replay one event's note payload into the per-bead note tables.
///
/// Never affects verbs or timestamps; those stay with [`TouchAcc`].
fn apply_note_event(
    notes: &mut HashMap<String, BTreeMap<String, LiveNote>>,
    event: &ParsedEvent,
) {
    match (&event.note_detail, event.operation) {
        (
            NoteDetail::Append { entry },
            BeadEventOperationWire::NoteAppended,
        ) => {
            let text = entry.trim();
            if text.is_empty() {
                return;
            }
            let Some(author) = agent_actor(&event.actor) else {
                return;
            };
            let note_id = event.event_id.trim();
            if note_id.is_empty() {
                return;
            }
            notes.entry(event.issue_id.clone()).or_default().insert(
                note_id.to_string(),
                LiveNote {
                    author: author.to_string(),
                    at: event.at,
                    at_text: event.at_text.clone(),
                    text: text.to_string(),
                    edited_at: None,
                    edited_by: None,
                },
            );
        }
        (
            NoteDetail::Edit { note_id, text },
            BeadEventOperationWire::NoteEdited,
        ) => {
            let note_id = note_id.trim();
            let text = text.trim();
            if note_id.is_empty() || text.is_empty() {
                return;
            }
            if let Some(note) = notes
                .get_mut(&event.issue_id)
                .and_then(|by_id| by_id.get_mut(note_id))
            {
                note.text = text.to_string();
                note.edited_at = Some(event.at_text.clone());
                note.edited_by = Some(event.actor.clone());
            }
        }
        (
            NoteDetail::Remove { note_id },
            BeadEventOperationWire::NoteRemoved,
        ) => {
            let note_id = note_id.trim();
            if note_id.is_empty() {
                return;
            }
            if let Some(by_id) = notes.get_mut(&event.issue_id) {
                by_id.remove(note_id);
            }
        }
        _ => {}
    }
}

/// Count the surviving notes one author holds on one bead and select the
/// newest preview by append instant, breaking ties on the stable note ID.
fn note_preview_for(
    bead_id: &str,
    actor: &str,
    notes: &HashMap<String, BTreeMap<String, LiveNote>>,
) -> (u64, Option<BeadNotePreviewWire>) {
    let Some(by_id) = notes.get(bead_id) else {
        return (0, None);
    };
    let mut authored: Vec<(&String, &LiveNote)> = by_id
        .iter()
        .filter(|(_, note)| note.author == actor)
        .collect();
    if authored.is_empty() {
        return (0, None);
    }
    authored.sort_by(|(left_id, left), (right_id, right)| {
        left.at.cmp(&right.at).then_with(|| left_id.cmp(right_id))
    });
    let count = authored.len() as u64;
    let (id, newest) = authored.last().expect("non-empty after length check");
    (count, Some(newest.preview(id)))
}

/// Cut note text to [`NOTE_PREVIEW_TEXT_LIMIT`] Unicode scalar values.
fn truncate_preview_text(text: &str) -> (String, bool) {
    if text.chars().nth(NOTE_PREVIEW_TEXT_LIMIT).is_none() {
        return (text.to_string(), false);
    }
    (text.chars().take(NOTE_PREVIEW_TEXT_LIMIT).collect(), true)
}

/// Bring the index at `index_path` up to date with `beads_dir`'s streams.
///
/// Incremental: a stream whose `(mtime_ns, size)` matches the index keeps its
/// cached rows verbatim; only new and changed streams are re-reduced and only
/// vanished streams are dropped. Idempotent: when nothing changed the file is
/// not rewritten, so its mtime stays put for downstream mtime caches. A
/// missing, unreadable, or wrong-schema index rebuilds from scratch.
///
/// Serialized by an exclusive `<index_path>.lock` file lock.
pub fn refresh_bead_touch_index(
    beads_dir: &Path,
    index_path: &Path,
) -> Result<BeadTouchRefreshWire, BeadError> {
    let parent = index_parent(index_path);
    fs::create_dir_all(parent)?;
    let lock_path = index_lock_path(index_path);
    let lock = acquire_store_lock(
        &lock_path,
        &holder_path_for(&lock_path),
        LockMode::Exclusive,
        timeout_from_env(LOCK_TIMEOUT_ENV, LOCK_TIMEOUT_DEFAULT),
        "bead touch index refresh",
    )
    .map_err(lock_error)?;
    let result = refresh_locked(beads_dir, index_path);
    let unlock = lock.release();
    match (result, unlock) {
        (Ok(outcome), Ok(())) => Ok(outcome),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error.into()),
    }
}

/// Return the indexed touches of `actors`, or of every actor when `None`.
///
/// Actors match exactly after trimming. Read-only and infallible: it loads
/// the index file and nothing else, and a missing, truncated, unparseable, or
/// wrong-schema index is a cache miss that returns no rows.
pub fn query_bead_touches(
    index_path: &Path,
    actors: Option<&[String]>,
) -> BeadTouchQueryWire {
    let IndexRead::Loaded(index) = read_index(index_path) else {
        return BeadTouchQueryWire {
            schema_version: BEAD_TOUCH_INDEX_WIRE_SCHEMA_VERSION,
            generation: String::new(),
            touches: Vec::new(),
        };
    };
    let wanted: Option<BTreeSet<&str>> =
        actors.map(|actors| actors.iter().map(|a| a.trim()).collect());
    let mut touches: Vec<BeadTouchWire> = index
        .touches
        .into_iter()
        .filter(|touch| match &wanted {
            Some(wanted) => wanted.contains(touch.actor.as_str()),
            None => true,
        })
        .collect();
    touches.sort_by(|a, b| {
        instant_or_min(&b.last_at)
            .cmp(&instant_or_min(&a.last_at))
            .then_with(|| a.bead_id.cmp(&b.bead_id))
            .then_with(|| a.actor.cmp(&b.actor))
    });
    BeadTouchQueryWire {
        schema_version: BEAD_TOUCH_INDEX_WIRE_SCHEMA_VERSION,
        generation: index.generation,
        touches,
    }
}

/// Classify the index against the live streams without reducing anything.
///
/// Stat-only and lock-free: it compares each stream file's `(mtime_ns, size)`
/// with the signature the index recorded.
pub fn bead_touch_index_status(
    beads_dir: &Path,
    index_path: &Path,
) -> Result<BeadTouchIndexStatusWire, BeadError> {
    let current = scan_stream_signatures(beads_dir)?;
    let status = |state,
                  index_schema_version,
                  generation: String,
                  indexed_streams,
                  changed_streams,
                  vanished_streams| BeadTouchIndexStatusWire {
        schema_version: BEAD_TOUCH_INDEX_WIRE_SCHEMA_VERSION,
        state,
        index_schema_version,
        generation,
        indexed_streams,
        current_streams: current.len(),
        changed_streams,
        vanished_streams,
    };
    Ok(match read_index(index_path) {
        IndexRead::Missing => status(
            BeadTouchIndexStateWire::Missing,
            None,
            String::new(),
            0,
            Vec::new(),
            Vec::new(),
        ),
        IndexRead::Unreadable => status(
            BeadTouchIndexStateWire::Unreadable,
            None,
            String::new(),
            0,
            Vec::new(),
            Vec::new(),
        ),
        IndexRead::SchemaMismatch(found) => status(
            BeadTouchIndexStateWire::SchemaMismatch,
            Some(found),
            String::new(),
            0,
            Vec::new(),
            Vec::new(),
        ),
        IndexRead::Loaded(index) => {
            let changed: Vec<String> = current
                .iter()
                .filter(|(id, sig)| index.streams.get(*id) != Some(*sig))
                .map(|(id, _)| id.clone())
                .collect();
            let vanished: Vec<String> = index
                .streams
                .keys()
                .filter(|id| !current.contains_key(*id))
                .cloned()
                .collect();
            let state = if changed.is_empty() && vanished.is_empty() {
                BeadTouchIndexStateWire::Fresh
            } else {
                BeadTouchIndexStateWire::Stale
            };
            status(
                state,
                Some(index.schema_version),
                index.generation.clone(),
                index.streams.len(),
                changed,
                vanished,
            )
        }
    })
}

fn refresh_locked(
    beads_dir: &Path,
    index_path: &Path,
) -> Result<BeadTouchRefreshWire, BeadError> {
    // Signatures are taken before any stream is read: a file that changes
    // mid-refresh is recorded with its older signature, so the next refresh
    // re-reduces it instead of trusting stale rows under a fresh signature.
    let current = scan_stream_signatures(beads_dir)?;
    let cached = match read_index(index_path) {
        IndexRead::Loaded(index) => Some(index),
        IndexRead::Missing
        | IndexRead::Unreadable
        | IndexRead::SchemaMismatch(_) => None,
    };
    let full_rebuild = cached.is_none();
    let (cached_streams, cached_generation, mut cached_rows) = match cached {
        Some(index) => {
            let mut by_stream: HashMap<String, Vec<BeadTouchWire>> =
                HashMap::new();
            for touch in index.touches {
                by_stream
                    .entry(touch.stream_id.clone())
                    .or_default()
                    .push(touch);
            }
            (index.streams, index.generation, by_stream)
        }
        None => (BTreeMap::new(), String::new(), HashMap::new()),
    };

    let streams_dir = event_streams_dir(beads_dir);
    let mut streams = BTreeMap::new();
    let mut touches = Vec::new();
    let mut reduced_streams = Vec::new();
    let mut reused_streams = 0;
    for (stream_id, signature) in &current {
        if cached_streams.get(stream_id) == Some(signature) {
            touches.extend(cached_rows.remove(stream_id).unwrap_or_default());
            streams.insert(stream_id.clone(), *signature);
            reused_streams += 1;
            continue;
        }
        let path = streams_dir.join(format!("{stream_id}.jsonl"));
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            // Vanished between the scan and the read: treat as removed.
            Err(error) if error.kind() == ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(BeadError::io(format!(
                    "failed to read bead event stream {}: {error}",
                    path.display()
                )))
            }
        };
        touches.extend(reduce_stream_touches(
            stream_id,
            &String::from_utf8_lossy(&bytes),
        ));
        streams.insert(stream_id.clone(), *signature);
        reduced_streams.push(stream_id.clone());
    }
    let removed_streams: Vec<String> = cached_streams
        .keys()
        .filter(|id| !streams.contains_key(*id))
        .cloned()
        .collect();

    touches.sort_by(|a, b| {
        (&a.stream_id, &a.bead_id, &a.actor).cmp(&(
            &b.stream_id,
            &b.bead_id,
            &b.actor,
        ))
    });
    let touch_count = touches.len();
    let stream_count = streams.len();

    let changed = full_rebuild
        || !reduced_streams.is_empty()
        || !removed_streams.is_empty();
    let generation = if changed {
        let now: DateTime<Utc> = SystemTime::now().into();
        let generation = now.to_rfc3339_opts(SecondsFormat::Secs, true);
        write_index_atomic(
            index_path,
            &BeadTouchIndexWire {
                schema_version: BEAD_TOUCH_INDEX_WIRE_SCHEMA_VERSION,
                generation: generation.clone(),
                streams,
                touches,
            },
        )?;
        generation
    } else {
        cached_generation
    };

    Ok(BeadTouchRefreshWire {
        schema_version: BEAD_TOUCH_INDEX_WIRE_SCHEMA_VERSION,
        generation,
        full_rebuild,
        wrote: changed,
        stream_count,
        reduced_streams,
        reused_streams,
        removed_streams,
        touch_count,
    })
}

#[derive(Debug, Default, Clone)]
struct BeadMeta {
    title: String,
    issue_type: String,
    status: String,
}

impl BeadMeta {
    fn apply(&mut self, update: &MetaUpdate) {
        if let Some(title) = &update.title {
            self.title.clone_from(title);
        }
        if let Some(issue_type) = update.issue_type {
            self.issue_type = issue_type.to_string();
        }
        if let Some(status) = update.status {
            self.status = status.to_string();
        }
    }
}

/// Metadata one event carries about its bead.
#[derive(Debug, Default)]
struct MetaUpdate {
    title: Option<String>,
    issue_type: Option<&'static str>,
    status: Option<&'static str>,
}

struct Stamp {
    at: DateTime<Utc>,
    text: String,
}

#[derive(Default)]
struct TouchAcc {
    verbs: BTreeMap<String, u64>,
    first: Option<Stamp>,
    last: Option<Stamp>,
}

impl TouchAcc {
    fn record(&mut self, verb: &str, event: &ParsedEvent) {
        *self.verbs.entry(verb.to_string()).or_insert(0) += 1;
        let stamp = || Stamp {
            at: event.at,
            text: event.at_text.clone(),
        };
        match &self.first {
            Some(first) if event.at >= first.at => {}
            _ => self.first = Some(stamp()),
        }
        match &self.last {
            Some(last) if event.at < last.at => {}
            _ => self.last = Some(stamp()),
        }
    }
}

struct ParsedEvent {
    operation: BeadEventOperationWire,
    event_id: String,
    issue_id: String,
    actor: String,
    at: DateTime<Utc>,
    at_text: String,
    meta_update: MetaUpdate,
    note_detail: NoteDetail,
    close_detail: CloseDetail,
}

/// Close facts carried by an `issue_closed` event's payload.
///
/// `closed_by` is the durable closer on new events and absent on legacy
/// ones; `resolution` is `None` when absent (which reads as `done`), and
/// `close_reason` is the raw payload value before trimming.
#[derive(Debug, Default, Clone)]
struct CloseDetail {
    closed_by: Option<String>,
    resolution: Option<BeadResolutionWire>,
    close_reason: Option<String>,
}

/// Structured note payload carried by one event, if any.
///
/// Kept separate from [`MetaUpdate`] because previews replay by stable note
/// ID while verbs and timestamps stay with [`TouchAcc`]. A detail whose
/// operation disagrees is ignored by [`apply_note_event`].
#[derive(Debug, Default)]
enum NoteDetail {
    Append {
        entry: String,
    },
    Edit {
        note_id: String,
        text: String,
    },
    Remove {
        note_id: String,
    },
    #[default]
    None,
}

impl ParsedEvent {
    /// Parse one stream line, or `None` for anything that is not a usable
    /// event: blank or non-JSON text, a missing field, an operation this
    /// build does not know, or a payload whose `kind` disagrees with it.
    fn parse(line: &str) -> Option<Self> {
        let line = line.trim();
        if line.is_empty() {
            return None;
        }
        let value: Value = serde_json::from_str(line).ok()?;
        let operation_text = value.get("operation")?.as_str()?;
        let operation: BeadEventOperationWire =
            serde_json::from_value(Value::String(operation_text.to_string()))
                .ok()?;
        let payload = value.get("payload")?;
        if payload.get("kind")?.as_str()? != operation_text {
            return None;
        }
        let issue_id = value.get("issue_id")?.as_str()?;
        if issue_id.is_empty() {
            return None;
        }
        let raw_at = value.get("timestamp")?.as_str()?;
        let at = DateTime::parse_from_rfc3339(raw_at.trim())
            .ok()?
            .with_timezone(&Utc);
        let event_id = value
            .get("event_id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let note_detail = note_detail(operation, payload);
        let close_detail = close_detail(operation, payload);
        Some(Self {
            operation,
            event_id,
            issue_id: issue_id.to_string(),
            actor: value.get("actor")?.as_str()?.trim().to_string(),
            at,
            at_text: display_timestamp(raw_at.trim(), at),
            meta_update: meta_update(operation, payload),
            note_detail,
            close_detail,
        })
    }
}

/// Extract the structured note payload, if the operation carries one.
///
/// Missing or mistyped fields yield [`NoteDetail::None`]: the event still
/// counts its verb, it just has no preview effect.
fn note_detail(
    operation: BeadEventOperationWire,
    payload: &Value,
) -> NoteDetail {
    match operation {
        BeadEventOperationWire::NoteAppended => match payload.get("entry") {
            Some(Value::String(entry)) => NoteDetail::Append {
                entry: entry.clone(),
            },
            _ => NoteDetail::None,
        },
        BeadEventOperationWire::NoteEdited => {
            match (payload.get("note_id"), payload.get("text")) {
                (Some(Value::String(note_id)), Some(Value::String(text))) => {
                    NoteDetail::Edit {
                        note_id: note_id.clone(),
                        text: text.clone(),
                    }
                }
                _ => NoteDetail::None,
            }
        }
        BeadEventOperationWire::NoteRemoved => match payload.get("note_id") {
            Some(Value::String(note_id)) => NoteDetail::Remove {
                note_id: note_id.clone(),
            },
            _ => NoteDetail::None,
        },
        _ => NoteDetail::None,
    }
}

/// Extract the close facts, if the operation is a close.
///
/// Missing or mistyped fields are tolerated: a non-string `closed_by`
/// reads as absent (the legacy path), and a missing or unknown resolution
/// reads as `done` downstream.
fn close_detail(
    operation: BeadEventOperationWire,
    payload: &Value,
) -> CloseDetail {
    if operation != BeadEventOperationWire::IssueClosed {
        return CloseDetail::default();
    }
    CloseDetail {
        closed_by: payload
            .get("closed_by")
            .and_then(Value::as_str)
            .map(str::to_string),
        resolution: payload.get("resolution").and_then(|value| {
            serde_json::from_value::<BeadResolutionWire>(value.clone()).ok()
        }),
        close_reason: payload
            .get("close_reason")
            .and_then(Value::as_str)
            .map(str::to_string),
    }
}

/// The trimmed actor when it names an agent, `None` for humans and garbage.
fn agent_actor(actor: &str) -> Option<&str> {
    validate_agent_name(actor).ok().map(|()| actor)
}

fn meta_update(
    operation: BeadEventOperationWire,
    payload: &Value,
) -> MetaUpdate {
    let title = |source: Option<&Value>| {
        source
            .and_then(|value| value.get("title"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|title| !title.is_empty())
            .map(str::to_string)
    };
    let status = |source: Option<&Value>| {
        source
            .and_then(|value| value.get("status"))
            .and_then(|value| {
                serde_json::from_value::<StatusWire>(value.clone()).ok()
            })
            .map(|status| status_text(&status))
    };
    match operation {
        BeadEventOperationWire::IssueCreated => {
            let issue = payload.get("issue");
            MetaUpdate {
                title: title(issue),
                issue_type: issue
                    .and_then(|issue| issue.get("issue_type"))
                    .and_then(|value| {
                        serde_json::from_value::<IssueTypeWire>(value.clone())
                            .ok()
                    })
                    .map(|issue_type| issue_type_text(&issue_type)),
                status: status(issue),
            }
        }
        BeadEventOperationWire::IssueUpdated => {
            let fields = payload.get("fields");
            MetaUpdate {
                title: title(fields),
                issue_type: None,
                status: status(fields),
            }
        }
        BeadEventOperationWire::IssueOpened => MetaUpdate {
            status: Some("open"),
            ..MetaUpdate::default()
        },
        BeadEventOperationWire::IssueClosed => MetaUpdate {
            status: Some("closed"),
            ..MetaUpdate::default()
        },
        BeadEventOperationWire::EpicWorkPreclaimed => MetaUpdate {
            status: Some("in_progress"),
            ..MetaUpdate::default()
        },
        BeadEventOperationWire::TaskSnoozed => MetaUpdate {
            status: Some("snoozed"),
            ..MetaUpdate::default()
        },
        BeadEventOperationWire::TaskSnoozeCanceled
        | BeadEventOperationWire::TaskSnoozeWoken => MetaUpdate {
            status: Some("ready"),
            ..MetaUpdate::default()
        },
        // `task_plus_one_recorded` can reopen a bead, but whether it does
        // depends on close and observation timestamps this reduction does
        // not replay; the status catches up at the bead's next status event.
        BeadEventOperationWire::NoteAppended
        | BeadEventOperationWire::NoteEdited
        | BeadEventOperationWire::NoteRemoved
        | BeadEventOperationWire::IssueRemoved
        | BeadEventOperationWire::DependencyAdded
        | BeadEventOperationWire::DependencyRemoved
        | BeadEventOperationWire::ReferenceAdded
        | BeadEventOperationWire::ReferenceRemoved
        | BeadEventOperationWire::LinkAdded
        | BeadEventOperationWire::LinkRemoved
        | BeadEventOperationWire::ReadyMarked
        | BeadEventOperationWire::ReadyUnmarked
        | BeadEventOperationWire::TaskPlusOneRecorded => MetaUpdate::default(),
    }
}

fn status_text(status: &StatusWire) -> &'static str {
    match status {
        StatusWire::Open => "open",
        StatusWire::Claimed => "claimed",
        StatusWire::Ready => "ready",
        StatusWire::Snoozed => "snoozed",
        StatusWire::InProgress => "in_progress",
        StatusWire::Closed => "closed",
    }
}

fn issue_type_text(issue_type: &IssueTypeWire) -> &'static str {
    match issue_type {
        IssueTypeWire::Plan => "plan",
        IssueTypeWire::Phase => "phase",
        IssueTypeWire::Task => "task",
    }
}

/// The store's own `...Z` spelling when the event used it, else the instant
/// normalized to UTC.
fn display_timestamp(raw: &str, at: DateTime<Utc>) -> String {
    if raw.ends_with('Z') {
        raw.to_string()
    } else {
        at.to_rfc3339_opts(SecondsFormat::AutoSi, true)
    }
}

fn instant_or_min(text: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(text)
        .map(|at| at.with_timezone(&Utc))
        .unwrap_or(DateTime::<Utc>::MIN_UTC)
}

enum IndexRead {
    Missing,
    Unreadable,
    SchemaMismatch(u32),
    Loaded(BeadTouchIndexWire),
}

fn read_index(index_path: &Path) -> IndexRead {
    let bytes = match fs::read(index_path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return IndexRead::Missing
        }
        Err(_) => return IndexRead::Unreadable,
    };
    // Probe the version alone first: a typed parse of another schema's file
    // would fail for the wrong reason, and building a `Value` tree of a
    // multi-megabyte index just to read one number doubles the query cost.
    let Ok(SchemaProbe { schema_version }) = serde_json::from_slice(&bytes)
    else {
        return IndexRead::Unreadable;
    };
    match schema_version.map(u32::try_from) {
        Some(Ok(BEAD_TOUCH_INDEX_WIRE_SCHEMA_VERSION)) => {
            match serde_json::from_slice(&bytes) {
                Ok(index) => IndexRead::Loaded(index),
                Err(_) => IndexRead::Unreadable,
            }
        }
        Some(Ok(other)) => IndexRead::SchemaMismatch(other),
        Some(Err(_)) | None => IndexRead::Unreadable,
    }
}

#[derive(Deserialize)]
struct SchemaProbe {
    schema_version: Option<u64>,
}

fn scan_stream_signatures(
    beads_dir: &Path,
) -> Result<BTreeMap<String, BeadStreamSignatureWire>, BeadError> {
    let streams_dir = event_streams_dir(beads_dir);
    let entries = match fs::read_dir(&streams_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Ok(BTreeMap::new())
        }
        Err(error) => {
            return Err(BeadError::io(format!(
                "failed to read bead event streams directory {}: {error}",
                streams_dir.display()
            )))
        }
    };
    let mut signatures = BTreeMap::new();
    for entry in entries {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("jsonl") {
            continue;
        }
        let Some(stream_id) = path.file_stem().and_then(|stem| stem.to_str())
        else {
            continue;
        };
        // A file that vanishes between the listing and the stat is simply
        // not there anymore.
        let Ok(metadata) = fs::metadata(&path) else {
            continue;
        };
        if !metadata.is_file() {
            continue;
        }
        signatures.insert(
            stream_id.to_string(),
            BeadStreamSignatureWire(
                mtime_ns(metadata.modified().ok()),
                metadata.len(),
            ),
        );
    }
    Ok(signatures)
}

fn mtime_ns(modified: Option<SystemTime>) -> i64 {
    let Some(modified) = modified else {
        return 0;
    };
    let nanos = match modified.duration_since(UNIX_EPOCH) {
        Ok(after) => i128::try_from(after.as_nanos()).unwrap_or(i128::MAX),
        Err(before) => {
            -i128::try_from(before.duration().as_nanos()).unwrap_or(i128::MAX)
        }
    };
    i64::try_from(nanos).unwrap_or(if nanos < 0 { i64::MIN } else { i64::MAX })
}

fn write_index_atomic(
    index_path: &Path,
    index: &BeadTouchIndexWire,
) -> Result<(), BeadError> {
    let mut temporary = NamedTempFile::new_in(index_parent(index_path))?;
    temporary.write_all(&serde_json::to_vec(index)?)?;
    temporary.write_all(b"\n")?;
    temporary.flush()?;
    temporary.as_file().sync_all()?;
    temporary
        .persist(index_path)
        .map_err(|error| BeadError::from(error.error))?;
    Ok(())
}

fn index_parent(index_path: &Path) -> &Path {
    match index_path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    }
}

fn index_lock_path(index_path: &Path) -> PathBuf {
    let filename = index_path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("bead_touch_index.json");
    index_path.with_file_name(format!("{filename}.lock"))
}

fn lock_error(error: StoreLockError) -> BeadError {
    match error {
        StoreLockError::Timeout { .. } => {
            BeadError::conflict(error.to_string())
        }
        StoreLockError::Open { .. } | StoreLockError::Acquire { .. } => {
            BeadError::io(error.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::{tempdir, TempDir};

    const OWNER: &str = "owner@example.com";
    const AGENT_A: &str = "bbugyi200.athena.0aa";
    const AGENT_B: &str = "bbugyi200.athena.0bb";

    fn event(
        actor: &str,
        operation: &str,
        issue_id: &str,
        timestamp: &str,
        mut payload: Value,
    ) -> String {
        payload["kind"] = json!(operation);
        json!({
            "schema_version": 1,
            "event_id": format!("{issue_id}:{operation}:{timestamp}"),
            "timestamp": timestamp,
            "actor": actor,
            "operation": operation,
            "issue_id": issue_id,
            "payload": payload,
        })
        .to_string()
    }

    fn created(
        actor: &str,
        issue_id: &str,
        title: &str,
        issue_type: &str,
        timestamp: &str,
    ) -> String {
        event(
            actor,
            "issue_created",
            issue_id,
            timestamp,
            json!({"issue": {
                "id": issue_id,
                "title": title,
                "status": "open",
                "issue_type": issue_type,
            }}),
        )
    }

    fn note(actor: &str, issue_id: &str, timestamp: &str) -> String {
        event(
            actor,
            "note_appended",
            issue_id,
            timestamp,
            json!({"entry": "text"}),
        )
    }

    fn note_entry(
        actor: &str,
        issue_id: &str,
        timestamp: &str,
        entry: &str,
    ) -> String {
        event(
            actor,
            "note_appended",
            issue_id,
            timestamp,
            json!({"entry": entry}),
        )
    }

    fn note_with_id(
        event_id: &str,
        actor: &str,
        issue_id: &str,
        timestamp: &str,
        entry: &str,
    ) -> String {
        json!({
            "schema_version": 1,
            "event_id": event_id,
            "timestamp": timestamp,
            "actor": actor,
            "operation": "note_appended",
            "issue_id": issue_id,
            "payload": {"kind": "note_appended", "entry": entry},
        })
        .to_string()
    }

    fn note_edited(
        actor: &str,
        issue_id: &str,
        timestamp: &str,
        note_id: &str,
        text: &str,
    ) -> String {
        event(
            actor,
            "note_edited",
            issue_id,
            timestamp,
            json!({"note_id": note_id, "text": text}),
        )
    }

    fn note_removed(
        actor: &str,
        issue_id: &str,
        timestamp: &str,
        note_id: &str,
    ) -> String {
        event(
            actor,
            "note_removed",
            issue_id,
            timestamp,
            json!({"note_id": note_id}),
        )
    }

    fn append_id(issue_id: &str, timestamp: &str) -> String {
        format!("{issue_id}:note_appended:{timestamp}")
    }

    fn bare(actor: &str, operation: &str, issue_id: &str, ts: &str) -> String {
        event(actor, operation, issue_id, ts, json!({}))
    }

    fn lines(events: &[String]) -> String {
        let mut text = events.join("\n");
        text.push('\n');
        text
    }

    fn touch_for<'a>(
        touches: &'a [BeadTouchWire],
        bead_id: &str,
        actor: &str,
    ) -> &'a BeadTouchWire {
        touches
            .iter()
            .find(|touch| touch.bead_id == bead_id && touch.actor == actor)
            .unwrap_or_else(|| panic!("no touch for {actor} on {bead_id}"))
    }

    fn verbs(pairs: &[(&str, u64)]) -> BTreeMap<String, u64> {
        pairs
            .iter()
            .map(|(verb, count)| (verb.to_string(), *count))
            .collect()
    }

    struct Store {
        _dir: TempDir,
        beads_dir: PathBuf,
        index_path: PathBuf,
    }

    impl Store {
        fn new() -> Self {
            let dir = tempdir().unwrap();
            let beads_dir = dir.path().join("beads");
            let index_path = dir.path().join("project/agent_bead_touches.json");
            Self {
                _dir: dir,
                beads_dir,
                index_path,
            }
        }

        fn stream_path(&self, stream_id: &str) -> PathBuf {
            event_streams_dir(&self.beads_dir)
                .join(format!("{stream_id}.jsonl"))
        }

        fn write_stream(&self, stream_id: &str, events: &[String]) {
            let path = self.stream_path(stream_id);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, lines(events)).unwrap();
        }

        fn append_stream(&self, stream_id: &str, event: &str) {
            let mut file = fs::OpenOptions::new()
                .append(true)
                .open(self.stream_path(stream_id))
                .unwrap();
            writeln!(file, "{event}").unwrap();
        }

        fn refresh(&self) -> BeadTouchRefreshWire {
            refresh_bead_touch_index(&self.beads_dir, &self.index_path).unwrap()
        }

        fn query(&self, actors: Option<&[String]>) -> BeadTouchQueryWire {
            query_bead_touches(&self.index_path, actors)
        }

        fn status(&self) -> BeadTouchIndexStatusWire {
            bead_touch_index_status(&self.beads_dir, &self.index_path).unwrap()
        }

        fn read_index(&self) -> BeadTouchIndexWire {
            serde_json::from_slice(&fs::read(&self.index_path).unwrap())
                .unwrap()
        }

        fn write_index(&self, index: &BeadTouchIndexWire) {
            fs::write(&self.index_path, serde_json::to_vec(index).unwrap())
                .unwrap();
        }
    }

    #[test]
    fn verb_table_covers_every_operation_and_excludes_preclaim() {
        use BeadEventOperationWire as Op;
        let table = [
            (Op::IssueCreated, Some("created")),
            (Op::IssueUpdated, Some("updated")),
            (Op::NoteAppended, Some("noted")),
            (Op::NoteEdited, Some("noted")),
            (Op::NoteRemoved, Some("noted")),
            (Op::IssueClosed, Some("closed")),
            (Op::IssueOpened, Some("reopened")),
            (Op::TaskPlusOneRecorded, Some("+1")),
            (Op::ReadyMarked, Some("ready")),
            (Op::ReadyUnmarked, Some("ready")),
            (Op::TaskSnoozed, Some("snoozed")),
            (Op::TaskSnoozeWoken, Some("snoozed")),
            (Op::TaskSnoozeCanceled, Some("snoozed")),
            (Op::DependencyAdded, Some("dep")),
            (Op::DependencyRemoved, Some("dep")),
            (Op::LinkAdded, Some("linked")),
            (Op::LinkRemoved, Some("linked")),
            (Op::ReferenceAdded, Some("ref")),
            (Op::ReferenceRemoved, Some("ref")),
            (Op::IssueRemoved, Some("removed")),
            (Op::EpicWorkPreclaimed, None),
        ];
        for (operation, verb) in table {
            assert_eq!(verb_for_operation(operation), verb, "{operation:?}");
        }
    }

    #[test]
    fn reduction_counts_verbs_and_bounds_timestamps_regardless_of_order() {
        let touches = reduce_stream_touches(
            "b-1",
            &lines(&[
                created(OWNER, "b-1", "Bead", "task", "2026-01-01T00:00:00Z"),
                note(AGENT_A, "b-1", "2026-01-01T00:05:00Z"),
                note(AGENT_A, "b-1", "2026-01-01T00:03:00Z"),
                note(AGENT_A, "b-1", "2026-01-01T00:09:00Z"),
                bare(AGENT_A, "issue_updated", "b-1", "2026-01-01T00:07:00Z"),
            ]),
        );
        assert_eq!(touches.len(), 1);
        let touch = &touches[0];
        assert_eq!(touch.verbs, verbs(&[("noted", 3), ("updated", 1)]));
        assert_eq!(touch.first_at, "2026-01-01T00:03:00Z");
        assert_eq!(touch.last_at, "2026-01-01T00:09:00Z");
        assert_eq!(touch.stream_id, "b-1");
    }

    #[test]
    fn a_single_contributing_event_bounds_both_ends() {
        let touches = reduce_stream_touches(
            "b-1",
            &lines(&[
                created(OWNER, "b-1", "Bead", "task", "2026-01-01T00:00:00Z"),
                event(
                    AGENT_A,
                    "issue_closed",
                    "b-1",
                    "2026-01-01T00:04:00Z",
                    json!({"closed_by": AGENT_A}),
                ),
            ]),
        );
        assert_eq!(touches[0].first_at, touches[0].last_at);
        assert_eq!(touches[0].last_at, "2026-01-01T00:04:00Z");
    }

    #[test]
    fn timestamps_with_offsets_normalize_to_utc_z() {
        let touches = reduce_stream_touches(
            "b-1",
            &lines(&[
                created(OWNER, "b-1", "Bead", "task", "2026-01-01T00:00:00Z"),
                note(AGENT_A, "b-1", "2026-01-01T05:30:00.250+05:30"),
            ]),
        );
        assert_eq!(touches[0].first_at, "2026-01-01T00:00:00.250Z");
    }

    #[test]
    fn title_type_and_status_follow_the_newest_carrying_event() {
        let touches = reduce_stream_touches(
            "b-1",
            &lines(&[
                created(
                    OWNER,
                    "b-1",
                    "First title",
                    "plan",
                    "2026-01-01T00:00:00Z",
                ),
                event(
                    AGENT_A,
                    "issue_updated",
                    "b-1",
                    "2026-01-01T00:01:00Z",
                    json!({"fields": {"title": "Second title"}}),
                ),
                event(
                    AGENT_A,
                    "issue_updated",
                    "b-1",
                    "2026-01-01T00:02:00Z",
                    json!({"fields": {"status": "in_progress"}}),
                ),
                note(AGENT_A, "b-1", "2026-01-01T00:03:00Z"),
            ]),
        );
        let touch = &touches[0];
        assert_eq!(touch.title, "Second title");
        assert_eq!(touch.issue_type, "plan");
        assert_eq!(touch.status, "in_progress");
    }

    #[test]
    fn status_tracks_close_reopen_preclaim_and_snooze_events() {
        let status_after = |operation: &str| {
            reduce_stream_touches(
                "b-1",
                &lines(&[
                    created(
                        OWNER,
                        "b-1",
                        "Bead",
                        "task",
                        "2026-01-01T00:00:00Z",
                    ),
                    bare(AGENT_A, operation, "b-1", "2026-01-01T00:01:00Z"),
                    // Guarantees a row even for the excluded preclaim.
                    note(AGENT_A, "b-1", "2026-01-01T00:02:00Z"),
                ]),
            )[0]
            .status
            .clone()
        };
        assert_eq!(status_after("issue_closed"), "closed");
        assert_eq!(status_after("issue_opened"), "open");
        assert_eq!(status_after("epic_work_preclaimed"), "in_progress");
        assert_eq!(status_after("task_snoozed"), "snoozed");
        assert_eq!(status_after("task_snooze_canceled"), "ready");
        assert_eq!(status_after("task_snooze_woken"), "ready");
        assert_eq!(status_after("note_appended"), "open");
    }

    #[test]
    fn owner_email_and_invalid_actors_never_become_touchers() {
        let touches = reduce_stream_touches(
            "b-1",
            &lines(&[
                created(OWNER, "b-1", "Bead", "task", "2026-01-01T00:00:00Z"),
                note(OWNER, "b-1", "2026-01-01T00:01:00Z"),
                note("", "b-1", "2026-01-01T00:02:00Z"),
                note("   ", "b-1", "2026-01-01T00:03:00Z"),
                note("has space", "b-1", "2026-01-01T00:04:00Z"),
                note("a/b", "b-1", "2026-01-01T00:05:00Z"),
                note("legacy--role.f0", "b-1", "2026-01-01T00:06:00Z"),
                note(AGENT_A, "b-1", "2026-01-01T00:07:00Z"),
            ]),
        );
        let actors: Vec<_> =
            touches.iter().map(|touch| touch.actor.as_str()).collect();
        assert_eq!(actors, vec![AGENT_A]);
    }

    #[test]
    fn owner_created_title_still_reaches_agent_rows() {
        let touches = reduce_stream_touches(
            "b-1",
            &lines(&[
                created(
                    OWNER,
                    "b-1",
                    "Owner titled",
                    "phase",
                    "2026-01-01T00:00:00Z",
                ),
                note(AGENT_A, "b-1", "2026-01-01T00:01:00Z"),
            ]),
        );
        assert_eq!(touches[0].title, "Owner titled");
        assert_eq!(touches[0].issue_type, "phase");
    }

    #[test]
    fn globalized_and_bare_local_actors_stay_distinct_and_trimmed() {
        let touches = reduce_stream_touches(
            "b-1",
            &lines(&[
                created(OWNER, "b-1", "Bead", "task", "2026-01-01T00:00:00Z"),
                note("013", "b-1", "2026-01-01T00:01:00Z"),
                note(AGENT_A, "b-1", "2026-01-01T00:02:00Z"),
                note("  bbugyi200.athena.0aa  ", "b-1", "2026-01-01T00:03:00Z"),
                note("sase-zt.6.5.land", "b-1", "2026-01-01T00:04:00Z"),
            ]),
        );
        let actors: Vec<_> =
            touches.iter().map(|touch| touch.actor.as_str()).collect();
        assert_eq!(actors, vec!["013", AGENT_A, "sase-zt.6.5.land"]);
        assert_eq!(
            touch_for(&touches, "b-1", AGENT_A).verbs,
            verbs(&[("noted", 2)])
        );
    }

    #[test]
    fn excluded_and_unknown_operations_contribute_no_touch() {
        let touches = reduce_stream_touches(
            "b-1",
            &lines(&[
                created(OWNER, "b-1", "Bead", "task", "2026-01-01T00:00:00Z"),
                bare(
                    AGENT_A,
                    "epic_work_preclaimed",
                    "b-1",
                    "2026-01-01T00:01:00Z",
                ),
                bare(
                    AGENT_A,
                    "brand_new_operation",
                    "b-1",
                    "2026-01-01T00:02:00Z",
                ),
            ]),
        );
        assert_eq!(touches, Vec::new());
    }

    #[test]
    fn malformed_lines_and_shapes_are_skipped_not_fatal() {
        let mut bad_kind = event(
            AGENT_B,
            "note_appended",
            "b-1",
            "2026-01-01T00:01:00Z",
            json!({}),
        );
        bad_kind = bad_kind
            .replace("\"kind\":\"note_appended\"", "\"kind\":\"note_edited\"");
        let text = format!(
            "{}\nnot json at all\n{{\"truncated\":\n[1,2,3]\n\n{bad_kind}\n{}\n{}\n{}\n{}\n",
            created(OWNER, "b-1", "Bead", "task", "2026-01-01T00:00:00Z"),
            // Missing payload, empty issue id, bad timestamp.
            json!({"timestamp": "2026-01-01T00:02:00Z", "actor": AGENT_B,
                   "operation": "note_appended", "issue_id": "b-1"}),
            note(AGENT_B, "", "2026-01-01T00:03:00Z"),
            note(AGENT_B, "b-1", "yesterday"),
            note(AGENT_A, "b-1", "2026-01-01T00:04:00Z"),
        );
        let touches = reduce_stream_touches("b-1", &text);
        assert_eq!(touches.len(), 1, "{touches:?}");
        assert_eq!(touches[0].actor, AGENT_A);
    }

    #[test]
    fn a_stream_that_never_mentions_its_own_root_is_skipped() {
        let touches = reduce_stream_touches(
            "copied",
            &lines(&[
                created(OWNER, "b-1", "Bead", "task", "2026-01-01T00:00:00Z"),
                note(AGENT_A, "b-1", "2026-01-01T00:01:00Z"),
            ]),
        );
        assert_eq!(touches, Vec::new());
    }

    #[test]
    fn a_plan_stream_attributes_touches_to_child_beads() {
        let touches = reduce_stream_touches(
            "p-1",
            &lines(&[
                created(OWNER, "p-1", "Plan", "plan", "2026-01-01T00:00:00Z"),
                created(
                    OWNER,
                    "p-1.1",
                    "Phase",
                    "phase",
                    "2026-01-01T00:01:00Z",
                ),
                note(AGENT_A, "p-1.1", "2026-01-01T00:02:00Z"),
            ]),
        );
        assert_eq!(touches.len(), 1);
        assert_eq!(touches[0].bead_id, "p-1.1");
        assert_eq!(touches[0].title, "Phase");
        assert_eq!(touches[0].stream_id, "p-1");
    }

    #[test]
    fn empty_store_writes_an_empty_index_and_queries_nothing() {
        let store = Store::new();
        let outcome = store.refresh();
        assert!(outcome.full_rebuild && outcome.wrote);
        assert_eq!(outcome.stream_count, 0);
        assert_eq!(outcome.touch_count, 0);
        let query = store.query(None);
        assert_eq!(query.touches, Vec::new());
        assert_eq!(query.generation, outcome.generation);

        fs::create_dir_all(event_streams_dir(&store.beads_dir)).unwrap();
        let again = store.refresh();
        assert!(!again.wrote && !again.full_rebuild);
    }

    #[test]
    fn refresh_ignores_non_jsonl_files_and_directories() {
        let store = Store::new();
        store.write_stream(
            "b-1",
            &[created(
                OWNER,
                "b-1",
                "Bead",
                "task",
                "2026-01-01T00:00:00Z",
            )],
        );
        let dir = event_streams_dir(&store.beads_dir);
        fs::write(dir.join("notes.txt"), "x").unwrap();
        fs::write(dir.join("b-1.jsonl.tmp"), "x").unwrap();
        fs::create_dir(dir.join("dir.jsonl")).unwrap();
        assert_eq!(store.refresh().stream_count, 1);
    }

    #[test]
    fn unchanged_streams_reuse_cached_rows_and_skip_the_write() {
        let store = Store::new();
        store.write_stream(
            "b-1",
            &[
                created(OWNER, "b-1", "Bead", "task", "2026-01-01T00:00:00Z"),
                note(AGENT_A, "b-1", "2026-01-01T00:01:00Z"),
            ],
        );
        let first = store.refresh();
        assert_eq!(first.reduced_streams, vec!["b-1"]);

        let mut index = store.read_index();
        index.touches[0].title = "SENTINEL".to_string();
        store.write_index(&index);
        let before =
            fs::metadata(&store.index_path).unwrap().modified().unwrap();

        let second = store.refresh();
        assert!(!second.full_rebuild);
        assert!(!second.wrote);
        assert_eq!(second.reduced_streams, Vec::<String>::new());
        assert_eq!(second.reused_streams, 1);
        assert_eq!(second.generation, first.generation);
        assert_eq!(store.query(None).touches[0].title, "SENTINEL");
        assert_eq!(
            fs::metadata(&store.index_path).unwrap().modified().unwrap(),
            before
        );
    }

    #[test]
    fn a_changed_stream_is_re_reduced_and_siblings_are_reused() {
        let store = Store::new();
        store.write_stream(
            "b-1",
            &[
                created(OWNER, "b-1", "One", "task", "2026-01-01T00:00:00Z"),
                note(AGENT_A, "b-1", "2026-01-01T00:01:00Z"),
            ],
        );
        store.write_stream(
            "b-2",
            &[
                created(OWNER, "b-2", "Two", "task", "2026-01-01T00:00:00Z"),
                note(AGENT_A, "b-2", "2026-01-01T00:01:00Z"),
            ],
        );
        store.refresh();
        let mut index = store.read_index();
        for touch in &mut index.touches {
            if touch.stream_id == "b-2" {
                touch.title = "SENTINEL".to_string();
            }
        }
        store.write_index(&index);

        store.append_stream(
            "b-1",
            &note(AGENT_A, "b-1", "2026-01-01T00:09:00Z"),
        );
        let outcome = store.refresh();

        assert_eq!(outcome.reduced_streams, vec!["b-1"]);
        assert_eq!(outcome.reused_streams, 1);
        assert!(outcome.wrote);
        let touches = store.query(None).touches;
        assert_eq!(
            touch_for(&touches, "b-1", AGENT_A).verbs,
            verbs(&[("noted", 2)])
        );
        assert_eq!(
            touch_for(&touches, "b-1", AGENT_A).last_at,
            "2026-01-01T00:09:00Z"
        );
        assert_eq!(touch_for(&touches, "b-2", AGENT_A).title, "SENTINEL");
    }

    #[test]
    fn new_and_vanished_streams_update_the_index() {
        let store = Store::new();
        store.write_stream(
            "b-1",
            &[
                created(OWNER, "b-1", "One", "task", "2026-01-01T00:00:00Z"),
                note(AGENT_A, "b-1", "2026-01-01T00:01:00Z"),
            ],
        );
        store.write_stream(
            "b-2",
            &[
                created(OWNER, "b-2", "Two", "task", "2026-01-01T00:00:00Z"),
                note(AGENT_A, "b-2", "2026-01-01T00:01:00Z"),
            ],
        );
        store.refresh();

        fs::remove_file(store.stream_path("b-2")).unwrap();
        store.write_stream(
            "b-3",
            &[
                created(OWNER, "b-3", "Three", "task", "2026-01-01T00:00:00Z"),
                note(AGENT_B, "b-3", "2026-01-01T00:02:00Z"),
            ],
        );
        let outcome = store.refresh();

        assert_eq!(outcome.removed_streams, vec!["b-2"]);
        assert_eq!(outcome.reduced_streams, vec!["b-3"]);
        assert_eq!(outcome.reused_streams, 1);
        let beads: Vec<_> = store
            .query(None)
            .touches
            .into_iter()
            .map(|touch| touch.bead_id)
            .collect();
        assert_eq!(beads, vec!["b-3", "b-1"]);
    }

    #[test]
    fn a_stream_that_stops_belonging_to_its_filename_is_dropped() {
        let store = Store::new();
        store.write_stream(
            "b-1",
            &[
                created(OWNER, "b-1", "One", "task", "2026-01-01T00:00:00Z"),
                note(AGENT_A, "b-1", "2026-01-01T00:01:00Z"),
            ],
        );
        store.refresh();
        store.write_stream(
            "b-1",
            &[
                created(
                    OWNER,
                    "other",
                    "Other",
                    "task",
                    "2026-01-01T00:00:00Z",
                ),
                note(AGENT_A, "other", "2026-01-01T00:01:00Z"),
            ],
        );
        let outcome = store.refresh();
        assert!(outcome.wrote);
        assert_eq!(outcome.reduced_streams, vec!["b-1"]);
        assert_eq!(outcome.touch_count, 0);
        assert_eq!(store.query(None).touches, Vec::new());
    }

    #[test]
    fn a_schema_version_bump_is_a_cache_miss_and_forces_a_full_rebuild() {
        let store = Store::new();
        store.write_stream(
            "b-1",
            &[
                created(OWNER, "b-1", "One", "task", "2026-01-01T00:00:00Z"),
                note(AGENT_A, "b-1", "2026-01-01T00:01:00Z"),
            ],
        );
        store.refresh();
        let mut index = store.read_index();
        index.schema_version = BEAD_TOUCH_INDEX_WIRE_SCHEMA_VERSION + 1;
        index.touches[0].title = "SENTINEL".to_string();
        store.write_index(&index);

        assert_eq!(store.query(None).touches, Vec::new());
        assert_eq!(
            store.status().state,
            BeadTouchIndexStateWire::SchemaMismatch
        );
        assert_eq!(
            store.status().index_schema_version,
            Some(BEAD_TOUCH_INDEX_WIRE_SCHEMA_VERSION + 1)
        );

        let outcome = store.refresh();
        assert!(outcome.full_rebuild && outcome.wrote);
        assert_eq!(outcome.reduced_streams, vec!["b-1"]);
        let touches = store.query(None).touches;
        assert_eq!(touches.len(), 1);
        assert_eq!(touches[0].title, "One");
        assert_eq!(
            store.read_index().schema_version,
            BEAD_TOUCH_INDEX_WIRE_SCHEMA_VERSION
        );
    }

    #[test]
    fn missing_truncated_and_garbage_indexes_are_cache_misses_never_errors() {
        let store = Store::new();
        store.write_stream(
            "b-1",
            &[
                created(OWNER, "b-1", "One", "task", "2026-01-01T00:00:00Z"),
                note(AGENT_A, "b-1", "2026-01-01T00:01:00Z"),
            ],
        );

        assert_eq!(store.query(None).touches, Vec::new());
        assert_eq!(store.query(None).generation, "");
        assert_eq!(store.status().state, BeadTouchIndexStateWire::Missing);

        store.refresh();
        let full = fs::read(&store.index_path).unwrap();
        let wrong_shape = serde_json::to_vec(&json!({
            "schema_version": BEAD_TOUCH_INDEX_WIRE_SCHEMA_VERSION,
            "touches": 7,
        }))
        .unwrap();
        for corrupt in [
            full[..full.len() / 2].to_vec(),
            b"not json".to_vec(),
            b"{}".to_vec(),
            b"[]".to_vec(),
            wrong_shape,
            Vec::new(),
        ] {
            fs::write(&store.index_path, &corrupt).unwrap();
            assert_eq!(store.query(None).touches, Vec::new(), "{corrupt:?}");
            assert_eq!(
                store.status().state,
                BeadTouchIndexStateWire::Unreadable
            );
            assert!(store.refresh().full_rebuild);
            assert_eq!(store.query(None).touches.len(), 1);
        }
    }

    #[test]
    fn query_filters_by_exact_trimmed_actor_and_orders_newest_first() {
        let store = Store::new();
        store.write_stream(
            "b-1",
            &[
                created(OWNER, "b-1", "One", "task", "2026-01-01T00:00:00Z"),
                note(AGENT_A, "b-1", "2026-01-01T00:01:00Z"),
                note(AGENT_B, "b-1", "2026-01-01T00:05:00Z"),
            ],
        );
        store.write_stream(
            "b-2",
            &[
                created(OWNER, "b-2", "Two", "task", "2026-01-01T00:00:00Z"),
                note(AGENT_A, "b-2", "2026-01-01T00:03:00Z"),
            ],
        );
        store.refresh();

        let only_a = store.query(Some(&[format!("  {AGENT_A} ")]));
        let order: Vec<_> =
            only_a.touches.iter().map(|t| t.bead_id.as_str()).collect();
        assert_eq!(order, vec!["b-2", "b-1"]);
        assert!(only_a.touches.iter().all(|t| t.actor == AGENT_A));

        assert_eq!(store.query(None).touches.len(), 3);
        assert_eq!(store.query(Some(&[])).touches, Vec::new());
        // No prefix or suffix matching.
        assert_eq!(
            store
                .query(Some(&[
                    "bbugyi200.athena".to_string(),
                    "0aa".to_string()
                ]))
                .touches,
            Vec::new()
        );
    }

    #[test]
    fn status_classifies_fresh_stale_and_vanished() {
        let store = Store::new();
        store.write_stream(
            "b-1",
            &[
                created(OWNER, "b-1", "One", "task", "2026-01-01T00:00:00Z"),
                note(AGENT_A, "b-1", "2026-01-01T00:01:00Z"),
            ],
        );
        store.write_stream(
            "b-2",
            &[created(OWNER, "b-2", "Two", "task", "2026-01-01T00:00:00Z")],
        );
        let missing = store.status();
        assert_eq!(missing.state, BeadTouchIndexStateWire::Missing);
        assert_eq!(missing.current_streams, 2);

        let outcome = store.refresh();
        let fresh = store.status();
        assert_eq!(fresh.state, BeadTouchIndexStateWire::Fresh);
        assert_eq!(fresh.generation, outcome.generation);
        assert_eq!((fresh.indexed_streams, fresh.current_streams), (2, 2));

        store.append_stream(
            "b-1",
            &note(AGENT_A, "b-1", "2026-01-01T00:09:00Z"),
        );
        fs::remove_file(store.stream_path("b-2")).unwrap();
        store.write_stream(
            "b-3",
            &[created(
                OWNER,
                "b-3",
                "Three",
                "task",
                "2026-01-01T00:00:00Z",
            )],
        );
        let stale = store.status();
        assert_eq!(stale.state, BeadTouchIndexStateWire::Stale);
        assert_eq!(stale.changed_streams, vec!["b-1", "b-3"]);
        assert_eq!(stale.vanished_streams, vec!["b-2"]);

        store.refresh();
        assert_eq!(store.status().state, BeadTouchIndexStateWire::Fresh);
    }

    #[test]
    fn stale_index_still_answers_without_touching_the_streams() {
        let store = Store::new();
        store.write_stream(
            "b-1",
            &[
                created(OWNER, "b-1", "One", "task", "2026-01-01T00:00:00Z"),
                note(AGENT_A, "b-1", "2026-01-01T00:01:00Z"),
            ],
        );
        store.refresh();
        fs::remove_dir_all(&store.beads_dir).unwrap();
        assert_eq!(store.query(None).touches.len(), 1);
    }

    #[test]
    fn index_file_shape_matches_the_documented_contract() {
        let store = Store::new();
        store.write_stream(
            "b-1",
            &[
                created(OWNER, "b-1", "One", "task", "2026-01-01T00:00:00Z"),
                note(AGENT_A, "b-1", "2026-01-01T00:01:00Z"),
            ],
        );
        let outcome = store.refresh();
        let raw: Value =
            serde_json::from_slice(&fs::read(&store.index_path).unwrap())
                .unwrap();
        assert_eq!(
            raw["schema_version"],
            json!(BEAD_TOUCH_INDEX_WIRE_SCHEMA_VERSION)
        );
        assert_eq!(raw["generation"], json!(outcome.generation));
        let signature = raw["streams"]["b-1"].as_array().unwrap();
        assert_eq!(signature.len(), 2);
        assert!(signature[0].as_i64().unwrap() > 0);
        assert!(signature[1].as_u64().unwrap() > 0);
        assert_eq!(
            raw["touches"][0],
            json!({
                "actor": AGENT_A,
                "bead_id": "b-1",
                "title": "One",
                "issue_type": "task",
                "status": "open",
                "verbs": {"noted": 1},
                "first_at": "2026-01-01T00:01:00Z",
                "last_at": "2026-01-01T00:01:00Z",
                "current_note_count": 1,
                "note_preview": {
                    "id": "b-1:note_appended:2026-01-01T00:01:00Z",
                    "author": AGENT_A,
                    "timestamp": "2026-01-01T00:01:00Z",
                    "text": "text",
                    "truncated": false,
                },
                "stream_id": "b-1",
            })
        );
    }

    #[test]
    fn note_preview_covers_append_and_selects_the_newest() {
        let touches = reduce_stream_touches(
            "b-1",
            &lines(&[
                created(OWNER, "b-1", "Bead", "task", "2026-01-01T00:00:00Z"),
                note_entry(AGENT_A, "b-1", "2026-01-01T00:01:00Z", "first"),
                note_entry(AGENT_A, "b-1", "2026-01-01T00:02:00Z", "second"),
            ]),
        );
        let touch = touch_for(&touches, "b-1", AGENT_A);
        assert_eq!(touch.current_note_count, 2);
        let preview = touch.note_preview.as_ref().expect("preview");
        assert_eq!(preview.id, append_id("b-1", "2026-01-01T00:02:00Z"));
        assert_eq!(preview.author, AGENT_A);
        assert_eq!(preview.timestamp, "2026-01-01T00:02:00Z");
        assert_eq!(preview.text, "second");
        assert_eq!(preview.edited_at, None);
        assert_eq!(preview.edited_by, None);
        assert!(!preview.truncated);
        // Verbs and timestamps are untouched by the preview replay.
        assert_eq!(touch.verbs, verbs(&[("noted", 2)]));
        assert_eq!(touch.first_at, "2026-01-01T00:01:00Z");
        assert_eq!(touch.last_at, "2026-01-01T00:02:00Z");
    }

    #[test]
    fn note_preview_tie_breaks_on_stable_id() {
        let touches = reduce_stream_touches(
            "b-1",
            &lines(&[
                created(OWNER, "b-1", "Bead", "task", "2026-01-01T00:00:00Z"),
                note_with_id(
                    "n-b",
                    AGENT_A,
                    "b-1",
                    "2026-01-01T00:01:00Z",
                    "bravo",
                ),
                note_with_id(
                    "n-a",
                    AGENT_A,
                    "b-1",
                    "2026-01-01T00:01:00Z",
                    "alpha",
                ),
            ]),
        );
        let touch = touch_for(&touches, "b-1", AGENT_A);
        assert_eq!(touch.current_note_count, 2);
        // Same append instant: the larger stable ID wins, deterministically.
        assert_eq!(touch.note_preview.as_ref().unwrap().id, "n-b");
        assert_eq!(touch.note_preview.as_ref().unwrap().text, "bravo");
    }

    #[test]
    fn note_previews_stay_scoped_to_author_and_bead() {
        let touches = reduce_stream_touches(
            "b-1",
            &lines(&[
                created(OWNER, "b-1", "Bead", "task", "2026-01-01T00:00:00Z"),
                created(OWNER, "b-2", "Other", "task", "2026-01-01T00:00:00Z"),
                note_entry(AGENT_A, "b-1", "2026-01-01T00:01:00Z", "a-note"),
                note_entry(AGENT_B, "b-1", "2026-01-01T00:02:00Z", "b-note"),
                note_entry(AGENT_A, "b-2", "2026-01-01T00:03:00Z", "a-other"),
            ]),
        );
        let a1 = touch_for(&touches, "b-1", AGENT_A);
        assert_eq!(a1.current_note_count, 1);
        assert_eq!(a1.note_preview.as_ref().unwrap().text, "a-note");
        let b1 = touch_for(&touches, "b-1", AGENT_B);
        assert_eq!(b1.current_note_count, 1);
        assert_eq!(b1.note_preview.as_ref().unwrap().text, "b-note");
        assert_eq!(b1.note_preview.as_ref().unwrap().author, AGENT_B);
    }

    #[test]
    fn edit_by_another_actor_updates_text_without_moving_authorship() {
        let first_id = append_id("b-1", "2026-01-01T00:01:00Z");
        let touches = reduce_stream_touches(
            "b-1",
            &lines(&[
                created(OWNER, "b-1", "Bead", "task", "2026-01-01T00:00:00Z"),
                note_entry(AGENT_A, "b-1", "2026-01-01T00:01:00Z", "original"),
                note_edited(
                    AGENT_B,
                    "b-1",
                    "2026-01-01T00:02:00Z",
                    &first_id,
                    "revised",
                ),
            ]),
        );
        let authored = touch_for(&touches, "b-1", AGENT_A);
        assert_eq!(authored.current_note_count, 1);
        let preview = authored.note_preview.as_ref().expect("preview");
        assert_eq!(preview.id, first_id);
        assert_eq!(preview.author, AGENT_A);
        assert_eq!(preview.timestamp, "2026-01-01T00:01:00Z");
        assert_eq!(preview.text, "revised");
        assert_eq!(preview.edited_at.as_deref(), Some("2026-01-01T00:02:00Z"));
        assert_eq!(preview.edited_by.as_deref(), Some(AGENT_B));
        // The editor still earns its noted verb, but gains no preview from
        // someone else's note.
        let editor = touch_for(&touches, "b-1", AGENT_B);
        assert_eq!(editor.verbs, verbs(&[("noted", 1)]));
        assert_eq!(editor.current_note_count, 0);
        assert_eq!(editor.note_preview, None);
    }

    #[test]
    fn removal_falls_back_to_the_previous_note_then_to_nothing() {
        let first_id = append_id("b-1", "2026-01-01T00:01:00Z");
        let second_id = append_id("b-1", "2026-01-01T00:02:00Z");
        let base = [
            created(OWNER, "b-1", "Bead", "task", "2026-01-01T00:00:00Z"),
            note_entry(AGENT_A, "b-1", "2026-01-01T00:01:00Z", "first"),
            note_entry(AGENT_A, "b-1", "2026-01-01T00:02:00Z", "second"),
        ];
        let touches = reduce_stream_touches("b-1", &lines(&base));
        assert_eq!(
            touch_for(&touches, "b-1", AGENT_A)
                .note_preview
                .as_ref()
                .unwrap()
                .id,
            second_id
        );

        let mut removed_second = base.to_vec();
        removed_second.push(note_removed(
            AGENT_A,
            "b-1",
            "2026-01-01T00:03:00Z",
            &second_id,
        ));
        let touches = reduce_stream_touches("b-1", &lines(&removed_second));
        let touch = touch_for(&touches, "b-1", AGENT_A);
        assert_eq!(touch.current_note_count, 1);
        assert_eq!(touch.note_preview.as_ref().unwrap().id, first_id);
        assert_eq!(touch.note_preview.as_ref().unwrap().text, "first");
        // Removal counts as a noted action even as the preview shrinks.
        assert_eq!(touch.verbs, verbs(&[("noted", 3)]));

        let mut removed_both = removed_second.clone();
        removed_both.push(note_removed(
            AGENT_B,
            "b-1",
            "2026-01-01T00:04:00Z",
            &first_id,
        ));
        let touches = reduce_stream_touches("b-1", &lines(&removed_both));
        let touch = touch_for(&touches, "b-1", AGENT_A);
        assert_eq!(touch.current_note_count, 0);
        assert_eq!(touch.note_preview, None);
    }

    #[test]
    fn unknown_edit_and_remove_ids_leave_previews_alone() {
        let touches = reduce_stream_touches(
            "b-1",
            &lines(&[
                created(OWNER, "b-1", "Bead", "task", "2026-01-01T00:00:00Z"),
                note_entry(AGENT_A, "b-1", "2026-01-01T00:01:00Z", "kept"),
                note_edited(
                    AGENT_A,
                    "b-1",
                    "2026-01-01T00:02:00Z",
                    "missing",
                    "elsewhere",
                ),
                note_removed(AGENT_A, "b-1", "2026-01-01T00:03:00Z", "missing"),
            ]),
        );
        let touch = touch_for(&touches, "b-1", AGENT_A);
        assert_eq!(touch.current_note_count, 1);
        assert_eq!(touch.note_preview.as_ref().unwrap().text, "kept");
        // Unknown IDs still count their noted verbs.
        assert_eq!(touch.verbs, verbs(&[("noted", 3)]));
    }

    #[test]
    fn non_agent_and_blank_appends_never_produce_previews() {
        let touches = reduce_stream_touches(
            "b-1",
            &lines(&[
                created(OWNER, "b-1", "Bead", "task", "2026-01-01T00:00:00Z"),
                note_entry(OWNER, "b-1", "2026-01-01T00:01:00Z", "owner note"),
                note_entry(
                    "has space",
                    "b-1",
                    "2026-01-01T00:02:00Z",
                    "bad actor",
                ),
                note_entry(AGENT_A, "b-1", "2026-01-01T00:03:00Z", "   "),
                note_entry(AGENT_A, "b-1", "2026-01-01T00:04:00Z", "real"),
            ]),
        );
        let touch = touch_for(&touches, "b-1", AGENT_A);
        assert_eq!(touch.current_note_count, 1);
        assert_eq!(touch.note_preview.as_ref().unwrap().text, "real");
        // Blank appends still earn verbs; human appends never become rows.
        assert_eq!(touch.verbs, verbs(&[("noted", 2)]));
        assert!(touches.iter().all(|touch| touch.actor != OWNER));
    }

    #[test]
    fn malformed_note_payloads_keep_verbs_but_skip_previews() {
        let text = lines(&[
            created(OWNER, "b-1", "Bead", "task", "2026-01-01T00:00:00Z"),
            // Non-string entry: verb without a preview.
            event(
                AGENT_A,
                "note_appended",
                "b-1",
                "2026-01-01T00:01:00Z",
                json!({"entry": 7}),
            ),
            // Blank edit text: preview effect skipped.
            note_edited(
                AGENT_A,
                "b-1",
                "2026-01-01T00:02:00Z",
                &append_id("b-1", "2026-01-01T00:01:00Z"),
                "   ",
            ),
            // Missing note_id: preview effect skipped.
            event(
                AGENT_A,
                "note_removed",
                "b-1",
                "2026-01-01T00:03:00Z",
                json!({}),
            ),
        ]);
        let touches = reduce_stream_touches("b-1", &text);
        let touch = touch_for(&touches, "b-1", AGENT_A);
        assert_eq!(touch.verbs, verbs(&[("noted", 3)]));
        assert_eq!(touch.current_note_count, 0);
        assert_eq!(touch.note_preview, None);
    }

    #[test]
    fn long_and_unicode_note_text_truncates_on_char_boundaries() {
        let long = "é".repeat(NOTE_PREVIEW_TEXT_LIMIT + 5);
        let touches = reduce_stream_touches(
            "b-1",
            &lines(&[
                created(OWNER, "b-1", "Bead", "task", "2026-01-01T00:00:00Z"),
                note_entry(AGENT_A, "b-1", "2026-01-01T00:01:00Z", &long),
            ]),
        );
        let preview = touch_for(&touches, "b-1", AGENT_A)
            .note_preview
            .clone()
            .unwrap();
        assert!(preview.truncated);
        assert_eq!(preview.text.chars().count(), NOTE_PREVIEW_TEXT_LIMIT);
        assert_eq!(preview.text, "é".repeat(NOTE_PREVIEW_TEXT_LIMIT));

        let exact = "x".repeat(NOTE_PREVIEW_TEXT_LIMIT);
        let touches = reduce_stream_touches(
            "b-1",
            &lines(&[
                created(OWNER, "b-1", "Bead", "task", "2026-01-01T00:00:00Z"),
                note_entry(AGENT_A, "b-1", "2026-01-01T00:01:00Z", &exact),
            ]),
        );
        let preview = touch_for(&touches, "b-1", AGENT_A)
            .note_preview
            .clone()
            .unwrap();
        assert!(!preview.truncated);
        assert_eq!(preview.text, exact);
    }

    #[test]
    fn a_schema_one_index_is_a_miss_until_the_rebuild() {
        let store = Store::new();
        store.write_stream(
            "b-1",
            &[
                created(OWNER, "b-1", "One", "task", "2026-01-01T00:00:00Z"),
                note_entry(AGENT_A, "b-1", "2026-01-01T00:01:00Z", "hello"),
            ],
        );
        store.refresh();
        // Hand back the file to the pre-preview schema: the query must miss
        // and the next refresh must rebuild with the preview attached.
        let raw = fs::read(&store.index_path).unwrap();
        let mut value: Value = serde_json::from_slice(&raw).unwrap();
        value["schema_version"] = json!(1);
        if let Some(touches) =
            value.get_mut("touches").and_then(Value::as_array_mut)
        {
            for touch in touches {
                touch.as_object_mut().unwrap().remove("current_note_count");
                touch.as_object_mut().unwrap().remove("note_preview");
            }
        }
        fs::write(&store.index_path, serde_json::to_vec(&value).unwrap())
            .unwrap();

        assert_eq!(store.query(None).touches, Vec::new());
        assert_eq!(
            store.status().state,
            BeadTouchIndexStateWire::SchemaMismatch
        );
        assert_eq!(store.status().index_schema_version, Some(1));

        let outcome = store.refresh();
        assert!(outcome.full_rebuild && outcome.wrote);
        let query = store.query(None);
        let touch = touch_for(&query.touches, "b-1", AGENT_A);
        assert_eq!(touch.current_note_count, 1);
        assert_eq!(touch.note_preview.as_ref().unwrap().text, "hello");
    }

    #[test]
    fn refresh_fails_loudly_on_a_held_lock() {
        let store = Store::new();
        fs::create_dir_all(store.index_path.parent().unwrap()).unwrap();
        let lock_path = index_lock_path(&store.index_path);
        let _held = acquire_store_lock(
            &lock_path,
            &holder_path_for(&lock_path),
            LockMode::Exclusive,
            Duration::from_secs(1),
            "test holder",
        )
        .unwrap();
        // Same-process flock on a second descriptor conflicts, so the bounded
        // wait must time out into a conflict error rather than hanging.
        std::env::set_var(LOCK_TIMEOUT_ENV, "0.05");
        let error =
            refresh_bead_touch_index(&store.beads_dir, &store.index_path)
                .unwrap_err();
        std::env::remove_var(LOCK_TIMEOUT_ENV);
        assert_eq!(error.kind, "conflict");
    }

    #[test]
    fn close_credits_the_closed_by_closer_never_the_creator() {
        let touches = reduce_stream_touches(
            "b-1",
            &lines(&[
                created(AGENT_A, "b-1", "Bead", "task", "2026-01-01T00:00:00Z"),
                event(
                    AGENT_B,
                    "issue_closed",
                    "b-1",
                    "2026-01-01T00:01:00Z",
                    json!({
                        "closed_by": AGENT_B,
                        "close_reason": "verified",
                        "resolution": "done",
                    }),
                ),
            ]),
        );
        let closer = touch_for(&touches, "b-1", AGENT_B);
        assert_eq!(closer.verbs, verbs(&[("closed", 1)]));
        let close = closer.close.clone().expect("close record");
        assert_eq!(close.closed_at, "2026-01-01T00:01:00Z");
        assert_eq!(close.resolution, "done");
        assert_eq!(close.reason.as_deref(), Some("verified"));
        assert!(close.standing);
        // The creator keeps its origin verb but earns no close.
        let creator = touch_for(&touches, "b-1", AGENT_A);
        assert_eq!(creator.verbs, verbs(&[("created", 1)]));
        assert_eq!(creator.close, None);
    }

    #[test]
    fn human_closed_by_produces_no_touch() {
        let touches = reduce_stream_touches(
            "b-1",
            &lines(&[
                created(OWNER, "b-1", "Bead", "task", "2026-01-01T00:00:00Z"),
                event(
                    OWNER,
                    "issue_closed",
                    "b-1",
                    "2026-01-01T00:01:00Z",
                    json!({"closed_by": OWNER}),
                ),
                note(AGENT_A, "b-1", "2026-01-01T00:02:00Z"),
            ]),
        );
        // Only the agent's note touch remains; the human close credits
        // nobody and leaves no close record behind.
        assert_eq!(touches.len(), 1);
        assert_eq!(touches[0].actor, AGENT_A);
        assert_eq!(touches[0].verbs, verbs(&[("noted", 1)]));
        assert_eq!(touches[0].close, None);
    }

    #[test]
    fn legacy_close_credits_only_the_unique_same_instant_note_author() {
        // One valid agent appended a note at the close instant.
        let touches = reduce_stream_touches(
            "b-1",
            &lines(&[
                created(OWNER, "b-1", "Bead", "task", "2026-01-01T00:00:00Z"),
                note(AGENT_B, "b-1", "2026-01-01T00:01:00Z"),
                bare(AGENT_A, "issue_closed", "b-1", "2026-01-01T00:01:00Z"),
            ]),
        );
        let closer = touch_for(&touches, "b-1", AGENT_B);
        assert_eq!(closer.verbs.get("closed"), Some(&1));
        assert!(closer.close.clone().is_some_and(|close| close.standing));
        assert!(touches.iter().all(|touch| touch.actor != AGENT_A));

        // Two different authors at the same instant are ambiguous.
        let touches = reduce_stream_touches(
            "b-2",
            &lines(&[
                created(OWNER, "b-2", "Bead", "task", "2026-01-01T00:00:00Z"),
                note_with_id(
                    "n-a",
                    AGENT_A,
                    "b-2",
                    "2026-01-01T00:01:00Z",
                    "alpha",
                ),
                note_with_id(
                    "n-b",
                    AGENT_B,
                    "b-2",
                    "2026-01-01T00:01:00Z",
                    "bravo",
                ),
                bare(OWNER, "issue_closed", "b-2", "2026-01-01T00:01:00Z"),
            ]),
        );
        for touch in &touches {
            assert!(!touch.verbs.contains_key("closed"), "{touch:?}");
            assert_eq!(touch.close, None);
        }

        // No note at the instant: the envelope actor is never a fallback.
        let touches = reduce_stream_touches(
            "b-3",
            &lines(&[
                created(OWNER, "b-3", "Bead", "task", "2026-01-01T00:00:00Z"),
                note(AGENT_B, "b-3", "2026-01-01T00:02:00Z"),
                bare(AGENT_A, "issue_closed", "b-3", "2026-01-01T00:01:00Z"),
            ]),
        );
        for touch in &touches {
            assert!(!touch.verbs.contains_key("closed"), "{touch:?}");
            assert_eq!(touch.close, None);
        }
    }

    #[test]
    fn close_record_reports_resolution_reason_and_standing() {
        let touches = reduce_stream_touches(
            "b-1",
            &lines(&[
                created(OWNER, "b-1", "Bead", "task", "2026-01-01T00:00:00Z"),
                event(
                    AGENT_A,
                    "issue_closed",
                    "b-1",
                    "2026-01-01T00:01:00Z",
                    json!({
                        "closed_by": AGENT_A,
                        "resolution": "canceled",
                        "close_reason": "  duplicate  ",
                    }),
                ),
            ]),
        );
        let close = touch_for(&touches, "b-1", AGENT_A)
            .close
            .clone()
            .expect("close");
        assert_eq!(close.closed_at, "2026-01-01T00:01:00Z");
        assert_eq!(close.resolution, "canceled");
        assert_eq!(close.reason.as_deref(), Some("duplicate"));
        assert!(close.standing);

        // A missing resolution reads as done; a blank reason reads as none.
        let touches = reduce_stream_touches(
            "b-2",
            &lines(&[
                created(OWNER, "b-2", "Bead", "task", "2026-01-01T00:00:00Z"),
                event(
                    AGENT_A,
                    "issue_closed",
                    "b-2",
                    "2026-01-01T00:01:00Z",
                    json!({"closed_by": AGENT_A, "close_reason": "   "}),
                ),
            ]),
        );
        let close = touch_for(&touches, "b-2", AGENT_A)
            .close
            .clone()
            .expect("close");
        assert_eq!(close.resolution, "done");
        assert_eq!(close.reason, None);
        assert!(close.standing);
    }

    #[test]
    fn reopened_close_loses_standing_and_the_next_closer_wins() {
        let touches = reduce_stream_touches(
            "b-1",
            &lines(&[
                created(OWNER, "b-1", "Bead", "task", "2026-01-01T00:00:00Z"),
                event(
                    AGENT_A,
                    "issue_closed",
                    "b-1",
                    "2026-01-01T00:01:00Z",
                    json!({"closed_by": AGENT_A}),
                ),
                bare(AGENT_A, "issue_opened", "b-1", "2026-01-01T00:02:00Z"),
                event(
                    AGENT_B,
                    "issue_closed",
                    "b-1",
                    "2026-01-01T00:03:00Z",
                    json!({"closed_by": AGENT_B}),
                ),
            ]),
        );
        let first = touch_for(&touches, "b-1", AGENT_A)
            .close
            .clone()
            .expect("close");
        assert!(!first.standing);
        let second = touch_for(&touches, "b-1", AGENT_B)
            .close
            .clone()
            .expect("close");
        assert!(second.standing);
        assert_eq!(second.closed_at, "2026-01-01T00:03:00Z");

        // A reopen with no later close leaves the record non-standing.
        let touches = reduce_stream_touches(
            "b-2",
            &lines(&[
                created(OWNER, "b-2", "Bead", "task", "2026-01-01T00:00:00Z"),
                event(
                    AGENT_A,
                    "issue_closed",
                    "b-2",
                    "2026-01-01T00:01:00Z",
                    json!({"closed_by": AGENT_A}),
                ),
                bare(AGENT_B, "issue_opened", "b-2", "2026-01-01T00:02:00Z"),
                note(AGENT_A, "b-2", "2026-01-01T00:03:00Z"),
            ]),
        );
        let touch = touch_for(&touches, "b-2", AGENT_A);
        assert_eq!(touch.status, "open");
        assert!(!touch.close.clone().expect("close").standing);
    }

    #[test]
    fn a_schema_two_index_is_a_miss_until_the_rebuild() {
        let store = Store::new();
        store.write_stream(
            "b-1",
            &[
                created(OWNER, "b-1", "One", "task", "2026-01-01T00:00:00Z"),
                note_entry(AGENT_A, "b-1", "2026-01-01T00:01:00Z", "hello"),
            ],
        );
        store.refresh();
        // Hand back the file to the pre-close schema: the query must miss
        // and the next refresh must rebuild with close support attached.
        let raw = fs::read(&store.index_path).unwrap();
        let mut value: Value = serde_json::from_slice(&raw).unwrap();
        value["schema_version"] = json!(2);
        if let Some(touches) =
            value.get_mut("touches").and_then(Value::as_array_mut)
        {
            for touch in touches {
                touch.as_object_mut().unwrap().remove("close");
            }
        }
        fs::write(&store.index_path, serde_json::to_vec(&value).unwrap())
            .unwrap();

        assert_eq!(store.query(None).touches, Vec::new());
        assert_eq!(
            store.status().state,
            BeadTouchIndexStateWire::SchemaMismatch
        );
        assert_eq!(store.status().index_schema_version, Some(2));

        let outcome = store.refresh();
        assert!(outcome.full_rebuild && outcome.wrote);
        let query = store.query(None);
        assert_eq!(query.touches.len(), 1);
        assert_eq!(
            store.read_index().schema_version,
            BEAD_TOUCH_INDEX_WIRE_SCHEMA_VERSION
        );
    }
}
