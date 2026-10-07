/// Append-only bead event wire records and pure reducers.
///
/// Phase 1 keeps this module side-effect free: callers can import legacy
/// `IssueWire` snapshots into deterministic streams, then reduce streams back
/// into the current snapshot model. Later phases own filesystem integration.
///
/// Split from the former 3,314-line `events.rs` along domain seams:
/// [`wire`](wire) owns the event wire records, [`import`](import) owns
/// legacy-snapshot import, [`reduction`](reduction) owns stream reduction
/// back into snapshots, and [`merge`](merge) owns three-way merges and
/// bead-id relocation.
mod import;
mod merge;
mod reduction;
#[cfg(test)]
mod tests;
mod wire;

pub use import::import_issues_to_event_streams;
pub(super) use merge::{
    event_operation_priority, merge_stream_events, mint_bead_event_id,
};
pub use merge::{
    merge_bead_event_streams, merge_bead_event_streams_with_relocation,
    BeadEventStreamMergeWire, BeadIdRelocationKindWire, BeadIdRelocationWire,
};
pub use reduction::reduce_event_streams;
pub(super) use reduction::{
    apply_event, archive_close_metadata, artifact_link_row_from_provenance,
    clear_snooze_record, compare_issues_canonically,
    reduce_parsed_event_streams,
    reduce_parsed_event_streams_with_link_provenance,
    task_plus_one_reopen_decision, ActiveLinkProvenance, StoredLinkIdentity,
    TaskPlusOneReopenDecision,
};
pub(super) use wire::canonical_bead_source_ref;
pub use wire::{
    BeadEventOperationWire, BeadEventPayloadWire, BeadEventRecordWire,
    BeadEventStoreManifestWire, BeadEventStreamWire,
    BeadIssueUpdateEventFieldsWire, BeadSnoozeWakeCauseWire,
    BEAD_EVENT_SCHEMA_VERSION,
};
