//! Note-attachment grammar: scan, compose, names, media classification,
//! manifests, tombstones, and policy.

mod extensions;
mod manifest;
mod names;
mod scan;

#[cfg(test)]
mod tests;

pub const NOTE_ATTACHMENT_SCAN_WIRE_SCHEMA_VERSION: u64 = 1;

pub use extensions::{classify_attachment, extension_mime_for};
pub use extensions::{AttachmentClassWire, AttachmentClassificationWire};
pub use manifest::{
    attachment_placement, attachment_sensitive_path_reason,
    attachment_should_auto_fetch, validate_note_attachment_manifest,
    AttachmentImageDimsWire, AttachmentPlacementWire, AttachmentStoreTierWire,
    AttachmentTombstoneWire, BeadAttachmentReferenceWire,
    BeadAttachmentRosterEntryWire, BeadNoteAttachmentWire,
    ATTACHMENT_TOMBSTONE_WIRE_SCHEMA_VERSION,
};
pub use names::{
    is_valid_attachment_name, sanitize_attachment_name, unique_attachment_name,
    AttachmentNameDigestWire, ATTACHMENT_NAME_MAX_LEN,
};
pub use scan::{
    compose_note_attachment_text, note_attachment_source_text,
    scan_note_attachment_refs, stored_attachment_tokens,
    NoteAttachmentBareWordWire, NoteAttachmentDiagnosticWire,
    NoteAttachmentError, NoteAttachmentPathRefWire,
    NoteAttachmentReuseBindingWire, NoteAttachmentReuseRefWire,
    NoteAttachmentScanWire, NoteAttachmentSpanWire, StoredAttachmentTokenWire,
};
