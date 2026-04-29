//! Pure-Rust core for the sase ChangeSpec wire contract.
//!
//! Phase 1A modeled the wire types defined in `sase_100/src/sase/core/wire.py`
//! using owned data only. Phase 1B added a minimal full-file parser for
//! ChangeSpec boundaries and scalar fields. Phase 1C adds section parity:
//! structured `COMMITS`, `HOOKS`, `COMMENTS`, `MENTORS`, `TIMESTAMPS`, and
//! `DELTAS` entries that match the Python parser output for the golden
//! corpus. The crate is deliberately free of PyO3 types so later
//! UniFFI/WASM/server work can reuse the same logic.

pub mod parser;
pub mod sections;
pub mod suffix;
pub mod wire;

pub use parser::parse_project_bytes;
pub use suffix::{is_entry_ref_suffix, parse_suffix_prefix, ParsedSuffix};
pub use wire::{
    ChangeSpecWire, CommentWire, CommitWire, DeltaWire, HookStatusLineWire,
    HookWire, MentorStatusLineWire, MentorWire, ParseErrorWire, SourceSpanWire,
    TimestampWire, CHANGESPEC_WIRE_SCHEMA_VERSION,
};
