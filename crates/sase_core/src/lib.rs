//! Pure-Rust core for the sase ChangeSpec wire contract.
//!
//! Phase 1A models the wire types defined in `sase_100/src/sase/core/wire.py`
//! using owned data only. Phase 1B adds a minimal full-file parser that
//! handles ChangeSpec boundaries and scalar fields; section bodies become
//! structured entries in Phase 1C. The crate is deliberately free of PyO3
//! types so later UniFFI/WASM/server work can reuse the same logic.

pub mod parser;
pub mod wire;

pub use parser::parse_project_bytes;
pub use wire::{
    ChangeSpecWire, CommentWire, CommitWire, DeltaWire, HookStatusLineWire,
    HookWire, MentorStatusLineWire, MentorWire, ParseErrorWire, SourceSpanWire,
    TimestampWire, CHANGESPEC_WIRE_SCHEMA_VERSION,
};
