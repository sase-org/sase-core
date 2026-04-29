//! Pure-Rust core for the sase ChangeSpec wire contract.
//!
//! Phase 1A models the wire types defined in `sase_100/src/sase/core/wire.py`
//! using owned data only. No parsing logic lives here yet — that arrives in
//! Phase 1B and 1C. The crate is deliberately free of PyO3 types so later
//! UniFFI/WASM/server work can reuse the same logic.

pub mod wire;

pub use wire::{
    ChangeSpecWire, CommentWire, CommitWire, DeltaWire, HookStatusLineWire, HookWire,
    MentorStatusLineWire, MentorWire, ParseErrorWire, SourceSpanWire, TimestampWire,
    CHANGESPEC_WIRE_SCHEMA_VERSION,
};
