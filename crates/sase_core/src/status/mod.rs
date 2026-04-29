//! Phase 4C: pure-Rust ChangeSpec status state machine.
//!
//! Mirrors the Phase 4B Python wire contract in
//! `sase_100/src/sase/core/status_wire.py` and the pure-Python planner in
//! `sase_100/src/sase/core/status_wire_conversion.py`. The crate stays
//! free of PyO3 types so later UniFFI/WASM/server work can reuse the same
//! logic; the PyO3 binding lives in `sase_core_py`.
//!
//! See `sase_100/plans/202604/rust_backend_phase4_status_machine.md` for
//! the full phase plan and `..._phase4b_handoff.md` for the wire contract
//! Phase 4C reproduces here.

pub mod constants;
pub mod field_updates;
pub mod name;
pub mod planner;
pub mod wire;

pub use constants::{
    is_valid_transition, remove_workspace_suffix, valid_transitions_from,
    ARCHIVE_STATUSES, VALID_STATUSES,
};
pub use field_updates::{apply_status_update, read_status_from_lines};
pub use name::{get_next_suffix_number, has_suffix};
pub use planner::plan_status_transition;
pub use wire::{
    status_plan_from_json_value, status_request_from_json_value,
    ChangespecChildWire, StatusFieldReadWire, StatusFieldUpdateWire,
    StatusTransitionPlanWire, StatusTransitionRequestWire,
    ARCHIVE_ACTION_FROM_ARCHIVE, ARCHIVE_ACTION_NONE,
    ARCHIVE_ACTION_TO_ARCHIVE, MENTOR_ACTION_CLEAR, MENTOR_ACTION_NONE,
    MENTOR_ACTION_SET, STATUS_WIRE_SCHEMA_VERSION, SUFFIX_ACTION_APPEND,
    SUFFIX_ACTION_NONE, SUFFIX_ACTION_STRIP,
};
