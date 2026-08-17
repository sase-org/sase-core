//! Task-type spec validation, digest, field-value checks, body rendering,
//! and the committed catalog snapshot format.
//!
//! Catalog membership stays in Python. This module validates plugin-produced
//! JSON and owns the closed field-type vocabulary.

mod error;
mod render;
mod snapshot;
mod spec;
mod values;

pub use error::{TaskTypeError, TaskTypeFieldValueError};
pub use render::render_task_type_body;
pub use snapshot::{
    parse_task_type_snapshot, serialize_task_type_snapshot,
    TaskTypeSnapshotEntryWire, TaskTypeSnapshotWire, TaskTypeSourceWire,
};
pub use spec::{
    task_type_spec_digest, validate_task_type_spec, TaskTypeFieldSpecWire,
    TaskTypeSpecWire, TaskTypeTriageWire, RESERVED_TASK_TYPE_SLUGS,
    TASK_TYPE_FIELD_ROLES, TASK_TYPE_FIELD_TYPES,
    TASK_TYPE_SPEC_WIRE_SCHEMA_VERSION,
};
pub use values::validate_task_type_field_values;
