//! Fleet reads: snapshot-backed fleet summary, catalog, detail, and
//! content APIs.
//!
//! Split from the former 3,371-line `fleet_reads.rs` along domain seams:
//! [`service`](service) owns `FleetReadService` and its snapshot-cache
//! machinery, [`snapshot`](snapshot) builds authoritative snapshots from the
//! artifact index, [`resolution`](resolution) projects index records into
//! served rows, [`content`](content) maps opaque content handles to
//! canonicalized artifact paths (a filesystem security boundary), and
//! [`invalidation`](invalidation) fans out cache-invalidation events.
//! [`errors`](errors) holds the shared error type.

mod content;
mod errors;
mod invalidation;
mod resolution;
mod service;
mod snapshot;
#[cfg(test)]
mod tests;

pub use errors::FleetReadError;
pub use invalidation::{
    resync_item, FleetEventSubscription, FleetInvalidationHub,
};
pub use service::FleetReadService;
