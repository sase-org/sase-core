//! Footprint usage-census ledger: spool fold, catalog ingest, status,
//! and report queries.

mod catalog;
mod derived;
mod export;
mod fold;
mod import;
mod maintenance;
mod query;
mod spool;
mod status;
mod store;
mod week;
mod wire;

pub use catalog::{
    catalog_rev, catalog_status, ingest_catalog, parse_catalog,
    validate_overlay_shape, CatalogFile,
};
pub use derived::upsert_derived;
pub use export::export;
pub use fold::fold;
pub use import::import;
pub use maintenance::{clear, prune};
pub use query::{card, decide, report};
pub use spool::{
    parse_spool_line, SpoolLine, SPOOL_DROP_SLACK_BYTES, SPOOL_FILE_MAX_BYTES,
    SPOOL_LINE_MAX_BYTES,
};
pub use status::status;
pub use store::{
    open_read_store, open_write_store, schema_columns, FOOTPRINT_BUSY_DEFAULT,
    FOOTPRINT_BUSY_MAX, SCHEMA_TABLE_COLUMNS,
};
pub use wire::{
    footprint_wire_schema_version, CardWire, ClearResultWire, CoverageRowWire,
    DecideResultWire, DerivedCoverageRowWire, DerivedDailyRowWire,
    ExportRequestWire, FoldResultWire, FootprintError, ImportResultWire,
    PackFeatureWire, PackWire, PruneResultWire, QueryRequestWire, ReportWire,
    SpoolPendingWire, StatusWire, FOOTPRINT_WIRE_SCHEMA_VERSION,
};

#[cfg(test)]
mod tests;
#[cfg(test)]
mod transfer_tests;
