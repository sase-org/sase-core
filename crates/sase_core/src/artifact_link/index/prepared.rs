//! Frozen in-memory artifact-link index.

use std::collections::HashMap;
use std::sync::Arc;

use crate::artifact_link::row_resolution::ArtifactRowIdentityWire;

use super::aliases::IndexIdentity;
use super::wire::{
    ArtifactLinkIndexChipWire, ArtifactLinkIndexRawRowWire,
    ArtifactLinkIndexStatsWire,
};

#[derive(Debug, Clone)]
pub(crate) struct IndexedRow {
    pub source_ref: String,
    pub target_ref: String,
    pub canon_source: String,
    pub canon_target: String,
    pub relation: String,
    pub description: String,
    pub origin: String,
    pub created_by: String,
    pub created_at: String,
    pub uses: u64,
    pub project: Option<String>,
}

impl IndexedRow {
    pub(crate) fn to_wire(&self) -> ArtifactLinkIndexRawRowWire {
        ArtifactLinkIndexRawRowWire {
            source_ref: self.source_ref.clone(),
            target_ref: self.target_ref.clone(),
            relation: self.relation.clone(),
            description: self.description.clone(),
            origin: self.origin.clone(),
            created_by: self.created_by.clone(),
            created_at: self.created_at.clone(),
            uses: self.uses,
            project: self.project.clone(),
        }
    }
}

/// Immutable query-ready artifact-link index.
#[derive(Debug, Clone)]
pub struct PreparedArtifactLinkIndex {
    pub(crate) rows: Vec<IndexedRow>,
    pub(crate) inverted: HashMap<Vec<String>, Vec<u32>>,
    pub(crate) chips: HashMap<String, Arc<[ArtifactLinkIndexChipWire]>>,
    pub(crate) targets: HashMap<String, Option<ArtifactRowIdentityWire>>,
    pub(crate) canonical_ref_count: usize,
    pub(crate) indexed_row_count: usize,
    pub(crate) identity: IndexIdentity,
}

impl PreparedArtifactLinkIndex {
    pub fn stats(&self) -> ArtifactLinkIndexStatsWire {
        ArtifactLinkIndexStatsWire {
            raw_row_count: self.rows.len() as u64,
            indexed_row_count: self.indexed_row_count as u64,
            canonical_ref_count: self.canonical_ref_count as u64,
            chip_ref_count: self.chips.len() as u64,
        }
    }

    pub fn raw_row_count(&self) -> usize {
        self.rows.len()
    }

    pub fn chip_ref_count(&self) -> usize {
        self.chips.len()
    }
}
