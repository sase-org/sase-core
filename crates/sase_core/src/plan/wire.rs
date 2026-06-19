//! Plan wire records for `sase plan search`.
//!
//! Mirrors the bead wire split (`bead/wire.rs`): owned, serde-friendly data
//! with no PyO3 types, so later binding/TUI/web work can reuse the same model.
//! A [`PlanWire`] describes a single markdown plan artifact discovered under a
//! repo `sdd/` tree or the machine-local `~/.sase/plans/` archive.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const PLAN_WIRE_SCHEMA_VERSION: u64 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{kind}: {message}")]
pub struct PlanError {
    pub kind: String,
    pub message: String,
}

impl PlanError {
    pub fn validation(message: impl Into<String>) -> Self {
        Self {
            kind: "validation".to_string(),
            message: message.into(),
        }
    }

    pub fn io(message: impl Into<String>) -> Self {
        Self {
            kind: "io".to_string(),
            message: message.into(),
        }
    }
}

impl From<std::io::Error> for PlanError {
    fn from(error: std::io::Error) -> Self {
        Self::io(error.to_string())
    }
}

/// A single discovered plan artifact.
///
/// `source` is `"repo"` for committed `sdd/` plans and `"local"` for the
/// machine-local archive. `kind` is one of the repo plan kinds
/// (`tale`/`epic`/`legend`/`myth`/`research`) or the synthetic `"local"` kind
/// for archive entries (which have no kind directory). `relpath` is relative to
/// the plan's root (the `sdd/` directory for repo plans, the local archive
/// directory for local plans) and always uses `/` separators.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanWire {
    pub source: String,
    pub kind: String,
    pub path: String,
    pub relpath: String,
    pub name: String,
    pub title: String,
    pub status: String,
    pub created_at: String,
    pub prompt_link: String,
    pub summary: String,
    pub body: String,
    #[serde(default)]
    pub frontmatter: BTreeMap<String, String>,
}

/// A single ranked plan-search result.
///
/// Mirrors the bead search match (`bead/wire.rs::BeadSearchMatchWire`) and
/// extends it with a numeric relevance `score`: plan search ranks results
/// (field-weighted relevance + repo boost) where bead search returns storage
/// order. `matched_fields` lists the searchable field names hit by the query
/// (empty in browse mode, when no query is supplied). The struct intentionally
/// does not derive `Eq` because `score` is an `f64`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlanSearchMatchWire {
    pub plan: PlanWire,
    pub matched_fields: Vec<String>,
    pub score: f64,
}
