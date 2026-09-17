//! Deterministic byte-bounded batching for full agent-hood publication.
//!
//! Host code owns filesystem writes, locks, Git transactions, and rollback.
//! This module only validates ordered publication path metadata and partitions
//! it into batches whose total byte size is within one configured budget.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const AGENT_PUBLICATION_BATCH_WIRE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentPublicationPathRecordWire {
    pub path: String,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentPublicationBatchWire {
    pub paths: Vec<String>,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentPublicationBatchPlanWire {
    pub schema_version: u32,
    pub budget_bytes: u64,
    pub total_size_bytes: u64,
    pub batches: Vec<AgentPublicationBatchWire>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AgentPublicationBatchError {
    #[error("agent publication batch budget must be positive")]
    NonPositiveBudget,
    #[error("publication path must be a non-empty string")]
    EmptyPath,
    #[error("unsafe publication path: {0:?}")]
    UnsafePath(String),
    #[error("duplicate publication path: {0:?}")]
    DuplicatePath(String),
    #[error(
        "publication file {path:?} is {size_bytes} bytes, exceeding the batch budget of {budget_bytes} bytes"
    )]
    FileExceedsBudget {
        path: String,
        size_bytes: u64,
        budget_bytes: u64,
    },
    #[error("publication payload byte total overflowed u64")]
    PayloadSizeOverflow,
}

pub fn plan_agent_publication_batches(
    records: &[AgentPublicationPathRecordWire],
    budget_bytes: u64,
) -> Result<AgentPublicationBatchPlanWire, AgentPublicationBatchError> {
    if budget_bytes == 0 {
        return Err(AgentPublicationBatchError::NonPositiveBudget);
    }

    let mut seen = BTreeSet::new();
    let mut total_size_bytes = 0_u64;
    let mut batches = Vec::new();
    let mut current_paths: Vec<String> = Vec::new();
    let mut current_size_bytes = 0_u64;

    for record in records {
        validate_relative_publication_path(&record.path)?;
        if !seen.insert(record.path.clone()) {
            return Err(AgentPublicationBatchError::DuplicatePath(
                record.path.clone(),
            ));
        }
        if record.size_bytes > budget_bytes {
            return Err(AgentPublicationBatchError::FileExceedsBudget {
                path: record.path.clone(),
                size_bytes: record.size_bytes,
                budget_bytes,
            });
        }
        total_size_bytes = total_size_bytes
            .checked_add(record.size_bytes)
            .ok_or(AgentPublicationBatchError::PayloadSizeOverflow)?;

        let next_size = current_size_bytes
            .checked_add(record.size_bytes)
            .ok_or(AgentPublicationBatchError::PayloadSizeOverflow)?;
        if !current_paths.is_empty() && next_size > budget_bytes {
            batches.push(AgentPublicationBatchWire {
                paths: std::mem::take(&mut current_paths),
                size_bytes: current_size_bytes,
            });
            current_size_bytes = 0;
        }

        current_paths.push(record.path.clone());
        current_size_bytes = current_size_bytes
            .checked_add(record.size_bytes)
            .ok_or(AgentPublicationBatchError::PayloadSizeOverflow)?;
    }

    if !current_paths.is_empty() {
        batches.push(AgentPublicationBatchWire {
            paths: current_paths,
            size_bytes: current_size_bytes,
        });
    }

    Ok(AgentPublicationBatchPlanWire {
        schema_version: AGENT_PUBLICATION_BATCH_WIRE_SCHEMA_VERSION,
        budget_bytes,
        total_size_bytes,
        batches,
    })
}

fn validate_relative_publication_path(
    path: &str,
) -> Result<(), AgentPublicationBatchError> {
    if path.is_empty() {
        return Err(AgentPublicationBatchError::EmptyPath);
    }
    if path.starts_with('/')
        || path.contains('\\')
        || path.contains('\0')
        || path.contains("//")
    {
        return Err(AgentPublicationBatchError::UnsafePath(path.to_string()));
    }
    for part in path.split('/') {
        if part.is_empty() || part == "." || part == ".." {
            return Err(AgentPublicationBatchError::UnsafePath(
                path.to_string(),
            ));
        }
        if part != ".gitkeep" && part.starts_with('.') {
            return Err(AgentPublicationBatchError::UnsafePath(
                path.to_string(),
            ));
        }
        if part.len() > 255 {
            return Err(AgentPublicationBatchError::UnsafePath(
                path.to_string(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(path: &str, size_bytes: u64) -> AgentPublicationPathRecordWire {
        AgentPublicationPathRecordWire {
            path: path.to_string(),
            size_bytes,
        }
    }

    #[test]
    fn batches_preserve_order_and_accept_exact_boundaries() {
        let plan = plan_agent_publication_batches(
            &[
                rec("a.txt", 4),
                rec("b.txt", 6),
                rec("c.txt", 1),
                rec("d.txt", 9),
            ],
            10,
        )
        .unwrap();

        assert_eq!(plan.schema_version, 1);
        assert_eq!(plan.total_size_bytes, 20);
        assert_eq!(
            plan.batches,
            vec![
                AgentPublicationBatchWire {
                    paths: vec!["a.txt".to_string(), "b.txt".to_string()],
                    size_bytes: 10,
                },
                AgentPublicationBatchWire {
                    paths: vec!["c.txt".to_string(), "d.txt".to_string()],
                    size_bytes: 10,
                },
            ]
        );
    }

    #[test]
    fn empty_and_zero_length_inputs_are_valid() {
        let empty = plan_agent_publication_batches(&[], 1).unwrap();
        assert!(empty.batches.is_empty());
        assert_eq!(empty.total_size_bytes, 0);

        let zeros = plan_agent_publication_batches(
            &[rec("a.txt", 0), rec("b.txt", 0)],
            1,
        )
        .unwrap();
        assert_eq!(zeros.total_size_bytes, 0);
        assert_eq!(zeros.batches.len(), 1);
        assert_eq!(zeros.batches[0].paths, ["a.txt", "b.txt"]);
    }

    #[test]
    fn rejects_invalid_or_duplicate_records_before_planning() {
        assert_eq!(
            plan_agent_publication_batches(&[rec("../escape", 1)], 1),
            Err(AgentPublicationBatchError::UnsafePath(
                "../escape".to_string()
            ))
        );
        assert_eq!(
            plan_agent_publication_batches(
                &[rec("a.txt", 0), rec("a.txt", 0)],
                1
            ),
            Err(AgentPublicationBatchError::DuplicatePath(
                "a.txt".to_string()
            ))
        );
        assert_eq!(
            plan_agent_publication_batches(&[rec("a.txt", 1)], 0),
            Err(AgentPublicationBatchError::NonPositiveBudget)
        );
    }

    #[test]
    fn rejects_one_file_larger_than_the_budget_with_metadata() {
        assert_eq!(
            plan_agent_publication_batches(&[rec("huge.bin", 11)], 10),
            Err(AgentPublicationBatchError::FileExceedsBudget {
                path: "huge.bin".to_string(),
                size_bytes: 11,
                budget_bytes: 10,
            })
        );
    }

    #[test]
    fn rejects_payload_size_overflow() {
        assert_eq!(
            plan_agent_publication_batches(
                &[rec("a.bin", u64::MAX), rec("b.bin", 1)],
                u64::MAX
            ),
            Err(AgentPublicationBatchError::PayloadSizeOverflow)
        );
    }
}
