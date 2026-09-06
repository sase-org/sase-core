//! Temporary offline migration-kit contract.
//!
//! Deletion owner: sase-x7.14.

use std::path::{Path, PathBuf};
use std::time::Duration;

use thiserror::Error;

use crate::store_lock::{
    acquire_store_lock, holder_path_for, HeldStoreLock, LockMode,
    StoreLockError,
};

pub mod digest;
pub mod journal;
pub mod manifest;
pub mod procs;
pub mod residue;

pub use digest::{
    fingerprint, tree_digest, MigrationDigestError, MigrationFingerprintWire,
    MigrationTreeDigestEntryWire, MigrationTreeDigestWire,
    MIGRATION_FINGERPRINT_ALGORITHM, MIGRATION_TREE_DIGEST_ALGORITHM,
};
pub use journal::{
    plan_next_step, MigrationDigestMismatchWire, MigrationJournalRecord,
    MigrationJournalStateWire, MigrationRefusalWire, MigrationResumePlanWire,
};
pub use manifest::{
    MigrationBackupRecord, MigrationConflictRecord, MigrationManifest,
    MigrationOperationEntry,
};
pub use procs::{
    reconcile_plan, MigrationCanonicalProcRefWire, MigrationLegacyProcRowWire,
    MigrationProcConflictWire, MigrationProcMatchWire,
    MigrationProcReconcilePlanWire,
};
pub use residue::{
    classify, classify_many, MigrationResidueClassificationWire,
    MigrationResidueDecisionWire, MigrationResidueEntryWire,
    MigrationResidueFactsWire,
};

pub const MIGRATION_WIRE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Error)]
pub enum MigrationLockError {
    #[error("{0}")]
    Timeout(String),
    #[error("{0}")]
    Acquire(String),
    #[error("failed to release migration lock: {0}")]
    Release(std::io::Error),
}

impl From<StoreLockError> for MigrationLockError {
    fn from(error: StoreLockError) -> Self {
        match error {
            StoreLockError::Timeout { .. } => Self::Timeout(error.to_string()),
            _ => Self::Acquire(error.to_string()),
        }
    }
}

#[derive(Debug)]
pub struct MigrationHeldLock {
    inner: Option<HeldStoreLock>,
    lock_path: PathBuf,
}

impl MigrationHeldLock {
    pub fn lock_path(&self) -> &Path {
        &self.lock_path
    }

    pub fn waited_ms(&self) -> u64 {
        self.inner
            .as_ref()
            .map(HeldStoreLock::waited_ms)
            .unwrap_or(0)
    }

    pub fn release(&mut self) -> Result<(), MigrationLockError> {
        let Some(lock) = self.inner.take() else {
            return Ok(());
        };
        lock.release().map_err(MigrationLockError::Release)
    }
}

pub fn acquire_bounded_lock(
    lock_path: &Path,
    timeout_ms: u64,
    operation: &str,
) -> Result<MigrationHeldLock, MigrationLockError> {
    let holder_path = holder_path_for(lock_path);
    let lock = acquire_store_lock(
        lock_path,
        &holder_path,
        LockMode::Exclusive,
        Duration::from_millis(timeout_ms),
        operation,
    )?;
    Ok(MigrationHeldLock {
        inner: Some(lock),
        lock_path: lock_path.to_path_buf(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_lock_wraps_store_lock() {
        let temp = tempfile::tempdir().unwrap();
        let lock_path = temp.path().join("migration.lock");
        let mut lock =
            acquire_bounded_lock(&lock_path, 250, "migration-test").unwrap();
        assert_eq!(lock.lock_path(), lock_path.as_path());
        lock.release().unwrap();
        lock.release().unwrap();
    }
}
