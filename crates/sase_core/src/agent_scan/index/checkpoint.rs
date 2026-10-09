use super::storage::{open_index_with_busy_timeout, sqlite_sidecar_path};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::ErrorKind;
use std::path::Path;
use std::time::Duration;

/// Short lock wait for the oversized-WAL checkpoint.
///
/// Checkpointing is best-effort background housekeeping: when the
/// database is busy it reports `checkpoint_busy` instead of stalling
/// the caller.
const CHECKPOINT_BUSY_TIMEOUT: Duration = Duration::from_secs(1);

/// Outcome of
/// [`checkpoint_agent_artifact_index_wal_if_oversized`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArtifactIndexWalCheckpointWire {
    /// Whether the WAL was over the threshold and a checkpoint ran.
    pub checkpoint_attempted: bool,
    /// WAL file size in bytes before the checkpoint.
    pub wal_bytes_before: u64,
    /// WAL file size in bytes after the checkpoint.
    pub wal_bytes_after: u64,
    /// Whether SQLite reported the checkpoint as busy. A busy
    /// checkpoint is a normal outcome, not an error: another
    /// connection is using the WAL and a later pass will retry.
    pub checkpoint_busy: bool,
    /// WAL frame count SQLite reported.
    pub checkpoint_log_frames: u64,
    /// Frame count SQLite checkpointed back into the database.
    pub checkpoint_frames_checkpointed: u64,
}

fn skipped_checkpoint(wal_bytes: u64) -> AgentArtifactIndexWalCheckpointWire {
    AgentArtifactIndexWalCheckpointWire {
        checkpoint_attempted: false,
        wal_bytes_before: wal_bytes,
        wal_bytes_after: wal_bytes,
        checkpoint_busy: false,
        checkpoint_log_frames: 0,
        checkpoint_frames_checkpointed: 0,
    }
}

/// Checkpoint an oversized artifact-index WAL back to (near) zero.
///
/// Stats `<index>-wal` first: a missing WAL, or one at most
/// `max_wal_bytes`, returns a skipped outcome without opening a
/// connection. Otherwise opens read-write with a short busy timeout
/// and runs `PRAGMA wal_checkpoint(TRUNCATE)`, returning the WAL
/// sizes before and after plus the checkpoint counters.
pub fn checkpoint_agent_artifact_index_wal_if_oversized(
    index_path: &Path,
    max_wal_bytes: u64,
) -> Result<AgentArtifactIndexWalCheckpointWire, String> {
    let wal_path = sqlite_sidecar_path(index_path, "-wal");
    let wal_bytes_before = match fs::metadata(&wal_path) {
        Ok(metadata) => metadata.len(),
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Ok(skipped_checkpoint(0));
        }
        Err(error) => return Err(error.to_string()),
    };
    if wal_bytes_before <= max_wal_bytes {
        return Ok(skipped_checkpoint(wal_bytes_before));
    }
    let conn =
        open_index_with_busy_timeout(index_path, CHECKPOINT_BUSY_TIMEOUT)?;
    let (busy, log_frames, frames_checkpointed): (i64, i64, i64) = conn
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .map_err(|e| e.to_string())?;
    let wal_bytes_after = fs::metadata(&wal_path)
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    Ok(AgentArtifactIndexWalCheckpointWire {
        checkpoint_attempted: true,
        wal_bytes_before,
        wal_bytes_after,
        checkpoint_busy: busy != 0,
        checkpoint_log_frames: u64::try_from(log_frames)
            .map_err(|e| e.to_string())?,
        checkpoint_frames_checkpointed: u64::try_from(frames_checkpointed)
            .map_err(|e| e.to_string())?,
    })
}
