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

/// How aggressively an oversized-WAL checkpoint tries to reset the WAL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentArtifactIndexWalCheckpointMode {
    /// `wal_checkpoint(PASSIVE)`: copy what it can without waiting on
    /// readers or writers. Used on read paths, which must never stall.
    Passive,
    /// `wal_checkpoint(TRUNCATE)`: wait (up to the busy timeout) for
    /// readers, then reset the WAL to zero. Used by maintenance.
    Truncate,
}

impl AgentArtifactIndexWalCheckpointMode {
    fn pragma(self) -> &'static str {
        match self {
            Self::Passive => "PRAGMA wal_checkpoint(PASSIVE)",
            Self::Truncate => "PRAGMA wal_checkpoint(TRUNCATE)",
        }
    }
}

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
    checkpoint_agent_artifact_index_wal_if_oversized_with_mode(
        index_path,
        max_wal_bytes,
        AgentArtifactIndexWalCheckpointMode::Truncate,
    )
}

/// Same as [`checkpoint_agent_artifact_index_wal_if_oversized`] with an
/// explicit checkpoint mode. SQLite reports a checkpoint that could not
/// run as `busy = 1`, or as `-1` together with `-1` frame counts (for
/// example when the database is locked or the connection is not in WAL
/// mode). Every nonzero busy value is a normal busy outcome and the
/// unavailable frame counts read as zero.
pub fn checkpoint_agent_artifact_index_wal_if_oversized_with_mode(
    index_path: &Path,
    max_wal_bytes: u64,
    mode: AgentArtifactIndexWalCheckpointMode,
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
        .query_row(mode.pragma(), [], |row| {
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
        checkpoint_log_frames: frame_count(log_frames),
        checkpoint_frames_checkpointed: frame_count(frames_checkpointed),
    })
}

/// SQLite reports `-1` for a frame count it could not compute; that reads
/// as zero frames rather than failing the `u64` conversion.
pub(super) fn frame_count(frames: i64) -> u64 {
    u64::try_from(frames).unwrap_or(0)
}
