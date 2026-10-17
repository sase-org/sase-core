//! Shared blob-line counter interface for the project statistics snapshot.

#![allow(dead_code)] // The snapshot phase is the first production caller.

use crate::project_stats::wire::ProjectStatsCodeCounterWire;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct BlobLineCounts {
    /// The count selected by the request's Code counter policy.
    pub selected_lines: u64,
    /// Nonblank lines, retained for prose and data roles under every policy.
    pub nonblank_lines: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CounterError {
    SlocPolicyNotAvailable,
}

impl std::fmt::Display for CounterError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SlocPolicyNotAvailable => {
                formatter.write_str("the sloc counter is not enabled")
            }
        }
    }
}

/// Count a blob with an explicit policy and language hint. The initial
/// production policy is nonblank; SLOC remains behind the later measured
/// dependency gate.
pub(crate) fn count_blob(
    policy: ProjectStatsCodeCounterWire,
    _language: Option<&str>,
    bytes: &[u8],
) -> Result<BlobLineCounts, CounterError> {
    let nonblank_lines = count_nonblank_lines(bytes);
    let selected_lines = match policy {
        ProjectStatsCodeCounterWire::Nonblank => nonblank_lines,
        ProjectStatsCodeCounterWire::Sloc => {
            return Err(CounterError::SlocPolicyNotAvailable);
        }
    };
    Ok(BlobLineCounts {
        selected_lines,
        nonblank_lines,
    })
}

/// Count lines containing a non-whitespace character. LF terminates lines,
/// CRLF is treated as one line ending, and an unterminated final line counts.
pub(crate) fn count_nonblank_lines(bytes: &[u8]) -> u64 {
    bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| {
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            match std::str::from_utf8(line) {
                Ok(text) => !text.trim().is_empty(),
                Err(_) => line.iter().any(|byte| !byte.is_ascii_whitespace()),
            }
        })
        .count() as u64
}
