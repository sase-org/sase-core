//! Agent status buckets and archive outcomes.
//!
//! This is the one status table for archive outcome: it moves the live
//! status-bucket match from `sase.agent.status_buckets` into the core so the
//! archive corpus and every frontend derive the same bucket. Callers pass the
//! canonical status text (Python strips presentation glyphs first); the match
//! itself is case-sensitive and adds no statuses beyond the live table.

/// Map a canonical agent status to its live status bucket.
///
/// `canonical_status` must already be glyph-stripped. Every other bucket maps
/// to `"Running"`, mirroring the Python table this was moved from.
pub fn status_bucket_for_status(canonical_status: &str) -> &'static str {
    match canonical_status {
        "DONE" | "PLAN DONE" | "TALE DONE" | "PLAN REJECTED"
        | "EPIC CREATED" | "STOPPED" | "FEEDBACK" => "Done",
        "EPIC" | "TALE" | "PLAN" | "QUESTION" => "Stopped",
        "STARTING" => "Starting",
        "PLAN APPROVED" | "TALE APPROVED" | "WORKING PLAN" | "WORKING TALE"
        | "ANSWERED" => "Running",
        "QUEUED" => "Queued",
        "WAITING" => "Waiting",
        _ if canonical_status == "PLAN FAILED"
            || canonical_status == "EPIC FAILED"
            || canonical_status.starts_with("FAILED") =>
        {
            "Failed"
        }
        _ => "Running",
    }
}

/// Derive the archive outcome from the status bucket: `Done` becomes `done`,
/// `Failed` becomes `failed`, and every other bucket becomes `interrupted`.
///
/// The outcome deliberately grows no second status list; anything the bucket
/// table does not already treat as done or failed reads as interrupted.
pub fn archive_outcome_for_status(canonical_status: &str) -> &'static str {
    match status_bucket_for_status(canonical_status) {
        "Done" => "done",
        "Failed" => "failed",
        _ => "interrupted",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_statuses_bucket_as_done() {
        for status in [
            "DONE",
            "PLAN DONE",
            "TALE DONE",
            "PLAN REJECTED",
            "EPIC CREATED",
            "STOPPED",
            "FEEDBACK",
        ] {
            assert_eq!(status_bucket_for_status(status), "Done", "{status}");
        }
    }

    #[test]
    fn input_pause_statuses_bucket_as_stopped() {
        for status in ["EPIC", "TALE", "PLAN", "QUESTION"] {
            assert_eq!(status_bucket_for_status(status), "Stopped", "{status}");
        }
    }

    #[test]
    fn handoff_and_transient_statuses_bucket_as_running() {
        for status in [
            "PLAN APPROVED",
            "TALE APPROVED",
            "WORKING PLAN",
            "WORKING TALE",
            "ANSWERED",
            "RUNNING",
            "RETRYING",
            "TESTING",
            "UNKNOWN",
        ] {
            assert_eq!(status_bucket_for_status(status), "Running", "{status}");
        }
    }

    #[test]
    fn scheduled_statuses_keep_their_own_buckets() {
        assert_eq!(status_bucket_for_status("STARTING"), "Starting");
        assert_eq!(status_bucket_for_status("QUEUED"), "Queued");
        assert_eq!(status_bucket_for_status("WAITING"), "Waiting");
    }

    #[test]
    fn failure_statuses_bucket_as_failed() {
        for status in ["FAILED", "FAILED RETRY", "PLAN FAILED", "EPIC FAILED"] {
            assert_eq!(status_bucket_for_status(status), "Failed", "{status}");
        }
    }

    #[test]
    fn done_bucket_means_done_outcome() {
        for status in ["DONE", "TALE DONE", "EPIC CREATED"] {
            assert_eq!(archive_outcome_for_status(status), "done", "{status}");
        }
    }

    #[test]
    fn failed_bucket_means_failed_outcome() {
        for status in ["FAILED", "PLAN FAILED"] {
            assert_eq!(
                archive_outcome_for_status(status),
                "failed",
                "{status}"
            );
        }
    }

    #[test]
    fn every_other_bucket_means_interrupted_outcome() {
        for status in [
            "RUNNING",
            "STARTING",
            "QUEUED",
            "WAITING",
            "QUESTION",
            "PLAN",
            "PLAN APPROVED",
            "UNKNOWN",
        ] {
            assert_eq!(
                archive_outcome_for_status(status),
                "interrupted",
                "{status}"
            );
        }
    }
}
