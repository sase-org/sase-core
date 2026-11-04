//! Parked-row recheck policy for publication protection withholds.

/// A parked request is due for one more publish attempt when the scanner
/// rules moved under it, when its recheck interval elapsed, or when its
/// last check lies in the future (clock skew).
pub fn publication_parked_recheck_due(
    now: f64,
    parked_checked_at: f64,
    parked_rules_version: u32,
    current_rules_version: u32,
    recheck_interval_seconds: f64,
) -> bool {
    if !now.is_finite()
        || !parked_checked_at.is_finite()
        || !recheck_interval_seconds.is_finite()
    {
        return true;
    }
    if parked_rules_version != current_rules_version {
        return true;
    }
    if parked_checked_at > now {
        return true;
    }
    now - parked_checked_at >= recheck_interval_seconds
}

#[cfg(test)]
mod tests {
    use super::publication_parked_recheck_due;

    #[test]
    fn rules_change_is_due() {
        assert!(publication_parked_recheck_due(100.0, 90.0, 1, 2, 3600.0));
    }

    #[test]
    fn elapsed_interval_is_due() {
        assert!(publication_parked_recheck_due(5000.0, 1000.0, 2, 2, 3600.0));
        assert!(publication_parked_recheck_due(4600.0, 1000.0, 2, 2, 3600.0));
    }

    #[test]
    fn fresh_park_is_not_due() {
        assert!(!publication_parked_recheck_due(
            2000.0, 1000.0, 2, 2, 3600.0
        ));
    }

    #[test]
    fn future_check_is_due() {
        assert!(publication_parked_recheck_due(1000.0, 2000.0, 2, 2, 3600.0));
    }
}
