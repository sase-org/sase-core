use std::cmp::Ordering;

use crate::queue_directive::queue_weight_is_valid;

use super::wire::RunnerCapacityClaimWire;

pub(super) fn compare_f64(left: f64, right: f64) -> Ordering {
    left.partial_cmp(&right).unwrap_or(Ordering::Equal)
}

pub(super) fn waiter_capacity_shortfall(
    claims: &[RunnerCapacityClaimWire],
    requested_weight: f64,
    limit: f64,
) -> f64 {
    if !queue_weight_is_valid(limit) {
        return f64::MAX;
    }
    let Some(proposed) = compensated_sum(
        claims
            .iter()
            .map(|claim| claim.occupied_capacity)
            .chain(std::iter::once(requested_weight)),
    ) else {
        return f64::MAX;
    };
    if capacity_fits(proposed, limit) {
        0.0
    } else {
        (proposed - limit).max(0.0)
    }
}

pub(super) fn weights_equal(left: f64, right: f64) -> bool {
    if !left.is_finite() || !right.is_finite() {
        return false;
    }
    let scale = left.abs().max(right.abs()).max(1.0);
    (left - right).abs() <= 4.0 * ulp_at(scale)
}

pub(super) fn compensated_sum(
    values: impl IntoIterator<Item = f64>,
) -> Option<f64> {
    let mut sum = 0.0;
    let mut compensation = 0.0;
    for value in values {
        if !value.is_finite() {
            return None;
        }
        let y = value - compensation;
        let next = sum + y;
        if !next.is_finite() {
            return None;
        }
        compensation = (next - sum) - y;
        sum = next;
    }
    Some(sum)
}

pub(super) fn capacity_fits(total: f64, limit: f64) -> bool {
    if !total.is_finite() || !limit.is_finite() {
        return false;
    }
    if total <= limit {
        return true;
    }
    total - limit <= 4.0 * ulp_at(total.max(limit))
}

pub(super) fn ulp_at(value: f64) -> f64 {
    if value == 0.0 {
        return f64::MIN_POSITIVE;
    }
    let value = value.abs();
    if !value.is_finite() {
        return f64::INFINITY;
    }
    next_up(value) - value
}

fn next_up(value: f64) -> f64 {
    if value.is_nan() || value == f64::INFINITY {
        return value;
    }
    if value == -0.0 {
        return f64::MIN_POSITIVE;
    }
    let bits = value.to_bits();
    if value >= 0.0 {
        f64::from_bits(bits + 1)
    } else {
        f64::from_bits(bits - 1)
    }
}
