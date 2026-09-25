use std::cmp::Ordering;
use std::collections::BTreeSet;

use chrono::{DateTime, Utc};

use crate::queue_directive::{
    normalize_persisted_queue_capacity, queue_capacity_budget_enabled,
    queue_weight_is_valid, DEFAULT_QUEUE_WEIGHT,
};

use super::capacity_math::{
    capacity_fits, compare_f64, compensated_sum,
    occupied_capacity_exceeds_threshold, ulp_at,
    waiter_capacity_condition_shortfall, waiter_capacity_shortfall,
};
use super::holds::hold_barrier_blockers;
use super::records::{
    effective_weight, explicit_queue_capacity, is_user_agent_record,
    is_waiting_record, record_index,
};
use super::wire::{
    blocker, diagnostic, RunnerCapacityBlockerWire, RunnerCapacityClaimWire,
    RunnerCapacityDiagnosticWire, RunnerCapacityRecordWire,
    RunnerCapacityRequestWire, RunnerCapacityWaiterWire, DEFAULT_WAIT_PRIORITY,
};

struct WaiterEvaluation<'a> {
    request: &'a RunnerCapacityRequestWire,
    record: &'a RunnerCapacityRecordWire,
    claims: &'a [RunnerCapacityClaimWire],
    occupied_capacity: f64,
    requested_weight: f64,
    queue_capacity: Option<u32>,
    admission_limit: f64,
    zero_drain_barrier: bool,
    capacity_budget: bool,
    priority: i32,
    fail_closed: bool,
}

struct WaiterDraft<'a> {
    record: &'a RunnerCapacityRecordWire,
    wire: RunnerCapacityWaiterWire,
}

pub(super) fn build_waiters(
    request: &RunnerCapacityRequestWire,
    occupied_capacity: f64,
    claims: &[RunnerCapacityClaimWire],
    active_claims: &BTreeSet<String>,
    fail_closed: bool,
    diagnostics: &mut Vec<RunnerCapacityDiagnosticWire>,
) -> Vec<RunnerCapacityWaiterWire> {
    let index = record_index(&request.records);
    let mut candidates: Vec<&RunnerCapacityRecordWire> = request
        .records
        .iter()
        .filter(|record| is_waiting_record(record, active_claims, &index))
        .collect();
    candidates.sort_by(|left, right| compare_waiters(left, right));

    let mut drafts = Vec::new();
    let mut prior_eligible = false;
    let capacity_budget = queue_capacity_budget_enabled(&request.feature_flags);
    for record in candidates {
        let priority = normalize_wait_priority(record.wait_priority);
        let requested_weight =
            effective_weight(record).unwrap_or(DEFAULT_QUEUE_WEIGHT);
        let queue_capacity = explicit_queue_capacity(record);
        let (admission_limit, zero_drain_barrier) = waiter_admission_limit(
            request,
            record,
            requested_weight,
            queue_capacity,
            capacity_budget,
            diagnostics,
        );
        let mut blockers = waiter_blockers(WaiterEvaluation {
            request,
            record,
            claims,
            occupied_capacity,
            requested_weight,
            queue_capacity,
            admission_limit,
            zero_drain_barrier,
            capacity_budget,
            priority,
            fail_closed,
        });
        if blockers.is_empty() {
            if prior_eligible {
                blockers.push(blocker(
                    "queue-order",
                    "An earlier currently eligible waiter is ahead in priority/FIFO order.",
                    None,
                    None,
                    None,
                    None,
                    None,
                ));
            } else {
                prior_eligible = true;
            }
        }
        let parked = has_resource_blocker(&blockers);
        let capacity_shortfall = if zero_drain_barrier {
            occupied_capacity.max(0.0)
        } else {
            waiter_capacity_shortfall(claims, requested_weight, admission_limit)
        };
        let wait_capacity_shortfall = if capacity_budget {
            0.0
        } else {
            waiter_capacity_condition_shortfall(
                occupied_capacity,
                queue_capacity,
            )
        };
        drafts.push(WaiterDraft {
            record,
            wire: RunnerCapacityWaiterWire {
                artifact_dir: record.artifact_dir.clone(),
                queue_position: 0,
                priority,
                slot_requested_at: record
                    .slot_requested_at
                    .clone()
                    .unwrap_or_default(),
                timestamp: record.timestamp.clone(),
                requested_weight,
                queue_capacity,
                admission_limit,
                eligible: blockers.is_empty(),
                parked,
                capacity_shortfall,
                wait_capacity_shortfall,
                blockers,
            },
        });
    }
    drafts.sort_by(compare_waiter_drafts);
    drafts
        .into_iter()
        .enumerate()
        .map(|(index, mut draft)| {
            draft.wire.queue_position = (index + 1) as u32;
            draft.wire
        })
        .collect()
}

fn waiter_admission_limit(
    request: &RunnerCapacityRequestWire,
    record: &RunnerCapacityRecordWire,
    requested_weight: f64,
    queue_capacity: Option<u32>,
    capacity_budget: bool,
    diagnostics: &mut Vec<RunnerCapacityDiagnosticWire>,
) -> (f64, bool) {
    let normalized = normalize_persisted_queue_capacity(
        queue_capacity,
        record.queue_capacity_explicit,
        requested_weight,
        request.effective_limit,
        capacity_budget,
    );
    if normalized.legacy_zero {
        diagnostics.push(diagnostic(
            "legacy-capacity-zero",
            "Persisted queue_capacity=0 was translated to this waiter's effective weight so it drains to zero before admission.",
            Some(record.artifact_dir.clone()),
        ));
    }
    (
        normalized.admission_limit,
        normalized.legacy_zero && normalized.admission_limit == 0.0,
    )
}

fn waiter_blockers(
    eval: WaiterEvaluation<'_>,
) -> Vec<RunnerCapacityBlockerWire> {
    let mut blockers = Vec::new();
    blockers.extend(hold_barrier_blockers(eval.request, eval.record));
    if eval.fail_closed {
        blockers.push(blocker(
            "capacity-snapshot-invalid",
            "Runner capacity cannot admit new work because the live snapshot is invalid.",
            None,
            None,
            None,
            None,
            None,
        ));
    }
    if let Err(message) = effective_weight(eval.record) {
        blockers.push(blocker(
            "invalid-request-weight",
            &message,
            None,
            None,
            None,
            None,
            None,
        ));
    }
    if eval.zero_drain_barrier {
        if eval.occupied_capacity > 0.0 {
            blockers.push(blocker(
                "insufficient-capacity",
                "Persisted queue_capacity=0 waits for occupied runner capacity to drain to zero.",
                Some(eval.requested_weight),
                Some(0.0),
                None,
                None,
                Some(eval.admission_limit),
            ));
        }
    } else if !queue_weight_is_valid(eval.admission_limit) {
        blockers.push(blocker(
            "invalid-capacity-limit",
            "The effective runner capacity limit must be positive and finite.",
            None,
            None,
            None,
            None,
            None,
        ));
    } else {
        let free_capacity =
            (eval.admission_limit - eval.occupied_capacity).max(0.0);
        if !capacity_fits(eval.requested_weight, eval.admission_limit) {
            blockers.push(blocker(
                "weight-exceeds-limit",
                "Requested capacity weight exceeds this waiter's admission limit.",
                Some(eval.requested_weight),
                Some(free_capacity),
                None,
                None,
                Some(eval.admission_limit),
            ));
        } else {
            let proposed = compensated_sum(
                eval.claims
                    .iter()
                    .map(|claim| claim.occupied_capacity)
                    .chain(std::iter::once(eval.requested_weight)),
            );
            if proposed.is_none() {
                blockers.push(blocker(
                    "capacity-overflow",
                    "Runner capacity cannot admit this waiter because the proposed total overflows.",
                    Some(eval.requested_weight),
                    Some(free_capacity),
                    None,
                    None,
                    Some(eval.admission_limit),
                ));
            } else if eval.requested_weight == 0.0
                && free_capacity <= 4.0 * ulp_at(eval.admission_limit.max(1.0))
            {
                blockers.push(blocker(
                    "insufficient-capacity",
                    "Zero-weight waiters still require free runner capacity before admission.",
                    Some(eval.requested_weight),
                    Some(free_capacity),
                    None,
                    None,
                    Some(eval.admission_limit),
                ));
            } else if !capacity_fits(
                proposed.unwrap_or(f64::MAX),
                eval.admission_limit,
            ) {
                blockers.push(blocker(
                    "insufficient-capacity",
                    "Insufficient free runner capacity for this waiter.",
                    Some(eval.requested_weight),
                    Some(free_capacity),
                    None,
                    None,
                    Some(eval.admission_limit),
                ));
            }
        }
    }
    if !eval.capacity_budget {
        if let Some(threshold) = eval.queue_capacity {
            if occupied_capacity_exceeds_threshold(
                eval.occupied_capacity,
                threshold,
            ) {
                let message = if threshold == 0 {
                    "Occupied weighted load must drain to zero before this capacity-0 waiter can start."
                } else {
                    "Occupied weighted load exceeds this explicit capacity threshold."
                };
                blockers.push(blocker(
                    "capacity-condition",
                    message,
                    None,
                    None,
                    Some(eval.occupied_capacity),
                    Some(threshold),
                    Some(eval.admission_limit),
                ));
            }
        }
    }
    if blockers.is_empty()
        && eval.priority > DEFAULT_WAIT_PRIORITY
        && deference_window_seconds(
            eval.priority,
            eval.request.deference_seconds_per_step,
            eval.request.deference_max_seconds,
        ) > 0.0
        && better_priority_agent_pending(
            &eval.request.records,
            eval.record,
            eval.priority,
        )
        && !deference_satisfied(
            eval.record.eligible_since.as_deref(),
            eval.request.now.as_deref(),
            deference_window_seconds(
                eval.priority,
                eval.request.deference_seconds_per_step,
                eval.request.deference_max_seconds,
            ),
        )
    {
        blockers.push(blocker(
            "deference-window",
            "A better-priority unparked waiter is pending and the deference window has not elapsed.",
            None,
            None,
            None,
            None,
            None,
        ));
    }
    blockers
}

fn compare_waiter_drafts(
    left: &WaiterDraft<'_>,
    right: &WaiterDraft<'_>,
) -> Ordering {
    waiter_display_bucket(&left.wire)
        .cmp(&waiter_display_bucket(&right.wire))
        .then_with(|| {
            if left.wire.parked || right.wire.parked {
                compare_f64(
                    left.wire.capacity_shortfall,
                    right.wire.capacity_shortfall,
                )
                .then_with(|| {
                    compare_f64(
                        left.wire.wait_capacity_shortfall,
                        right.wire.wait_capacity_shortfall,
                    )
                })
            } else {
                Ordering::Equal
            }
        })
        .then_with(|| compare_waiters(left.record, right.record))
}

fn waiter_display_bucket(waiter: &RunnerCapacityWaiterWire) -> u8 {
    if waiter.parked {
        2
    } else if waiter.eligible {
        0
    } else {
        1
    }
}

fn has_resource_blocker(blockers: &[RunnerCapacityBlockerWire]) -> bool {
    blockers.iter().any(|blocker| {
        matches!(
            blocker.code.as_str(),
            "hold-barrier"
                | "capacity-snapshot-invalid"
                | "invalid-request-weight"
                | "invalid-capacity-limit"
                | "weight-exceeds-limit"
                | "insufficient-capacity"
                | "capacity-overflow"
                | "capacity-condition"
        )
    })
}

fn normalize_wait_priority(value: Option<i64>) -> i32 {
    value
        .and_then(|value| i32::try_from(value).ok())
        .filter(|value| *value >= 0)
        .unwrap_or(DEFAULT_WAIT_PRIORITY)
}

fn compare_waiters(
    left: &RunnerCapacityRecordWire,
    right: &RunnerCapacityRecordWire,
) -> Ordering {
    normalize_wait_priority(left.wait_priority)
        .cmp(&normalize_wait_priority(right.wait_priority))
        .then_with(|| {
            requested_at_key(left.slot_requested_at.as_deref())
                .cmp(&requested_at_key(right.slot_requested_at.as_deref()))
        })
        .then_with(|| left.timestamp.cmp(&right.timestamp))
        .then_with(|| left.artifact_dir.cmp(&right.artifact_dir))
}

fn requested_at_key(value: Option<&str>) -> (u8, i64) {
    let Some(value) = value else {
        return (1, i64::MAX);
    };
    match DateTime::parse_from_rfc3339(value) {
        Ok(parsed) => (0, parsed.with_timezone(&Utc).timestamp_millis()),
        Err(_) => (1, i64::MAX),
    }
}

fn better_priority_agent_pending(
    records: &[RunnerCapacityRecordWire],
    me: &RunnerCapacityRecordWire,
    priority: i32,
) -> bool {
    records.iter().any(|record| {
        record.artifact_dir != me.artifact_dir
            && is_user_agent_record(record)
            && record.live
            && record
                .run_started_at
                .as_deref()
                .is_none_or(|value| value.is_empty())
            && record
                .slot_requested_at
                .as_deref()
                .is_none_or(|value| value.is_empty())
            && normalize_wait_priority(record.wait_priority) < priority
    })
}

fn deference_window_seconds(
    priority: i32,
    seconds_per_step: u32,
    max_seconds: u32,
) -> f64 {
    if priority <= DEFAULT_WAIT_PRIORITY {
        return 0.0;
    }
    ((priority - DEFAULT_WAIT_PRIORITY) as u32)
        .saturating_mul(seconds_per_step)
        .min(max_seconds) as f64
}

fn deference_satisfied(
    eligible_since: Option<&str>,
    now: Option<&str>,
    window_seconds: f64,
) -> bool {
    if window_seconds <= 0.0 {
        return true;
    }
    let (Some(eligible_since), Some(now)) = (eligible_since, now) else {
        return false;
    };
    let Ok(started) = DateTime::parse_from_rfc3339(eligible_since) else {
        return false;
    };
    let Ok(now) = DateTime::parse_from_rfc3339(now) else {
        return false;
    };
    let elapsed = now
        .with_timezone(&Utc)
        .signed_duration_since(started.with_timezone(&Utc))
        .num_milliseconds() as f64
        / 1000.0;
    elapsed >= 0.0 && elapsed >= window_seconds
}
