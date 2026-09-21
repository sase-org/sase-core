//! Claude Fable alias canonicalization (covers `compatibility`).

use super::super::*;
use super::support::*;

#[test]
fn claude_fable_alias_observation_canonicalizes_and_collapses() {
    let observation = claude_usage_observation(
        NOW - 10.0,
        UsageCompleteness::Partial,
        vec![
            named_window("session", 10.0, NOW - 10.0),
            named_window("weekly", 88.0, NOW - 10.0),
            claude_fable_window(
                "window:seven-day-overage-included",
                82.0,
                NOW - 10.0,
            ),
        ],
    );
    let validated = validate_usage_observation(observation, NOW).unwrap();
    assert_eq!(
        validated
            .windows
            .iter()
            .map(|window| window.key.as_str())
            .collect::<Vec<_>>(),
        vec!["session", "weekly", "weekly:claude-fable-5"]
    );
    let fable = validated
        .windows
        .iter()
        .find(|window| window.key == "weekly:claude-fable-5")
        .unwrap();
    assert_eq!(fable.label, "Claude weekly Fable");
    assert_eq!(fable.used_percent, 82.0);
    assert_eq!(fable.resets_at, Some(NOW + WEEK_SECONDS));
    assert_eq!(fable.source, UsageSource::StreamEvent);
    assert_eq!(fable.vendor_state, UsageVendorState::Unknown);
    assert_eq!(
        fable.applicability,
        UsageApplicabilityWire::Models {
            model_ids: vec!["claude-fable-5".to_string()]
        }
    );

    let alias_newer = validate_usage_observation(
        claude_usage_observation(
            NOW - 8.0,
            UsageCompleteness::Partial,
            vec![
                claude_fable_window("weekly:claude-fable-5", 81.0, NOW - 8.0),
                claude_fable_window(
                    "window:seven-day-overage-included",
                    83.0,
                    NOW - 7.0,
                ),
            ],
        ),
        NOW,
    )
    .unwrap();
    assert_eq!(alias_newer.windows.len(), 1);
    assert_eq!(alias_newer.windows[0].key, "weekly:claude-fable-5");
    assert_eq!(alias_newer.windows[0].used_percent, 83.0);

    let canonical_tie = validate_usage_observation(
        claude_usage_observation(
            NOW - 6.0,
            UsageCompleteness::Partial,
            vec![
                claude_fable_window(
                    "window:seven-day-overage-included",
                    84.0,
                    NOW - 6.0,
                ),
                claude_fable_window("weekly:claude-fable-5", 80.0, NOW - 6.0),
            ],
        ),
        NOW,
    )
    .unwrap();
    assert_eq!(canonical_tie.windows.len(), 1);
    assert_eq!(canonical_tie.windows[0].used_percent, 80.0);

    let other = usage_observation(
        "other",
        "ctx-other",
        1,
        NOW - 5.0,
        UsageCompleteness::Partial,
        vec![claude_fable_window(
            "window:seven-day-overage-included",
            84.0,
            NOW - 5.0,
        )],
    );
    let validated = validate_usage_observation(other, NOW).unwrap();
    assert_eq!(
        validated.windows[0].key,
        "window:seven-day-overage-included"
    );
}
