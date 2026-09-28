//! Goal id and event id tests.

use std::collections::BTreeSet;

use crate::goal::ids::{
    event_id_timestamp_ms, mint_event_id, mint_event_id_with, mint_goal_id,
    parse_event_id, parse_goal_id, test_event_id, GOAL_EVENT_ID_LEN,
    GOAL_ID_ALPHABET, GOAL_ID_LEN,
};

#[test]
fn goal_id_mints_at_five_crockford_chars() {
    for _ in 0..25 {
        let id = mint_goal_id();
        assert_eq!(id.chars().count(), GOAL_ID_LEN);
        assert!(id.chars().all(|char| GOAL_ID_ALPHABET.contains(char)));
    }
}

#[test]
fn goal_id_mints_are_unique() {
    let ids: BTreeSet<String> = (0..100).map(|_| mint_goal_id()).collect();
    assert_eq!(ids.len(), 100);
}

#[test]
fn goal_id_parse_folds_uppercase() {
    assert_eq!(parse_goal_id("7K2MQ").unwrap(), "7k2mq");
}

#[test]
fn goal_id_parse_rejects_bad_values() {
    assert!(parse_goal_id("").is_err());
    assert!(parse_goal_id("7k2m").is_err());
    assert!(parse_goal_id("7k2mqq").is_err());
    assert!(parse_goal_id("7k2m!").is_err());
    assert!(parse_goal_id("7k2mi").is_err());
    assert!(parse_goal_id("7k2ml").is_err());
    assert!(parse_goal_id("7k2mo").is_err());
    assert!(parse_goal_id("7k2mu").is_err());
}

#[test]
fn event_id_mints_at_26_crockford_chars() {
    let id = mint_event_id();
    assert_eq!(id.chars().count(), GOAL_EVENT_ID_LEN);
    assert!(id.chars().all(|char| GOAL_ID_ALPHABET.contains(char)));
    assert_eq!(parse_event_id(&id).unwrap(), id);
}

#[test]
fn event_id_carries_its_timestamp() {
    let id = mint_event_id_with(1_758_999_000_000, &[7u8; 10]);
    assert_eq!(event_id_timestamp_ms(&id), Some(1_758_999_000_000));
}

#[test]
fn event_id_mints_are_monotonic_in_process() {
    let first = mint_event_id();
    let second = mint_event_id();
    assert!(second > first, "{second} should sort after {first}");
}

#[test]
fn event_id_parse_rejects_bad_values() {
    assert!(parse_event_id("").is_err());
    assert!(parse_event_id("01j8x9").is_err());
    let long = "0".repeat(27);
    assert!(parse_event_id(&long).is_err());
    let bad = format!("{}!", "0".repeat(25));
    assert!(parse_event_id(&bad).is_err());
}

#[test]
fn test_event_id_helper_is_deterministic() {
    assert_eq!(test_event_id(1000, 1), test_event_id(1000, 1));
    assert_ne!(test_event_id(1000, 1), test_event_id(1000, 2));
    assert!(test_event_id(2000, 1) > test_event_id(1000, 1));
}
