//! Single-valued notification panel tab ownership.
//!
//! Every notification belongs to exactly one panel tab. The precedence is
//! declared here so the notification panel, the top-bar indicator, and the
//! mobile snapshot cannot disagree about which bucket a row lives in.

use std::collections::BTreeMap;

use super::wire::{
    notification_activity_at, NotificationCountsWire,
    NotificationTabClassificationWire, NotificationTabWire, NotificationWire,
    NOTIFICATION_STORE_WIRE_SCHEMA_VERSION,
};

pub const HITL_TAB_KEY: &str = "hitl";
pub const ERRORS_TAB_KEY: &str = "errors";
pub const GENERAL_TAB_KEY: &str = "general";
pub const DONE_TAB_KEY: &str = "done";
pub const MUTED_TAB_KEY: &str = "__muted__";
pub const SNOOZED_TAB_KEY: &str = "__snoozed__";

pub const TAB_KIND_HITL: &str = "hitl";
pub const TAB_KIND_PANEL: &str = "panel";
pub const TAB_KIND_ERRORS: &str = "errors";
pub const TAB_KIND_GENERAL: &str = "general";
pub const TAB_KIND_TAG: &str = "tag";
pub const TAB_KIND_MUTED: &str = "muted";
pub const TAB_KIND_SNOOZED: &str = "snoozed";

/// Gate actions that share the synthetic human-in-the-loop tab.
const HITL_ACTIONS: [&str; 7] = [
    "PlanApproval",
    "EpicApproval",
    "UserQuestion",
    "HITL",
    "LaunchApproval",
    "TaskTriage",
    "CustomGate",
];

/// Panel names a gate may not declare, because they name a synthetic tab.
const RESERVED_PANELS: [&str; 5] =
    ["errors", "general", "hitl", "muted", "snoozed"];

const MAX_PANEL_LEN: usize = 32;

const GATE_PANEL_ACTION_DATA_KEY: &str = "panel";

/// Return the single tab key that owns this notification, with its kind.
pub fn tab_key_for(notification: &NotificationWire) -> (String, &'static str) {
    if notification.muted {
        if notification.snooze_until.is_some() {
            return (SNOOZED_TAB_KEY.to_string(), TAB_KIND_SNOOZED);
        }
        return (MUTED_TAB_KEY.to_string(), TAB_KIND_MUTED);
    }
    if let Some(panel) = declared_panel(notification) {
        return (panel, TAB_KIND_PANEL);
    }
    if let Some(action) = notification.action.as_deref() {
        if HITL_ACTIONS.contains(&action) {
            return (HITL_TAB_KEY.to_string(), TAB_KIND_HITL);
        }
    }
    if super::mobile::mobile_notification_error_from_wire(notification) {
        return (ERRORS_TAB_KEY.to_string(), TAB_KIND_ERRORS);
    }
    if let Some(tag) = first_display_tag(notification) {
        return (tag, TAB_KIND_TAG);
    }
    (GENERAL_TAB_KEY.to_string(), TAB_KIND_GENERAL)
}

/// Classify rows into ordered tabs plus a row id to tab key map.
///
/// Every supplied row is counted. Callers that want the unread-only view
/// filter their rows first, as [`tabs_and_counts_for`] does.
pub fn classify_notification_tabs(
    rows: &[NotificationWire],
) -> NotificationTabClassificationWire {
    let mut accumulators: BTreeMap<String, TabAccumulator> = BTreeMap::new();
    let mut row_tab_keys: BTreeMap<String, String> = BTreeMap::new();
    for row in rows {
        let key = accumulate(&mut accumulators, row);
        row_tab_keys.insert(row.id.clone(), key);
    }
    NotificationTabClassificationWire {
        schema_version: NOTIFICATION_STORE_WIRE_SCHEMA_VERSION,
        tabs: ordered_tabs(&accumulators),
        row_tab_keys,
    }
}

/// Build the ordered unread tab list and the legacy counters in one pass.
pub(crate) fn tabs_and_counts_for(
    rows: &[NotificationWire],
) -> (Vec<NotificationTabWire>, NotificationCountsWire) {
    let mut accumulators: BTreeMap<String, TabAccumulator> = BTreeMap::new();
    let mut counts = NotificationCountsWire::default();
    for row in rows {
        if row.read || row.silent {
            continue;
        }
        accumulate(&mut accumulators, row);
        if row.muted {
            counts.muted += 1;
        } else if super::mobile::mobile_notification_error_from_wire(row) {
            counts.errors += 1;
        } else if super::mobile::mobile_notification_priority_from_wire(row) {
            counts.priority += 1;
        } else {
            counts.rest += 1;
        }
    }
    (ordered_tabs(&accumulators), counts)
}

#[derive(Debug, Default)]
struct TabAccumulator {
    kind: &'static str,
    count: u64,
    oldest_activity_at: Option<String>,
    next_wake_at: Option<String>,
    color: Option<String>,
}

fn accumulate(
    accumulators: &mut BTreeMap<String, TabAccumulator>,
    row: &NotificationWire,
) -> String {
    let (key, kind) = tab_key_for(row);
    let accumulator =
        accumulators
            .entry(key.clone())
            .or_insert_with(|| TabAccumulator {
                kind,
                ..TabAccumulator::default()
            });
    // A declared panel and a stored tag can name the same tab. The tab then
    // sorts and reads as the actionable panel whichever row arrived first.
    if kind_rank(kind) < kind_rank(accumulator.kind) {
        accumulator.kind = kind;
    }
    accumulator.count += 1;
    let activity_at = notification_activity_at(row);
    if is_smaller(activity_at, accumulator.oldest_activity_at.as_deref()) {
        accumulator.oldest_activity_at = Some(activity_at.to_string());
    }
    if let Some(until) = row.snooze_until.as_deref() {
        if is_smaller(until, accumulator.next_wake_at.as_deref()) {
            accumulator.next_wake_at = Some(until.to_string());
        }
    }
    key
}

/// Rank tab kinds so a key claimed by two kinds resolves order-independently.
fn kind_rank(kind: &'static str) -> u8 {
    match kind {
        TAB_KIND_HITL => 0,
        TAB_KIND_PANEL => 1,
        TAB_KIND_ERRORS => 2,
        TAB_KIND_GENERAL => 3,
        TAB_KIND_TAG => 4,
        TAB_KIND_SNOOZED => 5,
        _ => 6,
    }
}

/// Return whether ``candidate`` should replace the current minimum.
fn is_smaller(candidate: &str, current: Option<&str>) -> bool {
    match current {
        None => true,
        Some(current) => candidate < current,
    }
}

fn ordered_tabs(
    accumulators: &BTreeMap<String, TabAccumulator>,
) -> Vec<NotificationTabWire> {
    ordered_tab_keys(accumulators)
        .into_iter()
        .map(|key| {
            let accumulator = &accumulators[&key];
            NotificationTabWire {
                kind: accumulator.kind.to_string(),
                count: accumulator.count,
                oldest_activity_at: accumulator.oldest_activity_at.clone(),
                next_wake_at: if accumulator.kind == TAB_KIND_SNOOZED {
                    accumulator.next_wake_at.clone()
                } else {
                    None
                },
                color: accumulator.color.clone(),
                key,
            }
        })
        .collect()
}

/// Order tabs exactly as the notification panel renders them.
fn ordered_tab_keys(
    accumulators: &BTreeMap<String, TabAccumulator>,
) -> Vec<String> {
    let mut ordered: Vec<String> = Vec::new();

    if accumulators.contains_key(HITL_TAB_KEY) {
        ordered.push(HITL_TAB_KEY.to_string());
    }
    ordered.extend(sorted_by_label(
        accumulators
            .iter()
            .filter(|(_, accumulator)| accumulator.kind == TAB_KIND_PANEL)
            .map(|(key, _)| key.clone()),
    ));
    for key in [ERRORS_TAB_KEY, GENERAL_TAB_KEY, DONE_TAB_KEY] {
        if accumulators.contains_key(key) && !ordered.iter().any(|k| k == key) {
            ordered.push(key.to_string());
        }
    }
    ordered.extend(sorted_by_label(
        accumulators
            .keys()
            .filter(|key| {
                key.as_str() != MUTED_TAB_KEY
                    && key.as_str() != SNOOZED_TAB_KEY
                    && !ordered.iter().any(|k| k == *key)
            })
            .cloned(),
    ));
    for key in [SNOOZED_TAB_KEY, MUTED_TAB_KEY] {
        if accumulators.contains_key(key) {
            ordered.push(key.to_string());
        }
    }
    ordered
}

fn sorted_by_label(keys: impl Iterator<Item = String>) -> Vec<String> {
    let mut keys: Vec<String> = keys.collect();
    keys.sort_by_key(|key| tab_label(key).to_lowercase());
    keys
}

/// Return the display label the panel renders for one tab key.
pub fn tab_label(key: &str) -> String {
    match key {
        HITL_TAB_KEY => return "HITL".to_string(),
        ERRORS_TAB_KEY => return "Errors".to_string(),
        GENERAL_TAB_KEY => return "General".to_string(),
        MUTED_TAB_KEY => return "Muted".to_string(),
        SNOOZED_TAB_KEY => return "Snoozed".to_string(),
        _ => {}
    }
    let label = key
        .split(['-', '_'])
        .filter(|word| !word.is_empty())
        .map(capitalize)
        .collect::<Vec<String>>()
        .join(" ");
    if label.is_empty() {
        key.to_string()
    } else {
        label
    }
}

fn capitalize(word: &str) -> String {
    let mut chars = word.chars();
    match chars.next() {
        None => String::new(),
        Some(first) => {
            first.to_uppercase().collect::<String>() + chars.as_str()
        }
    }
}

/// Return a declared panel key, tolerating malformed stored data.
fn declared_panel(notification: &NotificationWire) -> Option<String> {
    let raw = notification.action_data.get(GATE_PANEL_ACTION_DATA_KEY)?;
    let panel = raw.trim().to_lowercase();
    if panel.len() > MAX_PANEL_LEN {
        return None;
    }
    if RESERVED_PANELS.contains(&panel.as_str()) || panel.starts_with("__") {
        return None;
    }
    let mut characters = panel.chars();
    if !characters.next()?.is_ascii_alphanumeric() {
        return None;
    }
    if !characters.all(|character| {
        character.is_ascii_alphanumeric() || matches!(character, '_' | '-')
    }) {
        return None;
    }
    Some(panel)
}

/// Return the first non-blank display tag, in stored order.
fn first_display_tag(notification: &NotificationWire) -> Option<String> {
    notification
        .tags
        .iter()
        .map(|tag| tag.trim())
        .find(|tag| !tag.is_empty())
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn notification(id: &str) -> NotificationWire {
        NotificationWire {
            id: id.to_string(),
            timestamp: "2026-03-17T10:00:00-04:00".to_string(),
            sender: "tester".to_string(),
            ..NotificationWire::default()
        }
    }

    fn tab_keys(rows: &[NotificationWire]) -> Vec<String> {
        classify_notification_tabs(rows)
            .tabs
            .into_iter()
            .map(|tab| tab.key)
            .collect()
    }

    #[test]
    fn snoozed_precedes_muted_for_a_muted_row_with_a_wake_time() {
        let mut snoozed = notification("s");
        snoozed.muted = true;
        snoozed.snooze_until = Some("2026-03-18T10:00:00-04:00".to_string());
        let mut muted = notification("m");
        muted.muted = true;

        assert_eq!(tab_key_for(&snoozed).0, SNOOZED_TAB_KEY);
        assert_eq!(tab_key_for(&snoozed).1, TAB_KIND_SNOOZED);
        assert_eq!(tab_key_for(&muted).0, MUTED_TAB_KEY);
        assert_eq!(tab_key_for(&muted).1, TAB_KIND_MUTED);
    }

    #[test]
    fn declared_panel_outranks_hitl_errors_and_tags() {
        let mut row = notification("p");
        row.action = Some("TaskTriage".to_string());
        row.tags = vec!["alpha".to_string()];
        row.action_data
            .insert("panel".to_string(), " Beads ".to_string());

        assert_eq!(tab_key_for(&row), ("beads".to_string(), TAB_KIND_PANEL));
    }

    #[test]
    fn reserved_and_malformed_panels_fall_through() {
        for panel in ["hitl", "snoozed", "__muted__", "Bad Panel", "-lead"] {
            let mut row = notification("p");
            row.action_data
                .insert("panel".to_string(), panel.to_string());
            assert_eq!(
                tab_key_for(&row),
                (GENERAL_TAB_KEY.to_string(), TAB_KIND_GENERAL),
                "panel {panel} should not be honored",
            );
        }
    }

    #[test]
    fn hitl_then_errors_then_first_tag_then_general() {
        let mut hitl = notification("h");
        hitl.action = Some("PlanApproval".to_string());
        let mut error = notification("e");
        error.action = Some("ViewErrorReport".to_string());
        error.sender = "axe".to_string();
        let mut tagged = notification("t");
        tagged.tags = vec!["  ".to_string(), "beta".to_string()];

        assert_eq!(tab_key_for(&hitl).0, HITL_TAB_KEY);
        assert_eq!(tab_key_for(&error).0, ERRORS_TAB_KEY);
        assert_eq!(tab_key_for(&tagged), ("beta".to_string(), TAB_KIND_TAG));
        assert_eq!(tab_key_for(&notification("g")).0, GENERAL_TAB_KEY);
    }

    #[test]
    fn a_two_tag_row_lands_in_exactly_one_tab() {
        let mut row = notification("x");
        row.tags = vec!["alpha".to_string(), "beta".to_string()];

        let classification = classify_notification_tabs(&[row]);

        assert_eq!(
            classification
                .tabs
                .iter()
                .map(|tab| (tab.key.as_str(), tab.count))
                .collect::<Vec<_>>(),
            vec![("alpha", 1)],
        );
        assert_eq!(classification.row_tab_keys["x"], "alpha");
    }

    #[test]
    fn a_snoozed_row_leaves_its_panel_tab() {
        let mut row = notification("b");
        row.action = Some("TaskTriage".to_string());
        row.action_data
            .insert("panel".to_string(), "beads".to_string());
        row.muted = true;
        row.snooze_until = Some("2026-03-18T10:00:00-04:00".to_string());

        assert_eq!(tab_keys(&[row]), vec![SNOOZED_TAB_KEY.to_string()]);
    }

    #[test]
    fn tab_order_matches_the_panel() {
        let mut hitl = notification("h");
        hitl.action = Some("PlanApproval".to_string());
        let mut zeta_panel = notification("zp");
        zeta_panel
            .action_data
            .insert("panel".to_string(), "zeta".to_string());
        let mut beads_panel = notification("bp");
        beads_panel
            .action_data
            .insert("panel".to_string(), "beads".to_string());
        let mut error = notification("e");
        error.action = Some("ViewErrorReport".to_string());
        error.sender = "axe".to_string();
        let general = notification("g");
        let mut done = notification("d");
        done.tags = vec!["done".to_string()];
        let mut alpha = notification("a");
        alpha.tags = vec!["alpha".to_string()];
        let mut muted = notification("m");
        muted.muted = true;
        let mut snoozed = notification("s");
        snoozed.muted = true;
        snoozed.snooze_until = Some("2026-03-18T10:00:00-04:00".to_string());

        let rows = vec![
            muted,
            snoozed,
            alpha,
            done,
            general,
            error,
            zeta_panel,
            beads_panel,
            hitl,
        ];

        assert_eq!(
            tab_keys(&rows),
            vec![
                "hitl".to_string(),
                "beads".to_string(),
                "zeta".to_string(),
                "errors".to_string(),
                "general".to_string(),
                "done".to_string(),
                "alpha".to_string(),
                "__snoozed__".to_string(),
                "__muted__".to_string(),
            ],
        );
    }

    #[test]
    fn a_panel_and_tag_collision_sorts_as_a_panel_either_way() {
        let mut panel = notification("p");
        panel.action = Some("CustomGate".to_string());
        panel
            .action_data
            .insert("panel".to_string(), "review".to_string());
        let mut tagged = notification("t");
        tagged.tags = vec!["review".to_string()];
        let mut error = notification("e");
        error.action = Some("ViewErrorReport".to_string());
        error.sender = "axe".to_string();

        for rows in [
            vec![tagged.clone(), error.clone(), panel.clone()],
            vec![panel, error, tagged],
        ] {
            let tabs = classify_notification_tabs(&rows).tabs;
            assert_eq!(
                tabs.iter()
                    .map(|tab| (tab.key.as_str(), tab.kind.as_str(), tab.count))
                    .collect::<Vec<_>>(),
                vec![("review", TAB_KIND_PANEL, 2), ("errors", "errors", 1)],
            );
        }
    }

    #[test]
    fn counts_and_tabs_agree_and_skip_read_or_silent_rows() {
        let mut hitl = notification("h");
        hitl.action = Some("PlanApproval".to_string());
        let mut error = notification("e");
        error.action = Some("ViewErrorReport".to_string());
        error.sender = "axe".to_string();
        let general = notification("g");
        let mut muted = notification("m");
        muted.muted = true;
        let mut read = notification("r");
        read.read = true;
        let mut silent = notification("si");
        silent.silent = true;

        let (tabs, counts) =
            tabs_and_counts_for(&[hitl, error, general, muted, read, silent]);

        let total: u64 = tabs.iter().map(|tab| tab.count).sum();
        assert_eq!(
            total,
            counts.priority + counts.errors + counts.rest + counts.muted,
        );
        assert_eq!(total, 4);
    }

    #[test]
    fn oldest_activity_and_next_wake_are_minimums() {
        let mut early = notification("early");
        early.tags = vec!["alpha".to_string()];
        early.timestamp = "2026-03-17T08:00:00-04:00".to_string();
        let mut late = notification("late");
        late.tags = vec!["alpha".to_string()];
        late.timestamp = "2026-03-17T09:00:00-04:00".to_string();
        // A resurfaced row counts as recent activity, not as its sent time.
        let mut resurfaced = notification("resurfaced");
        resurfaced.tags = vec!["alpha".to_string()];
        resurfaced.timestamp = "2026-03-01T00:00:00-04:00".to_string();
        resurfaced.resurfaced_at =
            Some("2026-03-17T12:00:00-04:00".to_string());
        let mut soon = notification("soon");
        soon.muted = true;
        soon.snooze_until = Some("2026-03-18T10:00:00-04:00".to_string());
        let mut later = notification("later");
        later.muted = true;
        later.snooze_until = Some("2026-03-19T10:00:00-04:00".to_string());

        let tabs =
            classify_notification_tabs(&[early, late, resurfaced, soon, later])
                .tabs;
        let alpha = tabs.iter().find(|tab| tab.key == "alpha").unwrap();
        let snoozed =
            tabs.iter().find(|tab| tab.key == SNOOZED_TAB_KEY).unwrap();

        assert_eq!(
            alpha.oldest_activity_at.as_deref(),
            Some("2026-03-17T08:00:00-04:00"),
        );
        assert_eq!(alpha.next_wake_at, None);
        assert_eq!(
            snoozed.next_wake_at.as_deref(),
            Some("2026-03-18T10:00:00-04:00"),
        );
    }

    #[test]
    fn labels_title_case_tag_words() {
        assert_eq!(tab_label("hitl"), "HITL");
        assert_eq!(tab_label(GENERAL_TAB_KEY), "General");
        assert_eq!(tab_label(SNOOZED_TAB_KEY), "Snoozed");
        assert_eq!(tab_label("plan-review_notes"), "Plan Review Notes");
        assert_eq!(tab_label("--"), "--");
    }
}
