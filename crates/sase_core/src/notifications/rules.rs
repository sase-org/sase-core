//! Client-side notification delivery rules.
//!
//! A rule matches notifications by tab, sender, action, tag, title, or note
//! text and decides whether a TUI toast is shown and how the arrival is
//! announced. Delivery is purely a presentation decision: no rule ever changes
//! whether a notification is created, stored, read, muted, or snoozed.
//!
//! Resolution is "first matching rule that sets a field wins that field".
//! Rules are consulted in descending `priority`, ties broken by position, and
//! `toast` and `sound` are resolved independently. A field no matching rule
//! sets keeps the built-in default (`toast: true`, `sound: bell`), which is
//! what every notification did before rules existed.

use std::cmp::Reverse;
use std::fmt;

use serde::de::{Deserializer, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};

use super::tabs::{tab_key_for, HITL_TAB_KEY};
use super::wire::{NotificationWire, NOTIFICATION_STORE_WIRE_SCHEMA_VERSION};

/// Reserved `sound` word for the terminal bell.
const SOUND_BELL: &str = "bell";
/// Reserved `sound` word for silence.
const SOUND_NONE: &str = "none";
/// `tab` alias for the core's `hitl` key; the panel labels that tab "Gates".
const TAB_GATES_ALIAS: &str = "gates";

/// One delivery rule, as authored under `ace.notification_rules`.
///
/// Unknown keys are rejected rather than ignored so a misspelled key cannot
/// leave a rule quietly matching nothing.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotificationRuleWire {
    /// Label used in diagnostics and in [`NotificationDeliveryWire`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Free prose explaining why the rule exists; never consulted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Higher is consulted earlier; ties keep list order.
    #[serde(default)]
    pub priority: i64,
    /// Omitted or empty matches every notification.
    #[serde(default)]
    pub r#match: NotificationRuleMatchWire,
    /// Whether a TUI toast is shown; unset leaves the field to later rules.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub toast: Option<bool>,
    /// `bell`, `none`, or a sound-file path; unset (or blank) leaves the
    /// field to later rules. See [`NotificationSoundWire::from_setting`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sound: Option<String>,
}

/// The criteria of one rule: all-of across criteria, any-of within a list.
///
/// Every value is a case-insensitive glob. Each criterion deserializes from a
/// bare string or a list of strings. A criterion given as an empty list is
/// any-of nothing, so it matches no notification: a rule may fail to apply
/// but never applies more broadly than written.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotificationRuleMatchWire {
    /// The owning panel tab key from [`tab_key_for`]; `gates` means `hitl`.
    #[serde(
        default,
        deserialize_with = "string_or_list",
        skip_serializing_if = "Option::is_none"
    )]
    pub tab: Option<Vec<String>>,
    /// `NotificationWire::sender`.
    #[serde(
        default,
        deserialize_with = "string_or_list",
        skip_serializing_if = "Option::is_none"
    )]
    pub sender: Option<Vec<String>>,
    /// `NotificationWire::action`, or `""` for a row with no action.
    #[serde(
        default,
        deserialize_with = "string_or_list",
        skip_serializing_if = "Option::is_none"
    )]
    pub action: Option<Vec<String>>,
    /// Any one of `NotificationWire::tags`.
    #[serde(
        default,
        deserialize_with = "string_or_list",
        skip_serializing_if = "Option::is_none"
    )]
    pub tags: Option<Vec<String>>,
    /// `notes[0]`, the row headline, or `""` for a row with no notes.
    #[serde(
        default,
        deserialize_with = "string_or_list",
        skip_serializing_if = "Option::is_none"
    )]
    pub title: Option<Vec<String>>,
    /// Any one entry of `NotificationWire::notes`.
    #[serde(
        default,
        deserialize_with = "string_or_list",
        skip_serializing_if = "Option::is_none"
    )]
    pub note: Option<Vec<String>>,
}

/// How a notification arrival is announced.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NotificationSoundWire {
    /// The terminal bell; what every arrival did before rules existed.
    #[default]
    Bell,
    /// Silence.
    #[serde(rename = "none")]
    Silent,
    /// Play this sound file. The path is carried verbatim: expanding `~` and
    /// `$VAR` is the player's job, not the matcher's.
    File { path: String },
}

impl NotificationSoundWire {
    /// Parse one `sound:` setting.
    ///
    /// `bell` and `none` are reserved words (case-insensitive); anything else
    /// is a file path, so a file literally named `bell` is addressed as
    /// `./bell`. A blank setting names nothing and returns `None`, leaving
    /// the field to later rules.
    pub fn from_setting(setting: &str) -> Option<Self> {
        if setting.trim().is_empty() {
            return None;
        }
        if setting.eq_ignore_ascii_case(SOUND_BELL) {
            return Some(Self::Bell);
        }
        if setting.eq_ignore_ascii_case(SOUND_NONE) {
            return Some(Self::Silent);
        }
        Some(Self::File {
            path: setting.to_string(),
        })
    }
}

/// The resolved delivery for one notification.
///
/// `toast_rule` and `sound_rule` name the rule that decided each field (its
/// `name`, else `rule[<index>]`, the zero-based position in the rule list
/// that was resolved), and are `None` when the field kept the built-in
/// default. That is all `sase notify rules --explain` needs, with no second
/// evaluation pass.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationDeliveryWire {
    pub schema_version: u32,
    pub toast: bool,
    pub sound: NotificationSoundWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub toast_rule: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sound_rule: Option<String>,
}

impl Default for NotificationDeliveryWire {
    /// The built-in delivery: toast and ring the bell, decided by no rule.
    fn default() -> Self {
        Self {
            schema_version: NOTIFICATION_STORE_WIRE_SCHEMA_VERSION,
            toast: true,
            sound: NotificationSoundWire::Bell,
            toast_rule: None,
            sound_rule: None,
        }
    }
}

/// Resolve the delivery of one notification against `rules`.
pub fn resolve_notification_delivery(
    rules: &[NotificationRuleWire],
    row: &NotificationWire,
) -> NotificationDeliveryWire {
    resolve_compiled(&compile_rules(rules), row)
}

/// Resolve the delivery of every row in one pass, in row order.
///
/// The rules are ordered and compiled once for the whole batch, so a poll
/// tick pays one FFI hop and one rule compilation however many rows arrive.
pub fn resolve_notification_deliveries(
    rules: &[NotificationRuleWire],
    rows: &[NotificationWire],
) -> Vec<NotificationDeliveryWire> {
    let compiled = compile_rules(rules);
    rows.iter()
        .map(|row| resolve_compiled(&compiled, row))
        .collect()
}

/// Accept a bare string or a list of strings; `null` means unset.
fn string_or_list<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<String>>, D::Error>
where
    D: Deserializer<'de>,
{
    struct StringOrList;

    impl<'de> Visitor<'de> for StringOrList {
        type Value = Option<Vec<String>>;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a string or a list of strings")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
            Ok(Some(vec![value.to_string()]))
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_none<E>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut values = Vec::with_capacity(seq.size_hint().unwrap_or(0));
            while let Some(value) = seq.next_element::<String>()? {
                values.push(value);
            }
            Ok(Some(values))
        }
    }

    deserializer.deserialize_any(StringOrList)
}

/// One rule with its globs compiled and its label chosen.
struct CompiledRule {
    label: String,
    toast: Option<bool>,
    sound: Option<NotificationSoundWire>,
    criteria: CompiledCriteria,
}

#[derive(Default)]
struct CompiledCriteria {
    tab: Option<Vec<Glob>>,
    sender: Option<Vec<Glob>>,
    action: Option<Vec<Glob>>,
    tags: Option<Vec<Glob>>,
    title: Option<Vec<Glob>>,
    note: Option<Vec<Glob>>,
}

/// Compile `rules` into evaluation order: descending `priority`, then list
/// position. A rule that sets no usable field can never decide anything, so
/// it is dropped here rather than matched against every row.
fn compile_rules(rules: &[NotificationRuleWire]) -> Vec<CompiledRule> {
    let mut order: Vec<usize> = (0..rules.len()).collect();
    order.sort_by_key(|&index| (Reverse(rules[index].priority), index));
    order
        .into_iter()
        .filter_map(|index| compile_rule(index, &rules[index]))
        .collect()
}

fn compile_rule(
    index: usize,
    rule: &NotificationRuleWire,
) -> Option<CompiledRule> {
    let sound = rule
        .sound
        .as_deref()
        .and_then(NotificationSoundWire::from_setting);
    if rule.toast.is_none() && sound.is_none() {
        return None;
    }
    let label = rule
        .name
        .as_deref()
        .filter(|name| !name.trim().is_empty())
        .map_or_else(|| format!("rule[{index}]"), str::to_string);
    let criteria = &rule.r#match;
    Some(CompiledRule {
        label,
        toast: rule.toast,
        sound,
        criteria: CompiledCriteria {
            tab: compile_globs(criteria.tab.as_deref(), normalize_tab),
            sender: compile_globs(criteria.sender.as_deref(), identity),
            action: compile_globs(criteria.action.as_deref(), identity),
            tags: compile_globs(criteria.tags.as_deref(), identity),
            title: compile_globs(criteria.title.as_deref(), identity),
            note: compile_globs(criteria.note.as_deref(), identity),
        },
    })
}

fn compile_globs(
    patterns: Option<&[String]>,
    normalize: fn(&str) -> &str,
) -> Option<Vec<Glob>> {
    patterns.map(|patterns| {
        patterns
            .iter()
            .map(|pattern| Glob::new(normalize(pattern)))
            .collect()
    })
}

fn identity(pattern: &str) -> &str {
    pattern
}

/// One-way `gates` -> `hitl`, case-insensitive; the panel labels the `hitl`
/// tab "Gates".
fn normalize_tab(pattern: &str) -> &str {
    if pattern.eq_ignore_ascii_case(TAB_GATES_ALIAS) {
        HITL_TAB_KEY
    } else {
        pattern
    }
}

fn resolve_compiled(
    rules: &[CompiledRule],
    row: &NotificationWire,
) -> NotificationDeliveryWire {
    let mut delivery = NotificationDeliveryWire::default();
    if rules.is_empty() {
        return delivery;
    }
    let facts = RowFacts::from_row(row);
    let mut toast_open = true;
    let mut sound_open = true;
    for rule in rules {
        let toast = rule.toast.filter(|_| toast_open);
        let sound = rule.sound.as_ref().filter(|_| sound_open);
        if toast.is_none() && sound.is_none() {
            continue;
        }
        if !rule.criteria.matches(&facts) {
            continue;
        }
        if let Some(toast) = toast {
            delivery.toast = toast;
            delivery.toast_rule = Some(rule.label.clone());
            toast_open = false;
        }
        if let Some(sound) = sound {
            delivery.sound = sound.clone();
            delivery.sound_rule = Some(rule.label.clone());
            sound_open = false;
        }
        if !toast_open && !sound_open {
            break;
        }
    }
    delivery
}

/// The case-folded values of one notification that criteria match against.
struct RowFacts {
    tab: Vec<char>,
    sender: Vec<char>,
    action: Vec<char>,
    tags: Vec<Vec<char>>,
    title: Vec<char>,
    notes: Vec<Vec<char>>,
}

impl RowFacts {
    fn from_row(row: &NotificationWire) -> Self {
        Self {
            tab: fold(&tab_key_for(row).0),
            sender: fold(&row.sender),
            action: fold(row.action.as_deref().unwrap_or("")),
            tags: row.tags.iter().map(|tag| fold(tag)).collect(),
            title: fold(row.notes.first().map_or("", String::as_str)),
            notes: row.notes.iter().map(|note| fold(note)).collect(),
        }
    }
}

impl CompiledCriteria {
    fn matches(&self, facts: &RowFacts) -> bool {
        matches_value(&self.tab, &facts.tab)
            && matches_value(&self.sender, &facts.sender)
            && matches_value(&self.action, &facts.action)
            && matches_any_value(&self.tags, &facts.tags)
            && matches_value(&self.title, &facts.title)
            && matches_any_value(&self.note, &facts.notes)
    }
}

/// An absent criterion matches everything; a present one is any-of.
fn matches_value(globs: &Option<Vec<Glob>>, value: &[char]) -> bool {
    match globs {
        None => true,
        Some(globs) => globs.iter().any(|glob| glob.matches(value)),
    }
}

/// Like [`matches_value`], over a row value that is itself a list.
fn matches_any_value(globs: &Option<Vec<Glob>>, values: &[Vec<char>]) -> bool {
    match globs {
        None => true,
        Some(globs) => globs
            .iter()
            .any(|glob| values.iter().any(|value| glob.matches(value))),
    }
}

fn fold(text: &str) -> Vec<char> {
    text.chars().flat_map(char::to_lowercase).collect()
}

/// A compiled, case-folded glob: `*`, `?`, `[...]`, and `[!...]`.
///
/// There is no escape character; a literal metacharacter is a one-member
/// class such as `[*]`. A `[` with no closing `]` is a literal `[`, as in
/// `fnmatch`, so no pattern is ever invalid. Pattern and text are both
/// lowercased before matching and compared per Unicode scalar, so `?` matches
/// one character rather than one byte.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Glob {
    tokens: Vec<GlobToken>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum GlobToken {
    Literal(char),
    AnyOne,
    AnyRun,
    Class(GlobClass),
}

/// Inclusive `(low, high)` ranges; a single member is `(c, c)`. A reversed
/// range such as `[z-a]` contains nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
struct GlobClass {
    negated: bool,
    ranges: Vec<(char, char)>,
}

impl Glob {
    fn new(pattern: &str) -> Self {
        let pattern = fold(pattern);
        let mut tokens: Vec<GlobToken> = Vec::new();
        let mut index = 0;
        while index < pattern.len() {
            match pattern[index] {
                '*' => {
                    // Runs of `*` mean the same as one.
                    if tokens.last() != Some(&GlobToken::AnyRun) {
                        tokens.push(GlobToken::AnyRun);
                    }
                    index += 1;
                }
                '?' => {
                    tokens.push(GlobToken::AnyOne);
                    index += 1;
                }
                '[' => match parse_class(&pattern, index) {
                    Some((class, next)) => {
                        tokens.push(GlobToken::Class(class));
                        index = next;
                    }
                    None => {
                        tokens.push(GlobToken::Literal('['));
                        index += 1;
                    }
                },
                literal => {
                    tokens.push(GlobToken::Literal(literal));
                    index += 1;
                }
            }
        }
        Self { tokens }
    }

    /// Return whether the whole of already case-folded `text` matches.
    ///
    /// Iterative with one backtrack point (the most recent `*`), so matching
    /// is `O(pattern * text)` in the worst case and never exponential.
    fn matches(&self, text: &[char]) -> bool {
        let tokens = &self.tokens;
        let mut token = 0;
        let mut at = 0;
        // Token just past the latest `*` and the text index it resumes from.
        let mut backtrack: Option<(usize, usize)> = None;
        while at < text.len() {
            match tokens.get(token) {
                Some(GlobToken::AnyRun) => {
                    token += 1;
                    backtrack = Some((token, at));
                    continue;
                }
                Some(one) if one.matches_char(text[at]) => {
                    token += 1;
                    at += 1;
                    continue;
                }
                _ => {}
            }
            let Some((resume_token, resume_at)) = backtrack else {
                return false;
            };
            // Let the last `*` swallow one more character and retry.
            token = resume_token;
            at = resume_at + 1;
            backtrack = Some((resume_token, at));
        }
        tokens[token..]
            .iter()
            .all(|token| *token == GlobToken::AnyRun)
    }
}

impl GlobToken {
    /// Match exactly one character; `AnyRun` is handled by the caller.
    fn matches_char(&self, character: char) -> bool {
        match self {
            Self::Literal(literal) => *literal == character,
            Self::AnyOne => true,
            Self::AnyRun => false,
            Self::Class(class) => class.matches(character),
        }
    }
}

impl GlobClass {
    fn matches(&self, character: char) -> bool {
        let contained = self
            .ranges
            .iter()
            .any(|&(low, high)| low <= character && character <= high);
        contained != self.negated
    }
}

/// Parse the class opening at `pattern[start] == '['`, returning it and the
/// index just past its closing `]`, or `None` when it is never closed.
fn parse_class(pattern: &[char], start: usize) -> Option<(GlobClass, usize)> {
    let mut index = start + 1;
    let negated = pattern.get(index) == Some(&'!');
    if negated {
        index += 1;
    }
    let members_start = index;
    let mut ranges = Vec::new();
    loop {
        let low = *pattern.get(index)?;
        // A `]` right after the opening is a member, not the terminator.
        if low == ']' && index > members_start {
            return Some((GlobClass { negated, ranges }, index + 1));
        }
        // `-` is a range only between two members; leading or trailing it is
        // a literal.
        let high = match (pattern.get(index + 1), pattern.get(index + 2)) {
            (Some('-'), Some(&high)) if high != ']' => Some(high),
            _ => None,
        };
        match high {
            Some(high) => {
                ranges.push((low, high));
                index += 3;
            }
            None => {
                ranges.push((low, low));
                index += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn glob_matches(pattern: &str, text: &str) -> bool {
        Glob::new(pattern).matches(&fold(text))
    }

    #[test]
    fn glob_matches_exact_text_case_insensitively() {
        assert!(glob_matches("beads", "beads"));
        assert!(glob_matches("Beads", "bEADS"));
        assert!(!glob_matches("beads", "bead"));
        assert!(!glob_matches("bead", "beads"));
    }

    #[test]
    fn glob_has_no_implicit_substring_match() {
        assert!(!glob_matches("Plan ready", "Plan ready for review"));
        assert!(glob_matches("Plan ready*", "Plan ready for review"));
        assert!(glob_matches("*ready*", "Plan ready for review"));
    }

    #[test]
    fn empty_pattern_matches_only_empty_text() {
        assert!(glob_matches("", ""));
        assert!(!glob_matches("", "x"));
        assert!(!glob_matches("x", ""));
    }

    #[test]
    fn bare_star_matches_everything_including_empty() {
        assert!(glob_matches("*", ""));
        assert!(glob_matches("*", "anything at all"));
        assert!(glob_matches("*", "line one\nline two"));
        assert!(glob_matches("***", "x"));
        assert!(glob_matches("a**b", "ab"));
    }

    #[test]
    fn star_backtracks_across_repeated_prefixes() {
        assert!(glob_matches("*ab", "aaab"));
        assert!(glob_matches("a*a*a", "aXaYa"));
        assert!(!glob_matches("a*a*a", "aXaY"));
        assert!(glob_matches("*a*b*c*", "xxaxxbxxcxx"));
        assert!(!glob_matches("*a*b*c*", "xxcxxbxxaxx"));
    }

    #[test]
    fn question_mark_matches_exactly_one_character() {
        assert!(glob_matches("t?g", "tag"));
        assert!(!glob_matches("t?g", "tg"));
        assert!(!glob_matches("t?g", "taag"));
        assert!(glob_matches("?", "x"));
        assert!(!glob_matches("?", ""));
    }

    #[test]
    fn classes_match_members_and_ranges() {
        assert!(glob_matches("[abc]x", "bx"));
        assert!(!glob_matches("[abc]x", "dx"));
        assert!(glob_matches("v[0-9]", "v7"));
        assert!(!glob_matches("v[0-9]", "vx"));
        assert!(glob_matches("[A-C]", "b"));
    }

    #[test]
    fn negated_classes_exclude_members() {
        assert!(glob_matches("[!abc]", "d"));
        assert!(!glob_matches("[!abc]", "a"));
        assert!(glob_matches("[!0-9]*", "x1"));
        assert!(!glob_matches("[!0-9]*", "1x"));
        // `^` is not a negation marker; it is an ordinary member.
        assert!(glob_matches("[^a]", "^"));
        assert!(!glob_matches("[^a]", "b"));
    }

    #[test]
    fn class_dash_is_literal_at_either_edge() {
        assert!(glob_matches("[-a]", "-"));
        assert!(glob_matches("[a-]", "-"));
        assert!(glob_matches("[a-]", "a"));
        assert!(!glob_matches("[a-]", "b"));
    }

    #[test]
    fn leading_close_bracket_is_a_member() {
        assert!(glob_matches("[]]", "]"));
        assert!(!glob_matches("[]]", "["));
        assert!(glob_matches("[!]]", "x"));
        assert!(!glob_matches("[!]]", "]"));
    }

    #[test]
    fn reversed_range_contains_nothing() {
        assert!(!glob_matches("[z-a]", "m"));
        assert!(glob_matches("[!z-a]", "m"));
    }

    #[test]
    fn unclosed_bracket_is_a_literal() {
        assert!(glob_matches("[abc", "[abc"));
        assert!(!glob_matches("[abc", "a"));
        assert!(glob_matches("a[", "a["));
        assert!(glob_matches("[", "["));
        assert!(glob_matches("[]", "[]"));
        assert!(glob_matches("[!]", "[!]"));
        assert!(glob_matches("x[y*", "x[yzz"));
    }

    #[test]
    fn bracketed_metacharacter_is_a_literal() {
        assert!(glob_matches("[*]", "*"));
        assert!(!glob_matches("[*]", "x"));
        assert!(glob_matches("a[?]b", "a?b"));
        assert!(!glob_matches("a[?]b", "axb"));
        assert!(glob_matches("[[]", "["));
    }

    #[test]
    fn non_ascii_is_matched_per_character_and_case_folded() {
        assert!(glob_matches("caf?", "café"));
        assert!(!glob_matches("caf?", "caf"));
        assert!(glob_matches("CAFÉ", "café"));
        assert!(glob_matches("café", "CAFÉ"));
        assert!(glob_matches("*é", "Été"));
        assert!(glob_matches("[α-ω]", "Σ"));
        assert!(glob_matches("日本?", "日本語"));
        assert!(!glob_matches("日本?", "日本"));
    }

    #[test]
    fn pathological_pattern_stays_linearish() {
        let text = "a".repeat(4_000);
        assert!(!glob_matches("*a*a*a*a*a*a*a*a*b", &text));
        assert!(glob_matches("*a*a*a*a*a*a*a*a*", &text));
    }

    fn rule(value: serde_json::Value) -> NotificationRuleWire {
        serde_json::from_value(value).expect("valid rule")
    }

    fn row(id: &str, sender: &str) -> NotificationWire {
        NotificationWire {
            id: id.to_string(),
            timestamp: "2026-09-20T12:00:00-04:00".to_string(),
            sender: sender.to_string(),
            ..NotificationWire::default()
        }
    }

    fn with_action(
        mut row: NotificationWire,
        action: &str,
    ) -> NotificationWire {
        row.action = Some(action.to_string());
        row
    }

    fn with_tags(mut row: NotificationWire, tags: &[&str]) -> NotificationWire {
        row.tags = tags.iter().map(|tag| tag.to_string()).collect();
        row
    }

    fn with_panel(mut row: NotificationWire, panel: &str) -> NotificationWire {
        row.action_data
            .insert("panel".to_string(), panel.to_string());
        row
    }

    fn with_notes(
        mut row: NotificationWire,
        notes: &[&str],
    ) -> NotificationWire {
        row.notes = notes.iter().map(|note| note.to_string()).collect();
        row
    }

    fn task_triage() -> NotificationWire {
        with_notes(
            with_panel(
                with_tags(
                    with_action(row("bead-1", "bead"), "TaskTriage"),
                    &["bead", "task", "bug"],
                ),
                "beads",
            ),
            &["Task triage: sase-14d.1 needs a decision"],
        )
    }

    fn axe_error() -> NotificationWire {
        with_notes(
            with_action(row("axe-1", "axe"), "ViewErrorReport"),
            &["Axe chop crashed"],
        )
    }

    /// One row per shape in the live-store field survey.
    fn survey_rows() -> Vec<NotificationWire> {
        vec![
            with_tags(
                row("wait-1", "wait_checks"),
                &["wait", "blocked", "terminal-dependency"],
            ),
            axe_error(),
            task_triage(),
            with_panel(
                with_tags(
                    with_action(row("bead-2", "bead"), "BeadStaleCleanup"),
                    &["bead", "task", "stale"],
                ),
                "beads",
            ),
            with_tags(
                row("cache-1", "poseidon-cache-watch"),
                &["poseidon", "storage"],
            ),
            with_tags(row("epic-1", "epic-launch"), &["epic", "launch"]),
            with_panel(
                with_tags(
                    with_action(
                        row("remote-1", "remote-attention"),
                        "RemoteAttention",
                    ),
                    &["attention", "apollo", "gate"],
                ),
                "attention",
            ),
            with_tags(
                with_action(row("gate-1", "gate"), "GateExecutionFailed"),
                &["gate", "execution", "error"],
            ),
            with_tags(
                with_action(row("user-1", "user-agent"), "JumpToAgent"),
                &["done"],
            ),
        ]
    }

    fn built_in() -> NotificationDeliveryWire {
        NotificationDeliveryWire::default()
    }

    #[test]
    fn built_in_delivery_is_toast_and_bell_decided_by_no_rule() {
        let delivery = built_in();
        assert!(delivery.toast);
        assert_eq!(delivery.sound, NotificationSoundWire::Bell);
        assert_eq!(delivery.toast_rule, None);
        assert_eq!(delivery.sound_rule, None);
        assert_eq!(
            delivery.schema_version,
            NOTIFICATION_STORE_WIRE_SCHEMA_VERSION
        );
    }

    #[test]
    fn no_rules_keep_toast_and_bell_for_every_survey_row() {
        let rows = survey_rows();
        let deliveries = resolve_notification_deliveries(&[], &rows);
        assert_eq!(deliveries.len(), rows.len());
        for (row, delivery) in rows.iter().zip(&deliveries) {
            assert_eq!(delivery, &built_in(), "row {}", row.id);
            assert_eq!(
                &resolve_notification_delivery(&[], row),
                delivery,
                "row {}",
                row.id
            );
        }
    }

    #[test]
    fn first_match_per_field_takes_one_field_from_each_rule() {
        let rules = vec![
            rule(json!({
                "name": "quiet-beads",
                "match": {"tab": "beads"},
                "toast": false,
            })),
            rule(json!({"name": "chime", "sound": "/sounds/glass.aiff"})),
        ];

        let delivery = resolve_notification_delivery(&rules, &task_triage());
        assert!(!delivery.toast);
        assert_eq!(delivery.toast_rule.as_deref(), Some("quiet-beads"));
        assert_eq!(
            delivery.sound,
            NotificationSoundWire::File {
                path: "/sounds/glass.aiff".to_string()
            }
        );
        assert_eq!(delivery.sound_rule.as_deref(), Some("chime"));

        // A row the narrow rule does not select still gets the catch-all.
        let delivery = resolve_notification_delivery(&rules, &axe_error());
        assert!(delivery.toast);
        assert_eq!(delivery.toast_rule, None);
        assert_eq!(delivery.sound_rule.as_deref(), Some("chime"));
    }

    #[test]
    fn earlier_rule_wins_a_field_and_later_rules_cannot_reopen_it() {
        let rules = vec![
            rule(json!({"name": "first", "toast": false})),
            rule(json!({"name": "second", "toast": true, "sound": "none"})),
            rule(json!({"name": "third", "sound": "bell"})),
        ];

        let delivery = resolve_notification_delivery(&rules, &axe_error());
        assert!(!delivery.toast);
        assert_eq!(delivery.toast_rule.as_deref(), Some("first"));
        assert_eq!(delivery.sound, NotificationSoundWire::Silent);
        assert_eq!(delivery.sound_rule.as_deref(), Some("second"));
    }

    #[test]
    fn unnamed_rules_are_labelled_by_list_position() {
        let rules = vec![
            rule(json!({"match": {"sender": "nobody"}, "toast": false})),
            rule(json!({"name": "   ", "toast": false})),
        ];
        let delivery = resolve_notification_delivery(&rules, &axe_error());
        assert_eq!(delivery.toast_rule.as_deref(), Some("rule[1]"));
    }

    #[test]
    fn rule_label_index_ignores_priority_reordering() {
        let rules = vec![
            rule(json!({"toast": true})),
            rule(json!({"priority": 5, "toast": false})),
        ];
        let delivery = resolve_notification_delivery(&rules, &axe_error());
        assert!(!delivery.toast);
        assert_eq!(delivery.toast_rule.as_deref(), Some("rule[1]"));
    }

    #[test]
    fn priority_moves_a_later_rule_ahead_of_an_earlier_one() {
        let mut rules = vec![
            rule(json!({"name": "early", "sound": "none"})),
            rule(json!({"name": "late", "sound": "bell"})),
        ];
        let delivery = resolve_notification_delivery(&rules, &axe_error());
        assert_eq!(delivery.sound, NotificationSoundWire::Silent);
        assert_eq!(delivery.sound_rule.as_deref(), Some("early"));

        rules[1].priority = 10;
        let delivery = resolve_notification_delivery(&rules, &axe_error());
        assert_eq!(delivery.sound, NotificationSoundWire::Bell);
        assert_eq!(delivery.sound_rule.as_deref(), Some("late"));
    }

    #[test]
    fn equal_priorities_keep_list_order_and_negative_priority_goes_last() {
        let rules = vec![
            rule(json!({"name": "low", "priority": -5, "toast": false})),
            rule(json!({"name": "a", "priority": 3, "toast": true})),
            rule(json!({"name": "b", "priority": 3, "toast": false})),
        ];
        let delivery = resolve_notification_delivery(&rules, &axe_error());
        assert!(delivery.toast);
        assert_eq!(delivery.toast_rule.as_deref(), Some("a"));

        let rules = vec![
            rule(json!({"name": "low", "priority": -5, "toast": false})),
            rule(json!({"name": "default", "toast": true})),
        ];
        let delivery = resolve_notification_delivery(&rules, &axe_error());
        assert_eq!(delivery.toast_rule.as_deref(), Some("default"));
    }

    #[test]
    fn beads_tab_rule_silences_task_triage_and_not_axe_errors() {
        let rules = vec![rule(json!({
            "name": "quiet-task-beads",
            "match": {"tab": "beads"},
            "toast": false,
            "sound": "none",
        }))];

        let triage = resolve_notification_delivery(&rules, &task_triage());
        assert!(!triage.toast);
        assert_eq!(triage.sound, NotificationSoundWire::Silent);
        assert_eq!(triage.toast_rule.as_deref(), Some("quiet-task-beads"));
        assert_eq!(triage.sound_rule.as_deref(), Some("quiet-task-beads"));

        let axe = resolve_notification_delivery(&rules, &axe_error());
        assert_eq!(axe, built_in());

        // The tab criterion sees the panel the row lives in, not its sender.
        let stale = &survey_rows()[3];
        assert!(!resolve_notification_delivery(&rules, stale).toast);
    }

    #[test]
    fn suppressing_one_tab_leaves_every_other_survey_row_alone() {
        let rules = vec![rule(json!({
            "match": {"tab": "beads"}, "toast": false, "sound": "none",
        }))];
        let rows = survey_rows();
        let deliveries = resolve_notification_deliveries(&rules, &rows);
        for (row, delivery) in rows.iter().zip(&deliveries) {
            let suppressed = row.sender == "bead";
            assert_eq!(delivery.toast, !suppressed, "row {}", row.id);
            assert_eq!(
                delivery.sound == NotificationSoundWire::Silent,
                suppressed,
                "row {}",
                row.id
            );
        }
    }

    #[test]
    fn gates_and_hitl_select_the_same_rows_in_any_case() {
        let plan_approval = with_action(row("hitl-1", "plan"), "PlanApproval");
        let question = with_action(row("hitl-2", "agent"), "UserQuestion");
        let others = [axe_error(), task_triage(), row("plain", "misc")];

        for spelling in ["hitl", "HITL", "gates", "Gates", "GATES"] {
            let rules = vec![rule(json!({
                "match": {"tab": spelling}, "toast": false,
            }))];
            for hitl_row in [&plan_approval, &question] {
                assert!(
                    !resolve_notification_delivery(&rules, hitl_row).toast,
                    "{spelling} should select {}",
                    hitl_row.id
                );
            }
            for other in &others {
                assert!(
                    resolve_notification_delivery(&rules, other).toast,
                    "{spelling} should not select {}",
                    other.id
                );
            }
        }
    }

    #[test]
    fn gates_alias_is_normalized_only_when_it_is_the_whole_value() {
        let plan_approval = with_action(row("hitl-1", "plan"), "PlanApproval");
        let rules = vec![rule(json!({
            "match": {"tab": ["gate*", "gates-x"]}, "toast": false,
        }))];
        assert!(resolve_notification_delivery(&rules, &plan_approval).toast);
    }

    #[test]
    fn muted_and_snoozed_rows_are_reachable_by_their_synthetic_tabs() {
        let mut muted = row("m", "misc");
        muted.muted = true;
        let mut snoozed = row("s", "misc");
        snoozed.muted = true;
        snoozed.snooze_until = Some("2026-09-21T09:00:00-04:00".to_string());

        let rules = vec![
            rule(
                json!({"name": "s", "match": {"tab": "__snoozed__"}, "toast": false}),
            ),
            rule(
                json!({"name": "m", "match": {"tab": "__muted__"}, "sound": "none"}),
            ),
        ];
        let muted = resolve_notification_delivery(&rules, &muted);
        assert!(muted.toast);
        assert_eq!(muted.sound_rule.as_deref(), Some("m"));
        let snoozed = resolve_notification_delivery(&rules, &snoozed);
        assert_eq!(snoozed.toast_rule.as_deref(), Some("s"));
        assert_eq!(snoozed.sound, NotificationSoundWire::Bell);
    }

    #[test]
    fn criteria_are_all_of_and_lists_are_any_of() {
        let rules = vec![rule(json!({
            "match": {
                "sender": "bead",
                "tags": ["nope", "TASK"],
                "action": ["Other", "TaskTriage"],
            },
            "toast": false,
        }))];
        assert!(!resolve_notification_delivery(&rules, &task_triage()).toast);

        // Sender alone would match, but one failing criterion fails the rule.
        let mut wrong_action = task_triage();
        wrong_action.action = Some("BeadSnooze".to_string());
        assert!(resolve_notification_delivery(&rules, &wrong_action).toast);

        let mut no_matching_tag = task_triage();
        no_matching_tag.tags = vec!["bead".to_string()];
        assert!(resolve_notification_delivery(&rules, &no_matching_tag).toast);
    }

    #[test]
    fn tags_match_any_tag_not_just_the_first() {
        let rules = vec![rule(json!({
            "match": {"tags": "stale"}, "toast": false,
        }))];
        let stale = &survey_rows()[3];
        assert!(!resolve_notification_delivery(&rules, stale).toast);
        assert!(resolve_notification_delivery(&rules, &task_triage()).toast);
        // A row with no tags has nothing to match.
        assert!(resolve_notification_delivery(&rules, &row("bare", "x")).toast);
    }

    #[test]
    fn action_criterion_matches_empty_string_for_action_less_rows() {
        let rules = vec![rule(json!({
            "match": {"action": ""}, "toast": false,
        }))];
        assert!(
            !resolve_notification_delivery(&rules, &row("bare", "x")).toast
        );
        assert!(resolve_notification_delivery(&rules, &axe_error()).toast);

        let rules = vec![rule(json!({
            "match": {"action": "*"}, "toast": false,
        }))];
        assert!(
            !resolve_notification_delivery(&rules, &row("bare", "x")).toast
        );
    }

    #[test]
    fn title_is_the_first_note_and_note_is_any_note() {
        let notified = with_notes(
            row("n", "misc"),
            &["Plan ready for review", "Approve to continue"],
        );

        let exact = vec![rule(json!({
            "match": {"title": "Plan ready"}, "toast": false,
        }))];
        assert!(resolve_notification_delivery(&exact, &notified).toast);

        let prefix = vec![rule(json!({
            "match": {"title": "plan ready*"}, "toast": false,
        }))];
        assert!(!resolve_notification_delivery(&prefix, &notified).toast);

        // Only the first note is the title.
        let second = vec![rule(json!({
            "match": {"title": "Approve*"}, "toast": false,
        }))];
        assert!(resolve_notification_delivery(&second, &notified).toast);

        let any_note = vec![rule(json!({
            "match": {"note": "Approve*"}, "toast": false,
        }))];
        assert!(!resolve_notification_delivery(&any_note, &notified).toast);
    }

    #[test]
    fn title_is_empty_and_note_matches_nothing_for_a_note_less_row() {
        let bare = row("bare", "x");
        let empty_title = vec![rule(json!({
            "match": {"title": ""}, "toast": false,
        }))];
        assert!(!resolve_notification_delivery(&empty_title, &bare).toast);
        let any_note = vec![rule(json!({
            "match": {"note": "*"}, "toast": false,
        }))];
        assert!(resolve_notification_delivery(&any_note, &bare).toast);
    }

    #[test]
    fn empty_criterion_list_matches_nothing() {
        let rules = vec![rule(json!({
            "match": {"tags": []}, "toast": false,
        }))];
        for row in survey_rows() {
            assert!(
                resolve_notification_delivery(&rules, &row).toast,
                "row {}",
                row.id
            );
        }
    }

    #[test]
    fn omitted_or_empty_match_matches_every_row() {
        for rules in [
            vec![rule(json!({"toast": false}))],
            vec![rule(json!({"match": {}, "toast": false}))],
        ] {
            for row in survey_rows() {
                assert!(
                    !resolve_notification_delivery(&rules, &row).toast,
                    "row {}",
                    row.id
                );
            }
        }
    }

    #[test]
    fn silence_everything_except_gates_needs_no_negation() {
        let rules = vec![
            rule(json!({
                "name": "keep-gates",
                "match": {"tab": "gates"},
                "toast": true,
                "sound": "bell",
            })),
            rule(json!({"name": "hush", "toast": false, "sound": "none"})),
        ];
        let gate = with_action(row("hitl-1", "plan"), "PlanApproval");

        let delivery = resolve_notification_delivery(&rules, &gate);
        assert_eq!(delivery.toast_rule.as_deref(), Some("keep-gates"));
        assert!(delivery.toast);
        assert_eq!(delivery.sound, NotificationSoundWire::Bell);

        let delivery = resolve_notification_delivery(&rules, &axe_error());
        assert_eq!(delivery.toast_rule.as_deref(), Some("hush"));
        assert!(!delivery.toast);
        assert_eq!(delivery.sound, NotificationSoundWire::Silent);
    }

    #[test]
    fn a_rule_that_sets_nothing_never_decides_anything() {
        let rules = vec![
            rule(json!({"name": "no-op"})),
            rule(json!({"name": "blank-sound", "sound": "  "})),
            rule(json!({"name": "real", "sound": "none"})),
        ];
        let delivery = resolve_notification_delivery(&rules, &axe_error());
        assert_eq!(delivery.toast_rule, None);
        assert_eq!(delivery.sound_rule.as_deref(), Some("real"));
    }

    #[test]
    fn sound_settings_parse_reserved_words_and_paths() {
        assert_eq!(
            NotificationSoundWire::from_setting("bell"),
            Some(NotificationSoundWire::Bell)
        );
        assert_eq!(
            NotificationSoundWire::from_setting("BELL"),
            Some(NotificationSoundWire::Bell)
        );
        assert_eq!(
            NotificationSoundWire::from_setting("None"),
            Some(NotificationSoundWire::Silent)
        );
        assert_eq!(
            NotificationSoundWire::from_setting("./bell"),
            Some(NotificationSoundWire::File {
                path: "./bell".to_string()
            })
        );
        assert_eq!(
            NotificationSoundWire::from_setting("~/My Sounds/$NAME.wav"),
            Some(NotificationSoundWire::File {
                path: "~/My Sounds/$NAME.wav".to_string()
            })
        );
        assert_eq!(NotificationSoundWire::from_setting(""), None);
        assert_eq!(NotificationSoundWire::from_setting("   "), None);
    }

    #[test]
    fn batch_resolution_equals_per_row_resolution_in_row_order() {
        let rules = vec![
            rule(
                json!({"name": "quiet", "match": {"tab": "beads"}, "toast": false, "sound": "none"}),
            ),
            rule(json!({"name": "chime", "sound": "/sounds/glass.aiff"})),
        ];
        let rows = survey_rows();
        let batch = resolve_notification_deliveries(&rules, &rows);
        let single: Vec<_> = rows
            .iter()
            .map(|row| resolve_notification_delivery(&rules, row))
            .collect();
        assert_eq!(batch, single);
        assert!(resolve_notification_deliveries(&rules, &[]).is_empty());
    }

    #[test]
    fn unknown_criterion_keys_are_rejected_not_ignored() {
        let err = serde_json::from_value::<NotificationRuleWire>(json!({
            "match": {"tabs": "beads"}, "toast": false,
        }))
        .unwrap_err()
        .to_string();
        assert!(err.contains("unknown field `tabs`"), "{err}");

        let err = serde_json::from_value::<NotificationRuleWire>(json!({
            "match": {"files": ["a"]}, "toast": false,
        }))
        .unwrap_err()
        .to_string();
        assert!(err.contains("unknown field `files`"), "{err}");
    }

    #[test]
    fn unknown_rule_keys_are_rejected_not_ignored() {
        let err = serde_json::from_value::<NotificationRuleWire>(json!({
            "toats": false,
        }))
        .unwrap_err()
        .to_string();
        assert!(err.contains("unknown field `toats`"), "{err}");
    }

    #[test]
    fn criteria_accept_a_bare_string_or_a_list_and_reject_other_shapes() {
        let bare = rule(json!({"match": {"sender": "bead"}}));
        assert_eq!(bare.r#match.sender, Some(vec!["bead".to_string()]));

        let list = rule(json!({"match": {"sender": ["bead", "axe"]}}));
        assert_eq!(
            list.r#match.sender,
            Some(vec!["bead".to_string(), "axe".to_string()])
        );

        let null = rule(json!({"match": {"sender": null}}));
        assert_eq!(null.r#match.sender, None);

        for bad in [json!(7), json!(true), json!({"a": 1}), json!([1, 2])] {
            let err = serde_json::from_value::<NotificationRuleWire>(json!({
                "match": {"tags": bad},
            }))
            .unwrap_err()
            .to_string();
            assert!(!err.is_empty());
        }
        let err = serde_json::from_value::<NotificationRuleWire>(json!({
            "match": {"tags": 7},
        }))
        .unwrap_err()
        .to_string();
        assert!(err.contains("a string or a list of strings"), "{err}");
    }

    #[test]
    fn rule_defaults_and_wire_shapes_round_trip() {
        let bare = rule(json!({}));
        assert_eq!(bare, NotificationRuleWire::default());
        assert_eq!(bare.priority, 0);

        let full = rule(json!({
            "name": "n",
            "description": "why",
            "priority": -3,
            "match": {"tab": "beads", "tags": ["a", "b"]},
            "toast": false,
            "sound": "none",
        }));
        let round_tripped: NotificationRuleWire =
            serde_json::from_value(serde_json::to_value(&full).unwrap())
                .unwrap();
        assert_eq!(round_tripped, full);
        // The keyword field serializes under its config name.
        assert_eq!(
            serde_json::to_value(&full).unwrap()["match"]["tab"],
            json!(["beads"])
        );
    }

    #[test]
    fn delivery_serializes_with_a_tagged_sound() {
        assert_eq!(
            serde_json::to_value(built_in()).unwrap(),
            json!({
                "schema_version": NOTIFICATION_STORE_WIRE_SCHEMA_VERSION,
                "toast": true,
                "sound": {"kind": "bell"},
            })
        );

        let rules =
            vec![rule(json!({"name": "a", "toast": false, "sound": "none"}))];
        let value = serde_json::to_value(resolve_notification_delivery(
            &rules,
            &axe_error(),
        ))
        .unwrap();
        assert_eq!(value["sound"], json!({"kind": "none"}));
        assert_eq!(value["toast_rule"], json!("a"));
        assert_eq!(value["sound_rule"], json!("a"));

        let rules = vec![rule(json!({"sound": "/x/y z.wav"}))];
        let value = serde_json::to_value(resolve_notification_delivery(
            &rules,
            &axe_error(),
        ))
        .unwrap();
        assert_eq!(
            value["sound"],
            json!({"kind": "file", "path": "/x/y z.wav"})
        );
        assert_eq!(value["sound_rule"], json!("rule[0]"));
    }
}
