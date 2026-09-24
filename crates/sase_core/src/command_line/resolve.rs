use std::collections::BTreeMap;

use crate::command_line::diagnostics::{sort_diagnostics, RawDiagnostic};
use crate::command_line::grammar::{CommandLineGrammar, PositionalCapacity};
use crate::command_line::run_policy::evaluate_run_policy;
use crate::command_line::signature::{build_signature, ActiveSlot};
use crate::command_line::tokenizer::{lex_line, unquoted_prefix, RawToken};
use crate::command_line::wire::{
    LineContextWire, LineSlotWire, LineTokenWire, RunPolicyOutcomeWire,
    COMMAND_LINE_WIRE_SCHEMA_VERSION,
};

struct PendingOption {
    option_index: usize,
    node_id: usize,
    token_index: usize,
}

struct PreState {
    node_id: usize,
    pending: Option<(usize, usize)>,
    after_separator: bool,
    in_remainder: bool,
    remainder_index: Option<usize>,
    unknown: bool,
    positional_counts: Vec<usize>,
}

pub fn resolve_line(
    grammar: &CommandLineGrammar,
    line: &str,
    cursor: usize,
) -> LineContextWire {
    let char_count = line.chars().count();
    let cursor = cursor.min(char_count);
    let raw_tokens = lex_line(line);
    let line_len = char_count;

    let prog_end = detect_prog(&raw_tokens);
    let walk_start = if prog_end { 1 } else { 0 };

    let argv: Vec<String> = raw_tokens
        .iter()
        .skip(walk_start)
        .map(|t| t.text.clone())
        .collect();

    let cursor_token_index = find_cursor_token(&raw_tokens, cursor);
    let cursor_on_prog = prog_end && cursor_token_index == Some(0);
    let effective_cursor: Option<usize> = if cursor_on_prog {
        None
    } else {
        cursor_token_index
    };

    let mut roles: Vec<String> = vec![String::new(); raw_tokens.len()];
    if prog_end {
        roles[0] = "prog".to_string();
    }

    let mut parsed: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut used_order: Vec<String> = Vec::new();
    let mut diagnostics: Vec<RawDiagnostic> = Vec::new();

    let mut node_id = grammar.root_id();
    let mut path: Vec<String> = Vec::new();
    let mut node_kind = "root".to_string();
    let mut pending: Option<PendingOption> = None;
    let mut after_separator = false;
    let mut in_remainder = false;
    let mut remainder_dest: Option<String> = None;
    let mut remainder_index: Option<usize> = None;
    let mut unknown = false;
    let mut positional_counts: Vec<usize> =
        vec![0; grammar.node(node_id).positionals.len()];
    let mut pre_state_for_cursor: Option<PreState> = None;
    let mut cursor_token_text = String::new();
    let mut cursor_token_span = (cursor, cursor);
    let mut cursor_token_quoted = false;

    if let Some(idx) = effective_cursor {
        cursor_token_text = raw_tokens[idx].text.clone();
        cursor_token_span = (raw_tokens[idx].start, raw_tokens[idx].end);
        cursor_token_quoted = raw_tokens[idx].quoted;
        if raw_tokens[idx].unterminated {
            diagnostics.push(RawDiagnostic {
                start: raw_tokens[idx].start,
                end: line_len,
                severity: "error".to_string(),
                code: "unterminated_quote".to_string(),
                message: "unterminated quote".to_string(),
                on_cursor_token: true,
            });
        }
    }

    for (walk_pos, token_index) in (walk_start..raw_tokens.len()).enumerate() {
        let _ = walk_pos;
        let token = &raw_tokens[token_index];
        if Some(token_index) == effective_cursor {
            pre_state_for_cursor = Some(PreState {
                node_id,
                pending: pending.as_ref().map(|p| (p.option_index, p.node_id)),
                after_separator,
                in_remainder,
                remainder_index,
                unknown,
                positional_counts: positional_counts.clone(),
            });
        }

        if unknown {
            roles[token_index] = "unknown".to_string();
            record_parsed_unknown(token, &mut parsed, &mut used_order);
            continue;
        }

        if in_remainder {
            roles[token_index] = "remainder".to_string();
            if let Some(dest) = remainder_dest.clone() {
                push_parsed(&mut parsed, &mut used_order, &dest, &token.text);
            }
            continue;
        }

        if let Some(pend) = pending.take() {
            if token.text == "--" {
                diagnostics.push(RawDiagnostic {
                    start: raw_tokens[pend.token_index].start,
                    end: raw_tokens[pend.token_index].end,
                    severity: "warning".to_string(),
                    code: "missing_value".to_string(),
                    message: format!(
                        "option {} needs a value",
                        display_option(
                            grammar,
                            pend.node_id,
                            pend.option_index
                        )
                    ),
                    on_cursor_token: Some(pend.token_index) == effective_cursor,
                });
                roles[token_index] = "separator".to_string();
                after_separator = true;
                continue;
            }
            if looks_like_option(&token.text) {
                diagnostics.push(RawDiagnostic {
                    start: raw_tokens[pend.token_index].start,
                    end: raw_tokens[pend.token_index].end,
                    severity: "warning".to_string(),
                    code: "missing_value".to_string(),
                    message: format!(
                        "option {} needs a value",
                        display_option(
                            grammar,
                            pend.node_id,
                            pend.option_index
                        )
                    ),
                    on_cursor_token: Some(pend.token_index) == effective_cursor,
                });
                pending = None;
            } else {
                roles[token_index] = "option_value".to_string();
                let dest = grammar.node(pend.node_id).options
                    [pend.option_index]
                    .dest
                    .clone();
                push_parsed(&mut parsed, &mut used_order, &dest, &token.text);
                check_invalid_choice_option(
                    grammar,
                    pend.node_id,
                    pend.option_index,
                    token,
                    token_index,
                    effective_cursor,
                    &mut diagnostics,
                );
                continue;
            }
        }

        if token.text == "--" && !token.quoted {
            roles[token_index] = "separator".to_string();
            after_separator = true;
            continue;
        }

        if after_separator {
            let (role, dest_opt, pos_idx) =
                assign_positional(grammar, node_id, &positional_counts);
            if role == "unknown" {
                roles[token_index] = "unknown".to_string();
                diagnostics.push(RawDiagnostic {
                    start: token.start,
                    end: token.end,
                    severity: "warning".to_string(),
                    code: "extra_argument".to_string(),
                    message: "extra argument".to_string(),
                    on_cursor_token: Some(token_index) == effective_cursor,
                });
            } else {
                roles[token_index] = role.clone();
                if let (Some(dest), Some(idx)) = (dest_opt.clone(), pos_idx) {
                    push_parsed(
                        &mut parsed,
                        &mut used_order,
                        &dest,
                        &token.text,
                    );
                    positional_counts[idx] += 1;
                    check_invalid_choice_positional(
                        grammar,
                        node_id,
                        idx,
                        token,
                        token_index,
                        effective_cursor,
                        &mut diagnostics,
                    );
                    if is_remainder_positional(grammar, node_id, idx) {
                        in_remainder = true;
                        remainder_dest = Some(dest);
                        remainder_index = Some(idx);
                    }
                }
            }
            continue;
        }

        if looks_like_option(&token.text) && !token.quoted {
            if token.text.starts_with("--") && token.text.len() > 2 {
                process_long_option(
                    grammar,
                    node_id,
                    token,
                    token_index,
                    effective_cursor,
                    &mut roles,
                    &mut parsed,
                    &mut used_order,
                    &mut pending,
                    &mut diagnostics,
                );
                continue;
            } else {
                process_short_option(
                    grammar,
                    node_id,
                    token,
                    token_index,
                    effective_cursor,
                    &mut roles,
                    &mut parsed,
                    &mut used_order,
                    &mut pending,
                    &mut diagnostics,
                );
                continue;
            }
        }

        let node = grammar.node(node_id);
        if !node.subcommand_ids.is_empty() {
            if let Some((child_id, canonical)) =
                grammar.child_canonical(node_id, &token.text)
            {
                roles[token_index] = "command".to_string();
                node_id = child_id;
                path.push(canonical);
                let child = grammar.node(node_id);
                node_kind = if child.subcommand_ids.is_empty() {
                    "leaf".to_string()
                } else {
                    "group".to_string()
                };
                positional_counts = vec![0; child.positionals.len()];
                after_separator = false;
                in_remainder = false;
                remainder_dest = None;
                remainder_index = None;
                continue;
            } else {
                roles[token_index] = "unknown".to_string();
                node_kind = "unknown".to_string();
                unknown = true;
                diagnostics.push(RawDiagnostic {
                    start: token.start,
                    end: token.end,
                    severity: "warning".to_string(),
                    code: "unknown_subcommand".to_string(),
                    message: format!("unknown subcommand '{}'", token.text),
                    on_cursor_token: Some(token_index) == effective_cursor,
                });
                record_parsed_unknown(token, &mut parsed, &mut used_order);
                continue;
            }
        }

        let (role, dest_opt, pos_idx) =
            assign_positional(grammar, node_id, &positional_counts);
        if role == "unknown" {
            roles[token_index] = "unknown".to_string();
            diagnostics.push(RawDiagnostic {
                start: token.start,
                end: token.end,
                severity: "warning".to_string(),
                code: "extra_argument".to_string(),
                message: "extra argument".to_string(),
                on_cursor_token: Some(token_index) == effective_cursor,
            });
        } else {
            roles[token_index] = role.clone();
            if let (Some(dest), Some(idx)) = (dest_opt.clone(), pos_idx) {
                push_parsed(&mut parsed, &mut used_order, &dest, &token.text);
                positional_counts[idx] += 1;
                check_invalid_choice_positional(
                    grammar,
                    node_id,
                    idx,
                    token,
                    token_index,
                    effective_cursor,
                    &mut diagnostics,
                );
                if role == "remainder"
                    || is_remainder_positional(grammar, node_id, idx)
                {
                    in_remainder = true;
                    remainder_dest = Some(dest);
                    remainder_index = Some(idx);
                }
            }
        }
    }

    if effective_cursor.is_none() {
        pre_state_for_cursor = Some(PreState {
            node_id,
            pending: pending.as_ref().map(|p| (p.option_index, p.node_id)),
            after_separator,
            in_remainder,
            remainder_index,
            unknown,
            positional_counts: positional_counts.clone(),
        });
        cursor_token_span = (cursor, cursor);
        cursor_token_text = String::new();
    }

    if let Some(pend) = &pending {
        if cursor != line_len {
            diagnostics.push(RawDiagnostic {
                start: raw_tokens[pend.token_index].start,
                end: raw_tokens[pend.token_index].end,
                severity: "warning".to_string(),
                code: "missing_value".to_string(),
                message: format!(
                    "option {} needs a value",
                    display_option(grammar, pend.node_id, pend.option_index)
                ),
                on_cursor_token: Some(pend.token_index) == effective_cursor,
            });
        }
    }

    let pre = pre_state_for_cursor.expect("pre-state always set");
    let (slot, active) = classify_slot(
        grammar,
        &raw_tokens,
        &pre,
        effective_cursor,
        &cursor_token_text,
        cursor_token_span,
        cursor_token_quoted,
        cursor,
        line,
    );

    let (eff_node_id, eff_is_bare_default) = resolve_effective_node(
        grammar,
        node_id,
        &node_kind,
        &path,
        &raw_tokens,
        walk_start,
    );
    let eff_node = grammar.node(eff_node_id);

    let mut run_policy: RunPolicyOutcomeWire =
        evaluate_run_policy(&eff_node.run_policy, &parsed);
    if node_kind == "unknown" || node_kind == "root" {
        run_policy = RunPolicyOutcomeWire {
            policy: "proc".to_string(),
            note: None,
        };
    }
    let mut writes = eff_node.writes;
    let mut stdin = eff_node.stdin;
    let mut confirms = eff_node.confirms_option.is_some();
    if node_kind == "unknown" || node_kind == "root" {
        writes = false;
        stdin = false;
        confirms = false;
    }
    let confirm_dest = eff_node
        .confirms_option
        .and_then(|i| eff_node.options.get(i))
        .map(|o| o.dest.clone());
    let confirm_flag_present = confirm_dest
        .as_ref()
        .is_some_and(|d| parsed.contains_key(d));

    if node_kind != "unknown" && node_kind != "root" {
        let node = grammar.node(node_id);
        if !node.subcommand_ids.is_empty()
            && eff_is_bare_default
            && node.default_child.is_none()
        {
            let typed_subcommand =
                raw_tokens.iter().skip(walk_start).any(|t| {
                    roles
                        .get(
                            raw_tokens
                                .iter()
                                .position(|x| {
                                    x.start == t.start && x.end == t.end
                                })
                                .unwrap_or(usize::MAX),
                        )
                        .is_some_and(|r| r == "command")
                });
            let _ = typed_subcommand;
            diagnostics.push(RawDiagnostic {
                start: line_len,
                end: line_len,
                severity: "info".to_string(),
                code: "missing_subcommand".to_string(),
                message: "missing subcommand".to_string(),
                on_cursor_token: false,
            });
        }
    }

    if node_kind == "leaf" || eff_is_bare_default {
        add_missing_required(
            grammar,
            eff_node_id,
            &parsed,
            &positional_counts,
            node_id,
            line_len,
            &mut diagnostics,
        );
    }

    if confirms && !confirm_flag_present {
        diagnostics.push(RawDiagnostic {
            start: line_len,
            end: line_len,
            severity: "info".to_string(),
            code: "asks_to_confirm".to_string(),
            message: "asks to confirm; add -y to skip".to_string(),
            on_cursor_token: false,
        });
    }

    let static_prefixes = static_candidates_for_filter(grammar, &pre, &slot);
    let filtered = apply_typing_filter(
        diagnostics,
        effective_cursor,
        &cursor_token_text,
        &static_prefixes,
    );

    let mut sorted = filtered;
    sort_diagnostics(&mut sorted);
    let diagnostics_wire = sorted.into_iter().map(|d| d.wire()).collect();

    let signature_segments =
        build_signature(grammar, eff_node_id, &node_kind, &active);
    let summary = if node_kind == "unknown" {
        String::new()
    } else if eff_is_bare_default {
        let child_name = grammar
            .node(node_id)
            .default_child
            .clone()
            .unwrap_or_default();
        let path_str = if path.is_empty() {
            grammar.prog.clone()
        } else {
            path.join(" ")
        };
        format!(
            "{} (runs '{} {}' by default)",
            eff_node.summary, path_str, child_name
        )
    } else {
        grammar.node(node_id).summary.clone()
    };

    let tokens_wire: Vec<LineTokenWire> = raw_tokens
        .iter()
        .enumerate()
        .map(|(i, t)| LineTokenWire {
            text: t.text.clone(),
            start: t.start,
            end: t.end,
            role: if roles[i].is_empty() {
                "unknown".to_string()
            } else {
                roles[i].clone()
            },
            quoted: t.quoted,
            unterminated: t.unterminated,
        })
        .collect();

    LineContextWire {
        tokens: tokens_wire,
        argv,
        path,
        node_kind,
        slot,
        used_dests: used_order,
        diagnostics: diagnostics_wire,
        signature: crate::command_line::wire::LineSignatureWire {
            segments: signature_segments,
            summary,
        },
        run_policy,
        writes,
        confirms,
        confirm_flag_present,
        stdin,
        schema_version: COMMAND_LINE_WIRE_SCHEMA_VERSION,
    }
}

fn detect_prog(tokens: &[RawToken]) -> bool {
    if tokens.is_empty() {
        return false;
    }
    tokens[0].start == 0 && tokens[0].text == "sase" && !tokens[0].quoted
}

fn find_cursor_token(tokens: &[RawToken], cursor: usize) -> Option<usize> {
    for (i, t) in tokens.iter().enumerate() {
        if t.start <= cursor && cursor <= t.end {
            return Some(i);
        }
    }
    None
}

pub fn looks_like_option(text: &str) -> bool {
    if text.len() <= 1 || !text.starts_with('-') {
        return false;
    }
    !is_negative_number(text)
}

pub fn is_negative_number(text: &str) -> bool {
    let bytes = text.as_bytes();
    if bytes.is_empty() || bytes[0] != b'-' {
        return false;
    }
    let rest = &text[1..];
    if rest.is_empty() {
        return false;
    }
    let mut chars = rest.chars();
    let mut seen_digit = false;
    let mut seen_dot = false;
    for c in chars.by_ref() {
        if c.is_ascii_digit() {
            seen_digit = true;
        } else if c == '.' && !seen_dot {
            seen_dot = true;
        } else {
            return false;
        }
    }
    seen_digit
}

fn push_parsed(
    parsed: &mut BTreeMap<String, Vec<String>>,
    order: &mut Vec<String>,
    dest: &str,
    value: &str,
) {
    if !parsed.contains_key(dest) {
        order.push(dest.to_string());
    }
    parsed
        .entry(dest.to_string())
        .or_default()
        .push(value.to_string());
}

fn record_parsed_unknown(
    token: &RawToken,
    parsed: &mut BTreeMap<String, Vec<String>>,
    order: &mut Vec<String>,
) {
    let _ = (token, parsed, order);
}

fn display_option(
    grammar: &CommandLineGrammar,
    node_id: usize,
    option_index: usize,
) -> String {
    grammar.node(node_id).options[option_index]
        .strings
        .first()
        .cloned()
        .unwrap_or_else(|| "--option".to_string())
}

#[allow(clippy::too_many_arguments)]
fn process_long_option(
    grammar: &CommandLineGrammar,
    node_id: usize,
    token: &RawToken,
    token_index: usize,
    cursor_index: Option<usize>,
    roles: &mut [String],
    parsed: &mut BTreeMap<String, Vec<String>>,
    order: &mut Vec<String>,
    pending: &mut Option<PendingOption>,
    diagnostics: &mut Vec<RawDiagnostic>,
) {
    let text = &token.text;
    let (name_part, value_part) = match text.find('=') {
        Some(pos) => {
            let byte: usize = text[..pos].chars().count();
            let _ = byte;
            (&text[..pos], Some(&text[pos + 1..]))
        }
        None => (text.as_str(), None),
    };
    let node = grammar.node(node_id);
    if let Some(&exact) = node.option_by_string.get(name_part) {
        let takes = node.options[exact].takes_value;
        let dest = node.options[exact].dest.clone();
        if takes {
            if let Some(value) = value_part {
                roles[token_index] = "option".to_string();
                push_parsed(parsed, order, &dest, value);
                check_invalid_choice_option(
                    grammar,
                    node_id,
                    exact,
                    &RawToken {
                        text: value.to_string(),
                        start: token.start,
                        end: token.end,
                        quoted: token.quoted,
                        unterminated: false,
                    },
                    token_index,
                    cursor_index,
                    diagnostics,
                );
            } else {
                roles[token_index] = "option".to_string();
                push_parsed(parsed, order, &dest, "true");
                *pending = Some(PendingOption {
                    option_index: exact,
                    node_id,
                    token_index,
                });
                if !node.options[exact].repeatable {}
            }
        } else {
            roles[token_index] = "option".to_string();
            push_parsed(parsed, order, &dest, "true");
            if value_part.is_some() {}
        }
        return;
    }
    let mut hits: Vec<usize> = Vec::new();
    for &idx in &node.long_options {
        for s in &node.options[idx].strings {
            if s.len() > 2 && s.starts_with("--") && s.starts_with(name_part) {
                hits.push(idx);
                break;
            }
        }
    }
    hits.sort_unstable();
    hits.dedup();
    if hits.is_empty() {
        roles[token_index] = "unknown".to_string();
        diagnostics.push(RawDiagnostic {
            start: token.start,
            end: token.end,
            severity: "warning".to_string(),
            code: "unknown_option".to_string(),
            message: format!("unknown option '{text}'"),
            on_cursor_token: Some(token_index) == cursor_index,
        });
        return;
    }
    let first_dest = node.options[hits[0]].dest.clone();
    let unique = hits.iter().all(|&i| node.options[i].dest == first_dest);
    if !unique {
        roles[token_index] = "unknown".to_string();
        let mut names: Vec<String> = hits
            .iter()
            .flat_map(|&i| node.options[i].strings.clone())
            .filter(|s| s.starts_with(name_part))
            .collect();
        names.sort();
        names.dedup();
        diagnostics.push(RawDiagnostic {
            start: token.start,
            end: token.end,
            severity: "warning".to_string(),
            code: "ambiguous_option".to_string(),
            message: format!("ambiguous option '{text}': {}", names.join(", ")),
            on_cursor_token: Some(token_index) == cursor_index,
        });
        return;
    }
    let idx = hits[0];
    let takes = node.options[idx].takes_value;
    let dest = node.options[idx].dest.clone();
    if takes {
        if let Some(value) = value_part {
            roles[token_index] = "option".to_string();
            push_parsed(parsed, order, &dest, value);
        } else {
            roles[token_index] = "option".to_string();
            push_parsed(parsed, order, &dest, "true");
            *pending = Some(PendingOption {
                option_index: idx,
                node_id,
                token_index,
            });
        }
    } else {
        roles[token_index] = "option".to_string();
        push_parsed(parsed, order, &dest, "true");
    }
}

#[allow(clippy::too_many_arguments)]
fn process_short_option(
    grammar: &CommandLineGrammar,
    node_id: usize,
    token: &RawToken,
    token_index: usize,
    cursor_index: Option<usize>,
    roles: &mut [String],
    parsed: &mut BTreeMap<String, Vec<String>>,
    order: &mut Vec<String>,
    pending: &mut Option<PendingOption>,
    diagnostics: &mut Vec<RawDiagnostic>,
) {
    let text = &token.text;
    let node = grammar.node(node_id);
    let chars: Vec<char> = text.chars().collect();
    let mut pos = 1;
    let mut consumed_value = false;
    while pos < chars.len() {
        let flag = format!("-{}", chars[pos]);
        if let Some(&opt_idx) = node.option_by_string.get(&flag) {
            let takes = node.options[opt_idx].takes_value;
            let dest = node.options[opt_idx].dest.clone();
            if takes {
                let rest: String = chars[pos + 1..].iter().collect();
                if rest.is_empty() {
                    push_parsed(parsed, order, &dest, "true");
                    *pending = Some(PendingOption {
                        option_index: opt_idx,
                        node_id,
                        token_index,
                    });
                } else {
                    let value =
                        rest.strip_prefix('=').unwrap_or(&rest).to_string();
                    push_parsed(parsed, order, &dest, &value);
                    check_invalid_choice_option(
                        grammar,
                        node_id,
                        opt_idx,
                        &RawToken {
                            text: value,
                            start: token.start,
                            end: token.end,
                            quoted: token.quoted,
                            unterminated: false,
                        },
                        token_index,
                        cursor_index,
                        diagnostics,
                    );
                }
                consumed_value = true;
                break;
            } else {
                push_parsed(parsed, order, &dest, "true");
                pos += 1;
                continue;
            }
        } else {
            diagnostics.push(RawDiagnostic {
                start: token.start,
                end: token.end,
                severity: "warning".to_string(),
                code: "unknown_option".to_string(),
                message: format!("unknown option '-{}'", chars[pos]),
                on_cursor_token: Some(token_index) == cursor_index,
            });
            break;
        }
    }
    roles[token_index] = "option".to_string();
    let _ = consumed_value;
}

fn assign_positional(
    grammar: &CommandLineGrammar,
    node_id: usize,
    counts: &[usize],
) -> (String, Option<String>, Option<usize>) {
    let node = grammar.node(node_id);
    for (i, positional) in node.positionals.iter().enumerate() {
        let used = counts.get(i).copied().unwrap_or(0);
        match positional.capacity() {
            PositionalCapacity::Fixed(n) => {
                if used < n {
                    return (
                        "positional".to_string(),
                        Some(positional.dest.clone()),
                        Some(i),
                    );
                }
            }
            PositionalCapacity::Optional => {
                if used < 1 {
                    return (
                        "positional".to_string(),
                        Some(positional.dest.clone()),
                        Some(i),
                    );
                }
            }
            PositionalCapacity::Greedy { .. } => {
                return (
                    "positional".to_string(),
                    Some(positional.dest.clone()),
                    Some(i),
                );
            }
            PositionalCapacity::Remainder => {
                return (
                    "remainder".to_string(),
                    Some(positional.dest.clone()),
                    Some(i),
                );
            }
        }
    }
    ("unknown".to_string(), None, None)
}

fn is_remainder_positional(
    grammar: &CommandLineGrammar,
    node_id: usize,
    index: usize,
) -> bool {
    grammar
        .node(node_id)
        .positionals
        .get(index)
        .is_some_and(|p| matches!(p.capacity(), PositionalCapacity::Remainder))
}

fn check_invalid_choice_option(
    grammar: &CommandLineGrammar,
    node_id: usize,
    option_index: usize,
    token: &RawToken,
    token_index: usize,
    cursor_index: Option<usize>,
    diagnostics: &mut Vec<RawDiagnostic>,
) {
    let option = &grammar.node(node_id).options[option_index];
    if let Some(choices) = &option.choices {
        if !choices.contains(&token.text) {
            diagnostics.push(RawDiagnostic {
                start: token.start,
                end: token.end,
                severity: "warning".to_string(),
                code: "invalid_choice".to_string(),
                message: format!(
                    "invalid value '{}' for {}",
                    token.text, option.dest
                ),
                on_cursor_token: Some(token_index) == cursor_index,
            });
        }
    }
}

fn check_invalid_choice_positional(
    grammar: &CommandLineGrammar,
    node_id: usize,
    positional_index: usize,
    token: &RawToken,
    token_index: usize,
    cursor_index: Option<usize>,
    diagnostics: &mut Vec<RawDiagnostic>,
) {
    let positional = &grammar.node(node_id).positionals[positional_index];
    if let Some(choices) = &positional.choices {
        if !choices.contains(&token.text) {
            diagnostics.push(RawDiagnostic {
                start: token.start,
                end: token.end,
                severity: "warning".to_string(),
                code: "invalid_choice".to_string(),
                message: format!(
                    "invalid value '{}' for {}",
                    token.text, positional.dest
                ),
                on_cursor_token: Some(token_index) == cursor_index,
            });
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn classify_slot(
    grammar: &CommandLineGrammar,
    raw_tokens: &[RawToken],
    pre: &PreState,
    cursor_index: Option<usize>,
    cursor_text: &str,
    cursor_span: (usize, usize),
    cursor_quoted: bool,
    cursor: usize,
    line: &str,
) -> (LineSlotWire, ActiveSlot) {
    let node = grammar.node(pre.node_id);
    if pre.unknown {
        return (
            LineSlotWire {
                kind: "none".to_string(),
                dest: None,
                value_kind: None,
                choices: None,
                value_hint: None,
                prefix: unquoted_prefix(line, cursor_span.0, cursor),
                replace_start: cursor_span.0,
                replace_end: cursor_span.1,
            },
            ActiveSlot::none(),
        );
    }
    if let Some(idx) = cursor_index {
        let token = &raw_tokens[idx];
        if !cursor_quoted && !pre.after_separator && !pre.in_remainder {
            if let Some((
                dest,
                value_kind,
                choices,
                value_hint,
                replace_start,
            )) = attached_value_slot(grammar, pre, token, cursor)
            {
                return (
                    LineSlotWire {
                        kind: "option_value".to_string(),
                        dest: Some(dest.clone()),
                        value_kind,
                        choices,
                        value_hint,
                        prefix: unquoted_prefix(line, replace_start, cursor),
                        replace_start,
                        replace_end: cursor_span.1,
                    },
                    ActiveSlot {
                        kind: "option_value".to_string(),
                        dest: Some(dest.clone()),
                        positional_index: None,
                        option_dest: Some(dest),
                    },
                );
            }
        }
        if !cursor_quoted
            && token.text.starts_with('-')
            && token.text.len() > 1
            && !is_negative_number(&token.text)
            && !pre.after_separator
            && !pre.in_remainder
        {
            return (
                LineSlotWire {
                    kind: "option_name".to_string(),
                    dest: None,
                    value_kind: None,
                    choices: None,
                    value_hint: None,
                    prefix: unquoted_prefix(line, cursor_span.0, cursor),
                    replace_start: cursor_span.0,
                    replace_end: cursor_span.1,
                },
                ActiveSlot {
                    kind: "option_name".to_string(),
                    dest: None,
                    positional_index: None,
                    option_dest: None,
                },
            );
        }
        let _ = cursor_text;
    }
    if let Some((option_index, option_node)) = pre.pending {
        let option = &grammar.node(option_node).options[option_index];
        return (
            LineSlotWire {
                kind: "option_value".to_string(),
                dest: Some(option.dest.clone()),
                value_kind: option.kind.clone(),
                choices: option.choices.clone(),
                value_hint: option.value_hint.clone(),
                prefix: unquoted_prefix(line, cursor_span.0, cursor),
                replace_start: cursor_span.0,
                replace_end: cursor_span.1,
            },
            ActiveSlot {
                kind: "option_value".to_string(),
                dest: Some(option.dest.clone()),
                positional_index: None,
                option_dest: Some(option.dest.clone()),
            },
        );
    }
    if !node.subcommand_ids.is_empty() {
        return (
            LineSlotWire {
                kind: "subcommand".to_string(),
                dest: None,
                value_kind: None,
                choices: None,
                value_hint: None,
                prefix: unquoted_prefix(line, cursor_span.0, cursor),
                replace_start: cursor_span.0,
                replace_end: cursor_span.1,
            },
            ActiveSlot {
                kind: "subcommand".to_string(),
                dest: None,
                positional_index: None,
                option_dest: None,
            },
        );
    }
    if pre.in_remainder {
        let (dest, value_kind, choices, value_hint) = pre
            .remainder_index
            .and_then(|i| grammar.node(pre.node_id).positionals.get(i))
            .map(|p| {
                (
                    Some(p.dest.clone()),
                    p.kind.clone(),
                    p.choices.clone(),
                    p.value_hint.clone(),
                )
            })
            .unwrap_or((None, None, None, None));
        return (
            LineSlotWire {
                kind: "remainder".to_string(),
                dest,
                value_kind,
                choices,
                value_hint,
                prefix: unquoted_prefix(line, cursor_span.0, cursor),
                replace_start: cursor_span.0,
                replace_end: cursor_span.1,
            },
            ActiveSlot {
                kind: "remainder".to_string(),
                dest: None,
                positional_index: pre.remainder_index,
                option_dest: None,
            },
        );
    }
    let (role, dest_opt, pos_idx) =
        assign_positional(grammar, pre.node_id, &pre.positional_counts);
    if role == "positional" || role == "remainder" {
        let positional =
            pos_idx.and_then(|i| grammar.node(pre.node_id).positionals.get(i));
        if role == "remainder" {
            return (
                LineSlotWire {
                    kind: "remainder".to_string(),
                    dest: dest_opt.clone(),
                    value_kind: positional.and_then(|p| p.kind.clone()),
                    choices: positional.and_then(|p| p.choices.clone()),
                    value_hint: positional.and_then(|p| p.value_hint.clone()),
                    prefix: unquoted_prefix(line, cursor_span.0, cursor),
                    replace_start: cursor_span.0,
                    replace_end: cursor_span.1,
                },
                ActiveSlot {
                    kind: "remainder".to_string(),
                    dest: dest_opt,
                    positional_index: pos_idx,
                    option_dest: None,
                },
            );
        }
        return (
            LineSlotWire {
                kind: "positional".to_string(),
                dest: dest_opt.clone(),
                value_kind: positional.and_then(|p| p.kind.clone()),
                choices: positional.and_then(|p| p.choices.clone()),
                value_hint: positional.and_then(|p| p.value_hint.clone()),
                prefix: unquoted_prefix(line, cursor_span.0, cursor),
                replace_start: cursor_span.0,
                replace_end: cursor_span.1,
            },
            ActiveSlot {
                kind: "positional".to_string(),
                dest: dest_opt.clone(),
                positional_index: pos_idx,
                option_dest: None,
            },
        );
    }
    (
        LineSlotWire {
            kind: "none".to_string(),
            dest: None,
            value_kind: None,
            choices: None,
            value_hint: None,
            prefix: unquoted_prefix(line, cursor_span.0, cursor),
            replace_start: cursor_span.0,
            replace_end: cursor_span.1,
        },
        ActiveSlot::none(),
    )
}

type AttachedValueSlot = (
    String,
    Option<String>,
    Option<Vec<String>>,
    Option<String>,
    usize,
);

fn attached_value_slot(
    grammar: &CommandLineGrammar,
    pre: &PreState,
    token: &RawToken,
    cursor: usize,
) -> Option<AttachedValueSlot> {
    let text = &token.text;
    if text.starts_with("--") && text.len() > 2 {
        let eq = text.find('=')?;
        let name_part = &text[..eq];
        let node = grammar.node(pre.node_id);
        let mut resolved: Option<usize> =
            node.option_by_string.get(name_part).copied();
        if resolved.is_none() {
            let mut hits = Vec::new();
            for &idx in &node.long_options {
                for s in &node.options[idx].strings {
                    if s.len() > 2
                        && s.starts_with("--")
                        && s.starts_with(name_part)
                    {
                        hits.push(idx);
                        break;
                    }
                }
            }
            hits.sort_unstable();
            hits.dedup();
            if hits.len() == 1
                || (hits.len() > 1
                    && hits.iter().all(|&i| {
                        node.options[i].dest == node.options[hits[0]].dest
                    }))
            {
                resolved = hits.first().copied();
            }
        }
        let idx = resolved?;
        let option = &node.options[idx];
        if !option.takes_value {
            return None;
        }
        let eq_chars: usize = text[..eq].chars().count();
        let replace_start = token.start + eq_chars + 1;
        if cursor <= replace_start {
            return None;
        }
        return Some((
            option.dest.clone(),
            option.kind.clone(),
            option.choices.clone(),
            option.value_hint.clone(),
            replace_start,
        ));
    }
    if text.starts_with('-') && text.len() > 2 && !text.starts_with("--") {
        let node = grammar.node(pre.node_id);
        let chars: Vec<char> = text.chars().collect();
        let mut pos = 1;
        while pos < chars.len() {
            let flag = format!("-{}", chars[pos]);
            if let Some(&opt_idx) = node.option_by_string.get(&flag) {
                let option = &node.options[opt_idx];
                if option.takes_value {
                    let mut replace_start = token.start + pos + 1;
                    let rest: String = chars[pos + 1..].iter().collect();
                    if rest.starts_with('=') {
                        replace_start += 1;
                    }
                    if cursor <= token.start + pos + 1 && !rest.starts_with('=')
                    {
                        return None;
                    }
                    if cursor < replace_start {
                        return None;
                    }
                    return Some((
                        option.dest.clone(),
                        option.kind.clone(),
                        option.choices.clone(),
                        option.value_hint.clone(),
                        replace_start,
                    ));
                }
                pos += 1;
            } else {
                return None;
            }
        }
    }
    None
}

fn resolve_effective_node(
    grammar: &CommandLineGrammar,
    node_id: usize,
    node_kind: &str,
    path: &[String],
    raw_tokens: &[RawToken],
    walk_start: usize,
) -> (usize, bool) {
    if node_kind == "unknown" || node_kind == "root" {
        return (node_id, false);
    }
    let node = grammar.node(node_id);
    if node.subcommand_ids.is_empty() {
        return (node_id, false);
    }
    let Some(default_child) = node.default_child.clone() else {
        return (node_id, false);
    };
    let typed_command = raw_tokens.iter().skip(walk_start).any(|_| false);
    let _ = typed_command;
    let has_command_role =
        path.len() > grammar.node(node_id).canonical_path.len();
    if has_command_role {
        return (node_id, false);
    }
    if let Some(&child_id) =
        grammar.node(node_id).child_by_name.get(&default_child)
    {
        return (child_id, true);
    }
    (node_id, false)
}

fn add_missing_required(
    grammar: &CommandLineGrammar,
    eff_node_id: usize,
    parsed: &BTreeMap<String, Vec<String>>,
    positional_counts: &[usize],
    node_id: usize,
    line_len: usize,
    diagnostics: &mut Vec<RawDiagnostic>,
) {
    let eff = grammar.node(eff_node_id);
    let actual = grammar.node(node_id);
    let counts: &[usize] = if eff_node_id == node_id {
        positional_counts
    } else {
        &[]
    };
    for option in &eff.options {
        if option.required
            && !option.hidden
            && !parsed.contains_key(&option.dest)
        {
            diagnostics.push(RawDiagnostic {
                start: line_len,
                end: line_len,
                severity: "info".to_string(),
                code: "missing_required".to_string(),
                message: format!("missing required option {}", option.dest),
                on_cursor_token: false,
            });
        }
    }
    for (i, positional) in eff.positionals.iter().enumerate() {
        if !positional.required {
            continue;
        }
        let used = counts.get(i).copied().unwrap_or(0);
        let needed = match positional.capacity() {
            PositionalCapacity::Fixed(n) => n,
            PositionalCapacity::Optional => 0,
            PositionalCapacity::Greedy { at_least } => at_least,
            PositionalCapacity::Remainder => 1,
        };
        if used < needed && eff_node_id == node_id {
            diagnostics.push(RawDiagnostic {
                start: line_len,
                end: line_len,
                severity: "info".to_string(),
                code: "missing_required".to_string(),
                message: format!("missing required {}", positional.dest),
                on_cursor_token: false,
            });
        } else if eff_node_id != node_id && needed > 0 {
            let _ = actual;
            diagnostics.push(RawDiagnostic {
                start: line_len,
                end: line_len,
                severity: "info".to_string(),
                code: "missing_required".to_string(),
                message: format!("missing required {}", positional.dest),
                on_cursor_token: false,
            });
        }
    }
}

fn static_candidates_for_filter(
    grammar: &CommandLineGrammar,
    pre: &PreState,
    slot: &LineSlotWire,
) -> Vec<String> {
    let mut out = Vec::new();
    match slot.kind.as_str() {
        "subcommand" => {
            let node = grammar.node(pre.node_id);
            for &child_id in &node.subcommand_ids {
                let child = grammar.node(child_id);
                if child.hidden {
                    continue;
                }
                out.push(child.name.clone());
                for alias in &child.aliases {
                    out.push(alias.clone());
                }
            }
        }
        "option_name" => {
            let node = grammar.node(pre.node_id);
            for option in &node.options {
                if option.hidden {
                    continue;
                }
                out.extend(option.strings.clone());
            }
        }
        "option_value" | "positional" => {
            if let Some(choices) = &slot.choices {
                out.extend(choices.clone());
            }
        }
        _ => {}
    }
    out
}

fn apply_typing_filter(
    diagnostics: Vec<RawDiagnostic>,
    cursor_index: Option<usize>,
    cursor_text: &str,
    static_prefixes: &[String],
) -> Vec<RawDiagnostic> {
    let Some(cursor_idx) = cursor_index else {
        return diagnostics;
    };
    if static_prefixes.is_empty() {
        return diagnostics;
    }
    diagnostics
        .into_iter()
        .filter(|diag| {
            if !diag.on_cursor_token {
                return true;
            }
            if diag.code == "unterminated_quote" {
                return true;
            }
            let cursor_lower = cursor_text.to_lowercase();
            let is_prefix = static_prefixes.iter().any(|candidate| {
                candidate.to_lowercase().starts_with(&cursor_lower)
                    && candidate.len() > cursor_text.len()
            });
            if is_prefix {
                return false;
            }
            let _ = cursor_idx;
            true
        })
        .collect()
}
