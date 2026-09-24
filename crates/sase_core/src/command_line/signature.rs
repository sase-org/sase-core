use crate::command_line::grammar::{CommandLineGrammar, PositionalCapacity};
use crate::command_line::wire::SignatureSegmentWire;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveSlot {
    pub kind: String,
    pub dest: Option<String>,
    pub positional_index: Option<usize>,
    pub option_dest: Option<String>,
}

impl ActiveSlot {
    pub fn none() -> Self {
        Self {
            kind: "none".to_string(),
            dest: None,
            positional_index: None,
            option_dest: None,
        }
    }
}

pub fn build_signature(
    grammar: &CommandLineGrammar,
    node_id: usize,
    node_kind: &str,
    active: &ActiveSlot,
) -> Vec<SignatureSegmentWire> {
    let node = grammar.node(node_id);
    let mut segments = Vec::new();
    segments.push(SignatureSegmentWire {
        text: grammar.prog.clone(),
        role: "command".to_string(),
        active: false,
        required: false,
    });
    for part in &node.canonical_path {
        segments.push(SignatureSegmentWire {
            text: part.clone(),
            role: "command".to_string(),
            active: false,
            required: false,
        });
    }
    if node_kind == "unknown" {
        return segments;
    }
    if !node.subcommand_ids.is_empty() {
        segments.push(SignatureSegmentWire {
            text: "<subcommand>".to_string(),
            role: "subcommand".to_string(),
            active: active.kind == "subcommand",
            required: false,
        });
        return segments;
    }
    for (index, positional) in node.positionals.iter().enumerate() {
        if positional.hidden_visual() {
            continue;
        }
        let texts = positional_display(positional);
        for text in texts {
            let is_active = (active.kind == "positional"
                || active.kind == "remainder")
                && active.positional_index == Some(index);
            segments.push(SignatureSegmentWire {
                text,
                role: "positional".to_string(),
                active: is_active,
                required: positional.required,
            });
        }
    }
    let mut required_options: Vec<usize> = node
        .options
        .iter()
        .enumerate()
        .filter(|(_, o)| o.required && !o.hidden)
        .map(|(i, _)| i)
        .collect();
    required_options.sort_by_key(|&i| node.options[i].dest.clone());
    for index in required_options {
        let option = &node.options[index];
        let text = format!(
            "{} {}",
            long_or_first(option),
            option
                .metavar
                .clone()
                .unwrap_or_else(|| option.dest.to_uppercase())
        );
        segments.push(SignatureSegmentWire {
            text,
            role: "option".to_string(),
            active: active.option_dest.as_deref() == Some(option.dest.as_str()),
            required: true,
        });
    }
    if active.kind == "option_value" || active.kind == "option_name" {
        if let Some(dest) = &active.option_dest {
            if let Some(option) = node.options.iter().find(|o| &o.dest == dest)
            {
                if !option.required && !option.hidden {
                    let text = if option.takes_value {
                        format!(
                            "{} {}",
                            long_or_first(option),
                            option
                                .metavar
                                .clone()
                                .unwrap_or_else(|| option.dest.to_uppercase())
                        )
                    } else {
                        long_or_first(option)
                    };
                    if !segments.iter().any(|s| s.text == text) {
                        segments.push(SignatureSegmentWire {
                            text,
                            role: "option".to_string(),
                            active: true,
                            required: false,
                        });
                    }
                }
            }
        }
    }
    let has_other_visible =
        node.options.iter().any(|o| !o.hidden && !o.required);
    if has_other_visible {
        segments.push(SignatureSegmentWire {
            text: "[options]".to_string(),
            role: "options".to_string(),
            active: false,
            required: false,
        });
    }
    segments
}

pub fn usage_string(
    grammar: &CommandLineGrammar,
    node_id: usize,
    node_kind: &str,
) -> String {
    let segments = build_signature(
        grammar,
        node_id,
        node_kind,
        &ActiveSlot {
            kind: String::new(),
            dest: None,
            positional_index: None,
            option_dest: None,
        },
    );
    let joined = segments
        .iter()
        .map(|s| s.text.clone())
        .collect::<Vec<_>>()
        .join(" ");
    format!("usage: {joined}")
}

fn long_or_first(option: &crate::command_line::grammar::OptionNode) -> String {
    for s in &option.strings {
        if s.len() > 2 && s.starts_with("--") {
            return s.clone();
        }
    }
    option.strings.first().cloned().unwrap_or_default()
}

fn positional_display(
    positional: &crate::command_line::grammar::PositionalNode,
) -> Vec<String> {
    let m = positional.metavar.clone();
    match positional.capacity() {
        PositionalCapacity::Fixed(1) => vec![m],
        PositionalCapacity::Fixed(n) => vec![m; n],
        PositionalCapacity::Optional => vec![format!("[{m}]")],
        PositionalCapacity::Greedy { .. } => {
            if positional.metavar.contains("...") {
                vec![m]
            } else if positional.nargs_is_star() {
                vec![format!("[{m} ...]")]
            } else {
                vec![m.clone(), format!("[{m} ...]")]
            }
        }
        PositionalCapacity::Remainder => vec![format!("{m} ...")],
    }
}

trait PositionalExt {
    fn hidden_visual(&self) -> bool;
    fn nargs_is_star(&self) -> bool;
}

impl PositionalExt for crate::command_line::grammar::PositionalNode {
    fn hidden_visual(&self) -> bool {
        false
    }

    fn nargs_is_star(&self) -> bool {
        matches!(
            &self.nargs,
            Some(crate::command_line::wire::NargsWire::Str(s)) if s == "*"
        )
    }
}
