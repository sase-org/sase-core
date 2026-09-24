use crate::command_line::grammar::CommandLineGrammar;
use crate::command_line::signature::usage_string;
use crate::command_line::wire::{
    CommandHelpWire, HelpChildWire, HelpOptionWire, HelpPositionalWire,
};

pub fn command_help(
    grammar: &CommandLineGrammar,
    path: &[String],
) -> Option<CommandHelpWire> {
    let node_id = grammar.lookup_path(path)?;
    let node = grammar.node(node_id);
    let node_kind = if node.subcommand_ids.is_empty() {
        "leaf"
    } else if node.parent.is_none() {
        "root"
    } else {
        "group"
    };
    let usage = usage_string(grammar, node_id, node_kind);
    let positionals = node
        .positionals
        .iter()
        .map(|p| HelpPositionalWire {
            metavar: p.metavar.clone(),
            dest: p.dest.clone(),
            summary: p.summary.clone(),
            nargs: p.nargs.as_ref().and_then(|n| n.into()),
            required: p.required,
            choices: p.choices.clone(),
            value_kind: p.kind.clone(),
            value_hint: p.value_hint.clone(),
        })
        .collect();
    let options = node
        .options
        .iter()
        .filter(|o| !o.hidden)
        .map(|o| HelpOptionWire {
            strings: o.strings.clone(),
            dest: o.dest.clone(),
            summary: o.summary.clone(),
            metavar: o.metavar.clone(),
            takes_value: o.takes_value,
            required: o.required,
            repeatable: o.repeatable,
            default: o.default.clone(),
            choices: o.choices.clone(),
            value_kind: o.kind.clone(),
            value_hint: o.value_hint.clone(),
        })
        .collect();
    let mut children: Vec<HelpChildWire> = node
        .subcommand_ids
        .iter()
        .map(|&child_id| {
            let child = grammar.node(child_id);
            HelpChildWire {
                name: child.name.clone(),
                aliases: child.aliases.clone(),
                summary: child.summary.clone(),
            }
        })
        .collect();
    children.sort_by(|a, b| a.name.cmp(&b.name));
    Some(CommandHelpWire {
        usage,
        summary: node.summary.clone(),
        positionals,
        options,
        children,
        default_child: node.default_child.clone(),
        run_policy: node.run_policy.clone(),
        writes: node.writes,
        stdin: node.stdin,
        confirms: node.confirms_option.is_some(),
    })
}
