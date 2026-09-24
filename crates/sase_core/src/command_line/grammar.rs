use std::collections::BTreeMap;
use thiserror::Error;

use crate::command_line::wire::{
    CommandLineSpecWire, CommandSpecWire, NargsWire, OptionSpecWire,
    PositionalSpecWire,
};

#[derive(Debug, Error)]
pub enum CommandLineGrammarError {
    #[error("invalid command line spec: {0}")]
    InvalidSpec(String),
}

#[derive(Debug, Clone)]
pub struct OptionNode {
    pub strings: Vec<String>,
    pub dest: String,
    pub summary: String,
    pub takes_value: bool,
    pub repeatable: bool,
    pub choices: Option<Vec<String>>,
    pub kind: Option<String>,
    pub hidden: bool,
    pub required: bool,
    pub metavar: Option<String>,
    pub default: Option<String>,
    pub value_hint: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PositionalNode {
    pub metavar: String,
    pub dest: String,
    pub summary: String,
    pub nargs: Option<NargsWire>,
    pub choices: Option<Vec<String>>,
    pub kind: Option<String>,
    pub is_remainder: bool,
    pub required: bool,
    pub value_hint: Option<String>,
}

impl PositionalNode {
    pub fn capacity(&self) -> PositionalCapacity {
        if self.is_remainder {
            return PositionalCapacity::Remainder;
        }
        match &self.nargs {
            None => PositionalCapacity::Fixed(1),
            Some(NargsWire::Int(n)) => PositionalCapacity::Fixed(*n as usize),
            Some(NargsWire::Str(s)) => match s.as_str() {
                "?" => PositionalCapacity::Optional,
                "*" => PositionalCapacity::Greedy { at_least: 0 },
                "+" => PositionalCapacity::Greedy { at_least: 1 },
                "..." => PositionalCapacity::Remainder,
                other => {
                    if let Ok(n) = other.parse::<usize>() {
                        PositionalCapacity::Fixed(n)
                    } else {
                        PositionalCapacity::Fixed(1)
                    }
                }
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PositionalCapacity {
    Fixed(usize),
    Optional,
    Greedy { at_least: usize },
    Remainder,
}

#[derive(Debug, Clone)]
pub struct CommandNode {
    pub name: String,
    pub canonical_path: Vec<String>,
    pub aliases: Vec<String>,
    pub hidden: bool,
    pub summary: String,
    pub options: Vec<OptionNode>,
    pub positionals: Vec<PositionalNode>,
    pub subcommand_ids: Vec<usize>,
    pub child_by_name: BTreeMap<String, usize>,
    pub option_by_string: BTreeMap<String, usize>,
    pub long_options: Vec<usize>,
    pub mutex_groups: Vec<Vec<String>>,
    pub default_child: Option<String>,
    pub run_policy: Vec<crate::command_line::wire::RunPolicyRuleWire>,
    pub writes: bool,
    pub stdin: bool,
    pub parent: Option<usize>,
    pub confirms_option: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct CommandLineGrammar {
    pub prog: String,
    pub version: String,
    nodes: Vec<CommandNode>,
}

impl CommandLineGrammar {
    pub fn from_json(text: &str) -> Result<Self, CommandLineGrammarError> {
        let spec: CommandLineSpecWire =
            serde_json::from_str(text).map_err(|error| {
                CommandLineGrammarError::InvalidSpec(error.to_string())
            })?;
        Self::from_spec(&spec)
    }

    pub fn from_spec(
        spec: &CommandLineSpecWire,
    ) -> Result<Self, CommandLineGrammarError> {
        let mut grammar = Self {
            prog: spec.prog.clone(),
            version: spec.version.clone(),
            nodes: Vec::new(),
        };
        grammar.insert_command(&spec.root, None, Vec::new())?;
        Ok(grammar)
    }

    fn insert_command(
        &mut self,
        wire: &CommandSpecWire,
        parent: Option<usize>,
        parent_path: Vec<String>,
    ) -> Result<usize, CommandLineGrammarError> {
        let mut canonical_path = parent_path;
        if parent.is_some() {
            canonical_path.push(wire.name.clone());
        }
        let id = self.nodes.len();
        let mut node = CommandNode {
            name: wire.name.clone(),
            canonical_path: canonical_path.clone(),
            aliases: wire.aliases.clone(),
            hidden: wire.hidden,
            summary: wire.summary.clone(),
            options: wire
                .options
                .iter()
                .map(|o| OptionNode {
                    strings: o.strings.clone(),
                    dest: o.dest.clone(),
                    summary: o.summary.clone(),
                    takes_value: o.takes_value,
                    repeatable: o.repeatable,
                    choices: o.choices.clone(),
                    kind: o.kind.clone(),
                    hidden: o.hidden,
                    required: o.required,
                    metavar: o.metavar.clone(),
                    default: o.default.clone(),
                    value_hint: o.value_hint.clone(),
                })
                .collect(),
            positionals: wire
                .positionals
                .iter()
                .map(|p| PositionalNode {
                    metavar: p.metavar.clone(),
                    dest: p.dest.clone(),
                    summary: p.summary.clone(),
                    nargs: p.nargs.clone(),
                    choices: p.choices.clone(),
                    kind: p.kind.clone(),
                    is_remainder: p.is_remainder
                        || matches!(
                            p.nargs,
                            Some(NargsWire::Str(ref s)) if s == "..."
                        ),
                    required: p.required,
                    value_hint: p.value_hint.clone(),
                })
                .collect(),
            subcommand_ids: Vec::new(),
            child_by_name: BTreeMap::new(),
            option_by_string: BTreeMap::new(),
            long_options: Vec::new(),
            mutex_groups: wire.mutex_groups.clone(),
            default_child: wire.default_child.clone(),
            run_policy: wire.run_policy.clone(),
            writes: wire.writes,
            stdin: wire.stdin,
            parent,
            confirms_option: None,
        };
        for (index, option) in node.options.iter().enumerate() {
            for string in &option.strings {
                node.option_by_string.insert(string.clone(), index);
            }
            if option
                .strings
                .iter()
                .any(|s| s.len() > 2 && s.starts_with("--"))
            {
                node.long_options.push(index);
            }
            if node.confirms_option.is_none()
                && option.strings.iter().any(|s| s == "-y" || s == "--yes")
            {
                node.confirms_option = Some(index);
            }
        }
        self.nodes.push(node);
        let mut child_ids = Vec::new();
        for child in &wire.subcommands {
            let child_id =
                self.insert_command(child, Some(id), canonical_path.clone())?;
            child_ids.push(child_id);
        }
        {
            let names: Vec<(String, Vec<String>)> = child_ids
                .iter()
                .map(|&child_id| {
                    let child = &self.nodes[child_id];
                    (child.name.clone(), child.aliases.clone())
                })
                .collect();
            let node = &mut self.nodes[id];
            node.subcommand_ids = child_ids.clone();
            for (child_id, (name, aliases)) in child_ids.iter().zip(names) {
                node.child_by_name.insert(name, *child_id);
                for alias in aliases {
                    node.child_by_name.insert(alias, *child_id);
                }
            }
        }
        Ok(id)
    }

    pub fn command_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn root_id(&self) -> usize {
        0
    }

    pub fn node(&self, id: usize) -> &CommandNode {
        &self.nodes[id]
    }

    pub fn lookup_path(&self, path: &[String]) -> Option<usize> {
        let mut id = 0;
        for part in path {
            let next = self.nodes[id].child_by_name.get(part).copied()?;
            id = next;
        }
        Some(id)
    }

    pub fn child_canonical(
        &self,
        id: usize,
        word: &str,
    ) -> Option<(usize, String)> {
        let child_id = self.nodes[id].child_by_name.get(word).copied()?;
        let name = self.nodes[child_id].name.clone();
        Some((child_id, name))
    }

    pub fn nodes(&self) -> &[CommandNode] {
        &self.nodes
    }

    pub fn option_by_dest(&self, id: usize, dest: &str) -> Option<&OptionNode> {
        self.nodes[id].options.iter().find(|o| o.dest == dest)
    }
}

impl From<&OptionSpecWire> for OptionNode {
    fn from(value: &OptionSpecWire) -> Self {
        Self {
            strings: value.strings.clone(),
            dest: value.dest.clone(),
            summary: value.summary.clone(),
            takes_value: value.takes_value,
            repeatable: value.repeatable,
            choices: value.choices.clone(),
            kind: value.kind.clone(),
            hidden: value.hidden,
            required: value.required,
            metavar: value.metavar.clone(),
            default: value.default.clone(),
            value_hint: value.value_hint.clone(),
        }
    }
}

impl From<&PositionalSpecWire> for PositionalNode {
    fn from(value: &PositionalSpecWire) -> Self {
        Self {
            metavar: value.metavar.clone(),
            dest: value.dest.clone(),
            summary: value.summary.clone(),
            nargs: value.nargs.clone(),
            choices: value.choices.clone(),
            kind: value.kind.clone(),
            is_remainder: value.is_remainder,
            required: value.required,
            value_hint: value.value_hint.clone(),
        }
    }
}
