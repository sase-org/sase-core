use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const COMMAND_LINE_WIRE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CommandLineSpecWire {
    #[serde(default)]
    pub prog: String,
    #[serde(default)]
    pub version: String,
    pub root: CommandSpecWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CommandSpecWire {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub path: Vec<String>,
    #[serde(default)]
    pub aliases: Vec<String>,
    #[serde(default)]
    pub hidden: bool,
    #[serde(default)]
    pub summary: String,
    #[serde(default)]
    pub options: Vec<OptionSpecWire>,
    #[serde(default)]
    pub positionals: Vec<PositionalSpecWire>,
    #[serde(default)]
    pub subcommands: Vec<CommandSpecWire>,
    #[serde(default)]
    pub default_child: Option<String>,
    #[serde(default)]
    pub mutex_groups: Vec<Vec<String>>,
    #[serde(default)]
    pub run_policy: Vec<RunPolicyRuleWire>,
    #[serde(default)]
    pub writes: bool,
    #[serde(default)]
    pub stdin: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct OptionSpecWire {
    #[serde(default)]
    pub strings: Vec<String>,
    #[serde(default)]
    pub dest: String,
    #[serde(default)]
    pub summary: String,
    #[serde(default)]
    pub takes_value: bool,
    #[serde(default)]
    pub repeatable: bool,
    #[serde(default)]
    pub choices: Option<Vec<String>>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub hidden: bool,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub metavar: Option<String>,
    #[serde(default)]
    pub default: Option<String>,
    #[serde(default)]
    pub value_hint: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PositionalSpecWire {
    #[serde(default)]
    pub metavar: String,
    #[serde(default)]
    pub dest: String,
    #[serde(default)]
    pub summary: String,
    #[serde(default)]
    pub nargs: Option<NargsWire>,
    #[serde(default)]
    pub choices: Option<Vec<String>>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub is_remainder: bool,
    #[serde(default = "default_true")]
    pub required: bool,
    #[serde(default)]
    pub value_hint: Option<String>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub enum NargsWire {
    Int(u32),
    Str(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct RunPolicyRuleWire {
    #[serde(default)]
    pub policy: String,
    #[serde(default)]
    pub when: Option<RunPolicyWhenWire>,
    #[serde(default)]
    pub note: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct RunPolicyWhenWire {
    #[serde(default)]
    pub absent: Option<Vec<String>>,
    #[serde(default)]
    pub equals: Option<BTreeMap<String, serde_json::Value>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LineTokenWire {
    pub text: String,
    pub start: usize,
    pub end: usize,
    pub role: String,
    pub quoted: bool,
    pub unterminated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LineSlotWire {
    pub kind: String,
    pub dest: Option<String>,
    pub value_kind: Option<String>,
    pub choices: Option<Vec<String>>,
    pub value_hint: Option<String>,
    pub prefix: String,
    pub replace_start: usize,
    pub replace_end: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LineDiagnosticWire {
    pub start: usize,
    pub end: usize,
    pub severity: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignatureSegmentWire {
    pub text: String,
    pub role: String,
    pub active: bool,
    pub required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LineSignatureWire {
    pub segments: Vec<SignatureSegmentWire>,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunPolicyOutcomeWire {
    pub policy: String,
    pub note: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LineContextWire {
    pub tokens: Vec<LineTokenWire>,
    pub argv: Vec<String>,
    pub path: Vec<String>,
    pub node_kind: String,
    pub slot: LineSlotWire,
    pub used_dests: Vec<String>,
    pub diagnostics: Vec<LineDiagnosticWire>,
    pub signature: LineSignatureWire,
    pub run_policy: RunPolicyOutcomeWire,
    pub writes: bool,
    pub confirms: bool,
    pub confirm_flag_present: bool,
    pub stdin: bool,
    pub schema_version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct DynamicCandidateWire {
    pub value: String,
    #[serde(default)]
    pub display: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub badge: Option<String>,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub partial: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletionItemWire {
    pub insert_text: String,
    pub display: String,
    pub description: String,
    pub badge: String,
    pub source: String,
    pub match_runs: Vec<Vec<u32>>,
    pub selected: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandLineCompletionWire {
    pub replace_start: usize,
    pub replace_end: usize,
    pub items: Vec<CompletionItemWire>,
    pub total: usize,
    pub kind: String,
    pub schema_version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HelpPositionalWire {
    pub metavar: String,
    pub dest: String,
    pub summary: String,
    pub nargs: Option<NargsWireOut>,
    pub required: bool,
    pub choices: Option<Vec<String>>,
    pub value_kind: Option<String>,
    pub value_hint: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum NargsWireOut {
    Int(u32),
    Str(String),
}

impl From<&NargsWire> for Option<NargsWireOut> {
    fn from(value: &NargsWire) -> Self {
        Some(match value {
            NargsWire::Int(n) => NargsWireOut::Int(*n),
            NargsWire::Str(s) => NargsWireOut::Str(s.clone()),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HelpOptionWire {
    pub strings: Vec<String>,
    pub dest: String,
    pub summary: String,
    pub metavar: Option<String>,
    pub takes_value: bool,
    pub required: bool,
    pub repeatable: bool,
    pub default: Option<String>,
    pub choices: Option<Vec<String>>,
    pub value_kind: Option<String>,
    pub value_hint: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HelpChildWire {
    pub name: String,
    pub aliases: Vec<String>,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandHelpWire {
    pub usage: String,
    pub summary: String,
    pub positionals: Vec<HelpPositionalWire>,
    pub options: Vec<HelpOptionWire>,
    pub children: Vec<HelpChildWire>,
    pub default_child: Option<String>,
    pub run_policy: Vec<RunPolicyRuleWire>,
    pub writes: bool,
    pub stdin: bool,
    pub confirms: bool,
}

impl Serialize for NargsWire {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match self {
            NargsWire::Int(n) => serializer.serialize_u32(*n),
            NargsWire::Str(s) => serializer.serialize_str(s),
        }
    }
}
